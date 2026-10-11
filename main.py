import asyncio
import html
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import Optional

from dotenv import load_dotenv

import yt_dlp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatAction
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import FSInputFile, Message
from aiogram.client.default import DefaultBotProperties

# ─────────────────────────── ENV ───────────────────────────
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ Укажи BOT_TOKEN в файле .env")

ADMIN_IDS = {
    int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_IDS", "")) if x.strip().isdigit()
}
if not ADMIN_IDS:
    print("⚠️  ADMIN_IDS пуст — админ-команды никому не доступны.")

DOWNLOAD_DIR       = os.getenv("DOWNLOAD_DIR", "downloads")
CACHE_DIR          = os.getenv("CACHE_DIR", "cache")
DB_PATH            = os.getenv("DB_PATH", "bot.db")
MAX_FILE_SIZE_MB   = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_DURATION_SEC   = int(os.getenv("MAX_DURATION_SEC", "600"))
DOWNLOAD_TIMEOUT   = int(os.getenv("DOWNLOAD_TIMEOUT", "300"))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", "3"))
USER_COOLDOWN_SEC  = int(os.getenv("USER_COOLDOWN_SEC", "10"))
COOKIES_FILE       = os.getenv("COOKIES_FILE", "").strip() or None
PROXY              = os.getenv("PROXY", "").strip() or None
LIKEE_USER_AGENT   = os.getenv("LIKEE_USER_AGENT", "").strip() or None
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO").upper()

for d in (DOWNLOAD_DIR, CACHE_DIR, "logs"):
    os.makedirs(d, exist_ok=True)

# ─────────────────────────── LOGGING ───────────────────────
logger = logging.getLogger("video-bot")
logger.setLevel(LOG_LEVEL)

_fmt = logging.Formatter(
    "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
logger.addHandler(_sh)
_fh = RotatingFileHandler("logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

# ─────────────────────────── DB ────────────────────────────
class DB:
    def __init__(self, path: str):
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._init()

    def _init(self):
        c = self._conn.cursor()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            full_name   TEXT,
            first_seen  INTEGER NOT NULL,
            last_seen   INTEGER NOT NULL,
            downloads   INTEGER NOT NULL DEFAULT 0,
            errors      INTEGER NOT NULL DEFAULT 0,
            bytes_total INTEGER NOT NULL DEFAULT 0,
            is_banned   INTEGER NOT NULL DEFAULT 0,
            ban_reason  TEXT
        );
        CREATE TABLE IF NOT EXISTS downloads (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            url         TEXT NOT NULL,
            video_id    TEXT,
            title       TEXT,
            size_bytes  INTEGER,
            duration    INTEGER,
            status      TEXT NOT NULL,   -- 'ok' | 'error'
            error       TEXT,
            ts          INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dl_user ON downloads(user_id);
        CREATE INDEX IF NOT EXISTS idx_dl_ts   ON downloads(ts);
        """)
        self._conn.commit()

    # --- users ---
    def upsert_user(self, user_id: int, username: str | None, full_name: str):
        now = int(time.time())
        with self._conn:
            self._conn.execute("""
                INSERT INTO users (user_id, username, full_name, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    full_name=excluded.full_name,
                    last_seen=excluded.last_seen
            """, (user_id, username, full_name, now, now))

    def get_user(self, user_id: int) -> Optional[sqlite3.Row]:
        return self._conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

    def is_banned(self, user_id: int) -> bool:
        r = self._conn.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(r and r["is_banned"])

    def set_ban(self, user_id: int, banned: bool, reason: str = ""):
        with self._conn:
            self._conn.execute(
                "UPDATE users SET is_banned=?, ban_reason=? WHERE user_id=?",
                (1 if banned else 0, reason, user_id),
            )

    def list_users(self, limit=20, offset=0):
        return self._conn.execute(
            "SELECT * FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()

    def users_count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]

    def banned_users(self):
        return self._conn.execute(
            "SELECT * FROM users WHERE is_banned=1 ORDER BY last_seen DESC"
        ).fetchall()

    def top_users(self, limit=10):
        return self._conn.execute("""
            SELECT user_id, username, full_name, downloads, bytes_total
            FROM users ORDER BY downloads DESC LIMIT ?
        """, (limit,)).fetchall()

    # --- downloads ---
    def log_download(self, user_id, url, video_id, title, size, duration, status, error=""):
        with self._conn:
            self._conn.execute("""
                INSERT INTO downloads (user_id, url, video_id, title, size_bytes,
                                       duration, status, error, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (user_id, url, video_id, title, size, duration, status, error, int(time.time())))

            if status == "ok":
                self._conn.execute("""
                    UPDATE users SET downloads = downloads + 1,
                                     bytes_total = bytes_total + ?
                    WHERE user_id = ?
                """, (size or 0, user_id))
            else:
                self._conn.execute(
                    "UPDATE users SET errors = errors + 1 WHERE user_id=?", (user_id,)
                )

    def stats(self) -> dict:
        c = self._conn
        total_users = c.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        banned      = c.execute("SELECT COUNT(*) c FROM users WHERE is_banned=1").fetchone()["c"]
        active_24h  = c.execute(
            "SELECT COUNT(*) c FROM users WHERE last_seen > ?", (int(time.time()) - 86400,)
        ).fetchone()["c"]
        ok   = c.execute("SELECT COUNT(*) c, COALESCE(SUM(size_bytes),0) s FROM downloads WHERE status='ok'").fetchone()
        err  = c.execute("SELECT COUNT(*) c FROM downloads WHERE status='error'").fetchone()["c"]
        today_start = int(time.time()) - 86400
        today = c.execute(
            "SELECT COUNT(*) c FROM downloads WHERE status='ok' AND ts > ?", (today_start,)
        ).fetchone()["c"]
        return {
            "users": total_users,
            "banned": banned,
            "active_24h": active_24h,
            "downloads_ok": ok["c"],
            "downloads_err": err,
            "bytes_total": ok["s"] or 0,
            "downloads_24h": today,
        }

    def cleanup_downloads_older_than(self, days: int) -> int:
        cutoff = int(time.time()) - days * 86400
        with self._conn:
            cur = self._conn.execute("DELETE FROM downloads WHERE ts < ?", (cutoff,))
        return cur.rowcount

    def close(self):
        self._conn.close()

db = DB(DB_PATH)

# ─────────────────────────── FILTERS ───────────────────────
class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return bool(message.from_user and message.from_user.id in ADMIN_IDS)

# ─────────────────────────── BOT ───────────────────────────
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

download_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
user_last_request: dict[int, float] = {}
queue_len = 0
queue_lock = asyncio.Lock()

# ─────────────────────────── HELPERS ───────────────────────
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

def extract_url(text: str) -> Optional[str]:
    m = _URL_RE.search(text or "")
    return m.group(0) if m else None

def human_size(n: int) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def human_time(sec: float) -> str:
    sec = int(sec or 0)
    if sec < 60:
        return f"{sec} с"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m} м {s} с"
    h, m = divmod(m, 60)
    return f"{h} ч {m} м"

def cleanup_stale_dirs():
    now = time.time()
    for base in (DOWNLOAD_DIR,):
        for name in os.listdir(base):
            path = os.path.join(base, name)
            try:
                if os.path.isdir(path) and now - os.path.getmtime(path) > 3600:
                    shutil.rmtree(path, ignore_errors=True)
                    logger.info("Удалена старая папка: %s", path)
            except OSError:
                pass

def is_likee(url: str) -> bool:
    return "likee" in url.lower()

def cache_dir_size() -> tuple[int, int]:
    total, count = 0, 0
    for root, _, files in os.walk(CACHE_DIR):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
                count += 1
            except OSError:
                pass
    return total, count

@dataclass
class DownloadResult:
    file_path: str
    title: str
    duration: Optional[int] = None
    thumbnail: Optional[str] = None
    video_id: Optional[str] = None
    ext: str = ".mp4"
    meta: dict = field(default_factory=dict)

# ─────────────────────────── YT-DLP ────────────────────────
class ProgressHook:
    def __init__(self, message: Message, loop: asyncio.AbstractEventLoop):
        self.message = message
        self.loop = loop
        self.last = 0.0

    def __call__(self, d: dict):
        if d.get("status") != "downloading":
            return
        now = time.time()
        if now - self.last < 1.5:
            return
        self.last = now
        downloaded = d.get("downloaded_bytes") or 0
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        speed = d.get("speed") or 0
        eta = d.get("eta") or 0
        if total:
            pct = downloaded / total * 100
            bar_len = 16
            filled = int(bar_len * pct / 100)
            bar = "█" * filled + "░" * (bar_len - filled)
            text = (f"⬇️ <b>{pct:.1f}%</b>  [{bar}]\n"
                    f"📦 {human_size(downloaded)} / {human_size(total)}  "
                    f"⚡ {human_size(speed)}/с  ⏱ ETA {human_time(eta)}")
        else:
            text = f"⬇️ Скачано {human_size(downloaded)}  ⚡ {human_size(speed)}/с"
        asyncio.run_coroutine_threadsafe(self._safe_edit(text), self.loop)

    async def _safe_edit(self, text: str):
        try:
            await self.message.edit_text(text)
        except Exception:
            pass

def _ydl_opts(url: str, temp_dir: str, hook=None, for_cache: bool = False) -> dict:
    opts = {
        "outtmpl": os.path.join(temp_dir, "%(title).100s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
        "retries": 3,
        "fragment_retries": 3,
        "socket_timeout": 30,
        "nocheckcertificate": True,
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "merge_output_format": "mp4",
        "writethumbnail": True,
        "postprocessors": [
            {"key": "FFmpegThumbnailsConvertor", "format": "jpg", "when": "before_dl"},
        ],
    }
    if for_cache:
        opts["format"] = "best[ext=mp4]/best"
    if hook:
        opts["progress_hooks"] = [hook]
    if COOKIES_FILE and os.path.isfile(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    if PROXY:
        opts["proxy"] = PROXY
    if is_likee(url) and LIKEE_USER_AGENT:
        opts["http_headers"] = {"User-Agent": LIKEE_USER_AGENT}
    return opts

def _sync_download(url: str, opts: dict) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=True)

async def download_video(url: str, hook=None) -> DownloadResult:
    temp_dir = tempfile.mkdtemp(dir=DOWNLOAD_DIR)
    opts = _ydl_opts(url, temp_dir, hook=hook)
    try:
        info = await asyncio.wait_for(
            asyncio.to_thread(_sync_download, url, opts),
            timeout=DOWNLOAD_TIMEOUT,
        )
    except asyncio.TimeoutError:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Таймаут скачивания ({DOWNLOAD_TIMEOUT} с).")
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    file_path = None
    if isinstance(info, dict):
        requested = info.get("requested_downloads") or []
        if requested:
            file_path = requested[0].get("filepath")
    if not file_path or not os.path.isfile(file_path):
        candidates = [
            os.path.join(temp_dir, f) for f in os.listdir(temp_dir)
            if os.path.isfile(os.path.join(temp_dir, f))
            and not f.endswith((".jpg", ".jpeg", ".png", ".webp", ".json", ".part"))
        ]
        if not candidates:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise FileNotFoundError("Файл не найден после скачивания.")
        file_path = max(candidates, key=os.path.getsize)

    thumb_path = None
    for f in os.listdir(temp_dir):
        if f.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            thumb_path = os.path.join(temp_dir, f)
            break

    return DownloadResult(
        file_path=file_path,
        title=(info.get("title") or "Видео") if isinstance(info, dict) else "Видео",
        duration=info.get("duration") if isinstance(info, dict) else None,
        thumbnail=thumb_path,
        video_id=info.get("id") if isinstance(info, dict) else None,
        ext=os.path.splitext(file_path)[1].lower() or ".mp4",
        meta=info if isinstance(info, dict) else {},
    )

# ─────────────────────────── USER HANDLERS ─────────────────
@dp.message(CommandStart())
async def start_handler(message: Message):
    u = message.from_user
    if u:
        db.upsert_user(u.id, u.username, u.full_name)
    if u and db.is_banned(u.id):
        await message.answer("🚫 Ты заблокирован в этом боте.")
        return
    await message.answer(
        "Привет! 👋\n\n"
        "Отправь ссылку на видео с <b>YouTube</b>, <b>TikTok</b>, "
        "<b>Instagram</b> или <b>Likee</b> — я скачаю и пришлю его.\n\n"
        f"⚠️ Максимум: <b>{MAX_FILE_SIZE_MB} МБ</b>, "
        f"длительность ≤ <b>{human_time(MAX_DURATION_SEC)}</b>."
    )

@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "📖 <b>Как пользоваться</b>\n\n"
        "1. Отправь ссылку на видео.\n"
        "2. Дождись загрузки.\n"
        "3. Получи видео в ответ.\n\n"
        f"⚙️ Лимиты: {MAX_FILE_SIZE_MB} МБ, "
        f"до {human_time(MAX_DURATION_SEC)}, "
        f"кулдаун {USER_COOLDOWN_SEC} с между запросами."
    )

@dp.message()
async def download_handler(message: Message):
    u = message.from_user
    if not u:
        return

    db.upsert_user(u.id, u.username, u.full_name)

    if db.is_banned(u.id):
        await message.answer("🚫 Ты заблокирован в этом боте.")
        return

    url = extract_url(message.text or "")
    if not url:
        await message.answer("Пожалуйста, отправь ссылку на видео (http...).")
        return

    now = time.time()
    last = user_last_request.get(u.id, 0)
    if now - last < USER_COOLDOWN_SEC:
        wait = USER_COOLDOWN_SEC - (now - last)
        await message.answer(f"⏳ Подожди ещё {wait:.1f} с перед следующим запросом.")
        return
    user_last_request[u.id] = now

    status_msg = await message.answer("🔍 Получаю информацию...")
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    global queue_len
    async with queue_lock:
        queue_len += 1
        queue_pos = queue_len

    if queue_pos > 1:
        try:
            await status_msg.edit_text(
                f"🕐 В очереди: <b>{queue_pos}</b>. Начну, как только освободится слот..."
            )
        except Exception:
            pass

    temp_dir_for_cleanup = None
    result: Optional[DownloadResult] = None

    try:
        async with download_semaphore:
            loop = asyncio.get_running_loop()
            hook = ProgressHook(status_msg, loop)
            try:
                await status_msg.edit_text("⏳ Скачиваю видео...")
            except Exception:
                pass

            result = await download_video(url, hook=hook)
            temp_dir_for_cleanup = os.path.dirname(result.file_path)

            if result.duration and result.duration > MAX_DURATION_SEC:
                db.log_download(u.id, url, result.video_id, result.title, None,
                                result.duration, "error", "too_long")
                await status_msg.edit_text(
                    f"❌ Видео слишком длинное "
                    f"({human_time(result.duration)} > {human_time(MAX_DURATION_SEC)})."
                )
                return

            size = os.path.getsize(result.file_path)
            if size > MAX_FILE_SIZE_MB * 1024 * 1024:
                db.log_download(u.id, url, result.video_id, result.title, size,
                                result.duration, "error", "too_big")
                await status_msg.edit_text(
                    f"❌ Видео слишком большое "
                    f"({human_size(size)} > {MAX_FILE_SIZE_MB} МБ).\n"
                    "Telegram не позволяет отправить такой файл."
                )
                return

            # Кэш
            cached_path = result.file_path
            if result.video_id:
                dst_dir = os.path.join(CACHE_DIR, result.video_id)
                try:
                    os.makedirs(dst_dir, exist_ok=True)
                    dst = os.path.join(dst_dir, os.path.basename(result.file_path))
                    if not os.path.isfile(dst):
                        shutil.copy2(result.file_path, dst)
                    cached_path = dst
                except Exception as e:
                    logger.warning("Кэш не удался: %s", e)

            await status_msg.edit_text("📤 Отправляю...")
            title = html.escape((result.title or "Видео")[:900])
            caption_parts = [f"🎬 <b>{title}</b>"]
            if result.duration:
                caption_parts.append(f"⏱ {human_time(result.duration)}")
            caption = "\n".join(caption_parts)

            file = FSInputFile(cached_path)
            thumb = (FSInputFile(result.thumbnail)
                     if result.thumbnail and os.path.isfile(result.thumbnail) else None)

            if result.ext in {".mp4", ".mov", ".m4v"}:
                await message.answer_video(
                    file, caption=caption, thumbnail=thumb,
                    supports_streaming=True, duration=result.duration,
                )
            else:
                await message.answer_document(file, caption=caption, thumbnail=thumb)

            db.log_download(u.id, url, result.video_id, result.title, size,
                            result.duration, "ok")
            await status_msg.delete()
            logger.info("OK user=%s id=%s size=%s", u.id, result.video_id, human_size(size))

    except yt_dlp.utils.DownloadError as e:
        err = str(e)[:300]
        db.log_download(u.id, url, None, None, None, None, "error", err)
        logger.warning("DownloadError user=%s: %s", u.id, err)
        await _safe_edit(status_msg, f"❌ Ошибка загрузки:\n<code>{html.escape(err)}</code>")
    except asyncio.TimeoutError:
        db.log_download(u.id, url, None, None, None, None, "error", "timeout")
        await _safe_edit(status_msg, "❌ Превышено время ожидания загрузки.")
    except Exception as e:
        err = str(e)[:300]
        db.log_download(u.id, url, None, None, None, None, "error", err)
        logger.exception("Ошибка: %s", e)
        await _safe_edit(status_msg, f"❌ Ошибка:\n<code>{html.escape(err)}</code>")
    finally:
        async with queue_lock:
            queue_len = max(0, queue_len - 1)
        if temp_dir_for_cleanup and os.path.isdir(temp_dir_for_cleanup):
            if not os.path.abspath(temp_dir_for_cleanup).startswith(os.path.abspath(CACHE_DIR)):
                shutil.rmtree(temp_dir_for_cleanup, ignore_errors=True)

async def _safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except Exception:
        pass

# ─────────────────────────── ADMIN HANDLERS ────────────────
@dp.message(Command("admin"), IsAdmin())
async def admin_root(message: Message):
    await message.answer(
        "🛠 <b>Админ-панель</b>\n\n"
        "<b>Статистика</b>\n"
        "/stats — общая сводка\n"
        "/users — последние 20 юзеров\n"
        "/user &lt;id&gt; — карточка юзера\n"
        "/top — топ-10 по скачиваниям\n\n"
        "<b>Модерация</b>\n"
        "/ban &lt;id&gt; [причина] — забанить\n"
        "/unban &lt;id&gt; — разбанить\n"
        "/bans — список забаненных\n\n"
        "<b>Обслуживание</b>\n"
        "/broadcast &lt;текст&gt; — рассылка всем\n"
        "/clearcache — очистить кэш видео\n"
        "/cleanup &lt;days&gt; — удалить записи старше N дней\n"
        "/limits — текущие лимиты\n"
        "/ping — проверка живости"
    )

@dp.message(Command("ping"), IsAdmin())
async def admin_ping(message: Message):
    t0 = time.perf_counter()
    m = await message.answer("🏓 pong")
    dt = (time.perf_counter() - t0) * 1000
    await m.edit_text(f"🏓 pong — {dt:.0f} мс")

@dp.message(Command("stats"), IsAdmin())
async def admin_stats(message: Message):
    s = db.stats()
    cache_bytes, cache_files = cache_dir_size()
    queue_info = f"{queue_len}/{MAX_CONCURRENT}"
    await message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"👥 Юзеров: <b>{s['users']}</b> "
        f"(активных за 24ч: {s['active_24h']}, забанено: {s['banned']})\n"
        f"⬇️ Успешных скачиваний: <b>{s['downloads_ok']}</b> "
        f"(за 24ч: {s['downloads_24h']})\n"
        f"❌ Ошибок: <b>{s['downloads_err']}</b>\n"
        f"💾 Всего скачано: <b>{human_size(s['bytes_total'])}</b>\n\n"
        f"🗂 Кэш: {cache_files} файлов, {human_size(cache_bytes)}\n"
        f"⚙️ Очередь: {queue_info}"
    )

@dp.message(Command("users"), IsAdmin())
async def admin_users(message: Message):
    rows = db.list_users(limit=20)
    if not rows:
        await message.answer("Пользователей пока нет.")
        return
    lines = ["👥 <b>Последние 20 юзеров</b>\n"]
    for r in rows:
        flag = "🚫" if r["is_banned"] else "•"
        uname = f"@{r['username']}" if r["username"] else html.escape(r["full_name"] or "—")
        lines.append(
            f"{flag} <code>{r['user_id']}</code> {uname} — "
            f"{r['downloads']}✅ / {r['errors']}❌ / {human_size(r['bytes_total'])}"
        )
    await message.answer("\n".join(lines))

@dp.message(Command("user"), IsAdmin())
async def admin_user(message: Message, command: Command):
    args = (command.args or "").split()
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/user &lt;id&gt;</code>")
        return
    uid = int(args[0])
    r = db.get_user(uid)
    if not r:
        await message.answer("Юзер не найден.")
        return
    last = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["last_seen"]))
    first = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["first_seen"]))
    status = "🚫 забанен" if r["is_banned"] else "✅ активен"
    reason = f"\nПричина: {html.escape(r['ban_reason'] or '')}" if r["is_banned"] else ""
    await message.answer(
        f"👤 <b>Карточка юзера</b>\n\n"
        f"ID: <code>{r['user_id']}</code>\n"
        f"Имя: {html.escape(r['full_name'] or '—')}\n"
        f"Username: @{r['username'] if r['username'] else '—'}\n"
        f"Статус: {status}{reason}\n\n"
        f"Первый визит: {first}\n"
        f"Последний: {last}\n"
        f"Скачиваний: {r['downloads']} ✅ / {r['errors']} ❌\n"
        f"Всего байт: {human_size(r['bytes_total'])}"
    )

@dp.message(Command("top"), IsAdmin())
async def admin_top(message: Message):
    rows = db.top_users(10)
    if not rows:
        await message.answer("Пока пусто.")
        return
    lines = ["🏆 <b>Топ-10 по скачиваниям</b>\n"]
    for i, r in enumerate(rows, 1):
        uname = f"@{r['username']}" if r["username"] else html.escape(r["full_name"] or "—")
        lines.append(f"{i}. <code>{r['user_id']}</code> {uname} — {r['downloads']} "
                     f"({human_size(r['bytes_total'])})")
    await message.answer("\n".join(lines))

@dp.message(Command("ban"), IsAdmin())
async def admin_ban(message: Message, command: Command):
    args = (command.args or "").split(maxsplit=1)
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/ban &lt;id&gt; [причина]</code>")
        return
    uid = int(args[0])
    reason = args[1] if len(args) > 1 else ""
    if uid in ADMIN_IDS:
        await message.answer("Нельзя банить админа. 🤨")
        return
    if not db.get_user(uid):
        await message.answer("Юзер не найден в базе.")
        return
    db.set_ban(uid, True, reason)
    logger.info("BAN user=%s reason=%s by=%s", uid, reason, message.from_user.id)
    await message.answer(f"🚫 Юзер <code>{uid}</code> забанен.\nПричина: {html.escape(reason or '—')}")

@dp.message(Command("unban"), IsAdmin())
async def admin_unban(message: Message, command: Command):
    args = (command.args or "").split()
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/unban &lt;id&gt;</code>")
        return
    uid = int(args[0])
    if not db.get_user(uid):
        await message.answer("Юзер не найден.")
        return
    db.set_ban(uid, False, "")
    logger.info("UNBAN user=%s by=%s", uid, message.from_user.id)
    await message.answer(f"✅ Юзер <code>{uid}</code> разбанен.")

@dp.message(Command("bans"), IsAdmin())
async def admin_bans(message: Message):
    rows = db.banned_users()
    if not rows:
        await message.answer("Никто не забанен. 🕊")
        return
    lines = ["🚫 <b>Забаненные</b>\n"]
    for r in rows:
        uname = f"@{r['username']}" if r["username"] else html.escape(r["full_name"] or "—")
        lines.append(f"• <code>{r['user_id']}</code> {uname} — {html.escape(r['ban_reason'] or '—')}")
    await message.answer("\n".join(lines))

@dp.message(Command("broadcast"), IsAdmin())
async def admin_broadcast(message: Message, command: Command):
    text = (command.args or "").strip()
    if not text:
        await message.answer("Использование: <code>/broadcast текст</code>")
        return

    rows = db.list_users(limit=10_000)
    targets = [r["user_id"] for r in rows if not r["is_banned"]]
    await message.answer(f"📣 Рассылаю {len(targets)} юзерам...")

    ok = fail = 0
    for uid in targets:
        try:
            await bot.send_message(uid, text)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)  # ~20 msg/s — под лимиты Telegram

    await message.answer(f"✅ Доставлено: {ok}\n❌ Не доставлено: {fail}")

@dp.message(Command("clearcache"), IsAdmin())
async def admin_clearcache(message: Message):
    size, count = cache_dir_size()
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    logger.info("CACHE cleared by=%s (%s files, %s)",
                message.from_user.id, count, human_size(size))
    await message.answer(
        f"🧹 Кэш очищен.\nУдалено: {count} файлов, {human_size(size)}"
    )

@dp.message(Command("cleanup"), IsAdmin())
async def admin_cleanup(message: Message, command: Command):
    args = (command.args or "").split()
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/cleanup &lt;дней&gt;</code>")
        return
    days = int(args[0])
    n = db.cleanup_downloads_older_than(days)
    cleanup_stale_dirs()
    logger.info("CLEANUP days=%s rows=%s by=%s", days, n, message.from_user.id)
    await message.answer(
        f"🧹 Удалено записей старше {days} дн.: <b>{n}</b>\n"
        f"Временные папки почищены."
    )

@dp.message(Command("limits"), IsAdmin())
async def admin_limits(message: Message):
    await message.answer(
        "⚙️ <b>Текущие лимиты</b>\n\n"
        f"Размер файла: {MAX_FILE_SIZE_MB} МБ\n"
        f"Длительность: {human_time(MAX_DURATION_SEC)}\n"
        f"Таймаут скачивания: {human_time(DOWNLOAD_TIMEOUT)}\n"
        f"Параллельных загрузок: {MAX_CONCURRENT}\n"
        f"Кулдаун юзера: {USER_COOLDOWN_SEC} с\n"
        f"Cookies: {'✅' if COOKIES_FILE else '—'}\n"
        f"Proxy: {'✅' if PROXY else '—'}\n"
        f"Likee UA: {'✅' if LIKEE_USER_AGENT else '—'}"
    )

# ─────────────────────────── START / STOP ──────────────────
async def on_startup():
    cleanup_stale_dirs()
    logger.info("Запуск. Админов: %s. Лимиты: %s МБ / %s / concurrent=%s",
                len(ADMIN_IDS), MAX_FILE_SIZE_MB,
                human_time(MAX_DURATION_SEC), MAX_CONCURRENT)

async def on_shutdown():
    logger.info("Останавливаюсь...")
    db.close()
    await bot.session.close()

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\n👋 Бот остановлен.")