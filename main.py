import asyncio
import html
import logging
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv

import yt_dlp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ChatAction
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.types import (
    FSInputFile, Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.client.default import DefaultBotProperties

# ─────────────────────────── ENV ───────────────────────────
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ Укажи BOT_TOKEN в файле .env")

ADMIN_IDS = {
    int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_IDS", "")) if x.strip().isdigit()
}

DOWNLOAD_DIR       = os.getenv("DOWNLOAD_DIR", "downloads")
CACHE_DIR          = os.getenv("CACHE_DIR", "cache")
DB_PATH            = os.getenv("DB_PATH", "bot.db")
MAX_FILE_SIZE_MB   = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_DURATION_SEC   = int(os.getenv("MAX_DURATION_SEC", "600"))
DOWNLOAD_TIMEOUT   = int(os.getenv("DOWNLOAD_TIMEOUT", "300"))
PROBE_TIMEOUT      = int(os.getenv("PROBE_TIMEOUT", "60"))
MAX_CONCURRENT     = int(os.getenv("MAX_CONCURRENT", "3"))
USER_COOLDOWN_SEC  = int(os.getenv("USER_COOLDOWN_SEC", "10"))
PENDING_TTL_SEC    = int(os.getenv("PENDING_TTL_SEC", "900"))
COOKIES_FILE       = os.getenv("COOKIES_FILE", "").strip() or None
PROXY              = os.getenv("PROXY", "").strip() or None
LIKEE_USER_AGENT   = os.getenv("LIKEE_USER_AGENT", "").strip() or None
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO").upper()

# Опционально: список разрешённых доменов (пусто = любые)
ALLOWED_DOMAINS = {
    d.strip().lower() for d in os.getenv("ALLOWED_DOMAINS", "").split(",") if d.strip()
}
# Опционально: список запрещённых доменов
BLOCKED_DOMAINS = {
    d.strip().lower() for d in os.getenv("BLOCKED_DOMAINS", "").split(",") if d.strip()
}

for d in (DOWNLOAD_DIR, CACHE_DIR, "logs"):
    os.makedirs(d, exist_ok=True)

# ─────────────────────────── LOGGING ───────────────────────
logger = logging.getLogger("video-bot")
logger.setLevel(LOG_LEVEL)
_fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                         datefmt="%Y-%m-%d %H:%M:%S")
_sh = logging.StreamHandler(); _sh.setFormatter(_fmt); logger.addHandler(_sh)
_fh = RotatingFileHandler("logs/bot.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt); logger.addHandler(_fh)

# ─────────────────────────── DB ────────────────────────────
class DB:
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self._conn.executescript("""
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
            kind        TEXT,
            quality     TEXT,
            size_bytes  INTEGER,
            duration    INTEGER,
            status      TEXT NOT NULL,
            error       TEXT,
            ts          INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dl_user ON downloads(user_id);
        CREATE INDEX IF NOT EXISTS idx_dl_ts   ON downloads(ts);
        """)
        self._conn.commit()

    def upsert_user(self, user_id, username, full_name):
        now = int(time.time())
        with self._conn:
            self._conn.execute("""
                INSERT INTO users (user_id, username, full_name, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username, full_name=excluded.full_name,
                    last_seen=excluded.last_seen
            """, (user_id, username, full_name, now, now))

    def get_user(self, uid):
        return self._conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

    def is_banned(self, uid) -> bool:
        r = self._conn.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
        return bool(r and r["is_banned"])

    def set_ban(self, uid, banned, reason=""):
        with self._conn:
            self._conn.execute("UPDATE users SET is_banned=?, ban_reason=? WHERE user_id=?",
                               (1 if banned else 0, reason, uid))

    def list_users(self, limit=20, offset=0):
        return self._conn.execute(
            "SELECT * FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?",
            (limit, offset)).fetchall()

    def banned_users(self):
        return self._conn.execute(
            "SELECT * FROM users WHERE is_banned=1 ORDER BY last_seen DESC").fetchall()

    def top_users(self, limit=10):
        return self._conn.execute("""
            SELECT user_id, username, full_name, downloads, bytes_total
            FROM users ORDER BY downloads DESC LIMIT ?
        """, (limit,)).fetchall()

    def log_download(self, uid, url, vid, title, kind, quality, size, duration, status, error=""):
        with self._conn:
            self._conn.execute("""
                INSERT INTO downloads (user_id, url, video_id, title, kind, quality,
                                       size_bytes, duration, status, error, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (uid, url, vid, title, kind, quality, size, duration, status, error, int(time.time())))
            if status == "ok":
                self._conn.execute(
                    "UPDATE users SET downloads=downloads+1, bytes_total=bytes_total+? WHERE user_id=?",
                    (size or 0, uid))
            else:
                self._conn.execute("UPDATE users SET errors=errors+1 WHERE user_id=?", (uid,))

    def stats(self) -> dict:
        c = self._conn
        total_users = c.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        banned      = c.execute("SELECT COUNT(*) c FROM users WHERE is_banned=1").fetchone()["c"]
        active_24h  = c.execute("SELECT COUNT(*) c FROM users WHERE last_seen > ?",
                                (int(time.time()) - 86400,)).fetchone()["c"]
        ok = c.execute("SELECT COUNT(*) c, COALESCE(SUM(size_bytes),0) s FROM downloads WHERE status='ok'").fetchone()
        err = c.execute("SELECT COUNT(*) c FROM downloads WHERE status='error'").fetchone()["c"]
        today = c.execute("SELECT COUNT(*) c FROM downloads WHERE status='ok' AND ts > ?",
                          (int(time.time()) - 86400,)).fetchone()["c"]
        return {"users": total_users, "banned": banned, "active_24h": active_24h,
                "downloads_ok": ok["c"], "downloads_err": err,
                "bytes_total": ok["s"] or 0, "downloads_24h": today}

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
_DIRECT_MEDIA_RE = re.compile(r"\.(mp4|webm|mov|mkv|m4v|mp3|m4a|ogg|opus|wav)(\?.*)?$", re.IGNORECASE)

def extract_url(text: str) -> Optional[str]:
    m = _URL_RE.search(text or "")
    if not m:
        return None
    return m.group(0).rstrip(".,;:!?)")

def human_size(n) -> str:
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def human_time(sec) -> str:
    sec = int(sec or 0)
    if sec < 60: return f"{sec} с"
    m, s = divmod(sec, 60)
    if m < 60: return f"{m} м {s} с"
    h, m = divmod(m, 60)
    return f"{h} ч {m} м"

def is_likee(url: str) -> bool:
    return "likee" in url.lower()

def is_direct_media(url: str) -> bool:
    return bool(_DIRECT_MEDIA_RE.search(url))

def domain_of(url: str) -> str:
    try:
        host = urlparse(url).hostname or ""
        # отрезаем www.
        return host.lower().lstrip(".").removeprefix("www.")
    except Exception:
        return ""

def is_domain_allowed(url: str) -> tuple[bool, str]:
    d = domain_of(url)
    if not d:
        return True, ""
    if BLOCKED_DOMAINS and any(d == b or d.endswith("." + b) for b in BLOCKED_DOMAINS):
        return False, f"Домен {d} в чёрном списке."
    if ALLOWED_DOMAINS and not any(d == a or d.endswith("." + a) for a in ALLOWED_DOMAINS):
        return False, f"Домен {d} не в белом списке."
    return True, ""

def cleanup_stale_dirs():
    now = time.time()
    for base in (DOWNLOAD_DIR,):
        for name in os.listdir(base):
            path = os.path.join(base, name)
            try:
                if os.path.isdir(path) and now - os.path.getmtime(path) > 3600:
                    shutil.rmtree(path, ignore_errors=True)
            except OSError:
                pass

def cache_dir_size() -> tuple[int, int]:
    total, count = 0, 0
    for root, _, files in os.walk(CACHE_DIR):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f)); count += 1
            except OSError: pass
    return total, count

def detect_source(url: str) -> str:
    d = domain_of(url)
    mapping = {
        "youtube.com": "YouTube", "youtu.be": "YouTube",
        "tiktok.com": "TikTok", "instagram.com": "Instagram",
        "likee.video": "Likee", "likee.com": "Likee",
        "twitter.com": "Twitter/X", "x.com": "Twitter/X",
        "facebook.com": "Facebook", "fb.watch": "Facebook",
        "vk.com": "VK", "vimeo.com": "Vimeo",
        "dailymotion.com": "Dailymotion", "twitch.tv": "Twitch",
        "rutube.ru": "Rutube", "ok.ru": "OK.ru",
        "bilibili.com": "Bilibili", "reddit.com": "Reddit",
        "soundcloud.com": "SoundCloud", "bandcamp.com": "Bandcamp",
    }
    for key, name in mapping.items():
        if d == key or d.endswith("." + key):
            return name
    return d or "сайт"

# ─────────────────────────── PENDING TASKS ─────────────────
@dataclass
class PendingTask:
    token: str
    url: str
    user_id: int
    chat_id: int
    message_id: int
    title: str
    duration: Optional[int]
    video_id: Optional[str]
    heights: list[int]
    source: str
    is_direct: bool = False
    ts: float = field(default_factory=time.time)

pending: dict[str, PendingTask] = {}

def new_token() -> str:
    return secrets.token_urlsafe(6)

def prune_pending():
    now = time.time()
    dead = [t for t, p in pending.items() if now - p.ts > PENDING_TTL_SEC]
    for t in dead:
        pending.pop(t, None)

def put_pending(task: PendingTask) -> str:
    prune_pending()
    pending[task.token] = task
    return task.token

# ─────────────────────────── YT-DLP ────────────────────────
def _base_opts(url: str, temp_dir: str) -> dict:
    o = {
        "outtmpl": os.path.join(temp_dir, "%(title).100s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
        "retries": 3,
        "fragment_retries": 3,
        "socket_timeout": 30,
        "nocheckcertificate": True,
        # ── Универсальность: пробуем generic-экстрактор для незнакомых сайтов
        "default_search": "auto",
        "extractor_args": {
            "generic": {
                "impersonate": ["chrome"],
            },
        },
        # Не падать, если не нашли идеальный формат
        "ignoreerrors": False,
        "no_color": True,
    }
    if COOKIES_FILE and os.path.isfile(COOKIES_FILE):
        o["cookiefile"] = COOKIES_FILE
    if PROXY:
        o["proxy"] = PROXY
    if is_likee(url) and LIKEE_USER_AGENT:
        o["http_headers"] = {"User-Agent": LIKEE_USER_AGENT}
    # Иначе — представимся обычным браузером (помогает generic-экстрактору)
    if "http_headers" not in o:
        o["http_headers"] = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0 Safari/537.36"),
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        }
    return o

def probe_url(url: str) -> dict:
    opts = _base_opts(url, DOWNLOAD_DIR)
    opts["skip_download"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)

def extract_heights(info: dict) -> list[int]:
    heights = set()
    for f in (info.get("formats") or []):
        h = f.get("height")
        vcodec = (f.get("vcodec") or "none").lower()
        if h and vcodec != "none":
            heights.add(int(h))
    allowed = [144, 240, 360, 480, 720, 1080, 1440, 2160, 4320]
    return sorted(h for h in heights if h in allowed)

class ProgressHook:
    def __init__(self, chat_id: int, message_id: int):
        self.chat_id = chat_id
        self.message_id = message_id
        self.loop = asyncio.get_running_loop()
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
            filled = int(16 * pct / 100)
            bar = "█" * filled + "░" * (16 - filled)
            text = (f"⬇️ <b>{pct:.1f}%</b>  [{bar}]\n"
                    f"📦 {human_size(downloaded)} / {human_size(total)}  "
                    f"⚡ {human_size(speed)}/с  ⏱ ETA {human_time(eta)}")
        else:
            text = f"⬇️ Скачано {human_size(downloaded)}  ⚡ {human_size(speed)}/с"
        asyncio.run_coroutine_threadsafe(self._edit(text), self.loop)

    async def _edit(self, text: str):
        try:
            await bot.edit_message_text(text, chat_id=self.chat_id, message_id=self.message_id)
        except Exception:
            pass

def _sync_extract(url: str, opts: dict) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=True)

@dataclass
class DownloadResult:
    file_path: str
    title: str
    duration: Optional[int]
    video_id: Optional[str]
    ext: str
    meta: dict

async def run_download(url: str, mode: str, height: Optional[int],
                       chat_id: int, message_id: int) -> DownloadResult:
    """mode: 'video' | 'audio'. height — только для video."""
    temp_dir = tempfile.mkdtemp(dir=DOWNLOAD_DIR)
    opts = _base_opts(url, temp_dir)
    opts["progress_hooks"] = [ProgressHook(chat_id, message_id)]

    if mode == "audio":
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]
    else:
        if height:
            opts["format"] = (
                f"bestvideo[ext=mp4][height<={height}]+bestaudio[ext=m4a]/"
                f"best[ext=mp4][height<={height}]/"
                f"bestvideo[height<={height}]+bestaudio/"
                f"best[height<={height}]/best"
            )
        else:
            opts["format"] = (
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]/"
                "bestvideo+bestaudio/best[ext=mp4]/best"
            )
        opts["merge_output_format"] = "mp4"

    try:
        info = await asyncio.wait_for(
            asyncio.to_thread(_sync_extract, url, opts),
            timeout=DOWNLOAD_TIMEOUT,
        )
    except asyncio.TimeoutError:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(f"Таймаут скачивания ({DOWNLOAD_TIMEOUT} с).")
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    # Плейлисты — берём первую запись
    if isinstance(info, dict) and info.get("_type") in ("playlist", "multi_video"):
        entries = info.get("entries") or []
        if entries:
            info = entries[0]

    # Ищем итоговый файл
    file_path = None
    requested = (info or {}).get("requested_downloads") or []
    if requested:
        file_path = requested[0].get("filepath")

    if not file_path or not os.path.isfile(file_path):
        exts = (".mp3",) if mode == "audio" else (".mp4", ".mkv", ".webm", ".mov", ".m4v")
        candidates = [
            os.path.join(temp_dir, f) for f in os.listdir(temp_dir)
            if os.path.isfile(os.path.join(temp_dir, f)) and f.lower().endswith(exts)
        ]
        if not candidates:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise FileNotFoundError("Файл не найден после скачивания.")
        file_path = max(candidates, key=os.path.getsize)

    return DownloadResult(
        file_path=file_path,
        title=(info.get("title") or "Видео") if isinstance(info, dict) else "Видео",
        duration=info.get("duration") if isinstance(info, dict) else None,
        video_id=info.get("id") if isinstance(info, dict) else None,
        ext=os.path.splitext(file_path)[1].lower(),
        meta=info if isinstance(info, dict) else {},
    )

async def run_direct_download(url: str, chat_id: int, message_id: int) -> DownloadResult:
    """Скачивает прямую ссылку на медиафайл (mp4/webm/mp3/...) через yt-dlp generic."""
    return await run_download(url, mode="video", height=None,
                              chat_id=chat_id, message_id=message_id)

# ─────────────────────────── KEYBOARDS ─────────────────────
def preview_kb(token: str, heights: list[int], is_direct: bool) -> InlineKeyboardMarkup:
    rows = []
    if is_direct:
        rows.append([InlineKeyboardButton(text="⬇️ Скачать файл", callback_data=f"v:{token}")])
    else:
        rows.append([
            InlineKeyboardButton(text="🎬 Видео", callback_data=f"v:{token}"),
            InlineKeyboardButton(text="🎵 Аудио (mp3)", callback_data=f"a:{token}"),
        ])
        if heights:
            rows.append([InlineKeyboardButton(text="📊 Другое качество", callback_data=f"q:{token}")])
    rows.append([InlineKeyboardButton(text="✖️ Отмена", callback_data=f"x:{token}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def quality_kb(token: str, heights: list[int]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for h in heights:
        row.append(InlineKeyboardButton(text=f"{h}p", callback_data=f"h:{token}:{h}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"b:{token}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def preview_text(task: PendingTask) -> str:
    title = html.escape(task.title[:200])
    parts = [f"🎬 <b>{title}</b>"]
    meta = []
    if task.duration:
        meta.append(f"⏱ {human_time(task.duration)}")
    if task.source:
        meta.append(f"🌐 {html.escape(task.source)}")
    if meta:
        parts.append(" · ".join(meta))
    parts.append("")
    if task.is_direct:
        parts.append("Прямая ссылка на медиафайл. Нажми кнопку ниже:")
    else:
        parts.append("Выбери, что скачать:")
    return "\n".join(parts)

# ─────────────────────────── USER: START/HELP ──────────────
@dp.message(CommandStart())
async def start_handler(message: Message):
    u = message.from_user
    if u:
        db.upsert_user(u.id, u.username, u.full_name)
        if db.is_banned(u.id):
            await message.answer("🚫 Ты заблокирован в этом боте.")
            return
    await message.answer(
        "Привет! 👋\n\n"
        "Кинь мне <b>ссылку на видео</b> — с любого сайта, который "
        "поддерживает yt-dlp (YouTube, TikTok, Instagram, Twitter, VK, "
        "Rutube, Twitch, Reddit и ещё ~1800 сайтов).\n"
        "А также <b>прямые ссылки</b> на .mp4 / .webm / .mp3 и т.п.\n\n"
        "Я покажу превью и дам выбрать: <b>видео</b>, <b>аудио</b> или "
        "<b>конкретное качество</b>.\n\n"
        f"⚠️ Максимум: {MAX_FILE_SIZE_MB} МБ, "
        f"длительность ≤ {human_time(MAX_DURATION_SEC)}."
    )

@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(
        "📖 <b>Как пользоваться</b>\n\n"
        "1. Отправь ссылку на видео (или прямую ссылку на файл).\n"
        "2. Получи превью с кнопками.\n"
        "3. Выбери <b>🎬 Видео</b>, <b>🎵 Аудио</b> или <b>📊 Качество</b>.\n"
        "4. Получи файл.\n\n"
        f"⚙️ Лимиты: {MAX_FILE_SIZE_MB} МБ · до {human_time(MAX_DURATION_SEC)} · "
        f"кулдаун {USER_COOLDOWN_SEC} с."
    )

# ─────────────────────────── USER: PROBE ───────────────────
@dp.message(F.text)
async def probe_handler(message: Message):
    u = message.from_user
    if not u:
        return
    db.upsert_user(u.id, u.username, u.full_name)

    if db.is_banned(u.id):
        await message.answer("🚫 Ты заблокирован в этом боте.")
        return

    url = extract_url(message.text or "")
    if not url:
        await message.answer("Пожалуйста, отправь ссылку (http...).")
        return

    ok, why = is_domain_allowed(url)
    if not ok:
        await message.answer(f"🚫 {html.escape(why)}")
        return

    now = time.time()
    last = user_last_request.get(u.id, 0)
    if now - last < USER_COOLDOWN_SEC:
        wait = USER_COOLDOWN_SEC - (now - last)
        await message.answer(f"⏳ Подожди ещё {wait:.1f} с перед следующим запросом.")
        return
    user_last_request[u.id] = now

    status = await message.answer("🔍 Получаю информацию...")
    await message.bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    direct = is_direct_media(url)

    try:
        info = await asyncio.wait_for(
            asyncio.to_thread(probe_url, url),
            timeout=PROBE_TIMEOUT,
        )
    except yt_dlp.utils.DownloadError as e:
        err = html.escape(str(e)[:300])
        await _safe_edit(status, f"❌ Не удалось получить информацию:\n<code>{err}</code>")
        return
    except asyncio.TimeoutError:
        await _safe_edit(status, "❌ Источник не ответил вовремя.")
        return
    except Exception as e:
        logger.exception("Probe failed: %s", e)
        await _safe_edit(status, f"❌ Ошибка: <code>{html.escape(str(e)[:300])}</code>")
        return

    if not isinstance(info, dict):
        await _safe_edit(status, "❌ Не удалось разобрать ответ источника.")
        return

    # Плейлисты / multi — берём первый
    if info.get("_type") in ("playlist", "multi_video"):
        entries = info.get("entries") or []
        if entries:
            info = entries[0]

    title = info.get("title") or ("Видео" if not direct else "Медиафайл")
    duration = info.get("duration")
    video_id = info.get("id")
    heights = extract_heights(info)

    if duration and duration > MAX_DURATION_SEC:
        await _safe_edit(status,
            f"❌ Слишком длинное ({human_time(duration)} > "
            f"{human_time(MAX_DURATION_SEC)}).")
        return

    # Прямые ссылки — упрощённое превью
    if direct and not info.get("formats"):
        # почти всегда уже понятно, что это mp4/mp3
        ext = os.path.splitext(domain_of(url) or url)[1]
        title = title or "Медиафайл"

    token = new_token()
    task = PendingTask(
        token=token, url=url, user_id=u.id, chat_id=message.chat.id,
        message_id=status.message_id, title=title, duration=duration,
        video_id=video_id, heights=heights, source=detect_source(url),
        is_direct=direct and not heights,
    )
    put_pending(task)

    thumb_url = info.get("thumbnail")
    kb = preview_kb(token, heights, task.is_direct)

    try:
        if thumb_url:
            await message.answer_photo(
                photo=thumb_url, caption=preview_text(task), reply_markup=kb,
            )
            try:
                await status.delete()
            except Exception:
                pass
        else:
            await status.edit_text(preview_text(task), reply_markup=kb)
    except Exception:
        await _safe_edit(status, preview_text(task))
        try:
            await status.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

async def _safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except Exception:
        pass

async def _safe_edit_markup(chat_id: int, message_id: int, text: str, kb=None):
    try:
        await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id,
                                    reply_markup=kb)
        return True
    except Exception:
        return False

# ─────────────────────────── USER: CALLBACKS ───────────────
@dp.callback_query(F.data.startswith("x:"))
async def cb_cancel(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    task = pending.pop(token, None)
    if not task or task.user_id != cb.from_user.id:
        await cb.answer("Задача устарела.", show_alert=False)
        return
    await cb.answer("Отменено")
    try:
        await bot.edit_message_text("✖️ Отменено.",
                                    chat_id=cb.message.chat.id,
                                    message_id=cb.message.message_id)
    except Exception:
        pass

@dp.callback_query(F.data.startswith("b:"))
async def cb_back(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    task = pending.get(token)
    if not task or task.user_id != cb.from_user.id:
        await cb.answer("Задача устарела.")
        return
    await cb.answer()
    await _safe_edit_markup(task.chat_id, task.message_id,
                            preview_text(task),
                            preview_kb(task.token, task.heights, task.is_direct))

@dp.callback_query(F.data.startswith("q:"))
async def cb_quality_menu(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    task = pending.get(token)
    if not task or task.user_id != cb.from_user.id:
        await cb.answer("Задача устарела.")
        return
    if not task.heights:
        await cb.answer("Источник не даёт выбор качества.", show_alert=True)
        return
    await cb.answer()
    kb = quality_kb(token, task.heights)
    heights_line = ", ".join(f"{h}p" for h in task.heights)
    text = f"📊 <b>Выбери качество</b>\n\nДоступно: {heights_line}"
    await _safe_edit_markup(task.chat_id, task.message_id, text, kb)

@dp.callback_query(F.data.startswith("h:"))
async def cb_quality_pick(cb: CallbackQuery):
    parts = cb.data.split(":")
    if len(parts) != 3:
        await cb.answer(); return
    _, token, h_raw = parts
    if not h_raw.isdigit():
        await cb.answer(); return
    height = int(h_raw)
    task = pending.get(token)
    if not task or task.user_id != cb.from_user.id:
        await cb.answer("Задача устарела.")
        return
    await cb.answer()
    await _do_download(cb, task, mode="video", height=height, quality_label=f"{height}p")

@dp.callback_query(F.data.startswith("v:"))
async def cb_video(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    task = pending.get(token)
    if not task or task.user_id != cb.from_user.id:
        await cb.answer("Задача устарела.")
        return
    await cb.answer()
    await _do_download(cb, task, mode="video", height=None, quality_label="best")

@dp.callback_query(F.data.startswith("a:"))
async def cb_audio(cb: CallbackQuery):
    token = cb.data.split(":", 1)[1]
    task = pending.get(token)
    if not task or task.user_id != cb.from_user.id:
        await cb.answer("Задача устарела.")
        return
    await cb.answer()
    await _do_download(cb, task, mode="audio", height=None, quality_label="mp3")

async def _do_download(cb: CallbackQuery, task: PendingTask,
                       mode: str, height: Optional[int], quality_label: str):
    pending.pop(task.token, None)

    chat_id = task.chat_id
    message_id = task.message_id
    user_id = task.user_id

    global queue_len
    async with queue_lock:
        queue_len += 1
        qpos = queue_len

    if qpos > 1:
        await _safe_edit_markup(chat_id, message_id,
                                f"🕐 В очереди: <b>{qpos}</b>...")
    else:
        await _safe_edit_markup(chat_id, message_id, "⏳ Скачиваю...")

    temp_dir_cleanup = None
    try:
        async with download_semaphore:
            result = await run_download(task.url, mode, height, chat_id, message_id)
            temp_dir_cleanup = os.path.dirname(result.file_path)

            size = os.path.getsize(result.file_path)
            if size > MAX_FILE_SIZE_MB * 1024 * 1024:
                db.log_download(user_id, task.url, result.video_id, result.title,
                                mode, quality_label, size, result.duration,
                                "error", "too_big")
                await _safe_edit_markup(chat_id, message_id,
                    f"❌ Файл слишком большой ({human_size(size)} > "
                    f"{MAX_FILE_SIZE_MB} МБ). Telegram не отправит такой файл.")
                return

            title = html.escape((result.title or "Видео")[:900])
            caption_parts = [f"🎬 <b>{title}</b>" if mode == "video" else f"🎵 <b>{title}</b>"]
            meta = []
            if result.duration: meta.append(f"⏱ {human_time(result.duration)}")
            if mode == "audio": meta.append("🎧 mp3 192 kbps")
            elif height: meta.append(f"📺 {height}p")
            else: meta.append("📺 лучшее")
            meta.append(human_size(size))
            caption_parts.append(" · ".join(meta))
            caption = "\n".join(caption_parts)

            await _safe_edit_markup(chat_id, message_id, "📤 Отправляю...")

            file = FSInputFile(result.file_path)
            if mode == "audio":
                await bot.send_audio(chat_id, file, caption=caption,
                                     title=result.title[:64], duration=result.duration)
            elif result.ext in {".mp4", ".mov", ".m4v"}:
                await bot.send_video(chat_id, file, caption=caption,
                                     duration=result.duration, supports_streaming=True)
            else:
                await bot.send_document(chat_id, file, caption=caption)

            db.log_download(user_id, task.url, result.video_id, result.title,
                            mode, quality_label, size, result.duration, "ok")
            try:
                await bot.delete_message(chat_id, message_id)
            except Exception:
                pass

            logger.info("OK user=%s id=%s mode=%s q=%s size=%s src=%s",
                        user_id, result.video_id, mode, quality_label,
                        human_size(size), task.source)

    except yt_dlp.utils.DownloadError as e:
        err = str(e)[:300]
        db.log_download(user_id, task.url, task.video_id, task.title, mode,
                        quality_label, None, task.duration, "error", err)
        logger.warning("DownloadError user=%s: %s", user_id, err)
        await _safe_edit_markup(chat_id, message_id,
                                f"❌ Ошибка загрузки:\n<code>{html.escape(err)}</code>")
    except asyncio.TimeoutError:
        db.log_download(user_id, task.url, task.video_id, task.title, mode,
                        quality_label, None, task.duration, "error", "timeout")
        await _safe_edit_markup(chat_id, message_id, "❌ Превышено время ожидания.")
    except Exception as e:
        err = str(e)[:300]
        db.log_download(user_id, task.url, task.video_id, task.title, mode,
                        quality_label, None, task.duration, "error", err)
        logger.exception("Ошибка скачивания: %s", e)
        await _safe_edit_markup(chat_id, message_id,
                                f"❌ Ошибка:\n<code>{html.escape(err)}</code>")
    finally:
        async with queue_lock:
            queue_len = max(0, queue_len - 1)
        if temp_dir_cleanup and os.path.isdir(temp_dir_cleanup):
            if not os.path.abspath(temp_dir_cleanup).startswith(os.path.abspath(CACHE_DIR)):
                shutil.rmtree(temp_dir_cleanup, ignore_errors=True)

# ─────────────────────────── ADMIN ─────────────────────────
@dp.message(Command("admin"), IsAdmin())
async def admin_root(message: Message):
    await message.answer(
        "🛠 <b>Админ-панель</b>\n\n"
        "<b>Статистика</b>\n"
        "/stats · /users · /user &lt;id&gt; · /top\n\n"
        "<b>Модерация</b>\n"
        "/ban &lt;id&gt; [причина] · /unban &lt;id&gt; · /bans\n\n"
        "<b>Обслуживание</b>\n"
        "/broadcast &lt;текст&gt; · /clearcache · /cleanup &lt;days&gt;\n"
        "/limits · /pending · /extractors · /ping"
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
    await message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"👥 Юзеров: <b>{s['users']}</b> "
        f"(24ч: {s['active_24h']}, бан: {s['banned']})\n"
        f"⬇️ OK: <b>{s['downloads_ok']}</b> (24ч: {s['downloads_24h']})\n"
        f"❌ Ошибок: <b>{s['downloads_err']}</b>\n"
        f"💾 Всего: <b>{human_size(s['bytes_total'])}</b>\n\n"
        f"🗂 Кэш: {cache_files} файлов, {human_size(cache_bytes)}\n"
        f"⚙️ Очередь: {queue_len}/{MAX_CONCURRENT} · "
        f"Задач в превью: {len(pending)}"
    )

@dp.message(Command("users"), IsAdmin())
async def admin_users(message: Message):
    rows = db.list_users(20)
    if not rows:
        await message.answer("Пусто."); return
    lines = ["👥 <b>Последние 20</b>\n"]
    for r in rows:
        flag = "🚫" if r["is_banned"] else "•"
        uname = f"@{r['username']}" if r["username"] else html.escape(r["full_name"] or "—")
        lines.append(f"{flag} <code>{r['user_id']}</code> {uname} — "
                     f"{r['downloads']}✅ / {r['errors']}❌ / {human_size(r['bytes_total'])}")
    await message.answer("\n".join(lines))

@dp.message(Command("user"), IsAdmin())
async def admin_user(message: Message, command: Command):
    args = (command.args or "").split()
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/user &lt;id&gt;</code>"); return
    r = db.get_user(int(args[0]))
    if not r:
        await message.answer("Не найден."); return
    st = "🚫 забанен" if r["is_banned"] else "✅ активен"
    reason = f"\nПричина: {html.escape(r['ban_reason'] or '')}" if r["is_banned"] else ""
    await message.answer(
        f"👤 <b>Юзер</b>\n\n"
        f"ID: <code>{r['user_id']}</code>\n"
        f"Имя: {html.escape(r['full_name'] or '—')}\n"
        f"Username: @{r['username'] or '—'}\n"
        f"Статус: {st}{reason}\n"
        f"Скачиваний: {r['downloads']} ✅ / {r['errors']} ❌\n"
        f"Байт: {human_size(r['bytes_total'])}"
    )

@dp.message(Command("top"), IsAdmin())
async def admin_top(message: Message):
    rows = db.top_users(10)
    if not rows:
        await message.answer("Пусто."); return
    lines = ["🏆 <b>Топ-10</b>\n"]
    for i, r in enumerate(rows, 1):
        uname = f"@{r['username']}" if r["username"] else html.escape(r["full_name"] or "—")
        lines.append(f"{i}. <code>{r['user_id']}</code> {uname} — {r['downloads']} "
                     f"({human_size(r['bytes_total'])})")
    await message.answer("\n".join(lines))

@dp.message(Command("ban"), IsAdmin())
async def admin_ban(message: Message, command: Command):
    args = (command.args or "").split(maxsplit=1)
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/ban &lt;id&gt; [причина]</code>"); return
    uid = int(args[0])
    reason = args[1] if len(args) > 1 else ""
    if uid in ADMIN_IDS:
        await message.answer("Нельзя банить админа. 🤨"); return
    if not db.get_user(uid):
        await message.answer("Юзер не найден."); return
    db.set_ban(uid, True, reason)
    logger.info("BAN user=%s reason=%s by=%s", uid, reason, message.from_user.id)
    await message.answer(f"🚫 <code>{uid}</code> забанен.\nПричина: {html.escape(reason or '—')}")

@dp.message(Command("unban"), IsAdmin())
async def admin_unban(message: Message, command: Command):
    args = (command.args or "").split()
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/unban &lt;id&gt;</code>"); return
    uid = int(args[0])
    if not db.get_user(uid):
        await message.answer("Юзер не найден."); return
    db.set_ban(uid, False, "")
    logger.info("UNBAN user=%s by=%s", uid, message.from_user.id)
    await message.answer(f"✅ <code>{uid}</code> разбанен.")

@dp.message(Command("bans"), IsAdmin())
async def admin_bans(message: Message):
    rows = db.banned_users()
    if not rows:
        await message.answer("Никто не забанен. 🕊"); return
    lines = ["🚫 <b>Забаненные</b>\n"]
    for r in rows:
        uname = f"@{r['username']}" if r["username"] else html.escape(r["full_name"] or "—")
        lines.append(f"• <code>{r['user_id']}</code> {uname} — {html.escape(r['ban_reason'] or '—')}")
    await message.answer("\n".join(lines))

@dp.message(Command("broadcast"), IsAdmin())
async def admin_broadcast(message: Message, command: Command):
    text = (command.args or "").strip()
    if not text:
        await message.answer("Использование: <code>/broadcast текст</code>"); return
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
        await asyncio.sleep(0.05)
    await message.answer(f"✅ Доставлено: {ok}\n❌ Не доставлено: {fail}")

@dp.message(Command("clearcache"), IsAdmin())
async def admin_clearcache(message: Message):
    size, count = cache_dir_size()
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    os.makedirs(CACHE_DIR, exist_ok=True)
    logger.info("CACHE cleared by=%s (%s files, %s)",
                message.from_user.id, count, human_size(size))
    await message.answer(f"🧹 Кэш очищен.\nУдалено: {count} файлов, {human_size(size)}")

@dp.message(Command("cleanup"), IsAdmin())
async def admin_cleanup(message: Message, command: Command):
    args = (command.args or "").split()
    if not args or not args[0].isdigit():
        await message.answer("Использование: <code>/cleanup &lt;дней&gt;</code>"); return
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
        f"Таймаут probe: {PROBE_TIMEOUT} с\n"
        f"Параллельных загрузок: {MAX_CONCURRENT}\n"
        f"Кулдаун юзера: {USER_COOLDOWN_SEC} с\n"
        f"Cookies: {'✅' if COOKIES_FILE else '—'}\n"
        f"Proxy: {'✅' if PROXY else '—'}\n"
        f"Likee UA: {'✅' if LIKEE_USER_AGENT else '—'}\n"
        f"Белый список: {', '.join(sorted(ALLOWED_DOMAINS)) or '—'}\n"
        f"Чёрный список: {', '.join(sorted(BLOCKED_DOMAINS)) or '—'}"
    )

@dp.message(Command("pending"), IsAdmin())
async def admin_pending(message: Message):
    prune_pending()
    if not pending:
        await message.answer("Активных превью нет."); return
    lines = [f"⏳ <b>Активных превью: {len(pending)}</b>\n"]
    now = time.time()
    for t, p in list(pending.items())[:30]:
        age = int(now - p.ts)
        lines.append(f"• <code>{t}</code> user=<code>{p.user_id}</code> "
                     f"{html.escape(p.title[:40])} ({age}с) — {html.escape(p.source)}")
    await message.answer("\n".join(lines))

@dp.message(Command("extractors"), IsAdmin())
async def admin_extractors(message: Message):
    try:
        exts = yt_dlp.extractor.gen_extractor_classes()
        names = sorted({e.IE_NAME for e in exts})
        sample = ", ".join(names[:60])
        await message.answer(
            f"🧩 Загружено экстракторов: <b>{len(names)}</b>\n\n"
            f"<i>Примеры:</i> {html.escape(sample)}…"
        )
    except Exception as e:
        await message.answer(f"❌ {html.escape(str(e))}")

# ─────────────────────────── START / STOP ──────────────────
async def on_startup():
    cleanup_stale_dirs()
    logger.info("Запуск. Админов: %s. Лимиты: %s МБ / %s / concurrent=%s",
                len(ADMIN_IDS), MAX_FILE_SIZE_MB,
                human_time(MAX_DURATION_SEC), MAX_CONCURRENT)
    try:
        cnt = len(yt_dlp.extractor.gen_extractor_classes())
        logger.info("yt-dlp: загружено экстракторов %s", cnt)
    except Exception:
        pass

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
