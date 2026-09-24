import asyncio
import os
import tempfile
from dotenv import load_dotenv

import yt_dlp
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import FSInputFile, Message
from aiogram.client.default import DefaultBotProperties

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ Укажи BOT_TOKEN в файле .env")

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
LIKEE_USER_AGENT = os.getenv("LIKEE_USER_AGENT", "").strip() or None

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- ИНИЦИАЛИЗАЦИЯ БОТА ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


# --- ХЕНДЛЕРЫ ---
@dp.message(CommandStart())
async def start_handler(message: Message):
    await message.answer(
        "Привет! 👋\n\n"
        "Отправь мне ссылку на видео с <b>YouTube</b>, <b>TikTok</b>, "
        "<b>Instagram</b> или <b>Likee</b> — я скачаю его и пришлю тебе.\n\n"
        f"⚠️ Максимальный размер файла: {MAX_FILE_SIZE_MB} МБ."
    )


@dp.message()
async def download_handler(message: Message):
    url = (message.text or "").strip()

    if not url.startswith("http"):
        await message.answer("Пожалуйста, отправь ссылку на видео (начинается с http).")
        return

    status_msg = await message.answer("⏳ Скачиваю видео, пожалуйста, подожди...")

    # Временная папка под каждый запрос
    temp_dir = tempfile.mkdtemp(dir=DOWNLOAD_DIR)
    output_template = os.path.join(temp_dir, "%(title).100s.%(ext)s")

    # Настройки yt-dlp
    ydl_opts = {
        "outtmpl": output_template,
        "format": "best[ext=mp4]/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
    }

    # Для Likee иногда нужен мобильный User-Agent
    if "likee" in url.lower() and LIKEE_USER_AGENT:
        ydl_opts["http_headers"] = {"User-Agent": LIKEE_USER_AGENT}

    try:
        # Скачивание в отдельном потоке, чтобы не блокировать Event Loop
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(
            None,
            lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(url, download=True),
        )

        # Ищем скачанный файл
        downloaded_files = [
            f for f in os.listdir(temp_dir)
            if os.path.isfile(os.path.join(temp_dir, f))
        ]
        if not downloaded_files:
            raise FileNotFoundError("Файл не найден после скачивания.")

        file_path = os.path.join(temp_dir, downloaded_files[0])
        file_size = os.path.getsize(file_path)

        if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
            await status_msg.edit_text(
                f"❌ Видео слишком большое "
                f"({file_size / 1024 / 1024:.1f} МБ > {MAX_FILE_SIZE_MB} МБ).\n"
                "Telegram не позволяет отправить такой файл."
            )
            return

        # Отправляем видео
        video = FSInputFile(file_path)
        title = (info.get("title") or "Видео")[:900]
        await message.answer_video(video, caption=f"🎬 {title}")
        await status_msg.delete()

    except yt_dlp.utils.DownloadError as e:
        await status_msg.edit_text(f"❌ Ошибка загрузки:\n<code>{str(e)[:300]}</code>")
    except Exception as e:
        await status_msg.edit_text(f"❌ Произошла ошибка:\n<code>{str(e)[:300]}</code>")
    finally:
        # Очистка временных файлов
        try:
            for f in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, f))
            os.rmdir(temp_dir)
        except Exception:
            pass


# --- ЗАПУСК ---
async def main():
    print("🚀 Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\n👋 Бот остановлен.")