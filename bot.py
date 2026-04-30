import os
import re
import asyncio
import logging
import secrets
import aiosqlite

from aiohttp import web
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── CONFIG ───────────────────────────────────────────────
BOT_TOKEN      = os.environ.get("BOT_TOKEN")
MY_USER_ID     = int(os.environ.get("MY_USER_ID", "0"))
API_ID         = int(os.environ.get("API_ID"))
API_HASH       = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
PORT           = int(os.environ.get("PORT", "8080"))
BASE_URL       = os.environ.get("BASE_URL", "").rstrip("/")
BATCH_WAIT     = 3
DB_PATH        = "/tmp/filestore.db"
# ────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
scheduler   = AsyncIOScheduler()
batch_store: dict = {}

telethon_client: TelegramClient = None


# ── Database ───────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS files (
                token     TEXT PRIMARY KEY,
                chat_id   INTEGER,
                msg_id    INTEGER,
                file_name TEXT,
                file_size INTEGER
            )
        """)
        await db.commit()
    logger.info("✅ Database ready")


async def save_token(token, chat_id, msg_id, file_name, file_size):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO files VALUES (?,?,?,?,?)",
            (token, chat_id, msg_id, file_name, file_size)
        )
        await db.commit()


async def get_token(token):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT chat_id, msg_id, file_name, file_size FROM files WHERE token=?",
            (token,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    "chat_id":   row[0],
                    "msg_id":    row[1],
                    "file_name": row[2],
                    "file_size": row[3],
                }
            return None


# ── Telethon ───────────────────────────────────────────
async def get_client() -> TelegramClient:
    global telethon_client
    if telethon_client is None or not telethon_client.is_connected():
        telethon_client = TelegramClient(
            StringSession(SESSION_STRING), API_ID, API_HASH
        )
        await telethon_client.connect()
        logger.info("✅ Telethon connected")
    return telethon_client


def generate_token() -> str:
    return secrets.token_urlsafe(16)


# ── Web Server ─────────────────────────────────────────
async def stream_handler(request: web.Request) -> web.Response:
    token = request.match_info.get("token")
    entry = await get_token(token)

    if not entry:
        return web.Response(status=404, text="Link not found.")

    chat_id   = entry["chat_id"]
    msg_id    = entry["msg_id"]
    file_name = entry["file_name"]
    file_size = entry["file_size"]

    range_header = request.headers.get("Range")
    offset   = 0
    end_byte = file_size - 1 if file_size else None

    if range_header and file_size:
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            offset   = int(match.group(1))
            end_byte = int(match.group(2)) if match.group(2) else file_size - 1

    is_download = "download" in request.query

    if is_download:
        disposition  = f'attachment; filename="{file_name}"'
        content_type = "application/octet-stream"
    else:
        disposition = f'inline; filename="{file_name}"'
        ext = file_name.lower().split(".")[-1]
        content_types = {
            "mp4":  "video/mp4",
            "mkv":  "video/x-matroska",
            "avi":  "video/x-msvideo",
            "mov":  "video/quicktime",
            "mp3":  "audio/mpeg",
            "m4a":  "audio/mp4",
            "pdf":  "application/pdf",
            "jpg":  "image/jpeg",
            "jpeg": "image/jpeg",
            "png":  "image/png",
        }
        content_type = content_types.get(ext, "video/mp4")

    headers = {
        "Content-Disposition": disposition,
        "Content-Type":        content_type,
        "Accept-Ranges":       "bytes",
    }

    if file_size:
        content_length = end_byte - offset + 1
        headers["Content-Length"] = str(content_length)

    status = 206 if range_header else 200
    if range_header and file_size:
        headers["Content-Range"] = f"bytes {offset}-{end_byte}/{file_size}"

    async def file_generator():
        try:
            client = await get_client()
            tl_msg = await client.get_messages(chat_id, ids=msg_id)
            if tl_msg is None:
                logger.error(f"Message not found chat={chat_id} msg={msg_id}")
                return
            async for chunk in client.iter_download(
                tl_msg,
                offset=offset,
                chunk_size=512 * 1024
            ):
                yield chunk
        except Exception as e:
            logger.error(f"Stream error: {e}")

    return web.Response(
        status=status,
        headers=headers,
        body=file_generator()
    )


async def index_handler(request: web.Request) -> web.Response:
    return web.Response(text="✅ Bot stream server is running.")


# ── Process Batch ──────────────────────────────────────
async def process_batch(
    user_id: int,
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE
):
    await asyncio.sleep(BATCH_WAIT)

    if user_id not in batch_store:
        return

    messages = batch_store[user_id]["messages"]
    del batch_store[user_id]

    if not messages:
        return

    total = len(messages)
    logger.info(f"Processing batch of {total} file(s)")

    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"⏳ Processing *{total}* file(s)...",
        parse_mode="Markdown"
    )

    download_lines = []
    stream_lines   = []
    total_size_mb  = 0
    failed         = []

    for i, message in enumerate(messages, 1):
        try:
            if message.video:
                file_name = message.video.file_name or f"video_{i}.mp4"
                file_size = message.video.file_size or 0
            elif message.document:
                file_name = message.document.file_name or f"file_{i}"
                file_size = message.document.file_size or 0
            elif message.audio:
                file_name = message.audio.file_name or f"audio_{i}.mp3"
                file_size = message.audio.file_size or 0
            else:
                continue

            file_size_mb   = file_size / (1024 * 1024)
            total_size_mb += file_size_mb

            # Save to database — survives restarts
            token = generate_token()
            await save_token(
                token    = token,
                chat_id  = chat_id,
                msg_id   = message.message_id,
                file_name = file_name,
                file_size = file_size
            )

            download_url = f"{BASE_URL}/stream/{token}?download=1"
            stream_url   = f"{BASE_URL}/stream/{token}"

            download_lines.append(
                f"⬇️ *Video {i}* : `{file_name}`\n"
                f"🔗 {download_url}"
            )
            stream_lines.append(
                f"▶️ *Video {i}* : `{file_name}` — "
                f"[Stream Now 🎬]({stream_url})"
            )

            logger.info(f"✅ {i}/{total}: {file_name} token={token}")

        except Exception as e:
            logger.error(f"❌ Failed file {i}: {e}", exc_info=True)
            failed.append(i)

    success_count = total - len(failed)

    text  = f"✅ *Ready Instantly!* ({success_count}/{total} files)\n"
    text += f"📦 Total size: {total_size_mb:.1f} MB\n"
    text += f"🔗 Links work permanently\n"

    text += "\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "⬇️ *DOWNLOAD LINKS*\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "\n\n".join(download_lines)

    text += "\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "▶️ *STREAM LINKS*\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    text += "\n".join(stream_lines)

    if failed:
        text += f"\n\n⚠️ Failed: Video(s) {', '.join(map(str, failed))}"

    if len(text) <= 4096:
        await status_msg.edit_text(text, parse_mode="Markdown")
    else:
        await status_msg.edit_text(
            f"✅ *Ready!* {success_count}/{total} files\n"
            f"📦 Total: {total_size_mb:.1f} MB\n"
            f"🔗 Links work permanently",
            parse_mode="Markdown"
        )

        dl_text  = "━━━━━━━━━━━━━━━━━━━━━━\n"
        dl_text += "⬇️ *DOWNLOAD LINKS*\n"
        dl_text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        dl_text += "\n\n".join(download_lines)

        for chunk in split_message(dl_text):
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode="Markdown"
            )

        st_text  = "━━━━━━━━━━━━━━━━━━━━━━\n"
        st_text += "▶️ *STREAM LINKS*\n"
        st_text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        st_text += "\n".join(stream_lines)

        for chunk in split_message(st_text):
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode="Markdown"
            )


def split_message(text: str, limit: int = 4096) -> list:
    lines   = text.split("\n")
    chunks  = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > limit:
            chunks.append(current)
            current = line + "\n"
        else:
            current += line + "\n"
    if current:
        chunks.append(current)
    return chunks


# ── Bot Handlers ───────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any file or *multiple files at once*:\n"
        "⚡ Instant stream and download links\n"
        "📦 Batch supported — 2 to 100 files\n"
        "🔗 Links work permanently\n"
        "🚀 Fast streaming — persistent connection",
        parse_mode="Markdown"
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return

    message = update.message
    if not (message.video or message.document or message.audio):
        return

    chat_id = update.effective_chat.id

    if user_id not in batch_store:
        batch_store[user_id] = {"messages": [], "task": None}

    batch_store[user_id]["messages"].append(message)

    if batch_store[user_id]["task"]:
        batch_store[user_id]["task"].cancel()

    task = asyncio.create_task(
        process_batch(user_id, chat_id, context)
    )
    batch_store[user_id]["task"] = task


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "Forward any file or multiple files to get instant links!"
    )


async def on_startup(app_obj):
    scheduler.start()
    await init_db()
    await get_client()
    logger.info(f"✅ Bot ready! BASE_URL={BASE_URL}")


def main():
    web_app = web.Application()
    web_app.router.add_get("/", index_handler)
    web_app.router.add_get("/stream/{token}", stream_handler)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    runner = web.AppRunner(web_app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    loop.run_until_complete(site.start())
    logger.info(f"🌐 Web server on port {PORT}")

    tg_app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL | filters.AUDIO,
        handle_video
    ))
    tg_app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_text
    ))

    logger.info("🤖 Bot is running!")
    tg_app.run_polling(drop_pending_updates=True, close_loop=False)


if __name__ == "__main__":
    main()
