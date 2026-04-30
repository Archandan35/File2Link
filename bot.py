import os
import re
import time
import asyncio
import logging
import secrets
from datetime import datetime, timedelta

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
BOT_TOKEN            = os.environ.get("BOT_TOKEN")
CHANNEL_ID           = int(os.environ.get("CHANNEL_ID"))
MY_USER_ID           = int(os.environ.get("MY_USER_ID", "0"))
API_ID               = int(os.environ.get("API_ID"))
API_HASH             = os.environ.get("API_HASH")
SESSION_STRING       = os.environ.get("SESSION_STRING", "")
DELETE_AFTER_MINUTES = int(os.environ.get("DELETE_AFTER_MINUTES", "60"))
PORT                 = int(os.environ.get("PORT", "8080"))
BASE_URL             = os.environ.get("BASE_URL", "").rstrip("/")
# ────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

# In-memory store: token -> {msg_id, file_name, file_size, expires_at}
file_store: dict = {}


def generate_token() -> str:
    return secrets.token_urlsafe(16)


def get_telethon_client():
    return TelegramClient(
        StringSession(SESSION_STRING), API_ID, API_HASH
    )


# ── Web Server Handler ─────────────────────────────────
async def stream_handler(request: web.Request) -> web.Response:
    token = request.match_info.get("token")

    if token not in file_store:
        return web.Response(status=404, text="Link expired or not found.")

    entry = file_store[token]

    # Check expiry
    if datetime.now() > entry["expires_at"]:
        del file_store[token]
        return web.Response(status=410, text="Link has expired.")

    msg_id    = entry["msg_id"]
    file_name = entry["file_name"]
    file_size = entry["file_size"]

    logger.info(f"Stream request for token={token} file={file_name}")

    # Handle range requests (for browser seeking/resuming)
    range_header = request.headers.get("Range")
    offset = 0
    end_byte = file_size - 1 if file_size else None

    if range_header and file_size:
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            offset = int(match.group(1))
            end_byte = int(match.group(2)) if match.group(2) else file_size - 1

    async def file_generator():
        async with get_telethon_client() as client:
            tl_msg = await client.get_messages(CHANNEL_ID, ids=msg_id)
            if tl_msg is None:
                return
            async for chunk in client.iter_download(
                tl_msg,
                offset=offset,
                chunk_size=512 * 1024  # 512KB chunks
            ):
                yield chunk

    headers = {
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Content-Type": "application/octet-stream",
        "Accept-Ranges": "bytes",
    }

    if file_size:
        content_length = end_byte - offset + 1
        headers["Content-Length"] = str(content_length)

    status = 206 if range_header else 200
    if range_header and file_size:
        headers["Content-Range"] = f"bytes {offset}-{end_byte}/{file_size}"

    return web.Response(
        status=status,
        headers=headers,
        body=file_generator()
    )


async def index_handler(request: web.Request) -> web.Response:
    return web.Response(text="✅ Bot stream server is running.")


# ── Delete job ─────────────────────────────────────────
async def delete_message_job(bot, chat_id, message_id, token):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Deleted message {message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not delete: {e}")
    # Remove from store
    file_store.pop(token, None)


# ── Bot Handlers ───────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any file (even 1GB+):\n"
        "⚡ Get instant stream/download link\n"
        "🔗 Works directly in browser\n"
        "🗑 Auto-deletes after set time\n\n"
        f"⏰ Current delete time: *{DELETE_AFTER_MINUTES} minutes*",
        parse_mode="Markdown"
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return

    message = update.message
    if not (message.video or message.document or message.audio or message.photo):
        await update.message.reply_text("⚠️ Please forward a file.")
        return

    # Get file info
    if message.video:
        file_name = message.video.file_name or "video.mp4"
        file_size = message.video.file_size or 0
    elif message.document:
        file_name = message.document.file_name or "file"
        file_size = message.document.file_size or 0
    elif message.audio:
        file_name = message.audio.file_name or "audio.mp3"
        file_size = message.audio.file_size or 0
    else:
        file_name = "file"
        file_size = 0

    file_size_mb = file_size / (1024 * 1024)

    status_msg = await update.message.reply_text(
        f"⏳ Processing `{file_name}`...",
        parse_mode="Markdown"
    )

    try:
        # Forward to private channel to store
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )
        channel_msg_id = forwarded.message_id

        # Generate unique token
        token = generate_token()
        expires_at = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)

        # Store file info
        file_store[token] = {
            "msg_id":     channel_msg_id,
            "file_name":  file_name,
            "file_size":  file_size,
            "expires_at": expires_at,
        }

        # Generate instant stream link
        stream_link   = f"{BASE_URL}/stream/{token}"
        download_link = f"{BASE_URL}/stream/{token}?download=1"

        # Schedule auto-delete
        scheduler.add_job(
            delete_message_job,
            "date",
            run_date=expires_at,
            args=[context.bot, CHANNEL_ID, channel_msg_id, token],
            id=f"del_{channel_msg_id}"
        )

        await status_msg.edit_text(
            f"✅ *Ready Instantly!*\n\n"
            f"📁 `{file_name}`\n"
            f"📦 {file_size_mb:.1f} MB\n\n"
            f"⬇️ *Download:* [{download_link}]({download_link})\n\n"
            f"▶️ *Stream:* [Stream Now 🎬]({stream_link})\n\n"
            f"⏰ Expires at: {expires_at.strftime('%I:%M %p')}\n"
            f"🗑 Auto-deleted in *{DELETE_AFTER_MINUTES} minutes*",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Error: {str(e)}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "Forward any file to get an instant stream/download link!"
    )


# ── Startup ────────────────────────────────────────────
async def on_startup(app_obj):
    scheduler.start()
    logger.info(f"✅ Bot started! BASE_URL={BASE_URL}")


def main():
    # ── Start web server ──
    web_app = web.Application()
    web_app.router.add_get("/", index_handler)
    web_app.router.add_get("/stream/{token}", stream_handler)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    runner = web.AppRunner(web_app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    loop.run_until_complete(site.start())
    logger.info(f"🌐 Web server running on port {PORT}")

    # ── Start Telegram bot ──
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
