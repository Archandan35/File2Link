import os
import re
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

# How many seconds to wait to collect batch messages
BATCH_WAIT_SECONDS   = 3
# ────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

# File store: token -> file info
file_store: dict = {}

# Batch store: user_id -> {messages: [], task: asyncio.Task}
batch_store: dict = {}


def generate_token() -> str:
    return secrets.token_urlsafe(16)


def get_telethon_client():
    return TelegramClient(
        StringSession(SESSION_STRING), API_ID, API_HASH
    )


# ── Web Server ─────────────────────────────────────────
async def stream_handler(request: web.Request) -> web.Response:
    token = request.match_info.get("token")

    if token not in file_store:
        return web.Response(status=404, text="Link expired or not found.")

    entry = file_store[token]

    if datetime.now() > entry["expires_at"]:
        del file_store[token]
        return web.Response(status=410, text="Link has expired.")

    msg_id    = entry["msg_id"]
    file_name = entry["file_name"]
    file_size = entry["file_size"]

    range_header = request.headers.get("Range")
    offset = 0
    end_byte = file_size - 1 if file_size else None

    if range_header and file_size:
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            offset = int(match.group(1))
            end_byte = int(match.group(2)) if match.group(2) else file_size - 1

    is_download = "download" in request.query

    if is_download:
        disposition  = f'attachment; filename="{file_name}"'
        content_type = "application/octet-stream"
    else:
        disposition = f'inline; filename="{file_name}"'
        ext = file_name.lower().split(".")[-1]
        content_types = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska",
            "avi": "video/x-msvideo",
            "mov": "video/quicktime",
            "mp3": "audio/mpeg",
            "m4a": "audio/mp4",
            "pdf": "application/pdf",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "png": "image/png",
        }
        content_type = content_types.get(ext, "video/mp4")

    headers = {
        "Content-Disposition": disposition,
        "Content-Type": content_type,
        "Accept-Ranges": "bytes",
    }

    if file_size:
        content_length = end_byte - offset + 1
        headers["Content-Length"] = str(content_length)

    status = 206 if range_header else 200
    if range_header and file_size:
        headers["Content-Range"] = f"bytes {offset}-{end_byte}/{file_size}"

    async def file_generator():
        async with get_telethon_client() as client:
            tl_msg = await client.get_messages(CHANNEL_ID, ids=msg_id)
            if tl_msg is None:
                return
            async for chunk in client.iter_download(
                tl_msg,
                offset=offset,
                chunk_size=512 * 1024
            ):
                yield chunk

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
    file_store.pop(token, None)


# ── Process Batch ──────────────────────────────────────
async def process_batch(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Wait for BATCH_WAIT_SECONDS then process all collected messages."""

    await asyncio.sleep(BATCH_WAIT_SECONDS)

    if user_id not in batch_store:
        return

    messages = batch_store[user_id]["messages"]
    del batch_store[user_id]

    if not messages:
        return

    total = len(messages)
    logger.info(f"Processing batch of {total} files for user {user_id}")

    # Send initial status
    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"⏳ Processing *{total}* file(s)... Please wait.",
        parse_mode="Markdown"
    )

    expires_at     = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)
    download_lines = []
    stream_lines   = []
    total_size_mb  = 0
    failed         = []

    for i, message in enumerate(messages, 1):
        try:
            # Get file info
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

            # Forward to private channel
            forwarded = await context.bot.forward_message(
                chat_id=CHANNEL_ID,
                from_chat_id=message.chat_id,
                message_id=message.message_id
            )
            channel_msg_id = forwarded.message_id

            # Generate token and store
            token = generate_token()
            file_store[token] = {
                "msg_id":     channel_msg_id,
                "file_name":  file_name,
                "file_size":  file_size,
                "expires_at": expires_at,
            }

            download_url = f"{BASE_URL}/stream/{token}?download=1"
            stream_url   = f"{BASE_URL}/stream/{token}"

            download_lines.append(
                f"⬇️ *Video {i}* [{file_name}]({download_url}) "
                f"_{file_size_mb:.1f}MB_"
            )
            stream_lines.append(
                f"▶️ *Video {i}* [Stream Now 🎬]({stream_url})"
            )

            # Schedule auto-delete
            scheduler.add_job(
                delete_message_job,
                "date",
                run_date=expires_at,
                args=[context.bot, CHANNEL_ID, channel_msg_id, token],
                id=f"del_{channel_msg_id}"
            )

            logger.info(f"Processed {i}/{total}: {file_name}")

        except Exception as e:
            logger.error(f"Failed file {i}: {e}")
            failed.append(i)

    # Build final message
    success_count = total - len(failed)

    text = f"✅ *Ready Instantly!* ({success_count}/{total} files)\n"
    text += f"📦 Total size: {total_size_mb:.1f} MB\n\n"

    # Download links
    text += "\n".join(download_lines)
    text += "\n\n"

    # Stream links
    text += "\n".join(stream_lines)
    text += "\n\n"

    text += f"⏰ Expires at: {expires_at.strftime('%I:%M %p')}\n"
    text += f"🗑 Auto-deleted in *{DELETE_AFTER_MINUTES} minutes*"

    if failed:
        text += f"\n\n⚠️ Failed: Video(s) {', '.join(map(str, failed))}"

    # Telegram message limit is 4096 chars
    # Split into chunks if too many files
    if len(text) <= 4096:
        await status_msg.edit_text(text, parse_mode="Markdown")
    else:
        # Send in multiple messages
        await status_msg.edit_text(
            f"✅ *Ready!* {success_count} files processed.\n"
            f"📦 Total: {total_size_mb:.1f} MB\n"
            f"⏰ Expires: {expires_at.strftime('%I:%M %p')}\n\n"
            f"_Links below:_",
            parse_mode="Markdown"
        )
        # Send download links
        dl_text = "⬇️ *Download Links:*\n\n" + "\n".join(download_lines)
        await context.bot.send_message(
            chat_id=chat_id,
            text=dl_text,
            parse_mode="Markdown"
        )
        # Send stream links
        st_text = "▶️ *Stream Links:*\n\n" + "\n".join(stream_lines)
        await context.bot.send_message(
            chat_id=chat_id,
            text=st_text,
            parse_mode="Markdown"
        )


# ── Bot Handlers ───────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any file or *multiple files at once*:\n"
        "⚡ Get instant stream/download links\n"
        "📦 Batch processing supported (2-100 files)\n"
        "🔗 All links in one combined message\n"
        "🗑 Auto-deletes after set time\n\n"
        f"⏰ Delete time: *{DELETE_AFTER_MINUTES} minutes*",
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

    # Add to batch store
    if user_id not in batch_store:
        batch_store[user_id] = {
            "messages": [],
            "task":     None
        }

    batch_store[user_id]["messages"].append(message)

    # Cancel existing timer and restart
    if batch_store[user_id]["task"]:
        batch_store[user_id]["task"].cancel()

    # Start new timer — waits BATCH_WAIT_SECONDS for more files
    task = asyncio.create_task(
        process_batch(user_id, chat_id, context)
    )
    batch_store[user_id]["task"] = task


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "Forward any file or multiple files together to get instant links!"
    )


async def on_startup(app_obj):
    scheduler.start()
    logger.info(f"✅ Bot started! BASE_URL={BASE_URL}")


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
