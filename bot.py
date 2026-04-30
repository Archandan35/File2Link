import os
import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta

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
DELETE_AFTER_MINUTES = 60
DOWNLOAD_DIR         = "/tmp/tgfiles"
# ────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


async def get_gofile_server() -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.gofile.io/servers") as r:
            data = await r.json()
            return data["data"]["servers"][0]["name"]


async def upload_to_gofile_streaming(
    client: TelegramClient,
    tl_message,
    filename: str,
    file_size: int,
    status_msg,
    context
) -> str:
    """Stream file from Telegram directly to Gofile chunk by chunk."""

    server = await get_gofile_server()
    url = f"https://{server}.gofile.io/uploadFile"

    # Use a pipe: async generator feeds Gofile upload
    chunk_size = 1024 * 1024  # 1MB chunks
    uploaded = 0
    last_update = 0

    async def file_chunks():
        nonlocal uploaded, last_update
        async for chunk in client.iter_download(tl_message, chunk_size=chunk_size):
            uploaded += len(chunk)
            percent = (uploaded / file_size * 100) if file_size else 0

            # Update status every 10%
            if percent - last_update >= 10:
                last_update = percent
                size_done = uploaded / (1024 * 1024)
                size_total = file_size / (1024 * 1024)
                try:
                    await status_msg.edit_text(
                        f"⚡ *Streaming to Gofile...*\n\n"
                        f"📦 {size_done:.1f} MB / {size_total:.1f} MB\n"
                        f"📊 Progress: {percent:.0f}%\n\n"
                        f"_(No local storage used)_",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
            yield chunk

    # Stream upload using aiohttp
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field(
            "file",
            file_chunks(),
            filename=filename,
            content_type="application/octet-stream"
        )
        async with session.post(url, data=form) as r:
            data = await r.json()
            if data["status"] == "ok":
                return data["data"]["downloadPage"]
            raise Exception(f"Gofile error: {data}")


async def delete_message_job(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Deleted message {message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not delete: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any video (even 1GB+):\n"
        "⚡ Streams directly — no wait for full download!\n"
        "🔗 Get Gofile direct download link\n"
        "🗑 Auto-deletes after 1 hour",
        parse_mode="Markdown"
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return

    message = update.message
    if not (message.video or message.document):
        await update.message.reply_text("⚠️ Please forward a video file.")
        return

    if message.video:
        file_name = message.video.file_name or "video.mp4"
        file_size = message.video.file_size or 0
    else:
        file_name = message.document.file_name or "file.mp4"
        file_size = message.document.file_size or 0

    file_size_mb = file_size / (1024 * 1024)

    status_msg = await update.message.reply_text(
        f"📥 *{file_name}*\n"
        f"📦 {file_size_mb:.1f} MB\n\n"
        f"⏳ Forwarding to channel...",
        parse_mode="Markdown"
    )

    try:
        # Forward to private channel
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )
        channel_msg_id = forwarded.message_id
        logger.info(f"Forwarded. channel_msg_id={channel_msg_id}")

        await status_msg.edit_text(
            f"📥 *{file_name}* ({file_size_mb:.1f} MB)\n\n"
            f"⚡ *Streaming to Gofile...*\n"
            f"_(Starting...)_",
            parse_mode="Markdown"
        )

        # Stream from Telegram → Gofile
        async with TelegramClient(
            StringSession(SESSION_STRING), API_ID, API_HASH
        ) as client:
            logger.info("Telethon connected")

            tl_message = await client.get_messages(
                CHANNEL_ID,
                ids=channel_msg_id
            )

            if tl_message is None:
                raise Exception(
                    "Could not find message in channel.\n"
                    "Make sure your Telegram account is a member of the channel."
                )

            download_link = await upload_to_gofile_streaming(
                client,
                tl_message,
                file_name,
                file_size,
                status_msg,
                context
            )

        logger.info(f"Gofile link: {download_link}")

        delete_at = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)
        scheduler.add_job(
            delete_message_job,
            "date",
            run_date=delete_at,
            args=[context.bot, CHANNEL_ID, channel_msg_id],
            id=f"del_{channel_msg_id}"
        )

        await status_msg.edit_text(
            f"✅ *Done!*\n\n"
            f"📁 `{file_name}`\n"
            f"📦 {file_size_mb:.1f} MB\n\n"
            f"🔗 *Direct Download:*\n{download_link}\n\n"
            f"⏰ Deletes at: {delete_at.strftime('%I:%M %p')}\n"
            f"🗑 Auto-deleted in *{DELETE_AFTER_MINUTES} minutes*",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        await status_msg.edit_text(
            f"❌ *Error:*\n`{str(e)}`",
            parse_mode="Markdown"
        )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "Forward any video to get a direct download link!"
    )


async def on_startup(app):
    scheduler.start()
    logger.info(f"✅ Bot started! MY_USER_ID={MY_USER_ID}")


def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL, handle_video
    ))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_text
    ))

    logger.info("🤖 Bot is running!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
