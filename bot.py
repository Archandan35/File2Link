import os
import logging
import aiohttp
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


async def get_gofile_server():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.gofile.io/servers") as r:
            data = await r.json()
            return data["data"]["servers"][0]["name"]


async def upload_to_gofile(file_path: str, filename: str) -> str:
    server = await get_gofile_server()
    url = f"https://{server}.gofile.io/uploadFile"
    async with aiohttp.ClientSession() as session:
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("file", f, filename=filename)
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
    logger.info(f"/start from {update.effective_user.id}")
    if update.effective_user.id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any video (even 1GB+):\n"
        "1️⃣ Downloads from Telegram\n"
        "2️⃣ Uploads to Gofile.io\n"
        "3️⃣ Sends direct download link\n"
        "4️⃣ Auto-deletes after 1 hour 🗑",
        parse_mode="Markdown"
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("===== handle_video triggered =====")

    user_id = update.effective_user.id
    logger.info(f"From user_id: {user_id}, MY_USER_ID: {MY_USER_ID}")

    if user_id != MY_USER_ID:
        logger.warning("Unauthorized user")
        await update.message.reply_text("⛔ Unauthorized.")
        return

    message = update.message
    logger.info(f"Message type - video: {message.video}, document: {message.document}")

    if not (message.video or message.document):
        await update.message.reply_text("⚠️ Please forward a video file.")
        return

    status_msg = await update.message.reply_text(
        "⏳ *Step 1/3:* Forwarding to channel...",
        parse_mode="Markdown"
    )

    try:
        # Step 1: Forward to channel
        logger.info(f"Forwarding to CHANNEL_ID: {CHANNEL_ID}")
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )
        channel_msg_id = forwarded.message_id
        logger.info(f"Forwarded. channel_msg_id: {channel_msg_id}")

        await status_msg.edit_text(
            "⏳ *Step 2/3:* Downloading from Telegram...\n"
            "_(Large files take several minutes)_",
            parse_mode="Markdown"
        )

        # Step 2: Download via Telethon
        logger.info("Starting Telethon download...")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        async with TelegramClient(
            StringSession(SESSION_STRING), API_ID, API_HASH
        ) as client:
            logger.info("Telethon client connected")
            file_path = await client.download_media(
                forwarded,
                file=DOWNLOAD_DIR + "/"
            )
            logger.info(f"Download complete: {file_path}")

        if not file_path or not os.path.exists(file_path):
            raise Exception(f"File not found after download: {file_path}")

        actual_filename = os.path.basename(file_path)
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"File ready: {actual_filename} ({file_size_mb:.1f} MB)")

        await status_msg.edit_text(
            f"⏳ *Step 3/3:* Uploading to Gofile.io...\n"
            f"📦 Size: {file_size_mb:.1f} MB",
            parse_mode="Markdown"
        )

        # Step 3: Upload to Gofile
        logger.info("Uploading to Gofile...")
        download_link = await upload_to_gofile(file_path, actual_filename)
        logger.info(f"Gofile link: {download_link}")

        # Cleanup
        try:
            os.remove(file_path)
            logger.info("Local file deleted")
        except Exception:
            pass

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
            f"📁 File: `{actual_filename}`\n"
            f"📦 Size: {file_size_mb:.1f} MB\n\n"
            f"🔗 *Direct Download:*\n{download_link}\n\n"
            f"⏰ Deletes at: {delete_at.strftime('%I:%M %p')}\n"
            f"🗑 Auto-deleted in *{DELETE_AFTER_MINUTES} minutes*",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"❌ Exception: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Error: {str(e)}")


async def handle_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Catch ALL incoming updates for debugging."""
    logger.info(f"📩 Incoming update from user: {update.effective_user.id if update.effective_user else 'unknown'}")
    logger.info(f"Message: {update.message}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Text from {update.effective_user.id}: {update.message.text}")
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "Forward any video to get a direct download link!"
    )


async def on_startup(app):
    scheduler.start()
    logger.info(f"✅ Bot started!")
    logger.info(f"MY_USER_ID = {MY_USER_ID}")
    logger.info(f"CHANNEL_ID = {CHANNEL_ID}")
    logger.info(f"API_ID     = {API_ID}")
    logger.info(f"SESSION    = {'SET' if SESSION_STRING else 'MISSING'}")


def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    # Catch ALL messages for debug
    app.add_handler(MessageHandler(filters.ALL, handle_all), group=0)

    app.add_handler(CommandHandler("start", start), group=1)
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL, handle_video
    ), group=1)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_text
    ), group=1)

    logger.info("🤖 Bot is running!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
