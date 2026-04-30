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
    logger.info("===== handle_video =====")
    user_id = update.effective_user.id

    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return

    message = update.message
    if not (message.video or message.document):
        await update.message.reply_text("⚠️ Please forward a video file.")
        return

    # Get file info from original message
    if message.video:
        file_id   = message.video.file_id
        file_name = message.video.file_name or "video.mp4"
        file_size = message.video.file_size or 0
    else:
        file_id   = message.document.file_id
        file_name = message.document.file_name or "file.mp4"
        file_size = message.document.file_size or 0

    file_size_mb = file_size / (1024 * 1024)
    logger.info(f"File: {file_name} ({file_size_mb:.1f} MB) file_id: {file_id}")

    status_msg = await update.message.reply_text(
        f"📥 Received: `{file_name}`\n"
        f"📦 Size: {file_size_mb:.1f} MB\n\n"
        f"⏳ *Step 1/3:* Forwarding to channel...",
        parse_mode="Markdown"
    )

    try:
        # Step 1: Forward to private channel
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )
        channel_msg_id = forwarded.message_id
        logger.info(f"Forwarded to channel. msg_id: {channel_msg_id}")

        await status_msg.edit_text(
            f"📥 File: `{file_name}` ({file_size_mb:.1f} MB)\n\n"
            f"⏳ *Step 2/3:* Downloading...\n"
            f"_(Large files take several minutes)_",
            parse_mode="Markdown"
        )

        # Step 2: Download directly using file_id via Telethon
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        logger.info(f"Connecting Telethon to download...")

        async with TelegramClient(
            StringSession(SESSION_STRING), API_ID, API_HASH
        ) as client:
            logger.info("Telethon connected. Getting messages from channel...")

            # Get the forwarded message directly from channel using Telethon
            tl_messages = await client.get_messages(
                CHANNEL_ID,
                ids=channel_msg_id
            )

            logger.info(f"Got message: {tl_messages}")

            if tl_messages is None:
                raise Exception(
                    "Telethon could not find the message in channel. "
                    "Make sure your SESSION_STRING account is a MEMBER of the channel."
                )

            file_path = await client.download_media(
                tl_messages,
                file=file_path
            )
            logger.info(f"Downloaded to: {file_path}")

        if not file_path or not os.path.exists(file_path):
            raise Exception(f"Download failed. file_path={file_path}")

        actual_size_mb = os.path.getsize(file_path) / (1024 * 1024)

        await status_msg.edit_text(
            f"📥 File: `{file_name}`\n\n"
            f"⏳ *Step 3/3:* Uploading to Gofile.io...\n"
            f"📦 Size: {actual_size_mb:.1f} MB",
            parse_mode="Markdown"
        )

        # Step 3: Upload to Gofile
        logger.info("Uploading to Gofile...")
        download_link = await upload_to_gofile(file_path, file_name)
        logger.info(f"Gofile link: {download_link}")

        # Cleanup local file
        try:
            os.remove(file_path)
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
            f"📁 File: `{file_name}`\n"
            f"📦 Size: {actual_size_mb:.1f} MB\n\n"
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
    logger.info(f"✅ Bot started!")
    logger.info(f"MY_USER_ID = {MY_USER_ID}")
    logger.info(f"CHANNEL_ID = {CHANNEL_ID}")
    logger.info(f"SESSION    = {'SET' if SESSION_STRING else 'MISSING'}")


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
