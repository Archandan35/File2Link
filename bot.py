import os
import logging
import asyncio
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

# Always create download folder on startup
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
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any video (even 1GB+):\n"
        "1️⃣ I download it from Telegram\n"
        "2️⃣ Upload to Gofile.io\n"
        "3️⃣ Send you a direct download link\n"
        "4️⃣ Auto-delete after 1 hour 🗑\n\n"
        "⚡ Large files take a few minutes.",
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

    status_msg = await update.message.reply_text(
        "⏳ *Step 1/3:* Forwarding to channel...",
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

        await status_msg.edit_text(
            "⏳ *Step 2/3:* Downloading from Telegram...\n"
            "_(Large files take several minutes)_",
            parse_mode="Markdown"
        )

        # Step 2: Download via Telethon into folder
        # Let Telethon name the file automatically
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        async with TelegramClient(
            StringSession(SESSION_STRING), API_ID, API_HASH
        ) as client:
            file_path = await client.download_media(
                forwarded,
