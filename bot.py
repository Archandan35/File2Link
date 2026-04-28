import os
import logging
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

# ── CONFIG ───────────────────────────────────────────────
BOT_TOKEN            = os.environ.get("BOT_TOKEN")
CHANNEL_ID           = int(os.environ.get("CHANNEL_ID"))
CHANNEL_USERNAME     = os.environ.get("CHANNEL_USERNAME")  # without @
MY_USER_ID           = int(os.environ.get("MY_USER_ID", "0"))
DELETE_AFTER_MINUTES = 60
# ────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()


async def delete_message_job(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Deleted message {message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not delete {message_id}: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any video to me and I will:\n"
        "1️⃣ Upload it to your channel\n"
        "2️⃣ Send you a *direct download link*\n"
        "3️⃣ Auto-delete after 1 hour 🗑",
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

    await update.message.reply_text("⏳ Uploading to channel...")

    try:
        # Forward to channel
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )

        channel_msg_id = forwarded.message_id

        # Get file_id from forwarded message
        if forwarded.video:
            file_id = forwarded.video.file_id
            file_name = forwarded.video.file_name or "video.mp4"
        elif forwarded.document:
            file_id = forwarded.document.file_id
            file_name = forwarded.document.file_name or "file"
        else:
            file_id = None
            file_name = "file"

        # Generate direct download link via bot file API
        file_obj = await context.bot.get_file(file_id)
        direct_link = file_obj.file_path  # This is a direct HTTPS download URL

        delete_at = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)

        # Schedule auto-delete from channel
        scheduler.add_job(
            delete_message_job,
            "date",
            run_date=delete_at,
            args=[context.bot, CHANNEL_ID, channel_msg_id],
            id=f"del_{channel_msg_id}"
        )

        await update.message.reply_text(
            f"✅ *Done!*\n\n"
            f"📁 File: `{file_name}`\n\n"
            f"🔗 *Direct Download Link:*\n`{direct_link}`\n\n"
            f"⏰ Auto-deletes at: {delete_at.strftime('%I:%M %p')}\n"
            f"🗑 Link expires in *{DELETE_AFTER_MINUTES} minutes*",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "👋 Forward any video to get a direct download link!"
    )


async def on_startup(app):
    scheduler.start()
    logger.info(f"✅ Bot started. MY_USER_ID={MY_USER_ID}")


def main():
    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN missing!")

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
    app.run_polling()


if __name__ == "__main__":
    main()
