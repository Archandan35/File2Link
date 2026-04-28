import os
import logging
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler,
    filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── CONFIG FROM ENVIRONMENT VARIABLES ───────────────────
BOT_TOKEN            = os.environ.get("BOT_TOKEN")
CHANNEL_ID           = int(os.environ.get("CHANNEL_ID"))
MY_USER_ID           = int(os.environ.get("MY_USER_ID"))
DELETE_AFTER_MINUTES = 60
# ────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def delete_message_job(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Deleted message {message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not delete {message_id}: {e}")


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # 🔒 Only you can use this bot
    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return

    message = update.message

    if not (message.video or message.document):
        await update.message.reply_text("⚠️ Please forward a video file.")
        return

    await update.message.reply_text("⏳ Uploading to your private channel...")

    try:
        # Forward video to private channel
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )

        channel_msg_id = forwarded.message_id
        clean_channel_id = str(CHANNEL_ID).replace("-100", "")
        link = f"https://t.me/c/{clean_channel_id}/{channel_msg_id}"

        delete_at = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)

        # Schedule auto-delete from channel
        scheduler.add_job(
            delete_message_job,
            "date",
            run_date=delete_at,
            args=[context.bot, CHANNEL_ID, channel_msg_id],
            id=f"del_ch_{channel_msg_id}"
        )

        await update.message.reply_text(
            f"✅ *Done! Here is your link:*\n\n"
            f"🔗 `{link}`\n\n"
            f"⏰ Auto-deletes at: {delete_at.strftime('%I:%M %p')}\n"
            f"🗑 File deleted after *{DELETE_AFTER_MINUTES} minutes*",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text(f"❌ Error occurred: {str(e)}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any video to me and I will:\n"
        "1️⃣ Upload it to your private channel\n"
        "2️⃣ Send you a download link\n"
        "3️⃣ Auto-delete after 1 hour 🗑",
        parse_mode="Markdown"
    )


async def post_init(application):
    """Start scheduler after bot initializes."""
    scheduler.start()
    logger.info("✅ Scheduler started")


def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN environment variable is missing!")
    if not CHANNEL_ID:
        raise ValueError("CHANNEL_ID environment variable is missing!")
    if not MY_USER_ID:
        raise ValueError("MY_USER_ID environment variable is missing!")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)   # ← fixes the asyncio scheduler crash
        .build()
    )

    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.VIDEO, handle_video
    ))
    app.add_handler(MessageHandler(filters.TEXT, handle_text))

    logger.info("🤖 Bot is running!")
    app.run_polling()


if __name__ == "__main__":
    main()
