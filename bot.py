import logging
import asyncio
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler,
    filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── YOUR CONFIG ─────────────────────────────────────────
BOT_TOKEN   = "8646945093:AAH5-cYiEL5nqb_QluUcLvmLnVxe-ftlTug"         # from BotFather
CHANNEL_ID  = -1002599818759          # your private channel ID
MY_USER_ID  = 26932049              # your Telegram user ID (only you)
DELETE_AFTER_MINUTES = 60              # auto-delete after 60 mins (1 hr)
# ────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
scheduler = AsyncIOScheduler()

# ── Track messages for auto-delete ──
# { (chat_id, message_id): delete_at_time }
pending_deletes: dict = {}


async def delete_message_job(bot, chat_id, message_id):
    """Delete a specific message from the channel."""
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logging.info(f"Deleted message {message_id} from {chat_id}")
    except Exception as e:
        logging.warning(f"Could not delete message {message_id}: {e}")


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Only respond to your own user ID."""
    user_id = update.effective_user.id

    # 🔒 Block everyone except you
    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized access.")
        return

    message = update.message

    # Check if it's a video or document (large videos sent as files)
    if not (message.video or message.document):
        await update.message.reply_text(
            "Please forward a video file."
        )
        return

    await update.message.reply_text("⏳ Forwarding to your private channel...")

    # Forward the video to your private channel
    forwarded = await context.bot.forward_message(
        chat_id=CHANNEL_ID,
        from_chat_id=message.chat_id,
        message_id=message.message_id
    )

    # Build the download link
    # For private channels, this is a deep link via your bot
    channel_msg_id = forwarded.message_id
    
    # Generate link (works for private channel via bot)
    link = f"https://t.me/c/{str(CHANNEL_ID).replace('-100', '')}/{channel_msg_id}"

    # Calculate delete time
    delete_at = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)

    # Schedule auto-delete from channel
    scheduler.add_job(
        delete_message_job,
        "date",
        run_date=delete_at,
        args=[context.bot, CHANNEL_ID, channel_msg_id],
        id=f"del_{CHANNEL_ID}_{channel_msg_id}"
    )

    # Also schedule delete of the original forwarded message in bot chat
    scheduler.add_job(
        delete_message_job,
        "date",
        run_date=delete_at,
        args=[context.bot, message.chat_id, message.message_id],
        id=f"del_{message.chat_id}_{message.message_id}"
    )

    await update.message.reply_text(
        f"✅ *Video Uploaded Successfully!*\n\n"
        f"🔗 *Download Link:*\n`{link}`\n\n"
        f"⏰ *Auto-deletes at:* {delete_at.strftime('%H:%M:%S')}\n"
        f"🗑 File will be deleted in *{DELETE_AFTER_MINUTES} minutes*",
        parse_mode="Markdown"
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text(
        "👋 Hi! Forward any video to me and I'll:\n"
        "1. Upload it to your private channel\n"
        "2. Give you a download link\n"
        "3. Auto-delete it after 1 hour 🗑"
    )


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Only handle videos/documents and text
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.VIDEO, handle_video
    ))
    app.add_handler(MessageHandler(filters.TEXT, handle_text))

    scheduler.start()
    print("🤖 Bot is running... Forward a video to get started!")
    app.run_polling()


if __name__ == "__main__":
    main()
