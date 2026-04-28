import os
import logging
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── CONFIG ───────────────────────────────────────────────
BOT_TOKEN            = os.environ.get("BOT_TOKEN")
CHANNEL_ID           = int(os.environ.get("CHANNEL_ID"))
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


# ── /start command ──
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    real_id = update.effective_user.id
    stored_id = MY_USER_ID

    logger.info(f"START from user_id={real_id}, stored MY_USER_ID={stored_id}")

    await update.message.reply_text(
        f"👤 Your Telegram ID: `{real_id}`\n"
        f"🔧 Bot stored ID: `{stored_id}`\n"
        f"✅ Match: `{real_id == stored_id}`",
        parse_mode="Markdown"
    )


# ── /id command ──
async def get_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    real_id = update.effective_user.id
    await update.message.reply_text(
        f"Your user ID is: `{real_id}`",
        parse_mode="Markdown"
    )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info(f"Video from user_id={user_id}, MY_USER_ID={MY_USER_ID}")

    if user_id != MY_USER_ID:
        await update.message.reply_text(
            f"⛔ Unauthorized.\n"
            f"Your ID: `{user_id}`\n"
            f"Required ID: `{MY_USER_ID}`",
            parse_mode="Markdown"
        )
        return

    message = update.message
    if not (message.video or message.document):
        await update.message.reply_text("⚠️ Please forward a video file.")
        return

    await update.message.reply_text("⏳ Uploading to your private channel...")

    try:
        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id
        )

        channel_msg_id = forwarded.message_id
        clean_id = str(CHANNEL_ID).replace("-100", "")
        link = f"https://t.me/c/{clean_id}/{channel_msg_id}"
        delete_at = datetime.now() + timedelta(minutes=DELETE_AFTER_MINUTES)

        scheduler.add_job(
            delete_message_job,
            "date",
            run_date=delete_at,
            args=[context.bot, CHANNEL_ID, channel_msg_id],
            id=f"del_{channel_msg_id}"
        )

        await update.message.reply_text(
            f"✅ *Done! Your download link:*\n\n"
            f"🔗 `{link}`\n\n"
            f"⏰ Auto-deletes at: {delete_at.strftime('%I:%M %p')}\n"
            f"🗑 Deleted after *{DELETE_AFTER_MINUTES} minutes*",
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text(f"❌ Error: {str(e)}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info(f"Text from user_id={user_id}")

    if user_id != MY_USER_ID:
        await update.message.reply_text(
            f"⛔ Unauthorized.\n"
            f"Your ID: `{user_id}`\n"
            f"Required ID: `{MY_USER_ID}`",
            parse_mode="Markdown"
        )
        return

    await update.message.reply_text(
        "👋 *Welcome!*\n\n"
        "Forward any video to me and I will:\n"
        "1️⃣ Upload it to your private channel\n"
        "2️⃣ Send you a download link\n"
        "3️⃣ Auto-delete after 1 hour 🗑",
        parse_mode="Markdown"
    )


async def on_startup(app):
    scheduler.start()
    logger.info(f"✅ Bot started. MY_USER_ID={MY_USER_ID}")


def main():
    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN is missing!")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("id", get_id))
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL, handle_video
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("🤖 Bot is running!")
    app.run_polling()


if __name__ == "__main__":
    main()
