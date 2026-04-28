import os
import logging
import asyncio
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler,
    filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── CONFIG ───────────────────────────────────────────────
BOT_TOKEN            = os.environ.get("BOT_TOKEN")
CHANNEL_ID           = int(os.environ.get("CHANNEL_ID"))
MY_USER_ID           = int(os.environ.get("MY_USER_ID"))
DELETE_AFTER_MINUTES = 60
# ────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def delete_message_job(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Deleted message {message_id}")
    except Exception as e:
        logger.warning(f"⚠️ Could not delete {message_id}: {e}")


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # 🔒 Block everyone except you
    if user_id != MY_USER_ID:
        await update.message.reply_text("⛔ Unauthorized.")
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

        # Schedule auto-delete
        context.application.scheduler.add_job(
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


async def main():
    # ── Start scheduler ──
    scheduler = AsyncIOScheduler()
    scheduler.start()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    # Attach scheduler to app so handlers can access it
    app.scheduler = scheduler

    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL, handle_video
    ))
    app.add_handler(MessageHandler(filters.TEXT, handle_text))

    logger.info("🤖 Bot is running!")

    # ── This replaces run_polling() for async main ──
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # Keep running forever
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
