import os
import re
import asyncio
import logging
import secrets
import sqlite3
import time

from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import RetryAfter
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── CONFIG ─────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID"))
MY_USER_ID = int(os.environ.get("MY_USER_ID", "0"))
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
PORT = int(os.environ.get("PORT", "8080"))

CHUNK_SIZE = 2 * 1024 * 1024
LINK_EXPIRE_SECONDS = 3600
SECRET_KEY = "mysecurekey"

MAX_FILES_PER_MESSAGE = 5
WORKERS = 2  # safe parallel workers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── DATABASE ───────────────────────────
DB = "files.db"

def db():
    return sqlite3.connect(DB)

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            token TEXT PRIMARY KEY,
            msg_id INTEGER,
            file_name TEXT,
            file_size INTEGER,
            created_at REAL,
            expires_at REAL
        )
        """)

def save_file(token, msg_id, file_name, file_size, expires):
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?)",
            (token, msg_id, file_name, file_size, time.time(), expires)
        )

def get_file(token):
    with db() as conn:
        return conn.execute(
            "SELECT msg_id, file_name, file_size, expires_at FROM files WHERE token=?",
            (token,)
        ).fetchone()

# ── TELETHON ───────────────────────────
def get_client():
    return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ── STREAM ─────────────────────────────
async def stream_handler(request):
    token = request.match_info.get("token")
    key = request.query.get("key")

    if key != SECRET_KEY:
        return web.Response(status=403, text="Unauthorized")

    row = get_file(token)
    if not row:
        return web.Response(status=404, text="Link not found")

    msg_id, file_name, file_size, expires = row

    if expires and time.time() > expires:
        return web.Response(status=403, text="Expired")

    async def gen():
        async with get_client() as client:
            msg = await client.get_messages(CHANNEL_ID, ids=msg_id)
            async for chunk in client.iter_download(msg, chunk_size=CHUNK_SIZE):
                yield chunk

    return web.Response(
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes"
        },
        body=gen()
    )

# ── QUEUE SYSTEM ───────────────────────
queue = asyncio.Queue()

async def worker():
    while True:
        job = await queue.get()
        await process_job(*job)
        queue.task_done()

async def process_job(messages, chat_id, context):
    total = len(messages)
    done = 0

    progress_msg = await context.bot.send_message(
        chat_id, f"⏳ Processing 0/{total}"
    )

    results = []

    for msg in messages:
        while True:
            try:
                forwarded = await context.bot.forward_message(
                    chat_id=CHANNEL_ID,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id
                )
                break
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except Exception:
                await asyncio.sleep(2)

        if msg.video:
            name = msg.video.file_name or "video.mp4"
            size = msg.video.file_size or 0
        else:
            name = msg.document.file_name or "file.bin"
            size = msg.document.file_size or 0

        token = secrets.token_urlsafe(16)
        save_file(token, forwarded.message_id, name, size, time.time() + LINK_EXPIRE_SECONDS)

        url = f"{BASE_URL}/stream/{token}?key={SECRET_KEY}&download=1"

        results.append((name, url))
        done += 1

        # progress update
        await progress_msg.edit_text(f"⏳ Processing {done}/{total}")

        await asyncio.sleep(0.6)

    await progress_msg.edit_text(f"✅ Done {total}/{total}")

    # send in chunks
    for i in range(0, len(results), MAX_FILES_PER_MESSAGE):
        chunk = results[i:i+MAX_FILES_PER_MESSAGE]

        text = ""
        keyboard = []

        for idx, (name, url) in enumerate(chunk, i+1):
            text += f"📦 *Video {idx}*\n`{name}`\n🔗 {url}\n\n"
            keyboard.append([InlineKeyboardButton(f"📥 Copy {idx}", url=url)])

        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )

# ── BOT ───────────────────────────────
async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return

    msg = update.message
    if not (msg.video or msg.document):
        return

    await queue.put(([msg], update.effective_chat.id, context))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send files.")

# ── MAIN ──────────────────────────────
def main():
    init_db()

    app = web.Application()
    app.router.add_get("/stream/{token}", stream_handler)

    loop = asyncio.get_event_loop()
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    loop.run_until_complete(web.TCPSite(runner, "0.0.0.0", PORT).start())

    tg = ApplicationBuilder().token(BOT_TOKEN).build()
    tg.add_handler(CommandHandler("start", start))
    tg.add_handler(MessageHandler(filters.ALL, handle))

    # start workers
    for _ in range(WORKERS):
        loop.create_task(worker())

    tg.run_polling()

if __name__ == "__main__":
    main()
