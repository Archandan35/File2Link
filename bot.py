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
AUTO_DELETE_SECONDS = int(os.environ.get("AUTO_DELETE_SECONDS", "0"))

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
        conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            token TEXT PRIMARY KEY,
            downloads INTEGER DEFAULT 0
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

def increase_download(token):
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO stats VALUES (?, 0)", (token,))
        conn.execute("UPDATE stats SET downloads = downloads + 1 WHERE token=?", (token,))

def get_downloads(token):
    with db() as conn:
        row = conn.execute("SELECT downloads FROM stats WHERE token=?", (token,)).fetchone()
        return row[0] if row else 0

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
        return web.Response(status=403, text="Link expired")

    increase_download(token)

    range_header = request.headers.get("Range")
    offset = 0

    if range_header:
        match = re.match(r"bytes=(\d+)-", range_header)
        if match:
            offset = int(match.group(1))

    headers = {
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Content-Type": "application/octet-stream",
        "Accept-Ranges": "bytes",
    }

    async def gen():
        async with get_client() as client:
            msg = await client.get_messages(CHANNEL_ID, ids=msg_id)
            async for chunk in client.iter_download(msg, offset=offset, chunk_size=CHUNK_SIZE):
                yield chunk

    return web.Response(status=206 if range_header else 200, headers=headers, body=gen())

# ── BATCH SYSTEM ───────────────────────
batch_store = {}
BATCH_WAIT = 3

def split_message(text, limit=4000):
    return [text[i:i+limit] for i in range(0, len(text), limit)]

async def process_batch(user_id, chat_id, context):
    await asyncio.sleep(BATCH_WAIT)

    if user_id not in batch_store:
        return

    messages = batch_store[user_id]["messages"]
    del batch_store[user_id]

    total = len(messages)
    total_size = 0

    download_lines = []
    keyboard = []

    for i, msg in enumerate(messages, 1):
        if msg.video:
            name = msg.video.file_name or f"video_{i}.mp4"
            size = msg.video.file_size or 0
        elif msg.document:
            name = msg.document.file_name or f"file_{i}"
            size = msg.document.file_size or 0
        else:
            continue

        total_size += size

        forwarded = await context.bot.forward_message(
            chat_id=CHANNEL_ID,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id
        )

        token = secrets.token_urlsafe(16)
        expires = time.time() + LINK_EXPIRE_SECONDS

        save_file(token, forwarded.message_id, name, size, expires)

        url = f"{BASE_URL}/stream/{token}?key={SECRET_KEY}&download=1"

        remaining = int((expires - time.time()) / 60)

        download_lines.append(
            f"♾️ *Video {i}*\n`{name}`\n⏳ Expires in: {remaining} min\n🔗 {url}\n📊 Downloads: {get_downloads(token)}"
        )

        keyboard.append([
            InlineKeyboardButton(f"📥 Copy Link {i}", url=url)
        ])

    total_mb = total_size / (1024 * 1024)

    text = (
        f"✅ *Ready Instantly!* {total}/{total} file(s) ready\n"
        f"📦 Total size: *{total_mb:.1f} MB*\n"
        f"─────────────────────────\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"♾️ *DOWNLOAD LINKS*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + "\n\n".join(download_lines)
    )

    parts = split_message(text)

    sent_msgs = []
    for part in parts:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=part,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        sent_msgs.append(msg)

    # auto delete
    if AUTO_DELETE_SECONDS > 0:
        await asyncio.sleep(AUTO_DELETE_SECONDS)
        for m in sent_msgs:
            try:
                await context.bot.delete_message(chat_id, m.message_id)
            except:
                pass

# ── BOT ───────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text("Send file(s) to get download links.")

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return

    msg = update.message
    if not (msg.video or msg.document):
        return

    uid = update.effective_user.id

    if uid not in batch_store:
        batch_store[uid] = {"messages": [], "task": None}

    batch_store[uid]["messages"].append(msg)

    if batch_store[uid]["task"]:
        batch_store[uid]["task"].cancel()

    batch_store[uid]["task"] = asyncio.create_task(
        process_batch(uid, update.effective_chat.id, context)
    )

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

    tg.run_polling()

if __name__ == "__main__":
    main()
