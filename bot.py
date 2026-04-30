import os
import re
import asyncio
import logging
import secrets
import sqlite3
import time

from aiohttp import web
from telegram import Update
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

        # ✅ Counter table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS counter (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            value INTEGER
        )
        """)
        conn.execute("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 1)")

def get_counter():
    with db() as conn:
        row = conn.execute("SELECT value FROM counter WHERE id=1").fetchone()
        return row[0] if row else 1

def increment_counter():
    with db() as conn:
        conn.execute("UPDATE counter SET value = value + 1 WHERE id=1")

def save_file(token, msg_id, file_name, file_size, expires_at):
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?, ?, ?)",
            (token, msg_id, file_name, file_size, time.time(), expires_at)
        )

def get_file(token):
    with db() as conn:
        return conn.execute(
            "SELECT msg_id, file_name, file_size, expires_at FROM files WHERE token=?",
            (token,)
        ).fetchone()

def increase_download(token):
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO stats (token, downloads) VALUES (?, 0)", (token,))
        conn.execute("UPDATE stats SET downloads = downloads + 1 WHERE token=?", (token,))

# ── TELETHON ───────────────────────────
def get_client():
    return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ── STREAM HANDLER ─────────────────────
async def stream_handler(request):
    token = request.match_info.get("token")
    key = request.query.get("key")

    if key != SECRET_KEY:
        return web.Response(status=403, text="Unauthorized")

    row = get_file(token)
    if not row:
        return web.Response(status=404, text="Link not found")

    msg_id, file_name, file_size, expires_at = row

    if expires_at and time.time() > expires_at:
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
        "Cache-Control": "no-cache",
    }

    async def generator():
        async with get_client() as client:
            msg = await client.get_messages(CHANNEL_ID, ids=msg_id)
            async for chunk in client.iter_download(msg, offset=offset, chunk_size=CHUNK_SIZE):
                yield chunk

    return web.Response(status=206 if range_header else 200, headers=headers, body=generator())

# ── BOT LOGIC ──────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return
    await update.message.reply_text("Send video/file to get download link.")

async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != MY_USER_ID:
        return

    msg = update.message

    if msg.video:
        file_name = msg.video.file_name or "video.mp4"
        file_size = msg.video.file_size or 0
    elif msg.document:
        file_name = msg.document.file_name or "file.bin"
        file_size = msg.document.file_size or 0
    else:
        return

    forwarded = await context.bot.forward_message(
        chat_id=CHANNEL_ID,
        from_chat_id=msg.chat_id,
        message_id=msg.message_id
    )

    token = secrets.token_urlsafe(16)
    expires = time.time() + LINK_EXPIRE_SECONDS

    save_file(token, forwarded.message_id, file_name, file_size, expires)

    link = f"{BASE_URL}/stream/{token}?key={SECRET_KEY}&download=1"

    # ✅ Persistent counter
    video_number = get_counter()

    # Convert size
    size_mb = round(file_size / (1024 * 1024), 2)

    # ✅ FINAL OUTPUT
    await update.message.reply_text(
        f"📦 File Size : {size_mb} MB\n"
        f"⬇️ Video {video_number} : {file_name}\n"
        f"🔗 {link}"
    )

    increment_counter()

# ── MAIN ───────────────────────────────
def main():
    init_db()

    app = web.Application()
    app.router.add_get("/stream/{token}", stream_handler)
    app.router.add_get("/", lambda r: web.Response(text="Running"))

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
