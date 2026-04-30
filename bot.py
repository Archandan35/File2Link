import os
import re
import asyncio
import logging
import secrets
import sqlite3
import time

from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
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

WORKERS = 3
CHUNK_SIZE = 2 * 1024 * 1024
LINK_EXPIRE = 3600
MAX_PER_MSG = 10

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── DB ─────────────────────────────
conn = sqlite3.connect("data.db", check_same_thread=False)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS jobs(
id INTEGER PRIMARY KEY AUTOINCREMENT,
user_id INT,
chat_id INT,
msg_id INT,
status TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS files(
token TEXT PRIMARY KEY,
msg_id INT,
name TEXT,
size INT,
expires REAL
)
""")

conn.commit()

# ── TELETHON ─────────────────────────
def client():
    return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ── STREAM ─────────────────────────
async def stream_handler(request):
    token = request.match_info["token"]
    row = cur.execute("SELECT msg_id,name,size,expires FROM files WHERE token=?", (token,)).fetchone()

    if not row:
        return web.Response(text="Not found", status=404)

    msg_id, name, size, exp = row
    if time.time() > exp:
        return web.Response(text="Expired", status=403)

    async def gen():
        async with client() as c:
            msg = await c.get_messages(CHANNEL_ID, ids=msg_id)
            async for chunk in c.iter_download(msg, chunk_size=CHUNK_SIZE):
                yield chunk

    return web.Response(
        headers={
            "Content-Disposition": f'attachment; filename="{name}"',
            "Content-Type": "application/octet-stream",
            "Accept-Ranges": "bytes"
        },
        body=gen()
    )

# ── PROGRESS BAR ─────────────────────
def progress_bar(done, total):
    percent = int((done/total)*10)
    bar = "█"*percent + "░"*(10-percent)
    return f"[{bar}] {done}/{total}"

# ── WORKER ─────────────────────────
queue = asyncio.Queue()

async def worker():
    while True:
        job = await queue.get()
        await process(job)
        queue.task_done()

async def process(job):
    messages, chat_id, context = job
    total = len(messages)
    done = 0

    progress_msg = await context.bot.send_message(chat_id, "⏳ Starting...")

    results = []

    for i, msg in enumerate(messages, 1):
        # retry system
        while True:
            try:
                fwd = await context.bot.forward_message(
                    chat_id=CHANNEL_ID,
                    from_chat_id=msg.chat_id,
                    message_id=msg.message_id
                )
                break
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after+1)
            except Exception:
                await asyncio.sleep(2)

        name = (msg.video.file_name if msg.video else msg.document.file_name) or f"file_{i}"
        size = (msg.video.file_size if msg.video else msg.document.file_size) or 0

        token = secrets.token_urlsafe(12)
        cur.execute("INSERT INTO files VALUES (?,?,?,?,?)",
                    (token, fwd.message_id, name, size, time.time()+LINK_EXPIRE))
        conn.commit()

        url = f"{BASE_URL}/stream/{token}?download=1"
        results.append((i,name,url))

        done += 1

        # progress update
        try:
            await progress_msg.edit_text(
                f"⏳ Processing\n{progress_bar(done,total)}"
            )
        except:
            pass

        await asyncio.sleep(0.5)

    await progress_msg.edit_text(f"✅ Done {total} files")

    # send in chunks
    for i in range(0, len(results), MAX_PER_MSG):
        chunk = results[i:i+MAX_PER_MSG]
        text = ""
        keyboard = []

        for idx,name,url in chunk:
            text += f"📦 *Video {idx}*\n`{name}`\n🔗 {url}\n\n"
            keyboard.append([InlineKeyboardButton(f"📥 Copy {idx}", url=url)])

        await context.bot.send_message(
            chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )

# ── BATCH ─────────────────────────
batch = {}

async def collect(update, context):
    if update.effective_user.id != MY_USER_ID:
        return

    msg = update.message
    if not (msg.video or msg.document):
        return

    uid = update.effective_user.id

    if uid not in batch:
        batch[uid] = []

    batch[uid].append(msg)

    await asyncio.sleep(3)

    if uid in batch:
        msgs = batch[uid]
        del batch[uid]
        await queue.put((msgs, update.effective_chat.id, context))

# ── MAIN ─────────────────────────
def main():
    app = web.Application()
    app.router.add_get("/stream/{token}", stream_handler)

    loop = asyncio.get_event_loop()
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    loop.run_until_complete(web.TCPSite(runner, "0.0.0.0", PORT).start())

    tg = ApplicationBuilder().token(BOT_TOKEN).build()
    tg.add_handler(CommandHandler("start", lambda u,c: u.message.reply_text("Send files")))
    tg.add_handler(MessageHandler(filters.ALL, collect))

    for _ in range(WORKERS):
        loop.create_task(worker())

    tg.run_polling()

if __name__ == "__main__":
    main()
