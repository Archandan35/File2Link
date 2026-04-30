import os
import re
import asyncio
import logging
import secrets

from aiohttp import web
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── CONFIG ───────────────────────────────────────────────
BOT_TOKEN          = os.environ.get("BOT_TOKEN")
MY_USER_ID         = int(os.environ.get("MY_USER_ID", "0"))
API_ID             = int(os.environ.get("API_ID"))
API_HASH           = os.environ.get("API_HASH")
SESSION_STRING     = os.environ.get("SESSION_STRING", "")
PORT               = int(os.environ.get("PORT", "8080"))
BASE_URL           = os.environ.get("BASE_URL", "").rstrip("/")
BATCH_WAIT_SECONDS = 3

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Global persistent Telethon client ─────────────────
telethon_client: TelegramClient = None

file_store:  dict = {}
batch_store: dict = {}


def generate_token() -> str:
    return secrets.token_urlsafe(16)


async def get_client() -> TelegramClient:
    global telethon_client
    if telethon_client is None or not telethon_client.is_connected():
        logger.info("🔌 Reconnecting Telethon client...")
        telethon_client = TelegramClient(
            StringSession(SESSION_STRING), API_ID, API_HASH
        )
        await telethon_client.connect()
        logger.info("✅ Telethon client connected")
    return telethon_client


# ── Web Server ─────────────────────────────────────────
async def stream_handler(request: web.Request) -> web.Response:
    token = request.match_info.get("token")

    # ── Token check only — no expiry ──
    if token not in file_store:
        return web.Response(status=404, text="Link not found.")

    entry     = file_store[token]
    chat_id   = entry["chat_id"]
    msg_id    = entry["msg_id"]
    file_name = entry["file_name"]
    file_size = entry["file_size"]

    range_header = request.headers.get("Range")
    offset       = 0
    end_byte     = file_size - 1 if file_size else None

    if range_header and file_size:
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            offset   = int(match.group(1))
            end_byte = int(match.group(2)) if match.group(2) else file_size - 1

    is_download = "download" in request.query

    if is_download:
        disposition  = f'attachment; filename="{file_name}"'
        content_type = "application/octet-stream"
    else:
        disposition = f'inline; filename="{file_name}"'
        ext = file_name.lower().split(".")[-1]
        content_types = {
            "mp4":  "video/mp4",
            "mkv":  "video/x-matroska",
            "avi":  "video/x-msvideo",
            "mov":  "video/quicktime",
            "mp3":  "audio/mpeg",
            "m4a":  "audio/mp4",
            "pdf":  "application/pdf",
            "jpg":  "image/jpeg",
            "jpeg": "image/jpeg",
            "png":  "image/png",
        }
        content_type = content_types.get(ext, "video/mp4")

    headers = {
        "Content-Disposition": disposition,
        "Content-Type":        content_type,
        "Accept-Ranges":       "bytes",
    }

    if file_size:
        content_length = end_byte - offset + 1
        headers["Content-Length"] = str(content_length)

    status = 206 if range_header else 200
    if range_header and file_size:
        headers["Content-Range"] = (
            f"bytes {offset}-{end_byte}/{file_size}"
        )

    # ── StreamResponse sends chunks immediately ────────
    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    try:
        client = await get_client()
        tl_msg = await client.get_messages(chat_id, ids=msg_id)

        if tl_msg is None:
            logger.error(
                f"❌ Message not found: chat={chat_id} msg={msg_id}"
            )
            return response

        async for chunk in client.iter_download(
            tl_msg,
            offset=offset,
            chunk_size=512 * 1024,
            request_size=512 * 1024,
        ):
            await response.write(chunk)

    except ConnectionResetError:
        logger.info("ℹ️ Client disconnected mid-stream")
    except Exception as e:
        logger.error(f"❌ Stream error: {e}", exc_info=True)

    return response


async def index_handler(request: web.Request) -> web.Response:
    return web.Response(text="✅ Bot stream server is running.")


# ── Process Batch ──────────────────────────────────────
async def process_batch(
    user_id: int,
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE
):
    await asyncio.sleep(BATCH_WAIT_SECONDS)

    if user_id not in batch_store:
        return

    messages = batch_store[user_id]["messages"]
    del batch_store[user_id]

    if not messages:
        return

    total = len(messages)
    logger.info(f"📦 Processing batch of {total} file(s)")

    status_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"⏳ Processing *{total}* file(s)...",
        parse_mode="Markdown"
    )

    download_lines = []
    stream_lines   = []
    total_size_mb  = 0
    failed         = []

    for i, message in enumerate(messages, 1):
        try:
            if message.video:
                file_name = message.video.file_name or f"video_{i}.mp4"
                file_size = message.video.file_size or 0
            elif message.document:
                file_name = message.document.file_name or f"file_{i}"
                file_size = message.document.file_size or 0
            elif message.audio:
                file_name = message.audio.file_name or f"audio_{i}.mp3"
                file_size = message.audio.file_size or 0
            else:
                continue

            file_size_mb   = file_size / (1024 * 1024)
            total_size_mb += file_size_mb

            token = generate_token()

            # ── No expiry — store permanently ──
            file_store[token] = {
                "chat_id":   chat_id,
                "msg_id":    message.message_id,
                "file_name": file_name,
                "file_size": file_size,
            }

            download_url = f"{BASE_URL}/stream/{token}?download=1"
            stream_url   = f"{BASE_URL}/stream/{token}"

            download_lines.append(
                f"⬇️ *File {i}* : `{file_name}`\n"
                f"🔗 {download_url}"
            )
            stream_lines.append(
                f"▶️ *File {i}* : `{file_name}` — "
                f"[Stream Now 🎬]({stream_url})"
            
