import io
import os
import re
import math
import time
import asyncio
from pymongo import AsyncMongoClient
from telethon import TelegramClient, helpers, utils
from telethon.sessions import StringSession
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest, InitConnectionRequest
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest, SaveFilePartRequest, SaveBigFilePartRequest
from telethon.tl.types import (
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputMediaUploadedDocument,
    DocumentAttributeVideo,
    DocumentAttributeFilename
)
from telethon.errors import FloodWaitError

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")

USER_ID = int(os.environ.get("USER_ID", "0"))
CHAT_ID = int(os.environ.get("CHAT_ID", "0"))
STATUS_MSG_ID = int(os.environ.get("STATUS_MSG_ID", "0"))
POST_LINK = os.environ.get("POST_LINK", "")

LINK_REGEX = r"https?://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))/(?:\d+/)?(\d+)(?:-(\d+))?"

# 512 KB (Telegram MTProto ၏ အမြင့်ဆုံး Chunk Size)
PART_SIZE = 512 * 1024
PARALLEL_WORKERS = 8

def parse_link(link: str):
    match = re.search(LINK_REGEX, link)
    if not match:
        return None, []
    
    private_id, public_username, start_id, end_id = match.groups()
    peer = int(f"-100{private_id}") if private_id else public_username

    start_msg = int(start_id)
    end_msg = int(end_id) if end_id else start_msg

    if end_msg < start_msg:
        end_msg = start_msg
    if end_msg - start_msg > 50:
        end_msg = start_msg + 50

    return peer, list(range(start_msg, end_msg + 1))

# ==========================================
# PARALLEL MTPROTO SENDER POOL
# ==========================================
async def create_sender_pool(client: TelegramClient, dc_id: int, count: int):
    """Telegram Data Center သို့ ပြိုင်တူ MTProto Connection များ တည်ဆောက်ခြင်း"""
    dc = await client._get_dc(dc_id)
    senders = []
    auth_key = None

    if dc_id == client.session.dc_id:
        auth_key = client.session.auth_key
    else:
        first_sender = MTProtoSender(None, loggers=client._log)
        await first_sender.connect(
            client._connection(
                dc.ip_address, dc.port, dc.id,
                loggers=client._log, proxy=client._proxy
            )
        )
        auth = await client(ExportAuthorizationRequest(dc_id))
        client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
        await first_sender.send(InvokeWithLayerRequest(LAYER, client._init_request))
        auth_key = first_sender.auth_key
        senders.append(first_sender)

    remaining = count - len(senders)
    for _ in range(remaining):
        sender = MTProtoSender(auth_key, loggers=client._log)
        await sender.connect(
            client._connection(
                dc.ip_address, dc.port, dc.id,
                loggers=client._log, proxy=client._proxy
            )
        )
        client._init_request.query = GetConfigRequest()
        await sender.send(InvokeWithLayerRequest(LAYER, client._init_request))
        senders.append(sender)

    return senders

async def close_sender_pool(senders):
    for s in senders:
        try:
            await s.disconnect()
        except Exception:
            pass

# ==========================================
# ZERO-DISK PARALLEL RAM STREAMING ENGINE
# ==========================================
async def parallel_ram_stream_document(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    document,
    file_name: str,
    step_label: str
):
    """
    Hard Disk လုံးဝမသုံးဘဲ Userbot မှ 512KB စီ ပြိုင်တူဆွဲထုတ်ပြီး
    RAM ပေါ်မှတစ်ဆင့် Bot ဆီသို့ တစ်ပြိုင်နက်တည်း တိုက်ရိုက်ပို့ဆောင်ပေးသည့် စနစ်
    """
    file_size = document.size
    total_parts = math.ceil(file_size / PART_SIZE)
    file_id = helpers.generate_random_long()
    is_big = file_size > 10 * 1024 * 1024

    location = InputDocumentFileLocation(
        id=document.id,
        access_hash=document.access_hash,
        file_reference=document.file_reference,
        thumb_size=""
    )

    worker_count = min(PARALLEL_WORKERS, total_parts)
    dl_senders = await create_sender_pool(user_client, document.dc_id, worker_count)
    ul_senders = await create_sender_pool(bot_client, bot_client.session.dc_id, worker_count)

    queue = asyncio.Queue()
    for part_idx in range(total_parts):
        queue.put_nowait(part_idx)

    transferred_bytes = 0
    start_time = time.time()
    last_edit_time = [0.0]

    async def stream_worker(dl_sender: MTProtoSender, ul_sender: MTProtoSender):
        nonlocal transferred_bytes
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * PART_SIZE
            expected_len = min(PART_SIZE, file_size - offset)

            # ၁။ 512KB Chunk ကို RAM ထဲသို့ ဒေါင်းလုဒ်ဆွဲခြင်း (Retry စနစ်ပါဝင်သည်)
            chunk_data = None
            for attempt in range(4):
                try:
                    buf = bytearray()
                    curr_off = offset
                    while len(buf) < expected_len:
                        res = await dl_sender.send(
                            GetFileRequest(location, offset=curr_off, limit=PART_SIZE)
                        )
                        if not res.bytes:
                            break
                        buf.extend(res.bytes)
                        curr_off += len(res.bytes)
                    chunk_data = bytes(buf[:expected_len])
                    break
                except FloodWaitError as fw:
                    await asyncio.sleep(fw.seconds + 1)
                except Exception:
                    if attempt == 3:
                        raise
                    await asyncio.sleep(1)

            # ၂။ ရလာသော 512KB Chunk ကို Bot မှ Telegram သို့ ချက်ချင်း တန်းတင်ခြင်း
            for attempt in range(4):
                try:
                    if is_big:
                        await ul_sender.send(
                            SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk_data)
                        )
                    else:
                        await ul_sender.send(
                            SaveFilePartRequest(file_id, part_idx, chunk_data)
                        )
                    break
                except FloodWaitError as fw:
                    await asyncio.sleep(fw.seconds + 1)
                except Exception:
                    if attempt == 3:
                        raise
                    await asyncio.sleep(1)

            transferred_bytes += len(chunk_data)

            # Progress Bar ကို ၅ စက္ကန့်လျှင် တစ်ကြိမ်သာ Update လုပ်ခြင်း
            now = time.time()
            if now - last_edit_time[0] >= 5.0:
                last_edit_time[0] = now
                elapsed = max(now - start_time, 0.1)
                speed_mb = (transferred_bytes / (1024 * 1024)) / elapsed
                pct = (transferred_bytes * 100) / file_size
                curr_mb = transferred_bytes / (1024 * 1024)
                tot_mb = file_size / (1024 * 1024)
                try:
                    await bot_client.edit_message(
                        CHAT_ID,
                        STATUS_MSG_ID,
                        f"⚡ {step_label} **အမြန်နှုန်းမြင့် စနစ်ဖြင့် လွှဲပြောင်းနေပါသည်...**\n"
                        f"📊 **Progress:** `{pct:.1f}%` (`{curr_mb:.1f} MB` / `{tot_mb:.1f} MB`)\n"
                        f"🚀 **Speed:** `{speed_mb:.1f} MB/s`"
                    )
                except Exception:
                    pass

            queue.task_done()

    try:
        tasks = [
            asyncio.create_task(stream_worker(dl_senders[i], ul_senders[i]))
            for i in range(worker_count)
        ]
        await asyncio.gather(*tasks)
    finally:
        await close_sender_pool(dl_senders)
        await close_sender_pool(ul_senders)

    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=file_name)
    return InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum="")

# ==========================================
# MAIN CLONING WORKFLOW
# ==========================================
async def process_cloning():
    bot_client = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)

    mongo = AsyncMongoClient(MONGO_URI)
    user_doc = await mongo["restricted_saver_db"]["users"].find_one({"user_id": USER_ID})

    if not user_doc:
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **Session မရှိတော့ပါ။** `/start` နှိပ်ပြီး ပြန်လည် Login ဝင်ပါ။")
        await bot_client.disconnect()
        await mongo.close()
        return

    peer, msg_ids = parse_link(POST_LINK)
    if not peer:
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **Link ပုံစံ မှားယွင်းနေပါသည်။**")
        await bot_client.disconnect()
        await mongo.close()
        return

    user_client = TelegramClient(StringSession(user_doc["session_string"]), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **သင်၏ Session သက်တမ်းကုန်သွားပါပြီ။** `/start` နှိပ်ပြီး ပြန်ဝင်ပါ။")
        await user_client.disconnect()
        await bot_client.disconnect()
        await mongo.close()
        return

    try:
        total_posts = len(msg_ids)
        for idx, msg_id in enumerate(msg_ids, start=1):
            step_label = f"[{idx}/{total_posts}]" if total_posts > 1 else ""
            msg = await user_client.get_messages(peer, ids=msg_id)

            if not msg:
                await bot_client.send_message(CHAT_ID, f"⚠️ {step_label} ပို့စ်နံပါတ် `{msg_id}` ကို ရှာမတွေ့ပါ။")
                continue

            # ၁။ စာသားသက်သက် ဖြစ်ပါက တိုက်ရိုက်ပို့မည်
            if not msg.media:
                if msg.text:
                    await bot_client.send_message(CHAT_ID, msg.text, formatting_entities=msg.entities)
                continue

            # ၂။ ပုံအသေး (Photo) သို့မဟုတ် 2MB အောက် ဖိုင်အသေးများကို RAM မှ တိုက်ရိုက်ပို့မည်
            if not msg.document or msg.document.size < 2 * 1024 * 1024:
                await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, f"⚡ {step_label} **ဖိုင်အသေးကို လွှဲပြောင်းနေပါသည်...**")
                ram_file = io.BytesIO()
                await user_client.download_media(msg, file=ram_file)
                ram_file.seek(0)
                ram_file.name = getattr(msg.file, "name", None) or f"file_{msg_id}" + (getattr(msg.file, "ext", "") or ".jpg")
                await bot_client.send_file(
                    CHAT_ID,
                    file=ram_file,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                continue

            # ၃။ 2MB အထက် ဖိုင်ကြီးများ (1.5GB/2GB Videos & Files) ကို Parallel RAM Streaming ဖြင့် ပို့မည်
            doc = msg.document
            file_name = getattr(msg.file, "name", None) or f"video_{msg_id}{getattr(msg.file, 'ext', '') or '.mp4'}"

            await bot_client.edit_message(
                CHAT_ID,
                STATUS_MSG_ID,
                f"🚀 {step_label} **Parallel High-Speed Engine စတင်နေပါသည်...**\n"
                f"📦 **ဖိုင်အရွယ်အစား:** `{doc.size / (1024 * 1024):.1f} MB`"
            )

            # Thumbnail ကို RAM ထဲသို့ ဒေါင်းလုဒ်ဆွဲပြီး တင်ထားခြင်း
            uploaded_thumb = None
            if doc.thumbs:
                try:
                    thumb_buf = io.BytesIO()
                    await user_client.download_media(msg, thumb=-1, file=thumb_buf)
                    thumb_buf.seek(0)
                    thumb_buf.name = "thumb.jpg"
                    uploaded_thumb = await bot_client.upload_file(thumb_buf)
                except Exception:
                    uploaded_thumb = None

            # Video Streaming Attributes များ မူရင်းအတိုင်း ထိန်းသိမ်းခြင်း
            attributes = []
            has_filename = False
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    attributes.append(DocumentAttributeVideo(
                        duration=attr.duration,
                        w=attr.w,
                        h=attr.h,
                        supports_streaming=True
                    ))
                elif isinstance(attr, DocumentAttributeFilename):
                    has_filename = True
                    attributes.append(attr)
                else:
                    attributes.append(attr)

            if not has_filename:
                attributes.append(DocumentAttributeFilename(file_name=file_name))

            # Parallel RAM Stream စတင်မောင်းနှင်ခြင်း
            input_file = await parallel_ram_stream_document(
                user_client=user_client,
                bot_client=bot_client,
                document=doc,
                file_name=file_name,
                step_label=step_label
            )

            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=doc.mime_type or "application/octet-stream",
                attributes=attributes,
                thumb=uploaded_thumb,
                force_file=False
            )

            await bot_client.send_file(
                CHAT_ID,
                file=media,
                caption=msg.text or "",
                formatting_entities=msg.entities
            )

            if total_posts > 1:
                await asyncio.sleep(2)

        await bot_client.delete_messages(CHAT_ID, STATUS_MSG_ID)

    except Exception as e:
        await bot_client.edit_message(
            CHAT_ID,
            STATUS_MSG_ID,
            f"❌ **အမှားဖြစ်ပေါ်နေပါသည်:** `{str(e)}`"
        )
    finally:
        await user_client.disconnect()
        await bot_client.disconnect()
        await mongo.close()

if __name__ == "__main__":
    asyncio.run(process_cloning())
