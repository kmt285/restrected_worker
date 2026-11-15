import io
import os
import math
import time
import asyncio
from datetime import datetime, timezone

# Linux ပေါ်တွင် Cython uvloop ဖြင့် Event Loop အမြန်နှုန်း မြှင့်တင်ခြင်း
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from pymongo import AsyncMongoClient
from telethon import TelegramClient, helpers, utils
from telethon.sessions import StringSession
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest, SaveFilePartRequest, SaveBigFilePartRequest
from telethon.tl.types import (
    PeerChannel,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputMediaUploadedDocument,
    DocumentAttributeVideo,
    DocumentAttributeFilename
)
from telethon.errors import FloodWaitError

# ==========================================
# ENVIRONMENT & PAYLOAD VARIABLES
# ==========================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
DUMP_CHANNEL_ID = int(os.environ.get("DUMP_CHANNEL_ID", "0"))

USER_ID = int(os.environ.get("USER_ID", "0"))
CHAT_ID = int(os.environ.get("CHAT_ID", "0"))
STATUS_MSG_ID = int(os.environ.get("STATUS_MSG_ID", "0"))
PEER_ID = os.environ.get("PEER_ID", "")
MSG_IDS_RAW = os.environ.get("MSG_IDS", "")
MAX_MB = int(os.environ.get("MAX_MB", "500"))
WORKERS = int(os.environ.get("WORKERS", "8"))
SOURCE_LINK = os.environ.get("SOURCE_LINK", "")

PART_SIZE = 512 * 1024  # 512 KB

async def resolve_dump_channel(bot: TelegramClient):
    if not DUMP_CHANNEL_ID:
        return None
    try:
        return await bot.get_input_entity(DUMP_CHANNEL_ID)
    except Exception:
        raw_id = int(str(abs(DUMP_CHANNEL_ID))[3:]) if str(abs(DUMP_CHANNEL_ID)).startswith("100") else abs(DUMP_CHANNEL_ID)
        return await bot.get_input_entity(PeerChannel(raw_id))

# ==========================================
# PARALLEL MTPROTO SENDER POOL
# ==========================================
async def create_sender_pool(client: TelegramClient, dc_id: int, count: int):
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
# DECOUPLED PRODUCER-CONSUMER TURBO ENGINE
# ==========================================
async def decoupled_parallel_stream(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    document,
    file_name: str,
    step_label: str,
    worker_count: int
):
    """
    Download Senders (Producers) နှင့် Upload Senders (Consumers) ကို သီးခြားခွဲထုတ်ပြီး
    RAM Queue မှတစ်ဆင့် စောင့်ဆိုင်းချိန်မရှိဘဲ အမြန်ဆုံး လွှဲပြောင်းပေးသည့် စနစ်
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

    n_workers = min(worker_count, total_parts)
    dl_senders, ul_senders = await asyncio.gather(
        create_sender_pool(user_client, document.dc_id, n_workers),
        create_sender_pool(bot_client, bot_client.session.dc_id, n_workers)
    )

    part_queue = asyncio.Queue()
    for part_idx in range(total_parts):
        part_queue.put_nowait(part_idx)

    # Bounded RAM Buffer (RAM အသုံးပြုမှု 30 MB ထက် မကျော်စေရန် ထိန်းချုပ်ထားသည်)
    chunk_queue = asyncio.Queue(maxsize=n_workers * 3)

    uploaded_bytes = 0
    start_time = time.time()
    last_edit_time = [0.0]

    async def dl_producer(dl_sender: MTProtoSender):
        while not part_queue.empty():
            try:
                part_idx = part_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            offset = part_idx * PART_SIZE
            expected_len = min(PART_SIZE, file_size - offset)
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

            await chunk_queue.put((part_idx, chunk_data))
            part_queue.task_done()

    async def ul_consumer(ul_sender: MTProtoSender):
        nonlocal uploaded_bytes
        while True:
            item = await chunk_queue.get()
            if item is None:
                chunk_queue.task_done()
                break

            part_idx, chunk_data = item
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

            uploaded_bytes += len(chunk_data)
            chunk_queue.task_done()

            now = time.time()
            if now - last_edit_time[0] >= 4.5:
                last_edit_time[0] = now
                elapsed = max(now - start_time, 0.1)
                speed_mb = (uploaded_bytes / (1024 * 1024)) / elapsed
                pct = (uploaded_bytes * 100) / file_size
                curr_mb = uploaded_bytes / (1024 * 1024)
                tot_mb = file_size / (1024 * 1024)
                try:
                    await bot_client.edit_message(
                        CHAT_ID,
                        STATUS_MSG_ID,
                        f"⚡ {step_label} **Decoupled Turbo Engine ({n_workers}x Workers)**\n"
                        f"📊 **Progress:** `{pct:.1f}%` (`{curr_mb:.1f} MB` / `{tot_mb:.1f} MB`)\n"
                        f"🚀 **Speed:** `{speed_mb:.2f} MB/s`"
                    )
                except Exception:
                    pass

    try:
        producers = [asyncio.create_task(dl_producer(s)) for s in dl_senders]
        consumers = [asyncio.create_task(ul_consumer(s)) for s in ul_senders]

        await asyncio.gather(*producers)
        for _ in range(n_workers):
            await chunk_queue.put(None)
        await asyncio.gather(*consumers)
    finally:
        await asyncio.gather(
            close_sender_pool(dl_senders),
            close_sender_pool(ul_senders)
        )

    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=file_name)
    return InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum="")

# ==========================================
# BACKUP CHANNEL & MONGODB FILE_ID SAVER
# ==========================================
async def backup_and_cache_post(
    bot_client: TelegramClient,
    dump_entity,
    cache_col,
    sent_msg,
    peer_key: str,
    msg_id: int,
    file_name: str,
    file_size: int,
    mime_type: str
):
    """
    User ထံ ပို့ပြီးသား မက်ဆေ့ချ်ကို Private Backup Channel သို့ Copy ပွားပြီး
    Telegram file_id နှင့် dump_msg_id ကို MongoDB တွင် သိမ်းဆည်းခြင်း (0 Extra Upload Bandwidth)
    """
    if not dump_entity or not sent_msg:
        return

    try:
        # ၁။ မူရင်း မက်ဆေ့ချ်အတိုင်း Backup Channel ထဲသို့ Copy ပို့ခြင်း
        dump_msg = await bot_client.send_message(dump_entity, sent_msg)

        # ၂။ Packed Telegram File ID ထုတ်ယူခြင်း
        packed_file_id = ""
        if dump_msg.media:
            try:
                packed_file_id = utils.pack_bot_file_id(dump_msg.media)
            except Exception:
                packed_file_id = ""

        # ၃။ Backup Channel တွင် Admin ကြည့်ရှုနိုင်ရန် Audit Info ကို Reply ပြန်ထားခြင်း
        await bot_client.send_message(
            dump_entity,
            f"🗂️ **Backup Audit Log**\n"
            f"👤 **User ID:** `{USER_ID}`\n"
            f"🔗 **Source:** `{SOURCE_LINK}` (Post `{msg_id}`)\n"
            f"📦 **Size:** `{file_size / (1024 * 1024):.2f} MB`\n"
            f"🆔 **File ID:** `{packed_file_id or 'Text/Other'}`",
            reply_to=dump_msg.id
        )

        # ၄။ MongoDB file_cache တွင် သိမ်းဆည်းခြင်း
        cache_key = f"{peer_key}:{msg_id}"
        await cache_col.update_one(
            {"cache_key": cache_key},
            {"$set": {
                "cache_key": cache_key,
                "peer_id": peer_key,
                "msg_id": msg_id,
                "dump_msg_id": dump_msg.id,
                "file_id": packed_file_id,
                "file_name": file_name,
                "file_size": file_size,
                "mime_type": mime_type,
                "is_media": bool(dump_msg.media),
                "cached_by": USER_ID,
                "cached_at": time.time()
            }},
            upsert=True
        )
    except Exception as e:
        print(f"Backup & Cache Error: {e}")

# ==========================================
# MAIN CLONING WORKFLOW
# ==========================================
async def process_cloning():
    bot_client = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)

    mongo = AsyncMongoClient(MONGO_URI)
    db = mongo["restricted_saver_db"]
    users_col = db["users"]
    cache_col = db["file_cache"]

    user_doc = await users_col.find_one({"user_id": USER_ID})
    if not user_doc or not user_doc.get("session_string"):
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **Session မရှိတော့ပါ။** `/start` နှိပ်ပြီး ပြန်လည် Login ဝင်ပါ။")
        await bot_client.disconnect()
        await mongo.close()
        return

    peer = int(PEER_ID) if PEER_ID.lstrip("-").isdigit() else PEER_ID
    msg_ids = [int(x) for x in MSG_IDS_RAW.split(",") if x.strip().isdigit()]

    user_client = TelegramClient(StringSession(user_doc["session_string"]), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **သင်၏ Session သက်တမ်းကုန်သွားပါပြီ။** `/start` နှိပ်ပြီး ပြန်ဝင်ပါ။")
        await user_client.disconnect()
        await bot_client.disconnect()
        await mongo.close()
        return

    dump_entity = await resolve_dump_channel(bot_client)
    max_allowed_bytes = MAX_MB * 1024 * 1024

    completed_files = 0
    completed_bytes = 0

    try:
        total_posts = len(msg_ids)
        for idx, msg_id in enumerate(msg_ids, start=1):
            step_label = f"[{idx}/{total_posts}]" if total_posts > 1 else ""
            msg = await user_client.get_messages(peer, ids=msg_id)

            if not msg:
                await bot_client.send_message(CHAT_ID, f"⚠️ {step_label} ပို့စ်နံပါတ် `{msg_id}` ကို ရှာမတွေ့ပါ။")
                continue

            # ၁။ စာသားသက်သက် ဖြစ်ပါက
            if not msg.media:
                if msg.text:
                    sent_msg = await bot_client.send_message(CHAT_ID, msg.text, formatting_entities=msg.entities)
                    await backup_and_cache_post(bot_client, dump_entity, cache_col, sent_msg, PEER_ID, msg_id, "text", 0, "text/plain")
                    completed_files += 1
                continue

            # ဖိုင်အရွယ်အစား စစ်ဆေးခြင်း (Free vs VIP Max MB)
            f_size = getattr(msg.file, "size", 0) or 0
            if f_size > max_allowed_bytes:
                await bot_client.send_message(
                    CHAT_ID,
                    f"⚠️ {step_label} ပို့စ် `{msg_id}` ၏ ဖိုင်ဆိုဒ် (`{f_size / (1024 * 1024):.1f} MB`) သည် သင်၏ အမြင့်ဆုံး ကန့်သတ်ချက် (`{MAX_MB} MB`) ထက် ကျော်လွန်နေပါသည်။\n"
                    f"*(2GB အထိ ဒေါင်းလုဒ်ဆွဲလိုပါက 💎 VIP သို့ အဆင့်မြှင့်တင်ပါ)*"
                )
                continue

            # ၂။ ပုံအသေး သို့မဟုတ် 2MB အောက် ဖိုင်အသေးများ
            if not msg.document or f_size < 2 * 1024 * 1024:
                await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, f"⚡ {step_label} **ဖိုင်အသေးကို လွှဲပြောင်းနေပါသည်...**")
                ram_file = io.BytesIO()
                await user_client.download_media(msg, file=ram_file)
                ram_file.seek(0)
                f_name = getattr(msg.file, "name", None) or f"file_{msg_id}" + (getattr(msg.file, "ext", "") or ".jpg")
                ram_file.name = f_name

                sent_msg = await bot_client.send_file(
                    CHAT_ID,
                    file=ram_file,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                await backup_and_cache_post(
                    bot_client, dump_entity, cache_col, sent_msg,
                    PEER_ID, msg_id, f_name, f_size, getattr(msg.file, "mime_type", "image/jpeg")
                )
                completed_files += 1
                completed_bytes += f_size
                continue

            # ၃။ 2MB အထက် ဖိုင်ကြီးများ (Decoupled Producer-Consumer Turbo Engine)
            doc = msg.document
            file_name = getattr(msg.file, "name", None) or f"video_{msg_id}{getattr(msg.file, 'ext', '') or '.mp4'}"

            await bot_client.edit_message(
                CHAT_ID,
                STATUS_MSG_ID,
                f"🚀 {step_label} **Turbo Engine ({WORKERS}x Workers) စတင်နေပါသည်...**\n"
                f"📦 **ဖိုင်အရွယ်အစား:** `{doc.size / (1024 * 1024):.1f} MB`"
            )

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

            input_file = await decoupled_parallel_stream(
                user_client=user_client,
                bot_client=bot_client,
                document=doc,
                file_name=file_name,
                step_label=step_label,
                worker_count=WORKERS
            )

            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=doc.mime_type or "application/octet-stream",
                attributes=attributes,
                thumb=uploaded_thumb,
                force_file=False
            )

            sent_msg = await bot_client.send_file(
                CHAT_ID,
                file=media,
                caption=msg.text or "",
                formatting_entities=msg.entities
            )

            # Backup Channel သို့ Copy ပို့ပြီး file_id သိမ်းဆည်းခြင်း
            await backup_and_cache_post(
                bot_client, dump_entity, cache_col, sent_msg,
                PEER_ID, msg_id, file_name, doc.size, doc.mime_type or "video/mp4"
            )

            completed_files += 1
            completed_bytes += doc.size

            if total_posts > 1:
                await asyncio.sleep(1.5)

        # User ၏ ယနေ့သုံးစွဲမှုနှင့် စုစုပေါင်း သုံးစွဲမှု မှတ်တမ်းတင်ခြင်း
        if completed_files > 0:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            await users_col.update_one(
                {"user_id": USER_ID},
                {
                    "$inc": {
                        "daily_count": completed_files,
                        "daily_bytes": completed_bytes,
                        "total_files": completed_files,
                        "total_bytes": completed_bytes
                    },
                    "$set": {"daily_date": today}
                }
            )

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
