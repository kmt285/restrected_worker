import os
import re
import time
import asyncio
from pymongo import AsyncMongoClient
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")

USER_ID = int(os.environ.get("USER_ID", "0"))
CHAT_ID = int(os.environ.get("CHAT_ID", "0"))
STATUS_MSG_ID = int(os.environ.get("STATUS_MSG_ID", "0"))
POST_LINK = os.environ.get("POST_LINK", "")

LINK_REGEX = r"https?://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))/(?:\d+/)?(\d+)(?:-(\d+))?"

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

async def progress_bar(current, total, bot_client, status_msg_id, prefix_text, last_update):
    now = time.time()
    if now - last_update[0] < 6 and current != total:
        return
    last_update[0] = now
    percentage = current * 100 / total if total else 0
    mb_curr = current / (1024 * 1024)
    mb_tot = total / (1024 * 1024)
    try:
        await bot_client.edit_message(
            CHAT_ID,
            status_msg_id,
            f"{prefix_text}\n📊 **Progress:** `{percentage:.1f}%` (`{mb_curr:.1f} MB` / `{mb_tot:.1f} MB`)"
        )
    except Exception:
        pass

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

    os.makedirs("downloads", exist_ok=True)

    try:
        total_posts = len(msg_ids)
        for idx, msg_id in enumerate(msg_ids, start=1):
            step_label = f"[{idx}/{total_posts}]" if total_posts > 1 else ""
            msg = await user_client.get_messages(peer, ids=msg_id)

            if not msg:
                await bot_client.send_message(CHAT_ID, f"⚠️ {step_label} ပို့စ်နံပါတ် `{msg_id}` ကို ရှာမတွေ့ပါ။")
                continue

            if not msg.media:
                if msg.text:
                    await bot_client.send_message(CHAT_ID, msg.text, formatting_entities=msg.entities)
                continue

            last_time = [0.0]
            await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, f"📥 {step_label} **ဖိုင်ကို ဒေါင်းလုဒ်ဆွဲနေပါသည်...**")

            file_path = await user_client.download_media(
                msg,
                file="downloads/",
                progress_callback=lambda c, t: asyncio.ensure_future(
                    progress_bar(c, t, bot_client, STATUS_MSG_ID, f"📥 {step_label} **ဒေါင်းလုဒ်ဆွဲနေပါသည်...**", last_time)
                )
            )

            thumb_path = None
            if msg.document and msg.document.thumbs:
                try:
                    thumb_path = await user_client.download_media(msg, thumb=-1, file="downloads/thumb.jpg")
                except Exception:
                    thumb_path = None

            attributes = []
            supports_streaming = False
            if msg.document and msg.document.attributes:
                for attr in msg.document.attributes:
                    if isinstance(attr, DocumentAttributeVideo):
                        supports_streaming = True
                        attributes.append(DocumentAttributeVideo(
                            duration=attr.duration,
                            w=attr.w,
                            h=attr.h,
                            supports_streaming=True
                        ))
                    else:
                        attributes.append(attr)

            last_time = [0.0]
            await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, f"📤 {step_label} **Bot မှ သင့်ထံသို့ ပြန်ပို့နေပါသည်...**")

            await bot_client.send_file(
                CHAT_ID,
                file=file_path,
                caption=msg.text or "",
                formatting_entities=msg.entities,
                thumb=thumb_path,
                attributes=attributes if attributes else None,
                supports_streaming=supports_streaming,
                progress_callback=lambda c, t: asyncio.ensure_future(
                    progress_bar(c, t, bot_client, STATUS_MSG_ID, f"📤 {step_label} **Telegram သို့ တင်ပေးနေပါသည်...**", last_time)
                )
            )

            if file_path and os.path.exists(file_path):
                os.remove(file_path)
            if thumb_path and os.path.exists(thumb_path):
                os.remove(thumb_path)

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
