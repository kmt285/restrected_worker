import os
import re
import time
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo

# Secrets & Payload Variables
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
    
    # Private Channel ID ဖြစ်ပါက -100 ပေါင်းထည့်ပေးရသည်
    if private_id:
        peer = int(f"-100{private_id}")
    else:
        peer = public_username

    start_msg = int(start_id)
    end_msg = int(end_id) if end_id else start_msg

    # တစ်ခါဆွဲလျှင် အများဆုံး ပို့စ် ၅၀ အထိ ခွင့်ပြုမည် (FloodWait ကာကွယ်ရန်)
    if end_msg < start_msg:
        end_msg = start_msg
    if end_msg - start_msg > 50:
        end_msg = start_msg + 50

    return peer, list(range(start_msg, end_msg + 1))

async def progress_bar(current, total, bot_client, status_msg_id, prefix_text, last_update):
    now = time.time()
    # Telegram Rate Limit မထိစေရန် ၆ စက္ကန့်လျှင် တစ်ကြိမ်သာ Progress ကို Edit လုပ်မည်
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
    # ၁။ Bot Client စတင်ခြင်း
    bot_client = TelegramClient('worker_bot', API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)

    # ၂။ MongoDB မှ User Session ယူခြင်း
    mongo = AsyncIOMotorClient(MONGO_URI)
    user_doc = await mongo["restricted_saver_db"]["users"].find_one({"user_id": USER_ID})

    if not user_doc:
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **Session မရှိတော့ပါ။** `/start` နှိပ်ပြီး ပြန်လည် Login ဝင်ပါ။")
        await bot_client.disconnect()
        return

    peer, msg_ids = parse_link(POST_LINK)
    if not peer:
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **Link ပုံစံ မှားယွင်းနေပါသည်။**")
        await bot_client.disconnect()
        return

    # ၃။ Userbot Client စတင်ခြင်း
    user_client = TelegramClient(StringSession(user_doc["session_string"]), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **သင်၏ Session သက်တမ်းကုန်သွားပါပြီ။** `/start` နှိပ်ပြီး ပြန်ဝင်ပါ။")
        await user_client.disconnect()
        await bot_client.disconnect()
        return

    os.makedirs("downloads", exist_ok=True)

    try:
        total_posts = len(msg_ids)
        for idx, msg_id in enumerate(msg_ids, start=1):
            step_label = f"[{idx}/{total_posts}]" if total_posts > 1 else ""
            msg = await user_client.get_messages(peer, ids=msg_id)

            if not msg:
                await bot_client.send_message(CHAT_ID, f"⚠️ {step_label} ပို့စ်နံပါတ် `{msg_id}` ကို ရှာမတွေ့ပါ (သို့မဟုတ် ဖျက်ထားပါသည်)။")
                continue

            # စာသားသက်သက် ဖြစ်ပါက တိုက်ရိုက်ပို့ပေးမည်
            if not msg.media:
                if msg.text:
                    await bot_client.send_message(
                        CHAT_ID,
                        msg.text,
                        formatting_entities=msg.entities
                    )
                continue

            # Media ပါဝင်ပါက ဒေါင်းလုဒ်ဆွဲမည်
            last_time = [0.0]
            await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, f"📥 {step_label} **Restricted ဖိုင်ကို ဒေါင်းလုဒ်ဆွဲနေပါသည်...**")

            file_path = await user_client.download_media(
                msg,
                file="downloads/",
                progress_callback=lambda c, t: asyncio.ensure_future(
                    progress_bar(c, t, bot_client, STATUS_MSG_ID, f"📥 {step_label} **ဒေါင်းလုဒ်ဆွဲနေပါသည်...**", last_time)
                )
            )

            # ဗီဒီယို Thumbnail ကိုပါ ဒေါင်းလုဒ်ဆွဲခြင်း
            thumb_path = None
            if msg.document and msg.document.thumbs:
                try:
                    thumb_path = await user_client.download_media(msg, thumb=-1, file="downloads/thumb.jpg")
                except Exception:
                    thumb_path = None

            # ဗီဒီယိုဖြစ်ပါက Streaming ကြည့်ရှုနိုင်ရန် Attribute စစ်ဆေးခြင်း
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

            # Bot မှတစ်ဆင့် User ထံသို့ Non-Restricted အဖြစ် ပြန်တင်ပေးခြင်း
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

            # Server Disk မပြည့်စေရန် ဖိုင်ပြန်ဖျက်ခြင်း
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
            f"❌ **ဖိုင်ဆွဲထုတ်ရာတွင် အမှားဖြစ်ပေါ်နေပါသည်:**\n`{str(e)}`\n\n*(သင်၏ အကောင့်သည် ထို Channel/Group ထဲသို့ ဝင်ထားပြီးသား ဖြစ်ရပါမည်)*"
        )
    finally:
        await user_client.disconnect()
        await bot_client.disconnect()

if __name__ == "__main__":
    asyncio.run(process_cloning())
