# ★CODING🧑‍💻BY:- @Hindu_papa✓
from dotenv import load_dotenv
load_dotenv()
import asyncio
import os
import re
import logging
import json # For broadcast user and group list
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated, PeerIdInvalid,
    ChatWriteForbidden, UserNotParticipant # Added for group broadcast errors
)
from pyrogram.enums import ChatType # For checking chat type

import yt_dlp

# --- Configuration ---
API_ID_STR = os.environ.get("API_ID") # Renamed to avoid conflict before int conversion
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_STR = os.environ.get("OWNER_ID")
STRING_SESSION = os.environ.get("STRING_SESSION") # ★★★ यह नई लाइन है सेशन स्ट्रिंग के लिए ★★★
# ★★★ नया ENV VAR इंस्टाग्राम कुकीज़ के कंटेंट के लिए ★★★
INSTAGRAM_COOKIES_CONTENT = os.environ.get("INSTAGRAM_COOKIES_CONTENT")

# Validate that essential environment variables are set
if not API_ID_STR:
    raise ValueError("Missing API_ID environment variable. Please set it.")
if not API_HASH:
    raise ValueError("Missing API_HASH environment variable. Please set it.")
# BOT_TOKEN is not strictly required if STRING_SESSION is present for a bot
if not BOT_TOKEN and not STRING_SESSION:
    raise ValueError("Missing BOT_TOKEN environment variable (and no STRING_SESSION provided). Please set at least one.")
if not OWNER_ID_STR:
    raise ValueError("Missing OWNER_ID environment variable. Please set it.")

try:
    API_ID = int(API_ID_STR) # Convert API_ID_STR to int
except ValueError:
    raise ValueError("API_ID environment variable must be an integer.")

try:
    OWNER_ID = int(OWNER_ID_STR)
except ValueError:
    raise ValueError("OWNER_ID environment variable must be an integer.")


DOWNLOAD_DIR = "./downloads"
USERS_FILE = "bot_users.json"
GROUPS_FILE = "bot_groups.json"
# ★★★ अस्थायी इंस्टाग्राम कुकी फ़ाइल का नाम ★★★
TEMP_INSTA_COOKIES_FILENAME = "temp_insta_cookies.txt"


if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("bot.log", mode='a', encoding='utf-8') # Railway पर यह उपयोगी नहीं
    ]
)
logger = logging.getLogger(__name__)

# ★★★ Pyrogram Client Initialization - UPDATED ★★★
app = None # Initialize app with None for now

if STRING_SESSION:
    logger.info("STRING_SESSION environment variable found. Attempting to initialize client with session string.")
    app = Client(
        name="media_bot_session",  # A name for the session context
        session_string=STRING_SESSION,
        api_id=API_ID,
        api_hash=API_HASH
        # bot_token is not needed here if STRING_SESSION is derived from that bot's token
    )
    logger.info("Client initialized using STRING_SESSION.")
elif BOT_TOKEN:
    logger.info("STRING_SESSION not found. Attempting to initialize client with BOT_TOKEN.")
    app = Client(
        name="media_bot_token", # A name for the session file that Pyrogram might create
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN
    )
    logger.info("Client initialized using BOT_TOKEN.")
else:
    logger.critical("CRITICAL ERROR: Neither STRING_SESSION nor BOT_TOKEN is available. Bot cannot start.")
    # Optional: raise an error or exit if you want the script to stop here
    # raise ValueError("Neither STRING_SESSION nor BOT_TOKEN is set. Bot cannot start.")

# --- Broadcast User & Group Management ---
def load_users():
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r') as f:
                return set(json.load(f))
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {USERS_FILE}. Initializing with empty set.")
            return set()
    return set()

def save_users(users_set):
    try:
        with open(USERS_FILE, 'w') as f:
            json.dump(list(users_set), f)
    except Exception as e:
        logger.error(f"Error saving users to {USERS_FILE}: {e}")

subscribed_users = load_users()

def load_groups():
    if os.path.exists(GROUPS_FILE):
        try:
            with open(GROUPS_FILE, 'r') as f:
                return set(json.load(f))
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {GROUPS_FILE}. Initializing with empty set.")
            return set()
    return set()

def save_groups(groups_set):
    try:
        with open(GROUPS_FILE, 'w') as f:
            json.dump(list(groups_set), f)
    except Exception as e:
        logger.error(f"Error saving groups to {GROUPS_FILE}: {e}")

active_groups = load_groups()

# --- Helper function to add chat to relevant list ---
async def add_chat_to_tracking(chat_id: int, chat_type: ChatType):
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        if chat_id not in active_groups:
            active_groups.add(chat_id)
            save_groups(active_groups)
            logger.info(f"New group chat {chat_id} added to broadcast list. Total groups: {len(active_groups)}")

# --- Constants & Buttons ---
WHOAMI_BTN = InlineKeyboardButton("WHO I AM? 👤", url="https://t.me/MeNetwork108/14")
CREATOR_BTN = InlineKeyboardButton("Made By ❤️", url="https://t.me/Hindu_papa")

TARGET_EXTRACTORS = ['youtube', 'instagram', 'facebook', 'fb'] # ★★★ 'youtu.be' हटाया गया ★★★

# --- Helper Functions ---
def clean_filename(name: str) -> str:
    name = str(name)
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = name.strip().replace(" ", "_")
    return name[:200]

async def run_ffmpeg(command: list) -> bool:
    logger.info(f"Running FFmpeg command: {' '.join(command)}")
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        logger.error(f"FFmpeg Error (Code {process.returncode}): {stderr.decode(errors='ignore').strip()}")
        logger.info(f"FFmpeg stdout (on error): {stdout.decode(errors='ignore').strip()[:500]}")
        return False
    else:
        logger.info(f"FFmpeg Success! stdout: {stdout.decode(errors='ignore').strip()[:500]}")
        logger.info(f"FFmpeg stderr (on success, if any): {stderr.decode(errors='ignore').strip()[:500]}")
        return True

user_interaction_states = {}

@app.on_message(filters.command("start"))
async def start_command(client: Client, msg: Message):
    user_id = msg.from_user.id
    chat_id = msg.chat.id
    logger.info(f"Received /start command from user {user_id} ({msg.from_user.first_name}) in chat {chat_id} (type: {msg.chat.type})")
    await add_chat_to_tracking(chat_id, msg.chat.type)
    if msg.chat.type == ChatType.PRIVATE:
        if user_id not in subscribed_users:
            subscribed_users.add(user_id)
            save_users(subscribed_users)
            logger.info(f"New user {user_id} added to subscribed list for broadcasts. Total users: {len(subscribed_users)}")
    start_text = (
        f"🛸 **Welcome, {msg.from_user.mention}!**\n\n"
        "⚡ I'm a multi-platform Media Extractor Bot.\n"
        "I can attempt to download from **YouTube, Instagram, Facebook**.\n"
        "Just send me a link (works in private chat or groups!).\n\n"
        "**I Can Extract:** Video 🎥 | Audio 🔊 | Both 🎥+🔊\n\n"
        "👤 **Creator:-** @Hindu_papa ✓\n"
    )
    reply_markup = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])
    try:
        await msg.reply_text(start_text, reply_markup=reply_markup, quote=True)
        logger.info(f"Successfully replied to /start for user {user_id} in chat {chat_id}")
    except (UserIsBlocked, InputUserDeactivated):
        logger.warning(f"User {user_id} has blocked the bot or is deactivated. Removing from broadcast list if present.")
        if user_id in subscribed_users:
            subscribed_users.discard(user_id)
            save_users(subscribed_users)
    except Exception as e:
        logger.error(f"Error replying to /start for user {user_id} in chat {chat_id}: {e}", exc_info=True)

@app.on_message(filters.command("help"))
async def help_command(client: Client, msg: Message):
    user_id = msg.from_user.id
    chat_id = msg.chat.id
    logger.info(f"Received /help command from user {user_id} ({msg.from_user.first_name}) in chat {chat_id} (type: {msg.chat.type})")
    await add_chat_to_tracking(chat_id, msg.chat.type)
    help_text = (
        "**☠️ Media Extraction Bot Help ☠️**\n\n"
        "🧠 **Name:** 🪬 🖇️𝐋𝐢𝐧𝐤 = 𝐕𝐢𝐝𝐞𝐨🎥 🪬\n"
        "🎯 **Purpose:** Download Video/Audio content from various social media posts.\n"
        "🔗 **Supported:** YouTube (Videos, Shorts), Instagram (Public Video/Reel Posts), Facebook (Public Video Posts).\n"
        "⚠️ **Limitations:** Instagram Stories and single Photo posts are not directly supported for download. I focus on video/audio content from posts.\n\n"
        "⚙️ **How to Use:**\n"
        "   1. Send me a direct link to the media you want.\n"
        "   2. I'll reply with options: Audio🔊,  Video🎥, or Both🔊+🎥.\n"
        "   3. Click your choice (only you can click your buttons!).\n"
        "   4. Wait for the magic! ✨\n\n"
        "★** It's also work:** TG Group ✓\n"
        "👤 **Creator:-** @Hindu_papa ✓\n"
        "🔐 **Fast, Efficient, and User-Friendly!**\n\n"
        "💀 **Stay Anonymous, Stay Powerful** 💀"
    )
    reply_markup = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])
    try:
        await msg.reply_text(help_text, reply_markup=reply_markup, quote=True)
        logger.info(f"Successfully replied to /help for user {user_id} in chat {chat_id}")
    except (UserIsBlocked, InputUserDeactivated):
        logger.warning(f"User {user_id} has blocked the bot or is deactivated (during /help).")
        if user_id in subscribed_users:
             subscribed_users.discard(user_id)
             save_users(subscribed_users)
    except Exception as e:
        logger.error(f"Error replying to /help for user {user_id} in chat {chat_id}: {e}", exc_info=True)

@app.on_message(filters.command(["broadcast", "podcast"]) & filters.user(OWNER_ID))
async def broadcast_command_handler(client: Client, msg: Message):
    if not msg.reply_to_message and len(msg.command) < 2:
        await msg.reply_text(
            "**📢 Podcast/Broadcast Usage:**\n\n"
            "1. Reply to a message with `/broadcast` or `/podcast` (to forward the replied message).\n"
            "2. Use `/broadcast <your message text>` or `/podcast <your message text>` (to send new text).\n\n"
            "This will send the message to all subscribed users (PM) and all active groups where the bot is a member.",
            quote=True
        )
        return
    broadcast_text = ""
    message_to_forward_id = None
    source_chat_id_for_forward = msg.chat.id
    if msg.reply_to_message:
        message_to_forward_id = msg.reply_to_message.id
        source_chat_id_for_forward = msg.reply_to_message.chat.id
        logger.info(f"Admin {msg.from_user.id} initiated broadcast by replying to message {message_to_forward_id} from chat {source_chat_id_for_forward}.")
    else:
        broadcast_text = msg.text.split(" ", 1)[1].strip()
        logger.info(f"Admin {msg.from_user.id} initiated broadcast with text: {broadcast_text[:70]}...")
    if not subscribed_users and not active_groups:
        await msg.reply_text("No users have subscribed and no active groups found to broadcast to.", quote=True)
        return
    status_update_msg = await msg.reply_text(
        f"🚀 **Starting Podcast Delivery**\n"
        f"Targeting: {len(subscribed_users)} users and {len(active_groups)} groups.\n"
        f"This may take a while... I'll update you once done.",
        quote=True
    )
    successful_user_sends = 0
    failed_user_sends = 0
    successful_group_sends = 0
    failed_group_sends = 0
    users_to_remove = set()
    groups_to_remove = set()
    if subscribed_users:
        logger.info(f"Broadcasting to {len(subscribed_users)} subscribed users...")
        for user_id_to_broadcast in list(subscribed_users):
            try:
                if message_to_forward_id:
                    await client.forward_messages(
                        chat_id=user_id_to_broadcast,
                        from_chat_id=source_chat_id_for_forward,
                        message_ids=message_to_forward_id
                    )
                elif broadcast_text:
                    await client.send_message(user_id_to_broadcast, broadcast_text)
                successful_user_sends += 1
            except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid) as e:
                logger.warning(f"Failed to send to user {user_id_to_broadcast}: {type(e).__name__}. Removing user.")
                users_to_remove.add(user_id_to_broadcast)
                failed_user_sends += 1
            except FloodWait as e:
                logger.warning(f"FloodWait for {e.value}s during broadcast to user {user_id_to_broadcast}. Sleeping...")
                await asyncio.sleep(e.value + 5)
                try:
                    if message_to_forward_id: await client.forward_messages(chat_id=user_id_to_broadcast, from_chat_id=source_chat_id_for_forward, message_ids=message_to_forward_id)
                    elif broadcast_text: await client.send_message(user_id_to_broadcast, broadcast_text)
                    successful_user_sends += 1
                except Exception as retry_e:
                    logger.error(f"Retry failed for user {user_id_to_broadcast} after FloodWait: {retry_e}")
                    users_to_remove.add(user_id_to_broadcast)
                    failed_user_sends += 1
            except Exception as e:
                logger.error(f"An unexpected error occurred sending to user {user_id_to_broadcast}: {e}", exc_info=True)
                failed_user_sends += 1
            await asyncio.sleep(0.25)
    if users_to_remove:
        logger.info(f"Attempting to remove {len(users_to_remove)} users from subscription list post-broadcast.")
        for uid_remove in users_to_remove:
            subscribed_users.discard(uid_remove)
        save_users(subscribed_users)
        logger.info(f"Successfully removed users. New user count: {len(subscribed_users)}.")
    if active_groups:
        logger.info(f"Broadcasting to {len(active_groups)} active groups...")
        for group_id_to_broadcast in list(active_groups):
            try:
                if message_to_forward_id:
                    await client.forward_messages(
                        chat_id=group_id_to_broadcast,
                        from_chat_id=source_chat_id_for_forward,
                        message_ids=message_to_forward_id
                    )
                elif broadcast_text:
                    await client.send_message(group_id_to_broadcast, broadcast_text)
                successful_group_sends += 1
            except (PeerIdInvalid, ChatWriteForbidden, UserNotParticipant) as e:
                logger.warning(f"Failed to send to group {group_id_to_broadcast}: {type(e).__name__}. Removing group from active list.")
                groups_to_remove.add(group_id_to_broadcast)
                failed_group_sends += 1
            except (UserIsBlocked, InputUserDeactivated) as e:
                logger.warning(f"User-specific error {type(e).__name__} for group target {group_id_to_broadcast}. This group ID might be invalid or a user. Removing.")
                groups_to_remove.add(group_id_to_broadcast)
                failed_group_sends += 1
            except FloodWait as e:
                logger.warning(f"FloodWait for {e.value}s during broadcast to group {group_id_to_broadcast}. Sleeping...")
                await asyncio.sleep(e.value + 5)
                try:
                    if message_to_forward_id: await client.forward_messages(chat_id=group_id_to_broadcast, from_chat_id=source_chat_id_for_forward, message_ids=message_to_forward_id)
                    elif broadcast_text: await client.send_message(group_id_to_broadcast, broadcast_text)
                    successful_group_sends += 1
                except Exception as retry_e:
                    logger.error(f"Retry failed for group {group_id_to_broadcast} after FloodWait: {retry_e}")
                    if isinstance(retry_e, (PeerIdInvalid, ChatWriteForbidden, UserNotParticipant)):
                        groups_to_remove.add(group_id_to_broadcast)
                    failed_group_sends += 1
            except Exception as e:
                logger.error(f"An unexpected error occurred sending to group {group_id_to_broadcast}: {e}", exc_info=True)
                if "chat_admin_required" in str(e).lower():
                     logger.warning(f"Group {group_id_to_broadcast}: Bot lacks admin rights to send message. Not removing, but counted as failed.")
                failed_group_sends += 1
            await asyncio.sleep(0.35)
    if groups_to_remove:
        logger.info(f"Attempting to remove {len(groups_to_remove)} groups from active list post-broadcast.")
        for gid_remove in groups_to_remove:
            active_groups.discard(gid_remove)
        save_groups(active_groups)
        logger.info(f"Successfully removed groups. New group count: {len(active_groups)}.")
    summary_text = (
        f"📢 **Podcast Delivery Report**\n\n"
        f"👤 **Users Targeted:** {successful_user_sends + failed_user_sends}\n"
        f"  ✅ Sent successfully: {successful_user_sends}\n"
        f"  ❌ Failed: {failed_user_sends}\n"
        f"  👥 Active users now: {len(subscribed_users)}\n\n"
        f"🏢 **Groups Targeted:** {successful_group_sends + failed_group_sends}\n"
        f"  ✅ Sent successfully: {successful_group_sends}\n"
        f"  ❌ Failed: {failed_group_sends}\n"
        f"  🏘️ Active groups now: {len(active_groups)}"
    )
    try:
        await status_update_msg.edit_text(summary_text)
    except Exception as e_edit:
        logger.warning(f"Could not edit broadcast status message: {e_edit}")
        await msg.reply_text(summary_text, quote=True)

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command_handler(client: Client, msg: Message):
    await msg.reply_text(
        f"📊 **Bot Statistics**\n\n"
        f"Total Subscribed Users (for PM broadcast): {len(subscribed_users)}\n"
        f"Total Active Groups (for group broadcast): {len(active_groups)}\n"
        f"Active Media Download Interactions: {len(user_interaction_states)}",
        quote=True
    )

@app.on_message(filters.text & ~filters.command(["start", "help", "broadcast", "podcast", "stats"]))
async def link_handler(client: Client, msg: Message):
    original_user_id = msg.from_user.id
    chat_id = msg.chat.id
    await add_chat_to_tracking(chat_id, msg.chat.type)
    url_match = re.match(r'(https?://[^\s]+)', msg.text.strip())
    if not url_match:
        if msg.chat.type == ChatType.PRIVATE:
             logger.info(f"User {original_user_id} sent non-URL text in private: {msg.text[:50]}. Informing user.")
             await msg.reply_text("Please send a valid link from a supported platform.", quote=True)
        else:
             logger.debug(f"Ignoring non-URL text in group {chat_id} from user {original_user_id}: {msg.text[:50]}")
        return
    url = url_match.group(1)
    logger.info(f"Received potential URL from user {original_user_id} in chat {chat_id}: {url[:100]}")
    
    # ★★★ अस्थायी कुकी फ़ाइल बनाने का लॉजिक (सिर्फ yt-dlp info extraction के लिए, अगर ज़रूरी हो) ★★★
    # Note: yt-dlp info extraction (download=False) ke liye cookies shayad kam zaroori hon, 
    # lekin agar pre-check fail ho raha hai login ki wajah se, to yahan bhi cookies de sakte hain.
    # Abhi ke liye, pre-check ko simple rakhte hain, cookies download ke time denge.
    temp_cookie_file_for_precheck = None
    # if "instagram.com" in url.lower() and INSTAGRAM_COOKIES_CONTENT:
    #     temp_cookie_file_for_precheck = os.path.join(DOWNLOAD_DIR, f"temp_precheck_{TEMP_INSTA_COOKIES_FILENAME}")
    #     try:
    #         with open(temp_cookie_file_for_precheck, 'w') as f_cookie:
    #             f_cookie.write(INSTAGRAM_COOKIES_CONTENT)
    #         logger.info(f"Created temp cookie file for pre-check: {temp_cookie_file_for_precheck}")
    #     except Exception as e:
    #         logger.error(f"Failed to create temp cookie file for pre-check: {e}")
    #         temp_cookie_file_for_precheck = None # Agar error aaye to None kar do

    pre_check_ydl_opts = {'quiet': True, 'skip_download': True, 'noplaylist': True, 'extract_flat': 'discard_in_playlist', 'source_address': '0.0.0.0'}
    # if temp_cookie_file_for_precheck:
    #    pre_check_ydl_opts['cookiefile'] = temp_cookie_file_for_precheck

    try:
        with yt_dlp.YoutubeDL(pre_check_ydl_opts) as ydl:
            info = await asyncio.to_thread(ydl.extract_info, url, download=False)
        extractor_key = info.get('extractor_key', '').lower() if info else ''
        is_supported_extractor = any(target in extractor_key for target in TARGET_EXTRACTORS)
        if not info or not extractor_key or not is_supported_extractor:
            logger.warning(f"URL '{url}' from user {original_user_id} in chat {chat_id} is not from a supported platform or extractor not recognized ('{extractor_key}').")
            if msg.chat.type == ChatType.PRIVATE:
                await msg.reply_text("Sorry, I can only process links from YouTube, Instagram, and Facebook video/reel posts.", quote=True)
            return
        logger.info(f"yt-dlp recognized supported extractor '{extractor_key}' for URL: {url}")
    except yt_dlp.utils.DownloadError as de:
        logger.warning(f"yt-dlp DownloadError during pre-check for {url} from user {original_user_id} in chat {chat_id}: {de}.")
        # ★★★ अगर प्री-चेक में इंस्टाग्राम लॉगिन एरर आए, तो भी बटन दिखा सकते हैं, क्योंकि डाउनलोड के समय कुकीज़ इस्तेमाल होंगी ★★★
        if "instagram" in str(de).lower() and ("login required" in str(de).lower() or "unavailable" in str(de).lower()):
            logger.warning(f"Instagram pre-check failed with login/availability issue, but proceeding to show options as cookies might help during download.")
        elif msg.chat.type == ChatType.PRIVATE: # दूसरे एरर्स के लिए
            await msg.reply_text(f"Could not process this link. Error: {str(de)[:100]}", quote=True)
            return
        else: # ग्रुप में अन्य एरर को अनदेखा करें
            return
            
    except Exception as e:
        logger.error(f"Unexpected error during URL pre-check for {url} from user {original_user_id} in chat {chat_id}: {e}", exc_info=True)
        if msg.chat.type == ChatType.PRIVATE:
             await msg.reply_text("An unexpected error occurred while checking the link.", quote=True)
        return
    # finally: # ★★★ प्री-चेक अस्थायी कुकी फ़ाइल को डिलीट करना (अगर इस्तेमाल हुई हो) ★★★
    #     if temp_cookie_file_for_precheck and os.path.exists(temp_cookie_file_for_precheck):
    #         try:
    #             os.remove(temp_cookie_file_for_precheck)
    #             logger.info(f"Deleted temp cookie file for pre-check: {temp_cookie_file_for_precheck}")
    #         except Exception as e_clean:
    #             logger.error(f"Error deleting temp cookie file for pre-check {temp_cookie_file_for_precheck}: {e_clean}")
                
    status_message = await msg.reply_text(
        f"🔗 Link from {msg.from_user.mention} received! Choose what you want to extract:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Audio 🔊", callback_data="audio"),
             InlineKeyboardButton("Video 🎥", callback_data="video")],
            [InlineKeyboardButton("Audio+Video 🔊+🎥", callback_data="both")],
            [WHOAMI_BTN]
        ]),
        quote=True
    )
    interaction_key = status_message.id
    user_interaction_states[interaction_key] = {
        "url": url, "status_message_id": status_message.id,
        "chat_id": chat_id, "original_user_id": original_user_id,
        "original_message_id": msg.id, "last_update_time": 0.0
    }
    logger.info(f"Stored link for user {original_user_id} in chat {chat_id}. Interaction key: {interaction_key}")

async def progress_hook(d, client: Client, chat_id_for_progress: int, status_message_id_for_progress: int, original_user_id_of_downloader: int):
    if status_message_id_for_progress not in user_interaction_states:
        logger.warning(f"Progress hook for non-existent interaction {status_message_id_for_progress}. User: {original_user_id_of_downloader}")
        return
    interaction_context = user_interaction_states[status_message_id_for_progress]
    try:
        current_time = asyncio.get_event_loop().time()
        if d['status'] == 'downloading':
            percentage = d.get('_percent_str', 'N/A').strip()
            speed = d.get('_speed_str', 'N/A').strip()
            eta = d.get('_eta_str', 'N/A').strip()
            downloaded_bytes = d.get('downloaded_bytes', 0)
            total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
            progress_text = f"Downloading... {percentage} ⏳"
            if total_bytes: progress_text += f"\n{sizeof_fmt(downloaded_bytes)} of {sizeof_fmt(total_bytes)}"
            progress_text += f"\nSpeed: {speed} | ETA: {eta}"
            last_update = interaction_context.get("last_update_time", 0.0)
            if (current_time - last_update) > 2.5: # हर 2.5 सेकंड में अपडेट करें
                # सुनिश्चित करें कि status_message_id अभी भी user_interaction_states में मौजूद है
                if status_message_id_for_progress in user_interaction_states:
                     await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, progress_text)
                     user_interaction_states[status_message_id_for_progress]["last_update_time"] = current_time
                else: # अगर इंटरेक्शन खत्म हो गया है तो हुक को रोकें
                    logger.info(f"Interaction {status_message_id_for_progress} no longer active. Stopping progress updates.")
                    raise yt_dlp.utils.DownloadCancelled("Interaction ended")


        elif d['status'] == 'finished':
            if status_message_id_for_progress in user_interaction_states: # सुनिश्चित करें कि इंटरेक्शन अभी भी सक्रिय है
                 await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, "Download complete! Processing... ⚙️")
        elif d['status'] == 'error':
            logger.error(f"yt-dlp reported error hook for user {original_user_id_of_downloader}, interaction {status_message_id_for_progress}: {d.get('error', 'Unknown error')}")
            if status_message_id_for_progress in user_interaction_states: # सुनिश्चित करें कि इंटरेक्शन अभी भी सक्रिय है
                 await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, "Error during download process. 😕")
    except FloodWait as fw:
        logger.warning(f"FloodWait {fw.value}s updating progress for user {original_user_id_of_downloader} (interaction {status_message_id_for_progress}). Waiting...")
        await asyncio.sleep(fw.value + 0.5)
    except yt_dlp.utils.DownloadCancelled: # ★★★ डाउनलोड कैंसल को हैंडल करना ★★★
        logger.info(f"Download explicitly cancelled for interaction {status_message_id_for_progress}.")
        # Yt-dlp को रुकने के लिए एरर रेज़ करें
        raise
    except Exception as e:
        # यह सुनिश्चित करने के लिए कि अगर इंटरेक्शन पहले ही हटा दिया गया है तो एरर न आए
        if status_message_id_for_progress in user_interaction_states:
            logger.warning(f"Failed to edit progress message for user {original_user_id_of_downloader}, interaction {status_message_id_for_progress}: {e}")
        else:
            logger.info(f"Progress hook called for already cleared interaction {status_message_id_for_progress}. Ignoring error: {e}")


def sizeof_fmt(num, suffix="B"):
    if not isinstance(num, (int, float)): return "N/A"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0: return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

@app.on_callback_query()
async def button_handler(client: Client, query: CallbackQuery):
    user_who_clicked_id = query.from_user.id
    interaction_key = query.message.id # यह status_message.id है
    logger.info(f"Callback query '{query.data}' from user {user_who_clicked_id} ({query.from_user.first_name}) for message ID {interaction_key}")

    if interaction_key not in user_interaction_states:
        await query.answer("❌ This download session has expired or is invalid. Please send the link again.", show_alert=True)
        try: await query.message.delete() # ओरिजिनल मैसेज को डिलीट करने की कोशिश
        except Exception: pass
        return

    interaction_data = user_interaction_states[interaction_key]
    original_user_id = interaction_data["original_user_id"]

    if user_who_clicked_id != original_user_id:
        await query.answer("⚠️ These buttons are for the user who sent the link. Please send your own link if you want to download.", show_alert=True)
        return

    current_url_for_download = interaction_data["url"] # ★★★ URL यहाँ से मिलेगा ★★★
    chat_id = interaction_data["chat_id"]
    choice = query.data

    await query.answer(f"Processing your choice: {choice}...", show_alert=False)
    try:
        await client.edit_message_text(chat_id, interaction_key, f"Initializing download for '{choice}'... 🚀")
    except Exception: pass

    download_path_template = os.path.join(DOWNLOAD_DIR, f"%(title).180s_{original_user_id}_{choice}.%(ext)s")
    main_event_loop = asyncio.get_running_loop() # ★★★ main_event_loop यहाँ डिफाइन किया गया ★★★

    ydl_opts = {
        "outtmpl": download_path_template, "noplaylist": True, "quiet": False, "merge_output_format": "mp4",
        "retries": 2, "geo_bypass": True, "nocheckcertificate": True, "ignoreerrors": False,
        "source_address": "0.0.0.0",
        "progress_hooks": [
            lambda d: asyncio.run_coroutine_threadsafe(
                progress_hook(d, client, chat_id, interaction_key, original_user_id), main_event_loop
            ).result(timeout=15) # ★★★ टाइमआउट थोड़ा बढ़ाया ★★★
        ],
        "postprocessor_hooks": [lambda d: logger.info(f"Postprocessor status for user {original_user_id} (interaction: {interaction_key}): {d['status']}") if d.get('status') != 'started' else None],
        "format_sort": ['res:1080', 'ext:mp4:m4a'], "restrictfilenames": True,
    }

    original_downloaded_file = None
    extracted_audio_file_for_both = None
    info_dict = None
    
    # ★★★ अस्थायी इंस्टाग्राम कुकी फ़ाइल बनाने और इस्तेमाल करने का लॉजिक ★★★
    temp_cookie_file_path = None
    if "instagram.com" in current_url_for_download.lower() and INSTAGRAM_COOKIES_CONTENT:
        temp_cookie_file_path = os.path.join(DOWNLOAD_DIR, TEMP_INSTA_COOKIES_FILENAME)
        try:
            with open(temp_cookie_file_path, 'w', encoding='utf-8') as f_cookie: # utf-8 encoding जोड़ी
                f_cookie.write(INSTAGRAM_COOKIES_CONTENT)
            ydl_opts['cookiefile'] = temp_cookie_file_path
            logger.info(f"Using temporary Instagram cookie file for download: {temp_cookie_file_path}")
        except Exception as e_cookie_write:
            logger.error(f"Failed to create/write temporary Instagram cookie file: {e_cookie_write}")
            temp_cookie_file_path = None # अगर एरर आए तो None कर दो ताकि इस्तेमाल न हो

    try:
        if choice == "audio":
            ydl_opts['format'] = "bestaudio[ext=m4a]/bestaudio/best"
            ydl_opts['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
        elif choice == "video":
            ydl_opts['format'] = "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=1080]/best[ext=mp4][height<=720]/best"
        elif choice == "both":
            ydl_opts['format'] = "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=1080]/best[ext=mp4][height<=720]/best"
        else:
            await client.edit_message_text(chat_id, interaction_key, "❌ Invalid choice processed.")
            # user_interaction_states.pop(interaction_key, None) # इसे finally में हैंडल किया जाएगा
            return # finally ब्लॉक निष्पादित होगा

        logger.info(f"User {original_user_id} (interaction: {interaction_key}) selected '{choice}'. Starting download for URL: {current_url_for_download}")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                await client.edit_message_text(chat_id, interaction_key, "Fetching media info... ℹ️")
                info_dict = await asyncio.to_thread(ydl.extract_info, current_url_for_download, download=False)
                if not info_dict:
                    raise yt_dlp.utils.DownloadError("Failed to extract media information (info_dict is None).")
                
                media_title = clean_filename(info_dict.get('title', 'Untitled_Media'))
                await client.edit_message_text(chat_id, interaction_key, f"Starting download: `{media_title[:60]}...` 📥")
                
                # download=True के साथ दोबारा extract_info कॉल करने की बजाय, ydl.download([url]) इस्तेमाल कर सकते हैं
                # लेकिन आपका मौजूदा तरीका भी ठीक है
                download_info = await asyncio.to_thread(ydl.extract_info, current_url_for_download, download=True)

                # ... (आपका मौजूदा original_downloaded_file ढूंढने का लॉजिक वैसा ही रहेगा) ...
                if 'requested_downloads' in download_info and download_info['requested_downloads'] and \
                   'filepath' in download_info['requested_downloads'][0] and \
                   os.path.exists(download_info['requested_downloads'][0]['filepath']):
                    original_downloaded_file = download_info['requested_downloads'][0]['filepath']
                elif 'filename' in download_info and os.path.exists(download_info['filename']): # yt-dlp कभी-कभी 'filename' देता है
                    original_downloaded_file = download_info['filename']
                # ★★★ fallback logic to find file if not directly provided by yt-dlp ★★★
                else:
                    _title_final = clean_filename(download_info.get("title", info_dict.get("title", "untitled_download")))
                    _ext_final = "mp3" if choice == "audio" and ydl_opts.get('postprocessors') else download_info.get('ext', info_dict.get('ext','mp4'))
                    _expected_base_name = f"{_title_final}_{original_user_id}_{choice}" # without extension initially
                    
                    # Try with expected extension
                    _expected_file_path_with_ext = os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.{_ext_final}")
                    if os.path.exists(_expected_file_path_with_ext):
                        original_downloaded_file = _expected_file_path_with_ext
                    elif choice == "audio" and _ext_final == "mp3": # Special handling for audio postprocessing
                        # Check for original extension before postprocessing (e.g., m4a)
                        original_ext_before_pp = download_info.get('ext', info_dict.get('ext'))
                        if original_ext_before_pp and original_ext_before_pp != 'mp3':
                             _path_before_pp = os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.{original_ext_before_pp}")
                             if os.path.exists(_path_before_pp):
                                # This would be the file *before* ffmpeg conversion to mp3 if it's an intermediate step
                                # However, ydl.extract_info with download=True and postprocessors *should* return the final mp3 path
                                # This logic might be more relevant if you were running ffmpeg manually after a non-mp3 download.
                                # For now, we trust yt-dlp to give the final path or we find it.
                                pass # Let the later general search handle it if needed.
                        # Check for .m4a if mp3 failed, as it's a common audio source for mp4 videos
                        _m4a_path = os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.m4a")
                        if os.path.exists(_m4a_path):
                            original_downloaded_file = _m4a_path # This could be before ffmpeg conversion

                    if not original_downloaded_file: # If still not found, search more broadly
                        logger.warning(f"Could not find exact file for '{_title_final}' (interaction {interaction_key}) with ext '{_ext_final}'. Checking for files matching pattern...")
                        # Search for files that start with the cleaned title and contain user_id and choice
                        # This is a bit fragile; yt-dlp should ideally give the correct path.
                        title_prefix_for_search = clean_filename(info_dict.get('title', 'Untitled_Media'))[:50] # Use a shorter prefix for robustness
                        
                        possible_files = []
                        for f_name in os.listdir(DOWNLOAD_DIR):
                            if (f_name.startswith(title_prefix_for_search) and 
                                f"_{original_user_id}_{choice}" in f_name and
                                f_name.endswith(f".{_ext_final}")): # Check for final expected extension
                                possible_files.append(os.path.join(DOWNLOAD_DIR, f_name))
                        
                        if not possible_files and choice == "audio" and _ext_final == "mp3": # If mp3 not found, check common source like m4a
                             for f_name in os.listdir(DOWNLOAD_DIR):
                                if (f_name.startswith(title_prefix_for_search) and 
                                    f"_{original_user_id}_{choice}" in f_name and
                                    f_name.endswith(".m4a")):
                                    possible_files.append(os.path.join(DOWNLOAD_DIR, f_name))


                        if possible_files:
                            # Sort by modification time, newest first, assuming it's the most recent download
                            possible_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                            original_downloaded_file = possible_files[0]
                            logger.info(f"Found a candidate file by pattern matching: {original_downloaded_file}")
                        else:
                             raise yt_dlp.utils.DownloadError(f"Could not reliably determine downloaded file path for '{_title_final}' after download (interaction {interaction_key}). Searched for pattern like '{_expected_base_name}.{_ext_final}'. Check 'filepath' in ydl output or download directory contents: {os.listdir(DOWNLOAD_DIR)}")


                if not original_downloaded_file or not os.path.exists(original_downloaded_file):
                    raise yt_dlp.utils.DownloadError(f"File path missing or file does not exist after download attempt: {original_downloaded_file} (interaction {interaction_key})")
                
                logger.info(f"File for user {original_user_id} (interaction: {interaction_key}) downloaded/processed to: {original_downloaded_file}")

            except yt_dlp.utils.DownloadError as de:
                error_message = str(de).splitlines()[-1] if str(de).splitlines() else str(de)
                logger.error(f"yt-dlp DownloadError for user {original_user_id} (interaction: {interaction_key}) on URL {current_url_for_download}: {error_message}", exc_info=False) # exc_info=False to avoid huge logs for common errors
                reply_error = f"❌ Download Error: `{error_message[:250]}`"
                if "Unsupported URL" in str(de): reply_error = f"❌ Error: Unsupported URL."
                elif "private" in str(de).lower() or "login required" in str(de).lower() or "authentication" in str(de).lower():
                     reply_error = f"❌ Error: This content might be private or require login. If you are the admin, please check/update Instagram cookies."
                try: await client.edit_message_text(chat_id, interaction_key, reply_error)
                except Exception: pass
                # user_interaction_states.pop(interaction_key, None) # Handled in finally
                return # finally will execute
            except Exception as e: # Other errors during ydl processing
                logger.error(f"Error during yt-dlp processing for user {original_user_id} (interaction: {interaction_key}): {e}", exc_info=True)
                try: await client.edit_message_text(chat_id, interaction_key, f"❌ An unexpected error during download: `{str(e)[:200]}`")
                except Exception: pass
                # user_interaction_states.pop(interaction_key, None) # Handled in finally
                return # finally will execute

        # If info_dict was not populated from download=True, try to get it again (it should have been from download=False earlier)
        if not info_dict: # This info_dict is from the initial download=False call.
            logger.warning(f"info_dict was not populated from initial pre-check for interaction {interaction_key}. Using filename as fallback for title.")
            # Fallback, as info_dict from pre-check should ideally be used.
            # If original_downloaded_file is None here, it means download itself failed and returned.
            if original_downloaded_file:
                 # Try to derive title from filename if absolutely necessary
                 base_fname = os.path.basename(original_downloaded_file)
                 # Attempt to remove _user_id_choice.ext suffix
                 derived_title = base_fname.rsplit(f"_{original_user_id}_{choice}.", 1)[0] if f"_{original_user_id}_{choice}." in base_fname else base_fname.rsplit('.',1)[0]
                 info_dict = {'title': derived_title, 'duration': None} # Minimal info
            else: # Should not happen if download succeeded
                 info_dict = {'title': "media_file", 'duration': None}


        title_final = clean_filename(info_dict.get("title", "media_file"))
        duration_seconds = info_dict.get("duration") # This might be None if info_dict is minimal
        duration_str = f"{int(duration_seconds // 60):02d}:{int(duration_seconds % 60):02d}" if isinstance(duration_seconds, (int, float)) and duration_seconds > 0 else "N/A"
        filesize_str = sizeof_fmt(os.path.getsize(original_downloaded_file)) if original_downloaded_file and os.path.exists(original_downloaded_file) else "N/A"
        
        base_caption = (
            f"🎬 **Title:** `{title_final}`\n"
            f"⏱️ **Duration:** `{duration_str}` | 💾 **Size:** `{filesize_str}`\n"
            f"🥀 **Requested by:** {query.from_user.mention}\n"
            f"👤 **Creator:** @Hindu_papa ✓"
        )
        final_ui = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])

        try: await client.edit_message_text(chat_id, interaction_key, f"Preparing to send your '{choice}'... 🎁")
        except Exception: pass # If message was deleted or something

        reply_to_msg_id = interaction_data["original_message_id"]
        final_output_file = original_downloaded_file

        # Check if audio postprocessing created a new file
        if choice == "audio" and ydl_opts.get('postprocessors') and original_downloaded_file:
            base, orig_ext = os.path.splitext(original_downloaded_file)
            expected_codec = ydl_opts['postprocessors'][0]['preferredcodec']
            # If original file was already the target codec (e.g. downloaded mp3 directly)
            if orig_ext.lstrip('.') == expected_codec:
                final_output_file = original_downloaded_file
            else: # Otherwise, look for the postprocessed file
                expected_audio_file = base + "." + expected_codec
                if os.path.exists(expected_audio_file):
                    final_output_file = expected_audio_file
                # If the expected postprocessed file isn't found, but the original exists, use original (yt-dlp might have handled it)
                elif not os.path.exists(final_output_file): # final_output_file is still original_downloaded_file here
                     logger.error(f"Postprocessed audio file {expected_audio_file} not found, and original {original_downloaded_file} also missing for interaction {interaction_key}")
                     raise FileNotFoundError(f"Could not find audio file after postprocessing for {title_final}")
                # If original_downloaded_file is already mp3 and postprocessor also mp3, no new file might be made.

        if not final_output_file or not os.path.exists(final_output_file):
            logger.error(f"Critical: final_output_file '{final_output_file}' does not exist before sending.")
            await client.edit_message_text(chat_id, interaction_key, "❌ Error: Output file not found after download.")
            return # finally will execute

        # Sending files
        if choice == "audio":
            await client.send_audio(
                chat_id=chat_id, audio=final_output_file,
                caption=f"**🔊 Audio Extracted!**\n\n{base_caption}",
                title=title_final[:30], duration=int(duration_seconds or 0), # Ensure duration is int
                reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )
        elif choice == "video":
            await client.send_video(
                chat_id=chat_id, video=final_output_file,
                caption=f"**🎥 Video Downloaded!**\n\n{base_caption}",
                duration=int(duration_seconds or 0), # Ensure duration is int
                reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )
        elif choice == "both":
            await client.send_video(
                chat_id=chat_id, video=final_output_file,
                caption=f"**🎥 Video Part!**\n\n{base_caption}",
                duration=int(duration_seconds or 0), # Ensure duration is int
                reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )
            
            extracted_audio_file_for_both = os.path.join(DOWNLOAD_DIR, f"{title_final}_{original_user_id}_audio_extract.mp3")
            logger.info(f"Attempting to extract audio for 'Both' to: {extracted_audio_file_for_both}")
            
            try: # Edit message before ffmpeg call
                await client.edit_message_text(chat_id, interaction_key, "Extracting audio for 'Both'... 🔊")
            except Exception: pass

            ffmpeg_command = [
                'ffmpeg', '-i', final_output_file, '-vn', '-acodec', 'libmp3lame',
                '-b:a', '192k', '-ar', '44100', '-y', extracted_audio_file_for_both
            ]
            if await run_ffmpeg(ffmpeg_command):
                if os.path.exists(extracted_audio_file_for_both):
                    logger.info(f"Audio extracted successfully for 'Both': {extracted_audio_file_for_both}")
                    audio_filesize_str = sizeof_fmt(os.path.getsize(extracted_audio_file_for_both))
                    # Recalculate duration for audio part if video had different (should be same)
                    audio_duration_seconds = duration_seconds # Assume same as video
                    audio_duration_str = f"{int(audio_duration_seconds // 60):02d}:{int(audio_duration_seconds % 60):02d}" if isinstance(audio_duration_seconds, (int, float)) and audio_duration_seconds > 0 else "N/A"

                    audio_caption_for_both = (
                        f"🎬 **Title:** `{title_final} (Audio)`\n"
                        f"⏱️ **Duration:** `{audio_duration_str}` | 💾 **Size:** `{audio_filesize_str}`\n"
                        f"🥀 **Requested by:** {query.from_user.mention}\n"
                        f"👤 **Creator:** @Hindu_papa ✓"
                    )
                    await client.send_audio(
                        chat_id=chat_id, audio=extracted_audio_file_for_both,
                        caption=f"**🔊 Audio Part!**\n\n{audio_caption_for_both}",
                        title=title_final[:30], duration=int(audio_duration_seconds or 0),
                        reply_markup=final_ui, reply_to_message_id=reply_to_msg_id
                    )
                else: # FFmpeg success but file not found
                    logger.error(f"FFmpeg seemed to succeed for 'Both', but extracted audio file NOT FOUND at: {extracted_audio_file_for_both}")
                    try: logger.info(f"Contents of {DOWNLOAD_DIR} after ffmpeg for 'Both': {os.listdir(DOWNLOAD_DIR)}")
                    except Exception as e_ls: logger.error(f"Could not list {DOWNLOAD_DIR}: {e_ls}")
                    await client.send_message(chat_id, "⚠️ Failed to process audio for 'Both' (file not found after extraction), but video was sent.", reply_to_message_id=reply_to_msg_id)
            else: # FFmpeg command failed
                logger.error(f"FFmpeg command failed for 'Both' for user {original_user_id} (interaction {interaction_key}). Video path was: {final_output_file}")
                try: logger.info(f"Contents of {DOWNLOAD_DIR} after failed ffmpeg for 'Both': {os.listdir(DOWNLOAD_DIR)}")
                except Exception as e_ls: logger.error(f"Could not list {DOWNLOAD_DIR}: {e_ls}")
                await client.send_message(chat_id, "⚠️ Failed to extract audio for 'Both' (ffmpeg error), but video was sent.", reply_to_message_id=reply_to_msg_id)
        
        # Delete the status message (e.g., "Preparing to send...")
        try:
            if interaction_key in user_interaction_states: # Check if it still exists
                 await client.delete_messages(chat_id, interaction_key)
        except Exception: pass # Ignore if already deleted or cannot be

    except FloodWait as fw:
        logger.warning(f"FloodWait for {fw.value}s in button_handler for user {original_user_id} (interaction {interaction_key}). Waiting...")
        await asyncio.sleep(fw.value + 1)
    except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, ChatWriteForbidden) as e:
        logger.warning(f"User {original_user_id} (interaction {interaction_key}) blocked bot, is deactivated, or chat/user ID is invalid/bot cannot write to chat: {type(e).__name__}")
        if original_user_id in subscribed_users:
            subscribed_users.discard(original_user_id)
            save_users(subscribed_users)
        if chat_id in active_groups and isinstance(e, (ChatWriteForbidden, PeerIdInvalid)): # Use chat_id from interaction_data
            logger.info(f"Removing group {chat_id} due to {type(e).__name__} during button handler.")
            active_groups.discard(chat_id)
            save_groups(active_groups)
    except Exception as e: # Catch-all for other unexpected errors in the handler
        logger.error(f"Overall error in button_handler for user {original_user_id} (interaction {interaction_key}): {e}", exc_info=True)
        try:
            if interaction_key in user_interaction_states and query.message: # query.message might be None if deleted
                 await client.edit_message_text(chat_id, interaction_key, f"❌ A critical error occurred: `{str(e)[:250]}`")
            elif query.message and query.message.chat : # Fallback if interaction_key was popped but we can still reply
                await client.send_message(chat_id, f"❌ A critical error occurred with your request. Please try again or contact admin if issue persists.", reply_to_message_id=interaction_data.get("original_message_id"))
        except Exception as e_reply:
            logger.error(f"Failed to even send error message to user {original_user_id} (interaction {interaction_key}): {e_reply}")
    finally:
        # ★★★ अस्थायी इंस्टाग्राम कुकी फ़ाइल को डिलीट करना ★★★
        if temp_cookie_file_path and os.path.exists(temp_cookie_file_path):
            try:
                os.remove(temp_cookie_file_path)
                logger.info(f"Deleted temporary Instagram cookie file: {temp_cookie_file_path}")
            except Exception as e_clean_cookie:
                logger.error(f"Error deleting temporary Instagram cookie file {temp_cookie_file_path}: {e_clean_cookie}")

        files_to_clean = []
        if original_downloaded_file and os.path.exists(original_downloaded_file):
            files_to_clean.append(original_downloaded_file)
        if extracted_audio_file_for_both and os.path.exists(extracted_audio_file_for_both):
            files_to_clean.append(extracted_audio_file_for_both)
        
        # If audio postprocessing created a new file (and original wasn't mp3), add original to clean list if different
        if choice == "audio" and ydl_opts.get('postprocessors') and original_downloaded_file and final_output_file != original_downloaded_file:
            # This case might be tricky if original_downloaded_file was already the mp3.
            # The logic for final_output_file tries to determine the correct one.
            # If final_output_file is the .mp3 and original_downloaded_file was .m4a, original_downloaded_file should be cleaned.
            # The current list `files_to_clean` should already contain `original_downloaded_file` if it existed.
            # And if `final_output_file` is different and is the one sent, then `original_downloaded_file` (the intermediate) should be cleaned.
            # No, the `final_output_file` itself is what we want to keep until sent, then clean.
            # `original_downloaded_file` might be an intermediate m4a, `final_output_file` the mp3.
            # The current logic cleans `original_downloaded_file`. If ffmpeg created a new file that was sent, that should also be cleaned.
            # Actually, `final_output_file` is the one that was sent.
            # The `files_to_clean` list should contain all intermediate and final files that were created.
            if final_output_file and os.path.exists(final_output_file) and final_output_file not in files_to_clean:
                 files_to_clean.append(final_output_file)


        for f_path in set(files_to_clean): # Use set to avoid duplicates
            if f_path and os.path.exists(f_path): # Double check existence
                try:
                    os.remove(f_path)
                    logger.info(f"Cleaned: {f_path} (interaction {interaction_key})")
                except Exception as e_clean:
                    logger.error(f"Error cleaning {f_path}: {e_clean}")
        
        if interaction_key in user_interaction_states:
            user_interaction_states.pop(interaction_key, None)
            logger.info(f"Cleared state for interaction key {interaction_key} (user {original_user_id})")

if __name__ == "__main__":
    logger.info("Bot starting up...")
    if not INSTAGRAM_COOKIES_CONTENT: # ★★★ चेतावनी अगर कुकी कंटेंट सेट नहीं है ★★★
        logger.warning("INSTAGRAM_COOKIES_CONTENT environment variable is not set. Instagram downloads may fail for private/login-required content.")
    try:
        if app: 
            app.start() 
            logger.info("Pyrogram Client started. Bot is now online!")
            idle() 
            logger.info("Bot received stop signal, shutting down...")
            app.stop() 
            logger.info("Bot has been stopped gracefully.")
        else:
            logger.critical("CRITICAL ERROR: Pyrogram Client (app) was not initialized. Bot cannot run.")
            
    except RuntimeError as e:
        if "another loop" in str(e).lower() or "different loop" in str(e).lower():
            logger.warning(f"Caught a common asyncio loop error during shutdown: {e}. This is often non-critical if the bot was stopping anyway.")
        else:
            logger.critical(f"A runtime error occurred during bot execution: {e}", exc_info=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt).")
        if app: 
            app.stop() 
    except Exception as e:
        logger.critical(f"An unexpected error occurred at the top level, causing bot to stop: {e}", exc_info=True)
        if app: 
            app.stop() 
    finally:
        logger.info("Script execution finished. Cleaning up if necessary.")

