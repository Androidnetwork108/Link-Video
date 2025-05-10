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
API_ID_STR = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_STR = os.environ.get("OWNER_ID")
STRING_SESSION = os.environ.get("STRING_SESSION")
# ★★★ कुकीज़ कंटेंट के लिए ENV VARs ★★★
INSTAGRAM_COOKIES_CONTENT = os.environ.get("INSTAGRAM_COOKIES_CONTENT")
FACEBOOK_COOKIES_CONTENT = os.environ.get("FACEBOOK_COOKIES_CONTENT")
YOUTUBE_COOKIES_CONTENT = os.environ.get("YOUTUBE_COOKIES_CONTENT")

# Validate that essential environment variables are set
if not API_ID_STR: raise ValueError("Missing API_ID environment variable.")
if not API_HASH: raise ValueError("Missing API_HASH environment variable.")
if not BOT_TOKEN and not STRING_SESSION: raise ValueError("Missing BOT_TOKEN or STRING_SESSION.")
if not OWNER_ID_STR: raise ValueError("Missing OWNER_ID environment variable.")

try:
    API_ID = int(API_ID_STR)
except ValueError: raise ValueError("API_ID must be an integer.")
try:
    OWNER_ID = int(OWNER_ID_STR)
except ValueError: raise ValueError("OWNER_ID must be an integer.")

DOWNLOAD_DIR = "./downloads"
USERS_FILE = "bot_users.json"
GROUPS_FILE = "bot_groups.json"
# ★★★ अस्थायी कुकी फ़ाइलों के नाम ★★★
TEMP_INSTA_COOKIES_FILENAME = "temp_insta_cookies.txt"
TEMP_FB_COOKIES_FILENAME = "temp_fb_cookies.txt"
TEMP_YT_COOKIES_FILENAME = "temp_yt_cookies.txt"

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Pyrogram Client Initialization
app = None
if STRING_SESSION:
    logger.info("STRING_SESSION found. Initializing client with session string.")
    app = Client(name="media_bot_session", session_string=STRING_SESSION, api_id=API_ID, api_hash=API_HASH)
elif BOT_TOKEN:
    logger.info("STRING_SESSION not found. Initializing client with BOT_TOKEN.")
    app = Client(name="media_bot_token", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
else:
    logger.critical("CRITICAL ERROR: Neither STRING_SESSION nor BOT_TOKEN is available.")
    exit()

# --- Broadcast, User, Group Management ---
def load_data(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f: return set(json.load(f))
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {file_path}. Initializing empty set.")
    return set()

def save_data(data_set, file_path):
    try:
        with open(file_path, 'w') as f: json.dump(list(data_set), f) # Convert set to list for JSON
    except Exception as e: logger.error(f"Error saving to {file_path}: {e}")

subscribed_users = load_data(USERS_FILE)
active_groups = load_data(GROUPS_FILE)

async def add_chat_to_tracking(chat_id: int, chat_type: ChatType):
    if chat_type in [ChatType.GROUP, ChatType.SUPERGROUP] and chat_id not in active_groups:
        active_groups.add(chat_id)
        save_data(active_groups, GROUPS_FILE)
        logger.info(f"New group chat {chat_id} added. Total groups: {len(active_groups)}")

# --- Constants & Buttons ---
WHOAMI_BTN = InlineKeyboardButton("WHO I AM? 👤", url="https://t.me/MeNetwork108/14")
CREATOR_BTN = InlineKeyboardButton("Made By ❤️", url="https://t.me/Hindu_papa")
TARGET_EXTRACTORS = ['youtube', 'instagram', 'facebook', 'fb', 'soundcloud']

# --- Helper Functions ---
def clean_filename(name: str) -> str:
    name = str(name); name = re.sub(r'[\\/*?:"<>|]', "", name); name = name.strip().replace(" ", "_"); return name[:200]

async def run_ffmpeg(command: list) -> bool:
    logger.info(f"Running FFmpeg: {' '.join(command)}")
    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        logger.error(f"FFmpeg Error (Code {process.returncode}): {stderr.decode(errors='ignore').strip()}"); return False
    logger.info(f"FFmpeg Success! Output: {stdout.decode(errors='ignore').strip()[:200]}"); return True

user_interaction_states = {}

# --- Pyrogram Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client: Client, msg: Message):
    user_id = msg.from_user.id; chat_id = msg.chat.id
    logger.info(f"Received /start from {user_id} in {chat_id} ({msg.chat.type})")
    await add_chat_to_tracking(chat_id, msg.chat.type)
    if msg.chat.type == ChatType.PRIVATE and user_id not in subscribed_users:
        subscribed_users.add(user_id); save_data(subscribed_users, USERS_FILE)
        logger.info(f"New user {user_id} added. Total: {len(subscribed_users)}")
    start_text = (f"🛸 **Welcome, {msg.from_user.mention}!**\n\n⚡ I'm a multi-platform Media Extractor Bot.\n"
                  "I can attempt to download from **YouTube, Instagram, Facebook**.\n"
                  "Just send me a link (works in private chat or groups!).\n\n"
                  "**I Can Extract:** Video 🎥 | Audio 🔊 | Both 🎥+🔊\n\n👤 **Creator:-** @Hindu_papa ✓\n")
    reply_markup = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])
    try: await msg.reply_text(start_text, reply_markup=reply_markup, quote=True)
    except (UserIsBlocked, InputUserDeactivated): logger.warning(f"User {user_id} blocked/deactivated during /start.")
    except Exception as e: logger.error(f"Error in /start for {user_id}: {e}", exc_info=True)

@app.on_message(filters.command("help"))
async def help_command(client: Client, msg: Message):
    help_text = ("**☠️ Media Extraction Bot Help ☠️**\n\n🧠 **Name:** 🪬 🖇️𝐋𝐢𝐧𝐤 = 𝐕𝐢𝐝𝐞𝐨🎥 🪬\n"
                 "🎯 **Purpose:** Download Video/Audio content from various social media posts.\n"
                 "🔗 **Supported:** YouTube (Videos, Shorts), Instagram (Public Video/Reel Posts), Facebook (Public Video Posts).\n"
                 "⚠️ **Limitations:** Some private/age-restricted content might require bot admin to configure cookies. Stories support can be limited.\n\n"
                 "⚙️ **How to Use:**\n   1. Send me a direct link to the media you want.\n"
                 "   2. I'll reply with options: Audio🔊,  Video🎥, or Both🔊+🎥.\n"
                 "   3. Click your choice (only you can click your buttons!).\n   4. Wait for the magic! ✨\n\n"
                 "★** It's also work:** TG Group ✓\n👤 **Creator:-** @Hindu_papa ✓\n"
                 "🔐 **Fast, Efficient, and User-Friendly!**\n\n💀 **Stay Anonymous, Stay Powerful** 💀")
    reply_markup = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])
    try: await msg.reply_text(help_text, reply_markup=reply_markup, quote=True)
    except (UserIsBlocked, InputUserDeactivated): logger.warning(f"User {msg.from_user.id} blocked/deactivated during /help.")
    except Exception as e: logger.error(f"Error in /help: {e}", exc_info=True)

@app.on_message(filters.command(["broadcast", "podcast"]) & filters.user(OWNER_ID))
async def broadcast_command_handler(client: Client, msg: Message):
    if not msg.reply_to_message and len(msg.command) < 2:
        await msg.reply_text(
            "**📢 Podcast/Broadcast Usage:**\n\n"
            "1. Reply to a message with `/broadcast` or `/podcast` (to forward the replied message).\n"
            "2. Use `/broadcast <your message text>` or `/podcast <your message text>` (to send new text).\n\n"
            "This will send the message to all subscribed users (PM) and all active groups where the bot is a member.",
            quote=True); return

    broadcast_content_message = None; broadcast_text_content = ""
    if msg.reply_to_message:
        broadcast_content_message = msg.reply_to_message
        logger.info(f"Admin {msg.from_user.id} initiated broadcast by reply: msg_id {broadcast_content_message.id}")
    else:
        broadcast_text_content = msg.text.split(" ", 1)[1].strip()
        if not broadcast_text_content:
             await msg.reply_text("Please provide a message to broadcast or reply to a message.", quote=True); return
        logger.info(f"Admin {msg.from_user.id} initiated broadcast with text: {broadcast_text_content[:70]}...")

    if not subscribed_users and not active_groups:
        await msg.reply_text("No users or groups to broadcast to.", quote=True); return

    status_msg_text = f"🚀 **Starting Broadcast**\nTargeting: {len(subscribed_users)} users, {len(active_groups)} groups.\nThis may take a while..."
    status_update_msg = await msg.reply_text(status_msg_text, quote=True)

    s_users, f_users, s_groups, f_groups = 0,0,0,0
    removed_users, removed_groups = set(), set()

    async def send_to_entity(entity_id, is_group=False):
        nonlocal s_users, f_users, s_groups, f_groups # Allow modification of outer scope vars
        try:
            if broadcast_content_message: await broadcast_content_message.forward(chat_id=entity_id)
            elif broadcast_text_content: await client.send_message(entity_id, broadcast_text_content)
            if is_group: s_groups += 1
            else: s_users += 1
            return True
        except FloodWait as fw:
            logger.warning(f"FloodWait for {fw.value}s for {'group' if is_group else 'user'} {entity_id}. Sleeping...")
            await asyncio.sleep(fw.value + 5)
            return await send_to_entity(entity_id, is_group) # Retry
        except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, ChatWriteForbidden, UserNotParticipant) as e:
            logger.warning(f"{'Group' if is_group else 'User'} {entity_id} error: {type(e).__name__}. Removing.")
            if is_group: removed_groups.add(entity_id); f_groups +=1
            else: removed_users.add(entity_id); f_users +=1
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending to {'group' if is_group else 'user'} {entity_id}: {e}", exc_info=True)
            if is_group: f_groups +=1
            else: f_users +=1
            return False

    if subscribed_users:
        logger.info(f"Broadcasting to {len(subscribed_users)} users...")
        for user_id in list(subscribed_users):
            await send_to_entity(user_id, is_group=False)
            await asyncio.sleep(0.25) # Rate limiting

    if active_groups:
        logger.info(f"Broadcasting to {len(active_groups)} groups...")
        for group_id in list(active_groups):
            await send_to_entity(group_id, is_group=True)
            await asyncio.sleep(0.35) # Rate limiting for groups

    if removed_users:
        for uid in removed_users: subscribed_users.discard(uid)
        save_data(subscribed_users, USERS_FILE)
        logger.info(f"Removed {len(removed_users)} users. New count: {len(subscribed_users)}.")
    if removed_groups:
        for gid in removed_groups: active_groups.discard(gid)
        save_data(active_groups, GROUPS_FILE)
        logger.info(f"Removed {len(removed_groups)} groups. New count: {len(active_groups)}.")

    summary_text = (f"📢 **Broadcast Report**\n\n👤 Users: {s_users} sent, {f_users} failed. Now: {len(subscribed_users)}\n"
                    f"🏢 Groups: {s_groups} sent, {f_groups} failed. Now: {len(active_groups)}")
    try: await status_update_msg.edit_text(summary_text)
    except Exception as e_edit: await msg.reply_text(summary_text, quote=True)


@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command_handler(client: Client, msg: Message):
    await msg.reply_text(
        f"📊 **Bot Statistics**\n\n"
        f"Total Subscribed Users (PM): {len(subscribed_users)}\n"
        f"Total Active Groups: {len(active_groups)}\n"
        f"Active Media Downloads: {len(user_interaction_states)}",
        quote=True)

@app.on_message(filters.text & ~filters.command(["start", "help", "broadcast", "podcast", "stats"]))
async def link_handler(client: Client, msg: Message):
    original_user_id = msg.from_user.id; chat_id = msg.chat.id
    await add_chat_to_tracking(chat_id, msg.chat.type)
    url_match = re.match(r'(https?://[^\s]+)', msg.text.strip())
    
    if not url_match:
        if msg.chat.type == ChatType.PRIVATE:
             await msg.reply_text("Please send a valid media link.", quote=True)
        return
        
    url = url_match.group(1)
    logger.info(f"Received URL from {original_user_id} in {chat_id}: {url[:100]}")
    
    pre_check_ydl_opts = {
        'quiet': True, 'skip_download': True, 'noplaylist': True, 
        'extract_flat': 'discard_in_playlist', 'source_address': '0.0.0.0',
        'logger': logger 
    }
    extractor_key = "unknown" 
    try:
        with yt_dlp.YoutubeDL(pre_check_ydl_opts) as ydl:
            info = await asyncio.to_thread(ydl.extract_info, url, download=False)
        extractor_key = info.get('extractor_key', '').lower() if info else 'unknown'
        is_supported = any(target in extractor_key for target in TARGET_EXTRACTORS) if extractor_key != 'unknown' else False
        
        if not info or not extractor_key or not is_supported:
            logger.warning(f"URL '{url}' (extractor: '{extractor_key}') not supported or info extraction failed (pre-check).")
            if msg.chat.type == ChatType.PRIVATE:
                await msg.reply_text("Sorry, this link is not supported or I couldn't fetch its details.", quote=True)
            return
        logger.info(f"Pre-check OK: extractor '{extractor_key}' for URL: {url}")

    except yt_dlp.utils.DownloadError as de:
        de_str = str(de).lower()
        logger.warning(f"yt-dlp DownloadError during pre-check for '{url}' (extractor: {extractor_key}): {de_str[:200]}")
        
        proceed_anyway = False
        url_lower_for_check = url.lower() # Use lowercase url for site checking

        if (any(site in url_lower_for_check for site in ["instagram.com", "facebook.com", "youtube.com", "youtu.be"]) and
            any(err_key in de_str for err_key in ["login required", "unavailable", "restricted video", 
                                                  "not available", "sign in to confirm your age", 
                                                  "confirm your age", "age gate", "age-restricted"])): # Added "age-restricted"
            logger.warning(f"Pre-check for '{extractor_key}' indicated issue possibly solvable by cookies. Proceeding.")
            proceed_anyway = True
        elif "facebook.com" in url_lower_for_check and "unsupported url" in de_str and "/login/" in url: 
            logger.warning("Facebook pre-check: login redirect. Proceeding (cookies might help).")
            proceed_anyway = True
        
        if not proceed_anyway:
            if msg.chat.type == ChatType.PRIVATE:
                reply_err_msg = f"Could not process this link. Error (pre-check): {str(de).splitlines()[-1][:150]}"
                await msg.reply_text(reply_err_msg, quote=True)
            return
            
    except Exception as e:
        logger.error(f"Unexpected error during URL pre-check for '{url}': {e}", exc_info=True)
        if msg.chat.type == ChatType.PRIVATE:
             await msg.reply_text("An unexpected error occurred while checking the link.", quote=True)
        return
                
    status_message = await msg.reply_text(
        f"🔗 Link from {msg.from_user.mention} received! Choose an option:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Audio 🔊", callback_data="audio"),
             InlineKeyboardButton("Video 🎥", callback_data="video")],
            [InlineKeyboardButton("Audio+Video 🔊+🎥", callback_data="both")],
            [WHOAMI_BTN]
        ]),
        quote=True
    )
    user_interaction_states[status_message.id] = {
        "url": url, "status_message_id": status_message.id, "chat_id": chat_id, 
        "original_user_id": original_user_id, "original_message_id": msg.id, "last_update_time": 0.0
    }
    logger.info(f"Stored link for user {original_user_id}. Interaction key: {status_message.id}")

async def progress_hook(d, client: Client, chat_id_for_progress: int, status_message_id_for_progress: int, original_user_id_of_downloader: int):
    if status_message_id_for_progress not in user_interaction_states: return
    interaction_context = user_interaction_states[status_message_id_for_progress]
    try:
        current_time = asyncio.get_event_loop().time()
        if d['status'] == 'downloading':
            percentage = d.get('_percent_str', 'N/A').strip(); speed = d.get('_speed_str', 'N/A').strip(); eta = d.get('_eta_str', 'N/A').strip()
            downloaded_bytes = d.get('downloaded_bytes', 0); total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
            progress_text = f"Downloading... {percentage} ⏳"
            if total_bytes: progress_text += f"\n{sizeof_fmt(downloaded_bytes)} of {sizeof_fmt(total_bytes)}"
            progress_text += f"\nSpeed: {speed} | ETA: {eta}"
            if (current_time - interaction_context.get("last_update_time", 0.0)) > 2.5:
                if status_message_id_for_progress in user_interaction_states:
                     await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, progress_text)
                     user_interaction_states[status_message_id_for_progress]["last_update_time"] = current_time
                else: raise yt_dlp.utils.DownloadCancelled("Interaction ended by bot")
        elif d['status'] == 'finished':
            if status_message_id_for_progress in user_interaction_states: await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, "Download complete! Processing... ⚙️")
        elif d['status'] == 'error':
            logger.error(f"yt-dlp reported error (hook) for user {original_user_id_of_downloader}, int {status_message_id_for_progress}: {d.get('error', 'Unknown ytdlp error')}")
            if status_message_id_for_progress in user_interaction_states: await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, "Error during download process. 😕")
    except FloodWait as fw: logger.warning(f"FloodWait {fw.value}s in progress_hook. Waiting..."); await asyncio.sleep(fw.value + 2)
    except yt_dlp.utils.DownloadCancelled: logger.info(f"Download explicitly cancelled (hook) for {status_message_id_for_progress}."); raise
    except Exception as e:
        if status_message_id_for_progress in user_interaction_states: logger.warning(f"Error editing progress for {status_message_id_for_progress}: {e}")

def sizeof_fmt(num, suffix="B"):
    if not isinstance(num, (int, float)): return "N/A"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0: return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

@app.on_callback_query()
async def button_handler(client: Client, query: CallbackQuery):
    user_who_clicked_id = query.from_user.id
    interaction_key = query.message.id 
    logger.info(f"Callback '{query.data}' from {user_who_clicked_id} for msg {interaction_key}")

    if interaction_key not in user_interaction_states:
        await query.answer("❌ Session expired/invalid. Send link again.", show_alert=True)
        try: await query.message.delete()
        except: pass
        return

    interaction_data = user_interaction_states[interaction_key]
    original_user_id = interaction_data["original_user_id"]

    if user_who_clicked_id != original_user_id:
        await query.answer("⚠️ These buttons are for the link sender.", show_alert=True)
        return

    current_url_for_download = interaction_data["url"]
    chat_id = interaction_data["chat_id"]
    choice = query.data

    await query.answer(f"Processing: {choice}...", show_alert=False)
    try:
        await client.edit_message_text(chat_id, interaction_key, f"Initializing for '{choice}'... 🚀")
    except: pass

    download_path_template = os.path.join(DOWNLOAD_DIR, f"%(title).180s_{original_user_id}_{choice}.%(ext)s")
    main_event_loop = asyncio.get_running_loop()

    # ★★★ format_sort हटाया गया, format स्ट्रिंग पर निर्भर ★★★
    ydl_opts = {
        "outtmpl": download_path_template, "noplaylist": True, "quiet": False, "logger": logger,
        "merge_output_format": "mp4", "retries": 3, "fragment_retries": 10,
        "retry_sleep_functions": {'http': lambda n: min(n * 2, 30)}, # Exponential backoff
        "geo_bypass": True, "nocheckcertificate": True, "ignoreerrors": False, # Don't ignore errors by default
        "source_address": "0.0.0.0", "restrictfilenames": True,
        "progress_hooks": [lambda d: asyncio.run_coroutine_threadsafe(
            progress_hook(d, client, chat_id, interaction_key, original_user_id), main_event_loop
        ).result(timeout=25)], # Timeout बढ़ाया
        "postprocessor_hooks": [lambda d: logger.info(f"Postprocessor (user {original_user_id}, int {interaction_key}): {d['status']}") if d.get('status') != 'started' else None],
        # "format_sort": [...] # ★★★ यह लाइन हटाई गई ★★★
        "prefer_ffmpeg": True, 
        "postprocessors": [] 
    }

    original_downloaded_file = None
    extracted_audio_file_for_both = None
    info_dict_for_metadata = None
    
    temp_cookie_file_to_use = None; active_cookie_content = None; temp_cookie_filename_for_provider = None
    url_lower = current_url_for_download.lower()

    # ★★★ YouTube डोमेन चेकिंग सुधारा गया ★★★
    if ("instagram.com" in url_lower and INSTAGRAM_COOKIES_CONTENT):
        active_cookie_content, temp_cookie_filename_for_provider = INSTAGRAM_COOKIES_CONTENT, TEMP_INSTA_COOKIES_FILENAME
        logger.info("Instagram link: Will use Insta cookies.")
    elif ("facebook.com" in url_lower and FACEBOOK_COOKIES_CONTENT):
        active_cookie_content, temp_cookie_filename_for_provider = FACEBOOK_COOKIES_CONTENT, TEMP_FB_COOKIES_FILENAME
        logger.info("Facebook link: Will use FB cookies.")
    elif (any(yt_domain in url_lower for yt_domain in ["youtube.com/", "youtu.be/"]) and YOUTUBE_COOKIES_CONTENT): # Better check
        active_cookie_content, temp_cookie_filename_for_provider = YOUTUBE_COOKIES_CONTENT, TEMP_YT_COOKIES_FILENAME
        logger.info("YouTube link: Will use YT cookies.")

    if active_cookie_content and temp_cookie_filename_for_provider:
        temp_cookie_file_to_use = os.path.join(DOWNLOAD_DIR, temp_cookie_filename_for_provider)
        try:
            with open(temp_cookie_file_to_use, 'w', encoding='utf-8') as f_cookie: f_cookie.write(active_cookie_content)
            ydl_opts['cookiefile'] = temp_cookie_file_to_use
            logger.info(f"Using temporary cookie file for download: {temp_cookie_file_to_use}")
        except Exception as e_cookie:
            logger.error(f"Failed to create/write temp cookie file {temp_cookie_file_to_use}: {e_cookie}"); temp_cookie_file_to_use = None

    try:
        if choice == "audio":
            ydl_opts['format'] = "bestaudio[ext=m4a]/bestaudio/best" # m4a is good source for mp3
            ydl_opts['postprocessors'].append({
                'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 
                'preferredquality': '192', # Standard good quality
                'nopostoverwrites': False 
            })
        elif choice in ["video", "both"]:
             # ★★★ बेहतर क्वालिटी के लिए format स्ट्रिंग ★★★
            ydl_opts['format'] = (
                "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=1080][ext=webm]+bestaudio[ext=webm]/bestvideo[ext=mp4]+bestaudio[ext=m4a]/" # 1080p MP4/WebM
                "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=720][ext=webm]+bestaudio[ext=webm]/bestvideo[ext=mp4]+bestaudio[ext=m4a]/"   # 720p MP4/WebM
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]/" # Best MP4
                "bestvideo+bestaudio/best" # Fallback to best available, yt-dlp will merge
            )
        else:
            await client.edit_message_text(chat_id, interaction_key, "❌ Invalid choice."); return

        logger.info(f"User {original_user_id} selected '{choice}'. URL: {current_url_for_download}. ydl_opts format set for choice.")
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                await client.edit_message_text(chat_id, interaction_key, "Fetching media info (this might take a moment)... ℹ️")
                info_dict_for_metadata = await asyncio.to_thread(ydl.extract_info, current_url_for_download, download=False) # Get metadata first
                if not info_dict_for_metadata: raise yt_dlp.utils.DownloadError("Failed to extract media info (pre-download).")
                
                media_title = clean_filename(info_dict_for_metadata.get('title', 'Untitled_Media'))
                await client.edit_message_text(chat_id, interaction_key, f"Starting download: `{media_title[:60]}...` 📥\n_(This can take time depending on size/speed)_")
                
                # Process and download
                final_info_after_download = await asyncio.to_thread(ydl.process_ie_result, info_dict_for_metadata, download=True)

                # Determine downloaded file path
                if final_info_after_download.get('filepath') and os.path.exists(final_info_after_download['filepath']):
                    original_downloaded_file = final_info_after_download['filepath']
                elif 'requested_downloads' in final_info_after_download and final_info_after_download['requested_downloads']:
                     filepath_from_req = final_info_after_download['requested_downloads'][0].get('filepath')
                     if filepath_from_req and os.path.exists(filepath_from_req):
                         original_downloaded_file = filepath_from_req
                else: # Fallback based on template
                    _title_final = clean_filename(info_dict_for_metadata.get("title", "untitled_dl"))
                    _ext_final = final_info_after_download.get('ext') or info_dict_for_metadata.get('ext', 'mp4') # Use ext from final_info if available
                    if choice == "audio" and ydl_opts.get('postprocessors') and ydl_opts['postprocessors'][0]['key'] == 'FFmpegExtractAudio':
                         _ext_final = ydl_opts['postprocessors'][0]['preferredcodec']
                    
                    _expected_file_path = os.path.join(DOWNLOAD_DIR, f"{_title_final}_{original_user_id}_{choice}.{_ext_final}")
                    if os.path.exists(_expected_file_path):
                        original_downloaded_file = _expected_file_path
                    else: # If still not found, list directory for debugging
                        logger.error(f"File '{_expected_file_path}' not found by template. Yt-dlp provided path: {final_info_after_download.get('filepath', 'N/A')}")
                        try: logger.info(f"Debug: Contents of {DOWNLOAD_DIR}: {os.listdir(DOWNLOAD_DIR)}")
                        except Exception as e_ls: logger.error(f"Could not list {DOWNLOAD_DIR}: {e_ls}")
                        raise yt_dlp.utils.DownloadError(f"Could not reliably determine downloaded file path for '{_title_final}'.")

                if not original_downloaded_file or not os.path.exists(original_downloaded_file):
                    raise yt_dlp.utils.DownloadError(f"File path missing or file does not exist after download attempt: {original_downloaded_file}")
                
                logger.info(f"File downloaded/processed to: {original_downloaded_file}")

            except yt_dlp.utils.DownloadError as de:
                de_str = str(de).lower(); error_message = str(de).splitlines()[-1] if str(de).splitlines() else str(de)
                logger.error(f"yt-dlp DownloadError (user {original_user_id}): {error_message[:250]}", exc_info=False)
                reply_error = f"❌ Download Error: `{error_message[:200]}`"
                if any(key in de_str for key in ["login required", "authentication", "private video"]):
                     reply_error = f"❌ Error: Content private/requires login. Admin: check cookies for this platform."
                elif any(key in de_str for key in ["age restricted", "age gate", "confirm your age", "sign in to confirm your age"]):
                    reply_error = f"❌ Error: Age restricted. Admin: ensure cookies are from an appropriate account."
                elif "unsupported url" in de_str: reply_error = f"❌ Error: Unsupported URL."
                elif "no space left on device" in de_str: reply_error = "❌ Error: No space left on server to download."
                try: await client.edit_message_text(chat_id, interaction_key, reply_error)
                except: pass
                return
            except Exception as e:
                logger.error(f"Error during yt-dlp processing (user {original_user_id}): {e}", exc_info=True)
                try: await client.edit_message_text(chat_id, interaction_key, f"❌ Unexpected download error: `{str(e)[:150]}`")
                except: pass
                return

        title_final = clean_filename(info_dict_for_metadata.get("title", "media_file"))
        duration_seconds = info_dict_for_metadata.get("duration")
        duration_str = f"{int(duration_seconds // 60):02d}:{int(duration_seconds % 60):02d}" if isinstance(duration_seconds, (int, float)) and duration_seconds > 0 else "N/A"
        filesize_str = sizeof_fmt(os.path.getsize(original_downloaded_file)) if original_downloaded_file and os.path.exists(original_downloaded_file) else "N/A"
        
        base_caption = (f"🎬 **Title:** `{title_final}`\n⏱️ **Duration:** `{duration_str}` | 💾 **Size:** `{filesize_str}`\n"
                        f"🥀 **Requested by:** {query.from_user.mention}\n👤 **Creator:** @Hindu_papa ✓")
        final_ui = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])

        try: await client.edit_message_text(chat_id, interaction_key, f"Preparing '{choice}' to send... 🎁")
        except: pass

        reply_to_msg_id = interaction_data["original_message_id"]
        final_output_file_to_send = original_downloaded_file

        if not os.path.exists(final_output_file_to_send):
            logger.error(f"CRITICAL: Output file {final_output_file_to_send} for sending does NOT exist!"); 
            await client.send_message(chat_id, "❌ Error: Final file vanished before sending!", reply_to_message_id=reply_to_msg_id); return

        if choice == "audio":
            await client.send_audio(chat_id, audio=final_output_file_to_send, caption=f"**🔊 Audio Extracted!**\n\n{base_caption}",
                                    title=title_final[:30], duration=int(duration_seconds or 0), reply_markup=final_ui, reply_to_message_id=reply_to_msg_id)
        elif choice == "video":
            await client.send_video(chat_id, video=final_output_file_to_send, caption=f"**🎥 Video Downloaded!**\n\n{base_caption}",
                                    duration=int(duration_seconds or 0), reply_markup=final_ui, reply_to_message_id=reply_to_msg_id)
        elif choice == "both":
            await client.send_video(chat_id, video=final_output_file_to_send, caption=f"**🎥 Video Part!**\n\n{base_caption}",
                                    duration=int(duration_seconds or 0), reply_markup=final_ui, reply_to_message_id=reply_to_msg_id)
            
            extracted_audio_file_for_both = os.path.join(DOWNLOAD_DIR, f"{clean_filename(title_final)}_{original_user_id}_audio_extract.mp3")
            logger.info(f"Attempting to extract audio for 'Both' to: {extracted_audio_file_for_both}")
            try: await client.edit_message_text(chat_id, interaction_key, "Extracting audio for 'Both'... 🔊")
            except: pass
            
            ffmpeg_command = ['ffmpeg', '-i', final_output_file_to_send, '-vn', '-acodec', 'libmp3lame', '-b:a', '192k', '-ar', '44100', '-y', extracted_audio_file_for_both]
            if await run_ffmpeg(ffmpeg_command):
                if os.path.exists(extracted_audio_file_for_both):
                    audio_filesize_both = sizeof_fmt(os.path.getsize(extracted_audio_file_for_both))
                    audio_caption_both = (f"🎬 **Title:** `{title_final} (Audio)`\n⏱️ **Duration:** `{duration_str}` | 💾 **Size:** `{audio_filesize_both}`\n"
                                       f"🥀 **Requested by:** {query.from_user.mention}\n👤 **Creator:** @Hindu_papa ✓")
                    await client.send_audio(chat_id, audio=extracted_audio_file_for_both, caption=f"**🔊 Audio Part!**\n\n{audio_caption_both}",
                                            title=title_final[:30], duration=int(duration_seconds or 0), reply_markup=final_ui, reply_to_message_id=reply_to_msg_id)
                else:
                    logger.error(f"FFmpeg success for 'Both', but audio file NOT FOUND: {extracted_audio_file_for_both}")
                    await client.send_message(chat_id, "⚠️ Failed to find audio for 'Both' (file not found after extraction), video was sent.", reply_to_message_id=reply_to_msg_id)
            else:
                logger.error(f"FFmpeg failed for 'Both' (user {original_user_id}). Video: {final_output_file_to_send}")
                await client.send_message(chat_id, "⚠️ Failed to extract audio for 'Both' (ffmpeg error), video was sent.", reply_to_message_id=reply_to_msg_id)
        
        try: 
            if interaction_key in user_interaction_states: await client.delete_messages(chat_id, interaction_key)
        except: pass

    except FloodWait as fw: 
        logger.warning(f"FloodWait {fw.value}s in button_handler. Waiting..."); await asyncio.sleep(fw.value + 2)
    except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, ChatWriteForbidden) as e:
        logger.warning(f"User/Chat error for {original_user_id} (interaction {interaction_key}): {type(e).__name__}")
        if original_user_id in subscribed_users: subscribed_users.discard(original_user_id); save_data(subscribed_users, USERS_FILE)
        if chat_id in active_groups and isinstance(e, (ChatWriteForbidden, PeerIdInvalid)):
            active_groups.discard(chat_id); save_data(active_groups, GROUPS_FILE)
            logger.info(f"Removed group {chat_id} due to {type(e).__name__}.")
    except Exception as e:
        logger.error(f"Overall error in button_handler (user {original_user_id}, interaction {interaction_key}): {e}", exc_info=True)
        try:
            error_reply_text = f"❌ Critical error: `{str(e)[:200]}`. Please try again or report to admin."
            if interaction_key in user_interaction_states and query.message:
                 await client.edit_message_text(chat_id, interaction_key, error_reply_text)
            elif query.message and query.message.chat: # Fallback if status message was deleted
                await client.send_message(chat_id, error_reply_text, reply_to_message_id=interaction_data.get("original_message_id"))
        except Exception as e_reply: logger.error(f"Failed to send critical error message to user {original_user_id}: {e_reply}")
    finally:
        # Clean up temporary cookie files
        for temp_cookie_file_path_to_clean in [
            os.path.join(DOWNLOAD_DIR, TEMP_INSTA_COOKIES_FILENAME),
            os.path.join(DOWNLOAD_DIR, TEMP_FB_COOKIES_FILENAME),
            os.path.join(DOWNLOAD_DIR, TEMP_YT_COOKIES_FILENAME)
        ]:
            if temp_cookie_file_path_to_clean and os.path.exists(temp_cookie_file_path_to_clean):
                try:
                    os.remove(temp_cookie_file_path_to_clean)
                    logger.info(f"Deleted temporary cookie file: {temp_cookie_file_path_to_clean}")
                except Exception as e_clean_cookie:
                    logger.error(f"Error deleting temp cookie file {temp_cookie_file_path_to_clean}: {e_clean_cookie}")

        # Clean up downloaded media files
        files_to_clean = set()
        if original_downloaded_file and os.path.exists(original_downloaded_file): files_to_clean.add(original_downloaded_file)
        if extracted_audio_file_for_both and os.path.exists(extracted_audio_file_for_both): files_to_clean.add(extracted_audio_file_for_both)
        
        for f_path in files_to_clean:
            if f_path and os.path.exists(f_path):
                try: os.remove(f_path); logger.info(f"Cleaned media file: {f_path}")
                except Exception as e_clean: logger.error(f"Error cleaning media file {f_path}: {e_clean}")
        
        if interaction_key in user_interaction_states: # Clear state at the very end
            user_interaction_states.pop(interaction_key, None)
            logger.info(f"Cleared state for interaction key {interaction_key}")

if __name__ == "__main__":
    logger.info("Bot starting up...")
    # Startup warnings for missing cookies
    if not INSTAGRAM_COOKIES_CONTENT: logger.warning("INSTAGRAM_COOKIES_CONTENT ENV VAR not set. Some Instagram downloads may fail.")
    if not FACEBOOK_COOKIES_CONTENT: logger.warning("FACEBOOK_COOKIES_CONTENT ENV VAR not set. Some Facebook downloads may fail.")
    if not YOUTUBE_COOKIES_CONTENT: logger.warning("YOUTUBE_COOKIES_CONTENT ENV VAR not set. Some YouTube downloads may fail.")
    
    try:
        if app:
            app.start(); logger.info("Pyrogram Client started. Bot is now online!"); idle()
            logger.info("Bot received stop signal, shutting down..."); app.stop(); logger.info("Bot has been stopped gracefully.")
        else: logger.critical("CRITICAL ERROR: Pyrogram Client (app) was not initialized.")
    except RuntimeError as e:
        if "another loop" in str(e).lower() or "different loop" in str(e).lower(): logger.warning(f"Asyncio loop error during shutdown (often ignorable): {e}")
        else: logger.critical(f"Runtime error: {e}", exc_info=True)
    except KeyboardInterrupt: logger.info("Bot stopped by user (KeyboardInterrupt).")
    except Exception as e: logger.critical(f"Unexpected top-level error, bot stopping: {e}", exc_info=True)
    finally: 
        if app and app.is_running: app.stop() # Ensure app is stopped in any case
        logger.info("Script execution finished.")

