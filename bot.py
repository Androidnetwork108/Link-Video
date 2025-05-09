# ★It's Maked by:- @Hindu_papa✓
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
# Load sensitive information from environment variables
# On Railway.app, you'll set these in the "Variables" section of your service.
# For local development, you can set these environment variables in your shell
# or create a .env file and use a library like python-dotenv to load them.

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID_STR = os.environ.get("OWNER_ID")

# Validate that environment variables are set
if not API_ID:
    raise ValueError("Missing API_ID environment variable. Please set it.")
if not API_HASH:
    raise ValueError("Missing API_HASH environment variable. Please set it.")
if not BOT_TOKEN:
    raise ValueError("Missing BOT_TOKEN environment variable. Please set it.")
if not OWNER_ID_STR:
    raise ValueError("Missing OWNER_ID environment variable. Please set it.")

try:
    API_ID = int(API_ID)
except ValueError:
    raise ValueError("API_ID environment variable must be an integer.")

try:
    OWNER_ID = int(OWNER_ID_STR)
except ValueError:
    raise ValueError("OWNER_ID environment variable must be an integer.")


DOWNLOAD_DIR = "./downloads"
USERS_FILE = "bot_users.json" # File to store user IDs for broadcast
GROUPS_FILE = "bot_groups.json" # File to store group chat IDs for broadcast

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

app = Client("media_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

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
    # User tracking (subscribed_users) is handled in /start for direct user interaction in private.

# --- Constants & Buttons ---
WHOAMI_BTN = InlineKeyboardButton("WHO I AM? 👤", url="https://t.me/MeNetwork108/14")
CREATOR_BTN = InlineKeyboardButton("Made By ❤️", url="https://t.me/Hindu_papa")

TARGET_EXTRACTORS = ['youtube', 'youtu.be', 'instagram', 'facebook', 'fb']

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
        return False
    logger.info(f"FFmpeg Success (first 200 chars of stdout): {stdout.decode(errors='ignore').strip()[:200]}")
    return True

user_interaction_states = {}


# --- Pyrogram Handlers ---
@app.on_message(filters.command("start"))
async def start_command(client: Client, msg: Message):
    user_id = msg.from_user.id
    chat_id = msg.chat.id

    logger.info(f"Received /start command from user {user_id} ({msg.from_user.first_name}) in chat {chat_id} (type: {msg.chat.type})")

    await add_chat_to_tracking(chat_id, msg.chat.type)

    if msg.chat.type == ChatType.PRIVATE: # Only add to subscribed_users if it's a private chat initiated by user
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

    await add_chat_to_tracking(chat_id, msg.chat.type) # Track group if help is used in a group

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
        if user_id in subscribed_users: # Ensure removal if they were subscribed
             subscribed_users.discard(user_id)
             save_users(subscribed_users)
    except Exception as e:
        logger.error(f"Error replying to /help for user {user_id} in chat {chat_id}: {e}", exc_info=True)

# Admin commands for broadcast (Podcast)
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
    source_chat_id_for_forward = msg.chat.id # Chat where the /broadcast command was issued

    if msg.reply_to_message:
        message_to_forward_id = msg.reply_to_message.id
        source_chat_id_for_forward = msg.reply_to_message.chat.id # Actual chat of the message to forward
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

    # Counters
    successful_user_sends = 0
    failed_user_sends = 0
    successful_group_sends = 0
    failed_group_sends = 0

    users_to_remove = set()
    groups_to_remove = set()

    # --- Broadcast to Users (Private Chats) ---
    if subscribed_users:
        logger.info(f"Broadcasting to {len(subscribed_users)} subscribed users...")
        for user_id_to_broadcast in list(subscribed_users): # Iterate over a copy
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
                await asyncio.sleep(e.value + 5) # Add a buffer
                try: # Retry after sleep
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
            await asyncio.sleep(0.25) # Rate limit: 4 messages per second to different users

    if users_to_remove:
        logger.info(f"Attempting to remove {len(users_to_remove)} users from subscription list post-broadcast.")
        for uid_remove in users_to_remove:
            subscribed_users.discard(uid_remove)
        save_users(subscribed_users)
        logger.info(f"Successfully removed users. New user count: {len(subscribed_users)}.")

    # --- Broadcast to Groups ---
    if active_groups:
        logger.info(f"Broadcasting to {len(active_groups)} active groups...")
        for group_id_to_broadcast in list(active_groups): # Iterate over a copy
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
            except (PeerIdInvalid, ChatWriteForbidden, UserNotParticipant) as e: # Specific errors indicating bot can't send to this group anymore
                logger.warning(f"Failed to send to group {group_id_to_broadcast}: {type(e).__name__}. Removing group from active list.")
                groups_to_remove.add(group_id_to_broadcast)
                failed_group_sends += 1
            except (UserIsBlocked, InputUserDeactivated) as e: # These are user-specific but might occur if a group_id is somehow a user_id
                logger.warning(f"User-specific error {type(e).__name__} for group target {group_id_to_broadcast}. This group ID might be invalid or a user. Removing.")
                groups_to_remove.add(group_id_to_broadcast)
                failed_group_sends += 1
            except FloodWait as e:
                logger.warning(f"FloodWait for {e.value}s during broadcast to group {group_id_to_broadcast}. Sleeping...")
                await asyncio.sleep(e.value + 5) # Add a buffer
                try: # Retry after sleep
                    if message_to_forward_id: await client.forward_messages(chat_id=group_id_to_broadcast, from_chat_id=source_chat_id_for_forward, message_ids=message_to_forward_id)
                    elif broadcast_text: await client.send_message(group_id_to_broadcast, broadcast_text)
                    successful_group_sends += 1
                except Exception as retry_e:
                    logger.error(f"Retry failed for group {group_id_to_broadcast} after FloodWait: {retry_e}")
                    # Decide if this warrants removal or just counts as a failed attempt
                    if isinstance(retry_e, (PeerIdInvalid, ChatWriteForbidden, UserNotParticipant)):
                        groups_to_remove.add(group_id_to_broadcast)
                    failed_group_sends += 1
            except Exception as e:
                logger.error(f"An unexpected error occurred sending to group {group_id_to_broadcast}: {e}", exc_info=True)
                if "chat_admin_required" in str(e).lower():
                     logger.warning(f"Group {group_id_to_broadcast}: Bot lacks admin rights to send message. Not removing, but counted as failed.")
                # For other generic errors, count as failed. Consider if removal is needed based on error type.
                failed_group_sends += 1
            await asyncio.sleep(0.35) # Rate limit: ~3 messages per second to different groups

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
    except Exception as e_edit: # Original status message might have issues or been deleted
        logger.warning(f"Could not edit broadcast status message: {e_edit}")
        await msg.reply_text(summary_text, quote=True) # Send as a new message

@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_command_handler(client: Client, msg: Message):
    await msg.reply_text(
        f"📊 **Bot Statistics**\n\n"
        f"Total Subscribed Users (for PM broadcast): {len(subscribed_users)}\n"
        f"Total Active Groups (for group broadcast): {len(active_groups)}\n"
        f"Active Media Download Interactions: {len(user_interaction_states)}",
        quote=True
    )

@app.on_message(filters.text & ~filters.command(["start", "help", "broadcast", "podcast", "stats"])) # Handles links in DMs and groups
async def link_handler(client: Client, msg: Message):
    original_user_id = msg.from_user.id
    chat_id = msg.chat.id

    # Track group if link is sent in a group
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

    try:
        with yt_dlp.YoutubeDL({'quiet': True, 'skip_download': True, 'noplaylist': True, 'extract_flat': 'discard_in_playlist', 'source_address': '0.0.0.0'}) as ydl:
            info = await asyncio.to_thread(ydl.extract_info, url, download=False)

        extractor_key = info.get('extractor_key', '').lower() if info else ''
        is_supported_extractor = any(target in extractor_key for target in TARGET_EXTRACTORS)

        if not info or not extractor_key or not is_supported_extractor:
            logger.warning(f"URL '{url}' from user {original_user_id} in chat {chat_id} is not from a supported platform or extractor not recognized ('{extractor_key}').")
            if msg.chat.type == ChatType.PRIVATE: # Reply only if in private chat
                await msg.reply_text("Sorry, I can only process links from YouTube, Instagram, and Facebook video/reel posts.", quote=True)
            return
        logger.info(f"yt-dlp recognized supported extractor '{extractor_key}' for URL: {url}")

    except yt_dlp.utils.DownloadError as de:
        logger.warning(f"yt-dlp DownloadError during pre-check for {url} from user {original_user_id} in chat {chat_id}: {de}.")
        if msg.chat.type == ChatType.PRIVATE:
            await msg.reply_text(f"Could not process this link. Error: {str(de)[:100]}", quote=True)
        return
    except Exception as e:
        logger.error(f"Unexpected error during URL pre-check for {url} from user {original_user_id} in chat {chat_id}: {e}", exc_info=True)
        if msg.chat.type == ChatType.PRIVATE:
             await msg.reply_text("An unexpected error occurred while checking the link.", quote=True)
        return

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
            if (current_time - last_update) > 2.5:
                await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, progress_text)
                interaction_context["last_update_time"] = current_time
        elif d['status'] == 'finished':
            await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, "Download complete! Processing... ⚙️")
        elif d['status'] == 'error':
            logger.error(f"yt-dlp reported error hook for user {original_user_id_of_downloader}, interaction {status_message_id_for_progress}: {d.get('error', 'Unknown error')}")
            await client.edit_message_text(chat_id_for_progress, status_message_id_for_progress, "Error during download process. 😕")
    except FloodWait as fw:
        logger.warning(f"FloodWait {fw.value}s updating progress for user {original_user_id_of_downloader} (interaction {status_message_id_for_progress}). Waiting...")
        await asyncio.sleep(fw.value + 0.5)
    except Exception as e:
        logger.warning(f"Failed to edit progress message for user {original_user_id_of_downloader}, interaction {status_message_id_for_progress}: {e}")


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

    logger.info(f"Callback query '{query.data}' from user {user_who_clicked_id} ({query.from_user.first_name}) for message ID {interaction_key}")

    if interaction_key not in user_interaction_states:
        await query.answer("❌ This download session has expired or is invalid. Please send the link again.", show_alert=True)
        try: await query.message.delete()
        except Exception: pass
        return

    interaction_data = user_interaction_states[interaction_key]
    original_user_id = interaction_data["original_user_id"]

    if user_who_clicked_id != original_user_id:
        await query.answer("⚠️ These buttons are for the user who sent the link. Please send your own link if you want to download.", show_alert=True)
        return

    url = interaction_data["url"]
    chat_id = interaction_data["chat_id"]
    choice = query.data

    await query.answer(f"Processing your choice: {choice}...", show_alert=False)
    try:
        await client.edit_message_text(chat_id, interaction_key, f"Initializing download for '{choice}'... 🚀")
    except Exception: pass

    download_path_template = os.path.join(DOWNLOAD_DIR, f"%(title).180s_{original_user_id}_{choice}.%(ext)s")
    main_event_loop = asyncio.get_running_loop()

    ydl_opts = {
        "outtmpl": download_path_template, "noplaylist": True, "quiet": False, "merge_output_format": "mp4",
        "retries": 2, "geo_bypass": True, "nocheckcertificate": True, "ignoreerrors": False,
        "source_address": "0.0.0.0",
        "progress_hooks": [
            lambda d: asyncio.run_coroutine_threadsafe(
                progress_hook(d, client, chat_id, interaction_key, original_user_id), main_event_loop
            ).result(timeout=10) # Added a timeout to prevent potential deadlocks
        ],
        "postprocessor_hooks": [lambda d: logger.info(f"Postprocessor status for user {original_user_id} (interaction: {interaction_key}): {d['status']}") if d.get('status') != 'started' else None],
        "format_sort": ['res:1080', 'ext:mp4:m4a'], "restrictfilenames": True,
    }

    original_downloaded_file = None
    extracted_audio_file_for_both = None
    info_dict = None

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
            user_interaction_states.pop(interaction_key, None)
            return

        logger.info(f"User {original_user_id} (interaction: {interaction_key}) selected '{choice}'. Starting download for URL: {url}")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                await client.edit_message_text(chat_id, interaction_key, "Fetching media info... ℹ️")
                info_dict = await asyncio.to_thread(ydl.extract_info, url, download=False)
                if not info_dict:
                    raise yt_dlp.utils.DownloadError("Failed to extract media information (info_dict is None).")

                media_title = clean_filename(info_dict.get('title', 'Untitled_Media'))
                await client.edit_message_text(chat_id, interaction_key, f"Starting download: `{media_title[:60]}...` 📥")

                download_info = await asyncio.to_thread(ydl.extract_info, url, download=True)

                if 'requested_downloads' in download_info and download_info['requested_downloads'] and \
                   'filepath' in download_info['requested_downloads'][0] and \
                   os.path.exists(download_info['requested_downloads'][0]['filepath']):
                    original_downloaded_file = download_info['requested_downloads'][0]['filepath']
                elif 'filename' in download_info and os.path.exists(download_info['filename']): # Legacy yt-dlp might use 'filename'
                    original_downloaded_file = download_info['filename']
                else: # Fallback based on template (less reliable)
                    _title_final = clean_filename(download_info.get("title", info_dict.get("title", "untitled_download")))
                    _ext_final = "mp3" if choice == "audio" and ydl_opts.get('postprocessors') else download_info.get('ext', info_dict.get('ext','mp4'))
                    _expected_base_name = f"{_title_final}_{original_user_id}_{choice}"
                    _expected_file_path = os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.{_ext_final}")

                    if os.path.exists(_expected_file_path):
                        original_downloaded_file = _expected_file_path
                    # Additional checks if primary expected path failed.
                    elif choice == "audio" and _ext_final == "mp3":
                        # Check for the original extension before postprocessing if mp3 not found
                        original_ext_before_pp = download_info.get('ext', info_dict.get('ext'))
                        if original_ext_before_pp and original_ext_before_pp != 'mp3':
                             _path_before_pp = os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.{original_ext_before_pp}")
                             if os.path.exists(_path_before_pp):
                                original_downloaded_file = _path_before_pp # Will be converted if ydl_opts specify postprocessor
                        elif os.path.exists(os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.m4a")): # Explicit check for m4a
                             original_downloaded_file = os.path.join(DOWNLOAD_DIR, f"{_expected_base_name}.m4a")
                        elif os.path.exists(os.path.join(DOWNLOAD_DIR, f"{clean_filename(info_dict.get('title', 'Untitled_Media'))}_{original_user_id}_{choice}.mp3")): # Simpler name
                            original_downloaded_file = os.path.join(DOWNLOAD_DIR, f"{clean_filename(info_dict.get('title', 'Untitled_Media'))}_{original_user_id}_{choice}.mp3")

                    if not original_downloaded_file:
                        logger.warning(f"Could not find exact file for {_title_final} (interaction {interaction_key}). Checking for files matching pattern...")
                        # Fallback: list files in download_dir and try to find a match
                        possible_files = [f for f in os.listdir(DOWNLOAD_DIR) if _expected_base_name.rsplit('.',1)[0] in f and f.startswith(_title_final[:50])] # Match start of title and user/choice part
                        if possible_files:
                            original_downloaded_file = os.path.join(DOWNLOAD_DIR, possible_files[0])
                            logger.info(f"Found a candidate file by pattern matching: {original_downloaded_file}")
                        else:
                             raise yt_dlp.utils.DownloadError(f"Could not reliably determine downloaded file path for '{_title_final}' after download (interaction {interaction_key}). Check 'filepath' in ydl output or download directory.")


                if not original_downloaded_file or not os.path.exists(original_downloaded_file):
                    raise yt_dlp.utils.DownloadError(f"File path missing or file does not exist after download attempt: {original_downloaded_file} (interaction {interaction_key})")
                logger.info(f"File for user {original_user_id} (interaction: {interaction_key}) downloaded/processed to: {original_downloaded_file}")

            except yt_dlp.utils.DownloadError as de:
                error_message = str(de).splitlines()[-1] if str(de).splitlines() else str(de)
                logger.error(f"yt-dlp DownloadError for user {original_user_id} (interaction: {interaction_key}) on URL {url}: {error_message}", exc_info=False) # exc_info=False to avoid huge logs for common errors
                reply_error = f"❌ Download Error: `{error_message[:250]}`"
                if "Unsupported URL" in str(de): reply_error = f"❌ Error: Unsupported URL."
                elif "private" in str(de).lower() or "login required" in str(de).lower() or "authentication" in str(de).lower():
                     reply_error = f"❌ Error: This content might be private or require login."
                try: await client.edit_message_text(chat_id, interaction_key, reply_error)
                except Exception: pass # Message might have been deleted or inaccessible
                user_interaction_states.pop(interaction_key, None) # Clear state on download error
                return
            except Exception as e:
                logger.error(f"Error during yt-dlp processing for user {original_user_id} (interaction: {interaction_key}): {e}", exc_info=True)
                try: await client.edit_message_text(chat_id, interaction_key, f"❌ An unexpected error during download: `{str(e)[:200]}`")
                except Exception: pass
                user_interaction_states.pop(interaction_key, None) # Clear state on unexpected error
                return

        # Ensure info_dict is available for metadata
        if not info_dict: # If download succeeded but info_dict was lost (e.g. error in initial fetch but download worked)
            logger.warning(f"info_dict was not populated for interaction {interaction_key}, attempting to re-fetch metadata (lightly).")
            try:
                with yt_dlp.YoutubeDL({'quiet': True, 'skip_download': True, 'noplaylist': True, 'extract_flat': 'discard_in_playlist'}) as ydl_meta:
                    info_dict = await asyncio.to_thread(ydl_meta.extract_info, url, download=False)
            except Exception as meta_e:
                logger.error(f"Could not re-fetch metadata for interaction {interaction_key}: {meta_e}")
                # Use filename as title if metadata fails
                info_dict = {'title': os.path.basename(original_downloaded_file).rsplit('_',3)[0] if original_downloaded_file else "media_file", 'duration': None}


        title_final = clean_filename(info_dict.get("title", "media_file"))
        duration_seconds = info_dict.get("duration")
        duration_str = f"{int(duration_seconds // 60):02d}:{int(duration_seconds % 60):02d}" if isinstance(duration_seconds, (int, float)) else "N/A"
        filesize_str = sizeof_fmt(os.path.getsize(original_downloaded_file)) if original_downloaded_file and os.path.exists(original_downloaded_file) else "N/A"

        base_caption = (
            f"🎬 **Title:** `{title_final}`\n"
            f"⏱️ **Duration:** `{duration_str}` | 💾 **Size:** `{filesize_str}`\n"
            f"🥀 **Requested by:** {query.from_user.mention}\n"
            f"👤 **Creator:** @Hindu_papa ✓"
        )
        final_ui = InlineKeyboardMarkup([[CREATOR_BTN, WHOAMI_BTN]])

        try: await client.edit_message_text(chat_id, interaction_key, f"Preparing to send your '{choice}'... 🎁")
        except Exception: pass # Message might have been deleted

        reply_to_msg_id = interaction_data["original_message_id"]

        # Determine the actual final file path after postprocessing
        final_output_file = original_downloaded_file
        if choice == "audio" and ydl_opts.get('postprocessors'):
            # yt-dlp with FFmpegExtractAudio renames the file to .mp3 (or preferredcodec)
            base, _ = os.path.splitext(original_downloaded_file)
            expected_audio_file = base + "." + ydl_opts['postprocessors'][0]['preferredcodec']
            if os.path.exists(expected_audio_file):
                final_output_file = expected_audio_file
            elif not os.path.exists(original_downloaded_file): # If original is gone and new one not found
                 logger.error(f"Postprocessed audio file {expected_audio_file} not found, and original {original_downloaded_file} also missing for interaction {interaction_key}")
                 raise FileNotFoundError(f"Could not find audio file after postprocessing for {title_final}")
            # If expected audio file doesn't exist but original still does, it means postprocessing might have failed or not run
            # In this case, final_output_file remains original_downloaded_file. Sending will likely fail if it's not the correct format.

        if choice == "audio":
            await client.send_audio(
                chat_id=chat_id, audio=final_output_file,
                caption=f"**🔊 Audio Extracted!**\n\n{base_caption}",
                title=title_final[:30], duration=int(duration_seconds or 0),
                reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )
        elif choice == "video":
            await client.send_video(
                chat_id=chat_id, video=final_output_file,
                caption=f"**🎥 Video Downloaded!**\n\n{base_caption}",
                duration=int(duration_seconds or 0),
                reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )
        elif choice == "both":
            await client.send_video( # Send video part first
                chat_id=chat_id, video=final_output_file, # final_output_file here is the video
                caption=f"**🎥 Video Part!**\n\n{base_caption}",
                duration=int(duration_seconds or 0),
                reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )

            extracted_audio_file_for_both = os.path.join(DOWNLOAD_DIR, f"{title_final}_{original_user_id}_audio_extract.mp3")
            try: await client.edit_message_text(chat_id, interaction_key, "Extracting audio for 'Both'... 🔊")
            except Exception: pass

            ffmpeg_command = [
                'ffmpeg', '-i', final_output_file, '-vn', '-acodec', 'libmp3lame',
                '-b:a', '192k', '-ar', '44100', '-y', extracted_audio_file_for_both ]
            if await run_ffmpeg(ffmpeg_command) and os.path.exists(extracted_audio_file_for_both):
                audio_filesize_str = sizeof_fmt(os.path.getsize(extracted_audio_file_for_both))
                audio_caption = f"🎬 **Title:** `{title_final} (Audio)`\n⏱️ **Duration:** `{duration_str}` | 💾 **Size:** `{audio_filesize_str}`\n🥀 **Requested by:** {query.from_user.mention}\n👤 **Creator:** @Hindu_papa ✓"
                await client.send_audio(
                    chat_id=chat_id, audio=extracted_audio_file_for_both,
                    caption=f"**🔊 Audio Part!**\n\n{audio_caption}",
                    title=title_final[:30], duration=int(duration_seconds or 0),
                    reply_markup=final_ui, reply_to_message_id=reply_to_msg_id )
            else:
                logger.error(f"FFmpeg failed or extracted audio not found for 'Both' for user {original_user_id} (interaction {interaction_key}). Video path was: {final_output_file}")
                await client.send_message(chat_id, "⚠️ Failed to extract audio for 'Both', but video was sent.", reply_to_message_id=reply_to_msg_id)

        try: await client.delete_messages(chat_id, interaction_key) # Delete the "Choose format" message
        except Exception: pass

    except FloodWait as fw:
        logger.warning(f"FloodWait for {fw.value}s in button_handler for user {original_user_id} (interaction {interaction_key}). Waiting...")
        await asyncio.sleep(fw.value + 1)
    except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid, ChatWriteForbidden):
        logger.warning(f"User {original_user_id} (interaction {interaction_key}) blocked bot, is deactivated, or chat/user ID is invalid/bot cannot write to chat.")
        if original_user_id in subscribed_users:
            subscribed_users.discard(original_user_id)
            save_users(subscribed_users)
        # If it's a group chat issue, it might be handled by broadcast cleanup, but good to log.
        if interaction_data["chat_id"] in active_groups and isinstance(e, (ChatWriteForbidden, PeerIdInvalid)):
            logger.info(f"Removing group {interaction_data['chat_id']} due to {type(e).__name__} during button handler.")
            active_groups.discard(interaction_data["chat_id"])
            save_groups(active_groups)

    except Exception as e:
        logger.error(f"Overall error in button_handler for user {original_user_id} (interaction {interaction_key}): {e}", exc_info=True)
        try:
            # Try to edit the status message first
            if interaction_key in user_interaction_states and query.message: # Ensure message still exists
                 await client.edit_message_text(chat_id, interaction_key, f"❌ A critical error occurred: `{str(e)[:250]}`")
            else: # If status message is gone, reply to original message
                await client.send_message(chat_id, f"❌ A critical error occurred with your request. Please try again or contact admin if issue persists.", reply_to_message_id=interaction_data.get("original_message_id"))
        except Exception as e_reply:
            logger.error(f"Failed to even send error message to user {original_user_id} (interaction {interaction_key}): {e_reply}")
    finally:
        # Cleanup downloaded files
        files_to_clean = []
        if original_downloaded_file and os.path.exists(original_downloaded_file):
            files_to_clean.append(original_downloaded_file)
        if extracted_audio_file_for_both and os.path.exists(extracted_audio_file_for_both):
            files_to_clean.append(extracted_audio_file_for_both)
        
        # Add the potentially postprocessed audio file name (if different from original_downloaded_file)
        if choice == "audio" and ydl_opts.get('postprocessors') and original_downloaded_file:
            base, _ = os.path.splitext(original_downloaded_file)
            expected_audio_file = base + "." + ydl_opts['postprocessors'][0]['preferredcodec']
            if expected_audio_file != original_downloaded_file and os.path.exists(expected_audio_file):
                files_to_clean.append(expected_audio_file)

        for f_path in set(files_to_clean): # Use set to avoid trying to delete same file twice
            if f_path and os.path.exists(f_path): # Double check existence before attempting removal
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
    try:
        # Pyrogram's app.run() handles the asyncio loop.
        app.run() # This is a blocking call until the bot is stopped.
        # Code here will run after bot stops.
        logger.info("Bot has been stopped gracefully.")
    except RuntimeError as e:
        if "another loop" in str(e).lower() or "different loop" in str(e).lower():
            logger.warning(f"Caught a common asyncio loop error during shutdown: {e}. This is often non-critical if the bot was stopping anyway.")
        else:
            logger.critical(f"A runtime error occurred during bot execution: {e}", exc_info=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"An unexpected error occurred at the top level, causing bot to stop: {e}", exc_info=True)
    finally:
        logger.info("Script execution finished. Cleaning up if necessary.")
        # Perform any final cleanup here if needed, though Pyrogram's stop should handle most.

