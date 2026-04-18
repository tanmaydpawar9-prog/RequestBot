import time
import random
import logging
import asyncio
import psycopg2
from aiogram import types # Moved from _process_start_args_internal
from datetime import datetime # Moved from _process_start_args_internal
from aiogram.enums import ParseMode
import psycopg2.extras

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

from config import bot, DATABASE_URL, DESTINATION_CHANNEL_ID, MAIN_CHANNEL_INVITE_LINK, WEB_APP_DOMAIN, POST_FORCE_JOIN_CACHE, POST_FORCE_JOIN_CACHE_DURATION
from utils import cleanup_unclicked_request, delete_message_later

user_router = Router()

# --- Database Functions (Synchronous) ---
# These functions are designed to be run in a separate thread to avoid blocking the event loop.

def db_get_posted_content(content_hash: str):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT * FROM posted_content WHERE hash = %s", (content_hash,))
            return cursor.fetchone()

def db_get_active_backup_channel():
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT channel_id, full_name FROM backup_channels WHERE is_active = TRUE LIMIT 1")
            return cursor.fetchone()

def db_is_join_request_pending(channel_id: int, user_id: int):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pending_join_requests WHERE chat_id = %s AND user_id = %s", (channel_id, user_id))
            return cursor.fetchone() is not None

def db_store_pending_join_request(channel_id: int, user_id: int, raw_args: str, message_id: int):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pending_join_requests (chat_id, user_id, timestamp, original_start_args, original_user_message_id)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT(chat_id, user_id) DO UPDATE SET timestamp = EXCLUDED.timestamp, original_start_args = EXCLUDED.original_start_args, original_user_message_id = EXCLUDED.original_user_message_id
            """, (channel_id, user_id, time.time(), raw_args, message_id))
            conn.commit()

def db_process_verification_start(chat_id: int, user_full_name: str):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("DELETE FROM ads WHERE %s - timestamp > 86400", (time.time(),))
            conn.commit()
            cursor.execute("SELECT channel_id, message_id, url FROM ads")
            ads_db = cursor.fetchall()
            cursor.execute("""
                INSERT INTO users (user_id, name, total_requests) VALUES (%s, %s, 1) 
                ON CONFLICT(user_id) DO UPDATE SET total_requests = users.total_requests + 1, name = EXCLUDED.name
            """, (chat_id, user_full_name))
            conn.commit()
            return ads_db

def db_remove_ad(ad_channel_id: int, ad_msg_id: int):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM ads WHERE channel_id = %s AND message_id = %s", (ad_channel_id, ad_msg_id))
            conn.commit()

def db_store_request_details(request_key: str, ad_url: str, user_msg_id: int, fwd_msg_id: int, bot_reply_msg_id: int):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO requests (request_key, verified, timestamp, target_url, user_msg_id, bot_fwd_msg_id, bot_reply_msg_id) 
                VALUES (%s, 0, %s, %s, %s, %s, %s)
                ON CONFLICT(request_key) DO UPDATE SET verified = 0, timestamp = EXCLUDED.timestamp, target_url = EXCLUDED.target_url, user_msg_id = EXCLUDED.user_msg_id, bot_fwd_msg_id = EXCLUDED.bot_fwd_msg_id, bot_reply_msg_id = EXCLUDED.bot_reply_msg_id
            """, (request_key, time.time(), ad_url, user_msg_id, fwd_msg_id, bot_reply_msg_id))
            conn.commit()

def db_get_all_force_join_channels():
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT short_name, channel_id, full_name FROM channels ORDER BY channel_id")
            return cursor.fetchall()

def db_get_continue_flow_context(user_id: int):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT original_user_message_id FROM pending_join_requests WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            if result:
                cursor.execute("DELETE FROM pending_join_requests WHERE user_id = %s", (user_id,))
                conn.commit()
                return result[0]
    return 0

def db_verify_and_get_file(request_key: str, user_id: int, file_hash: str):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT verified, timestamp, user_msg_id, bot_fwd_msg_id FROM requests WHERE request_key = %s", (request_key,))
            req = cursor.fetchone()
            if not req: return {"error": "invalid_session"}
            if time.time() - req['timestamp'] > 300:
                cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,)); conn.commit()
                return {"error": "expired"}
            if not req['verified']: return {"error": "not_verified"}
            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            file_row = cursor.fetchone()
            if file_row:
                cursor.execute("UPDATE users SET successful_receives = successful_receives + 1 WHERE user_id = %s", (user_id,))
                cursor.execute("INSERT INTO user_file_requests (user_id, file_hash, count) VALUES (%s, %s, 1) ON CONFLICT(user_id, file_hash) DO UPDATE SET count = user_file_requests.count + 1", (user_id, file_hash))
            cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,)); conn.commit()
            return {"req": req, "file_row": file_row}

def db_get_channel_by_short_name(short_name: str):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT channel_id FROM channels WHERE short_name = %s", (short_name,))
            return cursor.fetchone()

async def _serve_posted_content(user_id: int, content_hash: str):
    """Retrieves and sends content previously stored by the /post command."""
    content_row = await asyncio.to_thread(db_get_posted_content, content_hash)
    if content_row:
        try:
            # Prefer to send a link to the post if its location is stored
            if 'channel_id' in content_row and 'message_id' in content_row and content_row['channel_id'] and content_row['message_id']:
                channel_id = content_row['channel_id']
                message_id = content_row['message_id']
                
                link = ""
                try:
                    # Get chat info to build the best link (public vs private)
                    chat = await bot.get_chat(channel_id)
                    if chat.username:
                        link = f"https://t.me/{chat.username}/{message_id}"
                    else: # Private channel or public without username
                        clean_id = str(channel_id).replace("-100", "")
                        link = f"https://t.me/c/{clean_id}/{message_id}"
                except Exception as e:
                    logging.error(f"Could not get chat for link generation: {e}. Falling back to private link format.")
                    clean_id = str(channel_id).replace("-100", "")
                    link = f"https://t.me/c/{clean_id}/{message_id}"

                keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❤️‍🔥 Download Episode", url=link)]])
                await bot.send_message(user_id, "Thank you for being a member! Here is your episode:", reply_markup=keyboard)
            # Fallback to old behavior if location is not in DB
            else:
                await bot.send_photo(user_id, content_row['file_id'], caption=content_row['caption'])
        except Exception as e:
            logging.error(f"Failed to send posted content {content_hash} to user {user_id}: {e}")
            await bot.send_message(user_id, "❌ An error occurred while trying to send the content. It might be unavailable.")
    else:
        await bot.send_message(user_id, "❌ This content link is invalid or has expired.")

async def _handle_backup_channel_check(message: Message, raw_args: str):
    """
    Checks backup channel membership and proceeds if allowed.
    Returns True if allowed to proceed, False if blocked.
    """
    user_id = message.from_user.id
    user_full_name = message.from_user.full_name
    original_user_message_id = message.message_id

    active_backup_channel = None
    active_backup_channel = await asyncio.to_thread(db_get_active_backup_channel)

    if not active_backup_channel:
        return True # No active backup channel, proceed

    is_member = False
    try:
        member = await bot.get_chat_member(active_backup_channel['channel_id'], user_id)
        if member.status not in ['left', 'kicked']:
            is_member = True
    except (TelegramBadRequest, TelegramNotFound) as e:
        logging.info(f"User {user_id} is not a member of backup channel {active_backup_channel['channel_id']} (check resulted in: {e})")
        is_member = False
    except Exception as e:
        logging.error(f"Could not check backup channel membership for {user_id}: {e}")
        msg = await message.answer("<b>System Error</b>\n\nCould not verify your membership status. Please try again later or contact an admin.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return False # Block on critical error

    if is_member:
        return True # User is a member, proceed

    # User is not a member. Check for pending request.
    is_pending = await asyncio.to_thread(db_is_join_request_pending, active_backup_channel['channel_id'], user_id)
    
    if is_pending:
        return True # User has a pending request, proceed as normal

    # User is not a member and has no pending request. Prompt to join.
    try:
        chat = await bot.get_chat(active_backup_channel['channel_id'])
        invite_link = chat.invite_link
        if not invite_link:
            invite_link = await bot.export_chat_invite_link(active_backup_channel['channel_id'])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"➡️ Request to Join {active_backup_channel['full_name']}", url=invite_link)]
        ])
        msg = await message.answer(
            "<b>❗️ Access Requirement</b>\n\n"
            "To proceed, you must send a join request to our backup channel.\n\n"
            "1. Click the button below to send a request.\n"
            "2. Once your request is sent, we will notify you to continue.",
            reply_markup=keyboard
        )
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        
        # Store the context in pending_join_requests
        await asyncio.to_thread(db_store_pending_join_request, active_backup_channel['channel_id'], user_id, raw_args, message.message_id)
        return False # Block the user
    except Exception as e_link:
        logging.error(f"Failed to get invite link for backup channel {active_backup_channel['channel_id']}: {e_link}")
        msg = await message.answer("<b>System Error</b>\n\nCould not generate a join link. Please contact an admin.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return False # Block user

async def _process_start_args_internal(user_id: int, user_full_name: str, raw_args: str, original_user_message_id: int):
    """Internal helper to process start arguments after all checks."""
    if raw_args.startswith("post_"): # Handle ALL post links
        # Create a dummy Message object to pass to handle_post_deep_link
        dummy_message = types.Message(
            message_id=original_user_message_id,
            from_user=types.User(id=user_id, is_bot=False, first_name=user_full_name),
            chat=types.Chat(id=user_id, type='private'),
            date=datetime.now(),
            text=f"/start {raw_args}"
        )
        await handle_post_deep_link(dummy_message, raw_args)
        
        # Add warning for old format links
        if not raw_args.startswith("post_content_"):
            await bot.send_message(user_id, "⚠️ <b>Warning:</b> This post link is using an old format and might expire if the original message is deleted. Please ask the admin to re-post using the new system for more reliable access.", parse_mode=ParseMode.HTML)

    else: # It's a subtitle file hash
        file_hash = raw_args
        await proceed_with_verification(user_id, user_full_name, file_hash, original_user_message_id)

async def proceed_with_verification(chat_id: int, user_full_name: str, file_hash: str, user_msg_id: int):
    """Handles the ad forwarding and verification message sending."""
    request_key = f"{chat_id}_{file_hash}"
    
    ad_url = "https://example.com"
    fwd_msg_id = None
    
    ads_db = await asyncio.to_thread(db_process_verification_start, chat_id, user_full_name)

    max_ad_attempts = 5
    for _ in range(max_ad_attempts):
        if not ads_db:
            logging.warning("No ads available in the database for verification.")
            break

        selected_ad = random.choice(ads_db)
        ad_url, ad_msg_id, ad_channel_id = selected_ad['url'], selected_ad['message_id'], selected_ad['channel_id']

        try:
            sent_fwd = await bot.forward_message(chat_id=chat_id, from_chat_id=ad_channel_id, message_id=ad_msg_id)
            fwd_msg_id = sent_fwd.message_id
            break # Success, exit the loop
        except (TelegramBadRequest, TelegramNotFound) as e:
            logging.warning(f"Ad message {ad_msg_id} not found. Removing from DB. Error: {e}")
            await asyncio.to_thread(db_remove_ad, ad_channel_id, ad_msg_id)
            ads_db = [ad for ad in ads_db if not (ad['message_id'] == ad_msg_id and ad['channel_id'] == ad_channel_id)]

    track_url = f"{WEB_APP_DOMAIN}/track?u={chat_id}&h={file_hash}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Click Ad to Verify 👀", url=track_url)],
        [InlineKeyboardButton(text="2️⃣ Get Subtitle File 📥", callback_data=f"get_{file_hash}")]
    ])

    text = ("<b>Verification Required!</b>\n\nPlease click 'Click Ad to Verify' below. 👇\n" "After verifying, click 'Get Subtitle File' to receive your file. ✨")
    if not fwd_msg_id:
        text = (f"<b>Verification Required!</b>\n\nPlease click the verification button below. 👇\n\n"
                f"<i>(No specific ad available, but verification is still needed.)</i>")

    bot_reply_msg = await bot.send_message(chat_id, text, reply_markup=keyboard)
    
    await asyncio.to_thread(db_store_request_details, request_key, ad_url, user_msg_id, fwd_msg_id, bot_reply_msg.message_id)
    
    asyncio.create_task(cleanup_unclicked_request(request_key, chat_id, delay=300))

async def handle_post_deep_link(message: Message, raw_args: str):
    """Handles all post-related deep links, including force-join."""
    user_id = message.from_user.id
    
    # --- Force-Join Channel Selection Logic ---
    global POST_FORCE_JOIN_CACHE
    all_channels = await asyncio.to_thread(db_get_all_force_join_channels)
    
    # If no force-join channels, just serve content and exit
    if not all_channels:
        # This is a configuration error. The /post feature requires at least one force-join channel.
        logging.error(f"User {user_id} clicked a /post link, but no force-join channels are configured in the 'channels' table.")
        await bot.send_message(user_id, "❌ This link is currently unavailable due to a server configuration issue. Please try again later.")
        return
        
    # Select a channel from cache or random
    current_time = time.time()
    target_channel = None
    if POST_FORCE_JOIN_CACHE["channel"] and (current_time - POST_FORCE_JOIN_CACHE["timestamp"] < POST_FORCE_JOIN_CACHE_DURATION):
        target_channel = POST_FORCE_JOIN_CACHE["channel"]
    else:
        target_channel = random.choice(all_channels)
        POST_FORCE_JOIN_CACHE["channel"] = target_channel
        POST_FORCE_JOIN_CACHE["timestamp"] = current_time

    req_short_name = target_channel['short_name']
    req_channel_id = target_channel['channel_id']
    channel_full_name = target_channel['full_name']
    
    # Check membership
    is_member = False
    try:
        member = await bot.get_chat_member(req_channel_id, user_id)
        # Explicitly log the status for debugging
        logging.info(f"Force-join check for user {user_id} in channel {req_channel_id} ({channel_full_name}). User status: {member.status}")
        
        # A user is considered a member if they are not left, kicked, or restricted.
        if member.status not in ['left', 'kicked', 'restricted']:
            is_member = True
            
    except TelegramNotFound:
        # This can happen if the user has blocked the bot. We can't check, so we treat them as not a member.
        logging.warning(f"Could not find user {user_id} when checking membership for channel {req_channel_id}. They may have blocked the bot.")
        is_member = False
        
    except TelegramBadRequest as e:
        # This usually means the bot is not an admin in the channel.
        logging.warning(f"Could not check membership for channel {req_channel_id}, bot is likely not an admin. Proceeding with join prompt. Error: {e}")
        is_member = False

    except Exception as e:
        logging.error(f"An unexpected error occurred during membership check for user {user_id} in channel {req_channel_id}. Error: {e}")
        is_member = False
    
    # If member, serve content
    if is_member:
        if raw_args.startswith("post_content_"):
            await bot.send_message(user_id, "Thank you for being a member! Here is your content:")
            content_hash = raw_args.split("post_content_", 1)[1]
            await _serve_posted_content(user_id, content_hash)
        else: # old link
            parts = raw_args.split("_")
            message_id = parts[-1]
            username = "_".join(parts[1:-1])
            link = f"https://t.me/{username}/{message_id}"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❤️‍🔥 Download Episode", url=link)]])
            await bot.send_message(user_id, "Thank you for being a member! Here is your episode:", reply_markup=keyboard)
        return

    # If not member, show join prompt
    try:
        chat = await bot.get_chat(req_channel_id)
        invite_link = chat.invite_link or await bot.export_chat_invite_link(req_channel_id)
        
        callback_data = f"cp_check|{req_short_name}|{raw_args}"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"1. Join {channel_full_name} 🚀", url=invite_link)],
            [InlineKeyboardButton(text="2. I Have Joined ✅", callback_data=callback_data)]
        ])
        await bot.send_message(user_id, "<b>Join Required!</b>\n\nPlease join the following channel to get your episode link. 👇", reply_markup=keyboard)
    except Exception as e_final:
        logging.error(f"Failed to get invite link or show join prompt for {req_channel_id}: {e_final}")
        await bot.send_message(user_id, "❌ <b>System Error:</b> Could not verify channel membership. 🌐Please Report To Admin @CosmicAtomic")

@user_router.message(CommandStart())
async def handle_start(message: Message, command: CommandStart):
    """Handles the user clicking the deep link."""
    raw_args = command.args
    if not raw_args:
        msg = await message.answer(
            "👋 <b>Welcome!</b>\n\nTo get a file, use a special link from one of our channels."
        )
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    user_id = message.from_user.id
    user_full_name = message.from_user.full_name
    original_user_message_id = message.message_id

    # Perform backup channel check
    can_proceed = await _handle_backup_channel_check(message, raw_args)
    if not can_proceed:
        return # Blocked by backup channel check

    # If allowed to proceed, then process the start arguments
    await _process_start_args_internal(user_id, user_full_name, raw_args, original_user_message_id)

@user_router.callback_query(F.data.startswith("continue_flow_"))
async def handle_continue_flow(callback: CallbackQuery):
    """Handles the 'Continue' button after a backup channel join request."""
    await callback.answer("Continuing...", show_alert=False)
    start_args = callback.data.split("continue_flow_", 1)[1]
    user_id = callback.from_user.id
    user_full_name = callback.from_user.full_name

    # Retrieve the original user message ID from the database
    original_user_message_id = await asyncio.to_thread(db_get_continue_flow_context, user_id)
    await _process_start_args_internal(user_id, user_full_name, start_args, original_user_message_id)

@user_router.callback_query(F.data.startswith("get_"))
async def serve_file(callback: CallbackQuery):
    """Verifies the ad click and serves the file if valid."""
    await callback.answer() # Acknowledge the button press immediately

    user_id = callback.from_user.id
    file_hash = callback.data.split("_")[1]
    request_key = f"{user_id}_{file_hash}"
    
    db_result = await asyncio.to_thread(db_verify_and_get_file, request_key, user_id, file_hash)

    if "error" in db_result:
        error_map = {
            "invalid_session": "Invalid or expired session. Please click the original link again.",
            "expired": "Request expired (5 mins). Please generate a new one.",
            "not_verified": "❌ You must click 'Click Ad to Verify' first!"
        }
        # Use send_message instead of answer to provide better feedback
        await bot.send_message(user_id, error_map.get(db_result["error"], "An unknown error occurred."))
        return

    req = db_result["req"]
    file_row = db_result["file_row"]

    if file_row:
        status_msg = await bot.send_message(user_id, "✅ Verification successful! Preparing your file, please wait...")
        caption = "✅ Here is your file! 🎉\n\n⏳ <i>This message and file will be deleted in 5 minutes.</i>"
        sent_file = None
        try:
            file_info = await bot.get_file(file_row['file_id'])
            file_content = await bot.download_file(file_info.file_path)
            document_to_send = BufferedInputFile(file_content.read(), filename=file_row['filename'] or "subtitle.ass")
            sent_file = await bot.send_document(user_id, document_to_send, caption=caption)
        except Exception as e: # Broad exception to catch any telegram error during re-upload
            logging.error(f"Failed to re-upload file. Falling back. Error: {e}")
            try:
                sent_file = await bot.send_document(user_id, file_row['file_id'], caption=caption)
            except Exception as e2:
                logging.error(f"Fallback sending also failed: {e2}")
                await status_msg.edit_text("❌ A critical error occurred while sending the file. Please report this to an admin.")
                return

        await bot.delete_message(user_id, status_msg.message_id)
        
        for m_id in {callback.message.message_id, req.get('user_msg_id'), req.get('bot_fwd_msg_id')}:
            if m_id:
                try: await bot.delete_message(user_id, m_id)
                except (TelegramNotFound, TelegramBadRequest): pass
        
        if sent_file:
            asyncio.create_task(delete_message_later(user_id, sent_file.message_id, 300))
    else:
        await bot.send_message(user_id, "File no longer available. 😔")

@user_router.callback_query(F.data.startswith("verify_join_"))
async def handle_join_verification(callback: CallbackQuery):
    """Handles the 'I have joined' button click."""
    file_hash = callback.data.split("_")[2]
    user_id = callback.from_user.id

    if not DESTINATION_CHANNEL_ID:
        await callback.answer("This check is no longer required.", show_alert=True)
        return await callback.message.delete()

    try:
        member = await bot.get_chat_member(DESTINATION_CHANNEL_ID, user_id)
        if member.status not in ['left', 'kicked']:
            await callback.answer("Thank you for joining! Please wait... 🙏", show_alert=False)
            await callback.message.delete()
            await proceed_with_verification(user_id, callback.from_user.full_name, file_hash, 0)
        else:
            await callback.answer("❌ You haven't joined the channel yet. Please join and click again.", show_alert=True)
    except Exception as e:
        logging.error(f"Error during join verification for {user_id}: {e}")
        await callback.answer("An error occurred while verifying. Please try again.", show_alert=True)

@user_router.callback_query(F.data.startswith("cp_check|"))
async def handle_post_join_check(callback: CallbackQuery):
    """Handles the 'I Have Joined' button for post links."""
    parts = callback.data.split("|", 2)
    if len(parts) != 3:
        return await callback.answer("Invalid callback data.", show_alert=True)

    _, req_short_name, raw_args = parts
    user_id = callback.from_user.id
    
    channel_row = await asyncio.to_thread(db_get_channel_by_short_name, req_short_name)
    if not channel_row:
        return await callback.answer("The required channel is no longer registered.", show_alert=True)

    req_channel_id = channel_row['channel_id']

    try:
        member = await bot.get_chat_member(req_channel_id, user_id)
        if member.status in ['left', 'kicked', 'restricted']:
            return await callback.answer("❌ You haven't joined the required channel yet. Please join and try again.", show_alert=True)
    except Exception:
        return await callback.answer("❌ Configuration Error: The bot must be an admin in the channel to verify membership.", show_alert=True)

    # If check passes, serve content
    await callback.answer("Thank you for joining!", show_alert=False)
    await callback.message.delete() # Clean up the join prompt
    
    if raw_args.startswith("post_content_"):
        await callback.message.answer("✅ Verification successful! Here is your content:")
        content_hash = raw_args.split("post_content_", 1)[1]
        await _serve_posted_content(user_id, content_hash)
    else: # old link
        old_parts = raw_args.split("_")
        message_id = old_parts[-1]
        username = "_".join(old_parts[1:-1])
        link = f"https://t.me/{username}/{message_id}"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🟢 Download Episode", url=link)]])
        await callback.message.answer("✅ Verification successful! Here is your episode:", reply_markup=keyboard)