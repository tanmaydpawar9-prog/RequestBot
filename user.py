import time
import random
import logging
import asyncio
import psycopg2
import psycopg2.extras

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

from config import bot, DATABASE_URL, DESTINATION_CHANNEL_ID, MAIN_CHANNEL_INVITE_LINK, WEB_APP_DOMAIN, POST_FORCE_JOIN_CACHE, POST_FORCE_JOIN_CACHE_DURATION
from utils import cleanup_unclicked_request, delete_message_later

user_router = Router()

async def _process_start_args_internal(user_id: int, user_full_name: str, raw_args: str, original_user_message_id: int):
    """Internal helper to process start arguments after all checks."""
    if raw_args.startswith("post_"):
        # Reconstruct parts for handle_post_deep_link logic
        parts = raw_args.split("_")
        if len(parts) < 3:
            await bot.send_message(user_id, "❌ Invalid post link format.")
            return
        
        # Create a dummy Message object to pass to handle_post_deep_link
        # This is necessary because handle_post_deep_link expects a Message object
        # for various properties like from_user, chat, and message_id.
        dummy_message = Message(
            message_id=original_user_message_id, # Use the original message ID for context
            from_user=types.User(id=user_id, is_bot=False, first_name=user_full_name),
            chat=types.Chat(id=user_id, type='private'),
            date=datetime.now(), # Current time
            text=f"/start {raw_args}" # Reconstruct the command text
        )
        await handle_post_deep_link(dummy_message, raw_args)
        
    else: # It's a subtitle file hash
        file_hash = raw_args
        await proceed_with_verification(user_id, user_full_name, file_hash, original_user_message_id)

async def _check_backup_channel_and_proceed(message: Message, raw_args: str):
    """
    Checks backup channel membership and proceeds if allowed.
    Returns True if allowed to proceed, False if blocked.
    """
    user_id = message.from_user.id
    user_full_name = message.from_user.full_name
    original_user_message_id = message.message_id

    # --- Backup Channel Check ---
async def proceed_with_verification(chat_id: int, user_full_name: str, file_hash: str, user_msg_id: int):
    """Handles the ad forwarding and verification message sending."""
    request_key = f"{chat_id}_{file_hash}"
    
    ad_url = "https://example.com"
    fwd_msg_id = None
    ads_db = []

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Clean up old ads and fetch the list of available ads
            cursor.execute("DELETE FROM ads WHERE %s - timestamp > 86400", (time.time(),))
            conn.commit()
            cursor.execute("SELECT channel_id, message_id, url FROM ads")
            ads_db = cursor.fetchall()

            max_ad_attempts = 5
            for _ in range(max_ad_attempts):
                if not ads_db:
                    logging.warning("No ads available in the database for verification.")
                    break

                selected_ad = random.choice(ads_db)
                ad_url, ad_msg_id, ad_channel_id = selected_ad['url'], selected_ad['message_id'], selected_ad['channel_id']

                try:
                    # Attempt to forward the ad message
                    sent_fwd = await bot.forward_message(chat_id=chat_id, from_chat_id=ad_channel_id, message_id=ad_msg_id)
                    fwd_msg_id = sent_fwd.message_id
                    break # Success, exit the loop
                except (TelegramBadRequest, TelegramNotFound) as e:
                    # If forwarding fails, the ad is likely deleted. Remove it from DB and our local list.
                    logging.warning(f"Ad message {ad_msg_id} not found. Removing from DB. Error: {e}")
                    cursor.execute("DELETE FROM ads WHERE channel_id = %s AND message_id = %s", (ad_channel_id, ad_msg_id))
                    conn.commit()
                    # Remove from the local list to avoid retrying the same failed ad
                    ads_db = [ad for ad in ads_db if not (ad['message_id'] == ad_msg_id and ad['channel_id'] == ad_channel_id)]
            
            cursor.execute("""
                INSERT INTO users (user_id, name, total_requests) VALUES (%s, %s, 1) 
                ON CONFLICT(user_id) DO UPDATE SET total_requests = users.total_requests + 1, name = EXCLUDED.name
            """, (chat_id, user_full_name))
            
    track_url = f"{WEB_APP_DOMAIN}/track?u={chat_id}&h={file_hash}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Click Ad to Verify 👀", url=track_url)],
        [InlineKeyboardButton(text="2️⃣ Get Subtitle File 📥", callback_data=f"get_{file_hash}")]
    ])

    text = ("<b>Verification Required!</b>\n\nPlease click 'Click Ad to Verify' below. 👇\n"
            "After verifying, click 'Get Subtitle File' to receive your file. ✨")
    if not fwd_msg_id:
        text = (f"<b>Verification Required!</b>\n\nPlease click the verification button below. 👇\n\n"
                f"<i>(No specific ad available, but verification is still needed.)</i>")

    bot_reply_msg = await bot.send_message(chat_id, text, reply_markup=keyboard)
    
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO requests (request_key, verified, timestamp, target_url, user_msg_id, bot_fwd_msg_id, bot_reply_msg_id) 
                VALUES (%s, 0, %s, %s, %s, %s, %s)
                ON CONFLICT(request_key) DO UPDATE SET verified = 0, timestamp = EXCLUDED.timestamp, target_url = EXCLUDED.target_url, user_msg_id = EXCLUDED.user_msg_id, bot_fwd_msg_id = EXCLUDED.bot_fwd_msg_id, bot_reply_msg_id = EXCLUDED.bot_reply_msg_id
            """, (request_key, time.time(), ad_url, user_msg_id, fwd_msg_id, bot_reply_msg.message_id))
    
    asyncio.create_task(cleanup_unclicked_request(request_key, chat_id, delay=300))

async def handle_post_deep_link(message: Message, raw_args: str):
    """Handles the deep link generated by the /post command."""
    parts = raw_args.split("_")
    if len(parts) < 3:
        msg = await message.answer("❌ Invalid post link.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return
    
    message_id = parts[-1]
    username = "_".join(parts[1:-1])

    # --- NEW: Backup Channel Check ---
    active_backup_channel = None
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT channel_id, full_name FROM backup_channels WHERE is_active = TRUE LIMIT 1")
            active_backup_channel = cursor.fetchone()
    user_id = message.from_user.id

    if active_backup_channel:
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
            return # Block on critical error

        if not is_member: # If user is not a member of the backup channel
            with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM pending_join_requests WHERE chat_id = %s AND user_id = %s",
                                   (active_backup_channel['channel_id'], user_id))
                    is_pending = cursor.fetchone()

            # If no pending request is found, prompt the user to send one and store context.
            if not is_pending:
                # Store context before prompting to join
                link_type = "post" if raw_args.startswith("post_") else "subtitle"
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
                        "To get this content, you must send a join request to our backup channel.\n\n"
                        "1. Click the button below to send a request.\n"
                        "2. Once your request is sent, we will notify you to continue.", # Changed text
                        reply_markup=keyboard
                    )
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    
                    # Store the context in pending_join_requests
                    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO pending_join_requests (chat_id, user_id, timestamp, original_start_args, original_user_message_id)
                                VALUES (%s, %s, %s, %s, %s) ON CONFLICT(chat_id, user_id) DO UPDATE SET timestamp = EXCLUDED.timestamp, original_start_args = EXCLUDED.original_start_args, original_user_message_id = EXCLUDED.original_user_message_id
                            """, (active_backup_channel['channel_id'], user_id, time.time(), raw_args, message.message_id))
                            conn.commit()
                    return # Block the user
                except Exception as e_link: #
                    logging.error(f"Failed to get invite link for backup channel {active_backup_channel['channel_id']}: {e_link}")
                    msg = await message.answer("<b>System Error</b>\n\nCould not generate a join link. Please contact an admin.")
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    return # Block user
    # --- End of Backup Channel Check ---

    link = f"https://t.me/{username}/{message_id}"
    
    # If the user is a member, or has a pending request (and thus allowed to proceed),
    # then continue with the original /post deep link logic.
    if not active_backup_channel or is_member or is_pending:
        pass # Proceed with the rest of the function

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT short_name, channel_id, full_name FROM channels ORDER BY channel_id") # Order for consistent selection if cache is empty
            all_channels = cursor.fetchall()
    
    if not all_channels:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💥 Download Episode", url=link)]])
        msg = await message.answer("Here is your requested episode:", reply_markup=keyboard)
        return
        
    # --- Force-Join Channel Selection Logic ---
    global POST_FORCE_JOIN_CACHE

    current_time = time.time()
    target_channel = None

    # Check if the cached channel is still valid
    if POST_FORCE_JOIN_CACHE["channel"] and (current_time - POST_FORCE_JOIN_CACHE["timestamp"] < POST_FORCE_JOIN_CACHE_DURATION):
        target_channel = POST_FORCE_JOIN_CACHE["channel"]
        logging.info(f"Using cached force-join channel: {target_channel['full_name']}")
    else:
        # Select a new random channel and update the cache
        target_channel = random.choice(all_channels)
        POST_FORCE_JOIN_CACHE["channel"] = target_channel
        POST_FORCE_JOIN_CACHE["timestamp"] = current_time
        logging.info(f"Selected new force-join channel: {target_channel['full_name']}")

    req_short_name = target_channel['short_name']
    req_channel_id = target_channel['channel_id']
    channel_full_name = target_channel['full_name']
    
    try:
        member = await bot.get_chat_member(req_channel_id, user_id)
        # If user is already a member of the required channel, give the link directly
        if member.status not in ['left', 'kicked', 'restricted']:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❤️‍🔥 Download Episode", url=link)]])
            msg = await message.answer("Thank you for being a member! Here is your episode:", reply_markup=keyboard)
            return
    except Exception as e:
        # This can happen if bot is not admin. We can't check membership, so we'll just show the join buttons and let the callback handle verification.
        logging.warning(f"Could not check membership for channel {req_channel_id}, likely not admin. Proceeding with join prompt. Error: {e}")
    
    # Show the join prompt if the initial check failed OR if the user is confirmed not to be a member.
    try:
        chat = await bot.get_chat(req_channel_id)
        invite_link = chat.invite_link
        if not invite_link:
            try: invite_link = await bot.export_chat_invite_link(req_channel_id)
            except Exception as e_link:
                logging.warning(f"Failed to export invite link for {req_channel_id}: {e_link}")
                invite_link = f"https://t.me/{chat.username}" if chat.username else None
        
        if not invite_link:
            raise Exception("Could not retrieve a valid invite link for the channel.")

        callback_data = f"cp_check_{req_short_name}_{username}_{message_id}"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"1. Join {channel_full_name} 🚀", url=invite_link)],
            [InlineKeyboardButton(text="2. I Have Joined ✅", callback_data=callback_data)]
        ])
        msg = await message.answer("<b>Join Required!</b>\n\nPlease join the following channel to get your episode link. 👇", reply_markup=keyboard)
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except Exception as e_final:
        logging.error(f"Failed to get invite link or show join prompt for {req_channel_id}: {e_final}")
        msg = await message.answer("❌ <b>System Error:</b> Could not verify channel membership. 🌐Please Report To Admin @CosmicAtomic")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    return False # Blocked by force-join channel
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

    file_hash = raw_args
    user_id = message.from_user.id

    # --- NEW: Backup Channel Check ---
    active_backup_channel = None
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT channel_id, full_name FROM backup_channels WHERE is_active = TRUE LIMIT 1")
            active_backup_channel = cursor.fetchone()
    user_id = message.from_user.id

    if active_backup_channel:
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
            return # Block on critical error

        if not is_member: # If user is not a member of the backup channel
            with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM pending_join_requests WHERE chat_id = %s AND user_id = %s",
                                   (active_backup_channel['channel_id'], user_id))
                    is_pending = cursor.fetchone()

            # If no pending request is found, prompt the user to send one and store context.
            if not is_pending:
                # Store context before prompting to join
                link_type = "post" if raw_args.startswith("post_") else "subtitle"
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
                        "To use this bot, you must send a join request to our backup channel.\n\n"
                        "1. Click the button below to send a request.\n"
                        "2. Once your request is sent, we will notify you to continue.", # Changed text
                        reply_markup=keyboard
                    )
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    
                    # Store the context in pending_join_requests
                    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO pending_join_requests (chat_id, user_id, timestamp, original_start_args, original_user_message_id)
                                VALUES (%s, %s, %s, %s, %s) ON CONFLICT(chat_id, user_id) DO UPDATE SET timestamp = EXCLUDED.timestamp, original_start_args = EXCLUDED.original_start_args, original_user_message_id = EXCLUDED.original_user_message_id
                            """, (active_backup_channel['channel_id'], user_id, time.time(), raw_args, message.message_id))
                            conn.commit()
                    return # Block the user
                except Exception as e_link: #
                    logging.error(f"Failed to get invite link for backup channel {active_backup_channel['channel_id']}: {e_link}")
                    msg = await message.answer("<b>System Error</b>\n\nCould not generate a join link. Please contact an admin.")
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    return # Block user
    # --- End of Backup Channel Check ---

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor: #
            # If the user is a member, or has a pending request (and thus allowed to proceed),
            # then continue with the original /start deep link logic.
            # This check is now done within _check_backup_channel_and_proceed
            cursor.execute("SELECT file_id FROM files WHERE hash = %s", (file_hash,))
            if not cursor.fetchone():
                msg = await message.answer("This file link is invalid or has expired.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return

    if DESTINATION_CHANNEL_ID:
        try:
            member = await bot.get_chat_member(DESTINATION_CHANNEL_ID, user_id)
            if member.status in ['left', 'kicked']:
                chat = await bot.get_chat(DESTINATION_CHANNEL_ID)
                invite_link = chat.invite_link or MAIN_CHANNEL_INVITE_LINK
                if invite_link:
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=f"1. Join {chat.title} 🚀", url=invite_link)],
                        [InlineKeyboardButton(text="✅ I Have Joined", callback_data=f"verify_join_{file_hash}")]
                    ])
                    msg = await message.answer("<b>Join Required!</b>\n\nTo get your file, you must first join our channel. 👇", reply_markup=keyboard)
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    return
        except Exception as e:
            logging.error(f"Could not check channel membership for {user_id}: {e}")

    # If the user is a member, or has a pending request (and thus allowed to proceed),
    # then continue with the original /start deep link logic.
    if not active_backup_channel or is_member or is_pending:
        await _process_start_args_internal(user_id, user_full_name, raw_args, original_user_message_id)

@user_router.callback_query(F.data.startswith("continue_flow_"))
async def handle_continue_flow(callback: CallbackQuery):
    """Handles the 'Continue' button after a backup channel join request."""
    await callback.answer("Continuing...", show_alert=False)
    start_args = callback.data.split("continue_flow_", 1)[1]
    user_id = callback.from_user.id
    user_full_name = callback.from_user.full_name

    # Retrieve the original user message ID from the database
    original_user_message_id = 0
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT original_user_message_id FROM pending_join_requests WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            if result:
                original_user_message_id = result[0]
                cursor.execute("DELETE FROM pending_join_requests WHERE user_id = %s", (user_id,)) # Clean up
                conn.commit()
    await _process_start_args_internal(user_id, user_full_name, start_args, original_user_message_id)

@user_router.callback_query(F.data.startswith("get_"))
async def serve_file(callback: CallbackQuery):
    """Verifies the ad click and serves the file if valid."""
    user_id = callback.from_user.id
    file_hash = callback.data.split("_")[1]
    request_key = f"{user_id}_{file_hash}"
    
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT verified, timestamp, user_msg_id, bot_fwd_msg_id FROM requests WHERE request_key = %s", (request_key,))
            req = cursor.fetchone()
            
            if not req:
                return await callback.answer("Invalid or expired session. Please click the original link again.", show_alert=True)
            if time.time() - req['timestamp'] > 300:
                cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,))
                return await callback.answer("Request expired (5 mins). Please generate a new one.", show_alert=True)
            if not req['verified']:
                return await callback.answer("❌ You must click 'Click Ad to Verify' first!", show_alert=True)

            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            file_row = cursor.fetchone()
            if file_row:
                cursor.execute("UPDATE users SET successful_receives = successful_receives + 1 WHERE user_id = %s", (user_id,))
                cursor.execute("""
                    INSERT INTO user_file_requests (user_id, file_hash, count) VALUES (%s, %s, 1)
                    ON CONFLICT(user_id, file_hash) DO UPDATE SET count = user_file_requests.count + 1
                """, (user_id, file_hash))
                
            cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,))
            
    if file_row:
        caption = "✅ Here is your file! 🎉\n\n⏳ <i>This file will be deleted in 5 minutes.</i>"
        sent_file = None
        try:
            file_info = await bot.get_file(file_row['file_id'])
            file_content = await bot.download_file(file_info.file_path)
            document_to_send = BufferedInputFile(file_content.read(), filename=file_row['filename'] or "subtitle.ass")
            sent_file = await bot.send_document(user_id, document_to_send, caption=caption)
        except Exception as e:
            logging.error(f"Failed to re-upload file. Falling back. Error: {e}")
            sent_file = await bot.send_document(user_id, file_row['file_id'], caption=caption)

        for m_id in {callback.message.message_id, req.get('user_msg_id'), req.get('bot_fwd_msg_id')}:
            if m_id:
                try: await bot.delete_message(user_id, m_id)
                except (TelegramNotFound, TelegramBadRequest): pass
                
    else:
        await callback.answer("File no longer available. 😔", show_alert=True)
        
    await callback.answer()

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

@user_router.callback_query(F.data.startswith("cp_"))
async def handle_post_join_check(callback: CallbackQuery):
    """Handles the 'Download Episode' button click, verifying the single required channel join."""
    parts = callback.data.split("_")
    if len(parts) < 5 or parts[1] != 'check':
        return await callback.answer("Invalid callback data.", show_alert=True)

    req_short_name = parts[2]
    message_id = parts[-1]
    username = "_".join(parts[3:-1])
    user_id = callback.from_user.id

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT channel_id FROM channels WHERE short_name = %s", (req_short_name,))
            channel_row = cursor.fetchone()

    if not channel_row:
        return await callback.answer("The required channel is no longer registered.", show_alert=True)

    req_channel_id = channel_row['channel_id']
    
    try:
        member = await bot.get_chat_member(req_channel_id, user_id)
        if member.status in ['left', 'kicked', 'restricted']:
            return await callback.answer("❌ You haven't joined the required channel yet. Please join and try again.", show_alert=True)
    except Exception:
        return await callback.answer("❌ Configuration Error: The bot must be an admin in the channel to verify membership.", show_alert=True)

    # If check passes, update timestamp and give link
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO users (user_id, last_verified_timestamp) VALUES (%s, %s) ON CONFLICT(user_id) DO UPDATE SET last_verified_timestamp = EXCLUDED.last_verified_timestamp", (user_id, time.time()))

    try:
        await callback.answer("Thank you for joining!", show_alert=False)
        link = f"https://t.me/{username}/{message_id}"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬇️ Download Episode", url=link)]])
        await callback.message.edit_text("✅ Verification successful! Here is your episode:", reply_markup=keyboard)
    except Exception as e:
        logging.error(f"Error in final step of handle_post_join_check: {e}")
        await callback.answer("An error occurred. Please try again.", show_alert=True)