import logging
import time
import traceback
import psycopg2
import psycopg2.extras
import asyncio
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, ErrorEvent, ChatJoinRequest

from config import bot, ADMIN_ID, ADS_BOT_ID, DATABASE_URL
from utils import extract_ad_url, delete_message_later
from user import _process_start_args_internal

common_router = Router()

@common_router.error()
async def global_error_handler(event: ErrorEvent):
    """Catches all errors and sends a message to the Admin for debugging."""
    logging.error(f"Update: {event.update}\nException: {event.exception}")
    traceback.print_exception(type(event.exception), event.exception, event.exception.__traceback__)
    
    if ADMIN_ID:
        try:
            msg = await bot.send_message(
                ADMIN_ID,
                f"🚨 <b>BOT ERROR!</b>\n\n<code>{event.exception}</code>\n\n Contact Admin @cosmicatomic."
            )
            asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        except Exception:
            pass

@common_router.channel_post()
@common_router.edited_channel_post()
async def track_channel_ads(message: Message):
    """Monitors any channel the bot is in to automatically log ads."""
    # Only track messages sent by the designated ads bot.
    if not ADS_BOT_ID:
        return

    is_ad_bot = (
        (message.from_user and message.from_user.id == ADS_BOT_ID) or
        (message.sender_chat and message.sender_chat.id == ADS_BOT_ID) or
        (message.forward_from and message.forward_from.id == ADS_BOT_ID) or
        (message.forward_from_chat and message.forward_from_chat.id == ADS_BOT_ID) or
        (message.via_bot and message.via_bot.id == ADS_BOT_ID)
    )

    if not is_ad_bot:
        return

    ad_url = extract_ad_url(message)
    if ad_url:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM ads WHERE message_id = %s AND channel_id = %s", (message.message_id, message.chat.id))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (%s, %s, %s, %s)",
                                   (message.chat.id, message.message_id, ad_url, time.time()))
                    conn.commit()
                    logging.info(f"✨ New ad registered automatically from Ads Bot: {ad_url}")

@common_router.chat_join_request()
async def handle_join_requests(request: ChatJoinRequest):
    """Handles a join request, and if context is found, automatically proceeds with the user's original action."""
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Check if it's a backup channel we should be tracking
            cursor.execute("SELECT 1 FROM backup_channels WHERE channel_id = %s", (request.chat.id,))
            if not cursor.fetchone():
                return # Not a channel we manage

            logging.info(f"Received join request from user {request.from_user.id} for channel {request.chat.id}. Proceeding automatically.")
            
            # Retrieve the original context to auto-proceed
            cursor.execute("SELECT original_start_args, original_user_message_id FROM pending_join_requests WHERE chat_id = %s AND user_id = %s",
                           (request.chat.id, request.from_user.id))
            stored_context = cursor.fetchone()

            if stored_context and stored_context['original_start_args']:
                user_id = request.from_user.id
                user_full_name = request.from_user.full_name
                raw_args = stored_context['original_start_args']
                original_user_message_id = stored_context['original_user_message_id']
                
                try:
                    await bot.send_message(user_id, "<b>Request Sent!</b>\n\nYou can now proceed to the next step.")
                    await _process_start_args_internal(user_id, user_full_name, raw_args, original_user_message_id)
                except Exception as e:
                    logging.error(f"Failed to auto-proceed for user {user_id} after join request: {e}")
            else:
                # Fallback if context is missing
                try:
                    await bot.send_message(request.from_user.id, "<b>Request Received!</b>\n\nYour request is now pending. Please go back and click the original link again to continue.")
                except Exception as e:
                    logging.error(f"Failed to send fallback 'request received' message to user {request.from_user.id}: {e}")

@common_router.message(Command("ping"))
async def ping_handler(message: Message):
    """Simple command to test if the bot is alive."""
    msg = await message.answer("🏓 Pong! The bot is online and actively receiving messages.")
    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@common_router.message()
async def catch_all(message: Message):
    """Catches unhandled messages and provides helpful feedback, especially to the admin."""
    if message.from_user.id == ADMIN_ID and message.chat.type == 'private':
        if message.text:
            if message.text.startswith('/post'):
                msg = await message.reply("⚠️ <b>Command Error:</b>\nTo use <code>/post</code>, you must <b>reply</b> to a forwarded message that contains a photo and caption.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return
            if message.text.startswith('/addad'):
                msg = await message.reply("⚠️ <b>Command Error:</b>\nTo use <code>/addad</code>, you must <b>reply</b> to a forwarded ad message.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return
        return