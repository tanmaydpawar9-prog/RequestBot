import logging
import time
import traceback
import psycopg2
import asyncio
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, ErrorEvent, ChatJoinRequest

from config import bot, ADMIN_ID, ADS_BOT_ID, DATABASE_URL
from utils import extract_ad_url, delete_message_later

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
async def track_channel_ads(message: Message):
    """Monitors any channel the bot is in to automatically log ads."""
    # Only track messages sent by the designated ads bot.
    if not ADS_BOT_ID:
        return

    # To identify a message from the ad bot in a channel, we must check multiple fields.
    # 1. `sender_chat`: This is the most reliable method. When a bot posts in a channel,
    #    this field contains the Chat object for that bot.
    # 2. `forward_from`: If the ad bot is a user account that forwards messages.
    # 3. `via_bot`: If the message is sent via an inline bot.
    is_ad_bot = (
        (message.sender_chat and message.sender_chat.id == ADS_BOT_ID) or
        (message.forward_from and message.forward_from.id == ADS_BOT_ID) or
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
    """Stores a user's request to join a channel, so it can be approved later."""
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Check if it's a backup channel we should be tracking
            cursor.execute("SELECT 1 FROM backup_channels WHERE channel_id = %s", (request.chat.id,))
            if cursor.fetchone():
                logging.info(f"Storing join request from user {request.from_user.id} for channel {request.chat.id}")
                
                # Retrieve the original context to build a continue link
                cursor.execute("SELECT original_start_args FROM pending_join_requests WHERE chat_id = %s AND user_id = %s",
                               (request.chat.id, request.from_user.id))
                stored_context = cursor.fetchone()

                if stored_context and stored_context['original_start_args']:
                    original_start_args = stored_context['original_start_args']
                    bot_info = await bot.me()
                    continue_link = f"https://t.me/{bot_info.username}?start={original_start_args}"
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Request Sent! Click here to continue.", url=continue_link)]])
                    try:
                        await bot.send_message(request.from_user.id, "<b>Your request has been sent!</b>\n\nYou can now proceed to get your content.", reply_markup=keyboard)
                    except Exception as e:
                        logging.error(f"Failed to send continue message to user {request.from_user.id}: {e}")
                else:
                    # Fallback if context is missing
                    try:
                        await bot.send_message(request.from_user.id, "<b>Request Received!</b>\n\nPlease go back and click the original link again to continue.")
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