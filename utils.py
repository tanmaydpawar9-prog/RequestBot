import os
import re
import asyncio
import logging
import psycopg2
import psycopg2.extras
from typing import Optional

from aiogram.exceptions import TelegramNotFound, TelegramBadRequest
from aiogram.types import Message

from config import bot, DATABASE_URL

async def cleanup_unclicked_request(request_key: str, chat_id: int, delay: int = 300):
    """Deletes the verification messages if the user doesn't click the ad link in time."""
    await asyncio.sleep(delay)
    
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Check if the request is still unverified
            cursor.execute("SELECT verified, bot_fwd_msg_id, bot_reply_msg_id FROM requests WHERE request_key = %s", (request_key,))
            req = cursor.fetchone()

            if req and not req['verified']:
                logging.info(f"Cleaning up unclicked request {request_key} for user {chat_id}")
                
                # Delete bot's messages
                if req['bot_fwd_msg_id']:
                    try: await bot.delete_message(chat_id, req['bot_fwd_msg_id'])
                    except (TelegramNotFound, TelegramBadRequest): pass
                if req['bot_reply_msg_id']:
                    try: await bot.delete_message(chat_id, req['bot_reply_msg_id'])
                    except (TelegramNotFound, TelegramBadRequest): pass
                
                # Delete the request from DB
                cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,))
                conn.commit()

async def delete_message_later(chat_id: int, message_id: int, delay: int):
    """Deletes a message after a specified delay in seconds."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logging.error(f"Failed to delete delayed message: {e}")

def extract_ad_url(message: Message):
    """Identifies an ad by looking for inline buttons or text links."""
    # 1. Check for inline keyboard buttons (Standard for Inside Ads)
    if message.reply_markup and message.reply_markup.inline_keyboard:
        for row in message.reply_markup.inline_keyboard:
            for button in row:
                if button.url:
                    return button.url
                    
    # 2. Fallback to checking standard links in text/captions
    entities = message.entities or message.caption_entities
    text = message.text or message.caption
    if entities and text:
        for entity in entities:
            if entity.type == 'text_link':
                return entity.url
            elif entity.type == 'url':
                return text[entity.offset:entity.offset+entity.length]
    return None

def clean_filename_for_display(filename: str) -> str:
    """Extracts a clean title from a subtitle filename."""
    # Remove common subtitle file extensions and quality tags
    name = os.path.splitext(filename)[0]
    name = name.replace('.', ' ').replace('_', ' ')
    name = re.sub(r'\[.*?\]', '', name) # Remove [tags]
    name = re.sub(r'\(.*?\)', '', name) # Remove (tags)
    name = re.sub(r'\b(S\d{2}E\d{2}|s\d{2}e\d{2})\b', '', name, flags=re.IGNORECASE) # Remove S01E01
    name = re.sub(r'\b(HDTV|WEB-DL|WEBRip|BluRay|x264|x265|AAC|MP4|720p|1080p|480p|HDRip|XviD|AC3|E-AC3)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(Dual Audio|Hindi|English|Multi|Dubbed|Subbed)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip() # Remove extra spaces
    return name if name else filename # Return original if cleaning results in empty string

def extract_channel_short_name_from_filename(filename: str) -> Optional[str]:
    """
    Extracts a short channel name (e.g., 'RI', 'SS') from a subtitle filename.
    Assumes the short name is typically 2-5 uppercase letters,
    often surrounded by delimiters like spaces, underscores, or brackets,
    and not a common video/subtitle tag.
    """
    # Convert to uppercase for consistent matching
    upper_filename = filename.upper()

    # Common tags to ignore (can be expanded)
    common_tags = {'ENG', 'ESP', 'FRE', 'GER', 'ITA', 'JPN', 'KOR', 'CHN', # Languages
                   'EP', 'S', 'E', # Episode/Season indicators
                   'HD', 'SD', '4K', 'WEB', 'DL', 'RIP', 'AAC', 'MP4', 'ASS', 'SRT', 'VTT', # Quality/Format
                   'X264', 'X265', 'HEVC', 'AVC', 'HDR', 'DV', 'DUB', 'SUB', 'DUAL', 'AUDIO', 'MULTI'} # Codecs/Other

    # Look for patterns like [CODE], _CODE_, -CODE-, or standalone CODE
    # This regex tries to capture 2-5 uppercase letters that are somewhat isolated
    matches = re.findall(r'\b([A-Z]{2,5})\b', upper_filename)

    for match in matches:
        if match not in common_tags:
            # Further check: if it's followed by numbers, it might be part of an episode/season tag
            # e.g., S01E01, but we already filter 'S', 'E'.
            # This is a heuristic, might need fine-tuning.
            return match
            
    return None