import os
import time
import uuid
import asyncio
import logging
import random
import psycopg2
import traceback
import psycopg2.extras
import re
from dotenv import load_dotenv
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ErrorEvent, BufferedInputFile, ChatMemberOwner, ChatMemberAdministrator
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from typing import Optional # Added for extract_channel_short_name_from_filename
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from a .env file if present
load_dotenv()

# --- Configuration & Environment Variables ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
ADS_BOT_ID = int(os.getenv("ADS_BOT_ID", 0)) # ID of the bot that posts ads
# Channel Management Bot variables
DESTINATION_CHANNEL_ID = int(os.getenv("DESTINATION_CHANNEL_ID", 0))
# Force Join Configuration
MAIN_CHANNEL_INVITE_LINK = os.getenv("MAIN_CHANNEL_INVITE_LINK") # Optional: for private main channel
WEB_APP_DOMAIN = os.getenv("WEB_APP_DOMAIN", "http://localhost:8080").rstrip('/')
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- PostgreSQL Database Setup ---
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute('''CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, file_id TEXT, filename TEXT)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS ads (channel_id BIGINT, message_id BIGINT, url TEXT, timestamp REAL)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS requests (request_key TEXT PRIMARY KEY, verified INTEGER, timestamp REAL, target_url TEXT)''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS channels (short_name TEXT PRIMARY KEY, channel_id BIGINT UNIQUE, full_name TEXT)''') # Ensure this is created
            # Statistics Tables
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, name TEXT, total_requests INTEGER DEFAULT 0, successful_receives INTEGER DEFAULT 0)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_file_requests (user_id BIGINT, file_hash TEXT, count INTEGER DEFAULT 0, PRIMARY KEY(user_id, file_hash))''')
            
            # Add message tracking columns for cleanup if they don't exist yet (ensure these run after table creation)
            for col in ['user_msg_id', 'bot_fwd_msg_id', 'bot_reply_msg_id']:
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='requests' AND column_name='{col}'")
                if not cursor.fetchone():
                    cursor.execute(f"ALTER TABLE requests ADD COLUMN {col} BIGINT")
            
            # Add filename column to files table if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='files' AND column_name='filename'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE files ADD COLUMN filename TEXT")

            # Add download_filename column to requests table if it doesn't exist
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='requests' AND column_name='download_filename'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE requests ADD COLUMN download_filename TEXT")

            # Commit any schema changes
            conn.commit()
else:
    logging.error("DATABASE_URL is not set! Data will not be saved.")

# --- Error Logger ---
@dp.error()
async def global_error_handler(event: ErrorEvent):
    """Catches all errors and sends a message to the Admin for debugging."""
    logging.error(f"Update: {event.update}\nException: {event.exception}")
    traceback.print_exception(type(event.exception), event.exception, event.exception.__traceback__)
    
    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🚨 <b>BOT ERROR!</b>\n\n<code>{event.exception}</code>\n\nCheck Render logs for full details."
            )
        except Exception:
            pass

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

# --- Temporary Admin State (for multi-step commands) ---
# Stores {admin_id: {'file_hash': '...', 'file_name': '...'}}
admin_temp_state = {}

# --- Utility Functions ---

# --- Telegram Bot Handlers ---

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

# Cache for eligible force join channels
ELIGIBLE_CHANNELS_CACHE = {"channels": [], "timestamp": 0}
CACHE_DURATION = 300 # Cache for 5 minutes

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

@dp.message(Command("setchannel"), F.from_user.id == ADMIN_ID)
async def set_channel_command(message: Message):
    """Admin command to register a channel with a short name."""
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.answer("Usage: `/setchannel <short_name> <channel_id_or_username>`\n\nExample: `/setchannel RI @RenegadeImmoral` or `/setchannel SS -1001234567890`", parse_mode=ParseMode.MARKDOWN)

    short_name = args[1].upper()
    channel_identifier = args[2]
    
    try:
        # Try to get chat info to resolve ID and full name
        chat = await bot.get_chat(channel_identifier)
        channel_id = chat.id
        full_name = chat.title

        if chat.type != 'channel':
            return await message.answer("❌ The provided ID/username does not belong to a channel.")

        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO channels (short_name, channel_id, full_name) VALUES (%s, %s, %s) ON CONFLICT (short_name) DO UPDATE SET channel_id = EXCLUDED.channel_id, full_name = EXCLUDED.full_name",
                               (short_name, channel_id, full_name))
                conn.commit()
        await message.answer(f"✅ Channel '{full_name}' registered as '{short_name}' (ID: <code>{channel_id}</code>).", parse_mode=ParseMode.HTML)
    except TelegramNotFound:
        await message.answer("❌ Channel not found. Make sure the bot is an admin in the channel and the ID/username is correct.")
    except Exception as e:
        await message.answer(f"❌ An error occurred: {e}")

@dp.callback_query(F.data.startswith("post_to_channel_"))
async def post_to_channel_callback(callback: CallbackQuery):
    """Handles admin's selection of a channel to post the subtitle to."""
    admin_id = callback.from_user.id
    short_name = callback.data.split("_")[3] # post_to_channel_SHORTNAME

    if admin_id not in admin_temp_state:
        return await callback.answer("Session expired. Please re-upload the file.", show_alert=True)

    file_info = admin_temp_state.pop(admin_id) # Get info and clear state
    file_hash = file_info['file_hash']
    original_filename = file_info['file_name']
    
    await callback.answer("Posting to channel...", show_alert=False)

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT channel_id, full_name FROM channels WHERE short_name = %s", (short_name,))
            channel_data = cursor.fetchone()

            if not channel_data:
                return await callback.message.answer(f"❌ Channel '{short_name}' not found in database. Please register it first.")

            target_channel_id = channel_data['channel_id']
            channel_full_name = channel_data['full_name']
            
            bot_info = await bot.me()
            deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
            
            # Extract episode information from the original filename
            episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', original_filename, re.IGNORECASE)
            episode_info = episode_match.group(0).upper() if episode_match else "" # e.g., "EP136"

            # Construct the desired caption for the channel post
            if episode_info:
                caption_text = f"<b>{channel_full_name} {episode_info}</b>"
            else:
                caption_text = f"<b>{channel_full_name}</b>"

            # Construct the desired filename for the download
            file_extension = os.path.splitext(original_filename)[1] or ".ass" # Default to .ass
            download_filename = f"[ENG] {short_name} {episode_info} @{bot_info.username}{file_extension}".strip().replace("  ", " ")

            # Update the deep link to include the desired filename
            deep_link = f"https://t.me/{bot_info.username}?start={file_hash}_{download_filename}"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]
            ])
            
            try:
                await bot.send_message(
                    chat_id=target_channel_id, # Post to the target channel
                    text=caption_text, # Use the newly constructed text
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
                await callback.message.answer(f"✅ Subtitle for '<b>{original_filename}</b>' posted to channel '<b>{channel_full_name}</b>'!", parse_mode=ParseMode.HTML)
            except TelegramBadRequest as e:
                await callback.message.answer(f"❌ Failed to post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nMake sure the bot is an admin in the channel and has permission to post messages.", parse_mode=ParseMode.HTML)
            except Exception as e:
                await callback.message.answer(f"❌ An unexpected error occurred while posting: {e}", parse_mode=ParseMode.HTML)

@dp.message(Command("stats"), F.from_user.id == ADMIN_ID)
async def view_stats(message: Message):
    """Admin command to view user request statistics in a clean table format."""
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT SUM(total_requests), SUM(successful_receives) FROM users")
            totals = cursor.fetchone()
            tot_req = totals[0] or 0
            tot_succ = totals[1] or 0
 
            if tot_req == 0:
                return await message.answer("📊 No file requests have been made yet.")
 
            success_rate = (tot_succ / tot_req * 100) if tot_req > 0 else 0
 
            cursor.execute("SELECT user_id, name, total_requests, successful_receives FROM users ORDER BY successful_receives DESC, total_requests DESC")
            users = cursor.fetchall()
            user_count = len(users)
 
            # --- Build the table inside a <code> block ---
            lines = [
                "📊 Bot Statistics",
                "---------------------------------",
                "Global:",
                f"  - {'Clicks:':<17}{tot_req}",
                f"  - {'Files Received:':<17}{tot_succ}",
                f"  - {'Success Rate:':<17}{success_rate:.1f}%",
                f"  - {'Total Users:':<17}{user_count}",
                "",
                f"👥 Top {min(len(users), 10)} Users:",
                "---------------------------------"
            ]
 
            if not users:
                lines.append("No user data available yet.")
            
            for i, row in enumerate(users[:10], 1): # Limit to top 10 users
                uid = row['user_id']
                name = (row['name'] or "Unknown").strip()
                medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"#{i}")
 
                cursor.execute("""
                    SELECT f.filename, ufr.count 
                    FROM user_file_requests ufr
                    JOIN files f ON ufr.file_hash = f.hash
                    WHERE ufr.user_id = %s AND f.filename IS NOT NULL
                    ORDER BY ufr.count DESC
                """, (uid,))
                file_rows = cursor.fetchall()
 
                lines.append(f"{medal} {name}")
                lines.append(f"   - {'ID:':<11}{uid}")
                lines.append(f"   - {'Clicks:':<11}{row['total_requests']}")
                lines.append(f"   - {'Received:':<11}{row['successful_receives']}")
 
                if file_rows:
                    file_stats = {} # Aggregate counts per short_name
                    for f_row in file_rows:
                        short_name = extract_channel_short_name_from_filename(f_row['filename']) or "N/A"
                        file_stats[short_name] = file_stats.get(short_name, 0) + f_row['count']
                    
                    sorted_files = sorted(file_stats.items(), key=lambda item: item[1], reverse=True)
                    files_str = ", ".join([f"{name}(x{count})" for name, count in sorted_files])
                    lines.append(f"   - {'Files:':<11}{files_str}")
                lines.append("") # Add a blank line for spacing
 
    # Remove the last blank line
    if lines and lines[-1] == "":
        lines.pop()
 
    text = "\n".join(lines)
    # Sanitize for HTML before putting in <code>
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    await message.answer(f"<code>{text}</code>", parse_mode=ParseMode.HTML)

@dp.message(Command("getdata"), F.from_user.id == ADMIN_ID)
async def get_data_pdf(message: Message):
    """Admin command to export stats as a formatted PDF file."""
    try:
        import io
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    except ImportError:
        return await message.answer(
            "❌ <b>reportlab</b> is not installed.\n"
            "Add <code>reportlab</code> to your <code>requirements.txt</code> and redeploy."
        )

    status_msg = await message.answer("⏳ Generating PDF report…")

    # --- Fetch all data ---
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT SUM(total_requests), SUM(successful_receives) FROM users")
            totals = cursor.fetchone()
            tot_req = totals[0] or 0
            tot_succ = totals[1] or 0

            if tot_req == 0:
                await status_msg.delete()
                return await message.answer("📊 No data yet to export.")

            success_rate = (tot_succ / tot_req * 100) if tot_req > 0 else 0

            cursor.execute(
                "SELECT user_id, name, total_requests, successful_receives FROM users ORDER BY successful_receives DESC"
            )
            users = cursor.fetchall()

            user_data = []
            for row in users:
                cursor.execute(
                    "SELECT file_hash, count FROM user_file_requests WHERE user_id = %s ORDER BY count DESC",
                    (row['user_id'],)
                )
                user_data.append({
                    'user_id':   row['user_id'],
                    'name':      row['name'] or 'Unknown',
                    'clicks':    row['total_requests'],
                    'received':  row['successful_receives'],
                    'files':     cursor.fetchall()
                })

    # --- Build PDF in memory ---
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        topMargin=1.5*cm, bottomMargin=1.5*cm,
        leftMargin=2*cm, rightMargin=2*cm,
        title="TheFrictionRealm Stats"
    )

    # Colour palette
    C_DARK    = colors.HexColor('#1a1a2e')
    C_ACCENT  = colors.HexColor('#e94560')
    C_MID     = colors.HexColor('#16213e')
    C_ROW_A   = colors.HexColor('#f4f4f8')
    C_ROW_B   = colors.white
    C_BORDER  = colors.HexColor('#d0d0d8')
    C_TEXT    = colors.HexColor('#222222')
    C_SUBTLE  = colors.HexColor('#777777')

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('T', fontName='Helvetica-Bold', fontSize=20,
                                 textColor=C_DARK, alignment=TA_CENTER, spaceAfter=4)
    sub_style   = ParagraphStyle('S', fontName='Helvetica', fontSize=9,
                                 textColor=C_SUBTLE, alignment=TA_CENTER, spaceAfter=18)
    section_style = ParagraphStyle('SEC', fontName='Helvetica-Bold', fontSize=12,
                                   textColor=C_DARK, spaceBefore=14, spaceAfter=6)

    elements = []
    ts_display = time.strftime('%d %B %Y  ·  %H:%M UTC', time.gmtime())

    # Header
    elements.append(Paragraph("TheFrictionRealm — Stats Report", title_style))
    elements.append(Paragraph(f"Generated: {ts_display}", sub_style))
    elements.append(HRFlowable(width="100%", thickness=2, color=C_ACCENT, spaceAfter=16))

    # ── Global Overview table ──────────────────────────────────────────────
    elements.append(Paragraph("Global Overview", section_style))
    ov_data = [
        ['Metric', 'Value'],
        ['Total Links Clicked',  str(tot_req)],
        ['Total Files Received', str(tot_succ)],
        ['Overall Success Rate', f'{success_rate:.1f}%'],
        ['Total Users',          str(len(user_data))],
    ]
    ov_style = [
        ('BACKGROUND',   (0, 0), (-1, 0), C_DARK),
        ('TEXTCOLOR',    (0, 0), (-1, 0), colors.white),
        ('FONTNAME',     (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE',     (0, 0), (-1, 0), 10),
        ('FONTNAME',     (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE',     (0, 1), (-1, -1), 10),
        ('ALIGN',        (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN',       (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID',         (0, 0), (-1, -1), 0.5, C_BORDER),
        ('TOPPADDING',   (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 8),
    ]
    for i in range(1, len(ov_data)):
        ov_style.append(('BACKGROUND', (0, i), (-1, i), C_ROW_A if i % 2 else C_ROW_B))

    ov_table = Table(ov_data, colWidths=[10*cm, 6*cm])
    ov_table.setStyle(TableStyle(ov_style))
    elements.append(ov_table)
    elements.append(Spacer(1, 0.6*cm))

    # ── User Breakdown table ───────────────────────────────────────────────
    elements.append(HRFlowable(width="100%", thickness=1, color=C_BORDER, spaceAfter=0))
    elements.append(Paragraph("User Breakdown", section_style))

    medals = {1: '🥇', 2: '🥈', 3: '🥉'}
    ub_data = [['#', 'Name', 'User ID', 'Clicks', 'Received', 'Rate', 'Files Requested']]
    for i, u in enumerate(user_data, 1):
        rank = medals.get(i, str(i))
        rate = f"{(u['received'] / u['clicks'] * 100):.0f}%" if u['clicks'] > 0 else '0%'
        files_str = '\n'.join(
            [f"{f['file_hash']}  ×{f['count']}" for f in u['files']]
        ) or '—'
        ub_data.append([rank, u['name'], str(u['user_id']),
                        str(u['clicks']), str(u['received']), rate, files_str])

    # Landscape A4 usable width ≈ 25.7 cm
    col_w = [1.2*cm, 4.5*cm, 3.8*cm, 2*cm, 2.4*cm, 2*cm, 7*cm]
    ub_style = [
        ('BACKGROUND',   (0, 0), (-1, 0), C_ACCENT),
        ('TEXTCOLOR',    (0, 0), (-1, 0), colors.white),
        ('FONTNAME',     (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE',     (0, 0), (-1, 0), 9),
        ('FONTNAME',     (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE',     (0, 1), (-1, -1), 8),
        ('ALIGN',        (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN',        (1, 1), (1, -1), 'LEFT'),  # Name left-aligned
        ('ALIGN',        (6, 1), (6, -1), 'LEFT'),  # Files left-aligned
        ('VALIGN',       (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID',         (0, 0), (-1, -1), 0.5, C_BORDER),
        ('TOPPADDING',   (0, 0), (-1, -1), 7),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 7),
        ('LEFTPADDING',  (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [C_ROW_A, C_ROW_B]),
    ]
    ub_table = Table(ub_data, colWidths=col_w, repeatRows=1)
    ub_table.setStyle(TableStyle(ub_style))
    elements.append(ub_table)

    # Footer line
    elements.append(Spacer(1, 0.5*cm))
    elements.append(HRFlowable(width="100%", thickness=1, color=C_BORDER))
    footer_style = ParagraphStyle('F', fontName='Helvetica', fontSize=8,
                                  textColor=C_SUBTLE, alignment=TA_CENTER, spaceBefore=6)
    elements.append(Paragraph("TheFrictionRealm Bot  ·  Admin Export", footer_style))

    doc.build(elements)
    buffer.seek(0)

    ts_file = time.strftime('%Y%m%d_%H%M', time.gmtime())
    await status_msg.delete()
    await bot.send_document(
        message.chat.id,
        BufferedInputFile(buffer.read(), filename=f"frictionrealm_stats_{ts_file}.pdf"),
        caption=f"📊 <b>TheFrictionRealm Stats</b>\n<i>{ts_display}</i>",
        parse_mode=ParseMode.HTML
    )


@dp.message(F.document, F.from_user.id == ADMIN_ID)
async def handle_admin_upload(message: Message):
    """Admin uploads a subtitle file to generate a link."""
    file_id = message.document.file_id
    file_hash = uuid.uuid4().hex[:8]
    
    original_filename = message.document.file_name

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO files (hash, file_id, filename) VALUES (%s, %s, %s)", (file_hash, file_id, original_filename))
    
    # --- NEW LOGIC: Try to auto-detect channel from filename ---
    short_name_from_filename = extract_channel_short_name_from_filename(original_filename)
    
    if short_name_from_filename:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("SELECT channel_id, full_name FROM channels WHERE short_name = %s", (short_name_from_filename,))
                channel_data = cursor.fetchone()

                if channel_data:
                    target_channel_id = channel_data['channel_id']
                    channel_full_name = channel_data['full_name']
                    
                    bot_info = await bot.me()
                    deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
                    
                    # Extract episode information from the original filename
                    episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', original_filename, re.IGNORECASE)
                    episode_info = episode_match.group(0).upper() if episode_match else "" # e.g., "EP136"

                    # Construct the desired caption for the channel post
                    if episode_info:
                        caption_text = f"<b>{channel_full_name} {episode_info}</b>"
                    else:
                        caption_text = f"<b>{channel_full_name}</b>"

                    # Construct the desired filename for the download
                    file_extension = os.path.splitext(original_filename)[1] or ".ass" # Default to .ass
                    download_filename = f"[ENG] {short_name_from_filename} {episode_info} @{bot_info.username}{file_extension}".strip().replace("  ", " ")

                    deep_link = f"https://t.me/{bot_info.username}?start={file_hash}_{download_filename}"
                    
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]
                    ])
                    
                    try:
                        await bot.send_message(
                            chat_id=target_channel_id,
                            text=caption_text,
                            reply_markup=keyboard,
                            parse_mode=ParseMode.HTML
                        )
                        # If successful, we are done, return here.
                        return await message.answer(f"✅ Subtitle for '<b>{original_filename}</b>' automatically posted to channel '<b>{channel_full_name}</b>'!", parse_mode=ParseMode.HTML)
                    except TelegramBadRequest as e:
                        # Log error and fall through to manual selection
                        await message.answer(f"❌ Failed to auto-post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nFalling back to manual selection. Make sure the bot is an admin in the channel and has permission to post messages.", parse_mode=ParseMode.HTML)
                    except Exception as e:
                        # Log error and fall through to manual selection
                        await message.answer(f"❌ An unexpected error occurred during auto-post: {e}\n\nFalling back to manual selection.", parse_mode=ParseMode.HTML)
    # --- END NEW LOGIC ---

    # Fallback to manual channel selection if auto-detection failed or no short name was found
    # Store file info temporarily for channel selection
    admin_temp_state[message.from_user.id] = {
        'file_hash': file_hash,
        'file_name': original_filename
    }

    # Get registered channels for inline keyboard
    channels_keyboard_buttons = []
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT short_name, full_name FROM channels ORDER BY short_name")
            for row in cursor.fetchall():
                channels_keyboard_buttons.append(
                    [InlineKeyboardButton(text=f"{row['short_name']} ({row['full_name']})", callback_data=f"post_to_channel_{row['short_name']}")]
                )
    
    if not channels_keyboard_buttons:
        return await message.answer(
            f"✅ File '<b>{original_filename}</b>' uploaded successfully.\n\n"
            f"⚠️ No channels registered. Use `/setchannel <short_name> <channel_id_or_username>` to add one, then re-upload the file to post it."
        )

    await message.answer(
        f"✅ File '<b>{original_filename}</b>' uploaded successfully!\n\n"
        f"Now, select the channel where you want to post the 'Download Subtitle' button:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=channels_keyboard_buttons),
        parse_mode=ParseMode.HTML
    )

@dp.message(F.document)
async def handle_unauthorized_upload(message: Message):
    """Catches document uploads from non-admins and sends a generic denial message."""
    await message.answer(
        "🚫 <b>Access Denied</b>\n\n"
        "You are not authorized to upload files. This is a private bot and file uploads are restricted to the administrator.",
        parse_mode=ParseMode.HTML
    )

@dp.message(Command("check_dest"), F.from_user.id == ADMIN_ID)
async def check_destination_channel(message: Message):
    """Admin command to verify the bot can access the destination channel."""
    if not DESTINATION_CHANNEL_ID:
        return await message.answer("⚠️ The `DESTINATION_CHANNEL_ID` is not set. Please configure it in your environment variables.")

    try:
        chat = await bot.get_chat(DESTINATION_CHANNEL_ID)
        my_member_info = await bot.get_chat_member(DESTINATION_CHANNEL_ID, bot.id)
        
        can_post = False
        status = my_member_info.status.capitalize()
        if isinstance(my_member_info, ChatMemberOwner):
            can_post = True
        elif isinstance(my_member_info, ChatMemberAdministrator):
            if my_member_info.can_post_messages:
                can_post = True

        perm_text = "✅ Can post photos" if can_post else "❌ Cannot post photos"
        
        await message.answer(
            f"✅ <b>Destination Channel Check: OK</b>\n\n"
            f"The bot can access the destination channel.\n\n"
            f"<b>Name:</b> {chat.title}\n"
            f"<b>ID:</b> <code>{chat.id}</code>\n"
            f"<b>Bot Status:</b> {status}\n"
            f"<b>Permissions:</b> {perm_text}"
        , parse_mode=ParseMode.HTML)
    except TelegramNotFound:
        await message.answer(
            f"🚨 <b>Destination Channel Check: FAILED</b>\n\n"
            f"The bot could not find the channel with ID: <code>{DESTINATION_CHANNEL_ID}</code>.\n\n"
            f"<b>This is the reason the `/post` command is failing with 'chat not found'.</b>\n\n"
            f"<b>How to fix:</b>\n"
            f"1. Make sure the `DESTINATION_CHANNEL_ID` in your environment variables is correct.\n"
            f"2. Add the bot to your destination channel as an admin."
        , parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.answer(f"An unexpected error occurred while checking the channel: {e}")

@dp.message(Command("editbutton"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def edit_inline_button(message: Message):
    """
    Admin command to edit the inline button of a message previously posted by the bot.
    Admin replies to the bot's forwarded message with /editbutton New Button Text | https://new.link
    """
    forwarded_message = message.reply_to_message
    
    if not DESTINATION_CHANNEL_ID:
        return await message.reply("⚠️ `DESTINATION_CHANNEL_ID` is not set. Cannot edit buttons.")

    # Validate that the forwarded message is from the destination channel
    if not forwarded_message.forward_from_chat or forwarded_message.forward_from_chat.id != DESTINATION_CHANNEL_ID:
        return await message.reply(f"❌ The forwarded message must be from the destination channel (ID: <code>{DESTINATION_CHANNEL_ID}</code>).", parse_mode=ParseMode.HTML)

    # Validate that the forwarded message was originally sent by this bot
    # When a bot posts to a channel, and that message is forwarded, forward_from will be the bot.
    if not forwarded_message.forward_from or forwarded_message.forward_from.id != (await bot.me()).id:
        return await message.reply("❌ The forwarded message must be one originally posted by this bot.", parse_mode=ParseMode.HTML)

    # Parse command arguments: new_button_text | new_url
    command_args = message.text.split(' ', 1)
    if len(command_args) < 2 or '|' not in command_args[1]:
        return await message.reply("❌ Usage: `/editbutton New Button Text | https://new.link`", parse_mode=ParseMode.MARKDOWN_V2)

    args_string = command_args[1]
    button_text, url = args_string.split('|', 1)
    button_text = button_text.strip()
    url = url.strip()

    if not button_text or not url:
        return await message.reply("❌ Button text and URL cannot be empty.", parse_mode=ParseMode.HTML)
    if not (url.startswith("http://") or url.startswith("https://")):
        return await message.reply("❌ Invalid URL. Must start with `http://` or `https://`.", parse_mode=ParseMode.HTML)

    # Construct the new keyboard
    new_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=button_text, url=url)]
    ])

    try:
        await bot.edit_message_reply_markup(
            chat_id=DESTINATION_CHANNEL_ID,
            message_id=forwarded_message.forward_from_message_id,
            reply_markup=new_keyboard
        )
        await message.reply("✅ Inline button successfully updated! 🎉", parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Failed to edit inline button for message {forwarded_message.forward_from_message_id} in channel {DESTINATION_CHANNEL_ID}. Error: {e}")
        await message.reply(f"🚨 Failed to update button: <code>{e}</code>\n\nMake sure the bot has permission to edit messages in the channel.", parse_mode=ParseMode.HTML)

@dp.message(Command("post"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def post_forwarded_message(message: Message):
    """
    Admin command to re-post a forwarded message to the destination channel.
    The admin must reply to the forwarded message with /post.
    """
    # The message to be posted is the one being replied to
    forwarded_message = message.reply_to_message

    # --- Validations ---
    if not forwarded_message.forward_from_chat:
        return await message.reply("❌ This command only works when replying to a forwarded message from a channel.")

    if not forwarded_message.photo or not forwarded_message.caption:
        return await message.reply("❌ The forwarded message must contain a photo and a caption.")

    if not DESTINATION_CHANNEL_ID:
        return await message.reply("⚠️ The `DESTINATION_CHANNEL_ID` is not set. Please configure it in your environment variables.")

    # --- Logic from the old handler, adapted ---
    photo_file_id = forwarded_message.photo[-1].file_id

    # 1. Extract Episode Number (e.g., "EP12" or "S01E02")
    episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', forwarded_message.caption, re.IGNORECASE)
    if not episode_match:
        return await message.reply(f"❌ No episode pattern (e.g., EP123 or S01E02) found in the caption. Cannot create button.")
    
    episode_info = episode_match.group(0).upper()

    # 2. Get original post link
    original_chat = forwarded_message.forward_from_chat
    if not original_chat.username:
        return await message.reply(f"❌ The source channel (<code>{original_chat.id}</code>) must be public and have a username to create a post link.", parse_mode=ParseMode.HTML)
        
    original_post_link = f"https://t.me/{original_chat.username}/{forwarded_message.forward_from_message_id}"

    # 3. Construct the inline button
    button_text = f"⬇️ {episode_info} | Download Here"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=button_text, url=original_post_link)]])

    # 4. Send the new message
    try:
        # Use copy_message for a perfect clone, then edit to add the button.
        # This is the most reliable way to preserve all formatting (including quotes) and media.
        copied_message = await bot.copy_message(
            chat_id=DESTINATION_CHANNEL_ID,
            from_chat_id=forwarded_message.chat.id,
            message_id=forwarded_message.message_id
        )

        # Now, edit the copied message to add our custom inline button
        await bot.edit_message_reply_markup(
            chat_id=DESTINATION_CHANNEL_ID,
            message_id=copied_message.message_id,
            reply_markup=keyboard
        )

        await message.reply(f"✅ Successfully posted to channel <code>{DESTINATION_CHANNEL_ID}</code>.", parse_mode=ParseMode.HTML)
        logging.info(f"Admin {message.from_user.id} successfully posted message {forwarded_message.forward_from_message_id} to channel {DESTINATION_CHANNEL_ID}.")
    except TelegramBadRequest as e:
        logging.error(f"Failed to post forwarded message for admin. Error: {e}")
        error_message = str(e)
        reply_text = f"🚨 <b>Posting Failed!</b>\n\nCould not send the post to the destination channel (ID: <code>{DESTINATION_CHANNEL_ID}</code>).\n\n<b>Error:</b>\n<code>{error_message}</code>\n\n"
        
        if "chat not found" in error_message.lower():
            reply_text += "<b>Suggestion:</b> This error means the bot cannot find the channel. Please ensure:\n1. The `DESTINATION_CHANNEL_ID` is correct.\n2. The bot has been added as a member (or admin) to the channel."
        else:
            reply_text += "<b>Suggestion:</b> Make sure the bot is an admin in the destination channel with permission to post photos."
        await message.reply(reply_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Failed to post forwarded message for admin. Error: {e}")
        await message.reply(f"🚨 <b>An unexpected error occurred!</b>\n\n<b>Error:</b>\n<code>{e}</code>\n\nCheck the bot logs for more details.", parse_mode=ParseMode.HTML)

@dp.channel_post()
async def track_channel_ads(message: Message):
    """Monitors any channel the bot is in to automatically log ads."""
    # Only track messages sent by the designated ads bot.
    if not ADS_BOT_ID or not message.via_bot or message.via_bot.id != ADS_BOT_ID:
        return

    ad_url = extract_ad_url(message)
    if ad_url:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM ads WHERE message_id = %s", (message.message_id,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (%s, %s, %s, %s)",
                                   (message.chat.id, message.message_id, ad_url, time.time()))
                    logging.info(f"✨ New ad registered automatically from Ads Bot: {ad_url}")

@dp.message(Command("addad"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def register_previous_ad_command(message: Message):
    """Admin replies to a forwarded ad with /addad to register it."""
    forwarded_message = message.reply_to_message

    if not forwarded_message.forward_from_chat or forwarded_message.forward_from_chat.type != 'channel':
        return await message.reply("❌ This command only works when replying to a message forwarded from a channel.")

    ad_url = extract_ad_url(forwarded_message)
    if ad_url:
        orig_msg_id = forwarded_message.forward_from_message_id
        channel_id = forwarded_message.forward_from_chat.id
        
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM ads WHERE message_id = %s AND channel_id = %s", (orig_msg_id, channel_id))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (%s, %s, %s, %s)", (channel_id, orig_msg_id, ad_url, time.time())) # type: ignore
                    await message.reply(f"✅ Ad (ID: {orig_msg_id}) successfully registered! 🎉")
                else:
                    await message.reply("⚠️ This ad is already in the database. 🤔")
    else:
        await message.reply("❌ No URL found in the replied message. Is this an ad?")

async def proceed_with_verification(chat_id: int, user_full_name: str, file_hash: str, user_msg_id: int, download_filename_override: Optional[str] = None):
    """Handles the ad forwarding and verification message sending."""
    request_key = f"{chat_id}_{file_hash}"
    
    ad_url = "https://example.com"
    ad_msg_id = None
    ad_channel_id = None
    
    fwd_msg_id = None
    bot_reply_msg_id = None # The bot's message with the inline buttons
    max_ad_attempts = 5 # Prevent infinite loops if all ads are bad
    attempt_count = 0
    
    # Connect to DB once for the whole ad selection process
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Loop to find a valid ad to forward
            while attempt_count < max_ad_attempts:
                # Clean up ads older than 24 hours (86400 seconds) from DB
                cursor.execute("DELETE FROM ads WHERE %s - timestamp > 86400", (time.time(),))
                
                cursor.execute("SELECT channel_id, message_id, url FROM ads")
                ads_db = cursor.fetchall()

                if not ads_db:
                    # No ads available, break loop and use fallback
                    break 

                selected_ad = random.choice(ads_db)
                ad_url, ad_msg_id, ad_channel_id = selected_ad['url'], selected_ad['message_id'], selected_ad['channel_id']

                try:
                    # Attempt to forward the ad
                    sent_fwd = await bot.forward_message(
                        chat_id=chat_id,
                        from_chat_id=ad_channel_id,
                        message_id=ad_msg_id
                    )
                    fwd_msg_id = sent_fwd.message_id
                    # If successful, break the loop
                    break 
                except (TelegramBadRequest, TelegramNotFound) as e: # type: ignore
                    logging.warning(f"Ad message {ad_msg_id} in channel {ad_channel_id} not found or deleted. Removing from DB. Error: {e}")
                    # If ad is deleted, remove it from DB and try again
                    cursor.execute("DELETE FROM ads WHERE channel_id = %s AND message_id = %s", (ad_channel_id, ad_msg_id))
                    conn.commit() # Commit deletion immediately
                    ad_msg_id = None # Reset to ensure fallback if no other ads work
                    attempt_count += 1
                except Exception as e:
                    logging.error(f"Unexpected error forwarding ad {ad_msg_id}: {e}")
                    ad_msg_id = None
                    attempt_count += 1
            
            cursor.execute("""
                INSERT INTO users (user_id, name, total_requests, successful_receives) 
                VALUES (%s, %s, 1, 0) 
                ON CONFLICT(user_id) DO UPDATE SET total_requests = users.total_requests + 1, name = EXCLUDED.name
            """, (chat_id, user_full_name))
            
    track_url = f"{WEB_APP_DOMAIN}/track?u={chat_id}&h={file_hash}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Click Ad to Verify 👀", url=track_url)],
        [InlineKeyboardButton(text="2️⃣ Get Subtitle File 📥", callback_data=f"get_{file_hash}")]
    ])

    if ad_msg_id and ad_channel_id:
        bot_reply_msg_id = (await bot.send_message(
            chat_id,
            "<b>Verification Required!</b>\n\n"
            "Please click the 'Click Ad to Verify' button below. 👇\n"
            "After verifying, click 'Get Subtitle File' to receive your file. ✨",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )).message_id
    else:
        bot_reply_msg_id = (await bot.send_message(
            chat_id,
            f"<b>Verification Required!</b>\n\n"
            f"Please click the verification button below. 👇\n\n"
            f"<i>(No specific ad available at the moment, but verification is still needed.)</i>",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )).message_id
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO requests (request_key, verified, timestamp, target_url, user_msg_id, bot_fwd_msg_id, bot_reply_msg_id, download_filename) 
                VALUES (%s, 0, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(request_key) DO UPDATE SET verified = 0, timestamp = EXCLUDED.timestamp, target_url = EXCLUDED.target_url, user_msg_id = EXCLUDED.user_msg_id, bot_fwd_msg_id = EXCLUDED.bot_fwd_msg_id, bot_reply_msg_id = EXCLUDED.bot_reply_msg_id, download_filename = EXCLUDED.download_filename
            """, (request_key, time.time(), ad_url, user_msg_id, fwd_msg_id, bot_reply_msg_id, download_filename_override))
    
    # Schedule cleanup if user doesn't interact
    asyncio.create_task(cleanup_unclicked_request(request_key, chat_id, delay=300))

@dp.message(CommandStart())
async def handle_start(message: Message, command: CommandStart):
    """Handles the user clicking the deep link."""
    raw_args = command.args # Store original args
    file_hash = raw_args
    download_filename_override_decoded = None
    download_filename_override_encoded = None # To store the encoded version for callback_data
    user_id = message.from_user.id
    user_full_name = message.from_user.full_name

    # 1. Initial validation
    if not file_hash:
        return await message.answer(
            "👋 <b>Welcome to the File Request Bot!</b>\n\n"
            "To get a file, you need to use a special link from one of our channels. This bot doesn't support direct file searches or other commands.",
            parse_mode=ParseMode.HTML
        )

    # Check if a custom filename was provided in the deep link
    if '_' in raw_args:
        file_hash, download_filename_override_encoded = raw_args.split('_', 1)
        download_filename_override_decoded = unquote(download_filename_override_encoded) # Decode URL-encoded filename

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT file_id FROM files WHERE hash = %s", (file_hash,))
            if not cursor.fetchone():
                return await message.answer("This file link is invalid or has expired.")

    # 2. Force Join Check
    if DESTINATION_CHANNEL_ID:
        try:
            member = await bot.get_chat_member(DESTINATION_CHANNEL_ID, user_id)
            if member.status in ['left', 'kicked']:
                # User is not in the channel, ask them to join.
                chat = await bot.get_chat(DESTINATION_CHANNEL_ID)
                invite_link = chat.invite_link or MAIN_CHANNEL_INVITE_LINK
                
                if not invite_link:
                    logging.error(f"No invite link available for DESTINATION_CHANNEL_ID {DESTINATION_CHANNEL_ID}. Cannot enforce join. Please set MAIN_CHANNEL_INVITE_LINK for private channels.")
                else:
                    # Use the *encoded* filename for the callback data to prevent issues with splitting
                    callback_data_filename_part = download_filename_override_encoded if download_filename_override_encoded else ''
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=f"1. Join {chat.title} 🚀", url=invite_link)],
                        [InlineKeyboardButton(text="2. ✅ I Have Joined", callback_data=f"verify_join_{file_hash}_{callback_data_filename_part}")]
                    ])
                    return await message.answer(
                        "<b>Join Required!</b>\n\nTo get your file, you must first join our channel. 👇",
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML
                    )
        except Exception as e:
            logging.error(f"Could not check channel membership for user {user_id} in channel {DESTINATION_CHANNEL_ID}. Error: {e}")
            # If check fails, proceed without verification to not block the user.
    # 3. Proceed to ad verification with the *decoded* filename
    await proceed_with_verification(user_id, user_full_name, file_hash, message.message_id, download_filename_override_decoded)

@dp.callback_query(F.data.startswith("get_"))
async def serve_file(callback: CallbackQuery):
    """Verifies the ad click and serves the file if valid."""
    user_id = callback.from_user.id
    file_hash = callback.data.split("_")[1]
    request_key = f"{user_id}_{file_hash}"
    
    download_filename = None
    file_id = None
    user_msg_id = None
    bot_fwd_msg_id = None
    bot_reply_msg_id = None
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT verified, timestamp, user_msg_id, bot_fwd_msg_id, bot_reply_msg_id, download_filename FROM requests WHERE request_key = %s", (request_key,))
            req = cursor.fetchone()
            
            if not req:
                return await callback.answer("Invalid or expired session. Please click the original link again.", show_alert=True)
                
            if time.time() - req['timestamp'] > 300:
                cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,))
                return await callback.answer("Request expired (5 minutes limit). Please generate a new one.", show_alert=True)
                
            if not req['verified']:
                return await callback.answer("❌ You must click the 'Click Ad to Verify' button first!", show_alert=True)
                
            user_msg_id = req.get('user_msg_id')
            bot_fwd_msg_id = req.get('bot_fwd_msg_id')
            bot_reply_msg_id = req.get('bot_reply_msg_id') # Now this will be fetched correctly
            download_filename = req.get('download_filename')

            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            cursor.execute("SELECT file_id, filename FROM files WHERE hash = %s", (file_hash,))
            file_row = cursor.fetchone()
            if file_row: # type: ignore
                cursor.execute("UPDATE users SET successful_receives = successful_receives + 1 WHERE user_id = %s", (user_id,))
                cursor.execute("""
                    INSERT INTO user_file_requests (user_id, file_hash, count) VALUES (%s, %s, 1)
                    ON CONFLICT(user_id, file_hash) DO UPDATE SET count = user_file_requests.count + 1
                """, (user_id, file_hash))
                file_id = file_row[0]
                if not download_filename:
                    download_filename = file_row[1]
                
            cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,))
            
    if file_id:
        caption = "✅ Here is your requested subtitle file! 🎉\n\n⏳ <i>This file will be automatically deleted in 5 minutes.</i>"
        sent_file = None
        if download_filename:
            try:
                # To send with a custom filename, we must download and re-upload
                file_info = await bot.get_file(file_id)
                file_content = await bot.download_file(file_info.file_path)
                document_to_send = BufferedInputFile(file_content.read(), filename=download_filename)
                sent_file = await bot.send_document(user_id, document_to_send, caption=caption, parse_mode=ParseMode.HTML)
            except Exception as e:
                logging.error(f"Failed to re-upload file {file_id} with custom name. Falling back. Error: {e}")
                # Fallback to sending by file_id if re-upload fails
                sent_file = await bot.send_document(user_id, file_id, caption=caption, parse_mode=ParseMode.HTML)
        else:
            # No custom filename, just send by file_id
            sent_file = await bot.send_document(user_id, file_id, caption=caption, parse_mode=ParseMode.HTML)

        # Clean up previous messages
        msgs_to_delete = {callback.message.message_id}  # The message with the buttons
        if user_msg_id: msgs_to_delete.add(user_msg_id)  # The user's original /start command
        if bot_fwd_msg_id: msgs_to_delete.add(bot_fwd_msg_id) # The forwarded ad

        # Delete messages
        for m_id in msgs_to_delete:
            try:
                await bot.delete_message(user_id, m_id)
            except (TelegramNotFound, TelegramBadRequest):
                pass
                
        # Schedule deletion of the subtitle file
        if sent_file:
            asyncio.create_task(delete_message_later(user_id, sent_file.message_id, 300))
    else:
        await callback.answer("File no longer available. 😔", show_alert=True)
        
    await callback.answer()

@dp.callback_query(F.data.startswith("verify_join_"))
async def handle_join_verification(callback: CallbackQuery):
    """Handles the 'I have joined' button click."""
    # Split into 'verify', 'join', HASH
    parts = callback.data.split("_")
    file_hash = parts[2]
    # download_filename_override is no longer passed via callback data
    download_filename_override = None
    user_id = callback.from_user.id
    user_full_name = callback.from_user.full_name

    if not DESTINATION_CHANNEL_ID:
        await callback.answer("This check is no longer required.", show_alert=True)
        return await callback.message.delete()

    try:
        member = await bot.get_chat_member(DESTINATION_CHANNEL_ID, user_id)
        if member.status not in ['left', 'kicked']:
            # User has joined
            await callback.answer("Thank you for joining! Please wait... 🙏", show_alert=False)
            await callback.message.delete()
            # We don't have the original user message ID, but it's not critical. Pass 0.
            await proceed_with_verification(user_id, user_full_name, file_hash, 0)
        else:
            # User has not joined
            await callback.answer("❌ You haven't joined the channel yet. Please join and then click the button again. 🧐", show_alert=True)
    except Exception as e:
        logging.error(f"Error during join verification for user {user_id}: {e}")
        await callback.answer("An error occurred while verifying. Please try again. 🚧", show_alert=True)

@dp.callback_query(F.data.startswith("verify_post_join_"))
async def handle_post_join_verification(callback: CallbackQuery):
    """
    Handles the initial click on a /post button. Checks for channel join.
    """
    parts = callback.data.split("_", 3) # verify_post_join_CHANNELUSERNAME_MESSAGEID
    if len(parts) < 4:
        return await callback.answer("Invalid callback data.", show_alert=True)

    channel_username = parts[2]
    message_id = parts[3]
    user_id = callback.from_user.id

    if not DESTINATION_CHANNEL_ID:
        # If no force join channel is configured, just redirect directly
        original_post_link = f"https://t.me/{channel_username}/{message_id}"
        await callback.answer("Redirecting to post...", show_alert=False)
        # Edit the message to provide the direct link
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Download Here", url=original_post_link)]
        ])
        await callback.message.edit_reply_markup(reply_markup=keyboard)
        return

    try:
        member = await bot.get_chat_member(DESTINATION_CHANNEL_ID, user_id)
        if member.status not in ['left', 'kicked']:
            # User has joined, redirect them to the original post
            original_post_link = f"https://t.me/{channel_username}/{message_id}"
            await callback.answer("Redirecting to post...", show_alert=False)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬇️ Download Here", url=original_post_link)]
            ])
            await callback.message.edit_reply_markup(reply_markup=keyboard)
        else:
            # User has not joined, prompt them to join
            chat = await bot.get_chat(DESTINATION_CHANNEL_ID)
            invite_link = chat.invite_link or MAIN_CHANNEL_INVITE_LINK
            
            if not invite_link:
                logging.error(f"No invite link available for DESTINATION_CHANNEL_ID {DESTINATION_CHANNEL_ID}. Cannot enforce join for /post.")
                return await callback.answer("Channel join verification is currently unavailable. Please try again later.", show_alert=True)

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"1. Join {chat.title} 🚀", url=invite_link)],
                [InlineKeyboardButton(text="2. ✅ I Have Joined", callback_data=f"confirm_post_join_{channel_username}_{message_id}")]
            ])
            await callback.message.edit_text(
                "<b>Join Required!</b>\n\nTo access this post, you must first join our channel. 👇",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
            await callback.answer("Please join the channel first.", show_alert=True)
    except Exception as e:
        logging.error(f"Error during /post join verification for user {user_id}: {e}")
        await callback.answer("An error occurred while verifying. Please try again. 🚧", show_alert=True)

@dp.callback_query(F.data.startswith("confirm_post_join_"))
async def handle_confirm_post_join(callback: CallbackQuery):
    """Handles the 'I Have Joined' button click after a /post verification."""
    parts = callback.data.split("_", 3)
    channel_username = parts[2]
    message_id = parts[3]
    user_id = callback.from_user.id

    # Re-use the logic from handle_post_join_verification
    # This will either redirect or re-prompt if not joined
    await handle_post_join_verification(callback)


@dp.message(Command("ping"))
async def ping_handler(message: Message):
    """Simple command to test if the bot is alive."""
    await message.answer("🏓 Pong! The bot is online and actively receiving messages.")

@dp.message()
async def catch_all(message: Message):
    """Catches unhandled messages and provides helpful feedback, especially to the admin."""
    if message.from_user.id == ADMIN_ID and message.chat.type == 'private':
        if message.text:
            if message.text.startswith('/post'):
                return await message.reply("⚠️ <b>Command Error:</b>\nTo use <code>/post</code>, you must <b>reply</b> to a forwarded message that contains a photo and caption.", parse_mode=ParseMode.HTML)
            if message.text.startswith('/addad'):
                return await message.reply("⚠️ <b>Command Error:</b>\nTo use <code>/addad</code>, you must <b>reply</b> to a forwarded ad message.", parse_mode=ParseMode.HTML)
        # For other admin messages that are not handled, we can stay silent to avoid noise.
        return
    # For non-admins, we can also choose to be silent to prevent the bot from being chatty in groups or with random users.

# --- Web Server for Tracking Clicks ---
async def track_click(request: web.Request):
    """Endpoint that verifies the user and redirects to the actual ad."""
    user_id = request.query.get("u")
    file_hash = request.query.get("h")
    
    target_url = "https://t.me/TheFrictionRealm" # Fallback if none found

    if user_id and file_hash:
        request_key = f"{user_id}_{file_hash}"
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("SELECT target_url FROM requests WHERE request_key = %s", (request_key,))
                req = cursor.fetchone()
                if req:
                    # Use the stored target_url, or fallback to a default if it's None
                    target_url = req['target_url'] if req['target_url'] else target_url
                    cursor.execute("UPDATE requests SET verified = 1 WHERE request_key = %s", (request_key,))
                    conn.commit() # Commit verification update
                    logging.info(f"User {user_id} verified for file {file_hash}")
            
    raise web.HTTPFound(target_url)

async def health_check(request: web.Request):
    """Simple health check endpoint for cron jobs to keep the server alive."""
    logging.info("Health check / request received.")
    return web.Response(text="Bot is alive!", status=200)

# --- Startup Logic ---
async def main():
    app = web.Application()
    logging.info("Web application initialized.")
    app.router.add_get('/track', track_click)
    app.router.add_get('/', health_check) # Add the new health check route BEFORE runner.setup()
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, '0.0.0.0', PORT)
    logging.info(f"Attempting to start web server on port {PORT}...")
    await site.start()
    logging.info(f"Web server running on port {PORT}")
    
    # Delete any existing webhook before starting polling
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Starting Telegram bot...")
    await dp.start_polling(bot)
    logging.info("Telegram bot polling started.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped gracefully.")
