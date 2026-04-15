import os
import time
import uuid
import asyncio
import logging
import random
import psycopg2
import psycopg2.extras
import re
from dotenv import load_dotenv
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ErrorEvent, BufferedInputFile
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

async def delete_message_later(chat_id: int, message_id: int, delay: int):
    """Deletes a message after a specified delay in seconds."""
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logging.error(f"Failed to delete delayed message: {e}")

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT verified, bot_fwd_msg_id, bot_reply_msg_id FROM requests WHERE request_key = %s", (request_key,))
            req = cursor.fetchone()

            if req and not req['verified']: # If request exists and is still unverified
                logging.info(f"Cleaning up unclicked request {request_key} for user {chat_id}")
                
                # Delete bot's messages
                if req['bot_fwd_msg_id']:
                    try: await bot.delete_message(chat_id, req['bot_fwd_msg_id'])
                    except (TelegramNotFound, TelegramBadRequest): pass # Already deleted or invalid
                    except Exception as e: logging.error(f"Error deleting forwarded ad for {chat_id}: {e}")
                if req['bot_reply_msg_id']:
                    try: await bot.delete_message(chat_id, req['bot_reply_msg_id'])
                    except (TelegramNotFound, TelegramBadRequest): pass # Already deleted or invalid
                    except Exception as e: logging.error(f"Error deleting bot reply for {chat_id}: {e}")
                
                cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,)) # Delete the request from DB
                conn.commit()

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

            # Construct the desired message text for the channel post
            if episode_info:
                post_text = f"<b>{channel_full_name} {episode_info} Subtitle</b>"
            else:
                post_text = f"<b>{channel_full_name} Subtitle</b>"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="DOWNLOAD SUBTITLE", url=deep_link)]
            ])
            
            try:
                await bot.send_message(
                    chat_id=target_channel_id, # Post to the target channel
                    text=post_text, # Use the newly constructed text
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
                await callback.message.answer(f"✅ Subtitle for '<b>{display_name}</b>' posted to channel '<b>{channel_full_name}</b>'!", parse_mode=ParseMode.HTML)
            except TelegramBadRequest as e:
                await callback.message.answer(f"❌ Failed to post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nMake sure the bot is an admin in the channel and has permission to post messages.", parse_mode=ParseMode.HTML)
            except Exception as e:
                await callback.message.answer(f"❌ An unexpected error occurred while posting: {e}", parse_mode=ParseMode.HTML)

@dp.message(Command("stats"), F.from_user.id == ADMIN_ID)
async def view_stats(message: Message):
    """Admin command to view user request statistics."""
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

            lines = [
                "📊 <b>Bot Statistics</b>",
                "",
                "<b>Global Overview</b>",
                f"• Clicks: <b>{tot_req}</b>",
                f"• Files Received: <b>{tot_succ}</b>",
                f"• Success Rate: <b>{success_rate:.1f}%</b>",
                f"• Total Users: <b>{user_count}</b>",
                "",
                "---",
                "",
                f"👥 <b>Top Users</b> ({min(len(users), 10)} shown)",
            ]

            for i, row in enumerate(users[:10], 1): # Limit to top 10 users
                uid = row['user_id']
                name = row['name'] or "Unknown"
                medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"<b>#{i}</b>")

                cursor.execute("""
                    SELECT f.filename, ufr.count 
                    FROM user_file_requests ufr
                    JOIN files f ON ufr.file_hash = f.hash
                    WHERE ufr.user_id = %s AND f.filename IS NOT NULL
                    ORDER BY ufr.count DESC
                """, (uid,))
                file_rows = cursor.fetchall()

                lines.append("")
                lines.append(f"{medal} <b>{name}</b> (<code>{uid}</code>)")
                lines.append(f"    - Clicks: <b>{row['total_requests']}</b> | Received: <b>{row['successful_receives']}</b>")

                if file_rows:
                    file_stats = {} # Aggregate counts per short_name
                    for f_row in file_rows:
                        short_name = extract_channel_short_name_from_filename(f_row['filename']) or "N/A"
                        file_stats[short_name] = file_stats.get(short_name, 0) + f_row['count']
                    
                    sorted_files = sorted(file_stats.items(), key=lambda item: item[1], reverse=True)
                    files_str = ", ".join([f"<code>{name}</code> (x{count})" for name, count in sorted_files])
                    lines.append(f"    - Files: {files_str}")

    text = "\n".join(lines)
    await message.answer(text)

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
        caption=f"📊 <b>TheFrictionRealm Stats</b>\n<i>{ts_display}</i>"
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
                    
                    display_name = clean_filename_for_display(original_filename)
                    
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]
                    ])
                    
                    try:
                        await bot.send_message(
                            chat_id=target_channel_id,
                            text=f"🎬 <b>{display_name}</b>\n\nDownload the subtitle file below:",
                            reply_markup=keyboard,
                            parse_mode=ParseMode.HTML
                        )
                        # If successful, we are done, return here.
                        return await message.answer(f"✅ Subtitle for '<b>{display_name}</b>' automatically posted to channel '<b>{channel_full_name}</b>'!", parse_mode=ParseMode.HTML)
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
    """Catches document uploads from non-admins for debugging."""
    await message.answer(
        f"❌ <b>Unauthorized or ID Mismatch!</b>\n\n"
        f"Your Telegram ID: <code>{message.from_user.id}</code>\n"
        f"Bot's configured ADMIN_ID: <code>{ADMIN_ID}</code>\n\n"
        f"If these numbers don't match, you must update the ADMIN_ID variable in your Render dashboard!"
    )

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
    button_text = f"{episode_info} | Download Here"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=button_text, url=original_post_link)]])

    # 4. Send the new message
    try:
        await bot.send_photo(chat_id=DESTINATION_CHANNEL_ID, photo=photo_file_id, caption=forwarded_message.caption, caption_entities=forwarded_message.caption_entities, reply_markup=keyboard)
        await message.reply(f"✅ Successfully posted to channel <code>{DESTINATION_CHANNEL_ID}</code>.", parse_mode=ParseMode.HTML)
        logging.info(f"Admin {message.from_user.id} successfully posted message {forwarded_message.forward_from_message_id} to channel {DESTINATION_CHANNEL_ID}.")
    except Exception as e:
        logging.error(f"Failed to post forwarded message for admin. Error: {e}")
        await message.reply(f"🚨 <b>Posting Failed!</b>\n\nCould not send the post to the destination channel (ID: <code>{DESTINATION_CHANNEL_ID}</code>).\n\n<b>Error:</b>\n<code>{e}</code>\n\nMake sure the bot is an admin in the destination channel with permission to post photos.", parse_mode=ParseMode.HTML)

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
                    logging.info(f"New ad registered automatically from Ads Bot: {ad_url}")

@dp.message(F.from_user.id == ADMIN_ID, F.forward_from_chat, F.forward_from_chat.type == 'channel')
async def register_previous_ad(message: Message):
    """Admin forwards an old ad from the channel to register it."""
    ad_url = extract_ad_url(message)
    if ad_url:
        orig_msg_id = message.forward_from_message_id
        channel_id = message.forward_from_chat.id
        
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM ads WHERE message_id = %s", (orig_msg_id,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (%s, %s, %s, %s)", (channel_id, orig_msg_id, ad_url, time.time()))
                    await message.reply(f"✅ Previous ad (ID: {orig_msg_id}) successfully registered into rotation!")
                else:
                    await message.reply("⚠️ This ad is already in the database.")
    else:
        await message.reply("❌ No URL found. Is this definitely an ad?")

@dp.message(CommandStart())
async def handle_start(message: Message, command: CommandStart):
    """Handles the user clicking the deep link."""
    file_hash = command.args
    
    user_id = message.from_user.id
    
    request_key = f"{user_id}_{file_hash}"
    
    ad_url = "https://example.com"
    ad_msg_id = None
    ad_channel_id = None
    
    fwd_msg_id = None
    user_msg_id = message.message_id # The /start message from the user
    bot_reply_msg_id = None # The bot's message with the inline buttons
    max_ad_attempts = 5 # Prevent infinite loops if all ads are bad
    attempt_count = 0
    
    # Connect to DB once for the whole ad selection process
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Check if file exists first
            cursor.execute("SELECT file_id FROM files WHERE hash = %s", (file_hash,))
            if not file_hash or not cursor.fetchone(): # Use fetchone() to check existence
                return await message.answer("Welcome! Please use a valid file request link.")
                
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
                        chat_id=user_id,
                        from_chat_id=ad_channel_id,
                        message_id=ad_msg_id
                    )
                    fwd_msg_id = sent_fwd.message_id
                    # If successful, break the loop
                    break 
                except (TelegramBadRequest, TelegramNotFound) as e:
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
            """, (user_id, message.from_user.full_name))
            
    track_url = f"{WEB_APP_DOMAIN}/track?u={user_id}&h={file_hash}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Click Ad to Verify", url=track_url)],
        [InlineKeyboardButton(text="2️⃣ Get Subtitle File", callback_data=f"get_{file_hash}")]
    ])

    if ad_msg_id and ad_channel_id:
        bot_reply_msg_id = (await message.answer(
            "<b>Verification Required</b>\n\n"
            "Please click the 'Click Ad to Verify' button below.\n"
            "After verifying, click 'Get Subtitle File' to receive your file.",
            reply_markup=keyboard
        )).message_id
    else:
        bot_reply_msg_id = (await message.answer(
            f"<b>Verification Required</b>\n\n"
            f"Please click the verification button below.\n\n"
            f"<i>(No specific ad available at the moment.)</i>",
            reply_markup=keyboard
        )).message_id
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO requests (request_key, verified, timestamp, target_url, user_msg_id, bot_fwd_msg_id, bot_reply_msg_id) 
                VALUES (%s, 0, %s, %s, %s, %s, %s)
                ON CONFLICT(request_key) DO UPDATE SET verified = 0, timestamp = EXCLUDED.timestamp, target_url = EXCLUDED.target_url, user_msg_id = EXCLUDED.user_msg_id, bot_fwd_msg_id = EXCLUDED.bot_fwd_msg_id, bot_reply_msg_id = EXCLUDED.bot_reply_msg_id
            """, (request_key, time.time(), ad_url, user_msg_id, fwd_msg_id, bot_reply_msg_id))
    
    # Schedule cleanup if user doesn't interact
    asyncio.create_task(cleanup_unclicked_request(request_key, user_id))

@dp.callback_query(F.data.startswith("get_"))
async def serve_file(callback: CallbackQuery):
    """Verifies the ad click and serves the file if valid."""
    user_id = callback.from_user.id
    file_hash = callback.data.split("_")[1]
    request_key = f"{user_id}_{file_hash}"
    
    file_id = None
    user_msg_id = None
    bot_fwd_msg_id = None
    bot_reply_msg_id = None
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT verified, timestamp, user_msg_id, bot_fwd_msg_id FROM requests WHERE request_key = %s", (request_key,))
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
            bot_reply_msg_id = req.get('bot_reply_msg_id')

            cursor.execute("SELECT file_id FROM files WHERE hash = %s", (file_hash,))
            file_row = cursor.fetchone()
            
            if file_row:
                cursor.execute("UPDATE users SET successful_receives = successful_receives + 1 WHERE user_id = %s", (user_id,))
                cursor.execute("""
                    INSERT INTO user_file_requests (user_id, file_hash, count) VALUES (%s, %s, 1)
                    ON CONFLICT(user_id, file_hash) DO UPDATE SET count = user_file_requests.count + 1
                """, (user_id, file_hash))
                file_id = file_row[0]
                
            cursor.execute("DELETE FROM requests WHERE request_key = %s", (request_key,))
            
    if file_id:
        caption = "✅ Here is your requested subtitle file!\n\n⏳ <i>This file will be automatically deleted in 5 minutes.</i>"
        sent_file = await bot.send_document(user_id, file_id, caption=caption)
        
        # Clean up previous messages
        msgs_to_delete = [callback.message.message_id] # The message with the inline buttons
        # if user_msg_id: msgs_to_delete.append(user_msg_id) # User's /start message - generally not deleted
        if bot_fwd_msg_id: msgs_to_delete.append(bot_fwd_msg_id) # The forwarded ad
        if bot_reply_msg_id: msgs_to_delete.append(bot_reply_msg_id) # The bot's reply with buttons (if different from callback.message.message_id)

        # Delete messages
        for m_id in msgs_to_delete:
            try:
                await bot.delete_message(user_id, m_id)
            except Exception:
                pass
                
        # Schedule deletion of the subtitle file
        asyncio.create_task(delete_message_later(user_id, sent_file.message_id, 300))
    else:
        await callback.answer("File no longer available.", show_alert=True)
        
    await callback.answer()

@dp.message(Command("ping"))
async def ping_handler(message: Message):
    """Simple command to test if the bot is alive."""
    await message.answer("🏓 Pong! The bot is online and actively receiving messages.")

@dp.message()
async def catch_all(message: Message):
    """Catches all other messages to let you know the bot is alive but confused."""
    await message.answer(f"🤖 I am alive! But I only understand Subtitle Files.\n\nPlease make sure you are sending your subtitle as an attached <b>Document/File</b>.\nYour ID: <code>{message.from_user.id}</code>")

# --- Web Server for Tracking Clicks ---
async def track_click(request: web.Request):
    """Endpoint that verifies the user and redirects to the actual ad."""
    user_id = request.query.get("u")
    file_hash = request.query.get("h")
    
    target_url = "https://example.com"
    
    if user_id and file_hash:
        request_key = f"{user_id}_{file_hash}"
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("SELECT target_url FROM requests WHERE request_key = %s", (request_key,))
                req = cursor.fetchone()
                if req:
                    cursor.execute("UPDATE requests SET verified = 1 WHERE request_key = %s", (request_key,))
                    target_url = req['target_url']
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
