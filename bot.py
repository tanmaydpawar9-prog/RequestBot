import os
import time
import uuid
import asyncio
import logging
import random
import sqlite3
from dotenv import load_dotenv
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from a .env file if present
load_dotenv()

# --- Configuration & Environment Variables ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
WEB_APP_DOMAIN = os.getenv("WEB_APP_DOMAIN", "http://localhost:8080").rstrip('/')
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- SQLite Database Setup ---
conn = sqlite3.connect('database.db', check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, file_id TEXT)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS ads (channel_id INTEGER, message_id INTEGER, url TEXT, timestamp REAL)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS requests (request_key TEXT PRIMARY KEY, verified INTEGER, timestamp REAL, target_url TEXT)''')

# Statistics Tables
cursor.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, name TEXT, total_requests INTEGER DEFAULT 0, successful_receives INTEGER DEFAULT 0)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS user_file_requests (user_id INTEGER, file_hash TEXT, count INTEGER DEFAULT 0, PRIMARY KEY(user_id, file_hash))''')

conn.commit()

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

@dp.message(Command("stats") & (F.from_user.id == ADMIN_ID))
async def view_stats(message: Message):
    """Admin command to view user request statistics."""
    cursor.execute("SELECT SUM(total_requests), SUM(successful_receives) FROM users")
    totals = cursor.fetchone()
    tot_req = totals[0] or 0
    tot_succ = totals[1] or 0
    
    if tot_req == 0:
        return await message.answer("📊 No file requests have been made yet.")
        
    lines = [
        "📊 <b>Global Statistics:</b>",
        f"Total Links Clicked: {tot_req}",
        f"Total Files Received: {tot_succ}",
        "\n👥 <b>User Breakdown:</b>\n"
    ]
    
    cursor.execute("SELECT user_id, name, total_requests, successful_receives FROM users ORDER BY successful_receives DESC")
    for row in cursor.fetchall():
        uid = row['user_id']
        lines.append(f"👤 <b>{row['name']}</b> (<code>{uid}</code>)\nClicks: {row['total_requests']} | Received: {row['successful_receives']}")
        
        cursor.execute("SELECT file_hash, count FROM user_file_requests WHERE user_id = ?", (uid,))
        for f_row in cursor.fetchall():
            lines.append(f"  └ File <code>{f_row['file_hash']}</code>: {f_row['count']} times")
        lines.append("")
        
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n... (truncated)"
    await message.answer(text)

@dp.message(F.document & (F.from_user.id == ADMIN_ID))
async def handle_admin_upload(message: Message):
    """Admin uploads a subtitle file to generate a link."""
    file_id = message.document.file_id
    file_hash = uuid.uuid4().hex[:8]
    
    cursor.execute("INSERT INTO files (hash, file_id) VALUES (?, ?)", (file_hash, file_id))
    conn.commit()
    
    bot_info = await bot.me()
    deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
    
    await message.answer(
        f"✅ <b>File uploaded successfully!</b>\n\n"
        f"Here is the request link to share:\n<code>{deep_link}</code>"
    )

@dp.channel_post()
async def track_channel_ads(message: Message):
    """Monitors any channel the bot is in to automatically log ads."""
    ad_url = extract_ad_url(message)
    if ad_url:
        cursor.execute("SELECT 1 FROM ads WHERE message_id = ?", (message.message_id,))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (?, ?, ?, ?)", 
                           (message.chat.id, message.message_id, ad_url, time.time()))
            conn.commit()
            logging.info(f"New ad registered automatically: {ad_url}")

@dp.message((F.from_user.id == ADMIN_ID) & F.forward_from_chat & (F.forward_from_chat.type == 'channel'))
async def register_previous_ad(message: Message):
    """Admin forwards an old ad from the channel to register it."""
    ad_url = extract_ad_url(message)
    if ad_url:
        orig_msg_id = message.forward_from_message_id
        channel_id = message.forward_from_chat.id
        
        cursor.execute("SELECT 1 FROM ads WHERE message_id = ?", (orig_msg_id,))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (?, ?, ?, ?)", (channel_id, orig_msg_id, ad_url, time.time()))
            conn.commit()
            await message.reply(f"✅ Previous ad (ID: {orig_msg_id}) successfully registered into rotation!")
        else:
            await message.reply("⚠️ This ad is already in the database.")
    else:
        await message.reply("❌ No URL found. Is this definitely an ad?")

@dp.message(CommandStart())
async def handle_start(message: Message, command: CommandStart):
    """Handles the user clicking the deep link."""
    file_hash = command.args
    
    cursor.execute("SELECT file_id FROM files WHERE hash = ?", (file_hash,))
    if not file_hash or not cursor.fetchone():
        return await message.answer("Welcome! Please use a valid file request link.")
        
    user_id = message.from_user.id
    
    # Track Total Request Clicks for this user
    cursor.execute("""
        INSERT INTO users (user_id, name, total_requests, successful_receives) 
        VALUES (?, ?, 1, 0) 
        ON CONFLICT(user_id) DO UPDATE SET total_requests = total_requests + 1, name = excluded.name
    """, (user_id, message.from_user.full_name))
    conn.commit()
    
    request_key = f"{user_id}_{file_hash}"
    
    ad_url = "https://example.com"
    ad_msg_id = None
    ad_channel_id = None
    
    # Clean up ads older than 24 hours (86400 seconds) from DB
    cursor.execute("DELETE FROM ads WHERE ? - timestamp > 86400", (time.time(),))
    conn.commit()
    
    cursor.execute("SELECT channel_id, message_id, url FROM ads")
    ads_db = cursor.fetchall()
    if ads_db:
        selected_ad = random.choice(ads_db)
        ad_url, ad_msg_id, ad_channel_id = selected_ad['url'], selected_ad['message_id'], selected_ad['channel_id']

    # Save verification session to DB
    cursor.execute("INSERT OR REPLACE INTO requests (request_key, verified, timestamp, target_url) VALUES (?, 0, ?, ?)", 
                   (request_key, time.time(), ad_url))
    conn.commit()
    
    track_url = f"{WEB_APP_DOMAIN}/track?u={user_id}&h={file_hash}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Click Ad to Verify", url=track_url)],
        [InlineKeyboardButton(text="2️⃣ Get Subtitle File", callback_data=f"get_{file_hash}")]
    ])
    
    if ad_msg_id and ad_channel_id:
        await bot.forward_message(
            chat_id=user_id,
            from_chat_id=ad_channel_id,
            message_id=ad_msg_id
        )
        await message.answer(
            "👆 <b>Please click the ad above to verify!</b>\n\n"
            "After verifying, click the button below to get your file.",
            reply_markup=keyboard
        )
    else:
        await message.answer(
            f"<b>Verification Required</b>\n\n"
            f"To get your file, please support us by clicking the ad below.\n\n"
            f"<i>Ad:</i>\nPlease support us by visiting our sponsor!", 
            reply_markup=keyboard
        )

@dp.callback_query(F.data.startswith("get_"))
async def serve_file(callback: CallbackQuery):
    """Verifies the ad click and serves the file if valid."""
    user_id = callback.from_user.id
    file_hash = callback.data.split("_")[1]
    request_key = f"{user_id}_{file_hash}"
    
    cursor.execute("SELECT verified, timestamp FROM requests WHERE request_key = ?", (request_key,))
    req = cursor.fetchone()
    
    if not req:
        return await callback.answer("Invalid or expired session. Please click the original link again.", show_alert=True)
        
    if time.time() - req['timestamp'] > 300:
        cursor.execute("DELETE FROM requests WHERE request_key = ?", (request_key,))
        conn.commit()
        return await callback.answer("Request expired (5 minutes limit). Please generate a new one.", show_alert=True)
        
    if not req['verified']:
        return await callback.answer("❌ You must click the 'Click Ad to Verify' button first!", show_alert=True)
        
    cursor.execute("SELECT file_id FROM files WHERE hash = ?", (file_hash,))
    file_row = cursor.fetchone()
    
    if file_row:
        # Update Success Statistics
        cursor.execute("UPDATE users SET successful_receives = successful_receives + 1 WHERE user_id = ?", (user_id,))
        cursor.execute("""
            INSERT INTO user_file_requests (user_id, file_hash, count) VALUES (?, ?, 1)
            ON CONFLICT(user_id, file_hash) DO UPDATE SET count = count + 1
        """, (user_id, file_hash))
        conn.commit()

        # Send File
        await bot.send_document(user_id, file_row[0], caption="✅ Here is your requested subtitle file!")
    else:
        await callback.answer("File no longer available.", show_alert=True)
        
    await callback.answer()
    cursor.execute("DELETE FROM requests WHERE request_key = ?", (request_key,))
    conn.commit()

# --- Web Server for Tracking Clicks ---
async def track_click(request: web.Request):
    """Endpoint that verifies the user and redirects to the actual ad."""
    user_id = request.query.get("u")
    file_hash = request.query.get("h")
    
    target_url = "https://example.com"
    
    if user_id and file_hash:
        request_key = f"{user_id}_{file_hash}"
        cursor.execute("SELECT target_url FROM requests WHERE request_key = ?", (request_key,))
        req = cursor.fetchone()
        if req:
            cursor.execute("UPDATE requests SET verified = 1 WHERE request_key = ?", (request_key,))
            conn.commit()
            target_url = req['target_url']
            logging.info(f"User {user_id} verified for file {file_hash}")
            
    raise web.HTTPFound(target_url)

# --- Startup Logic ---
async def main():
    app = web.Application()
    app.router.add_get('/track', track_click)
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logging.info(f"Web server running on port {PORT}")
    
    logging.info("Starting Telegram bot...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped gracefully.")
