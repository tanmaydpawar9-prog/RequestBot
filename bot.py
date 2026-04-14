import os
import time
import uuid
import asyncio
import logging
import random
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration & Environment Variables ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
WEB_APP_DOMAIN = os.getenv("WEB_APP_DOMAIN", "http://localhost:8080").rstrip('/')
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- In-Memory Database ---
files_db = {}       # { hash: file_id }
requests_db = {}    # { "user_id_file_hash": {"verified": bool, "timestamp": float, "target_url": str} }
ads_db = []         # [{"message_id": int, "url": str}]

# --- Telegram Bot Handlers ---
@dp.message(F.document & (F.from_user.id == ADMIN_ID))
async def handle_admin_upload(message: Message):
    """Admin uploads a subtitle file to generate a link."""
    file_id = message.document.file_id
    file_hash = uuid.uuid4().hex[:8]
    
    files_db[file_hash] = file_id
    
    bot_info = await bot.me()
    deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
    
    await message.answer(
        f"✅ <b>File uploaded successfully!</b>\n\n"
        f"Here is the request link to share:\n<code>{deep_link}</code>"
    )

@dp.channel_post(F.chat.id == CHANNEL_ID)
async def track_channel_ads(message: Message):
    """Monitors the ad channel to update the latest ad."""
    ad_url = None
    if message.entities:
        for entity in message.entities:
            if entity.type == 'text_link':
                ad_url = entity.url
                break
            elif entity.type == 'url' and message.text:
                ad_url = message.text[entity.offset:entity.offset+entity.length]
                break
                
    if ad_url:
        current_ad["text"] = message.text or message.caption or "Please click the ad below to continue."
        current_ad["url"] = ad_url
        logging.info(f"New ad registered: {ad_url}")

@dp.message(CommandStart())
async def handle_start(message: Message, command: CommandStart):
    """Handles the user clicking the deep link."""
    file_hash = command.args
    
    if not file_hash or file_hash not in files_db:
        return await message.answer("Welcome! Please use a valid file request link.")
        
    user_id = message.from_user.id
    request_key = f"{user_id}_{file_hash}"
    
    ad_url = "https://example.com"
    ad_msg_id = None
    
    if ads_db:
        selected_ad = random.choice(ads_db)
        ad_url = selected_ad["url"]
        ad_msg_id = selected_ad["message_id"]

    # Create a request valid for 5 minutes
    requests_db[request_key] = {
        "verified": False,
        "timestamp": time.time(),
        "target_url": ad_url
    }
    
    track_url = f"{WEB_APP_DOMAIN}/track?u={user_id}&h={file_hash}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Click Ad to Verify", url=track_url)],
        [InlineKeyboardButton(text="2️⃣ Get Subtitle File", callback_data=f"get_{file_hash}")]
    ])
    
    if ad_msg_id:
        await bot.copy_message(
            chat_id=user_id,
            from_chat_id=CHANNEL_ID,
            message_id=ad_msg_id,
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
    req = requests_db.get(request_key)
    
    if not req:
        return await callback.answer("Invalid or expired session. Please click the original link again.", show_alert=True)
        
    if time.time() - req["timestamp"] > 300:
        del requests_db[request_key]
        return await callback.answer("Request expired (5 minutes limit). Please generate a new one.", show_alert=True)
        
    if not req["verified"]:
        return await callback.answer("❌ You must click the 'Click Ad to Verify' button first!", show_alert=True)
        
    file_id = files_db.get(file_hash)
    if file_id:
        await bot.send_document(user_id, file_id, caption="✅ Here is your requested subtitle file!")
    else:
        await callback.answer("File no longer available.", show_alert=True)
        
    await callback.answer()
    del requests_db[request_key]

# --- Web Server for Tracking Clicks ---
async def track_click(request: web.Request):
    """Endpoint that verifies the user and redirects to the actual ad."""
    user_id = request.query.get("u")
    file_hash = request.query.get("h")
    
    target_url = "https://example.com"
    
    if user_id and file_hash:
        request_key = f"{user_id}_{file_hash}"
        if request_key in requests_db:
            requests_db[request_key]["verified"] = True
            target_url = requests_db[request_key].get("target_url", target_url)
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
