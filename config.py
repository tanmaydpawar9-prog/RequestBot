import os
import logging
from dotenv import load_dotenv
import psycopg2
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

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
    try:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute('''CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, file_id TEXT, filename TEXT)''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS ads (channel_id BIGINT, message_id BIGINT, url TEXT, timestamp REAL)''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS requests (request_key TEXT PRIMARY KEY, verified INTEGER, timestamp REAL, target_url TEXT)''')
                
                cursor.execute('''CREATE TABLE IF NOT EXISTS channels (short_name TEXT PRIMARY KEY, channel_id BIGINT UNIQUE, full_name TEXT)''') # Ensure this is created
                # New tables for backup channel feature
                cursor.execute('''CREATE TABLE IF NOT EXISTS backup_channels (channel_id BIGINT PRIMARY KEY, full_name TEXT, is_active BOOLEAN DEFAULT FALSE)''')
                cursor.execute('''CREATE TABLE IF NOT EXISTS pending_join_requests (chat_id BIGINT, user_id BIGINT, timestamp REAL, PRIMARY KEY(chat_id, user_id))''')

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

                # Add last_verified_timestamp column to users table if it doesn't exist
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name='users' AND column_name='last_verified_timestamp'")
                if not cursor.fetchone():
                    cursor.execute("ALTER TABLE users ADD COLUMN last_verified_timestamp REAL")

                # Commit any schema changes
                conn.commit()
        logging.info("Database schema checked and initialized.")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")
else:
    logging.error("DATABASE_URL is not set! Data will not be saved.")

# --- Temporary Admin State (for multi-step commands) ---
# Stores {admin_id: {'file_hash': '...', 'file_name': '...'}}
admin_temp_state = {}

# Cache for eligible force join channels
ELIGIBLE_CHANNELS_CACHE = {"channels": [], "timestamp": 0}
CACHE_DURATION = 300 # Cache for 5 minutes