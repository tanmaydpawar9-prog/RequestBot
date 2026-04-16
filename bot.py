import asyncio
import logging
from aiohttp import web

from config import bot, dp, PORT
from admin import admin_router
from user import user_router
from common import common_router
from webapp import track_click, health_check

# --- Startup Logic ---
async def main():
    # Register routers
    dp.include_router(admin_router)
    dp.include_router(user_router)
    dp.include_router(common_router)

    # --- Web Server Setup ---
    app = web.Application()
    logging.info("Web application initialized.")
    app.router.add_get('/track', track_click)
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, '0.0.0.0', PORT)
    logging.info(f"Attempting to start web server on port {PORT}...")
    await site.start()
    logging.info(f"Web server running on port {PORT}")
    
    # --- Bot Startup ---
    # Delete any existing webhook before starting polling
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Starting Telegram bot polling...")
    await dp.start_polling(bot)
    logging.info("Telegram bot polling started.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped gracefully.")
