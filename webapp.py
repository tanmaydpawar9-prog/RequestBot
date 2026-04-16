import logging
import psycopg2
from aiohttp import web

from config import DATABASE_URL

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