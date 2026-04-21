import os
import re
import time
import uuid
import logging
import psycopg2
import psycopg2.extras
import asyncio
import uuid

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile, ChatMemberOwner, ChatMemberAdministrator

from config import bot, ADMIN_ID, DESTINATION_CHANNEL_ID, DATABASE_URL, admin_temp_state
from utils import extract_channel_short_name_from_filename, extract_ad_url, delete_message_later

admin_router = Router()

@admin_router.message(Command("setchannel"), F.from_user.id == ADMIN_ID)
async def set_channel_command(message: Message):
    """Admin command to register a channel with a short name."""
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        msg = await message.answer("Usage: `/setchannel <short_name> <channel_id_or_username>`\n\nExample: `/setchannel RI @RenegadeImmoral` or `/setchannel SS -1001234567890`", parse_mode=ParseMode.MARKDOWN)
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    short_name = args[1].upper()
    channel_identifier = args[2]
    
    try:
        # Try to get chat info to resolve ID and full name
        chat = await bot.get_chat(channel_identifier)
        channel_id = chat.id
        full_name = chat.title

        if chat.type != 'channel':
            msg = await message.answer("❌ The provided ID/username does not belong to a channel.")
            asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
            return

        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO channels (short_name, channel_id, full_name) VALUES (%s, %s, %s) ON CONFLICT (short_name) DO UPDATE SET channel_id = EXCLUDED.channel_id, full_name = EXCLUDED.full_name",
                               (short_name, channel_id, full_name))
                conn.commit()
        msg = await message.answer(f"✅ Channel '{full_name}' registered as '{short_name}' (ID: <code>{channel_id}</code>).")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except TelegramNotFound:
        msg = await message.answer("❌ Channel not found. Make sure the bot is an admin in the channel and the ID/username is correct.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except Exception as e:
        msg = await message.answer(f"❌ An error occurred: {e}")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("addback"), F.from_user.id == ADMIN_ID)
async def add_backup_channel_command(message: Message):
    """Admin command to register a backup channel."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        msg = await message.answer("Usage: `/addback <channel_id_or_username>`\n\nExample: `/addback @MyBackupChannel` or `/addback -1001234567890`", parse_mode=ParseMode.MARKDOWN)
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    channel_identifier = args[1]
    
    try:
        chat = await bot.get_chat(channel_identifier)
        channel_id = chat.id
        full_name = chat.title

        if chat.type != 'channel':
            msg = await message.answer("❌ The provided ID/username does not belong to a channel.")
            asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
            return
        
        bot_member = await bot.get_chat_member(channel_id, bot.id)
        if not isinstance(bot_member, (ChatMemberOwner, ChatMemberAdministrator)) or (isinstance(bot_member, ChatMemberAdministrator) and not bot_member.can_invite_users):
             await message.answer(f"⚠️ Bot is not an admin in '{full_name}' or lacks the 'Invite Users via Link' permission, which is required to approve join requests.")

        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO backup_channels (channel_id, full_name) VALUES (%s, %s) ON CONFLICT (channel_id) DO UPDATE SET full_name = EXCLUDED.full_name",
                               (channel_id, full_name))
                conn.commit()
        msg = await message.answer(f"✅ Backup channel '{full_name}' registered (ID: <code>{channel_id}</code>).")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except TelegramNotFound:
        msg = await message.answer("❌ Channel not found. Make sure the bot is in the channel and the ID/username is correct.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except Exception as e:
        msg = await message.answer(f"❌ An error occurred: {e}")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("backup"), F.from_user.id == ADMIN_ID)
async def backup_command(message: Message):
    """Admin command to activate or deactivate force-join for a backup channel."""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        msg = await message.answer("Usage: `/backup <channel_id>` or `/backup off`\n\nUse `/backup off` to disable backup channel force-join.", parse_mode=ParseMode.MARKDOWN)
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    target = args[1].lower()

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("UPDATE backup_channels SET is_active = FALSE")
            
            if target == 'off':
                conn.commit()
                msg = await message.answer("✅ Backup channel force-join has been deactivated.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return

            try:
                target_id = int(target)
                cursor.execute("SELECT full_name FROM backup_channels WHERE channel_id = %s", (target_id,))
                channel = cursor.fetchone()
                if not channel:
                    msg = await message.answer(f"❌ Channel ID <code>{target_id}</code> is not registered as a backup channel. Use `/addback` first.")
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    return
                
                cursor.execute("UPDATE backup_channels SET is_active = TRUE WHERE channel_id = %s", (target_id,))
                conn.commit()
                msg = await message.answer(f"✅ Force-join activated for backup channel '{channel['full_name']}' (<code>{target_id}</code>).")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

            except ValueError:
                msg = await message.answer("❌ Invalid Channel ID. Please provide a numeric ID or 'off'.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
            except Exception as e:
                conn.rollback()
                msg = await message.answer(f"❌ An error occurred: {e}")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.callback_query(F.data.startswith("post_to_channel_"), F.from_user.id == ADMIN_ID)
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
                msg = await callback.message.answer(f"❌ Channel '{short_name}' not found in database. Please register it first.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return

            target_channel_id = channel_data['channel_id']
            channel_full_name = channel_data['full_name']
            
            bot_info = await bot.me()
            
            # Extract episode information from the original filename
            episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', original_filename, re.IGNORECASE)
            episode_info = episode_match.group(0).upper() if episode_match else "" # e.g., "EP136"

            # Construct the desired caption for the channel post
            if episode_info:
                caption_text = f"<b>{channel_full_name} {episode_info}</b>"
            else:
                caption_text = f"<b>{channel_full_name}</b>"

            # Create a clean link
            deep_link = f"https://t.me/{bot_info.username}?start={file_hash}" # The filename is already in the DB
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]
            ])
            
            try:
                await bot.send_message(
                    chat_id=target_channel_id, # Post to the target channel
                    text=caption_text, # Use the newly constructed text
                    reply_markup=keyboard,
                )
                msg = await callback.message.answer(f"✅ Subtitle for '<b>{original_filename}</b>' posted to channel '<b>{channel_full_name}</b>'!")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
            except TelegramBadRequest as e:
                msg = await callback.message.answer(f"❌ Failed to post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nMake sure the bot is an admin in the channel and has permission to post messages.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
            except Exception as e:
                msg = await callback.message.answer(f"❌ An unexpected error occurred while posting: {e}")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("stats"), F.from_user.id == ADMIN_ID)
async def view_stats(message: Message):
    """Admin command to view user request statistics in a clean table format."""
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT SUM(total_requests), SUM(successful_receives) FROM users")
            totals = cursor.fetchone()
            tot_req = totals[0] or 0
            tot_succ = totals[1] or 0
 
            if tot_req == 0:
                msg = await message.answer("📊 No file requests have been made yet.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return
 
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
 
    if lines and lines[-1] == "":
        lines.pop()
 
    text = "\n".join(lines)
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    msg = await message.answer(f"<code>{text}</code>")
    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("getdata"), F.from_user.id == ADMIN_ID)
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
        msg = await message.answer(
            "❌ <b>reportlab</b> is not installed.\n"
            "Add <code>reportlab</code> to your <code>requirements.txt</code> and redeploy."
        )
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    status_msg = await message.answer("⏳ Generating PDF report…")

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT SUM(total_requests), SUM(successful_receives) FROM users")
            totals = cursor.fetchone()
            tot_req = totals[0] or 0
            tot_succ = totals[1] or 0

            if tot_req == 0:
                await status_msg.delete()
                msg = await message.answer("📊 No data yet to export.")
                asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                return

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

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        topMargin=1.5*cm, bottomMargin=1.5*cm,
        leftMargin=2*cm, rightMargin=2*cm,
        title="TheFrictionRealm Stats"
    )

    C_DARK    = colors.HexColor('#1a1a2e')
    C_ACCENT  = colors.HexColor('#e94560')
    C_ROW_A   = colors.HexColor('#f4f4f8')
    C_ROW_B   = colors.white
    C_BORDER  = colors.HexColor('#d0d0d8')
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

    elements.append(Paragraph("TheFrictionRealm — Stats Report", title_style))
    elements.append(Paragraph(f"Generated: {ts_display}", sub_style))
    elements.append(HRFlowable(width="100%", thickness=2, color=C_ACCENT, spaceAfter=16))

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
        ('ALIGN',        (0, 0), (-1, -1), 'CENTER'),
        ('GRID',         (0, 0), (-1, -1), 0.5, C_BORDER),
    ]
    for i in range(1, len(ov_data)):
        ov_style.append(('BACKGROUND', (0, i), (-1, i), C_ROW_A if i % 2 else C_ROW_B))

    ov_table = Table(ov_data, colWidths=[10*cm, 6*cm])
    ov_table.setStyle(TableStyle(ov_style))
    elements.append(ov_table)
    elements.append(Spacer(1, 0.6*cm))

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

    col_w = [1.2*cm, 4.5*cm, 3.8*cm, 2*cm, 2.4*cm, 2*cm, 7*cm]
    ub_style = [
        ('BACKGROUND',   (0, 0), (-1, 0), C_ACCENT),
        ('TEXTCOLOR',    (0, 0), (-1, 0), colors.white),
        ('FONTNAME',     (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('ALIGN',        (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN',        (1, 1), (1, -1), 'LEFT'),
        ('ALIGN',        (6, 1), (6, -1), 'LEFT'),
        ('VALIGN',       (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID',         (0, 0), (-1, -1), 0.5, C_BORDER),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [C_ROW_A, C_ROW_B]),
    ]
    ub_table = Table(ub_data, colWidths=col_w, repeatRows=1)
    ub_table.setStyle(TableStyle(ub_style))
    elements.append(ub_table)

    elements.append(Spacer(1, 0.5*cm))
    elements.append(HRFlowable(width="100%", thickness=1, color=C_BORDER))
    footer_style = ParagraphStyle('F', fontName='Helvetica', fontSize=8,
                                  textColor=C_SUBTLE, alignment=TA_CENTER, spaceBefore=6)
    elements.append(Paragraph("TheFrictionRealm Bot  ·  Admin Export", footer_style))

    doc.build(elements)
    buffer.seek(0)

    ts_file = time.strftime('%Y%m%d_%H%M', time.gmtime())
    await status_msg.delete()
    msg = await bot.send_document(
        message.chat.id,
        BufferedInputFile(buffer.read(), filename=f"frictionrealm_stats_{ts_file}.pdf"),
        caption=f"📊 <b>TheFrictionRealm Stats</b>\n<i>{ts_display}</i>",
    )
    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(F.document, F.from_user.id == ADMIN_ID)
async def handle_admin_upload(message: Message):
    """Admin uploads a subtitle file to generate a link."""
    file_id = message.document.file_id
    file_hash = uuid.uuid4().hex[:8]
    
    original_filename = message.document.file_name

    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO files (hash, file_id, filename) VALUES (%s, %s, %s)", (file_hash, file_id, original_filename))
    
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
                    episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', original_filename, re.IGNORECASE)
                    episode_info = episode_match.group(0).upper() if episode_match else ""
                    caption_text = f"<b>{channel_full_name} {episode_info}</b>" if episode_info else f"<b>{channel_full_name}</b>"
                    file_extension = os.path.splitext(original_filename)[1] or ".ass"

                    deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]])
                    
                    try:
                        await bot.send_message(chat_id=target_channel_id, text=caption_text, reply_markup=keyboard)
                        msg = await message.answer(f"✅ Subtitle for '<b>{original_filename}</b>' automatically posted to channel '<b>{channel_full_name}</b>'!")
                        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                        return
                    except TelegramBadRequest as e:
                        msg = await message.answer(f"❌ Failed to auto-post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nFalling back to manual selection.")
                        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                    except Exception as e:
                        msg = await message.answer(f"❌ An unexpected error occurred during auto-post: {e}\n\nFalling back to manual selection.")
                        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

    admin_temp_state[message.from_user.id] = {'file_hash': file_hash, 'file_name': original_filename}

    channels_keyboard_buttons = []
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT short_name, full_name FROM channels ORDER BY short_name")
            for row in cursor.fetchall():
                channels_keyboard_buttons.append([InlineKeyboardButton(text=f"{row['short_name']} ({row['full_name']})", callback_data=f"post_to_channel_{row['short_name']}")])
    
    if not channels_keyboard_buttons:
        msg = await message.answer(f"✅ File '<b>{original_filename}</b>' uploaded.\n\n⚠️ No channels registered. Use `/setchannel` to add one.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    msg = await message.answer(
        f"✅ File '<b>{original_filename}</b>' uploaded!\n\nSelect the channel to post to:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=channels_keyboard_buttons),
    )
    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(F.document)
async def handle_unauthorized_upload(message: Message):
    """Catches document uploads from non-admins."""
    msg = await message.answer("🚫 <b>Access Denied</b>\n\nYou are not authorized to upload files.")
    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("check_dest"), F.from_user.id == ADMIN_ID)
async def check_destination_channel(message: Message):
    """Admin command to verify the bot can access the destination channel."""
    if not DESTINATION_CHANNEL_ID:
        msg = await message.answer("⚠️ `DESTINATION_CHANNEL_ID` is not set.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    try:
        chat = await bot.get_chat(DESTINATION_CHANNEL_ID)
        my_member_info = await bot.get_chat_member(DESTINATION_CHANNEL_ID, bot.id)
        
        can_post = False
        status = my_member_info.status.capitalize()
        if isinstance(my_member_info, (ChatMemberOwner, ChatMemberAdministrator)):
            can_post = my_member_info.can_post_messages if isinstance(my_member_info, ChatMemberAdministrator) else True

        perm_text = "✅ Can post messages" if can_post else "❌ Cannot post messages"
        
        msg = await message.answer(
            f"✅ <b>Destination Channel Check: OK</b>\n\n"
            f"<b>Name:</b> {chat.title}\n<b>ID:</b> <code>{chat.id}</code>\n"
            f"<b>Bot Status:</b> {status}\n<b>Permissions:</b> {perm_text}"
        )
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except TelegramNotFound:
        msg = await message.answer(
            f"🚨 <b>Destination Channel Check: FAILED</b>\n\n"
            f"Could not find channel ID: <code>{DESTINATION_CHANNEL_ID}</code>.\n\n"
            f"<b>Fix:</b>\n1. Ensure `DESTINATION_CHANNEL_ID` is correct.\n"
            f"2. Add the bot to the channel as an admin."
        )
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except Exception as e:
        msg = await message.answer(f"An unexpected error occurred: {e}")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("editpost"), F.from_user.id == ADMIN_ID)
async def edit_post_command(message: Message):
    """Admin command to edit the button of a post via message link."""
    args = message.text.split(maxsplit=2)
    if len(args) < 3 or '|' not in args[2]:
        msg = await message.reply(
            "❌ <b>Usage:</b>\n"
            "<code>/editpost &lt;message_link&gt; New Button Text | https://new.link</code>\n\n"
            "You can get the message link by right-clicking the post in the channel and selecting 'Copy Message Link'."
        )
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    message_link = args[1]
    button_text, url = [arg.strip() for arg in args[2].split('|', 1)]

    public_link_match = re.match(r'https://t\.me/([^/]+)/(\d+)', message_link)
    private_link_match = re.match(r'https://t\.me/c/(\d+)/(\d+)', message_link)

    if private_link_match:
        chat_id = int(f"-100{private_link_match.group(1)}")
        message_id = int(private_link_match.group(2))
    elif public_link_match:
        chat_id = f"@{public_link_match.group(1)}"
        message_id = int(public_link_match.group(2))
    else:
        msg = await message.reply("❌ Invalid message link format. Please copy the link directly from Telegram.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    if not button_text or not url or not url.startswith("https://"):
        msg = await message.reply("❌ Invalid button format. Both text and a valid https:// URL are required.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    new_keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=button_text, url=url)]])

    try:
        await bot.get_chat(chat_id)
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=new_keyboard
        )
        msg = await message.reply("✅ Post button updated successfully!")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except TelegramNotFound:
        msg = await message.reply(f"🚨 <b>Update Failed!</b>\n\nCould not find the channel or message. Ensure the link is correct and the bot is an admin in that channel.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except TelegramBadRequest as e:
        logging.error(f"Failed to edit post button. Error: {e}")
        msg = await message.reply(f"🚨 <b>Update Failed!</b>\n\nError: <code>{e}</code>\n\nThis usually means the message wasn't sent by the bot, or the bot lacks permission to edit messages.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except Exception as e:
        logging.error(f"Failed to edit post button. Error: {e}")
        msg = await message.reply(f"🚨 <b>An unexpected error occurred!</b>\n\nError: <code>{e}</code>")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("post"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def post_forwarded_message(message: Message):
    """Admin command to re-post a forwarded message to the destination channel."""
    forwarded_message = message.reply_to_message

    if not forwarded_message.forward_from_chat:
        msg = await message.reply("❌ This command only works when replying to a forwarded message from a channel.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    if not forwarded_message.photo or not forwarded_message.caption:
        msg = await message.reply("❌ The forwarded message must contain a photo and a caption.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    if not DESTINATION_CHANNEL_ID:
        msg = await message.reply("⚠️ `DESTINATION_CHANNEL_ID` is not set.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', forwarded_message.caption, re.IGNORECASE)
    if not episode_match:
        msg = await message.reply(f"❌ No episode pattern (e.g., EP123) found in the caption.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return
    
    episode_info = episode_match.group(0).upper()
    
    # Always store the photo and caption in the database for robust links.
    # The force-join check will happen in user.py when the link is clicked.
    content_hash = uuid.uuid4().hex[:8]
    bot_info = await bot.me()
    button_link = f"https://t.me/{bot_info.username}?start=post_content_{content_hash}" # New robust link
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"⬇️ {episode_info} | Download Here", url=button_link)]])

    try:
        # Use copy_message to send an exact copy of the forwarded message,
        # but with our new download button attached. This preserves the original message perfectly.
        sent_message = await bot.copy_message(
            chat_id=DESTINATION_CHANNEL_ID,
            from_chat_id=forwarded_message.chat.id,
            message_id=forwarded_message.message_id,
            reply_markup=keyboard,
        )
        # Store information about the new post for robust linking
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO posted_content (hash, file_id, caption, timestamp, channel_id, message_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (content_hash, forwarded_message.photo[-1].file_id, forwarded_message.caption, time.time(), DESTINATION_CHANNEL_ID, sent_message.message_id))
                conn.commit()
        msg = await message.reply(f"✅ Successfully posted to channel <code>{DESTINATION_CHANNEL_ID}</code>.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except TelegramBadRequest as e:
        logging.error(f"Failed to post forwarded message. Error: {e}")
        reply_text = f"🚨 <b>Posting Failed!</b>\n\nError: <code>{e}</code>\n\n"
        if "chat not found" in str(e).lower():
            reply_text += "<b>Suggestion:</b> Ensure `DESTINATION_CHANNEL_ID` is correct and the bot is an admin in the channel."
        msg = await message.reply(reply_text)
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    except Exception as e:
        logging.error(f"Failed to post forwarded message. Error: {e}")
        msg = await message.reply(f"🚨 <b>An unexpected error occurred!</b>\n\nError: <code>{e}</code>")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("addad"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def register_previous_ad_command(message: Message):
    """Admin replies to a forwarded ad with /addad to register it."""
    forwarded_message = message.reply_to_message

    if not forwarded_message.forward_from_chat or forwarded_message.forward_from_chat.type != 'channel':
        msg = await message.reply("❌ This command only works when replying to a message forwarded from a channel.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
        return

    ad_url = extract_ad_url(forwarded_message)
    if ad_url:
        orig_msg_id = forwarded_message.forward_from_message_id
        channel_id = forwarded_message.forward_from_chat.id
        
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM ads WHERE message_id = %s AND channel_id = %s", (orig_msg_id, channel_id))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (%s, %s, %s, %s)", (channel_id, orig_msg_id, ad_url, time.time()))
                    conn.commit()
                    msg = await message.reply(f"✅ Ad (ID: {orig_msg_id}) successfully registered!")
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
                else:
                    msg = await message.reply("⚠️ This ad is already in the database.")
                    asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))
    else:
        msg = await message.reply("❌ No URL found in the replied message.")
        asyncio.create_task(delete_message_later(msg.chat.id, msg.message_id, 300))

@admin_router.message(Command("accept"), F.from_user.id == ADMIN_ID)
async def accept_join_requests(message: Message):
    """Admin command to approve all pending join requests for backup channels."""
    status_msg = await message.answer("⏳ Processing pending join requests...")
    
    approved_count = 0
    failed_count = 0
    
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT chat_id, user_id, original_start_args FROM pending_join_requests")
            requests = cursor.fetchall()
            
            if not requests:
                await status_msg.edit_text("✅ No pending join requests to approve.")
                asyncio.create_task(delete_message_later(status_msg.chat.id, status_msg.message_id, 300))
                return

            bot_info = await bot.me()
            for req in requests:
                try:
                    await bot.approve_chat_join_request(chat_id=req['chat_id'], user_id=req['user_id'])
                    approved_count += 1
                    # Notify the user with a direct link to continue
                    if req['original_start_args']:
                        try:
                            continue_link = f"https://t.me/{bot_info.username}?start={req['original_start_args']}"
                            keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ You've been approved! Click here to continue.", url=continue_link)]])
                            await bot.send_message(req['user_id'], "<b>You're in!</b>\n\nYour request to join the backup channel was approved.", reply_markup=keyboard)
                        except Exception as e:
                            logging.warning(f"Could not send approval notification to user {req['user_id']}: {e}")
                except Exception as e:
                    logging.error(f"Failed to approve join request for user {req['user_id']} in chat {req['chat_id']}: {e}")
                    failed_count += 1
                
                cursor.execute("DELETE FROM pending_join_requests WHERE chat_id = %s AND user_id = %s", (req['chat_id'], req['user_id']))
            
            conn.commit()

    await status_msg.edit_text(f"✅ <b>Join Request Processing Complete</b>\n\n- Approved: {approved_count}\n- Failed: {failed_count}")
    asyncio.create_task(delete_message_later(status_msg.chat.id, status_msg.message_id, 300))