import os
import re
import time
import uuid
import logging
import psycopg2
import psycopg2.extras

from aiogram import Router, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramNotFound
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile, ChatMemberOwner, ChatMemberAdministrator

from config import bot, ADMIN_ID, DESTINATION_CHANNEL_ID, DATABASE_URL, admin_temp_state
from utils import extract_channel_short_name_from_filename, extract_ad_url

admin_router = Router()

@admin_router.message(Command("setchannel"), F.from_user.id == ADMIN_ID)
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
        await message.answer(f"✅ Channel '{full_name}' registered as '{short_name}' (ID: <code>{channel_id}</code>).")
    except TelegramNotFound:
        await message.answer("❌ Channel not found. Make sure the bot is an admin in the channel and the ID/username is correct.")
    except Exception as e:
        await message.answer(f"❌ An error occurred: {e}")

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
                return await callback.message.answer(f"❌ Channel '{short_name}' not found in database. Please register it first.")

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

            # Construct the desired filename for the download
            file_extension = os.path.splitext(original_filename)[1] or ".ass" # Default to .ass
            download_filename = f"[ENG] {short_name} {episode_info} @{bot_info.username}{file_extension}".strip().replace("  ", " ")

            # Update the filename in the DB and create a clean link
            cursor.execute("UPDATE files SET filename = %s WHERE hash = %s", (download_filename, file_hash))
            conn.commit()
            deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]
            ])
            
            try:
                await bot.send_message(
                    chat_id=target_channel_id, # Post to the target channel
                    text=caption_text, # Use the newly constructed text
                    reply_markup=keyboard,
                )
                await callback.message.answer(f"✅ Subtitle for '<b>{original_filename}</b>' posted to channel '<b>{channel_full_name}</b>'!")
            except TelegramBadRequest as e:
                await callback.message.answer(f"❌ Failed to post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nMake sure the bot is an admin in the channel and has permission to post messages.")
            except Exception as e:
                await callback.message.answer(f"❌ An unexpected error occurred while posting: {e}")

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
 
    if lines and lines[-1] == "":
        lines.pop()
 
    text = "\n".join(lines)
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    await message.answer(f"<code>{text}</code>")

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
        return await message.answer(
            "❌ <b>reportlab</b> is not installed.\n"
            "Add <code>reportlab</code> to your <code>requirements.txt</code> and redeploy."
        )

    status_msg = await message.answer("⏳ Generating PDF report…")

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
    await bot.send_document(
        message.chat.id,
        BufferedInputFile(buffer.read(), filename=f"frictionrealm_stats_{ts_file}.pdf"),
        caption=f"📊 <b>TheFrictionRealm Stats</b>\n<i>{ts_display}</i>",
    )

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
                    download_filename = f"[ENG] {short_name_from_filename} {episode_info} @{bot_info.username}{file_extension}".strip().replace("  ", " ")

                    cursor.execute("UPDATE files SET filename = %s WHERE hash = %s", (download_filename, file_hash))
                    conn.commit()
                    deep_link = f"https://t.me/{bot_info.username}?start={file_hash}"
                    
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬇️ Download Subtitle", url=deep_link)]])
                    
                    try:
                        await bot.send_message(chat_id=target_channel_id, text=caption_text, reply_markup=keyboard)
                        return await message.answer(f"✅ Subtitle for '<b>{original_filename}</b>' automatically posted to channel '<b>{channel_full_name}</b>'!")
                    except TelegramBadRequest as e:
                        await message.answer(f"❌ Failed to auto-post to channel '<b>{channel_full_name}</b>'. Error: {e}\n\nFalling back to manual selection.")
                    except Exception as e:
                        await message.answer(f"❌ An unexpected error occurred during auto-post: {e}\n\nFalling back to manual selection.")

    admin_temp_state[message.from_user.id] = {'file_hash': file_hash, 'file_name': original_filename}

    channels_keyboard_buttons = []
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT short_name, full_name FROM channels ORDER BY short_name")
            for row in cursor.fetchall():
                channels_keyboard_buttons.append([InlineKeyboardButton(text=f"{row['short_name']} ({row['full_name']})", callback_data=f"post_to_channel_{row['short_name']}")])
    
    if not channels_keyboard_buttons:
        return await message.answer(f"✅ File '<b>{original_filename}</b>' uploaded.\n\n⚠️ No channels registered. Use `/setchannel` to add one.")

    await message.answer(
        f"✅ File '<b>{original_filename}</b>' uploaded!\n\nSelect the channel to post to:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=channels_keyboard_buttons),
    )

@admin_router.message(F.document)
async def handle_unauthorized_upload(message: Message):
    """Catches document uploads from non-admins."""
    await message.answer("🚫 <b>Access Denied</b>\n\nYou are not authorized to upload files.")

@admin_router.message(Command("check_dest"), F.from_user.id == ADMIN_ID)
async def check_destination_channel(message: Message):
    """Admin command to verify the bot can access the destination channel."""
    if not DESTINATION_CHANNEL_ID:
        return await message.answer("⚠️ `DESTINATION_CHANNEL_ID` is not set.")

    try:
        chat = await bot.get_chat(DESTINATION_CHANNEL_ID)
        my_member_info = await bot.get_chat_member(DESTINATION_CHANNEL_ID, bot.id)
        
        can_post = False
        status = my_member_info.status.capitalize()
        if isinstance(my_member_info, (ChatMemberOwner, ChatMemberAdministrator)):
            can_post = my_member_info.can_post_messages if isinstance(my_member_info, ChatMemberAdministrator) else True

        perm_text = "✅ Can post messages" if can_post else "❌ Cannot post messages"
        
        await message.answer(
            f"✅ <b>Destination Channel Check: OK</b>\n\n"
            f"<b>Name:</b> {chat.title}\n<b>ID:</b> <code>{chat.id}</code>\n"
            f"<b>Bot Status:</b> {status}\n<b>Permissions:</b> {perm_text}"
        )
    except TelegramNotFound:
        await message.answer(
            f"🚨 <b>Destination Channel Check: FAILED</b>\n\n"
            f"Could not find channel ID: <code>{DESTINATION_CHANNEL_ID}</code>.\n\n"
            f"<b>Fix:</b>\n1. Ensure `DESTINATION_CHANNEL_ID` is correct.\n"
            f"2. Add the bot to the channel as an admin."
        )
    except Exception as e:
        await message.answer(f"An unexpected error occurred: {e}")

@admin_router.message(Command("editbutton"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def edit_inline_button(message: Message):
    """Admin command to edit the inline button of a message previously posted by the bot."""
    forwarded_message = message.reply_to_message
    
    if not DESTINATION_CHANNEL_ID:
        return await message.reply("⚠️ `DESTINATION_CHANNEL_ID` is not set.")

    if not forwarded_message.forward_from_chat or forwarded_message.forward_from_chat.id != DESTINATION_CHANNEL_ID:
        return await message.reply(f"❌ The forwarded message must be from the destination channel (ID: <code>{DESTINATION_CHANNEL_ID}</code>).")

    if not forwarded_message.forward_from or forwarded_message.forward_from.id != (await bot.me()).id:
        return await message.reply("❌ The forwarded message must be one originally posted by this bot.")

    command_args = message.text.split(' ', 1)
    if len(command_args) < 2 or '|' not in command_args[1]:
        return await message.reply("❌ Usage: `/editbutton New Text | https://new.link`", parse_mode=ParseMode.MARKDOWN_V2)

    button_text, url = [arg.strip() for arg in command_args[1].split('|', 1)]

    if not button_text or not url or not url.startswith("https://"):
        return await message.reply("❌ Invalid format. Button text and a valid https URL are required.")

    new_keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=button_text, url=url)]])

    try:
        await bot.edit_message_reply_markup(
            chat_id=DESTINATION_CHANNEL_ID,
            message_id=forwarded_message.forward_from_message_id,
            reply_markup=new_keyboard
        )
        await message.reply("✅ Inline button updated!")
    except Exception as e:
        logging.error(f"Failed to edit inline button. Error: {e}")
        await message.reply(f"🚨 Failed to update button: <code>{e}</code>")

@admin_router.message(Command("post"), F.from_user.id == ADMIN_ID, F.reply_to_message)
async def post_forwarded_message(message: Message):
    """Admin command to re-post a forwarded message to the destination channel."""
    forwarded_message = message.reply_to_message

    if not forwarded_message.forward_from_chat:
        return await message.reply("❌ This command only works when replying to a forwarded message from a channel.")

    if not forwarded_message.photo or not forwarded_message.caption:
        return await message.reply("❌ The forwarded message must contain a photo and a caption.")

    if not DESTINATION_CHANNEL_ID:
        return await message.reply("⚠️ `DESTINATION_CHANNEL_ID` is not set.")

    episode_match = re.search(r'\b(EP\d+|S\d+E\d+)\b', forwarded_message.caption, re.IGNORECASE)
    if not episode_match:
        return await message.reply(f"❌ No episode pattern (e.g., EP123) found in the caption.")
    
    episode_info = episode_match.group(0).upper()
    original_chat = forwarded_message.forward_from_chat
    if not original_chat.username:
        return await message.reply(f"❌ The source channel (<code>{original_chat.id}</code>) must be public.")
        
    # Check if there are any force-join channels registered
    has_channels = False
    try:
        with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM channels")
                has_channels = cursor.fetchone()[0] > 0
    except Exception:
        pass

    if has_channels:
        bot_info = await bot.me()
        button_link = f"https://t.me/{bot_info.username}?start=post_{original_chat.username}_{forwarded_message.forward_from_message_id}"
    else:
        button_link = f"https://t.me/{original_chat.username}/{forwarded_message.forward_from_message_id}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"⬇️ {episode_info} | Download Here", url=button_link)]])

    try:
        copied_message = await bot.copy_message(
            chat_id=DESTINATION_CHANNEL_ID,
            from_chat_id=forwarded_message.chat.id,
            message_id=forwarded_message.message_id
        )
        await bot.edit_message_reply_markup(
            chat_id=DESTINATION_CHANNEL_ID,
            message_id=copied_message.message_id,
            reply_markup=keyboard
        )
        await message.reply(f"✅ Successfully posted to channel <code>{DESTINATION_CHANNEL_ID}</code>.")
    except TelegramBadRequest as e:
        logging.error(f"Failed to post forwarded message. Error: {e}")
        reply_text = f"🚨 <b>Posting Failed!</b>\n\nError: <code>{e}</code>\n\n"
        if "chat not found" in str(e).lower():
            reply_text += "<b>Suggestion:</b> Ensure `DESTINATION_CHANNEL_ID` is correct and the bot is an admin in the channel."
        await message.reply(reply_text)
    except Exception as e:
        logging.error(f"Failed to post forwarded message. Error: {e}")
        await message.reply(f"🚨 <b>An unexpected error occurred!</b>\n\nError: <code>{e}</code>")

@admin_router.message(Command("addad"), F.from_user.id == ADMIN_ID, F.reply_to_message)
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
                    cursor.execute("INSERT INTO ads (channel_id, message_id, url, timestamp) VALUES (%s, %s, %s, %s)", (channel_id, orig_msg_id, ad_url, time.time()))
                    await message.reply(f"✅ Ad (ID: {orig_msg_id}) successfully registered!")
                else:
                    await message.reply("⚠️ This ad is already in the database.")
    else:
        await message.reply("❌ No URL found in the replied message.")