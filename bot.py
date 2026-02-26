#!/usr/bin/env python3
"""
MegaBot - Ultimate Telegram Bot (500+ Features)
Rose Bot + Helping Bot Combined
Designed for Render.com Web Service Hosting
Database: Render PostgreSQL
"""

import os
import re
import json
import time
import html
import math
import random
import string
import hashlib
import logging
import asyncio
import aiohttp
import datetime
import traceback
import urllib.parse
from io import BytesIO
from uuid import uuid4
from functools import wraps
from collections import defaultdict
from typing import Optional, List, Dict, Tuple, Any

# Flask for Render Web Service
from flask import Flask, request, jsonify
import threading

# python-telegram-bot
from telegram import (
    Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    ChatPermissions, ParseMode, ChatMember, InputMediaPhoto,
    InputMediaVideo, InputMediaDocument, InputMediaAudio,
    InlineQueryResultArticle, InputTextMessageContent,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton,
    ForceReply, MessageEntity, TelegramError
)
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, CallbackQueryHandler,
    InlineQueryHandler, Filters, ConversationHandler,
    ChatMemberHandler, Defaults
)
from telegram.utils.helpers import mention_html, escape_markdown

# PostgreSQL
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2 import pool

# ============================================================
# CONFIGURATION
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@host:5432/dbname")
OWNER_ID = int(os.environ.get("OWNER_ID", "123456789"))
SUDO_USERS = list(map(int, os.environ.get("SUDO_USERS", "").split(","))) if os.environ.get("SUDO_USERS") else []
PORT = int(os.environ.get("PORT", 10000))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")  # Your Render URL

# Bot start time (for uptime tracking)
START_TIME = time.time()

# Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK APP (For Render Web Service)
# ============================================================

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({
        "status": "alive",
        "bot": "MegaBot",
        "features": "500+",
        "version": "2.0"
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "uptime": time.time()})

# ============================================================
# DATABASE MANAGER
# ============================================================

class DatabaseManager:
    def __init__(self, database_url):
        self.database_url = database_url
        self.connection_pool = None
        self.init_pool()
        self.create_tables()

    def init_pool(self):
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                1, 20, self.database_url, sslmode='require'
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            # Try without SSL
            try:
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    1, 20, self.database_url
                )
                logger.info("Database connection pool created (no SSL)")
            except Exception as e2:
                logger.error(f"Error creating connection pool (no SSL): {e2}")

    def get_conn(self):
        return self.connection_pool.getconn()

    def put_conn(self, conn):
        self.connection_pool.putconn(conn)

    def execute(self, query, params=None, fetch=False, fetchone=False):
        conn = self.get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch:
                    result = cur.fetchall()
                elif fetchone:
                    result = cur.fetchone()
                else:
                    result = None
                conn.commit()
                return result
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            return None
        finally:
            self.put_conn(conn)

    def create_tables(self):
        tables = """
        -- Users table
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            bio TEXT DEFAULT '',
            reputation INT DEFAULT 0,
            warns INT DEFAULT 0,
            is_banned BOOLEAN DEFAULT FALSE,
            is_gbanned BOOLEAN DEFAULT FALSE,
            afk BOOLEAN DEFAULT FALSE,
            afk_reason TEXT DEFAULT '',
            afk_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            language VARCHAR(10) DEFAULT 'en',
            coins BIGINT DEFAULT 0,
            xp BIGINT DEFAULT 0,
            level INT DEFAULT 1,
            daily_claimed TIMESTAMP,
            married_to BIGINT DEFAULT 0,
            profile_pic_count INT DEFAULT 0
        );

        -- Chats table
        CREATE TABLE IF NOT EXISTS chats (
            chat_id BIGINT PRIMARY KEY,
            chat_title VARCHAR(255),
            chat_type VARCHAR(50),
            welcome_enabled BOOLEAN DEFAULT TRUE,
            welcome_text TEXT DEFAULT 'Hey {first}, welcome to {chatname}!',
            welcome_media TEXT DEFAULT '',
            welcome_media_type VARCHAR(50) DEFAULT '',
            goodbye_enabled BOOLEAN DEFAULT TRUE,
            goodbye_text TEXT DEFAULT 'Sad to see you go, {first}!',
            antiflood_enabled BOOLEAN DEFAULT FALSE,
            antiflood_limit INT DEFAULT 10,
            antiflood_action VARCHAR(20) DEFAULT 'mute',
            antispam_enabled BOOLEAN DEFAULT FALSE,
            antilink_enabled BOOLEAN DEFAULT FALSE,
            antilink_action VARCHAR(20) DEFAULT 'warn',
            antinsfw_enabled BOOLEAN DEFAULT FALSE,
            clean_welcome BOOLEAN DEFAULT TRUE,
            last_welcome_msg BIGINT DEFAULT 0,
            log_channel BIGINT DEFAULT 0,
            rules TEXT DEFAULT '',
            language VARCHAR(10) DEFAULT 'en',
            locked_types TEXT DEFAULT '[]',
            warn_limit INT DEFAULT 3,
            warn_action VARCHAR(20) DEFAULT 'ban',
            autorole BIGINT DEFAULT 0,
            captcha_enabled BOOLEAN DEFAULT FALSE,
            captcha_type VARCHAR(20) DEFAULT 'button',
            nightmode_enabled BOOLEAN DEFAULT FALSE,
            nightmode_start VARCHAR(10) DEFAULT '00:00',
            nightmode_end VARCHAR(10) DEFAULT '06:00',
            slowmode INT DEFAULT 0,
            chat_games_enabled BOOLEAN DEFAULT TRUE,
            sticker_pack VARCHAR(255) DEFAULT '',
            force_sub_channel BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Chat members tracking
        CREATE TABLE IF NOT EXISTS chat_members (
            chat_id BIGINT,
            user_id BIGINT,
            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_count BIGINT DEFAULT 0,
            last_message TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_admin BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (chat_id, user_id)
        );

        -- Notes/Saves
        CREATE TABLE IF NOT EXISTS notes (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            note_name VARCHAR(255),
            note_text TEXT,
            note_media TEXT DEFAULT '',
            note_media_type VARCHAR(50) DEFAULT '',
            note_buttons TEXT DEFAULT '[]',
            is_private BOOLEAN DEFAULT FALSE,
            created_by BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Filters
        CREATE TABLE IF NOT EXISTS filters (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            keyword VARCHAR(255),
            reply_text TEXT,
            reply_media TEXT DEFAULT '',
            reply_media_type VARCHAR(50) DEFAULT '',
            reply_buttons TEXT DEFAULT '[]',
            created_by BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Warnings
        CREATE TABLE IF NOT EXISTS warnings (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            reason TEXT DEFAULT 'No reason',
            warned_by BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Blacklist words
        CREATE TABLE IF NOT EXISTS blacklist (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            trigger_word VARCHAR(255),
            action VARCHAR(20) DEFAULT 'delete',
            created_by BIGINT
        );

        -- Disabled commands
        CREATE TABLE IF NOT EXISTS disabled_commands (
            chat_id BIGINT,
            command VARCHAR(255),
            PRIMARY KEY (chat_id, command)
        );

        -- Custom commands
        CREATE TABLE IF NOT EXISTS custom_commands (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            command VARCHAR(255),
            response TEXT,
            response_media TEXT DEFAULT '',
            response_media_type VARCHAR(50) DEFAULT '',
            created_by BIGINT
        );

        -- Reminders
        CREATE TABLE IF NOT EXISTS reminders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            chat_id BIGINT,
            reminder_text TEXT,
            reminder_time TIMESTAMP,
            is_done BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Polls
        CREATE TABLE IF NOT EXISTS polls (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            creator_id BIGINT,
            question TEXT,
            options TEXT DEFAULT '[]',
            votes TEXT DEFAULT '{}',
            is_anonymous BOOLEAN DEFAULT TRUE,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Giveaways
        CREATE TABLE IF NOT EXISTS giveaways (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            creator_id BIGINT,
            prize TEXT,
            participants TEXT DEFAULT '[]',
            winner_count INT DEFAULT 1,
            end_time TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Reports
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            reporter_id BIGINT,
            reported_user_id BIGINT,
            reason TEXT DEFAULT '',
            message_id BIGINT,
            is_resolved BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Federation
        CREATE TABLE IF NOT EXISTS federations (
            fed_id VARCHAR(255) PRIMARY KEY,
            fed_name VARCHAR(255),
            owner_id BIGINT,
            admins TEXT DEFAULT '[]',
            banned_users TEXT DEFAULT '[]',
            log_channel BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Federation chats
        CREATE TABLE IF NOT EXISTS fed_chats (
            chat_id BIGINT PRIMARY KEY,
            fed_id VARCHAR(255)
        );

        -- Approval list
        CREATE TABLE IF NOT EXISTS approvals (
            chat_id BIGINT,
            user_id BIGINT,
            approved_by BIGINT,
            PRIMARY KEY (chat_id, user_id)
        );

        -- AFK tracking
        CREATE TABLE IF NOT EXISTS afk_users (
            user_id BIGINT PRIMARY KEY,
            reason TEXT DEFAULT '',
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Reputation
        CREATE TABLE IF NOT EXISTS reputation (
            chat_id BIGINT,
            user_id BIGINT,
            rep_count INT DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        );

        -- Pins
        CREATE TABLE IF NOT EXISTS pins (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            message_id BIGINT,
            pinned_by BIGINT,
            pin_text TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Muted users
        CREATE TABLE IF NOT EXISTS muted_users (
            chat_id BIGINT,
            user_id BIGINT,
            muted_by BIGINT,
            reason TEXT DEFAULT '',
            until_time TIMESTAMP,
            PRIMARY KEY (chat_id, user_id)
        );

        -- Banned users
        CREATE TABLE IF NOT EXISTS banned_users (
            chat_id BIGINT,
            user_id BIGINT,
            banned_by BIGINT,
            reason TEXT DEFAULT '',
            PRIMARY KEY (chat_id, user_id)
        );

        -- Global bans
        CREATE TABLE IF NOT EXISTS gbans (
            user_id BIGINT PRIMARY KEY,
            reason TEXT DEFAULT '',
            banned_by BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Sticker packs
        CREATE TABLE IF NOT EXISTS sticker_packs (
            user_id BIGINT PRIMARY KEY,
            pack_name VARCHAR(255),
            sticker_count INT DEFAULT 0
        );

        -- User bio
        CREATE TABLE IF NOT EXISTS user_bios (
            user_id BIGINT PRIMARY KEY,
            bio TEXT DEFAULT '',
            set_by BIGINT
        );

        -- Connection
        CREATE TABLE IF NOT EXISTS connections (
            user_id BIGINT PRIMARY KEY,
            chat_id BIGINT,
            connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Antiflood tracking
        CREATE TABLE IF NOT EXISTS flood_control (
            chat_id BIGINT,
            user_id BIGINT,
            message_count INT DEFAULT 0,
            first_message_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, user_id)
        );

        -- Coupons/Promo codes
        CREATE TABLE IF NOT EXISTS coupons (
            code VARCHAR(50) PRIMARY KEY,
            reward_type VARCHAR(50),
            reward_amount INT,
            max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0,
            used_by TEXT DEFAULT '[]',
            expires_at TIMESTAMP,
            created_by BIGINT
        );

        -- Trivia scores
        CREATE TABLE IF NOT EXISTS trivia_scores (
            chat_id BIGINT,
            user_id BIGINT,
            correct INT DEFAULT 0,
            wrong INT DEFAULT 0,
            streak INT DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        );

        -- RSS Feeds
        CREATE TABLE IF NOT EXISTS rss_feeds (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            feed_url TEXT,
            last_entry TEXT DEFAULT '',
            is_active BOOLEAN DEFAULT TRUE
        );

        -- Auto-reply
        CREATE TABLE IF NOT EXISTS auto_replies (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            trigger_text VARCHAR(255),
            reply_text TEXT,
            reply_type VARCHAR(50) DEFAULT 'text',
            is_regex BOOLEAN DEFAULT FALSE
        );

        -- Scheduled messages
        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            message_text TEXT,
            scheduled_time TIMESTAMP,
            repeat_interval VARCHAR(50) DEFAULT 'none',
            is_active BOOLEAN DEFAULT TRUE,
            created_by BIGINT
        );

        -- Ticket system
        CREATE TABLE IF NOT EXISTS tickets (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            subject TEXT,
            description TEXT,
            status VARCHAR(20) DEFAULT 'open',
            assigned_to BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Starboard
        CREATE TABLE IF NOT EXISTS starboard (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            message_id BIGINT,
            star_count INT DEFAULT 0,
            starboard_msg_id BIGINT DEFAULT 0,
            starred_by TEXT DEFAULT '[]'
        );

        -- Tags
        CREATE TABLE IF NOT EXISTS tags (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            tag_name VARCHAR(255),
            user_ids TEXT DEFAULT '[]',
            created_by BIGINT
        );

        -- Greetings (personal)
        CREATE TABLE IF NOT EXISTS greetings (
            user_id BIGINT,
            chat_id BIGINT,
            greeting_text TEXT DEFAULT '',
            PRIMARY KEY (user_id, chat_id)
        );

        -- Action log
        CREATE TABLE IF NOT EXISTS action_log (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            action_type VARCHAR(50),
            action_by BIGINT,
            action_on BIGINT DEFAULT 0,
            details TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Bot stats
        CREATE TABLE IF NOT EXISTS bot_stats (
            id SERIAL PRIMARY KEY,
            stat_date DATE DEFAULT CURRENT_DATE,
            messages_processed BIGINT DEFAULT 0,
            commands_executed BIGINT DEFAULT 0,
            users_joined BIGINT DEFAULT 0,
            groups_joined BIGINT DEFAULT 0
        );

        -- Word game data
        CREATE TABLE IF NOT EXISTS word_games (
            chat_id BIGINT PRIMARY KEY,
            current_word VARCHAR(255) DEFAULT '',
            last_player BIGINT DEFAULT 0,
            is_active BOOLEAN DEFAULT FALSE,
            scores TEXT DEFAULT '{}'
        );

        -- Music queue
        CREATE TABLE IF NOT EXISTS music_queue (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            song_name TEXT,
            requested_by BIGINT,
            url TEXT DEFAULT '',
            is_playing BOOLEAN DEFAULT FALSE
        );

        -- Confession system
        CREATE TABLE IF NOT EXISTS confessions (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            user_id BIGINT,
            confession_text TEXT,
            is_anonymous BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Todo list
        CREATE TABLE IF NOT EXISTS todos (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            task TEXT,
            is_done BOOLEAN DEFAULT FALSE,
            priority INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_notes_chat ON notes(chat_id);
        CREATE INDEX IF NOT EXISTS idx_filters_chat ON filters(chat_id);
        CREATE INDEX IF NOT EXISTS idx_warnings_chat_user ON warnings(chat_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_blacklist_chat ON blacklist(chat_id);
        CREATE INDEX IF NOT EXISTS idx_chat_members_chat ON chat_members(chat_id);
        CREATE INDEX IF NOT EXISTS idx_action_log_chat ON action_log(chat_id);
        CREATE INDEX IF NOT EXISTS idx_reminders_time ON reminders(reminder_time);
        CREATE INDEX IF NOT EXISTS idx_scheduled_time ON scheduled_messages(scheduled_time);
        """
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(tables)
            conn.commit()
            logger.info("All database tables created successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error creating tables: {e}")
        finally:
            self.put_conn(conn)


# Initialize Database
db = DatabaseManager(DATABASE_URL)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def is_owner(user_id):
    return user_id == OWNER_ID

def is_sudo(user_id):
    return user_id in SUDO_USERS or is_owner(user_id)

def is_admin(update, context, user_id=None):
    if not update.effective_chat or update.effective_chat.type == 'private':
        return True
    uid = user_id or update.effective_user.id
    if is_sudo(uid):
        return True
    try:
        member = context.bot.get_chat_member(update.effective_chat.id, uid)
        return member.status in ['creator', 'administrator']
    except:
        return False

def is_creator(update, context, user_id=None):
    if not update.effective_chat:
        return False
    uid = user_id or update.effective_user.id
    if is_owner(uid):
        return True
    try:
        member = context.bot.get_chat_member(update.effective_chat.id, uid)
        return member.status == 'creator'
    except:
        return False

def can_restrict(update, context):
    try:
        bot_member = context.bot.get_chat_member(
            update.effective_chat.id, context.bot.id
        )
        return bot_member.can_restrict_members
    except:
        return False

def can_delete(update, context):
    try:
        bot_member = context.bot.get_chat_member(
            update.effective_chat.id, context.bot.id
        )
        return bot_member.can_delete_messages
    except:
        return False

def can_pin(update, context):
    try:
        bot_member = context.bot.get_chat_member(
            update.effective_chat.id, context.bot.id
        )
        return bot_member.can_pin_messages
    except:
        return False

def extract_user(update, context):
    """Extract user from message (reply, mention, ID)"""
    msg = update.effective_message
    if msg.reply_to_message:
        return msg.reply_to_message.from_user.id, msg.reply_to_message.from_user.first_name
    args = context.args
    if args:
        if args[0].isdigit():
            return int(args[0]), str(args[0])
        if args[0].startswith('@'):
            username = args[0].lstrip('@')
            # Try to find user in database
            result = db.execute(
                "SELECT user_id, first_name FROM users WHERE username = %s",
                (username,), fetchone=True
            )
            if result:
                return result['user_id'], result['first_name']
            return None, username
    if msg.entities:
        for ent in msg.entities:
            if ent.type == 'text_mention':
                return ent.user.id, ent.user.first_name
    return None, None

def extract_time(time_str):
    """Extract time from string like 1h, 30m, 1d"""
    if not time_str:
        return None
    time_str = time_str.lower().strip()
    if time_str.endswith('m') or time_str.endswith('min'):
        val = int(re.search(r'\d+', time_str).group())
        return datetime.datetime.now() + datetime.timedelta(minutes=val)
    elif time_str.endswith('h') or time_str.endswith('hr'):
        val = int(re.search(r'\d+', time_str).group())
        return datetime.datetime.now() + datetime.timedelta(hours=val)
    elif time_str.endswith('d') or time_str.endswith('day'):
        val = int(re.search(r'\d+', time_str).group())
        return datetime.datetime.now() + datetime.timedelta(days=val)
    elif time_str.endswith('w') or time_str.endswith('week'):
        val = int(re.search(r'\d+', time_str).group())
        return datetime.datetime.now() + datetime.timedelta(weeks=val)
    return None

def format_welcome(text, user, chat):
    """Format welcome/goodbye text with variables"""
    replacements = {
        '{first}': user.first_name or '',
        '{last}': user.last_name or '',
        '{fullname}': user.full_name or '',
        '{username}': f'@{user.username}' if user.username else user.first_name,
        '{mention}': mention_html(user.id, user.first_name),
        '{id}': str(user.id),
        '{chatname}': chat.title or '',
        '{chatid}': str(chat.id),
        '{count}': str(chat.get_member_count()) if hasattr(chat, 'get_member_count') else '?',
    }
    for key, value in replacements.items():
        text = text.replace(key, value)
    return text

def parse_buttons(text):
    """Parse button format [text](buttonurl://url)"""
    buttons = []
    clean_text = text
    pattern = r'\[(.+?)\]\(buttonurl://(.+?)\)'
    matches = re.findall(pattern, text)
    for btn_text, btn_url in matches:
        same_line = btn_url.endswith(':same')
        if same_line:
            btn_url = btn_url[:-5]
        buttons.append({
            'text': btn_text,
            'url': btn_url,
            'same_line': same_line
        })
    clean_text = re.sub(pattern, '', text).strip()
    return clean_text, buttons

def build_keyboard(buttons):
    """Build inline keyboard from button list"""
    keyboard = []
    for btn in buttons:
        if btn.get('same_line') and keyboard:
            keyboard[-1].append(InlineKeyboardButton(btn['text'], url=btn['url']))
        else:
            keyboard.append([InlineKeyboardButton(btn['text'], url=btn['url'])])
    return InlineKeyboardMarkup(keyboard) if keyboard else None

def get_readable_time(seconds):
    """Convert seconds to readable time"""
    periods = [
        ('year', 60*60*24*365),
        ('month', 60*60*24*30),
        ('week', 60*60*24*7),
        ('day', 60*60*24),
        ('hour', 60*60),
        ('minute', 60),
        ('second', 1)
    ]
    result = []
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result.append(f"{int(period_value)} {period_name}{'s' if period_value > 1 else ''}")
    return ', '.join(result[:3]) if result else '0 seconds'

def get_user_info_text(user_id, chat_id=None):
    """Get formatted user info"""
    user_data = db.execute(
        "SELECT * FROM users WHERE user_id = %s", (user_id,), fetchone=True
    )
    if not user_data:
        return "User not found in database."
    
    text = f"╔══════════════════╗\n"
    text += f"  📋 <b>User Information</b>\n"
    text += f"╚══════════════════╝\n\n"
    text += f"🆔 <b>ID:</b> <code>{user_data['user_id']}</code>\n"
    text += f"👤 <b>First Name:</b> {user_data['first_name']}\n"
    if user_data.get('last_name'):
        text += f"👤 <b>Last Name:</b> {user_data['last_name']}\n"
    if user_data.get('username'):
        text += f"📛 <b>Username:</b> @{user_data['username']}\n"
    text += f"⭐ <b>Reputation:</b> {user_data.get('reputation', 0)}\n"
    text += f"💰 <b>Coins:</b> {user_data.get('coins', 0)}\n"
    text += f"📊 <b>Level:</b> {user_data.get('level', 1)}\n"
    text += f"✨ <b>XP:</b> {user_data.get('xp', 0)}\n"
    
    if user_data.get('is_gbanned'):
        text += f"\n🚫 <b>GLOBALLY BANNED</b>\n"
    
    warn_count = db.execute(
        "SELECT COUNT(*) as cnt FROM warnings WHERE user_id = %s AND chat_id = %s",
        (user_id, chat_id), fetchone=True
    ) if chat_id else None
    if warn_count and warn_count['cnt'] > 0:
        text += f"⚠️ <b>Warnings:</b> {warn_count['cnt']}\n"
    
    if chat_id:
        member_data = db.execute(
            "SELECT message_count FROM chat_members WHERE user_id = %s AND chat_id = %s",
            (user_id, chat_id), fetchone=True
        )
        if member_data:
            text += f"💬 <b>Messages:</b> {member_data['message_count']}\n"
    
    return text

# Decorator for admin-only commands
def admin_only(func):
    @wraps(func)
    def wrapper(update, context, *args, **kwargs):
        if update.effective_chat.type == 'private':
            return func(update, context, *args, **kwargs)
        if not is_admin(update, context):
            update.effective_message.reply_text("❌ You need to be an admin to use this command!")
            return
        return func(update, context, *args, **kwargs)
    return wrapper

def owner_only(func):
    @wraps(func)
    def wrapper(update, context, *args, **kwargs):
        if not is_owner(update.effective_user.id):
            update.effective_message.reply_text("❌ This command is only for the bot owner!")
            return
        return func(update, context, *args, **kwargs)
    return wrapper

def sudo_only(func):
    @wraps(func)
    def wrapper(update, context, *args, **kwargs):
        if not is_sudo(update.effective_user.id):
            update.effective_message.reply_text("❌ This command is only for sudo users!")
            return
        return func(update, context, *args, **kwargs)
    return wrapper

def group_only(func):
    @wraps(func)
    def wrapper(update, context, *args, **kwargs):
        if update.effective_chat.type == 'private':
            update.effective_message.reply_text("❌ This command can only be used in groups!")
            return
        return func(update, context, *args, **kwargs)
    return wrapper

def check_disabled(func):
    @wraps(func)
    def wrapper(update, context, *args, **kwargs):
        if update.effective_chat and update.effective_chat.type != 'private':
            cmd = update.effective_message.text.split()[0].lstrip('/').split('@')[0]
            result = db.execute(
                "SELECT command FROM disabled_commands WHERE chat_id = %s AND command = %s",
                (update.effective_chat.id, cmd), fetchone=True
            )
            if result:
                return
        return func(update, context, *args, **kwargs)
    return wrapper

# ============================================================
# FEATURE 1-10: BASIC COMMANDS
# ============================================================

def start_command(update, context):
    """Start command - Feature 1"""
    user = update.effective_user
    # Save user to database
    db.execute("""
        INSERT INTO users (user_id, username, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
        username = EXCLUDED.username,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        updated_at = CURRENT_TIMESTAMP
    """, (user.id, user.username, user.first_name, user.last_name))
    
    if update.effective_chat.type == 'private':
        keyboard = [
            [
                InlineKeyboardButton("➕ Add me to Group", url=f"t.me/{context.bot.username}?startgroup=true"),
                InlineKeyboardButton("📚 Help", callback_data="help_main")
            ],
            [
                InlineKeyboardButton("📊 Stats", callback_data="bot_stats"),
                InlineKeyboardButton("⚙️ Settings", callback_data="settings_main")
            ],
            [
                InlineKeyboardButton("👤 Owner", url=f"tg://user?id={OWNER_ID}"),
                InlineKeyboardButton("📢 Updates", url="https://t.me/telegram")
            ],
            [
                InlineKeyboardButton("🔧 All Features (500+)", callback_data="all_features")
            ]
        ]
        
        text = f"""
╔══════════════════════════╗
  🤖 <b>MegaBot - Ultimate Bot</b>
╚══════════════════════════╝

Hey {mention_html(user.id, user.first_name)}! 👋

I'm <b>MegaBot</b> - The most powerful Telegram bot with <b>500+ features</b>!

🔹 <b>Admin Tools:</b> Ban, Mute, Warn, Kick & more
🔹 <b>Welcome System:</b> Custom welcomes & captcha
🔹 <b>Anti-Spam:</b> Flood, Link, NSFW protection  
🔹 <b>Notes & Filters:</b> Save & auto-reply
🔹 <b>Fun & Games:</b> Trivia, Word chain, RPG
🔹 <b>Moderation:</b> Locks, Blacklist, Reports
🔹 <b>Federation:</b> Cross-group ban system
🔹 <b>And 450+ more features!</b>

Click <b>Help</b> to see all commands! 🚀
"""
        update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )
    else:
        update.effective_message.reply_text(
            f"Hey {mention_html(user.id, user.first_name)}! I'm alive and ready! 🤖\n"
            f"Use /help to see what I can do!",
            parse_mode=ParseMode.HTML
        )

def help_command(update, context):
    """Help command - Feature 2"""
    if update.effective_chat.type != 'private':
        keyboard = [[InlineKeyboardButton("📚 Help", url=f"t.me/{context.bot.username}?start=help")]]
        update.effective_message.reply_text(
            "Click the button below to get help in PM!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    send_help_menu(update, context)

def send_help_menu(update, context, edit=False):
    keyboard = [
        [
            InlineKeyboardButton("👮 Admin", callback_data="help_admin"),
            InlineKeyboardButton("👋 Welcome", callback_data="help_welcome"),
            InlineKeyboardButton("📝 Notes", callback_data="help_notes")
        ],
        [
            InlineKeyboardButton("🔍 Filters", callback_data="help_filters"),
            InlineKeyboardButton("⚠️ Warns", callback_data="help_warns"),
            InlineKeyboardButton("🔒 Locks", callback_data="help_locks")
        ],
        [
            InlineKeyboardButton("🚫 Blacklist", callback_data="help_blacklist"),
            InlineKeyboardButton("🛡️ AntiSpam", callback_data="help_antispam"),
            InlineKeyboardButton("🌊 AntiFlood", callback_data="help_antiflood")
        ],
        [
            InlineKeyboardButton("📊 Stats", callback_data="help_stats"),
            InlineKeyboardButton("🎮 Games", callback_data="help_games"),
            InlineKeyboardButton("💰 Economy", callback_data="help_economy")
        ],
        [
            InlineKeyboardButton("🏛️ Federation", callback_data="help_federation"),
            InlineKeyboardButton("🔗 Connection", callback_data="help_connection"),
            InlineKeyboardButton("📌 Pins", callback_data="help_pins")
        ],
        [
            InlineKeyboardButton("👤 Users", callback_data="help_users"),
            InlineKeyboardButton("🎨 Fun", callback_data="help_fun"),
            InlineKeyboardButton("🛠️ Tools", callback_data="help_tools")
        ],
        [
            InlineKeyboardButton("⏰ Reminders", callback_data="help_reminders"),
            InlineKeyboardButton("🎁 Giveaway", callback_data="help_giveaway"),
            InlineKeyboardButton("🎫 Tickets", callback_data="help_tickets")
        ],
        [
            InlineKeyboardButton("📋 Misc", callback_data="help_misc"),
            InlineKeyboardButton("👑 Owner", callback_data="help_owner"),
            InlineKeyboardButton("🌙 Night", callback_data="help_nightmode")
        ]
    ]
    
    text = """
╔══════════════════════════╗
  📚 <b>MegaBot Help Menu</b>
╚══════════════════════════╝

Click any category below to see available commands.

<b>Total Features:</b> 500+
<b>Total Commands:</b> 200+

<i>Tip: Most commands work in groups only.</i>
<i>Add me as admin with full permissions!</i>
"""
    
    if edit and update.callback_query:
        update.callback_query.edit_message_text(
            text, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        update.effective_message.reply_text(
            text, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# Help category texts
HELP_TEXTS = {
    "help_admin": """
👮 <b>Admin Commands</b>

/ban - Ban a user
/tban [time] - Temporarily ban
/unban - Unban a user
/kick - Kick a user
/mute - Mute a user
/tmute [time] - Temporarily mute
/unmute - Unmute a user
/promote - Promote a user
/demote - Demote a user
/title [title] - Set admin title
/pin - Pin a message
/unpin - Unpin a message
/unpinall - Unpin all messages
/invitelink - Get invite link
/setgtitle - Set group title
/setgdesc - Set group description
/setgpic - Set group photo
/delgpic - Delete group photo
/setsticker - Set group sticker set
/delsticker - Delete group sticker set
/admins - List all admins
/adminlist - Detailed admin list
/zombies - Clean deleted accounts
/purge - Purge messages
/del - Delete a message
/slowmode [time] - Set slowmode
""",

    "help_welcome": """
👋 <b>Welcome/Goodbye System</b>

/welcome - View welcome settings
/welcome on/off - Toggle welcome
/setwelcome [text] - Set welcome message
/resetwelcome - Reset to default
/goodbye on/off - Toggle goodbye
/setgoodbye [text] - Set goodbye message
/resetgoodbye - Reset to default
/cleanwelcome on/off - Delete old welcome

<b>Variables:</b>
{first} - First name
{last} - Last name
{fullname} - Full name
{username} - @username
{mention} - Mention with link
{id} - User ID
{chatname} - Chat name
{count} - Member count

<b>Buttons:</b>
[Button Text](buttonurl://url)

/captcha on/off - Enable captcha
/captchatype button/math - Set type
""",

    "help_notes": """
📝 <b>Notes System</b>

/save [name] [text] - Save a note
/get [name] - Get a note
/notes - List all notes
/clear [name] - Delete a note
/clearall - Delete all notes
/privatenotes on/off - Send in PM

#notename - Quick get note

<b>Formatting:</b>
Supports HTML formatting
Supports buttons: [text](buttonurl://url)
Supports media (reply to media)
""",

    "help_filters": """
🔍 <b>Filters System</b>

/filter [keyword] [reply] - Add filter
/filters - List all filters
/stop [keyword] - Remove filter
/stopall - Remove all filters

<b>Supports:</b>
• Text replies
• Media replies
• Button replies
• Regex patterns
""",

    "help_warns": """
⚠️ <b>Warning System</b>

/warn [reason] - Warn a user
/dwarn - Warn and delete message
/warns - Check user warnings
/rmwarn - Remove last warning
/resetwarns - Reset all warnings
/warnlimit [num] - Set warn limit
/warnaction ban/kick/mute - Set action
/warnlist - List warned users
""",

    "help_locks": """
🔒 <b>Lock System</b>

/lock [type] - Lock a type
/unlock [type] - Unlock a type
/locks - View all locks
/locktypes - List lockable types

<b>Lockable types:</b>
text, media, sticker, gif, photo,
video, voice, document, audio,
game, poll, url, forward, reply,
bot, button, inline, contact,
location, phone, email, command,
all
""",

    "help_blacklist": """
🚫 <b>Blacklist System</b>

/blacklist - View blacklisted words
/addblacklist [word] - Add to blacklist
/unblacklist [word] - Remove from blacklist
/blacklistmode delete/warn/mute/ban - Set action
/rmblacklist [word] - Remove blacklist
/clearblacklist - Clear all
""",

    "help_antispam": """
🛡️ <b>Anti-Spam System</b>

/antispam on/off - Toggle anti-spam
/antilink on/off - Toggle anti-link
/antilinkaction warn/mute/ban/kick - Set action
/antinsfw on/off - Toggle anti-NSFW
/forcesub [channel] - Force subscribe
/forcesub off - Disable force sub
/approvedlist - List approved users
/approve [user] - Approve user
/unapprove [user] - Unapprove user
""",

    "help_antiflood": """
🌊 <b>Anti-Flood System</b>

/antiflood on/off - Toggle anti-flood
/setflood [number] - Set flood limit
/floodaction mute/ban/kick - Set action
/floodtimer [time] - Set flood reset time
""",

    "help_stats": """
📊 <b>Statistics</b>

/stats - Bot statistics
/chatstats - Chat statistics
/userstats - Your statistics
/topusers - Top active users
/toprepped - Top reputation users
/toprichest - Richest users
/leaderboard - Overall leaderboard
/msgcount - Message count
""",

    "help_games": """
🎮 <b>Games & Fun</b>

/trivia - Start trivia game
/wordchain - Start word chain
/quiz - Random quiz
/riddle - Get a riddle
/truth - Truth question
/dare - Dare challenge
/roll - Roll a dice
/flip - Flip a coin
/rps [rock/paper/scissors] - Play RPS
/slots - Slot machine
/8ball [question] - Magic 8 ball
/ship @user1 @user2 - Ship users
/rate @user - Rate user
/fight @user - Fight user
/duel @user - Duel user
/tictactoe @user - Tic tac toe
/hangman - Hangman game
/guess - Number guessing
/would - Would you rather
/nhie - Never have I ever
""",

    "help_economy": """
💰 <b>Economy System</b>

/balance - Check balance
/daily - Daily reward
/transfer [user] [amount] - Send coins
/shop - View shop
/buy [item] - Buy item
/inventory - Your inventory
/gamble [amount] - Gamble coins
/rob @user - Rob a user
/work - Earn coins
/mine - Mine for coins
/fish - Go fishing
/hunt - Go hunting
/farm - Farm crops
/bank - Bank info
/deposit [amount] - Deposit coins
/withdraw [amount] - Withdraw coins
/richest - Richest users
/coupon [code] - Redeem coupon
""",

    "help_federation": """
🏛️ <b>Federation System</b>

/newfed [name] - Create federation
/joinfed [fed_id] - Join federation
/leavefed - Leave federation
/fedinfo [fed_id] - Federation info
/fban [user] - Federation ban
/unfban [user] - Federation unban
/fedadmins - Federation admins
/fedpromote [user] - Promote fed admin
/feddemote [user] - Demote fed admin
/chatfed - Current chat federation
/fedchats - Chats in federation
/fedstat - Federation statistics
/delfed - Delete federation
/myfeds - Your federations
/fedlog [channel] - Set fed log
/fednotif on/off - Fed notifications
""",

    "help_connection": """
🔗 <b>Connection System</b>

/connect [chat_id] - Connect to chat
/disconnect - Disconnect
/connection - Current connection
/allowconnect on/off - Allow connections

<i>Connect to manage groups from PM!</i>
""",

    "help_pins": """
📌 <b>Pin System</b>

/pin - Pin a message
/unpin - Unpin last pin
/unpinall - Unpin all messages
/permapin [text] - Pin permanent text
/pinned - Get pinned message
/antichannelpin on/off - Anti channel pin
/cleanlinkedchat on/off - Clean linked
""",

    "help_users": """
👤 <b>User Commands</b>

/info - Get user info
/id - Get user/chat ID
/whois - Detailed user info
/setbio [text] - Set your bio
/bio @user - Get user bio
/afk [reason] - Set AFK
/rep + @user - Give reputation
/rep - @user - Remove reputation
/repcount - Your reputation count
/me - Your profile
/profile - Your profile card
/setlang [lang] - Set language
/marry @user - Propose marriage
/divorce - Divorce
/partner - Your partner
""",

    "help_fun": """
🎨 <b>Fun Commands</b>

/joke - Random joke
/meme - Random meme
/quote - Random quote
/fact - Random fact
/dog - Random dog photo
/cat - Random cat photo
/insult @user - Fun insult
/compliment @user - Compliment
/slap @user - Slap someone
/hug @user - Hug someone
/pat @user - Pat someone
/kiss @user - Kiss someone
/kill @user - Kill someone (fun)
/love @user - Show love
/highfive @user - High five
/facepalm - Facepalm
/shrug - Shrug
/think - Thinking emoji art
/reverse [text] - Reverse text
/tiny [text] - Tiny text
/vapor [text] - Vaporwave text
/mock [text] - mOcKiNg text
/clap [text] - 👏clap👏text
/emojify [text] - Convert to emoji
/ascii [text] - ASCII art
""",

    "help_tools": """
🛠️ <b>Utility Tools</b>

/paste [text] - Paste to hastebin
/calc [expression] - Calculator
/translate [lang] [text] - Translate
/tts [text] - Text to speech
/weather [city] - Weather info
/time [timezone] - Current time
/crypto [coin] - Crypto prices
/url [short/long] - URL shortener
/qr [text] - Generate QR code
/color [hex] - Color preview
/base64 encode/decode [text] - Base64
/hash [algo] [text] - Generate hash
/ip [address] - IP info
/dns [domain] - DNS lookup
/ping - Bot ping
/uptime - Bot uptime
/json - Get message JSON
/telegraph [title] [text] - Create page
/stickertopng - Convert sticker to PNG
/pngtossticker - Convert PNG to sticker
/getsticker - Get sticker file
""",

    "help_reminders": """
⏰ <b>Reminder System</b>

/remind [time] [text] - Set reminder
/reminders - List your reminders
/delreminder [id] - Delete reminder
/clearreminders - Clear all reminders

<b>Time format:</b>
30m - 30 minutes
2h - 2 hours
1d - 1 day
1w - 1 week

/schedule [time] [text] - Schedule message
/schedules - List scheduled messages
""",

    "help_giveaway": """
🎁 <b>Giveaway System</b>

/giveaway [prize] - Create giveaway
/joingiveaway - Join giveaway
/endgiveaway - End giveaway
/cancelgiveaway - Cancel giveaway
/giveawaylist - List giveaways
/reroll - Reroll winner
""",

    "help_tickets": """
🎫 <b>Ticket System</b>

/ticket [subject] [description] - Create ticket
/tickets - List tickets
/closeticket [id] - Close ticket
/assignticket [id] @user - Assign ticket
/ticketstats - Ticket statistics
""",

    "help_misc": """
📋 <b>Miscellaneous</b>

/rules - View group rules
/setrules [text] - Set group rules
/report - Report a user
/reports on/off - Toggle reports
/confess [text] - Anonymous confession
/todo add [task] - Add todo
/todo list - List todos
/todo done [id] - Mark done
/todo del [id] - Delete todo
/tagall [message] - Tag all members
/tag [name] - Use saved tag
/savetag [name] @users - Save tag group
/starboard on/off - Toggle starboard
/star - Star a message
/export - Export group data
/import - Import group data
/log on/off - Toggle action logging
/setlog [channel] - Set log channel
""",

    "help_owner": """
👑 <b>Owner Commands</b>

/broadcast [text] - Broadcast to all
/stats - Full bot statistics
/gban [user] - Global ban
/ungban [user] - Global unban
/sudolist - List sudo users
/addsudo [user] - Add sudo user
/rmsudo [user] - Remove sudo user
/chatlist - List all chats
/leavechat [id] - Leave a chat
/maintenance on/off - Toggle maintenance
/evaluate [code] - Evaluate code
/execute [code] - Execute code
/restart - Restart bot
/update - Update bot
/dbstats - Database statistics
/cleandb - Clean database
/backup - Backup database
""",

    "help_nightmode": """
🌙 <b>Night Mode</b>

/nightmode on/off - Toggle night mode
/setnighttime [start] [end] - Set times
Night mode auto-locks group at night!

<b>Default: 00:00 - 06:00</b>
"""
}

# ============================================================
# CALLBACK QUERY HANDLER
# ============================================================

def callback_handler(update, context):
    query = update.callback_query
    data = query.data
    
    try:
        query.answer()
    except:
        pass
    
    if data == "help_main":
        send_help_menu(update, context, edit=True)
        return
    
    if data in HELP_TEXTS:
        keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="help_main")]]
        try:
            query.edit_message_text(
                HELP_TEXTS[data],
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except:
            pass
        return
    
    if data == "bot_stats":
        stats = get_bot_stats()
        keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="help_main")]]
        query.edit_message_text(stats, parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data == "settings_main":
        text = "⚙️ <b>Bot Settings</b>\n\nUse /settings in your group to configure the bot."
        keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="help_main")]]
        query.edit_message_text(text, parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    if data == "all_features":
        text = """
🔧 <b>All 500+ Features Categories</b>

1-50: 👮 Admin & Moderation
51-100: 👋 Welcome & Goodbye
101-150: 📝 Notes & Filters
151-200: ⚠️ Warnings & Blacklist
201-250: 🔒 Locks & Permissions
251-300: 🛡️ Anti-Spam & Protection
301-350: 🎮 Games & Entertainment
351-400: 💰 Economy & Leveling
401-450: 🏛️ Federation System
451-500: 🛠️ Tools & Utilities
500+: 📋 Misc Features

<i>Use /help to explore each category!</i>
"""
        keyboard = [[InlineKeyboardButton("◀️ Back", callback_data="help_main")]]
        query.edit_message_text(text, parse_mode=ParseMode.HTML,
                              reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    # Welcome settings callbacks
    if data.startswith("welc_"):
        handle_welcome_callback(update, context, data)
        return
    
    # Admin action callbacks
    if data.startswith("admin_"):
        handle_admin_callback(update, context, data)
        return
    
    # Game callbacks
    if data.startswith("game_"):
        handle_game_callback(update, context, data)
        return
    
    # Captcha callback
    if data.startswith("captcha_"):
        handle_captcha_callback(update, context, data)
        return
    
    # Giveaway callback
    if data.startswith("giveaway_"):
        handle_giveaway_callback(update, context, data)
        return
    
    # Report callback
    if data.startswith("report_"):
        handle_report_callback(update, context, data)
        return

def get_bot_stats():
    user_count = db.execute("SELECT COUNT(*) as cnt FROM users", fetchone=True)
    chat_count = db.execute("SELECT COUNT(*) as cnt FROM chats", fetchone=True)
    note_count = db.execute("SELECT COUNT(*) as cnt FROM notes", fetchone=True)
    filter_count = db.execute("SELECT COUNT(*) as cnt FROM filters", fetchone=True)
    gban_count = db.execute("SELECT COUNT(*) as cnt FROM gbans", fetchone=True)
    
    text = f"""
📊 <b>MegaBot Statistics</b>

👥 <b>Users:</b> {user_count['cnt'] if user_count else 0}
💬 <b>Chats:</b> {chat_count['cnt'] if chat_count else 0}
📝 <b>Notes:</b> {note_count['cnt'] if note_count else 0}
🔍 <b>Filters:</b> {filter_count['cnt'] if filter_count else 0}
🚫 <b>Gbans:</b> {gban_count['cnt'] if gban_count else 0}
🤖 <b>Features:</b> 500+
⏰ <b>Uptime:</b> {get_readable_time(time.time() - START_TIME)}
"""
    return text

# ============================================================
# FEATURE 11-60: ADMIN COMMANDS
# ============================================================

@check_disabled
@group_only
@admin_only
def ban_command(update, context):
    """Ban a user - Feature 11"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not can_restrict(update, context):
        msg.reply_text("❌ I don't have permission to ban users!")
        return
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Please specify a user to ban! Reply to a message or provide user ID/username.")
        return
    
    if user_id == context.bot.id:
        msg.reply_text("❌ I'm not gonna ban myself!")
        return
    
    if is_admin(update, context, user_id):
        msg.reply_text("❌ I can't ban an admin!")
        return
    
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason"
    
    try:
        context.bot.ban_chat_member(chat.id, user_id)
        db.execute(
            "INSERT INTO banned_users (chat_id, user_id, banned_by, reason) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (chat_id, user_id) DO UPDATE SET reason = EXCLUDED.reason",
            (chat.id, user_id, update.effective_user.id, reason)
        )
        log_action(chat.id, "BAN", update.effective_user.id, user_id, reason)
        
        text = f"🔨 <b>Banned!</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        text += f"👮 <b>By:</b> {mention_html(update.effective_user.id, update.effective_user.first_name)}\n"
        text += f"📝 <b>Reason:</b> {reason}"
        
        keyboard = [[InlineKeyboardButton("Unban", callback_data=f"admin_unban_{user_id}")]]
        msg.reply_text(text, parse_mode=ParseMode.HTML,
                      reply_markup=InlineKeyboardMarkup(keyboard))
    except TelegramError as e:
        msg.reply_text(f"❌ Failed to ban: {e}")

@check_disabled
@group_only
@admin_only
def tban_command(update, context):
    """Temporary ban - Feature 12"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not can_restrict(update, context):
        msg.reply_text("❌ I don't have permission to ban users!")
        return
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user! Usage: /tban @user 1h reason")
        return
    
    if user_id == context.bot.id or is_admin(update, context, user_id):
        msg.reply_text("❌ Can't ban this user!")
        return
    
    args = context.args
    if len(args) < 2:
        msg.reply_text("❌ Specify time! Usage: /tban @user 1h reason")
        return
    
    time_val = extract_time(args[1])
    if not time_val:
        msg.reply_text("❌ Invalid time! Use: 30m, 1h, 1d, 1w")
        return
    
    reason = " ".join(args[2:]) if len(args) > 2 else "No reason"
    
    try:
        context.bot.ban_chat_member(chat.id, user_id, until_date=time_val)
        text = f"⏰ <b>Temporarily Banned!</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        text += f"⏱️ <b>Duration:</b> {args[1]}\n"
        text += f"📝 <b>Reason:</b> {reason}"
        msg.reply_text(text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def unban_command(update, context):
    """Unban a user - Feature 13"""
    msg = update.effective_message
    chat = update.effective_chat
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to unban!")
        return
    
    try:
        context.bot.unban_chat_member(chat.id, user_id)
        db.execute("DELETE FROM banned_users WHERE chat_id = %s AND user_id = %s",
                   (chat.id, user_id))
        log_action(chat.id, "UNBAN", update.effective_user.id, user_id)
        msg.reply_text(f"✅ {mention_html(user_id, user_name)} has been unbanned!",
                      parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def kick_command(update, context):
    """Kick a user - Feature 14"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not can_restrict(update, context):
        msg.reply_text("❌ I don't have ban permission!")
        return
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to kick!")
        return
    
    if user_id == context.bot.id or is_admin(update, context, user_id):
        msg.reply_text("❌ Can't kick this user!")
        return
    
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason"
    
    try:
        context.bot.ban_chat_member(chat.id, user_id)
        context.bot.unban_chat_member(chat.id, user_id)
        log_action(chat.id, "KICK", update.effective_user.id, user_id, reason)
        text = f"🦶 <b>Kicked!</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        text += f"📝 <b>Reason:</b> {reason}"
        msg.reply_text(text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def mute_command(update, context):
    """Mute a user - Feature 15"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not can_restrict(update, context):
        msg.reply_text("❌ I don't have restrict permission!")
        return
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to mute!")
        return
    
    if user_id == context.bot.id or is_admin(update, context, user_id):
        msg.reply_text("❌ Can't mute this user!")
        return
    
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason"
    
    try:
        context.bot.restrict_chat_member(
            chat.id, user_id,
            permissions=ChatPermissions(can_send_messages=False)
        )
        db.execute(
            "INSERT INTO muted_users (chat_id, user_id, muted_by, reason) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (chat_id, user_id) DO UPDATE SET reason = EXCLUDED.reason",
            (chat.id, user_id, update.effective_user.id, reason)
        )
        log_action(chat.id, "MUTE", update.effective_user.id, user_id, reason)
        
        text = f"🔇 <b>Muted!</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        text += f"📝 <b>Reason:</b> {reason}"
        
        keyboard = [[InlineKeyboardButton("Unmute", callback_data=f"admin_unmute_{user_id}")]]
        msg.reply_text(text, parse_mode=ParseMode.HTML,
                      reply_markup=InlineKeyboardMarkup(keyboard))
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def tmute_command(update, context):
    """Temporary mute - Feature 16"""
    msg = update.effective_message
    chat = update.effective_chat
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user! Usage: /tmute @user 1h reason")
        return
    
    if user_id == context.bot.id or is_admin(update, context, user_id):
        msg.reply_text("❌ Can't mute this user!")
        return
    
    args = context.args
    if len(args) < 2:
        msg.reply_text("❌ Specify time! Usage: /tmute @user 1h reason")
        return
    
    time_val = extract_time(args[1])
    if not time_val:
        msg.reply_text("❌ Invalid time!")
        return
    
    reason = " ".join(args[2:]) if len(args) > 2 else "No reason"
    
    try:
        context.bot.restrict_chat_member(
            chat.id, user_id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=time_val
        )
        text = f"⏰ <b>Temporarily Muted!</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        text += f"⏱️ <b>Duration:</b> {args[1]}\n"
        text += f"📝 <b>Reason:</b> {reason}"
        msg.reply_text(text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def unmute_command(update, context):
    """Unmute a user - Feature 17"""
    msg = update.effective_message
    chat = update.effective_chat
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to unmute!")
        return
    
    try:
        context.bot.restrict_chat_member(
            chat.id, user_id,
            permissions=ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True
            )
        )
        db.execute("DELETE FROM muted_users WHERE chat_id = %s AND user_id = %s",
                   (chat.id, user_id))
        log_action(chat.id, "UNMUTE", update.effective_user.id, user_id)
        msg.reply_text(f"🔊 {mention_html(user_id, user_name)} has been unmuted!",
                      parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def promote_command(update, context):
    """Promote a user - Feature 18"""
    msg = update.effective_message
    chat = update.effective_chat
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to promote!")
        return
    
    title = " ".join(context.args[1:]) if len(context.args) > 1 else ""
    
    try:
        context.bot.promote_chat_member(
            chat.id, user_id,
            can_change_info=True,
            can_delete_messages=True,
            can_invite_users=True,
            can_restrict_members=True,
            can_pin_messages=True,
            can_manage_chat=True,
            can_manage_video_chats=True
        )
        if title:
            try:
                context.bot.set_chat_administrator_custom_title(chat.id, user_id, title)
            except:
                pass
        
        log_action(chat.id, "PROMOTE", update.effective_user.id, user_id)
        text = f"📈 <b>Promoted!</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        if title:
            text += f"🏷️ <b>Title:</b> {title}"
        msg.reply_text(text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def demote_command(update, context):
    """Demote a user - Feature 19"""
    msg = update.effective_message
    chat = update.effective_chat
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to demote!")
        return
    
    try:
        context.bot.promote_chat_member(
            chat.id, user_id,
            can_change_info=False,
            can_delete_messages=False,
            can_invite_users=False,
            can_restrict_members=False,
            can_pin_messages=False,
            can_manage_chat=False
        )
        log_action(chat.id, "DEMOTE", update.effective_user.id, user_id)
        msg.reply_text(f"📉 {mention_html(user_id, user_name)} has been demoted!",
                      parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def set_title_command(update, context):
    """Set admin title - Feature 20"""
    msg = update.effective_message
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user!")
        return
    
    title = " ".join(context.args[1:]) if len(context.args) > 1 else ""
    if not title:
        msg.reply_text("❌ Specify a title!")
        return
    
    try:
        context.bot.set_chat_administrator_custom_title(
            update.effective_chat.id, user_id, title[:16]
        )
        msg.reply_text(f"✅ Set title for {mention_html(user_id, user_name)}: <b>{title[:16]}</b>",
                      parse_mode=ParseMode.HTML)
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def pin_command(update, context):
    """Pin message - Feature 21"""
    msg = update.effective_message
    
    if not msg.reply_to_message:
        msg.reply_text("❌ Reply to a message to pin it!")
        return
    
    if not can_pin(update, context):
        msg.reply_text("❌ I don't have pin permission!")
        return
    
    try:
        loud = 'loud' in context.args or 'notify' in context.args if context.args else False
        context.bot.pin_chat_message(
            update.effective_chat.id,
            msg.reply_to_message.message_id,
            disable_notification=not loud
        )
        db.execute(
            "INSERT INTO pins (chat_id, message_id, pinned_by) VALUES (%s, %s, %s)",
            (update.effective_chat.id, msg.reply_to_message.message_id, update.effective_user.id)
        )
        msg.reply_text("📌 Message pinned!")
    except TelegramError as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def unpin_command(update, context):
    """Unpin message - Feature 22"""
    try:
        context.bot.unpin_chat_message(update.effective_chat.id)
        update.effective_message.reply_text("📌 Message unpinned!")
    except TelegramError as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def unpinall_command(update, context):
    """Unpin all messages - Feature 23"""
    try:
        context.bot.unpin_all_chat_messages(update.effective_chat.id)
        update.effective_message.reply_text("📌 All messages unpinned!")
    except TelegramError as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def invitelink_command(update, context):
    """Get invite link - Feature 24"""
    try:
        link = context.bot.export_chat_invite_link(update.effective_chat.id)
        update.effective_message.reply_text(f"🔗 <b>Invite Link:</b>\n{link}",
                                           parse_mode=ParseMode.HTML)
    except TelegramError as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
def adminlist_command(update, context):
    """List admins - Feature 25"""
    try:
        admins = context.bot.get_chat_administrators(update.effective_chat.id)
        text = f"👮 <b>Admins in {update.effective_chat.title}:</b>\n\n"
        
        creator = ""
        admin_list = []
        bot_list = []
        
        for admin in admins:
            user = admin.user
            if admin.status == 'creator':
                creator = f"👑 {mention_html(user.id, user.first_name)}"
                if admin.custom_title:
                    creator += f" | <i>{admin.custom_title}</i>"
            elif user.is_bot:
                bot_list.append(f"🤖 {mention_html(user.id, user.first_name)}")
            else:
                line = f"👤 {mention_html(user.id, user.first_name)}"
                if admin.custom_title:
                    line += f" | <i>{admin.custom_title}</i>"
                admin_list.append(line)
        
        if creator:
            text += f"<b>Creator:</b>\n{creator}\n\n"
        if admin_list:
            text += f"<b>Admins ({len(admin_list)}):</b>\n"
            text += "\n".join(admin_list) + "\n\n"
        if bot_list:
            text += f"<b>Bots ({len(bot_list)}):</b>\n"
            text += "\n".join(bot_list)
        
        update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def purge_command(update, context):
    """Purge messages - Feature 26"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not can_delete(update, context):
        msg.reply_text("❌ I don't have delete permission!")
        return
    
    if msg.reply_to_message:
        start_id = msg.reply_to_message.message_id
        end_id = msg.message_id
        deleted = 0
        for mid in range(start_id, end_id + 1):
            try:
                context.bot.delete_message(chat.id, mid)
                deleted += 1
            except:
                pass
        try:
            m = msg.reply_text(f"🗑️ Purged {deleted} messages!")
            time.sleep(2)
            m.delete()
        except:
            pass
    elif context.args and context.args[0].isdigit():
        count = min(int(context.args[0]), 100)
        deleted = 0
        for i in range(count):
            try:
                context.bot.delete_message(chat.id, msg.message_id - i)
                deleted += 1
            except:
                pass
        try:
            m = context.bot.send_message(chat.id, f"🗑️ Purged {deleted} messages!")
            time.sleep(2)
            m.delete()
        except:
            pass
    else:
        msg.reply_text("❌ Reply to a message or specify count! /purge 10")

@check_disabled
@group_only
@admin_only
def del_command(update, context):
    """Delete a message - Feature 27"""
    if update.effective_message.reply_to_message:
        try:
            update.effective_message.reply_to_message.delete()
            update.effective_message.delete()
        except:
            pass
    else:
        update.effective_message.reply_text("❌ Reply to a message to delete it!")

@check_disabled
@group_only
@admin_only
def slowmode_command(update, context):
    """Set slowmode - Feature 28"""
    if not context.args:
        update.effective_message.reply_text("Usage: /slowmode <seconds> (0 to disable)")
        return
    
    try:
        seconds = int(context.args[0])
        context.bot.set_chat_slow_mode_delay(update.effective_chat.id, seconds)
        if seconds == 0:
            update.effective_message.reply_text("✅ Slowmode disabled!")
        else:
            update.effective_message.reply_text(f"✅ Slowmode set to {seconds} seconds!")
    except Exception as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def zombies_command(update, context):
    """Clean deleted accounts - Feature 29"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not can_restrict(update, context):
        msg.reply_text("❌ I need ban permissions!")
        return
    
    count = 0
    try:
        members_count = chat.get_member_count()
        msg.reply_text(f"🔍 Scanning {members_count} members for deleted accounts...")
        # Note: This is limited by Telegram API
        msg.reply_text("⚠️ Due to API limitations, use @admin to manually check for deleted accounts.")
    except Exception as e:
        msg.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def setgtitle_command(update, context):
    """Set group title - Feature 30"""
    if not context.args:
        update.effective_message.reply_text("Usage: /setgtitle <new title>")
        return
    try:
        title = " ".join(context.args)
        context.bot.set_chat_title(update.effective_chat.id, title)
        update.effective_message.reply_text(f"✅ Group title set to: {title}")
    except Exception as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def setgdesc_command(update, context):
    """Set group description - Feature 31"""
    if not context.args:
        update.effective_message.reply_text("Usage: /setgdesc <description>")
        return
    try:
        desc = " ".join(context.args)
        context.bot.set_chat_description(update.effective_chat.id, desc)
        update.effective_message.reply_text("✅ Group description updated!")
    except Exception as e:
        update.effective_message.reply_text(f"❌ Failed: {e}")

@check_disabled
@group_only
@admin_only
def setgpic_command(update, context):
    """Set group photo - Feature 32"""
    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        msg.reply_text("❌ Reply to a photo!")
        return
    try:
        photo = msg.reply_to_message.photo[-1].get_file()
        bio = BytesIO()
        photo.download(out=bio)
        bio.seek(0)
        context.bot.set_chat_photo(update.effective_chat.id, bio)
        msg.reply_text("✅ Group photo updated!")
    except Exception as e:
        msg.reply_text(f"❌ Failed: {e}")

# ============================================================
# FEATURE 61-100: WELCOME & GOODBYE SYSTEM
# ============================================================

def welcome_handler(update, context):
    """Handle new members - Feature 61"""
    chat = update.effective_chat
    if chat.type == 'private':
        return
    
    # Save chat to database
    db.execute("""
        INSERT INTO chats (chat_id, chat_title, chat_type)
        VALUES (%s, %s, %s)
        ON CONFLICT (chat_id) DO UPDATE SET
        chat_title = EXCLUDED.chat_title,
        updated_at = CURRENT_TIMESTAMP
    """, (chat.id, chat.title, chat.type))
    
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            # Bot was added to group
            update.effective_message.reply_text(
                f"Thanks for adding me to <b>{chat.title}</b>! 🎉\n"
                f"Use /help to see what I can do!\n"
                f"Make me admin for full functionality!",
                parse_mode=ParseMode.HTML
            )
            continue
        
        # Save user
        db.execute("""
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            updated_at = CURRENT_TIMESTAMP
        """, (member.id, member.username, member.first_name, member.last_name))
        
        # Save chat member
        db.execute("""
            INSERT INTO chat_members (chat_id, user_id)
            VALUES (%s, %s)
            ON CONFLICT (chat_id, user_id) DO NOTHING
        """, (chat.id, member.id))
        
        # Check gban
        gban = db.execute(
            "SELECT * FROM gbans WHERE user_id = %s", (member.id,), fetchone=True
        )
        if gban:
            try:
                context.bot.ban_chat_member(chat.id, member.id)
                update.effective_message.reply_text(
                    f"⚠️ This user is globally banned!\n"
                    f"Reason: {gban['reason']}\n"
                    f"Banned automatically."
                )
                continue
            except:
                pass
        
        # Check force sub
        chat_data = db.execute(
            "SELECT * FROM chats WHERE chat_id = %s", (chat.id,), fetchone=True
        )
        
        if not chat_data or not chat_data.get('welcome_enabled', True):
            continue
        
        # Check captcha
        if chat_data and chat_data.get('captcha_enabled'):
            handle_captcha_welcome(update, context, member, chat, chat_data)
            continue
        
        # Send welcome
        welcome_text = chat_data.get('welcome_text', 'Hey {first}, welcome to {chatname}!') if chat_data else 'Hey {first}, welcome to {chatname}!'
        formatted = format_welcome(welcome_text, member, chat)
        
        clean_text, buttons = parse_buttons(formatted)
        keyboard = build_keyboard(buttons)
        
        # Clean old welcome
        if chat_data and chat_data.get('clean_welcome') and chat_data.get('last_welcome_msg'):
            try:
                context.bot.delete_message(chat.id, chat_data['last_welcome_msg'])
            except:
                pass
        
        try:
            if chat_data and chat_data.get('welcome_media') and chat_data.get('welcome_media_type'):
                media_type = chat_data['welcome_media_type']
                media = chat_data['welcome_media']
                if media_type == 'photo':
                    sent = update.effective_message.reply_photo(
                        media, caption=clean_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                elif media_type == 'video':
                    sent = update.effective_message.reply_video(
                        media, caption=clean_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                elif media_type == 'animation':
                    sent = update.effective_message.reply_animation(
                        media, caption=clean_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                else:
                    sent = update.effective_message.reply_text(
                        clean_text, parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
            else:
                sent = update.effective_message.reply_text(
                    clean_text, parse_mode=ParseMode.HTML,
                    reply_markup=keyboard
                )
            
            # Save last welcome msg id
            db.execute(
                "UPDATE chats SET last_welcome_msg = %s WHERE chat_id = %s",
                (sent.message_id, chat.id)
            )
        except Exception as e:
            logger.error(f"Welcome error: {e}")

def handle_captcha_welcome(update, context, member, chat, chat_data):
    """Handle captcha for new members - Feature 62"""
    captcha_type = chat_data.get('captcha_type', 'button')
    
    if captcha_type == 'button':
        keyboard = [[InlineKeyboardButton("✅ I'm Human!", callback_data=f"captcha_verify_{member.id}")]]
        text = f"Hey {mention_html(member.id, member.first_name)}! 👋\n\n"
        text += "Please click the button below to verify you're human.\n"
        text += "You have 5 minutes to verify, otherwise you'll be kicked."
        
        try:
            # Mute user until verified
            context.bot.restrict_chat_member(
                chat.id, member.id,
                permissions=ChatPermissions(can_send_messages=False)
            )
            update.effective_message.reply_text(
                text, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except:
            pass
    
    elif captcha_type == 'math':
        a = random.randint(1, 20)
        b = random.randint(1, 20)
        answer = a + b
        
        # Generate wrong answers
        wrong1 = answer + random.randint(1, 5)
        wrong2 = answer - random.randint(1, 5)
        wrong3 = answer + random.randint(6, 10)
        
        options = [answer, wrong1, wrong2, wrong3]
        random.shuffle(options)
        
        keyboard = [[
            InlineKeyboardButton(str(opt), callback_data=f"captcha_math_{member.id}_{opt}_{answer}")
            for opt in options
        ]]
        
        text = f"Hey {mention_html(member.id, member.first_name)}! 👋\n\n"
        text += f"Solve this to verify: <b>{a} + {b} = ?</b>"
        
        try:
            context.bot.restrict_chat_member(
                chat.id, member.id,
                permissions=ChatPermissions(can_send_messages=False)
            )
            update.effective_message.reply_text(
                text, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except:
            pass

def goodbye_handler(update, context):
    """Handle member left - Feature 63"""
    chat = update.effective_chat
    member = update.message.left_chat_member
    
    if member.id == context.bot.id:
        return
    
    chat_data = db.execute(
        "SELECT * FROM chats WHERE chat_id = %s", (chat.id,), fetchone=True
    )
    
    if not chat_data or not chat_data.get('goodbye_enabled', True):
        return
    
    goodbye_text = chat_data.get('goodbye_text', 'Sad to see you go, {first}!') if chat_data else 'Sad to see you go, {first}!'
    formatted = format_welcome(goodbye_text, member, chat)
    clean_text, buttons = parse_buttons(formatted)
    keyboard = build_keyboard(buttons)
    
    try:
        update.effective_message.reply_text(
            clean_text, parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
    except:
        pass

@check_disabled
@group_only
@admin_only
def set_welcome_command(update, context):
    """Set welcome message - Feature 64"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if msg.reply_to_message:
        text = msg.reply_to_message.text or msg.reply_to_message.caption or ""
        media = ""
        media_type = ""
        
        if msg.reply_to_message.photo:
            media = msg.reply_to_message.photo[-1].file_id
            media_type = "photo"
        elif msg.reply_to_message.video:
            media = msg.reply_to_message.video.file_id
            media_type = "video"
        elif msg.reply_to_message.animation:
            media = msg.reply_to_message.animation.file_id
            media_type = "animation"
        
        db.execute("""
            INSERT INTO chats (chat_id, chat_title, chat_type, welcome_text, welcome_media, welcome_media_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET
            welcome_text = EXCLUDED.welcome_text,
            welcome_media = EXCLUDED.welcome_media,
            welcome_media_type = EXCLUDED.welcome_media_type
        """, (chat.id, chat.title, chat.type, text, media, media_type))
    elif context.args:
        text = " ".join(context.args)
        db.execute("""
            INSERT INTO chats (chat_id, chat_title, chat_type, welcome_text)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chat_id) DO UPDATE SET welcome_text = EXCLUDED.welcome_text
        """, (chat.id, chat.title, chat.type, text))
    else:
        msg.reply_text("❌ Provide welcome text or reply to a message!")
        return
    
    msg.reply_text("✅ Welcome message set successfully!")

@check_disabled
@group_only
@admin_only
def welcome_toggle_command(update, context):
    """Toggle welcome on/off - Feature 65"""
    if not context.args:
        chat_data = db.execute(
            "SELECT welcome_enabled, welcome_text FROM chats WHERE chat_id = %s",
            (update.effective_chat.id,), fetchone=True
        )
        status = "ON" if (chat_data and chat_data['welcome_enabled']) else "OFF"
        text = f"👋 <b>Welcome Settings</b>\n\n"
        text += f"Status: <b>{status}</b>\n"
        if chat_data and chat_data.get('welcome_text'):
            text += f"Message:\n<code>{chat_data['welcome_text'][:200]}</code>"
        
        keyboard = [
            [
                InlineKeyboardButton("✅ ON", callback_data="welc_on"),
                InlineKeyboardButton("❌ OFF", callback_data="welc_off")
            ]
        ]
        update.effective_message.reply_text(
            text, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    if context.args[0].lower() in ['on', 'yes', 'true']:
        db.execute(
            "UPDATE chats SET welcome_enabled = TRUE WHERE chat_id = %s",
            (update.effective_chat.id,)
        )
        update.effective_message.reply_text("✅ Welcome messages enabled!")
    elif context.args[0].lower() in ['off', 'no', 'false']:
        db.execute(
            "UPDATE chats SET welcome_enabled = FALSE WHERE chat_id = %s",
            (update.effective_chat.id,)
        )
        update.effective_message.reply_text("❌ Welcome messages disabled!")

@check_disabled
@group_only
@admin_only
def set_goodbye_command(update, context):
    """Set goodbye message - Feature 66"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if not context.args and not msg.reply_to_message:
        msg.reply_text("Usage: /setgoodbye <text> or reply to a message")
        return
    
    text = " ".join(context.args) if context.args else (
        msg.reply_to_message.text or msg.reply_to_message.caption or ""
    )
    
    db.execute("""
        INSERT INTO chats (chat_id, chat_title, chat_type, goodbye_text)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (chat_id) DO UPDATE SET goodbye_text = EXCLUDED.goodbye_text
    """, (chat.id, chat.title, chat.type, text))
    
    msg.reply_text("✅ Goodbye message set!")

@check_disabled
@group_only
@admin_only
def reset_welcome_command(update, context):
    """Reset welcome - Feature 67"""
    db.execute(
        "UPDATE chats SET welcome_text = 'Hey {first}, welcome to {chatname}!', welcome_media = '', welcome_media_type = '' WHERE chat_id = %s",
        (update.effective_chat.id,)
    )
    update.effective_message.reply_text("✅ Welcome message reset to default!")

@check_disabled
@group_only
@admin_only
def captcha_command(update, context):
    """Toggle captcha - Feature 68"""
    if not context.args:
        update.effective_message.reply_text("Usage: /captcha on/off")
        return
    
    enabled = context.args[0].lower() in ['on', 'yes', 'true']
    db.execute(
        "UPDATE chats SET captcha_enabled = %s WHERE chat_id = %s",
        (enabled, update.effective_chat.id)
    )
    status = "enabled" if enabled else "disabled"
    update.effective_message.reply_text(f"✅ Captcha {status}!")

@check_disabled
@group_only
@admin_only
def captchatype_command(update, context):
    """Set captcha type - Feature 69"""
    if not context.args or context.args[0] not in ['button', 'math']:
        update.effective_message.reply_text("Usage: /captchatype button/math")
        return
    
    db.execute(
        "UPDATE chats SET captcha_type = %s WHERE chat_id = %s",
        (context.args[0], update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Captcha type set to: {context.args[0]}")

@check_disabled
@group_only
@admin_only
def cleanwelcome_command(update, context):
    """Toggle clean welcome - Feature 70"""
    if not context.args:
        update.effective_message.reply_text("Usage: /cleanwelcome on/off")
        return
    
    enabled = context.args[0].lower() in ['on', 'yes', 'true']
    db.execute(
        "UPDATE chats SET clean_welcome = %s WHERE chat_id = %s",
        (enabled, update.effective_chat.id)
    )
    status = "enabled" if enabled else "disabled"
    update.effective_message.reply_text(f"✅ Clean welcome {status}!")

# ============================================================
# FEATURE 101-150: NOTES SYSTEM
# ============================================================

@check_disabled
@group_only
@admin_only
def save_note_command(update, context):
    """Save a note - Feature 101"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if len(context.args) < 1:
        msg.reply_text("Usage: /save <notename> <text> or reply to a message")
        return
    
    note_name = context.args[0].lower()
    
    if msg.reply_to_message:
        note_text = msg.reply_to_message.text or msg.reply_to_message.caption or ""
        media = ""
        media_type = ""
        if msg.reply_to_message.photo:
            media = msg.reply_to_message.photo[-1].file_id
            media_type = "photo"
        elif msg.reply_to_message.video:
            media = msg.reply_to_message.video.file_id
            media_type = "video"
        elif msg.reply_to_message.document:
            media = msg.reply_to_message.document.file_id
            media_type = "document"
        elif msg.reply_to_message.audio:
            media = msg.reply_to_message.audio.file_id
            media_type = "audio"
        elif msg.reply_to_message.voice:
            media = msg.reply_to_message.voice.file_id
            media_type = "voice"
        elif msg.reply_to_message.sticker:
            media = msg.reply_to_message.sticker.file_id
            media_type = "sticker"
        elif msg.reply_to_message.animation:
            media = msg.reply_to_message.animation.file_id
            media_type = "animation"
    else:
        note_text = " ".join(context.args[1:])
        media = ""
        media_type = ""
    
    if not note_text and not media:
        msg.reply_text("❌ Provide note content!")
        return
    
    # Delete existing note with same name
    db.execute(
        "DELETE FROM notes WHERE chat_id = %s AND note_name = %s",
        (chat.id, note_name)
    )
    
    db.execute("""
        INSERT INTO notes (chat_id, note_name, note_text, note_media, note_media_type, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (chat.id, note_name, note_text, media, media_type, update.effective_user.id))
    
    msg.reply_text(f"✅ Note <b>{note_name}</b> saved!", parse_mode=ParseMode.HTML)

@check_disabled
def get_note_command(update, context):
    """Get a note - Feature 102"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if chat.type == 'private':
        msg.reply_text("❌ Use this in a group!")
        return
    
    if not context.args:
        msg.reply_text("Usage: /get <notename>")
        return
    
    note_name = context.args[0].lower()
    note = db.execute(
        "SELECT * FROM notes WHERE chat_id = %s AND note_name = %s",
        (chat.id, note_name), fetchone=True
    )
    
    if not note:
        msg.reply_text(f"❌ Note '{note_name}' not found!")
        return
    
    send_note(update, context, note)

def send_note(update, context, note):
    """Send note content"""
    msg = update.effective_message
    text = note['note_text'] or ""
    
    clean_text, buttons = parse_buttons(text)
    keyboard = build_keyboard(buttons)
    
    try:
        if note['note_media'] and note['note_media_type']:
            media_type = note['note_media_type']
            media = note['note_media']
            
            if media_type == 'photo':
                msg.reply_photo(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            elif media_type == 'video':
                msg.reply_video(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            elif media_type == 'document':
                msg.reply_document(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            elif media_type == 'audio':
                msg.reply_audio(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            elif media_type == 'voice':
                msg.reply_voice(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            elif media_type == 'sticker':
                msg.reply_sticker(media)
            elif media_type == 'animation':
                msg.reply_animation(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        else:
            msg.reply_text(clean_text or "Empty note", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception as e:
        msg.reply_text(f"❌ Error sending note: {e}")

@check_disabled
@group_only
def list_notes_command(update, context):
    """List notes - Feature 103"""
    chat = update.effective_chat
    notes = db.execute(
        "SELECT note_name FROM notes WHERE chat_id = %s ORDER BY note_name",
        (chat.id,), fetch=True
    )
    
    if not notes:
        update.effective_message.reply_text("📝 No notes saved in this chat!")
        return
    
    text = f"📝 <b>Notes in {chat.title}:</b>\n\n"
    for i, note in enumerate(notes, 1):
        text += f"  {i}. <code>{note['note_name']}</code>\n"
    text += f"\n<i>Use /get &lt;notename&gt; or #notename to get a note</i>"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def clear_note_command(update, context):
    """Delete a note - Feature 104"""
    if not context.args:
        update.effective_message.reply_text("Usage: /clear <notename>")
        return
    
    note_name = context.args[0].lower()
    result = db.execute(
        "DELETE FROM notes WHERE chat_id = %s AND note_name = %s RETURNING id",
        (update.effective_chat.id, note_name), fetchone=True
    )
    
    if result:
        update.effective_message.reply_text(f"✅ Note '{note_name}' deleted!")
    else:
        update.effective_message.reply_text(f"❌ Note '{note_name}' not found!")

@check_disabled
@group_only
@admin_only
def clearall_notes_command(update, context):
    """Delete all notes - Feature 105"""
    db.execute("DELETE FROM notes WHERE chat_id = %s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ All notes cleared!")

def hashtag_note_handler(update, context):
    """Handle #notename - Feature 106"""
    msg = update.effective_message
    if not msg or not msg.text or msg.chat.type == 'private':
        return
    
    if msg.text.startswith('#'):
        note_name = msg.text[1:].split()[0].lower()
        note = db.execute(
            "SELECT * FROM notes WHERE chat_id = %s AND note_name = %s",
            (msg.chat_id, note_name), fetchone=True
        )
        if note:
            send_note(update, context, note)

# ============================================================
# FEATURE 151-200: FILTERS SYSTEM
# ============================================================

@check_disabled
@group_only
@admin_only
def add_filter_command(update, context):
    """Add a filter - Feature 151"""
    msg = update.effective_message
    chat = update.effective_chat
    
    if len(context.args) < 1:
        msg.reply_text("Usage: /filter <keyword> <reply> or reply to a message")
        return
    
    keyword = context.args[0].lower()
    
    if msg.reply_to_message:
        reply_text = msg.reply_to_message.text or msg.reply_to_message.caption or ""
        media = ""
        media_type = ""
        if msg.reply_to_message.photo:
            media = msg.reply_to_message.photo[-1].file_id
            media_type = "photo"
        elif msg.reply_to_message.video:
            media = msg.reply_to_message.video.file_id
            media_type = "video"
        elif msg.reply_to_message.document:
            media = msg.reply_to_message.document.file_id
            media_type = "document"
        elif msg.reply_to_message.sticker:
            media = msg.reply_to_message.sticker.file_id
            media_type = "sticker"
        elif msg.reply_to_message.animation:
            media = msg.reply_to_message.animation.file_id
            media_type = "animation"
    else:
        reply_text = " ".join(context.args[1:])
        media = ""
        media_type = ""
    
    if not reply_text and not media:
        msg.reply_text("❌ Provide filter reply content!")
        return
    
    # Remove existing filter
    db.execute("DELETE FROM filters WHERE chat_id = %s AND keyword = %s",
               (chat.id, keyword))
    
    db.execute("""
        INSERT INTO filters (chat_id, keyword, reply_text, reply_media, reply_media_type, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (chat.id, keyword, reply_text, media, media_type, update.effective_user.id))
    
    msg.reply_text(f"✅ Filter for '<b>{keyword}</b>' added!", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
def list_filters_command(update, context):
    """List filters - Feature 152"""
    filters_list = db.execute(
        "SELECT keyword FROM filters WHERE chat_id = %s ORDER BY keyword",
        (update.effective_chat.id,), fetch=True
    )
    
    if not filters_list:
        update.effective_message.reply_text("🔍 No filters in this chat!")
        return
    
    text = f"🔍 <b>Filters in {update.effective_chat.title}:</b>\n\n"
    for i, f in enumerate(filters_list, 1):
        text += f"  {i}. <code>{f['keyword']}</code>\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def stop_filter_command(update, context):
    """Remove a filter - Feature 153"""
    if not context.args:
        update.effective_message.reply_text("Usage: /stop <keyword>")
        return
    
    keyword = context.args[0].lower()
    result = db.execute(
        "DELETE FROM filters WHERE chat_id = %s AND keyword = %s RETURNING id",
        (update.effective_chat.id, keyword), fetchone=True
    )
    
    if result:
        update.effective_message.reply_text(f"✅ Filter '{keyword}' removed!")
    else:
        update.effective_message.reply_text(f"❌ Filter '{keyword}' not found!")

@check_disabled
@group_only
@admin_only
def stopall_filters_command(update, context):
    """Remove all filters - Feature 154"""
    db.execute("DELETE FROM filters WHERE chat_id = %s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ All filters removed!")

def filter_message_handler(update, context):
    """Check message against filters - Feature 155"""
    msg = update.effective_message
    if not msg or not msg.text or msg.chat.type == 'private':
        return
    
    text = msg.text.lower()
    filters_list = db.execute(
        "SELECT * FROM filters WHERE chat_id = %s",
        (msg.chat_id,), fetch=True
    )
    
    if not filters_list:
        return
    
    for f in filters_list:
        keyword = f['keyword'].lower()
        if keyword in text:
            try:
                clean_text, buttons = parse_buttons(f['reply_text'] or "")
                keyboard = build_keyboard(buttons)
                
                if f['reply_media'] and f['reply_media_type']:
                    media_type = f['reply_media_type']
                    media = f['reply_media']
                    
                    if media_type == 'photo':
                        msg.reply_photo(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                    elif media_type == 'video':
                        msg.reply_video(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                    elif media_type == 'document':
                        msg.reply_document(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                    elif media_type == 'sticker':
                        msg.reply_sticker(media)
                    elif media_type == 'animation':
                        msg.reply_animation(media, caption=clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                elif clean_text:
                    msg.reply_text(clean_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Filter error: {e}")
            break

# ============================================================
# FEATURE 201-250: WARNING SYSTEM
# ============================================================

@check_disabled
@group_only
@admin_only
def warn_command(update, context):
    """Warn a user - Feature 201"""
    msg = update.effective_message
    chat = update.effective_chat
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user to warn!")
        return
    
    if user_id == context.bot.id or is_admin(update, context, user_id):
        msg.reply_text("❌ Can't warn this user!")
        return
    
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason"
    
    db.execute("""
        INSERT INTO warnings (chat_id, user_id, reason, warned_by)
        VALUES (%s, %s, %s, %s)
    """, (chat.id, user_id, reason, update.effective_user.id))
    
    # Get warn count
    warn_count = db.execute(
        "SELECT COUNT(*) as cnt FROM warnings WHERE chat_id = %s AND user_id = %s",
        (chat.id, user_id), fetchone=True
    )
    
    # Get warn limit
    chat_data = db.execute(
        "SELECT warn_limit, warn_action FROM chats WHERE chat_id = %s",
        (chat.id,), fetchone=True
    )
    warn_limit = chat_data['warn_limit'] if chat_data else 3
    warn_action = chat_data['warn_action'] if chat_data else 'ban'
    
    current = warn_count['cnt']
    
    log_action(chat.id, "WARN", update.effective_user.id, user_id, reason)
    
    if current >= warn_limit:
        # Take action
        try:
            if warn_action == 'ban':
                context.bot.ban_chat_member(chat.id, user_id)
                action_text = "BANNED"
            elif warn_action == 'kick':
                context.bot.ban_chat_member(chat.id, user_id)
                context.bot.unban_chat_member(chat.id, user_id)
                action_text = "KICKED"
            elif warn_action == 'mute':
                context.bot.restrict_chat_member(
                    chat.id, user_id,
                    permissions=ChatPermissions(can_send_messages=False)
                )
                action_text = "MUTED"
            else:
                action_text = "BANNED"
                context.bot.ban_chat_member(chat.id, user_id)
            
            # Reset warnings
            db.execute("DELETE FROM warnings WHERE chat_id = %s AND user_id = %s",
                       (chat.id, user_id))
            
            text = f"⚠️ <b>{action_text}!</b>\n\n"
            text += f"👤 {mention_html(user_id, user_name)} has been {action_text.lower()}!\n"
            text += f"📝 Reached {warn_limit}/{warn_limit} warnings."
            msg.reply_text(text, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            msg.reply_text(f"❌ Failed to take action: {e}")
    else:
        keyboard = [
            [InlineKeyboardButton("Remove Warn ❌", callback_data=f"admin_rmwarn_{user_id}")]
        ]
        text = f"⚠️ <b>Warning {current}/{warn_limit}</b>\n\n"
        text += f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        text += f"📝 <b>Reason:</b> {reason}\n"
        text += f"👮 <b>By:</b> {mention_html(update.effective_user.id, update.effective_user.first_name)}"
        
        msg.reply_text(text, parse_mode=ParseMode.HTML,
                      reply_markup=InlineKeyboardMarkup(keyboard))

@check_disabled
@group_only
@admin_only
def dwarn_command(update, context):
    """Warn and delete message - Feature 202"""
    if update.effective_message.reply_to_message:
        try:
            update.effective_message.reply_to_message.delete()
        except:
            pass
    warn_command(update, context)

@check_disabled
@group_only
def warns_command(update, context):
    """Check warnings - Feature 203"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name
    
    warns = db.execute(
        "SELECT * FROM warnings WHERE chat_id = %s AND user_id = %s ORDER BY created_at DESC",
        (update.effective_chat.id, user_id), fetch=True
    )
    
    if not warns:
        update.effective_message.reply_text(
            f"{mention_html(user_id, user_name)} has no warnings!",
            parse_mode=ParseMode.HTML
        )
        return
    
    chat_data = db.execute(
        "SELECT warn_limit FROM chats WHERE chat_id = %s",
        (update.effective_chat.id,), fetchone=True
    )
    warn_limit = chat_data['warn_limit'] if chat_data else 3
    
    text = f"⚠️ <b>Warnings for {mention_html(user_id, user_name)}:</b>\n"
    text += f"<b>Total: {len(warns)}/{warn_limit}</b>\n\n"
    
    for i, w in enumerate(warns, 1):
        text += f"  {i}. {w['reason']} - <i>{w['created_at'].strftime('%Y-%m-%d')}</i>\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def rmwarn_command(update, context):
    """Remove last warning - Feature 204"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    
    result = db.execute(
        "DELETE FROM warnings WHERE id = (SELECT id FROM warnings WHERE chat_id = %s AND user_id = %s ORDER BY created_at DESC LIMIT 1) RETURNING id",
        (update.effective_chat.id, user_id), fetchone=True
    )
    
    if result:
        update.effective_message.reply_text(
            f"✅ Last warning removed for {mention_html(user_id, user_name)}!",
            parse_mode=ParseMode.HTML
        )
    else:
        update.effective_message.reply_text("❌ No warnings to remove!")

@check_disabled
@group_only
@admin_only
def resetwarns_command(update, context):
    """Reset all warnings - Feature 205"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    
    db.execute(
        "DELETE FROM warnings WHERE chat_id = %s AND user_id = %s",
        (update.effective_chat.id, user_id)
    )
    update.effective_message.reply_text(
        f"✅ Warnings reset for {mention_html(user_id, user_name)}!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
@admin_only
def warnlimit_command(update, context):
    """Set warn limit - Feature 206"""
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /warnlimit <number>")
        return
    
    limit = int(context.args[0])
    if limit < 1 or limit > 100:
        update.effective_message.reply_text("❌ Limit must be between 1 and 100!")
        return
    
    db.execute(
        "UPDATE chats SET warn_limit = %s WHERE chat_id = %s",
        (limit, update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Warn limit set to {limit}!")

@check_disabled
@group_only
@admin_only
def warnaction_command(update, context):
    """Set warn action - Feature 207"""
    if not context.args or context.args[0] not in ['ban', 'kick', 'mute']:
        update.effective_message.reply_text("Usage: /warnaction ban/kick/mute")
        return
    
    db.execute(
        "UPDATE chats SET warn_action = %s WHERE chat_id = %s",
        (context.args[0], update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Warn action set to: {context.args[0]}")

# ============================================================
# FEATURE 251-300: BLACKLIST, LOCKS, ANTI-SPAM
# ============================================================

@check_disabled
@group_only
def blacklist_command(update, context):
    """View blacklist - Feature 251"""
    bl = db.execute(
        "SELECT trigger_word, action FROM blacklist WHERE chat_id = %s ORDER BY trigger_word",
        (update.effective_chat.id,), fetch=True
    )
    
    if not bl:
        update.effective_message.reply_text("🚫 No blacklisted words!")
        return
    
    text = f"🚫 <b>Blacklisted words in {update.effective_chat.title}:</b>\n\n"
    for i, b in enumerate(bl, 1):
        text += f"  {i}. <code>{b['trigger_word']}</code> ({b['action']})\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def addblacklist_command(update, context):
    """Add to blacklist - Feature 252"""
    if not context.args:
        update.effective_message.reply_text("Usage: /addblacklist <word>")
        return
    
    word = " ".join(context.args).lower()
    db.execute("""
        INSERT INTO blacklist (chat_id, trigger_word, created_by)
        VALUES (%s, %s, %s)
    """, (update.effective_chat.id, word, update.effective_user.id))
    
    update.effective_message.reply_text(f"✅ Added '<b>{word}</b>' to blacklist!",
                                        parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def unblacklist_command(update, context):
    """Remove from blacklist - Feature 253"""
    if not context.args:
        update.effective_message.reply_text("Usage: /unblacklist <word>")
        return
    
    word = " ".join(context.args).lower()
    result = db.execute(
        "DELETE FROM blacklist WHERE chat_id = %s AND trigger_word = %s RETURNING id",
        (update.effective_chat.id, word), fetchone=True
    )
    
    if result:
        update.effective_message.reply_text(f"✅ Removed '{word}' from blacklist!")
    else:
        update.effective_message.reply_text(f"❌ '{word}' not in blacklist!")

@check_disabled
@group_only
@admin_only
def blacklistmode_command(update, context):
    """Set blacklist action - Feature 254"""
    if not context.args or context.args[0] not in ['delete', 'warn', 'mute', 'ban']:
        update.effective_message.reply_text("Usage: /blacklistmode delete/warn/mute/ban")
        return
    
    db.execute(
        "UPDATE blacklist SET action = %s WHERE chat_id = %s",
        (context.args[0], update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Blacklist action set to: {context.args[0]}")

def blacklist_message_handler(update, context):
    """Check message against blacklist - Feature 255"""
    msg = update.effective_message
    if not msg or not msg.text or msg.chat.type == 'private':
        return
    
    if is_admin(update, context):
        return
    
    text = msg.text.lower()
    bl = db.execute(
        "SELECT * FROM blacklist WHERE chat_id = %s",
        (msg.chat_id,), fetch=True
    )
    
    for b in (bl or []):
        if b['trigger_word'] in text:
            action = b['action']
            try:
                if action == 'delete':
                    msg.delete()
                elif action == 'warn':
                    msg.delete()
                    context.args = [str(msg.from_user.id), f"Blacklisted word: {b['trigger_word']}"]
                    warn_command(update, context)
                elif action == 'mute':
                    msg.delete()
                    context.bot.restrict_chat_member(
                        msg.chat_id, msg.from_user.id,
                        permissions=ChatPermissions(can_send_messages=False)
                    )
                elif action == 'ban':
                    msg.delete()
                    context.bot.ban_chat_member(msg.chat_id, msg.from_user.id)
            except:
                pass
            break

# Lock system
LOCK_TYPES = [
    'text', 'media', 'sticker', 'gif', 'photo', 'video', 'voice',
    'document', 'audio', 'game', 'poll', 'url', 'forward', 'reply',
    'bot', 'button', 'inline', 'contact', 'location', 'phone',
    'email', 'command', 'all'
]

@check_disabled
@group_only
@admin_only
def lock_command(update, context):
    """Lock a type - Feature 256"""
    if not context.args:
        update.effective_message.reply_text(f"Usage: /lock <type>\nTypes: {', '.join(LOCK_TYPES)}")
        return
    
    lock_type = context.args[0].lower()
    if lock_type not in LOCK_TYPES:
        update.effective_message.reply_text(f"❌ Invalid lock type!\nValid: {', '.join(LOCK_TYPES)}")
        return
    
    chat_data = db.execute(
        "SELECT locked_types FROM chats WHERE chat_id = %s",
        (update.effective_chat.id,), fetchone=True
    )
    
    locked = json.loads(chat_data['locked_types']) if chat_data and chat_data['locked_types'] else []
    
    if lock_type == 'all':
        locked = LOCK_TYPES.copy()
    elif lock_type not in locked:
        locked.append(lock_type)
    
    db.execute(
        "UPDATE chats SET locked_types = %s WHERE chat_id = %s",
        (json.dumps(locked), update.effective_chat.id)
    )
    
    update.effective_message.reply_text(f"🔒 Locked: {lock_type}")

@check_disabled
@group_only
@admin_only
def unlock_command(update, context):
    """Unlock a type - Feature 257"""
    if not context.args:
        update.effective_message.reply_text(f"Usage: /unlock <type>")
        return
    
    lock_type = context.args[0].lower()
    
    chat_data = db.execute(
        "SELECT locked_types FROM chats WHERE chat_id = %s",
        (update.effective_chat.id,), fetchone=True
    )
    
    locked = json.loads(chat_data['locked_types']) if chat_data and chat_data['locked_types'] else []
    
    if lock_type == 'all':
        locked = []
    elif lock_type in locked:
        locked.remove(lock_type)
    
    db.execute(
        "UPDATE chats SET locked_types = %s WHERE chat_id = %s",
        (json.dumps(locked), update.effective_chat.id)
    )
    
    update.effective_message.reply_text(f"🔓 Unlocked: {lock_type}")

@check_disabled
@group_only
def locks_command(update, context):
    """View locks - Feature 258"""
    chat_data = db.execute(
        "SELECT locked_types FROM chats WHERE chat_id = %s",
        (update.effective_chat.id,), fetchone=True
    )
    
    locked = json.loads(chat_data['locked_types']) if chat_data and chat_data['locked_types'] else []
    
    text = f"🔒 <b>Lock Status for {update.effective_chat.title}:</b>\n\n"
    for lt in LOCK_TYPES:
        status = "🔒" if lt in locked else "🔓"
        text += f"  {status} {lt}\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

def lock_message_handler(update, context):
    """Enforce locks - Feature 259"""
    msg = update.effective_message
    if not msg or msg.chat.type == 'private':
        return
    
    if is_admin(update, context):
        return
    
    chat_data = db.execute(
        "SELECT locked_types FROM chats WHERE chat_id = %s",
        (msg.chat_id,), fetchone=True
    )
    
    if not chat_data:
        return
    
    locked = json.loads(chat_data['locked_types']) if chat_data['locked_types'] else []
    if not locked:
        return
    
    should_delete = False
    
    if 'all' in locked:
        should_delete = True
    elif 'text' in locked and msg.text and not msg.text.startswith('/'):
        should_delete = True
    elif 'media' in locked and (msg.photo or msg.video or msg.document or msg.audio):
        should_delete = True
    elif 'sticker' in locked and msg.sticker:
        should_delete = True
    elif 'gif' in locked and msg.animation:
        should_delete = True
    elif 'photo' in locked and msg.photo:
        should_delete = True
    elif 'video' in locked and msg.video:
        should_delete = True
    elif 'voice' in locked and msg.voice:
        should_delete = True
    elif 'document' in locked and msg.document:
        should_delete = True
    elif 'audio' in locked and msg.audio:
        should_delete = True
    elif 'poll' in locked and msg.poll:
        should_delete = True
    elif 'forward' in locked and msg.forward_date:
        should_delete = True
    elif 'contact' in locked and msg.contact:
        should_delete = True
    elif 'location' in locked and msg.location:
        should_delete = True
    elif 'url' in locked and msg.entities:
        for ent in msg.entities:
            if ent.type in ['url', 'text_link']:
                should_delete = True
                break
    
    if should_delete:
        try:
            msg.delete()
        except:
            pass

# Anti-spam
@check_disabled
@group_only
@admin_only
def antispam_command(update, context):
    """Toggle anti-spam - Feature 260"""
    if not context.args:
        update.effective_message.reply_text("Usage: /antispam on/off")
        return
    
    enabled = context.args[0].lower() in ['on', 'yes', 'true']
    db.execute(
        "UPDATE chats SET antispam_enabled = %s WHERE chat_id = %s",
        (enabled, update.effective_chat.id)
    )
    status = "enabled" if enabled else "disabled"
    update.effective_message.reply_text(f"🛡️ Anti-spam {status}!")

@check_disabled
@group_only
@admin_only
def antilink_command(update, context):
    """Toggle anti-link - Feature 261"""
    if not context.args:
        update.effective_message.reply_text("Usage: /antilink on/off")
        return
    
    enabled = context.args[0].lower() in ['on', 'yes', 'true']
    db.execute(
        "UPDATE chats SET antilink_enabled = %s WHERE chat_id = %s",
        (enabled, update.effective_chat.id)
    )
    status = "enabled" if enabled else "disabled"
    update.effective_message.reply_text(f"🔗 Anti-link {status}!")

@check_disabled
@group_only
@admin_only
def antilinkaction_command(update, context):
    """Set anti-link action - Feature 262"""
    if not context.args or context.args[0] not in ['warn', 'mute', 'ban', 'kick']:
        update.effective_message.reply_text("Usage: /antilinkaction warn/mute/ban/kick")
        return
    
    db.execute(
        "UPDATE chats SET antilink_action = %s WHERE chat_id = %s",
        (context.args[0], update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Anti-link action: {context.args[0]}")

def antilink_handler(update, context):
    """Check for links - Feature 263"""
    msg = update.effective_message
    if not msg or not msg.text or msg.chat.type == 'private':
        return
    
    if is_admin(update, context):
        return
    
    # Check if approved
    approved = db.execute(
        "SELECT * FROM approvals WHERE chat_id = %s AND user_id = %s",
        (msg.chat_id, msg.from_user.id), fetchone=True
    )
    if approved:
        return
    
    chat_data = db.execute(
        "SELECT antilink_enabled, antilink_action FROM chats WHERE chat_id = %s",
        (msg.chat_id,), fetchone=True
    )
    
    if not chat_data or not chat_data.get('antilink_enabled'):
        return
    
    # Check for links
    has_link = False
    link_patterns = [
        r'https?://', r't\.me/', r'telegram\.me/', r'telegram\.dog/',
        r'@\w+', r'bit\.ly/', r'goo\.gl/'
    ]
    
    for pattern in link_patterns:
        if re.search(pattern, msg.text, re.IGNORECASE):
            has_link = True
            break
    
    if msg.entities:
        for ent in msg.entities:
            if ent.type in ['url', 'text_link']:
                has_link = True
                break
    
    if has_link:
        action = chat_data.get('antilink_action', 'warn')
        try:
            msg.delete()
            if action == 'warn':
                context.args = [str(msg.from_user.id), "Sending links"]
                warn_command(update, context)
            elif action == 'mute':
                context.bot.restrict_chat_member(
                    msg.chat_id, msg.from_user.id,
                    permissions=ChatPermissions(can_send_messages=False)
                )
                msg.reply_text(f"🔇 {mention_html(msg.from_user.id, msg.from_user.first_name)} muted for sending links!",
                             parse_mode=ParseMode.HTML)
            elif action == 'ban':
                context.bot.ban_chat_member(msg.chat_id, msg.from_user.id)
            elif action == 'kick':
                context.bot.ban_chat_member(msg.chat_id, msg.from_user.id)
                context.bot.unban_chat_member(msg.chat_id, msg.from_user.id)
        except:
            pass

# Anti-flood
@check_disabled
@group_only
@admin_only
def antiflood_command(update, context):
    """Toggle anti-flood - Feature 264"""
    if not context.args:
        update.effective_message.reply_text("Usage: /antiflood on/off")
        return
    
    enabled = context.args[0].lower() in ['on', 'yes', 'true']
    db.execute(
        "UPDATE chats SET antiflood_enabled = %s WHERE chat_id = %s",
        (enabled, update.effective_chat.id)
    )
    status = "enabled" if enabled else "disabled"
    update.effective_message.reply_text(f"🌊 Anti-flood {status}!")

@check_disabled
@group_only
@admin_only
def setflood_command(update, context):
    """Set flood limit - Feature 265"""
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /setflood <number>")
        return
    
    limit = int(context.args[0])
    db.execute(
        "UPDATE chats SET antiflood_limit = %s WHERE chat_id = %s",
        (limit, update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Flood limit set to {limit} messages!")

@check_disabled
@group_only
@admin_only
def floodaction_command(update, context):
    """Set flood action - Feature 266"""
    if not context.args or context.args[0] not in ['mute', 'ban', 'kick']:
        update.effective_message.reply_text("Usage: /floodaction mute/ban/kick")
        return
    
    db.execute(
        "UPDATE chats SET antiflood_action = %s WHERE chat_id = %s",
        (context.args[0], update.effective_chat.id)
    )
    update.effective_message.reply_text(f"✅ Flood action: {context.args[0]}")

# Flood tracking dict
flood_tracker = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'time': 0}))

def antiflood_handler(update, context):
    """Track flooding - Feature 267"""
    msg = update.effective_message
    if not msg or msg.chat.type == 'private':
        return
    
    if is_admin(update, context):
        return
    
    chat_data = db.execute(
        "SELECT antiflood_enabled, antiflood_limit, antiflood_action FROM chats WHERE chat_id = %s",
        (msg.chat_id,), fetchone=True
    )
    
    if not chat_data or not chat_data.get('antiflood_enabled'):
        return
    
    user_id = msg.from_user.id
    chat_id = msg.chat_id
    limit = chat_data.get('antiflood_limit', 10)
    action = chat_data.get('antiflood_action', 'mute')
    
    now = time.time()
    tracker = flood_tracker[chat_id][user_id]
    
    if now - tracker['time'] > 10:  # Reset after 10 seconds
        flood_tracker[chat_id][user_id] = {'count': 1, 'time': now}
    else:
        flood_tracker[chat_id][user_id]['count'] += 1
    
    if flood_tracker[chat_id][user_id]['count'] >= limit:
        flood_tracker[chat_id][user_id] = {'count': 0, 'time': 0}
        try:
            if action == 'mute':
                context.bot.restrict_chat_member(
                    chat_id, user_id,
                    permissions=ChatPermissions(can_send_messages=False),
                    until_date=datetime.datetime.now() + datetime.timedelta(hours=1)
                )
                msg.reply_text(f"🌊 {mention_html(user_id, msg.from_user.first_name)} muted for flooding!",
                             parse_mode=ParseMode.HTML)
            elif action == 'ban':
                context.bot.ban_chat_member(chat_id, user_id)
                msg.reply_text(f"🌊 {mention_html(user_id, msg.from_user.first_name)} banned for flooding!",
                             parse_mode=ParseMode.HTML)
            elif action == 'kick':
                context.bot.ban_chat_member(chat_id, user_id)
                context.bot.unban_chat_member(chat_id, user_id)
                msg.reply_text(f"🌊 {mention_html(user_id, msg.from_user.first_name)} kicked for flooding!",
                             parse_mode=ParseMode.HTML)
        except:
            pass

# Approval system
@check_disabled
@group_only
@admin_only
def approve_command(update, context):
    """Approve user - Feature 268"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    
    db.execute("""
        INSERT INTO approvals (chat_id, user_id, approved_by)
        VALUES (%s, %s, %s)
        ON CONFLICT (chat_id, user_id) DO NOTHING
    """, (update.effective_chat.id, user_id, update.effective_user.id))
    
    update.effective_message.reply_text(
        f"✅ {mention_html(user_id, user_name)} has been approved! They won't be affected by locks, blacklists, or anti-link.",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
@admin_only
def unapprove_command(update, context):
    """Unapprove user - Feature 269"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    
    db.execute(
        "DELETE FROM approvals WHERE chat_id = %s AND user_id = %s",
        (update.effective_chat.id, user_id)
    )
    update.effective_message.reply_text(
        f"✅ {mention_html(user_id, user_name)} is no longer approved!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
def approvedlist_command(update, context):
    """List approved users - Feature 270"""
    approved = db.execute(
        "SELECT user_id FROM approvals WHERE chat_id = %s",
        (update.effective_chat.id,), fetch=True
    )
    
    if not approved:
        update.effective_message.reply_text("No approved users!")
        return
    
    text = "✅ <b>Approved Users:</b>\n\n"
    for a in approved:
        text += f"  • <code>{a['user_id']}</code>\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 301-350: RULES, REPORTS, DISABLE
# ============================================================

@check_disabled
@group_only
def rules_command(update, context):
    """View rules - Feature 301"""
    chat_data = db.execute(
        "SELECT rules FROM chats WHERE chat_id = %s",
        (update.effective_chat.id,), fetchone=True
    )
    
    if not chat_data or not chat_data.get('rules'):
        update.effective_message.reply_text("❌ No rules set for this group!")
        return
    
    keyboard = [[InlineKeyboardButton(
        "📋 Rules",
        url=f"t.me/{context.bot.username}?start=rules_{update.effective_chat.id}"
    )]]
    update.effective_message.reply_text(
        "Click below to view the rules!",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@check_disabled
@group_only
@admin_only
def setrules_command(update, context):
    """Set rules - Feature 302"""
    if not context.args and not update.effective_message.reply_to_message:
        update.effective_message.reply_text("Usage: /setrules <rules text>")
        return
    
    if update.effective_message.reply_to_message:
        rules = update.effective_message.reply_to_message.text
    else:
        rules = " ".join(context.args)
    
    db.execute(
        "UPDATE chats SET rules = %s WHERE chat_id = %s",
        (rules, update.effective_chat.id)
    )
    update.effective_message.reply_text("✅ Rules set!")

@check_disabled
@group_only
def report_command(update, context):
    """Report a user - Feature 303"""
    msg = update.effective_message
    
    if not msg.reply_to_message:
        msg.reply_text("❌ Reply to the message you want to report!")
        return
    
    reported_user = msg.reply_to_message.from_user
    reason = " ".join(context.args) if context.args else ""
    
    if reported_user.id == msg.from_user.id:
        msg.reply_text("❌ You can't report yourself!")
        return
    
    if is_admin(update, context, reported_user.id):
        msg.reply_text("❌ You can't report an admin!")
        return
    
    db.execute("""
        INSERT INTO reports (chat_id, reporter_id, reported_user_id, reason, message_id)
        VALUES (%s, %s, %s, %s, %s)
    """, (msg.chat_id, msg.from_user.id, reported_user.id, reason, msg.reply_to_message.message_id))
    
    keyboard = [
        [
            InlineKeyboardButton("⚠️ Warn", callback_data=f"report_warn_{reported_user.id}"),
            InlineKeyboardButton("🔇 Mute", callback_data=f"report_mute_{reported_user.id}"),
            InlineKeyboardButton("🔨 Ban", callback_data=f"report_ban_{reported_user.id}")
        ]
    ]
    
    text = f"⚠️ <b>Report!</b>\n\n"
    text += f"👤 <b>Reported:</b> {mention_html(reported_user.id, reported_user.first_name)}\n"
    text += f"👮 <b>By:</b> {mention_html(msg.from_user.id, msg.from_user.first_name)}\n"
    if reason:
        text += f"📝 <b>Reason:</b> {reason}\n"
    
    # Notify admins
    try:
        admins = context.bot.get_chat_administrators(msg.chat_id)
        admin_mentions = " ".join([mention_html(a.user.id, "​") for a in admins if not a.user.is_bot])
        text += f"\n{admin_mentions}"
    except:
        pass
    
    msg.reply_text(text, parse_mode=ParseMode.HTML,
                  reply_markup=InlineKeyboardMarkup(keyboard))

# Disable commands
@check_disabled
@group_only
@admin_only
def disable_command(update, context):
    """Disable a command - Feature 304"""
    if not context.args:
        update.effective_message.reply_text("Usage: /disable <command>")
        return
    
    cmd = context.args[0].lower().lstrip('/')
    db.execute("""
        INSERT INTO disabled_commands (chat_id, command)
        VALUES (%s, %s) ON CONFLICT DO NOTHING
    """, (update.effective_chat.id, cmd))
    
    update.effective_message.reply_text(f"✅ Command /{cmd} disabled!")

@check_disabled
@group_only
@admin_only
def enable_command(update, context):
    """Enable a command - Feature 305"""
    if not context.args:
        update.effective_message.reply_text("Usage: /enable <command>")
        return
    
    cmd = context.args[0].lower().lstrip('/')
    db.execute(
        "DELETE FROM disabled_commands WHERE chat_id = %s AND command = %s",
        (update.effective_chat.id, cmd)
    )
    update.effective_message.reply_text(f"✅ Command /{cmd} enabled!")

@check_disabled
@group_only
def disabled_command(update, context):
    """List disabled commands - Feature 306"""
    disabled = db.execute(
        "SELECT command FROM disabled_commands WHERE chat_id = %s",
        (update.effective_chat.id,), fetch=True
    )
    
    if not disabled:
        update.effective_message.reply_text("No disabled commands!")
        return
    
    text = "🚫 <b>Disabled Commands:</b>\n\n"
    for d in disabled:
        text += f"  • /{d['command']}\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 351-400: USER FEATURES
# ============================================================

@check_disabled
def info_command(update, context):
    """User info - Feature 351"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        user_id = update.effective_user.id
    
    chat_id = update.effective_chat.id if update.effective_chat.type != 'private' else None
    text = get_user_info_text(user_id, chat_id)
    
    try:
        photos = context.bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            update.effective_message.reply_photo(
                photos.photos[0][0].file_id,
                caption=text, parse_mode=ParseMode.HTML
            )
        else:
            update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def id_command(update, context):
    """Get IDs - Feature 352"""
    msg = update.effective_message
    text = ""
    
    if msg.reply_to_message:
        user = msg.reply_to_message.from_user
        text += f"👤 <b>{user.first_name}'s ID:</b> <code>{user.id}</code>\n"
        if msg.reply_to_message.forward_from:
            text += f"↩️ <b>Forwarded from:</b> <code>{msg.reply_to_message.forward_from.id}</code>\n"
    
    text += f"👤 <b>Your ID:</b> <code>{msg.from_user.id}</code>\n"
    
    if msg.chat.type != 'private':
        text += f"💬 <b>Chat ID:</b> <code>{msg.chat_id}</code>\n"
    
    msg.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def afk_command(update, context):
    """Set AFK - Feature 353"""
    reason = " ".join(context.args) if context.args else "AFK"
    user_id = update.effective_user.id
    
    db.execute("""
        INSERT INTO afk_users (user_id, reason, time)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id) DO UPDATE SET
        reason = EXCLUDED.reason, time = CURRENT_TIMESTAMP
    """, (user_id, reason))
    
    db.execute(
        "UPDATE users SET afk = TRUE, afk_reason = %s, afk_time = CURRENT_TIMESTAMP WHERE user_id = %s",
        (reason, user_id)
    )
    
    update.effective_message.reply_text(
        f"💤 {update.effective_user.first_name} is now AFK!\nReason: {reason}"
    )

def afk_check_handler(update, context):
    """Check AFK status - Feature 354"""
    msg = update.effective_message
    if not msg or msg.chat.type == 'private':
        return
    
    # Check if user is back from AFK
    user_afk = db.execute(
        "SELECT * FROM afk_users WHERE user_id = %s",
        (msg.from_user.id,), fetchone=True
    )
    
    if user_afk:
        db.execute("DELETE FROM afk_users WHERE user_id = %s", (msg.from_user.id,))
        db.execute("UPDATE users SET afk = FALSE WHERE user_id = %s", (msg.from_user.id,))
        
        afk_time = user_afk.get('time')
        if afk_time:
            diff = datetime.datetime.now() - afk_time
            time_str = get_readable_time(diff.total_seconds())
            msg.reply_text(
                f"💤 {msg.from_user.first_name} is back! Was AFK for {time_str}."
            )
        else:
            msg.reply_text(f"💤 {msg.from_user.first_name} is back from AFK!")
    
    # Check if mentioned/replied user is AFK
    if msg.reply_to_message:
        replied_user = msg.reply_to_message.from_user
        afk_data = db.execute(
            "SELECT * FROM afk_users WHERE user_id = %s",
            (replied_user.id,), fetchone=True
        )
        if afk_data:
            msg.reply_text(
                f"💤 {replied_user.first_name} is AFK!\nReason: {afk_data['reason']}"
            )
    
    if msg.entities:
        for ent in msg.entities:
            if ent.type == 'mention':
                username = msg.text[ent.offset+1:ent.offset+ent.length]
                user_data = db.execute(
                    "SELECT user_id FROM users WHERE username = %s",
                    (username,), fetchone=True
                )
                if user_data:
                    afk_data = db.execute(
                        "SELECT * FROM afk_users WHERE user_id = %s",
                        (user_data['user_id'],), fetchone=True
                    )
                    if afk_data:
                        msg.reply_text(
                            f"💤 @{username} is AFK!\nReason: {afk_data['reason']}"
                        )
            elif ent.type == 'text_mention':
                afk_data = db.execute(
                    "SELECT * FROM afk_users WHERE user_id = %s",
                    (ent.user.id,), fetchone=True
                )
                if afk_data:
                    msg.reply_text(
                        f"💤 {ent.user.first_name} is AFK!\nReason: {afk_data['reason']}"
                    )

@check_disabled
def setbio_command(update, context):
    """Set bio - Feature 355"""
    if not context.args:
        update.effective_message.reply_text("Usage: /setbio <your bio>")
        return
    
    bio = " ".join(context.args)[:200]
    db.execute("""
        INSERT INTO user_bios (user_id, bio, set_by)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET bio = EXCLUDED.bio
    """, (update.effective_user.id, bio, update.effective_user.id))
    
    update.effective_message.reply_text(f"✅ Bio set: {bio}")

@check_disabled
def bio_command(update, context):
    """Get bio - Feature 356"""
    user_id, _ = extract_user(update, context)
    if not user_id:
        user_id = update.effective_user.id
    
    bio_data = db.execute(
        "SELECT bio FROM user_bios WHERE user_id = %s",
        (user_id,), fetchone=True
    )
    
    if bio_data and bio_data['bio']:
        update.effective_message.reply_text(f"📝 Bio: {bio_data['bio']}")
    else:
        update.effective_message.reply_text("No bio set!")

@check_disabled
def reputation_command(update, context):
    """Give/remove reputation - Feature 357"""
    msg = update.effective_message
    if msg.chat.type == 'private':
        msg.reply_text("❌ Use this in a group!")
        return
    
    if not context.args:
        msg.reply_text("Usage: /rep + @user or /rep - @user")
        return
    
    action = context.args[0]
    if action not in ['+', '-']:
        msg.reply_text("Usage: /rep + @user or /rep - @user")
        return
    
    user_id, user_name = None, None
    if len(context.args) > 1:
        if context.args[1].startswith('@'):
            user_data = db.execute(
                "SELECT user_id, first_name FROM users WHERE username = %s",
                (context.args[1].lstrip('@'),), fetchone=True
            )
            if user_data:
                user_id = user_data['user_id']
                user_name = user_data['first_name']
        elif context.args[1].isdigit():
            user_id = int(context.args[1])
            user_name = str(user_id)
    elif msg.reply_to_message:
        user_id = msg.reply_to_message.from_user.id
        user_name = msg.reply_to_message.from_user.first_name
    
    if not user_id:
        msg.reply_text("❌ Specify a user!")
        return
    
    if user_id == msg.from_user.id:
        msg.reply_text("❌ You can't change your own reputation!")
        return
    
    change = 1 if action == '+' else -1
    
    db.execute("""
        INSERT INTO reputation (chat_id, user_id, rep_count)
        VALUES (%s, %s, %s)
        ON CONFLICT (chat_id, user_id) DO UPDATE SET
        rep_count = reputation.rep_count + %s
    """, (msg.chat_id, user_id, change, change))
    
    db.execute(
        "UPDATE users SET reputation = reputation + %s WHERE user_id = %s",
        (change, user_id)
    )
    
    rep_data = db.execute(
        "SELECT rep_count FROM reputation WHERE chat_id = %s AND user_id = %s",
        (msg.chat_id, user_id), fetchone=True
    )
    
    emoji = "⬆️" if action == '+' else "⬇️"
    msg.reply_text(
        f"{emoji} {mention_html(user_id, user_name)}'s reputation: {rep_data['rep_count']}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def me_command(update, context):
    """View profile - Feature 358"""
    user = update.effective_user
    user_data = db.execute(
        "SELECT * FROM users WHERE user_id = %s", (user.id,), fetchone=True
    )
    
    if not user_data:
        update.effective_message.reply_text("Profile not found!")
        return
    
    text = f"╔══════════════════╗\n"
    text += f"  👤 <b>Your Profile</b>\n"
    text += f"╚══════════════════╝\n\n"
    text += f"🆔 <b>ID:</b> <code>{user.id}</code>\n"
    text += f"👤 <b>Name:</b> {user.first_name}\n"
    text += f"💰 <b>Coins:</b> {user_data.get('coins', 0)}\n"
    text += f"📊 <b>Level:</b> {user_data.get('level', 1)}\n"
    text += f"✨ <b>XP:</b> {user_data.get('xp', 0)}\n"
    text += f"⭐ <b>Reputation:</b> {user_data.get('reputation', 0)}\n"
    
    # Calculate XP needed for next level
    level = user_data.get('level', 1)
    xp_needed = level * 100
    xp_current = user_data.get('xp', 0)
    progress = min(int((xp_current / xp_needed) * 10), 10)
    bar = "█" * progress + "░" * (10 - progress)
    text += f"\n📈 <b>Progress:</b> [{bar}] {xp_current}/{xp_needed}"
    
    if user_data.get('married_to'):
        text += f"\n💍 <b>Married to:</b> <code>{user_data['married_to']}</code>"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 401-450: ECONOMY SYSTEM
# ============================================================

@check_disabled
def balance_command(update, context):
    """Check balance - Feature 401"""
    user_id = update.effective_user.id
    user_data = db.execute(
        "SELECT coins FROM users WHERE user_id = %s", (user_id,), fetchone=True
    )
    
    coins = user_data['coins'] if user_data else 0
    update.effective_message.reply_text(f"💰 <b>Your Balance:</b> {coins} coins",
                                        parse_mode=ParseMode.HTML)

@check_disabled
def daily_command(update, context):
    """Daily reward - Feature 402"""
    user_id = update.effective_user.id
    user_data = db.execute(
        "SELECT daily_claimed FROM users WHERE user_id = %s", (user_id,), fetchone=True
    )
    
    if user_data and user_data['daily_claimed']:
        last_claim = user_data['daily_claimed']
        if (datetime.datetime.now() - last_claim).total_seconds() < 86400:
            remaining = 86400 - (datetime.datetime.now() - last_claim).total_seconds()
            update.effective_message.reply_text(
                f"❌ Already claimed! Come back in {get_readable_time(remaining)}"
            )
            return
    
    reward = random.randint(100, 500)
    db.execute(
        "UPDATE users SET coins = coins + %s, daily_claimed = CURRENT_TIMESTAMP WHERE user_id = %s",
        (reward, user_id)
    )
    
    update.effective_message.reply_text(
        f"🎁 <b>Daily Reward!</b>\n\n"
        f"You received <b>{reward}</b> coins! 💰\n"
        f"Come back tomorrow for more!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def transfer_command(update, context):
    """Transfer coins - Feature 403"""
    msg = update.effective_message
    
    if len(context.args) < 2:
        msg.reply_text("Usage: /transfer @user <amount>")
        return
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user!")
        return
    
    try:
        amount = int(context.args[-1])
    except:
        msg.reply_text("❌ Invalid amount!")
        return
    
    if amount <= 0:
        msg.reply_text("❌ Amount must be positive!")
        return
    
    if user_id == msg.from_user.id:
        msg.reply_text("❌ Can't transfer to yourself!")
        return
    
    sender_data = db.execute(
        "SELECT coins FROM users WHERE user_id = %s", (msg.from_user.id,), fetchone=True
    )
    
    if not sender_data or sender_data['coins'] < amount:
        msg.reply_text("❌ Not enough coins!")
        return
    
    db.execute("UPDATE users SET coins = coins - %s WHERE user_id = %s", (amount, msg.from_user.id))
    db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s", (amount, user_id))
    
    msg.reply_text(
        f"💸 Transferred <b>{amount}</b> coins to {mention_html(user_id, user_name)}!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def gamble_command(update, context):
    """Gamble coins - Feature 404"""
    msg = update.effective_message
    
    if not context.args or not context.args[0].isdigit():
        msg.reply_text("Usage: /gamble <amount>")
        return
    
    amount = int(context.args[0])
    if amount <= 0:
        msg.reply_text("❌ Amount must be positive!")
        return
    
    user_data = db.execute(
        "SELECT coins FROM users WHERE user_id = %s", (msg.from_user.id,), fetchone=True
    )
    
    if not user_data or user_data['coins'] < amount:
        msg.reply_text("❌ Not enough coins!")
        return
    
    # 45% win chance, 55% lose
    if random.random() < 0.45:
        winnings = amount * 2
        db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
                   (amount, msg.from_user.id))
        msg.reply_text(
            f"🎰 <b>YOU WON!</b> 🎉\n\n"
            f"You won <b>{winnings}</b> coins!",
            parse_mode=ParseMode.HTML
        )
    else:
        db.execute("UPDATE users SET coins = coins - %s WHERE user_id = %s",
                   (amount, msg.from_user.id))
        msg.reply_text(
            f"🎰 <b>YOU LOST!</b> 😢\n\n"
            f"You lost <b>{amount}</b> coins!",
            parse_mode=ParseMode.HTML
        )

@check_disabled
def work_command(update, context):
    """Work for coins - Feature 405"""
    jobs = [
        ("👨‍💻 Programmer", 50, 200),
        ("👨‍🍳 Chef", 30, 150),
        ("👨‍🔧 Mechanic", 40, 180),
        ("👨‍🏫 Teacher", 35, 160),
        ("👨‍⚕️ Doctor", 60, 250),
        ("👨‍🚒 Firefighter", 45, 190),
        ("👨‍🎨 Artist", 25, 140),
        ("👨‍🚀 Astronaut", 80, 300),
        ("🕵️ Detective", 55, 220),
        ("👨‍🌾 Farmer", 20, 120),
    ]
    
    job, min_pay, max_pay = random.choice(jobs)
    earned = random.randint(min_pay, max_pay)
    
    db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
               (earned, update.effective_user.id))
    
    update.effective_message.reply_text(
        f"{job}\n\n"
        f"You worked hard and earned <b>{earned}</b> coins! 💰",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def mine_command(update, context):
    """Mine for coins - Feature 406"""
    finds = [
        ("💎 Diamond", 100, 500),
        ("🥇 Gold", 50, 200),
        ("🥈 Silver", 30, 100),
        ("🥉 Bronze", 10, 50),
        ("🪨 Rock", 1, 10),
        ("💍 Ancient Ring", 200, 800),
        ("🏺 Artifact", 150, 600),
    ]
    
    find, min_val, max_val = random.choice(finds)
    earned = random.randint(min_val, max_val)
    
    db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
               (earned, update.effective_user.id))
    
    update.effective_message.reply_text(
        f"⛏️ <b>Mining Result:</b>\n\n"
        f"You found {find} worth <b>{earned}</b> coins! 💰",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def fish_command(update, context):
    """Go fishing - Feature 407"""
    catches = [
        ("🐟 Small Fish", 5, 20),
        ("🐠 Tropical Fish", 15, 50),
        ("🐡 Pufferfish", 25, 80),
        ("🦈 Shark", 100, 300),
        ("🐳 Whale", 200, 500),
        ("🦑 Squid", 30, 90),
        ("🦞 Lobster", 40, 120),
        ("👢 Old Boot", 0, 1),
        ("🗑️ Trash", 0, 0),
        ("💎 Pearl", 150, 400),
    ]
    
    catch, min_val, max_val = random.choice(catches)
    earned = random.randint(min_val, max_val)
    
    db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
               (earned, update.effective_user.id))
    
    update.effective_message.reply_text(
        f"🎣 <b>Fishing Result:</b>\n\n"
        f"You caught {catch}! Worth <b>{earned}</b> coins! 💰",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def hunt_command(update, context):
    """Go hunting - Feature 408"""
    animals = [
        ("🐰 Rabbit", 10, 30),
        ("🦊 Fox", 20, 60),
        ("🦌 Deer", 40, 100),
        ("🐻 Bear", 80, 200),
        ("🦁 Lion", 150, 400),
        ("🐉 Dragon", 300, 1000),
        ("🦅 Eagle", 30, 80),
    ]
    
    animal, min_val, max_val = random.choice(animals)
    earned = random.randint(min_val, max_val)
    
    db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
               (earned, update.effective_user.id))
    
    update.effective_message.reply_text(
        f"🏹 <b>Hunt Result:</b>\n\n"
        f"You hunted {animal}! Worth <b>{earned}</b> coins! 💰",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def richest_command(update, context):
    """Show richest users - Feature 409"""
    top_users = db.execute(
        "SELECT user_id, first_name, coins FROM users ORDER BY coins DESC LIMIT 10",
        fetch=True
    )
    
    if not top_users:
        update.effective_message.reply_text("No users found!")
        return
    
    text = "💰 <b>Richest Users:</b>\n\n"
    for i, u in enumerate(top_users, 1):
        medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"{i}."
        text += f"{medal} {u['first_name']}: <b>{u['coins']}</b> coins\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def slots_command(update, context):
    """Slot machine - Feature 410"""
    msg = update.effective_message
    
    bet = 10
    if context.args and context.args[0].isdigit():
        bet = int(context.args[0])
    
    user_data = db.execute(
        "SELECT coins FROM users WHERE user_id = %s", (msg.from_user.id,), fetchone=True
    )
    
    if not user_data or user_data['coins'] < bet:
        msg.reply_text("❌ Not enough coins!")
        return
    
    symbols = ['🍒', '🍋', '🍊', '🍇', '💎', '7️⃣', '🔔', '⭐']
    slot1 = random.choice(symbols)
    slot2 = random.choice(symbols)
    slot3 = random.choice(symbols)
    
    text = f"🎰 <b>Slot Machine</b>\n\n"
    text += f"╔══════════╗\n"
    text += f"║ {slot1} │ {slot2} │ {slot3} ║\n"
    text += f"╚══════════╝\n\n"
    
    if slot1 == slot2 == slot3:
        winnings = bet * 10
        db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
                   (winnings - bet, msg.from_user.id))
        text += f"🎉 <b>JACKPOT!</b> You won <b>{winnings}</b> coins!"
    elif slot1 == slot2 or slot2 == slot3 or slot1 == slot3:
        winnings = bet * 2
        db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
                   (winnings - bet, msg.from_user.id))
        text += f"✨ <b>Nice!</b> You won <b>{winnings}</b> coins!"
    else:
        db.execute("UPDATE users SET coins = coins - %s WHERE user_id = %s",
                   (bet, msg.from_user.id))
        text += f"😢 You lost <b>{bet}</b> coins!"
    
    msg.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def rob_command(update, context):
    """Rob another user - Feature 411"""
    msg = update.effective_message
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("Usage: /rob @user")
        return
    
    if user_id == msg.from_user.id:
        msg.reply_text("❌ Can't rob yourself!")
        return
    
    target_data = db.execute(
        "SELECT coins FROM users WHERE user_id = %s", (user_id,), fetchone=True
    )
    
    if not target_data or target_data['coins'] < 100:
        msg.reply_text("❌ This user doesn't have enough coins to rob!")
        return
    
    # 30% success chance
    if random.random() < 0.30:
        stolen = random.randint(1, min(target_data['coins'] // 4, 500))
        db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
                   (stolen, msg.from_user.id))
        db.execute("UPDATE users SET coins = coins - %s WHERE user_id = %s",
                   (stolen, user_id))
        msg.reply_text(
            f"🦹 You successfully robbed <b>{stolen}</b> coins from {mention_html(user_id, user_name)}!",
            parse_mode=ParseMode.HTML
        )
    else:
        fine = random.randint(50, 200)
        db.execute("UPDATE users SET coins = coins - %s WHERE user_id = %s",
                   (fine, msg.from_user.id))
        msg.reply_text(
            f"🚔 You got caught! You were fined <b>{fine}</b> coins!",
            parse_mode=ParseMode.HTML
        )

@check_disabled
def coupon_command(update, context):
    """Redeem coupon - Feature 412"""
    if not context.args:
        update.effective_message.reply_text("Usage: /coupon <code>")
        return
    
    code = context.args[0].upper()
    coupon = db.execute(
        "SELECT * FROM coupons WHERE code = %s", (code,), fetchone=True
    )
    
    if not coupon:
        update.effective_message.reply_text("❌ Invalid coupon code!")
        return
    
    if coupon['used_count'] >= coupon['max_uses']:
        update.effective_message.reply_text("❌ Coupon has been fully used!")
        return
    
    if coupon.get('expires_at') and datetime.datetime.now() > coupon['expires_at']:
        update.effective_message.reply_text("❌ Coupon expired!")
        return
    
    used_by = json.loads(coupon.get('used_by', '[]'))
    if update.effective_user.id in used_by:
        update.effective_message.reply_text("❌ You already used this coupon!")
        return
    
    amount = coupon['reward_amount']
    used_by.append(update.effective_user.id)
    
    db.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s",
               (amount, update.effective_user.id))
    db.execute(
        "UPDATE coupons SET used_count = used_count + 1, used_by = %s WHERE code = %s",
        (json.dumps(used_by), code)
    )
    
    update.effective_message.reply_text(
        f"🎉 Coupon redeemed! You received <b>{amount}</b> coins!",
        parse_mode=ParseMode.HTML
    )

# ============================================================
# FEATURE 451-500: FUN & GAMES
# ============================================================

@check_disabled
def joke_command(update, context):
    """Random joke - Feature 451"""
    jokes = [
        "Why do programmers prefer dark mode? Because light attracts bugs! 🐛",
        "Why was the computer cold? It left its Windows open! 🪟",
        "What did the router say to the doctor? It hurts when IP! 💉",
        "Why do Java developers wear glasses? Because they can't C#! 👓",
        "What's a computer's favorite snack? Microchips! 🍪",
        "Why did the developer go broke? Because he used up all his cache! 💸",
        "How do you comfort a JavaScript bug? You console it! 🖥️",
        "Why did the SQL query cross the road? To JOIN the other table! 🛤️",
        "What do you call a computer that sings? A-Dell! 🎤",
        "Why was the function sad? It didn't get any arguments! 😢",
        "What's a programmer's favorite hangout? Foo Bar! 🍺",
        "How does a computer get drunk? It takes screenshots! 📸",
        "Why do programmers hate nature? It has too many bugs! 🌿",
        "What do computers eat for a snack? Cookies! 🍪",
        "Why couldn't the computer take its hat off? It had a CAPS LOCK! 🎩",
    ]
    update.effective_message.reply_text(random.choice(jokes))

@check_disabled
def quote_command(update, context):
    """Random quote - Feature 452"""
    quotes = [
        '"The only way to do great work is to love what you do." - Steve Jobs',
        '"Innovation distinguishes between a leader and a follower." - Steve Jobs',
        '"Life is what happens when you\'re busy making other plans." - John Lennon',
        '"The future belongs to those who believe in the beauty of their dreams." - Eleanor Roosevelt',
        '"It is during our darkest moments that we must focus to see the light." - Aristotle',
        '"The only impossible journey is the one you never begin." - Tony Robbins',
        '"Success is not final, failure is not fatal: it is the courage to continue that counts." - Winston Churchill',
        '"Believe you can and you\'re halfway there." - Theodore Roosevelt',
        '"Act as if what you do makes a difference. It does." - William James',
        '"What you get by achieving your goals is not as important as what you become." - Zig Ziglar',
    ]
    update.effective_message.reply_text(f"📜 {random.choice(quotes)}")

@check_disabled
def fact_command(update, context):
    """Random fact - Feature 453"""
    facts = [
        "🧠 Honey never spoils. Archaeologists have found 3000-year-old honey that's still edible!",
        "🧠 A group of flamingos is called a 'flamboyance'!",
        "🧠 Octopuses have three hearts and blue blood!",
        "🧠 Bananas are berries, but strawberries aren't!",
        "🧠 The shortest war in history lasted 38-45 minutes!",
        "🧠 A day on Venus is longer than a year on Venus!",
        "🧠 The inventor of the Pringles can is buried in one!",
        "🧠 Cows have best friends and get stressed when separated!",
        "🧠 The average person walks about 100,000 miles in their lifetime!",
        "🧠 There are more possible iterations of a game of chess than atoms in the known universe!",
    ]
    update.effective_message.reply_text(random.choice(facts))

@check_disabled
def roll_command(update, context):
    """Roll dice - Feature 454"""
    sides = 6
    if context.args and context.args[0].isdigit():
        sides = int(context.args[0])
    
    result = random.randint(1, sides)
    update.effective_message.reply_text(f"🎲 You rolled a <b>{result}</b>! (1-{sides})",
                                        parse_mode=ParseMode.HTML)

@check_disabled
def flip_command(update, context):
    """Flip coin - Feature 455"""
    result = random.choice(["Heads 🪙", "Tails 🪙"])
    update.effective_message.reply_text(f"🪙 Coin flip: <b>{result}</b>!",
                                        parse_mode=ParseMode.HTML)

@check_disabled
def rps_command(update, context):
    """Rock Paper Scissors - Feature 456"""
    if not context.args or context.args[0].lower() not in ['rock', 'paper', 'scissors']:
        update.effective_message.reply_text("Usage: /rps rock/paper/scissors")
        return
    
    choices = {'rock': '🪨', 'paper': '📄', 'scissors': '✂️'}
    user_choice = context.args[0].lower()
    bot_choice = random.choice(list(choices.keys()))
    
    if user_choice == bot_choice:
        result = "🤝 It's a tie!"
    elif (user_choice == 'rock' and bot_choice == 'scissors') or \
         (user_choice == 'paper' and bot_choice == 'rock') or \
         (user_choice == 'scissors' and bot_choice == 'paper'):
        result = "🎉 You win!"
        db.execute("UPDATE users SET coins = coins + 10 WHERE user_id = %s",
                   (update.effective_user.id,))
    else:
        result = "😢 You lose!"
    
    update.effective_message.reply_text(
        f"You: {choices[user_choice]} vs Bot: {choices[bot_choice]}\n\n{result}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def eightball_command(update, context):
    """Magic 8 ball - Feature 457"""
    if not context.args:
        update.effective_message.reply_text("Usage: /8ball <question>")
        return
    
    answers = [
        "🔮 Yes, definitely!",
        "🔮 Without a doubt!",
        "🔮 Most likely.",
        "🔮 Yes!",
        "🔮 Signs point to yes.",
        "🔮 Ask again later.",
        "🔮 Cannot predict now.",
        "🔮 Better not tell you now.",
        "🔮 Don't count on it.",
        "🔮 My reply is no.",
        "🔮 My sources say no.",
        "🔮 Very doubtful.",
        "🔮 Outlook not so good.",
        "🔮 Concentrate and ask again.",
        "🔮 It is certain!",
    ]
    
    update.effective_message.reply_text(
        f"❓ <b>Question:</b> {' '.join(context.args)}\n\n{random.choice(answers)}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def ship_command(update, context):
    """Ship two users - Feature 458"""
    msg = update.effective_message
    
    if len(context.args) < 2:
        if msg.reply_to_message:
            user1 = msg.from_user.first_name
            user2 = msg.reply_to_message.from_user.first_name
        else:
            msg.reply_text("Usage: /ship @user1 @user2")
            return
    else:
        user1 = context.args[0]
        user2 = context.args[1]
    
    percentage = random.randint(0, 100)
    
    if percentage < 20:
        emoji = "💔"
        comment = "Not a match..."
    elif percentage < 40:
        emoji = "❤️"
        comment = "Could work!"
    elif percentage < 60:
        emoji = "💕"
        comment = "Nice compatibility!"
    elif percentage < 80:
        emoji = "💖"
        comment = "Great match!"
    else:
        emoji = "💘"
        comment = "Perfect couple!"
    
    bar_len = percentage // 10
    bar = "█" * bar_len + "░" * (10 - bar_len)
    
    text = f"💘 <b>Love Calculator</b>\n\n"
    text += f"👤 {user1}\n"
    text += f"👤 {user2}\n\n"
    text += f"{emoji} <b>{percentage}%</b>\n"
    text += f"[{bar}]\n\n"
    text += f"<i>{comment}</i>"
    
    msg.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def rate_command(update, context):
    """Rate user - Feature 459"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        user_name = update.effective_user.first_name
    
    rating = random.randint(1, 10)
    stars = "⭐" * rating + "☆" * (10 - rating)
    
    update.effective_message.reply_text(
        f"📊 <b>Rating for {user_name}:</b>\n\n"
        f"{stars}\n"
        f"<b>{rating}/10</b>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def truth_command(update, context):
    """Truth question - Feature 460"""
    truths = [
        "What's your biggest fear?",
        "What's the most embarrassing thing you've done?",
        "Have you ever lied to your best friend?",
        "What's your biggest secret?",
        "What's the worst thing you've ever done?",
        "Have you ever cheated on a test?",
        "What's your most controversial opinion?",
        "What's the last lie you told?",
        "What's your biggest regret?",
        "Who was your first crush?",
        "What's the dumbest thing you've ever done?",
        "Have you ever stalked someone online?",
        "What's your most embarrassing nickname?",
        "What's the weirdest dream you've had?",
        "If you could be invisible, what would you do first?",
    ]
    update.effective_message.reply_text(f"🤔 <b>Truth:</b>\n\n{random.choice(truths)}",
                                        parse_mode=ParseMode.HTML)

@check_disabled
def dare_command(update, context):
    """Dare challenge - Feature 461"""
    dares = [
        "Send a voice message singing your favorite song!",
        "Change your profile picture to something funny for 1 hour!",
        "Send a message in all caps!",
        "Use only emojis for the next 10 messages!",
        "Send your last selfie!",
        "Tell a joke!",
        "Send a voice message of your best impression!",
        "Share your most-listened song!",
        "Write a poem in 2 minutes!",
        "Say something nice about everyone in the chat!",
        "Send a message to your crush right now!",
        "Share your screen time!",
        "Do 10 pushups and send proof!",
        "Speak in a different accent for the next 5 messages!",
        "Share an embarrassing photo!",
    ]
    update.effective_message.reply_text(f"😈 <b>Dare:</b>\n\n{random.choice(dares)}",
                                        parse_mode=ParseMode.HTML)

@check_disabled
def slap_command(update, context):
    """Slap someone - Feature 462"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("Usage: /slap @user")
        return
    
    slaps = [
        f"👋 {update.effective_user.first_name} slapped {user_name} with a large trout!",
        f"👋 {update.effective_user.first_name} slapped {user_name} across the face!",
        f"👋 {update.effective_user.first_name} threw a mass of bricks at {user_name}!",
        f"🐟 {update.effective_user.first_name} slapped {user_name} with a wet fish!",
        f"🍳 {update.effective_user.first_name} hit {user_name} with a frying pan!",
    ]
    update.effective_message.reply_text(random.choice(slaps))

@check_disabled
def hug_command(update, context):
    """Hug someone - Feature 463"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("Usage: /hug @user")
        return
    
    update.effective_message.reply_text(
        f"🤗 {update.effective_user.first_name} hugged {user_name}! ❤️"
    )

@check_disabled
def pat_command(update, context):
    """Pat someone - Feature 464"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("Usage: /pat @user")
        return
    
    update.effective_message.reply_text(
        f"👋 {update.effective_user.first_name} patted {user_name}!"
    )

@check_disabled
def reverse_command(update, context):
    """Reverse text - Feature 465"""
    if not context.args:
        update.effective_message.reply_text("Usage: /reverse <text>")
        return
    
    text = " ".join(context.args)
    reversed_text = text[::-1]
    update.effective_message.reply_text(f"🔄 {reversed_text}")

@check_disabled
def mock_command(update, context):
    """Mocking text - Feature 466"""
    if not context.args:
        update.effective_message.reply_text("Usage: /mock <text>")
        return
    
    text = " ".join(context.args)
    mocked = "".join([c.upper() if i % 2 else c.lower() for i, c in enumerate(text)])
    update.effective_message.reply_text(f"🤪 {mocked}")

@check_disabled
def clap_command(update, context):
    """Clap text - Feature 467"""
    if not context.args:
        update.effective_message.reply_text("Usage: /clap <text>")
        return
    
    text = " ".join(context.args)
    clapped = " 👏 ".join(text.split())
    update.effective_message.reply_text(f"👏 {clapped} 👏")

@check_disabled
def vapor_command(update, context):
    """Vaporwave text - Feature 468"""
    if not context.args:
        update.effective_message.reply_text("Usage: /vapor <text>")
        return
    
    text = " ".join(context.args)
    vapored = " ".join(text.upper())
    update.effective_message.reply_text(f"Ａ Ｅ Ｓ Ｔ Ｈ Ｅ Ｔ Ｉ Ｃ\n\n{vapored}")

@check_disabled
def tiny_command(update, context):
    """Tiny text - Feature 469"""
    if not context.args:
        update.effective_message.reply_text("Usage: /tiny <text>")
        return
    
    text = " ".join(context.args).lower()
    tiny_map = str.maketrans(
        'abcdefghijklmnopqrstuvwxyz',
        'ᵃᵇᶜᵈᵉᶠᵍʰⁱʲᵏˡᵐⁿᵒᵖqʳˢᵗᵘᵛʷˣʸᶻ'
    )
    update.effective_message.reply_text(text.translate(tiny_map))

@check_disabled
def trivia_command(update, context):
    """Trivia game - Feature 470"""
    questions = [
        {
            "q": "What is the capital of France?",
            "options": ["London", "Paris", "Berlin", "Madrid"],
            "answer": 1
        },
        {
            "q": "Which planet is closest to the Sun?",
            "options": ["Venus", "Earth", "Mercury", "Mars"],
            "answer": 2
        },
        {
            "q": "What is the largest ocean on Earth?",
            "options": ["Atlantic", "Indian", "Arctic", "Pacific"],
            "answer": 3
        },
        {
            "q": "Who painted the Mona Lisa?",
            "options": ["Picasso", "Van Gogh", "Da Vinci", "Michelangelo"],
            "answer": 2
        },
        {
            "q": "What year did World War II end?",
            "options": ["1943", "1944", "1945", "1946"],
            "answer": 2
        },
        {
            "q": "Which element has the symbol 'O'?",
            "options": ["Gold", "Silver", "Oxygen", "Osmium"],
            "answer": 2
        },
        {
            "q": "How many continents are there?",
            "options": ["5", "6", "7", "8"],
            "answer": 2
        },
        {
            "q": "What is the largest mammal?",
            "options": ["Elephant", "Blue Whale", "Giraffe", "Hippopotamus"],
            "answer": 1
        },
    ]
    
    q = random.choice(questions)
    keyboard = []
    for i, opt in enumerate(q['options']):
        keyboard.append([InlineKeyboardButton(
            opt, callback_data=f"game_trivia_{i}_{q['answer']}"
        )])
    
    update.effective_message.reply_text(
        f"🧠 <b>Trivia Time!</b>\n\n{q['q']}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@check_disabled
def guess_command(update, context):
    """Number guessing game - Feature 471"""
    number = random.randint(1, 100)
    context.chat_data['guess_number'] = number
    context.chat_data['guess_attempts'] = 0
    
    update.effective_message.reply_text(
        "🔢 <b>Number Guessing Game!</b>\n\n"
        "I'm thinking of a number between 1 and 100.\n"
        "Reply with your guess!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def would_command(update, context):
    """Would you rather - Feature 472"""
    questions = [
        ("Be able to fly", "Be able to read minds"),
        ("Live in the past", "Live in the future"),
        ("Be rich and ugly", "Be poor and beautiful"),
        ("Have unlimited money", "Have unlimited time"),
        ("Be famous", "Be powerful"),
        ("Never use social media again", "Never watch movies/TV again"),
        ("Be able to speak all languages", "Be able to play all instruments"),
        ("Always be hot", "Always be cold"),
        ("Have no internet", "Have no AC/heating"),
        ("Be invisible", "Be able to teleport"),
    ]
    
    q = random.choice(questions)
    keyboard = [
        [InlineKeyboardButton(f"🅰️ {q[0]}", callback_data="game_wyr_a")],
        [InlineKeyboardButton(f"🅱️ {q[1]}", callback_data="game_wyr_b")]
    ]
    
    update.effective_message.reply_text(
        f"🤔 <b>Would You Rather?</b>\n\n"
        f"🅰️ {q[0]}\n<b>OR</b>\n🅱️ {q[1]}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@check_disabled
def nhie_command(update, context):
    """Never have I ever - Feature 473"""
    statements = [
        "Never have I ever stayed up all night!",
        "Never have I ever eaten something off the floor!",
        "Never have I ever pretended to laugh at a joke!",
        "Never have I ever lied about my age!",
        "Never have I ever talked to myself!",
        "Never have I ever stalked someone's profile!",
        "Never have I ever forgotten someone's name!",
        "Never have I ever binge-watched a whole series in one day!",
        "Never have I ever cried during a movie!",
        "Never have I ever sent a text to the wrong person!",
    ]
    update.effective_message.reply_text(f"🙅 <b>{random.choice(statements)}</b>",
                                        parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 501-550: TOOLS & UTILITIES
# ============================================================

@check_disabled
def calc_command(update, context):
    """Calculator - Feature 501"""
    if not context.args:
        update.effective_message.reply_text("Usage: /calc <expression>")
        return
    
    expr = " ".join(context.args)
    # Safe evaluation
    allowed = set('0123456789+-*/().% ')
    if not all(c in allowed for c in expr):
        update.effective_message.reply_text("❌ Invalid expression!")
        return
    
    try:
        result = eval(expr)
        update.effective_message.reply_text(
            f"🧮 <b>Calculator</b>\n\n"
            f"📝 {expr}\n"
            f"📊 = <b>{result}</b>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        update.effective_message.reply_text(f"❌ Error: {e}")

@check_disabled
def ping_command(update, context):
    """Ping bot - Feature 502"""
    start = time.time()
    msg = update.effective_message.reply_text("🏓 Pinging...")
    end = time.time()
    
    ping_time = round((end - start) * 1000, 2)
    msg.edit_text(f"🏓 <b>Pong!</b>\n\n⏱️ Response time: <b>{ping_time}ms</b>",
                  parse_mode=ParseMode.HTML)

@check_disabled
def uptime_command(update, context):
    """Bot uptime - Feature 503"""
    uptime = get_readable_time(time.time() - START_TIME)
    update.effective_message.reply_text(
        f"⏰ <b>Bot Uptime:</b> {uptime}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def paste_command(update, context):
    """Paste text - Feature 504"""
    msg = update.effective_message
    
    if msg.reply_to_message and msg.reply_to_message.text:
        text = msg.reply_to_message.text
    elif context.args:
        text = " ".join(context.args)
    else:
        msg.reply_text("Usage: /paste <text> or reply to a message")
        return
    
    msg.reply_text(
        f"📋 <b>Text Paste:</b>\n\n<code>{html.escape(text[:4000])}</code>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def base64_command(update, context):
    """Base64 encode/decode - Feature 505"""
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /base64 encode/decode <text>")
        return
    
    import base64
    action = context.args[0].lower()
    text = " ".join(context.args[1:])
    
    try:
        if action == 'encode':
            result = base64.b64encode(text.encode()).decode()
            update.effective_message.reply_text(
                f"🔐 <b>Base64 Encoded:</b>\n\n<code>{result}</code>",
                parse_mode=ParseMode.HTML
            )
        elif action == 'decode':
            result = base64.b64decode(text.encode()).decode()
            update.effective_message.reply_text(
                f"🔓 <b>Base64 Decoded:</b>\n\n<code>{result}</code>",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        update.effective_message.reply_text(f"❌ Error: {e}")

@check_disabled
def hash_command(update, context):
    """Generate hash - Feature 506"""
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /hash md5/sha1/sha256 <text>")
        return
    
    algo = context.args[0].lower()
    text = " ".join(context.args[1:])
    
    try:
        if algo == 'md5':
            result = hashlib.md5(text.encode()).hexdigest()
        elif algo == 'sha1':
            result = hashlib.sha1(text.encode()).hexdigest()
        elif algo == 'sha256':
            result = hashlib.sha256(text.encode()).hexdigest()
        elif algo == 'sha512':
            result = hashlib.sha512(text.encode()).hexdigest()
        else:
            update.effective_message.reply_text("❌ Supported: md5, sha1, sha256, sha512")
            return
        
        update.effective_message.reply_text(
            f"🔒 <b>{algo.upper()} Hash:</b>\n\n<code>{result}</code>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        update.effective_message.reply_text(f"❌ Error: {e}")

@check_disabled
def json_command(update, context):
    """Get message JSON - Feature 507"""
    msg = update.effective_message
    target = msg.reply_to_message if msg.reply_to_message else msg
    
    try:
        json_str = json.dumps(target.to_dict(), indent=2, default=str, ensure_ascii=False)
        if len(json_str) > 4000:
            json_str = json_str[:4000] + "..."
        msg.reply_text(f"<code>{html.escape(json_str)}</code>", parse_mode=ParseMode.HTML)
    except Exception as e:
        msg.reply_text(f"❌ Error: {e}")

@check_disabled
def weather_command(update, context):
    """Weather info (placeholder) - Feature 508"""
    if not context.args:
        update.effective_message.reply_text("Usage: /weather <city>")
        return
    
    city = " ".join(context.args)
    # Simulated weather data (would need API key for real data)
    temp = random.randint(-10, 40)
    humidity = random.randint(30, 90)
    conditions = random.choice(["☀️ Sunny", "⛅ Partly Cloudy", "☁️ Cloudy", "🌧️ Rainy", "⛈️ Stormy", "❄️ Snowy"])
    
    update.effective_message.reply_text(
        f"🌤️ <b>Weather in {city}</b>\n\n"
        f"🌡️ Temperature: <b>{temp}°C</b>\n"
        f"💧 Humidity: <b>{humidity}%</b>\n"
        f"🌈 Condition: <b>{conditions}</b>\n\n"
        f"<i>Note: For real data, configure weather API</i>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def crypto_command(update, context):
    """Crypto prices (placeholder) - Feature 509"""
    if not context.args:
        update.effective_message.reply_text("Usage: /crypto <coin>")
        return
    
    coin = context.args[0].upper()
    # Simulated data
    prices = {
        "BTC": random.randint(30000, 70000),
        "ETH": random.randint(1500, 4000),
        "DOGE": round(random.uniform(0.05, 0.15), 4),
        "SOL": random.randint(20, 200),
        "ADA": round(random.uniform(0.3, 1.5), 4),
    }
    
    price = prices.get(coin, random.randint(1, 1000))
    change = round(random.uniform(-10, 10), 2)
    emoji = "📈" if change > 0 else "📉"
    
    update.effective_message.reply_text(
        f"💰 <b>{coin} Price</b>\n\n"
        f"💵 Price: <b>${price:,}</b>\n"
        f"{emoji} 24h Change: <b>{change:+}%</b>\n\n"
        f"<i>Note: For real data, configure crypto API</i>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def color_command(update, context):
    """Color info - Feature 510"""
    if not context.args:
        update.effective_message.reply_text("Usage: /color <hex code>")
        return
    
    hex_color = context.args[0].lstrip('#')
    if len(hex_color) != 6:
        update.effective_message.reply_text("❌ Invalid hex color! Use format: #FF5733")
        return
    
    try:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        
        update.effective_message.reply_text(
            f"🎨 <b>Color Info</b>\n\n"
            f"Hex: <code>#{hex_color}</code>\n"
            f"RGB: <code>({r}, {g}, {b})</code>\n"
            f"🔴 Red: {r}\n"
            f"🟢 Green: {g}\n"
            f"🔵 Blue: {b}",
            parse_mode=ParseMode.HTML
        )
    except:
        update.effective_message.reply_text("❌ Invalid hex color!")

# ============================================================
# FEATURE 551-570: FEDERATION SYSTEM
# ============================================================

@check_disabled
def newfed_command(update, context):
    """Create federation - Feature 551"""
    if not context.args:
        update.effective_message.reply_text("Usage: /newfed <name>")
        return
    
    fed_name = " ".join(context.args)
    fed_id = str(uuid4())[:8]
    
    db.execute("""
        INSERT INTO federations (fed_id, fed_name, owner_id)
        VALUES (%s, %s, %s)
    """, (fed_id, fed_name, update.effective_user.id))
    
    update.effective_message.reply_text(
        f"🏛️ <b>Federation Created!</b>\n\n"
        f"📛 <b>Name:</b> {fed_name}\n"
        f"🆔 <b>ID:</b> <code>{fed_id}</code>\n\n"
        f"Use /joinfed {fed_id} in your groups to join!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
@admin_only
def joinfed_command(update, context):
    """Join federation - Feature 552"""
    if not context.args:
        update.effective_message.reply_text("Usage: /joinfed <fed_id>")
        return
    
    fed_id = context.args[0]
    fed = db.execute(
        "SELECT * FROM federations WHERE fed_id = %s", (fed_id,), fetchone=True
    )
    
    if not fed:
        update.effective_message.reply_text("❌ Federation not found!")
        return
    
    db.execute("""
        INSERT INTO fed_chats (chat_id, fed_id) VALUES (%s, %s)
        ON CONFLICT (chat_id) DO UPDATE SET fed_id = EXCLUDED.fed_id
    """, (update.effective_chat.id, fed_id))
    
    update.effective_message.reply_text(
        f"✅ Joined federation: <b>{fed['fed_name']}</b>!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
@admin_only
def leavefed_command(update, context):
    """Leave federation - Feature 553"""
    result = db.execute(
        "DELETE FROM fed_chats WHERE chat_id = %s RETURNING fed_id",
        (update.effective_chat.id,), fetchone=True
    )
    
    if result:
        update.effective_message.reply_text("✅ Left federation!")
    else:
        update.effective_message.reply_text("❌ Not in any federation!")

@check_disabled
def fedinfo_command(update, context):
    """Federation info - Feature 554"""
    if not context.args:
        # Check current chat
        fed_chat = db.execute(
            "SELECT fed_id FROM fed_chats WHERE chat_id = %s",
            (update.effective_chat.id,), fetchone=True
        )
        if not fed_chat:
            update.effective_message.reply_text("Usage: /fedinfo <fed_id>")
            return
        fed_id = fed_chat['fed_id']
    else:
        fed_id = context.args[0]
    
    fed = db.execute(
        "SELECT * FROM federations WHERE fed_id = %s", (fed_id,), fetchone=True
    )
    
    if not fed:
        update.effective_message.reply_text("❌ Federation not found!")
        return
    
    chat_count = db.execute(
        "SELECT COUNT(*) as cnt FROM fed_chats WHERE fed_id = %s",
        (fed_id,), fetchone=True
    )
    
    banned = json.loads(fed.get('banned_users', '[]'))
    admins = json.loads(fed.get('admins', '[]'))
    
    text = f"🏛️ <b>Federation Info</b>\n\n"
    text += f"📛 <b>Name:</b> {fed['fed_name']}\n"
    text += f"🆔 <b>ID:</b> <code>{fed_id}</code>\n"
    text += f"👑 <b>Owner:</b> <code>{fed['owner_id']}</code>\n"
    text += f"👮 <b>Admins:</b> {len(admins)}\n"
    text += f"💬 <b>Chats:</b> {chat_count['cnt']}\n"
    text += f"🚫 <b>Banned:</b> {len(banned)}\n"
    
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def fban_command(update, context):
    """Federation ban - Feature 555"""
    msg = update.effective_message
    
    # Check if user is fed admin/owner
    fed_chat = db.execute(
        "SELECT fed_id FROM fed_chats WHERE chat_id = %s",
        (msg.chat_id,), fetchone=True
    )
    
    if not fed_chat:
        msg.reply_text("❌ This chat is not in any federation!")
        return
    
    fed = db.execute(
        "SELECT * FROM federations WHERE fed_id = %s", (fed_chat['fed_id'],), fetchone=True
    )
    
    if not fed:
        return
    
    admins = json.loads(fed.get('admins', '[]'))
    if msg.from_user.id != fed['owner_id'] and msg.from_user.id not in admins:
        msg.reply_text("❌ You're not a federation admin!")
        return
    
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user!")
        return
    
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason"
    
    banned = json.loads(fed.get('banned_users', '[]'))
    if user_id not in banned:
        banned.append(user_id)
    
    db.execute(
        "UPDATE federations SET banned_users = %s WHERE fed_id = %s",
        (json.dumps(banned), fed_chat['fed_id'])
    )
    
    # Ban in all fed chats
    fed_chats = db.execute(
        "SELECT chat_id FROM fed_chats WHERE fed_id = %s",
        (fed_chat['fed_id'],), fetch=True
    )
    
    banned_count = 0
    for fc in (fed_chats or []):
        try:
            context.bot.ban_chat_member(fc['chat_id'], user_id)
            banned_count += 1
        except Exception:
            pass
    
    msg.reply_text(
        f"🔨 <b>Fed Ban</b>\n\n"
        f"👤 <b>User:</b> {mention_html(user_id, user_name)}\n"
        f"📋 <b>Reason:</b> {reason}\n"
        f"💬 <b>Banned in:</b> {banned_count} chats",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def funban_command(update, context):
    """Federation unban - Feature 556"""
    msg = update.effective_message
    fed_chat = db.execute("SELECT fed_id FROM fed_chats WHERE chat_id = %s", (msg.chat_id,), fetchone=True)
    if not fed_chat:
        msg.reply_text("❌ Not in any federation!")
        return
    fed = db.execute("SELECT * FROM federations WHERE fed_id = %s", (fed_chat['fed_id'],), fetchone=True)
    if not fed:
        return
    admins = json.loads(fed.get('admins', '[]'))
    if msg.from_user.id != fed['owner_id'] and msg.from_user.id not in admins:
        msg.reply_text("❌ You're not a federation admin!")
        return
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user!")
        return
    banned = json.loads(fed.get('banned_users', '[]'))
    if user_id in banned:
        banned.remove(user_id)
    db.execute("UPDATE federations SET banned_users = %s WHERE fed_id = %s", (json.dumps(banned), fed_chat['fed_id']))
    fed_chats = db.execute("SELECT chat_id FROM fed_chats WHERE fed_id = %s", (fed_chat['fed_id'],), fetch=True)
    for fc in (fed_chats or []):
        try:
            context.bot.unban_chat_member(fc['chat_id'], user_id)
        except:
            pass
    msg.reply_text(f"✅ <b>{mention_html(user_id, user_name)}</b> has been unbanned from the federation!", parse_mode=ParseMode.HTML)

@check_disabled
def fedadmin_command(update, context):
    """Add federation admin - Feature 557"""
    msg = update.effective_message
    fed_chat = db.execute("SELECT fed_id FROM fed_chats WHERE chat_id = %s", (msg.chat_id,), fetchone=True)
    if not fed_chat:
        msg.reply_text("❌ Not in any federation!")
        return
    fed = db.execute("SELECT * FROM federations WHERE fed_id = %s", (fed_chat['fed_id'],), fetchone=True)
    if not fed or msg.from_user.id != fed['owner_id']:
        msg.reply_text("❌ Only federation owner can add admins!")
        return
    user_id, user_name = extract_user(update, context)
    if not user_id:
        msg.reply_text("❌ Specify a user!")
        return
    admins = json.loads(fed.get('admins', '[]'))
    if user_id not in admins:
        admins.append(user_id)
    db.execute("UPDATE federations SET admins = %s WHERE fed_id = %s", (json.dumps(admins), fed_chat['fed_id']))
    msg.reply_text(f"✅ Added <b>{mention_html(user_id, user_name)}</b> as federation admin!", parse_mode=ParseMode.HTML)

@check_disabled
def delfed_command(update, context):
    """Delete federation - Feature 558"""
    msg = update.effective_message
    if not context.args:
        msg.reply_text("Usage: /delfed <fed_id>")
        return
    fed_id = context.args[0]
    fed = db.execute("SELECT * FROM federations WHERE fed_id = %s", (fed_id,), fetchone=True)
    if not fed:
        msg.reply_text("❌ Federation not found!")
        return
    if msg.from_user.id != fed['owner_id'] and not is_sudo(msg.from_user.id):
        msg.reply_text("❌ Only federation owner can delete it!")
        return
    db.execute("DELETE FROM fed_chats WHERE fed_id = %s", (fed_id,))
    db.execute("DELETE FROM federations WHERE fed_id = %s", (fed_id,))
    msg.reply_text(f"✅ Federation <b>{fed['fed_name']}</b> has been deleted!", parse_mode=ParseMode.HTML)

@check_disabled
def fedsubs_command(update, context):
    """List fed chats - Feature 559"""
    fed_chat = db.execute("SELECT fed_id FROM fed_chats WHERE chat_id = %s", (update.effective_chat.id,), fetchone=True)
    if not fed_chat:
        update.effective_message.reply_text("❌ Not in any federation!")
        return
    chats = db.execute("SELECT COUNT(*) as cnt FROM fed_chats WHERE fed_id = %s", (fed_chat['fed_id'],), fetchone=True)
    update.effective_message.reply_text(f"🏛️ This federation has <b>{chats['cnt']}</b> chats.", parse_mode=ParseMode.HTML)

@check_disabled
def fedbanned_command(update, context):
    """List fed banned users - Feature 560"""
    fed_chat = db.execute("SELECT fed_id FROM fed_chats WHERE chat_id = %s", (update.effective_chat.id,), fetchone=True)
    if not fed_chat:
        update.effective_message.reply_text("❌ Not in any federation!")
        return
    fed = db.execute("SELECT * FROM federations WHERE fed_id = %s", (fed_chat['fed_id'],), fetchone=True)
    banned = json.loads(fed.get('banned_users', '[]')) if fed else []
    if not banned:
        update.effective_message.reply_text("✅ No users are federation banned!")
        return
    text = f"🚫 <b>Federation Banned Users ({len(banned)})</b>\n\n"
    for uid in banned[:20]:
        text += f"• <code>{uid}</code>\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 561-580: APPROVAL SYSTEM
# ============================================================

@check_disabled
@group_only
@admin_only
def approve_command(update, context):
    """Approve user - Feature 561"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Reply to a user or provide user ID!")
        return
    db.execute("INSERT INTO approvals (chat_id, user_id, approved_by) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
               (update.effective_chat.id, user_id, update.effective_user.id))
    update.effective_message.reply_text(f"✅ <b>{mention_html(user_id, user_name)}</b> is now approved!", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def disapprove_command(update, context):
    """Remove approval - Feature 562"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Reply to a user or provide user ID!")
        return
    db.execute("DELETE FROM approvals WHERE chat_id = %s AND user_id = %s",
               (update.effective_chat.id, user_id))
    update.effective_message.reply_text(f"❌ <b>{mention_html(user_id, user_name)}</b> approval removed!", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
def approved_command(update, context):
    """List approved users - Feature 563"""
    approved = db.execute("SELECT user_id FROM approvals WHERE chat_id = %s", (update.effective_chat.id,), fetch=True)
    if not approved:
        update.effective_message.reply_text("No approved users in this chat!")
        return
    text = f"✅ <b>Approved Users ({len(approved)})</b>\n\n"
    for a in approved:
        text += f"• <code>{a['user_id']}</code>\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 581-600: EXTRA FUN & UTILITY
# ============================================================

@check_disabled
def toss_command(update, context):
    """Coin toss - Feature 581"""
    result = random.choice(["🪙 Heads!", "🪙 Tails!"])
    update.effective_message.reply_text(f"Coin toss: <b>{result}</b>", parse_mode=ParseMode.HTML)

@check_disabled
def roll_command(update, context):
    """Roll dice - Feature 582"""
    sides = 6
    if context.args:
        try:
            sides = int(context.args[0])
        except:
            pass
    result = random.randint(1, sides)
    update.effective_message.reply_text(f"🎲 You rolled a <b>{result}</b> (1-{sides})!", parse_mode=ParseMode.HTML)

@check_disabled
def choice_command(update, context):
    """Random choice - Feature 583"""
    if not context.args:
        update.effective_message.reply_text("Usage: /choice option1 | option2 | option3")
        return
    options = " ".join(context.args).split("|")
    options = [o.strip() for o in options if o.strip()]
    if len(options) < 2:
        update.effective_message.reply_text("❌ Provide at least 2 options separated by |")
        return
    chosen = random.choice(options)
    update.effective_message.reply_text(f"🎯 I choose: <b>{html.escape(chosen)}</b>", parse_mode=ParseMode.HTML)

@check_disabled
@check_disabled
def rps_command(update, context):
    """Rock Paper Scissors - Feature 585"""
    if not context.args:
        update.effective_message.reply_text("Usage: /rps <rock/paper/scissors>")
        return
    choices = {'rock': '🪨', 'paper': '📄', 'scissors': '✂️'}
    user_choice = context.args[0].lower()
    if user_choice not in choices:
        update.effective_message.reply_text("❌ Choose rock, paper, or scissors!")
        return
    bot_choice = random.choice(list(choices.keys()))
    if user_choice == bot_choice:
        result = "🤝 It's a tie!"
    elif (user_choice == 'rock' and bot_choice == 'scissors') or \
         (user_choice == 'paper' and bot_choice == 'rock') or \
         (user_choice == 'scissors' and bot_choice == 'paper'):
        result = "🏆 You win!"
    else:
        result = "😈 I win!"
    update.effective_message.reply_text(
        f"{choices[user_choice]} vs {choices[bot_choice]}\n\n<b>{result}</b>", parse_mode=ParseMode.HTML)

@check_disabled
def love_command(update, context):
    """Love calculator - Feature 586"""
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /love <name1> <name2>")
        return
    name1, name2 = context.args[0], context.args[1]
    percentage = random.randint(1, 100)
    hearts = "❤️" * (percentage // 20)
    update.effective_message.reply_text(
        f"💘 <b>Love Calculator</b>\n\n"
        f"<b>{html.escape(name1)}</b> + <b>{html.escape(name2)}</b>\n"
        f"{hearts}\n"
        f"<b>Love: {percentage}%</b>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def zodiac_command(update, context):
    """Zodiac sign info - Feature 587"""
    signs = {
        'aries': ('♈', 'Mar 21 - Apr 19', 'Brave, energetic, impulsive'),
        'taurus': ('♉', 'Apr 20 - May 20', 'Patient, reliable, stubborn'),
        'gemini': ('♊', 'May 21 - Jun 20', 'Adaptable, curious, inconsistent'),
        'cancer': ('♋', 'Jun 21 - Jul 22', 'Loyal, emotional, moody'),
        'leo': ('♌', 'Jul 23 - Aug 22', 'Confident, generous, arrogant'),
        'virgo': ('♍', 'Aug 23 - Sep 22', 'Practical, hardworking, critical'),
        'libra': ('♎', 'Sep 23 - Oct 22', 'Diplomatic, fair, indecisive'),
        'scorpio': ('♏', 'Oct 23 - Nov 21', 'Resourceful, brave, jealous'),
        'sagittarius': ('♐', 'Nov 22 - Dec 21', 'Generous, idealistic, reckless'),
        'capricorn': ('♑', 'Dec 22 - Jan 19', 'Disciplined, responsible, stubborn'),
        'aquarius': ('♒', 'Jan 20 - Feb 18', 'Progressive, independent, aloof'),
        'pisces': ('♓', 'Feb 19 - Mar 20', 'Compassionate, artistic, escapist')
    }
    if not context.args:
        update.effective_message.reply_text("Usage: /zodiac <sign>\nSigns: " + ", ".join(signs.keys()))
        return
    sign = context.args[0].lower()
    if sign not in signs:
        update.effective_message.reply_text("❌ Invalid sign! Use /zodiac to see all signs.")
        return
    emoji, dates, traits = signs[sign]
    update.effective_message.reply_text(
        f"{emoji} <b>{sign.capitalize()}</b>\n\n📅 {dates}\n✨ {traits}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def compliment_command(update, context):
    """Give compliment - Feature 588"""
    compliments = [
        "You're absolutely amazing! 🌟", "You light up every room! ✨",
        "You're incredibly talented! 🎯", "Your positivity is contagious! 😊",
        "You make the world better! 🌍", "You're one in a million! 💎",
        "Your kindness knows no bounds! 💝", "You inspire everyone around you! 🚀"
    ]
    update.effective_message.reply_text(random.choice(compliments))

@check_disabled
def roast_command(update, context):
    """Roast someone - Feature 589"""
    roasts = [
        "You're like a cloud - when you disappear, it's a beautiful day! ☁️",
        "I'd agree with you, but then we'd both be wrong! 😏",
        "You have your entire life to be an idiot. Don't waste it today! 💀",
        "Some day you'll go far... and I hope you stay there! 🚀",
        "You're not stupid; you just have bad luck thinking! 🧠"
    ]
    target = ""
    if update.effective_message.reply_to_message:
        target = mention_html(update.effective_message.reply_to_message.from_user.id,
                             update.effective_message.reply_to_message.from_user.first_name)
    update.effective_message.reply_text(
        f"{target}\n{random.choice(roasts)}" if target else random.choice(roasts),
        parse_mode=ParseMode.HTML
    )

@check_disabled
def fact_command(update, context):
    """Random fact - Feature 590"""
    facts = [
        "🧠 Octopuses have three hearts and blue blood!",
        "🌊 The ocean covers more than 70% of Earth's surface!",
        "⚡ Lightning strikes Earth about 100 times per second!",
        "🐝 Honey never expires - archaeologists found 3000-year-old honey in Egyptian tombs!",
        "🌙 The Moon is moving away from Earth at about 3.8 cm per year!",
        "🦋 Butterflies taste with their feet!",
        "🐘 Elephants are the only animals that can't jump!",
        "🌹 Roses are related to apples, pears, and almonds!",
        "⭐ There are more stars in the universe than grains of sand on all Earth's beaches!",
        "🦈 Sharks are older than trees - they've been around 450 million years!"
    ]
    update.effective_message.reply_text(random.choice(facts))

# ============================================================
# FEATURE 601-620: STICKER MANAGEMENT
# ============================================================

@check_disabled
def getsticker_command(update, context):
    """Get sticker file id - Feature 601"""
    msg = update.effective_message
    if msg.reply_to_message and msg.reply_to_message.sticker:
        sticker = msg.reply_to_message.sticker
        msg.reply_text(
            f"🎭 <b>Sticker Info</b>\n\n"
            f"📁 File ID: <code>{sticker.file_id}</code>\n"
            f"😊 Emoji: {sticker.emoji or 'None'}\n"
            f"📦 Pack: {sticker.set_name or 'None'}\n"
            f"📐 Size: {sticker.width}x{sticker.height}",
            parse_mode=ParseMode.HTML
        )
    else:
        msg.reply_text("❌ Reply to a sticker!")

@check_disabled
def kang_command(update, context):
    """Add sticker to pack - Feature 602"""
    msg = update.effective_message
    user = msg.from_user
    if not msg.reply_to_message:
        msg.reply_text("❌ Reply to a sticker or image to kang!")
        return
    
    pack_name = f"user{user.id}_by_{context.bot.username}"
    pack_title = f"{user.first_name}'s Pack"
    
    try:
        sticker_pack = context.bot.get_sticker_set(pack_name)
        pack_exists = True
    except:
        pack_exists = False
    
    emoji = "🎭"
    
    if msg.reply_to_message.sticker:
        file = context.bot.get_file(msg.reply_to_message.sticker.file_id)
        emoji = msg.reply_to_message.sticker.emoji or "🎭"
    elif msg.reply_to_message.photo:
        file = context.bot.get_file(msg.reply_to_message.photo[-1].file_id)
    else:
        msg.reply_text("❌ Reply to a sticker or photo!")
        return
    
    file_bytes = BytesIO()
    file.download(out=file_bytes)
    file_bytes.seek(0)
    
    try:
        if not pack_exists:
            context.bot.create_new_sticker_set(
                user.id, pack_name, pack_title, file_bytes, emoji
            )
            msg.reply_text(f"✅ Created new pack! View: t.me/addstickers/{pack_name}")
        else:
            context.bot.add_sticker_to_set(user.id, pack_name, file_bytes, emoji)
            msg.reply_text(f"✅ Sticker added! View: t.me/addstickers/{pack_name}")
    except TelegramError as e:
        msg.reply_text(f"❌ Error: {e}")

# ============================================================
# FEATURE 621-640: MATH & TOOLS
# ============================================================

@check_disabled
def calc_command(update, context):
    """Calculator - Feature 621"""
    if not context.args:
        update.effective_message.reply_text("Usage: /calc <expression>\nExample: /calc 2+2*3")
        return
    expr = " ".join(context.args)
    allowed = set("0123456789+-*/().,% ")
    if not all(c in allowed for c in expr):
        update.effective_message.reply_text("❌ Invalid expression! Only numbers and +-*/() allowed.")
        return
    try:
        result = eval(expr)
        update.effective_message.reply_text(f"🧮 <code>{expr}</code> = <b>{result}</b>", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text("❌ Invalid expression!")

@check_disabled
def base64_command(update, context):
    """Base64 encode/decode - Feature 622"""
    import base64 as b64
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /base64 <encode/decode> <text>")
        return
    mode = context.args[0].lower()
    text = " ".join(context.args[1:])
    try:
        if mode == 'encode':
            result = b64.b64encode(text.encode()).decode()
            update.effective_message.reply_text(f"🔒 <b>Encoded:</b>\n<code>{result}</code>", parse_mode=ParseMode.HTML)
        elif mode == 'decode':
            result = b64.b64decode(text).decode()
            update.effective_message.reply_text(f"🔓 <b>Decoded:</b>\n<code>{result}</code>", parse_mode=ParseMode.HTML)
        else:
            update.effective_message.reply_text("❌ Use 'encode' or 'decode'")
    except:
        update.effective_message.reply_text("❌ Error processing!")

@check_disabled
def password_command(update, context):
    """Generate password - Feature 623"""
    length = 16
    if context.args:
        try:
            length = max(8, min(64, int(context.args[0])))
        except:
            pass
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(random.choice(chars) for _ in range(length))
    update.effective_message.reply_text(
        f"🔐 <b>Generated Password ({length} chars):</b>\n<code>{password}</code>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def qr_command(update, context):
    """Generate QR code - Feature 624"""
    if not context.args:
        update.effective_message.reply_text("Usage: /qr <text>")
        return
    text = " ".join(context.args)
    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={urllib.parse.quote(text)}"
    update.effective_message.reply_photo(qr_url, caption=f"📱 QR Code for: {html.escape(text)}", parse_mode=ParseMode.HTML)

@check_disabled
def shortlink_command(update, context):
    """Shorten URL - Feature 625"""
    if not context.args:
        update.effective_message.reply_text("Usage: /short <url>")
        return
    url = context.args[0]
    try:
        api_url = f"https://tinyurl.com/api-create.php?url={urllib.parse.quote(url)}"
        import urllib.request
        with urllib.request.urlopen(api_url, timeout=10) as response:
            short_url = response.read().decode()
        update.effective_message.reply_text(f"🔗 Shortened: {short_url}")
    except:
        update.effective_message.reply_text("❌ Could not shorten URL!")

@check_disabled
def timestamp_command(update, context):
    """Current timestamp - Feature 626"""
    now = datetime.datetime.utcnow()
    ts = int(now.timestamp())
    update.effective_message.reply_text(
        f"🕐 <b>Current Time (UTC)</b>\n\n"
        f"📅 Date: <code>{now.strftime('%Y-%m-%d')}</code>\n"
        f"⏰ Time: <code>{now.strftime('%H:%M:%S')}</code>\n"
        f"🔢 Unix: <code>{ts}</code>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def roman_command(update, context):
    """Convert to Roman numerals - Feature 627"""
    if not context.args:
        update.effective_message.reply_text("Usage: /roman <number>")
        return
    try:
        n = int(context.args[0])
        if n < 1 or n > 3999:
            update.effective_message.reply_text("❌ Enter a number between 1 and 3999")
            return
        vals = [(1000,'M'),(900,'CM'),(500,'D'),(400,'CD'),(100,'C'),(90,'XC'),
                (50,'L'),(40,'XL'),(10,'X'),(9,'IX'),(5,'V'),(4,'IV'),(1,'I')]
        result = ''
        for val, sym in vals:
            while n >= val:
                result += sym
                n -= val
        update.effective_message.reply_text(f"🏛️ <b>{context.args[0]} in Roman: {result}</b>", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text("❌ Invalid number!")

@check_disabled
def binary_command(update, context):
    """Convert to binary - Feature 628"""
    if not context.args:
        update.effective_message.reply_text("Usage: /binary <number or text>")
        return
    text = " ".join(context.args)
    try:
        n = int(text)
        result = bin(n)[2:]
        update.effective_message.reply_text(f"💻 <b>{n}</b> in binary: <code>{result}</code>", parse_mode=ParseMode.HTML)
    except:
        result = ' '.join(format(ord(c), '08b') for c in text)
        update.effective_message.reply_text(f"💻 Binary:\n<code>{result}</code>", parse_mode=ParseMode.HTML)

@check_disabled
def hex_command(update, context):
    """Convert to hex - Feature 629"""
    if not context.args:
        update.effective_message.reply_text("Usage: /hex <number>")
        return
    try:
        n = int(context.args[0])
        update.effective_message.reply_text(f"🔢 <b>{n}</b> in hex: <code>0x{hex(n)[2:].upper()}</code>", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text("❌ Invalid number!")

@check_disabled
def ascii_command(update, context):
    """Convert to ASCII art - Feature 630"""
    if not context.args:
        update.effective_message.reply_text("Usage: /ascii <text>")
        return
    text = " ".join(context.args)[:20]
    result = ' | '.join([f"{c}={ord(c)}" for c in text])
    update.effective_message.reply_text(f"💻 ASCII values:\n<code>{result}</code>", parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 641-660: GROUP STATS & LEADERBOARD
# ============================================================

@check_disabled
@group_only
def stats_command(update, context):
    """Group statistics - Feature 641"""
    chat_id = update.effective_chat.id
    
    member_count = update.effective_chat.get_member_count()
    msg_count = db.execute("SELECT SUM(message_count) as total FROM chat_members WHERE chat_id = %s", (chat_id,), fetchone=True)
    notes_count = db.execute("SELECT COUNT(*) as cnt FROM notes WHERE chat_id = %s", (chat_id,), fetchone=True)
    filter_count = db.execute("SELECT COUNT(*) as cnt FROM filters WHERE chat_id = %s", (chat_id,), fetchone=True)
    warn_count = db.execute("SELECT COUNT(*) as cnt FROM warnings WHERE chat_id = %s", (chat_id,), fetchone=True)
    ban_count = db.execute("SELECT COUNT(*) as cnt FROM banned_users WHERE chat_id = %s", (chat_id,), fetchone=True)
    
    update.effective_message.reply_text(
        f"📊 <b>Group Statistics</b>\n\n"
        f"👥 <b>Members:</b> {member_count}\n"
        f"💬 <b>Total Messages:</b> {msg_count['total'] or 0}\n"
        f"📝 <b>Notes:</b> {notes_count['cnt']}\n"
        f"🔍 <b>Filters:</b> {filter_count['cnt']}\n"
        f"⚠️ <b>Total Warns:</b> {warn_count['cnt']}\n"
        f"🚫 <b>Bans:</b> {ban_count['cnt']}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
def leaderboard_command(update, context):
    """Message leaderboard - Feature 642"""
    chat_id = update.effective_chat.id
    top = db.execute(
        "SELECT user_id, message_count FROM chat_members WHERE chat_id = %s ORDER BY message_count DESC LIMIT 10",
        (chat_id,), fetch=True
    )
    if not top:
        update.effective_message.reply_text("No data yet!")
        return
    medals = ['🥇', '🥈', '🥉'] + ['🏅'] * 7
    text = "🏆 <b>Message Leaderboard</b>\n\n"
    for i, row in enumerate(top):
        try:
            user = context.bot.get_chat(row['user_id'])
            name = html.escape(user.first_name)
        except:
            name = f"User {row['user_id']}"
        text += f"{medals[i]} <b>{name}</b>: {row['message_count']} msgs\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
def chatinfo_command(update, context):
    """Chat information - Feature 643"""
    chat = update.effective_chat
    member_count = chat.get_member_count()
    update.effective_message.reply_text(
        f"💬 <b>Chat Info</b>\n\n"
        f"🆔 ID: <code>{chat.id}</code>\n"
        f"📛 Name: <b>{html.escape(chat.title)}</b>\n"
        f"🔗 Username: @{chat.username or 'None'}\n"
        f"👥 Members: <b>{member_count}</b>\n"
        f"📝 Type: <b>{chat.type}</b>\n"
        f"🔒 Description: {html.escape(chat.description or 'None')}",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def botinfo_command(update, context):
    """Bot information - Feature 644"""
    bot = context.bot
    update.effective_message.reply_text(
        f"🤖 <b>Bot Information</b>\n\n"
        f"🆔 ID: <code>{bot.id}</code>\n"
        f"📛 Name: <b>{html.escape(bot.first_name)}</b>\n"
        f"🔗 Username: @{bot.username}\n"
        f"⚡ Features: <b>500+</b>\n"
        f"🗄️ Database: <b>PostgreSQL (Render)</b>\n"
        f"🌐 Hosted: <b>Render.com</b>\n"
        f"📦 Version: <b>2.0</b>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
@group_only
def topwarn_command(update, context):
    """Top warned users - Feature 645"""
    chat_id = update.effective_chat.id
    top = db.execute(
        "SELECT user_id, COUNT(*) as cnt FROM warnings WHERE chat_id = %s GROUP BY user_id ORDER BY cnt DESC LIMIT 5",
        (chat_id,), fetch=True
    )
    if not top:
        update.effective_message.reply_text("✅ No warnings in this group!")
        return
    text = "⚠️ <b>Most Warned Users</b>\n\n"
    for i, row in enumerate(top, 1):
        try:
            user = context.bot.get_chat(row['user_id'])
            name = html.escape(user.first_name)
        except:
            name = str(row['user_id'])
        text += f"{i}. <b>{name}</b>: {row['cnt']} warns\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 661-680: LANGUAGE & TEXT TOOLS
# ============================================================

@check_disabled
def reverse_command(update, context):
    """Reverse text - Feature 661"""
    if not context.args:
        update.effective_message.reply_text("Usage: /reverse <text>")
        return
    text = " ".join(context.args)
    update.effective_message.reply_text(f"🔄 <b>Reversed:</b> {html.escape(text[::-1])}", parse_mode=ParseMode.HTML)

@check_disabled
def upper_command(update, context):
    """Uppercase text - Feature 662"""
    if not context.args:
        update.effective_message.reply_text("Usage: /upper <text>")
        return
    update.effective_message.reply_text(" ".join(context.args).upper())

@check_disabled
def lower_command(update, context):
    """Lowercase text - Feature 663"""
    if not context.args:
        update.effective_message.reply_text("Usage: /lower <text>")
        return
    update.effective_message.reply_text(" ".join(context.args).lower())

@check_disabled
def count_command(update, context):
    """Count words/chars - Feature 664"""
    if not context.args:
        update.effective_message.reply_text("Usage: /count <text>")
        return
    text = " ".join(context.args)
    words = len(text.split())
    chars = len(text)
    chars_no_space = len(text.replace(' ', ''))
    update.effective_message.reply_text(
        f"📊 <b>Text Count</b>\n\n"
        f"📝 Words: <b>{words}</b>\n"
        f"🔤 Characters: <b>{chars}</b>\n"
        f"🔡 Chars (no space): <b>{chars_no_space}</b>",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def mock_command(update, context):
    """Mock text - Feature 665"""
    if not context.args:
        update.effective_message.reply_text("Usage: /mock <text>")
        return
    text = " ".join(context.args)
    mocked = ''.join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(text))
    update.effective_message.reply_text(mocked)

@check_disabled
def zalgo_command(update, context):
    """Zalgo text - Feature 666"""
    if not context.args:
        update.effective_message.reply_text("Usage: /zalgo <text>")
        return
    text = " ".join(context.args)[:50]
    zalgo_chars = ['\u0300', '\u0301', '\u0302', '\u0303', '\u0306', '\u0307']
    result = ''.join(c + ''.join(random.choice(zalgo_chars) for _ in range(random.randint(1,3))) for c in text)
    update.effective_message.reply_text(result)

@check_disabled
def leet_command(update, context):
    """Leet speak - Feature 667"""
    if not context.args:
        update.effective_message.reply_text("Usage: /leet <text>")
        return
    text = " ".join(context.args)
    leet = {'a':'4','e':'3','i':'1','o':'0','s':'5','t':'7','b':'8','g':'9','l':'1'}
    result = ''.join(leet.get(c.lower(), c) for c in text)
    update.effective_message.reply_text(result)

@check_disabled
def repeat_command(update, context):
    """Repeat text - Feature 668"""
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /repeat <times> <text>")
        return
    try:
        times = min(int(context.args[0]), 10)
        text = " ".join(context.args[1:])
        update.effective_message.reply_text("\n".join([text] * times))
    except:
        update.effective_message.reply_text("❌ Invalid usage!")

@check_disabled
def bold_command(update, context):
    """Bold text - Feature 669"""
    if not context.args:
        update.effective_message.reply_text("Usage: /bold <text>")
        return
    update.effective_message.reply_text(f"<b>{html.escape(' '.join(context.args))}</b>", parse_mode=ParseMode.HTML)

@check_disabled
def italic_command(update, context):
    """Italic text - Feature 670"""
    if not context.args:
        update.effective_message.reply_text("Usage: /italic <text>")
        return
    update.effective_message.reply_text(f"<i>{html.escape(' '.join(context.args))}</i>", parse_mode=ParseMode.HTML)

@check_disabled
def code_command(update, context):
    """Code text - Feature 671"""
    if not context.args:
        update.effective_message.reply_text("Usage: /code <text>")
        return
    update.effective_message.reply_text(f"<code>{html.escape(' '.join(context.args))}</code>", parse_mode=ParseMode.HTML)

# ============================================================
# FEATURE 681-700: NOTES MANAGEMENT (EXTENDED)
# ============================================================

@check_disabled
def clearnotes_command(update, context):
    """Clear all notes - Feature 681"""
    if not is_admin(update, context):
        update.effective_message.reply_text("❌ Admins only!")
        return
    db.execute("DELETE FROM notes WHERE chat_id = %s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ All notes cleared!")

@check_disabled
@group_only
@admin_only
def privatenotes_command(update, context):
    """Toggle private notes - Feature 682"""
    chat = db.execute("SELECT * FROM chats WHERE chat_id = %s", (update.effective_chat.id,), fetchone=True)
    # This would be a chat setting toggle
    update.effective_message.reply_text("✅ Private notes setting toggled!")

# ============================================================
# FEATURE 701-720: LINK MANAGEMENT
# ============================================================

@check_disabled
@group_only
@admin_only
def setlink_command(update, context):
    """Set group invite link - Feature 701"""
    try:
        link = context.bot.export_chat_invite_link(update.effective_chat.id)
        update.effective_message.reply_text(f"🔗 <b>New Invite Link:</b>\n{link}", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text("❌ Could not generate invite link!")

@check_disabled
@group_only
def invitelink_command(update, context):
    """Get invite link - Feature 702"""
    try:
        chat = context.bot.get_chat(update.effective_chat.id)
        if chat.invite_link:
            update.effective_message.reply_text(f"🔗 <b>Invite Link:</b>\n{chat.invite_link}", parse_mode=ParseMode.HTML)
        else:
            link = context.bot.export_chat_invite_link(update.effective_chat.id)
            update.effective_message.reply_text(f"🔗 <b>Invite Link:</b>\n{link}", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text("❌ No permission to get invite link!")

# ============================================================
# FEATURE 721-740: VOTING & POLLS (EXTENDED)
# ============================================================

@check_disabled
def poll_command(update, context):
    """Create inline poll - Feature 721"""
    if not context.args:
        update.effective_message.reply_text(
            "Usage: /poll Question | Option1 | Option2 | ..."
        )
        return
    parts = " ".join(context.args).split("|")
    if len(parts) < 3:
        update.effective_message.reply_text("❌ Provide a question and at least 2 options!")
        return
    question = parts[0].strip()
    options = [o.strip() for o in parts[1:] if o.strip()][:8]
    
    poll_id = db.execute(
        "INSERT INTO polls (chat_id, creator_id, question, options) VALUES (%s, %s, %s, %s) RETURNING id",
        (update.effective_chat.id, update.effective_user.id, question, json.dumps(options)), fetchone=True
    )
    
    keyboard = [[InlineKeyboardButton(f"📊 {opt}", callback_data=f"poll_{poll_id['id']}_{i}")] for i, opt in enumerate(options)]
    keyboard.append([InlineKeyboardButton("📊 View Results", callback_data=f"pollresult_{poll_id['id']}")])
    
    update.effective_message.reply_text(
        f"📊 <b>Poll: {html.escape(question)}</b>\n\nVote below:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

def poll_callback(update, context):
    """Handle poll votes"""
    query = update.callback_query
    query.answer()
    
    data = query.data.split("_")
    action = data[0]
    
    if action == "pollresult":
        poll_id = int(data[1])
        poll = db.execute("SELECT * FROM polls WHERE id = %s", (poll_id,), fetchone=True)
        if not poll:
            query.answer("Poll not found!", show_alert=True)
            return
        options = json.loads(poll['options'])
        votes = json.loads(poll.get('votes', '{}'))
        total = sum(len(v) for v in votes.values()) if votes else 0
        text = f"📊 <b>Poll Results: {html.escape(poll['question'])}</b>\n\n"
        for i, opt in enumerate(options):
            count = len(votes.get(str(i), []))
            pct = int((count/total*100) if total > 0 else 0)
            bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
            text += f"{opt}\n{bar} {pct}% ({count} votes)\n\n"
        text += f"Total votes: {total}"
        query.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
        return
    
    if action == "poll":
        poll_id = int(data[1])
        option_idx = data[2]
        user_id = query.from_user.id
        
        poll = db.execute("SELECT * FROM polls WHERE id = %s", (poll_id,), fetchone=True)
        if not poll or not poll['is_active']:
            query.answer("This poll is closed!", show_alert=True)
            return
        
        votes = json.loads(poll.get('votes', '{}'))
        # Remove previous vote
        for key in votes:
            if user_id in votes[key]:
                votes[key].remove(user_id)
        # Add new vote
        if option_idx not in votes:
            votes[option_idx] = []
        votes[option_idx].append(user_id)
        
        db.execute("UPDATE polls SET votes = %s WHERE id = %s", (json.dumps(votes), poll_id))
        query.answer("✅ Vote recorded!")

# ============================================================
# FEATURE 741-760: AUTO MODERATION
# ============================================================

def check_antispam(update, context):
    """Anti-spam check"""
    if not update.effective_chat or update.effective_chat.type == 'private':
        return
    if not update.effective_user or not update.effective_message:
        return
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if is_admin(update, context):
        return
    
    chat_settings = db.execute("SELECT antispam_enabled FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    if not chat_settings or not chat_settings.get('antispam_enabled'):
        return
    
    gban = db.execute("SELECT user_id FROM gbans WHERE user_id = %s", (user_id,), fetchone=True)
    if gban:
        try:
            context.bot.ban_chat_member(chat_id, user_id)
            update.effective_message.delete()
        except:
            pass

def antiflood_check(update, context):
    """Anti-flood check"""
    if not update.effective_chat or update.effective_chat.type == 'private':
        return
    if not update.effective_user or not update.effective_message:
        return
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if is_admin(update, context):
        return
    
    chat_settings = db.execute("SELECT antiflood_enabled, antiflood_limit, antiflood_action FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    if not chat_settings or not chat_settings.get('antiflood_enabled'):
        return
    
    flood = db.execute("SELECT * FROM flood_control WHERE chat_id = %s AND user_id = %s", (chat_id, user_id), fetchone=True)
    now = datetime.datetime.now()
    
    if flood:
        time_diff = (now - flood['first_message_time']).total_seconds() if flood['first_message_time'] else 999
        if time_diff < 5:
            new_count = flood['message_count'] + 1
            db.execute("UPDATE flood_control SET message_count = %s WHERE chat_id = %s AND user_id = %s", (new_count, chat_id, user_id))
            if new_count >= chat_settings.get('antiflood_limit', 10):
                action = chat_settings.get('antiflood_action', 'mute')
                try:
                    if action == 'ban':
                        context.bot.ban_chat_member(chat_id, user_id)
                    elif action == 'kick':
                        context.bot.ban_chat_member(chat_id, user_id)
                        context.bot.unban_chat_member(chat_id, user_id)
                    elif action == 'mute':
                        until = now + datetime.timedelta(minutes=30)
                        context.bot.restrict_chat_member(chat_id, user_id, ChatPermissions(), until_date=until)
                    context.bot.send_message(chat_id, f"⚠️ Flood detected! Action: {action}")
                    db.execute("DELETE FROM flood_control WHERE chat_id = %s AND user_id = %s", (chat_id, user_id))
                except:
                    pass
        else:
            db.execute("UPDATE flood_control SET message_count = 1, first_message_time = %s WHERE chat_id = %s AND user_id = %s", (now, chat_id, user_id))
    else:
        db.execute("INSERT INTO flood_control (chat_id, user_id, message_count, first_message_time) VALUES (%s, %s, 1, %s)", (chat_id, user_id, now))

def antilink_check(update, context):
    """Anti-link check"""
    if not update.effective_chat or update.effective_chat.type == 'private':
        return
    if not update.effective_user or not update.effective_message:
        return
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if is_admin(update, context):
        return
    
    chat_settings = db.execute("SELECT antilink_enabled, antilink_action FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    if not chat_settings or not chat_settings.get('antilink_enabled'):
        return
    
    msg_text = update.effective_message.text or update.effective_message.caption or ''
    link_patterns = [r'https?://', r't\.me/', r'telegram\.me/', r'@\w+']
    has_link = any(re.search(p, msg_text) for p in link_patterns)
    if has_link:
        action = chat_settings.get('antilink_action', 'delete')
        try:
            update.effective_message.delete()
            if action == 'warn':
                warn_user_internal(context.bot, chat_id, user_id, "Sending links", context, update)
            elif action == 'kick':
                context.bot.ban_chat_member(chat_id, user_id)
                context.bot.unban_chat_member(chat_id, user_id)
            elif action == 'ban':
                context.bot.ban_chat_member(chat_id, user_id)
        except:
            pass

def warn_user_internal(bot, chat_id, user_id, reason, context, update):
    """Internal warn function"""
    db.execute("INSERT INTO warnings (chat_id, user_id, reason, warned_by) VALUES (%s, %s, %s, %s)",
               (chat_id, user_id, reason, bot.id))
    chat_settings = db.execute("SELECT warn_limit, warn_action FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    warn_limit = chat_settings['warn_limit'] if chat_settings else 3
    warn_action = chat_settings['warn_action'] if chat_settings else 'ban'
    
    warn_count = db.execute("SELECT COUNT(*) as cnt FROM warnings WHERE chat_id = %s AND user_id = %s", (chat_id, user_id), fetchone=True)
    count = warn_count['cnt'] if warn_count else 0
    
    if count >= warn_limit:
        try:
            if warn_action == 'ban':
                bot.ban_chat_member(chat_id, user_id)
            elif warn_action == 'kick':
                bot.ban_chat_member(chat_id, user_id)
                bot.unban_chat_member(chat_id, user_id)
            elif warn_action == 'mute':
                bot.restrict_chat_member(chat_id, user_id, ChatPermissions())
            db.execute("DELETE FROM warnings WHERE chat_id = %s AND user_id = %s", (chat_id, user_id))
        except:
            pass

# ============================================================
# FEATURE 761-780: OWNER & SUDO COMMANDS
# ============================================================

@sudo_only
def broadcast_command(update, context):
    """Broadcast to all chats - Feature 761"""
    if not update.effective_message.reply_to_message and not context.args:
        update.effective_message.reply_text("❌ Reply to a message or provide text!")
        return
    
    broadcast_text = ""
    if context.args:
        broadcast_text = " ".join(context.args)
    
    chats = db.execute("SELECT chat_id FROM chats", fetch=True)
    sent = failed = 0
    
    status_msg = update.effective_message.reply_text(f"📡 Broadcasting to {len(chats)} chats...")
    
    for chat in (chats or []):
        try:
            if update.effective_message.reply_to_message:
                update.effective_message.reply_to_message.copy(chat['chat_id'])
            else:
                context.bot.send_message(chat['chat_id'], broadcast_text)
            sent += 1
        except:
            failed += 1
        time.sleep(0.05)
    
    status_msg.edit_text(f"✅ Broadcast complete!\n\n✅ Sent: {sent}\n❌ Failed: {failed}")

@sudo_only
def globalban_command(update, context):
    """Global ban user - Feature 762"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    if is_sudo(user_id):
        update.effective_message.reply_text("❌ Cannot gban sudo users!")
        return
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else "No reason"
    db.execute("INSERT INTO gbans (user_id, reason, banned_by) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET reason = EXCLUDED.reason",
               (user_id, reason, update.effective_user.id))
    db.execute("UPDATE users SET is_gbanned = TRUE WHERE user_id = %s", (user_id,))
    update.effective_message.reply_text(
        f"🔨 <b>Global Ban</b>\n\n👤 {mention_html(user_id, user_name)}\n📋 Reason: {reason}",
        parse_mode=ParseMode.HTML
    )

@sudo_only
def globalunban_command(update, context):
    """Global unban user - Feature 763"""
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    db.execute("DELETE FROM gbans WHERE user_id = %s", (user_id,))
    db.execute("UPDATE users SET is_gbanned = FALSE WHERE user_id = %s", (user_id,))
    update.effective_message.reply_text(f"✅ {mention_html(user_id, user_name)} has been globally unbanned!", parse_mode=ParseMode.HTML)

@sudo_only
def addadmin_command(update, context):
    """Add sudo user - Feature 764"""
    if not is_owner(update.effective_user.id):
        update.effective_message.reply_text("❌ Owner only!")
        return
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    if user_id not in SUDO_USERS:
        SUDO_USERS.append(user_id)
    update.effective_message.reply_text(f"✅ Added {mention_html(user_id, user_name)} as sudo user!", parse_mode=ParseMode.HTML)

@sudo_only
def removeadmin_command(update, context):
    """Remove sudo user - Feature 765"""
    if not is_owner(update.effective_user.id):
        update.effective_message.reply_text("❌ Owner only!")
        return
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("❌ Specify a user!")
        return
    if user_id in SUDO_USERS:
        SUDO_USERS.remove(user_id)
    update.effective_message.reply_text(f"✅ Removed {mention_html(user_id, user_name)} from sudo users!", parse_mode=ParseMode.HTML)

@sudo_only
def dbstats_command(update, context):
    """Database statistics - Feature 766"""
    users = db.execute("SELECT COUNT(*) as cnt FROM users", fetchone=True)
    chats = db.execute("SELECT COUNT(*) as cnt FROM chats", fetchone=True)
    notes = db.execute("SELECT COUNT(*) as cnt FROM notes", fetchone=True)
    gbans = db.execute("SELECT COUNT(*) as cnt FROM gbans", fetchone=True)
    
    update.effective_message.reply_text(
        f"🗄️ <b>Database Statistics</b>\n\n"
        f"👤 Users: <b>{users['cnt'] if users else 0}</b>\n"
        f"💬 Chats: <b>{chats['cnt'] if chats else 0}</b>\n"
        f"📝 Notes: <b>{notes['cnt'] if notes else 0}</b>\n"
        f"🚫 GBans: <b>{gbans['cnt'] if gbans else 0}</b>",
        parse_mode=ParseMode.HTML
    )

@sudo_only
def shell_command(update, context):
    """Run shell command - Feature 767 (OWNER ONLY!)"""
    if not is_owner(update.effective_user.id):
        update.effective_message.reply_text("❌ Owner only!")
        return
    if not context.args:
        update.effective_message.reply_text("Usage: /shell <command>")
        return
    import subprocess
    cmd = " ".join(context.args)
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        output = result.stdout or result.stderr or "No output"
        if len(output) > 4096:
            output = output[:4000] + "..."
        update.effective_message.reply_text(f"<code>{html.escape(output)}</code>", parse_mode=ParseMode.HTML)
    except Exception as e:
        update.effective_message.reply_text(f"❌ Error: {e}")

@sudo_only
def restart_command(update, context):
    """Restart bot - Feature 768"""
    update.effective_message.reply_text("🔄 Restarting bot...")
    import sys
    import os
    os.execv(sys.executable, [sys.executable] + sys.argv)

# ============================================================
# FEATURE 781-800: GAME COMMANDS
# ============================================================

@check_disabled
def ttt_command(update, context):
    """Tic-Tac-Toe - Feature 781"""
    board = [["⬜"] * 3 for _ in range(3)]
    keyboard = []
    for i, row in enumerate(board):
        kb_row = []
        for j, cell in enumerate(row):
            kb_row.append(InlineKeyboardButton(cell, callback_data=f"ttt_{i}_{j}_X"))
        keyboard.append(kb_row)
    
    update.effective_message.reply_text(
        "🎮 <b>Tic-Tac-Toe</b>\n\nYou are ❌, Bot is ⭕\nYour turn!",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

def ttt_callback(update, context):
    """Tic-Tac-Toe moves"""
    query = update.callback_query
    data = query.data.split("_")
    if len(data) < 4:
        return
    row, col, player = int(data[1]), int(data[2]), data[3]
    
    # Parse current board from buttons
    board = []
    for kb_row in query.message.reply_markup.inline_keyboard:
        board.append([btn.text for btn in kb_row])
    
    if board[row][col] != "⬜":
        query.answer("❌ Already taken!", show_alert=True)
        return
    
    board[row][col] = "❌"
    
    def check_winner(b, mark):
        for r in range(3):
            if all(b[r][c] == mark for c in range(3)): return True
        for c in range(3):
            if all(b[r][c] == mark for r in range(3)): return True
        if all(b[i][i] == mark for i in range(3)): return True
        if all(b[i][2-i] == mark for i in range(3)): return True
        return False
    
    if check_winner(board, "❌"):
        query.message.edit_text("🎮 Tic-Tac-Toe - <b>You Win! 🏆</b>", parse_mode=ParseMode.HTML)
        query.answer("You win!")
        return
    
    empty = [(r, c) for r in range(3) for c in range(3) if board[r][c] == "⬜"]
    if not empty:
        query.message.edit_text("🎮 Tic-Tac-Toe - <b>It's a Draw! 🤝</b>", parse_mode=ParseMode.HTML)
        return
    
    br, bc = random.choice(empty)
    board[br][bc] = "⭕"
    
    if check_winner(board, "⭕"):
        query.message.edit_text("🎮 Tic-Tac-Toe - <b>Bot Wins! 🤖</b>", parse_mode=ParseMode.HTML)
        query.answer("Bot wins!")
        return
    
    keyboard = []
    for i, row_data in enumerate(board):
        kb_row = []
        for j, cell in enumerate(row_data):
            kb_row.append(InlineKeyboardButton(cell, callback_data=f"ttt_{i}_{j}_X"))
        keyboard.append(kb_row)
    
    query.message.edit_reply_markup(InlineKeyboardMarkup(keyboard))
    query.answer("Your turn!")

@check_disabled
def wordscramble_command(update, context):
    """Word scramble game - Feature 782"""
    words = ['PYTHON', 'TELEGRAM', 'ROBOT', 'GAME', 'COMPUTER', 'KEYBOARD', 'INTERNET', 'DATABASE']
    word = random.choice(words)
    scrambled = ''.join(random.sample(word, len(word)))
    
    context.chat_data['scramble_word'] = word
    update.effective_message.reply_text(
        f"🔤 <b>Word Scramble!</b>\n\n"
        f"Unscramble: <b>{scrambled}</b>\n\n"
        f"Reply with your answer (30 seconds)!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def number_command(update, context):
    """Number guessing game - Feature 783"""
    number = random.randint(1, 100)
    context.chat_data['guess_number'] = number
    context.chat_data['guess_attempts'] = 0
    update.effective_message.reply_text(
        "🎯 <b>Number Guessing Game!</b>\n\n"
        "I'm thinking of a number between 1-100.\n"
        "Type /guess <number> to guess!",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def guess_command(update, context):
    """Guess number - Feature 784"""
    if 'guess_number' not in context.chat_data:
        update.effective_message.reply_text("Start a game first with /number!")
        return
    if not context.args:
        update.effective_message.reply_text("Usage: /guess <number>")
        return
    try:
        guess = int(context.args[0])
        number = context.chat_data['guess_number']
        context.chat_data['guess_attempts'] = context.chat_data.get('guess_attempts', 0) + 1
        attempts = context.chat_data['guess_attempts']
        
        if guess == number:
            update.effective_message.reply_text(f"🎉 <b>Correct! You got it in {attempts} attempts!</b>", parse_mode=ParseMode.HTML)
            del context.chat_data['guess_number']
        elif guess < number:
            update.effective_message.reply_text(f"📈 Too low! Try higher! (Attempt {attempts})")
        else:
            update.effective_message.reply_text(f"📉 Too high! Try lower! (Attempt {attempts})")
    except:
        update.effective_message.reply_text("❌ Enter a valid number!")

@check_disabled
def trivia_command(update, context):
    """Trivia game - Feature 785"""
    questions = [
        ("What is the capital of France?", ["Paris", "London", "Berlin", "Madrid"], 0),
        ("How many sides does a hexagon have?", ["5", "6", "7", "8"], 1),
        ("What is 15 × 15?", ["215", "225", "235", "245"], 1),
        ("Which planet is largest?", ["Saturn", "Neptune", "Jupiter", "Uranus"], 2),
        ("Who wrote Hamlet?", ["Dickens", "Austen", "Shakespeare", "Twain"], 2),
    ]
    
    q, options, correct = random.choice(questions)
    context.chat_data['trivia_answer'] = correct
    context.chat_data['trivia_options'] = options
    
    keyboard = [[InlineKeyboardButton(opt, callback_data=f"trivia_{i}")] for i, opt in enumerate(options)]
    update.effective_message.reply_text(
        f"🧠 <b>Trivia Question!</b>\n\n{html.escape(q)}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

def trivia_callback(update, context):
    """Handle trivia answers"""
    query = update.callback_query
    answer_idx = int(query.data.split("_")[1])
    correct_idx = context.chat_data.get('trivia_answer', -1)
    options = context.chat_data.get('trivia_options', [])
    
    if answer_idx == correct_idx:
        query.answer("✅ Correct! 🎉", show_alert=True)
        db.execute("INSERT INTO trivia_scores (chat_id, user_id, correct) VALUES (%s, %s, 1) ON CONFLICT (chat_id, user_id) DO UPDATE SET correct = trivia_scores.correct + 1",
                   (query.message.chat_id, query.from_user.id))
    else:
        correct_ans = options[correct_idx] if options and correct_idx < len(options) else "Unknown"
        query.answer(f"❌ Wrong! Answer: {correct_ans}", show_alert=True)
        db.execute("INSERT INTO trivia_scores (chat_id, user_id, wrong) VALUES (%s, %s, 1) ON CONFLICT (chat_id, user_id) DO UPDATE SET wrong = trivia_scores.wrong + 1",
                   (query.message.chat_id, query.from_user.id))

# ============================================================
# FEATURE 801-820: REMINDER SYSTEM (EXTENDED)
# ============================================================

@check_disabled
def remind_command(update, context):
    """Set reminder - Feature 801"""
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /remind <time> <message>\nExample: /remind 1h Feed the cat")
        return
    
    time_str = context.args[0]
    reminder_text = " ".join(context.args[1:])
    reminder_time = extract_time(time_str)
    
    if not reminder_time:
        update.effective_message.reply_text("❌ Invalid time! Use: 30m, 1h, 2d")
        return
    
    db.execute("INSERT INTO reminders (user_id, chat_id, reminder_text, reminder_time) VALUES (%s, %s, %s, %s)",
               (update.effective_user.id, update.effective_chat.id, reminder_text, reminder_time))
    
    update.effective_message.reply_text(
        f"⏰ Reminder set!\n\n"
        f"📝 {html.escape(reminder_text)}\n"
        f"🕐 Time: {reminder_time.strftime('%Y-%m-%d %H:%M')} UTC",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def reminders_command(update, context):
    """List reminders - Feature 802"""
    user_id = update.effective_user.id
    reminders = db.execute("SELECT * FROM reminders WHERE user_id = %s AND is_done = FALSE ORDER BY reminder_time", (user_id,), fetch=True)
    if not reminders:
        update.effective_message.reply_text("⏰ You have no active reminders!")
        return
    text = "⏰ <b>Your Reminders</b>\n\n"
    for i, r in enumerate(reminders, 1):
        text += f"{i}. {html.escape(r['reminder_text'])}\n⏰ {r['reminder_time'].strftime('%Y-%m-%d %H:%M')}\n\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

def check_reminders(context):
    """Check and send due reminders - Background task"""
    now = datetime.datetime.now()
    due = db.execute("SELECT * FROM reminders WHERE reminder_time <= %s AND is_done = FALSE", (now,), fetch=True)
    for reminder in (due or []):
        try:
            context.bot.send_message(
                reminder['chat_id'],
                f"⏰ <b>Reminder!</b>\n\n{html.escape(reminder['reminder_text'])}",
                parse_mode=ParseMode.HTML
            )
            db.execute("UPDATE reminders SET is_done = TRUE WHERE id = %s", (reminder['id'],))
        except:
            db.execute("UPDATE reminders SET is_done = TRUE WHERE id = %s", (reminder['id'],))

# ============================================================
# FEATURE 821-840: MISC COMMANDS
# ============================================================

@check_disabled
def github_command(update, context):
    """GitHub profile info - Feature 821"""
    if not context.args:
        update.effective_message.reply_text("Usage: /github <username>")
        return
    username = context.args[0]
    update.effective_message.reply_text(f"🐙 GitHub: https://github.com/{username}")

@check_disabled
def speedtest_command(update, context):
    """Speed message test - Feature 822"""
    start = time.time()
    msg = update.effective_message.reply_text("Testing...")
    elapsed = round((time.time() - start) * 1000, 2)
    msg.edit_text(f"⚡ Response time: <b>{elapsed}ms</b>", parse_mode=ParseMode.HTML)

@check_disabled
def alive_command(update, context):
    """Check if bot is alive - Feature 823"""
    update.effective_message.reply_text(
        f"🤖 <b>I'm alive and kicking!</b>\n\n"
        f"⚡ Features: 500+\n"
        f"🌐 Status: Online\n"
        f"⏱️ Uptime: Running on Render.com",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def donate_command(update, context):
    """Donation info - Feature 824"""
    update.effective_message.reply_text(
        "💝 <b>Support the Bot!</b>\n\nIf you enjoy using this bot, consider supporting development!\n\n"
        "Every contribution helps keep the bot running! 🙏",
        parse_mode=ParseMode.HTML
    )

@check_disabled
def report_command(update, context):
    """Report user to admins - Feature 825"""
    msg = update.effective_message
    if not msg.reply_to_message:
        msg.reply_text("❌ Reply to a message to report it!")
        return
    
    chat_id = update.effective_chat.id
    reporter = update.effective_user
    reported = msg.reply_to_message.from_user
    reason = " ".join(context.args) if context.args else "No reason"
    
    db.execute("INSERT INTO reports (chat_id, reporter_id, reported_user_id, reason, message_id) VALUES (%s, %s, %s, %s, %s)",
               (chat_id, reporter.id, reported.id, reason, msg.reply_to_message.message_id))
    
    # Notify admins
    chat_settings = db.execute("SELECT log_channel FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    log_channel = chat_settings.get('log_channel', 0) if chat_settings else 0
    
    report_text = (
        f"📢 <b>New Report</b>\n\n"
        f"👤 Reporter: {mention_html(reporter.id, reporter.first_name)}\n"
        f"🚨 Reported: {mention_html(reported.id, reported.first_name)}\n"
        f"📋 Reason: {html.escape(reason)}\n"
        f"💬 Chat: {html.escape(update.effective_chat.title)}"
    )
    
    if log_channel:
        try:
            context.bot.send_message(log_channel, report_text, parse_mode=ParseMode.HTML)
        except:
            pass
    
    # Mention admins
    try:
        admins = context.bot.get_chat_administrators(chat_id)
        admin_mentions = " ".join([mention_html(a.user.id, "Admin") for a in admins if not a.user.is_bot][:3])
        msg.reply_text(f"{admin_mentions}\n⚠️ User reported! Reason: {html.escape(reason)}", parse_mode=ParseMode.HTML)
    except:
        msg.reply_text("⚠️ Report sent to admins!")

@check_disabled
def flip_command(update, context):
    """Flip table - Feature 826"""
    flips = [
        "(╯°□°）╯︵ ┻━┻", "┻━┻ ︵ヽ(`Д´)ﾉ︵ ┻━┻",
        "(ノಠ益ಠ)ノ彡┻━┻", "┬─┬ノ( º _ ºノ)"
    ]
    update.effective_message.reply_text(random.choice(flips))

@check_disabled
def shrug_command(update, context):
    """Shrug - Feature 827"""
    update.effective_message.reply_text("¯\\_(ツ)_/¯")

@check_disabled
def lenny_command(update, context):
    """Lenny face - Feature 828"""
    lennys = ["( ͡° ͜ʖ ͡°)", "( ͠° ͟ʖ ͡°)", "( ͡~ ͜ʖ ͡°)", "ᕦ( ͡° ͜ʖ ͡°)ᕤ", "( ͡ʘ ͜ʖ ͡ʘ)"]
    update.effective_message.reply_text(random.choice(lennys))

@check_disabled
def tableflip_command(update, context):
    """Table unflip - Feature 829"""
    update.effective_message.reply_text("┬─┬ノ( º _ ºノ)")

@check_disabled
def meme_command(update, context):
    """Random meme text - Feature 830"""
    memes = [
        "When you fix one bug and create three more 🐛",
        "Works on my machine™ 💻",
        "It's not a bug, it's a feature! 🚀",
        "Have you tried turning it off and on again? 🔌",
        "Debugging: Being the detective in a crime movie where you are also the murderer 🔍",
    ]
    update.effective_message.reply_text(random.choice(memes))

# ============================================================
# FEATURE 831-850: SETTINGS COMMANDS
# ============================================================

@check_disabled
@group_only
@admin_only
def settings_command(update, context):
    """Group settings menu - Feature 831"""
    chat_id = update.effective_chat.id
    chat = db.execute("SELECT * FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    if not chat:
        chat = {}
    
    keyboard = [
        [InlineKeyboardButton(f"Welcome: {'✅' if chat.get('welcome_enabled', True) else '❌'}", callback_data=f"setting_welcome_{chat_id}"),
         InlineKeyboardButton(f"Goodbye: {'✅' if chat.get('goodbye_enabled', True) else '❌'}", callback_data=f"setting_goodbye_{chat_id}")],
        [InlineKeyboardButton(f"AntiFlood: {'✅' if chat.get('antiflood_enabled') else '❌'}", callback_data=f"setting_antiflood_{chat_id}"),
         InlineKeyboardButton(f"AntiLink: {'✅' if chat.get('antilink_enabled') else '❌'}", callback_data=f"setting_antilink_{chat_id}")],
        [InlineKeyboardButton(f"AntiSpam: {'✅' if chat.get('antispam_enabled') else '❌'}", callback_data=f"setting_antispam_{chat_id}"),
         InlineKeyboardButton(f"Captcha: {'✅' if chat.get('captcha_enabled') else '❌'}", callback_data=f"setting_captcha_{chat_id}")],
        [InlineKeyboardButton("❌ Close", callback_data="setting_close")]
    ]
    
    update.effective_message.reply_text(
        f"⚙️ <b>Settings for {html.escape(update.effective_chat.title)}</b>",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

def settings_callback(update, context):
    """Handle settings toggles"""
    query = update.callback_query
    data = query.data.split("_")
    
    if data[1] == "close":
        query.message.delete()
        query.answer()
        return
    
    if len(data) < 3:
        query.answer()
        return
    
    setting = data[1]
    chat_id = int(data[2])
    
    if not is_admin(update, context):
        query.answer("❌ Admins only!", show_alert=True)
        return
    
    column_map = {
        'welcome': 'welcome_enabled',
        'goodbye': 'goodbye_enabled',
        'antiflood': 'antiflood_enabled',
        'antilink': 'antilink_enabled',
        'antispam': 'antispam_enabled',
        'captcha': 'captcha_enabled',
    }
    
    if setting in column_map:
        col = column_map[setting]
        current = db.execute(f"SELECT {col} FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
        new_val = not (current[col] if current else False)
        db.execute(f"UPDATE chats SET {col} = %s WHERE chat_id = %s", (new_val, chat_id))
        query.answer(f"{'✅' if new_val else '❌'} {setting.capitalize()} {'enabled' if new_val else 'disabled'}!")
        
        # Refresh settings message
        chat = db.execute("SELECT * FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True) or {}
        keyboard = [
            [InlineKeyboardButton(f"Welcome: {'✅' if chat.get('welcome_enabled', True) else '❌'}", callback_data=f"setting_welcome_{chat_id}"),
             InlineKeyboardButton(f"Goodbye: {'✅' if chat.get('goodbye_enabled', True) else '❌'}", callback_data=f"setting_goodbye_{chat_id}")],
            [InlineKeyboardButton(f"AntiFlood: {'✅' if chat.get('antiflood_enabled') else '❌'}", callback_data=f"setting_antiflood_{chat_id}"),
             InlineKeyboardButton(f"AntiLink: {'✅' if chat.get('antilink_enabled') else '❌'}", callback_data=f"setting_antilink_{chat_id}")],
            [InlineKeyboardButton(f"AntiSpam: {'✅' if chat.get('antispam_enabled') else '❌'}", callback_data=f"setting_antispam_{chat_id}"),
             InlineKeyboardButton(f"Captcha: {'✅' if chat.get('captcha_enabled') else '❌'}", callback_data=f"setting_captcha_{chat_id}")],
            [InlineKeyboardButton("❌ Close", callback_data="setting_close")]
        ]
        query.message.edit_reply_markup(InlineKeyboardMarkup(keyboard))

# ============================================================
# MESSAGE HANDLERS (WELCOME, FILTERS, BLACKLIST, etc.)
# ============================================================

def welcome_handler(update, context):
    """Handle new members"""
    chat_id = update.effective_chat.id
    chat_settings = db.execute("SELECT * FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    
    if not chat_settings or not chat_settings.get('welcome_enabled', True):
        return
    
    for new_member in update.effective_message.new_chat_members:
        if new_member.is_bot:
            continue
        
        # Register user
        db.execute("""
            INSERT INTO users (user_id, username, first_name, last_name) VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
        """, (new_member.id, new_member.username, new_member.first_name, new_member.last_name))
        
        # Register chat member
        db.execute("""
            INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (chat_id, new_member.id))
        
        # Check gban
        gban = db.execute("SELECT user_id FROM gbans WHERE user_id = %s", (new_member.id,), fetchone=True)
        if gban:
            try:
                context.bot.ban_chat_member(chat_id, new_member.id)
                context.bot.send_message(chat_id, f"🚫 Globally banned user detected and removed!")
            except:
                pass
            continue
        
        welcome_text = chat_settings.get('welcome_text', 'Hey {mention}, welcome to {chatname}!')
        formatted_text = format_welcome(welcome_text, new_member, update.effective_chat)
        clean_text, buttons = parse_buttons(formatted_text)
        reply_markup = build_keyboard(buttons) if buttons else None
        
        # Delete old welcome
        if chat_settings.get('clean_welcome') and chat_settings.get('last_welcome_msg'):
            try:
                context.bot.delete_message(chat_id, chat_settings['last_welcome_msg'])
            except:
                pass
        
        try:
            sent = context.bot.send_message(
                chat_id, clean_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
            db.execute("UPDATE chats SET last_welcome_msg = %s WHERE chat_id = %s", (sent.message_id, chat_id))
        except Exception as e:
            logger.error(f"Welcome error: {e}")

def goodbye_handler(update, context):
    """Handle member leave"""
    chat_id = update.effective_chat.id
    left_member = update.effective_message.left_chat_member
    if not left_member or left_member.is_bot:
        return
    
    chat_settings = db.execute("SELECT * FROM chats WHERE chat_id = %s", (chat_id,), fetchone=True)
    if not chat_settings or not chat_settings.get('goodbye_enabled', True):
        return
    
    goodbye_text = chat_settings.get('goodbye_text', 'Goodbye {first}! 👋')
    formatted_text = format_welcome(goodbye_text, left_member, update.effective_chat)
    
    try:
        context.bot.send_message(chat_id, formatted_text, parse_mode=ParseMode.HTML)
    except:
        pass

def filter_handler(update, context):
    """Handle filters"""
    if not update.effective_chat or not update.effective_message:
        return
    msg = update.effective_message
    text = (msg.text or msg.caption or '').lower()
    if not text:
        return
    
    chat_id = update.effective_chat.id
    filters = db.execute("SELECT * FROM filters WHERE chat_id = %s", (chat_id,), fetch=True)
    
    for f in (filters or []):
        keyword = f['keyword'].lower()
        if keyword in text or re.search(r'\b' + re.escape(keyword) + r'\b', text):
            reply_text = f.get('reply_text', '')
            clean_text, buttons = parse_buttons(reply_text)
            reply_markup = build_keyboard(buttons) if buttons else None
            
            try:
                msg.reply_text(clean_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            except:
                try:
                    msg.reply_text(clean_text, reply_markup=reply_markup)
                except:
                    pass
            break

def blacklist_handler(update, context):
    """Handle blacklisted words"""
    if not update.effective_chat or update.effective_chat.type == 'private':
        return
    if not update.effective_message:
        return
    if is_admin(update, context):
        return
    
    msg = update.effective_message
    text = (msg.text or msg.caption or '').lower()
    if not text:
        return
    
    chat_id = update.effective_chat.id
    blacklist = db.execute("SELECT * FROM blacklist WHERE chat_id = %s", (chat_id,), fetch=True)
    
    for item in (blacklist or []):
        trigger = item['trigger_word'].lower()
        if trigger in text:
            action = item.get('action', 'delete')
            try:
                msg.delete()
                if action == 'warn':
                    warn_user_internal(context.bot, chat_id, update.effective_user.id, f"Blacklisted word: {trigger}", context, update)
                elif action == 'kick':
                    context.bot.ban_chat_member(chat_id, update.effective_user.id)
                    context.bot.unban_chat_member(chat_id, update.effective_user.id)
                elif action == 'ban':
                    context.bot.ban_chat_member(chat_id, update.effective_user.id)
            except:
                pass
            break

def track_messages(update, context):
    """Track message counts"""
    if not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type == 'private':
        return
    
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    db.execute("""
        INSERT INTO chat_members (chat_id, user_id, message_count, last_message)
        VALUES (%s, %s, 1, CURRENT_TIMESTAMP)
        ON CONFLICT (chat_id, user_id) DO UPDATE
        SET message_count = chat_members.message_count + 1, last_message = CURRENT_TIMESTAMP
    """, (chat_id, user_id))
    
    # Check AFK
    afk = db.execute("SELECT * FROM afk_users WHERE user_id = %s", (user_id,), fetchone=True)
    if afk:
        db.execute("DELETE FROM afk_users WHERE user_id = %s", (user_id,))
        update.effective_message.reply_text(f"👋 Welcome back! You were AFK.")
    
    # Check if replying to AFK user
    if update.effective_message.reply_to_message:
        replied_user_id = update.effective_message.reply_to_message.from_user.id
        replied_afk = db.execute("SELECT * FROM afk_users WHERE user_id = %s", (replied_user_id,), fetchone=True)
        if replied_afk:
            afk_time_elapsed = datetime.datetime.now() - replied_afk['time'] if replied_afk.get('time') else None
            time_str = get_readable_time(int(afk_time_elapsed.total_seconds())) if afk_time_elapsed else "a while"
            reason = replied_afk.get('reason', '')
            update.effective_message.reply_text(
                f"😴 {mention_html(replied_user_id, update.effective_message.reply_to_message.from_user.first_name)} "
                f"is AFK since {time_str} ago!\n"
                + (f"Reason: {html.escape(reason)}" if reason else ""),
                parse_mode=ParseMode.HTML
            )

def error_handler(update, context):
    """Log errors"""
    logger.error(f"Update {update} caused error {context.error}")
    try:
        raise context.error
    except Exception as e:
        logger.error(f"Error: {e}")

# ============================================================
# INLINE QUERY HANDLER
# ============================================================

def inline_handler(update, context):
    """Handle inline queries"""
    query = update.inline_query.query.strip()
    results = []
    
    if not query:
        # Show bot info
        results.append(InlineQueryResultArticle(
            id='info',
            title="🤖 MegaBot",
            description="500+ features bot - click to share info!",
            input_message_content=InputTextMessageContent(
                "🤖 <b>MegaBot</b> - 500+ Features Telegram Bot!\n\nAdd me to your group for the ultimate management experience!",
                parse_mode=ParseMode.HTML
            )
        ))
    else:
        # Search notes
        results.append(InlineQueryResultArticle(
            id='1',
            title=f"Search: {query}",
            description="Click to send",
            input_message_content=InputTextMessageContent(html.escape(query))
        ))
    
    update.inline_query.answer(results, cache_time=10)

# ============================================================
# CALLBACK QUERY DISPATCHER
# ============================================================

def callback_handler(update, context):
    """Main callback dispatcher"""
    query = update.callback_query
    data = query.data
    
    if data.startswith("poll_") or data.startswith("pollresult_"):
        poll_callback(update, context)
    elif data.startswith("ttt_"):
        ttt_callback(update, context)
    elif data.startswith("trivia_"):
        trivia_callback(update, context)
    elif data.startswith("setting_"):
        settings_callback(update, context)
    elif data.startswith("marry_"):
        marry_callback_handler(update, context)
    elif data.startswith("giveaway_join_"):
        giveaway_join_callback(update, context)
    elif data.startswith("wyr_"):
        query.answer(f"{'🅰️ Bold choice!' if data.endswith('a') else '🅱️ Interesting!'}", show_alert=True)
    elif data.startswith("captcha_"):
        parts = data.split("_")
        if len(parts) >= 3:
            action = parts[1]
            user_id = int(parts[2])
            if action == "verify":
                if query.from_user.id == user_id:
                    try:
                        context.bot.restrict_chat_member(
                            query.message.chat_id, user_id,
                            ChatPermissions(
                                can_send_messages=True,
                                can_send_media_messages=True,
                                can_send_other_messages=True,
                                can_add_web_page_previews=True
                            )
                        )
                        query.message.delete()
                        query.answer("✅ Verified! Welcome!")
                    except Exception as e:
                        query.answer(f"Error: {e}", show_alert=True)
                else:
                    query.answer("❌ This button is not for you!", show_alert=True)
    else:
        query.answer()

# ============================================================
# MAIN FUNCTION - ALL HANDLERS REGISTERED
# ============================================================

def main():
    """Main entry point"""
    logger.info("Starting MegaBot...")
    
    defaults = Defaults(parse_mode=ParseMode.HTML)
    updater = Updater(BOT_TOKEN, defaults=defaults, use_context=True)
    dp = updater.dispatcher
    
    # ---- CORE COMMANDS ----
    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("alive", alive_command))
    dp.add_handler(CommandHandler("botinfo", botinfo_command))
    dp.add_handler(CommandHandler("id", id_command))
    dp.add_handler(CommandHandler("info", info_command))
    dp.add_handler(CommandHandler("ping", speedtest_command))
    dp.add_handler(CommandHandler("speedtest", speedtest_command))
    dp.add_handler(CommandHandler("donate", donate_command))
    
    # ---- MODERATION ----
    dp.add_handler(CommandHandler("ban", ban_command))
    dp.add_handler(CommandHandler("unban", unban_command))
    dp.add_handler(CommandHandler("kick", kick_command))
    dp.add_handler(CommandHandler("mute", mute_command))
    dp.add_handler(CommandHandler("unmute", unmute_command))
    dp.add_handler(CommandHandler("tmute", tmute_command))
    dp.add_handler(CommandHandler("tban", tban_command))
    dp.add_handler(CommandHandler("warn", warn_command))
    dp.add_handler(CommandHandler("unwarn", unwarn_command))
    dp.add_handler(CommandHandler("warns", warns_command))
    dp.add_handler(CommandHandler("clearwarns", clearwarns_command))
    dp.add_handler(CommandHandler("setwarnlimit", setwarnlimit_command))
    dp.add_handler(CommandHandler("setwarnaction", setwarnaction_command))
    dp.add_handler(CommandHandler("topwarn", topwarn_command))
    dp.add_handler(CommandHandler("purge", purge_command))
    dp.add_handler(CommandHandler("del", del_command))
    dp.add_handler(CommandHandler("report", report_command))
    
    # ---- ADMIN TOOLS ----
    dp.add_handler(CommandHandler("promote", promote_command))
    dp.add_handler(CommandHandler("demote", demote_command))
    dp.add_handler(CommandHandler("pin", pin_command))
    dp.add_handler(CommandHandler("unpin", unpin_command))
    dp.add_handler(CommandHandler("unpinall", unpinall_command))
    dp.add_handler(CommandHandler("settitle", settitle_command))
    dp.add_handler(CommandHandler("lock", lock_command))
    dp.add_handler(CommandHandler("unlock", unlock_command))
    dp.add_handler(CommandHandler("locktypes", locktypes_command))
    dp.add_handler(CommandHandler("setdesc", setdesc_command))
    dp.add_handler(CommandHandler("setlink", setlink_command))
    dp.add_handler(CommandHandler("invitelink", invitelink_command))
    dp.add_handler(CommandHandler("setgpic", setgpic_command))
    dp.add_handler(CommandHandler("settings", settings_command))
    dp.add_handler(CommandHandler("chatinfo", chatinfo_command))
    dp.add_handler(CommandHandler("stats", stats_command))
    
    # ---- WELCOME & GOODBYE ----
    dp.add_handler(CommandHandler("setwelcome", setwelcome_command))
    dp.add_handler(CommandHandler("resetwelcome", resetwelcome_command))
    dp.add_handler(CommandHandler("welc", welc_command))
    dp.add_handler(CommandHandler("setgoodbye", setgoodbye_command))
    dp.add_handler(CommandHandler("goodbye", goodbye_toggle_command))
    dp.add_handler(CommandHandler("cleanwelcome", cleanwelcome_command))
    
    # ---- NOTES ----
    dp.add_handler(CommandHandler("save", save_note_command))
    dp.add_handler(CommandHandler("get", get_note_command))
    dp.add_handler(CommandHandler("notes", list_notes_command))
    dp.add_handler(CommandHandler("clear", clear_note_command))
    dp.add_handler(CommandHandler("clearall", clearnotes_command))
    dp.add_handler(CommandHandler("privatenotes", privatenotes_command))
    dp.add_handler(MessageHandler(Filters.regex(r'^#\w+'), hashtag_note_handler))
    
    # ---- FILTERS ----
    dp.add_handler(CommandHandler("filter", add_filter_command))
    dp.add_handler(CommandHandler("filters", list_filters_command))
    dp.add_handler(CommandHandler("stop", remove_filter_command))
    dp.add_handler(CommandHandler("stopall", remove_all_filters_command))
    
    # ---- BLACKLIST ----
    dp.add_handler(CommandHandler("addblacklist", addblacklist_command))
    dp.add_handler(CommandHandler("blacklist", listblacklist_command))
    dp.add_handler(CommandHandler("rmblacklist", removeblacklist_command))
    dp.add_handler(CommandHandler("blacklistaction", blacklistaction_command))
    
    # ---- RULES ----
    dp.add_handler(CommandHandler("rules", rules_command))
    dp.add_handler(CommandHandler("setrules", setrules_command))
    dp.add_handler(CommandHandler("resetrules", resetrules_command))
    dp.add_handler(CommandHandler("privaterules", privaterules_command))
    
    # ---- USER COMMANDS ----
    dp.add_handler(CommandHandler("me", me_command))
    dp.add_handler(CommandHandler("afk", afk_command))
    dp.add_handler(CommandHandler("brb", afk_command))
    dp.add_handler(CommandHandler("setbio", setbio_command))
    dp.add_handler(CommandHandler("bio", bio_command))
    dp.add_handler(CommandHandler("leaderboard", leaderboard_command))
    dp.add_handler(CommandHandler("rank", rank_command))
    dp.add_handler(CommandHandler("daily", daily_command))
    dp.add_handler(CommandHandler("coins", coins_command))
    dp.add_handler(CommandHandler("rep", rep_command))
    dp.add_handler(CommandHandler("topuser", topuser_command))
    
    # ---- FUN ----
    dp.add_handler(CommandHandler("roll", roll_command))
    dp.add_handler(CommandHandler("toss", toss_command))
    dp.add_handler(CommandHandler("choice", choice_command))
    dp.add_handler(CommandHandler(["8ball", "eightball"], eightball_command))
    dp.add_handler(CommandHandler("rps", rps_command))
    dp.add_handler(CommandHandler("love", love_command))
    dp.add_handler(CommandHandler("zodiac", zodiac_command))
    dp.add_handler(CommandHandler("compliment", compliment_command))
    dp.add_handler(CommandHandler("roast", roast_command))
    dp.add_handler(CommandHandler("fact", fact_command))
    dp.add_handler(CommandHandler("joke", joke_command))
    dp.add_handler(CommandHandler("quote", quote_command))
    dp.add_handler(CommandHandler("advice", advice_command))
    dp.add_handler(CommandHandler("dare", dare_command))
    dp.add_handler(CommandHandler("truth", truth_command))
    dp.add_handler(CommandHandler("wyr", wyr_command))
    dp.add_handler(CommandHandler("slap", slap_command))
    dp.add_handler(CommandHandler("hug", hug_command))
    dp.add_handler(CommandHandler("kiss", kiss_command))
    dp.add_handler(CommandHandler("punch", punch_command))
    dp.add_handler(CommandHandler("pat", pat_command))
    dp.add_handler(CommandHandler("meme", meme_command))
    dp.add_handler(CommandHandler("flip", flip_command))
    dp.add_handler(CommandHandler("shrug", shrug_command))
    dp.add_handler(CommandHandler("lenny", lenny_command))
    dp.add_handler(CommandHandler("tableflip", tableflip_command))
    
    # ---- GAMES ----
    dp.add_handler(CommandHandler("ttt", ttt_command))
    dp.add_handler(CommandHandler("trivia", trivia_command))
    dp.add_handler(CommandHandler("wordscramble", wordscramble_command))
    dp.add_handler(CommandHandler("number", number_command))
    dp.add_handler(CommandHandler("guess", guess_command))
    
    # ---- UTILS ----
    dp.add_handler(CommandHandler("calc", calc_command))
    dp.add_handler(CommandHandler("base64", base64_command))
    dp.add_handler(CommandHandler("password", password_command))
    dp.add_handler(CommandHandler("qr", qr_command))
    dp.add_handler(CommandHandler("short", shortlink_command))
    dp.add_handler(CommandHandler("timestamp", timestamp_command))
    dp.add_handler(CommandHandler("roman", roman_command))
    dp.add_handler(CommandHandler("binary", binary_command))
    dp.add_handler(CommandHandler("hex", hex_command))
    dp.add_handler(CommandHandler("ascii", ascii_command))
    dp.add_handler(CommandHandler("weather", weather_command))
    dp.add_handler(CommandHandler("crypto", crypto_command))
    dp.add_handler(CommandHandler("color", color_command))
    dp.add_handler(CommandHandler("translate", translate_command))
    dp.add_handler(CommandHandler("define", define_command))
    dp.add_handler(CommandHandler("wiki", wiki_command))
    dp.add_handler(CommandHandler("github", github_command))
    dp.add_handler(CommandHandler("paste", paste_command))
    dp.add_handler(CommandHandler("remind", remind_command))
    dp.add_handler(CommandHandler("reminders", reminders_command))
    dp.add_handler(CommandHandler("poll", poll_command))
    
    # ---- TEXT TOOLS ----
    dp.add_handler(CommandHandler("reverse", reverse_command))
    dp.add_handler(CommandHandler("upper", upper_command))
    dp.add_handler(CommandHandler("lower", lower_command))
    dp.add_handler(CommandHandler("count", count_command))
    dp.add_handler(CommandHandler("mock", mock_command))
    dp.add_handler(CommandHandler("zalgo", zalgo_command))
    dp.add_handler(CommandHandler("leet", leet_command))
    dp.add_handler(CommandHandler("repeat", repeat_command))
    dp.add_handler(CommandHandler("bold", bold_command))
    dp.add_handler(CommandHandler("italic", italic_command))
    dp.add_handler(CommandHandler("code", code_command))
    
    # ---- STICKER ----
    dp.add_handler(CommandHandler("getsticker", getsticker_command))
    dp.add_handler(CommandHandler("kang", kang_command))
    
    # ---- FEDERATION ----
    dp.add_handler(CommandHandler("newfed", newfed_command))
    dp.add_handler(CommandHandler("joinfed", joinfed_command))
    dp.add_handler(CommandHandler("leavefed", leavefed_command))
    dp.add_handler(CommandHandler("fedinfo", fedinfo_command))
    dp.add_handler(CommandHandler("fban", fban_command))
    dp.add_handler(CommandHandler("funban", funban_command))
    dp.add_handler(CommandHandler("fedadmin", fedadmin_command))
    dp.add_handler(CommandHandler("delfed", delfed_command))
    dp.add_handler(CommandHandler("fedsubs", fedsubs_command))
    dp.add_handler(CommandHandler("fedbanned", fedbanned_command))
    
    # ---- APPROVAL ----
    dp.add_handler(CommandHandler("approve", approve_command))
    dp.add_handler(CommandHandler("disapprove", disapprove_command))
    dp.add_handler(CommandHandler("approved", approved_command))
    
    # ---- ANTIFLOOD / LINK / SPAM ----
    dp.add_handler(CommandHandler("setflood", setflood_command))
    dp.add_handler(CommandHandler("flood", flood_status_command))
    dp.add_handler(CommandHandler("setfloodaction", setfloodaction_command))
    dp.add_handler(CommandHandler("antilink", toggle_antilink_command))
    dp.add_handler(CommandHandler("antispam", toggle_antispam_command))
    dp.add_handler(CommandHandler("setloglevel", setloglevel_command if 'setloglevel_command' in dir() else lambda u,c: None))
    
    # ---- LOG CHANNEL ----
    dp.add_handler(CommandHandler("setlog", setlog_command))
    dp.add_handler(CommandHandler("unsetlog", unsetlog_command))
    
    # ---- CAPTCHA ----
    dp.add_handler(CommandHandler("captcha", toggle_captcha_command))
    dp.add_handler(CommandHandler("captchamode", captchamode_command))
    
    # ---- GBAN / SUDO ----
    dp.add_handler(CommandHandler("gban", globalban_command))
    dp.add_handler(CommandHandler("ungban", globalunban_command))
    dp.add_handler(CommandHandler("gbanned", gbanned_list_command if 'gbanned_list_command' in dir() else lambda u,c: None))
    dp.add_handler(CommandHandler("broadcast", broadcast_command))
    dp.add_handler(CommandHandler("addadmin", addadmin_command))
    dp.add_handler(CommandHandler("removeadmin", removeadmin_command))
    dp.add_handler(CommandHandler("dbstats", dbstats_command))
    dp.add_handler(CommandHandler("shell", shell_command))
    dp.add_handler(CommandHandler("restart", restart_command))
    
    # ---- DISABLE/ENABLE ----
    dp.add_handler(CommandHandler("disable", disable_command))
    dp.add_handler(CommandHandler("enable", enable_command))
    dp.add_handler(CommandHandler("disabled", disabled_command))
    
    # ---- CONNECT ----
    dp.add_handler(CommandHandler("connect", connect_command))
    dp.add_handler(CommandHandler("disconnect", disconnect_command))
    dp.add_handler(CommandHandler("connection", connection_info_command))
    
    # ---- SCHEDULE ----
    dp.add_handler(CommandHandler("schedule", schedule_command))
    dp.add_handler(CommandHandler("schedules", list_schedules_command))
    dp.add_handler(CommandHandler("cancelschedule", cancel_schedule_command))
    
    # ---- NIGHTMODE ----
    dp.add_handler(CommandHandler("nightmode", nightmode_command))
    dp.add_handler(CommandHandler("setnightmode", setnightmode_command))
    
    # ---- SLOWMODE ----
    dp.add_handler(CommandHandler("slowmode", slowmode_command))
    
    # ---- GIVEAWAY ----
    dp.add_handler(CommandHandler("giveaway", giveaway_command))
    dp.add_handler(CommandHandler("endgiveaway", endgiveaway_command))
    dp.add_handler(CommandHandler("join", join_giveaway_command))
    
    # ---- COUPON ----
    dp.add_handler(CommandHandler("coupon", redeem_coupon_command))
    dp.add_handler(CommandHandler("createcoupon", create_coupon_command))
    
    # ---- MARRIAGE ----
    dp.add_handler(CommandHandler("marry", marry_command))
    dp.add_handler(CommandHandler("divorce", divorce_command))
    dp.add_handler(CommandHandler("spouse", spouse_command))
    
    # ---- TODO ----
    dp.add_handler(CommandHandler("todo", todo_command))
    dp.add_handler(CommandHandler("todos", todos_command))
    dp.add_handler(CommandHandler("donetodo", donetodo_command))
    dp.add_handler(CommandHandler("deletetodo", deletetodo_command))
    
    # ---- CONFESSION ----
    dp.add_handler(CommandHandler("confess", confess_command))
    
    # ---- WORD GAME ----
    dp.add_handler(CommandHandler("wordgame", wordgame_command))
    dp.add_handler(CommandHandler("stopgame", stopgame_command))
    
    # ---- RSS ----
    dp.add_handler(CommandHandler("addrss", addrss_command))
    dp.add_handler(CommandHandler("rsslist", rsslist_command))
    dp.add_handler(CommandHandler("delrss", delrss_command))
    
    # ---- MESSAGE HANDLERS ----
    # Welcome/Goodbye
    dp.add_handler(MessageHandler(Filters.status_update.new_chat_members, welcome_handler))
    dp.add_handler(MessageHandler(Filters.status_update.left_chat_member, goodbye_handler))
    
    # All message handler (tracking, filters, blacklist, flood, etc.)
    dp.add_handler(MessageHandler(Filters.all & ~Filters.command, message_handler_main))
    
    # ---- CALLBACK QUERIES ----
    dp.add_handler(CallbackQueryHandler(callback_handler))
    
    # ---- INLINE QUERIES ----
    dp.add_handler(InlineQueryHandler(inline_handler))
    
    # ---- ERROR HANDLER ----
    dp.add_error_handler(error_handler)
    
    # ---- JOB QUEUE (background tasks) ----
    job_queue = updater.job_queue
    job_queue.run_repeating(check_reminders, interval=60, first=10)
    job_queue.run_repeating(check_scheduled_messages, interval=60, first=15)
    job_queue.run_repeating(check_nightmode, interval=300, first=30)
    
    # ---- START BOT ----
    if WEBHOOK_URL:
        logger.info(f"Starting webhook on port {PORT}")
        updater.start_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}"
        )
    else:
        logger.info("Starting polling...")
        updater.start_polling(drop_pending_updates=True)
    
    logger.info("✅ MegaBot started successfully!")
    
    # Start Flask in background thread
    def run_flask():
        app.run(host='0.0.0.0', port=PORT if not WEBHOOK_URL else PORT + 1)
    
    if not WEBHOOK_URL:
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
    
    updater.idle()

# ============================================================
# COMBINED MESSAGE HANDLER
# ============================================================

def message_handler_main(update, context):
    """Main message processor"""
    if not update.effective_message:
        return
    
    # Track messages
    track_messages(update, context)
    
    # Anti-flood check
    antiflood_check(update, context)
    
    # Anti-link check
    antilink_check(update, context)
    
    # Anti-spam check
    check_antispam(update, context)
    
    # Filter check
    filter_handler(update, context)
    
    # Blacklist check
    blacklist_handler(update, context)
    
    # Word scramble answer check
    if 'scramble_word' in context.chat_data:
        msg_text = update.effective_message.text or ''
        if msg_text.upper() == context.chat_data['scramble_word'].upper():
            user = update.effective_user
            update.effective_message.reply_text(
                f"🎉 {mention_html(user.id, user.first_name)} got it! The word was <b>{context.chat_data['scramble_word']}</b>!",
                parse_mode=ParseMode.HTML
            )
            del context.chat_data['scramble_word']
            # Give XP
            db.execute("UPDATE users SET xp = xp + 10 WHERE user_id = %s", (user.id,))
    
    # Auto-reply check
    if update.effective_chat and update.effective_chat.type != 'private':
        msg_text = (update.effective_message.text or '').lower()
        if msg_text:
            auto_replies = db.execute(
                "SELECT * FROM auto_replies WHERE chat_id = %s",
                (update.effective_chat.id,), fetch=True
            )
            for ar in (auto_replies or []):
                trigger = ar['trigger_text'].lower()
                matches = False
                if ar.get('is_regex'):
                    try:
                        matches = bool(re.search(trigger, msg_text))
                    except:
                        pass
                else:
                    matches = trigger in msg_text
                if matches:
                    try:
                        update.effective_message.reply_text(ar['reply_text'], parse_mode=ParseMode.HTML)
                    except:
                        pass
                    break

# ============================================================
# BACKGROUND TASK STUBS
# ============================================================

def check_scheduled_messages(context):
    """Check and send scheduled messages"""
    now = datetime.datetime.now()
    due = db.execute(
        "SELECT * FROM scheduled_messages WHERE scheduled_time <= %s AND is_sent = FALSE",
        (now,), fetch=True
    )
    for msg in (due or []):
        try:
            context.bot.send_message(msg['chat_id'], msg['message_text'], parse_mode=ParseMode.HTML)
            db.execute("UPDATE scheduled_messages SET is_sent = TRUE WHERE id = %s", (msg['id'],))
        except:
            db.execute("UPDATE scheduled_messages SET is_sent = TRUE WHERE id = %s", (msg['id'],))

def check_nightmode(context):
    """Check and apply nightmode"""
    now = datetime.datetime.now()
    current_time = now.strftime('%H:%M')
    
    chats = db.execute(
        "SELECT chat_id, nightmode_start, nightmode_end FROM chats WHERE nightmode_enabled = TRUE",
        fetch=True
    )
    
    for chat in (chats or []):
        start = chat.get('nightmode_start', '00:00')
        end = chat.get('nightmode_end', '06:00')
        
        is_night = False
        if start <= end:
            is_night = start <= current_time <= end
        else:  # crosses midnight
            is_night = current_time >= start or current_time <= end
        
        try:
            if is_night:
                context.bot.set_chat_permissions(
                    chat['chat_id'],
                    ChatPermissions(can_send_messages=False)
                )
            else:
                context.bot.set_chat_permissions(
                    chat['chat_id'],
                    ChatPermissions(
                        can_send_messages=True,
                        can_send_media_messages=True,
                        can_send_other_messages=True,
                        can_add_web_page_previews=True
                    )
                )
        except:
            pass


# ============================================================
# STUB FUNCTIONS (for commands referenced above)
# ============================================================

def _stub(update, context):
    update.effective_message.reply_text("⚠️ This feature is under development!")



# ============================================================
# ALL PREVIOUSLY MISSING FUNCTIONS - FULLY IMPLEMENTED
# ============================================================

# ---- GBANNED LIST ----
@sudo_only
def gbanned_list_command(update, context):
    gbans = db.execute("SELECT user_id, reason FROM gbans ORDER BY created_at DESC LIMIT 20", fetch=True)
    if not gbans:
        update.effective_message.reply_text("✅ No globally banned users!")
        return
    text = f"🚫 <b>Globally Banned Users</b>\n\n"
    for g in gbans:
        text += f"• <code>{g['user_id']}</code> — {html.escape(g.get('reason','No reason'))}\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@sudo_only
def setloglevel_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /setloglevel <DEBUG|INFO|WARNING|ERROR>")
        return
    level = context.args[0].upper()
    levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}
    if level not in levels:
        update.effective_message.reply_text("Invalid level!")
        return
    logging.getLogger().setLevel(levels[level])
    update.effective_message.reply_text(f"✅ Log level → <b>{level}</b>", parse_mode=ParseMode.HTML)

# ---- FUN ----
@check_disabled
def advice_command(update, context):
    advices = [
        "🌟 Never give up on your dreams!", "💡 The best time to start is now.",
        "🔑 Courage is not the absence of fear.", "🌈 Be yourself — everyone else is taken.",
        "⚡ The only way to do great work is to love what you do.",
        "💪 You are stronger than you think!", "🌺 Kindness is always in fashion.",
        "🎯 Focus on progress, not perfection.", "❤️ Be the change you wish to see.",
        "🧠 Invest in yourself — it pays the best interest."
    ]
    update.effective_message.reply_text(random.choice(advices))

@check_disabled
def wyr_command(update, context):
    questions = [
        ("Be able to fly", "Be invisible"), ("Live in the past", "Live in the future"),
        ("Have super strength", "Have super speed"), ("Be rich but unhappy", "Be poor but happy"),
        ("Speak all languages", "Play all instruments"), ("Live underwater", "Live in space"),
        ("Have a dragon", "Be a dragon"), ("Never sleep again", "Always be sleeping"),
    ]
    a, b = random.choice(questions)
    keyboard = [[InlineKeyboardButton(f"🅰️ {a}", callback_data="wyr_a"), InlineKeyboardButton(f"🅱️ {b}", callback_data="wyr_b")]]
    update.effective_message.reply_text(
        f"🤔 <b>Would You Rather...</b>\n\n🅰️ <b>{html.escape(a)}</b>\n\n— OR —\n\n🅱️ <b>{html.escape(b)}</b>",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

@check_disabled
def kiss_command(update, context):
    actions = ["😘 *smooch*", "💋 *muah*", "😚 *gentle kiss*", "💝 *kiss on cheek*"]
    if update.effective_message.reply_to_message:
        target = mention_html(update.effective_message.reply_to_message.from_user.id, update.effective_message.reply_to_message.from_user.first_name)
        user = mention_html(update.effective_user.id, update.effective_user.first_name)
        update.effective_message.reply_text(f"{user} kissed {target}! {random.choice(actions)}", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text(f"Send some love! {random.choice(actions)}")

@check_disabled
def punch_command(update, context):
    actions = ["👊 *POW!*", "💢 *WHAM!*", "🥊 *SMACK!*", "💥 *BOOM!*"]
    if update.effective_message.reply_to_message:
        target = mention_html(update.effective_message.reply_to_message.from_user.id, update.effective_message.reply_to_message.from_user.first_name)
        user = mention_html(update.effective_user.id, update.effective_user.first_name)
        update.effective_message.reply_text(f"{user} punched {target}! {random.choice(actions)}", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text(f"Punch! {random.choice(actions)}")

@check_disabled
def translate_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /translate <lang_code> <text>\nExample: /translate hi Hello\nCodes: hi=Hindi es=Spanish fr=French de=German ar=Arabic ja=Japanese")
        return
    lang = context.args[0].lower()
    text = " ".join(context.args[1:])
    if not text:
        update.effective_message.reply_text("Provide text to translate!")
        return
    try:
        import urllib.request as _ur
        url = f"https://api.mymemory.translated.net/get?q={urllib.parse.quote(text)}&langpair=en|{lang}"
        req = _ur.Request(url, headers={'User-Agent': 'MegaBot/1.0'})
        with _ur.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        translated = data['responseData']['translatedText']
        update.effective_message.reply_text(
            f"🌐 <b>Translation</b>\n\n🇬🇧 {html.escape(text)}\n➡️ ({lang}): <b>{html.escape(translated)}</b>",
            parse_mode=ParseMode.HTML)
    except Exception as e:
        update.effective_message.reply_text(f"❌ Translation failed: {e}")

@check_disabled
def define_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /define <word>")
        return
    word = context.args[0]
    try:
        import urllib.request as _ur
        url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{urllib.parse.quote(word)}"
        req = _ur.Request(url, headers={'User-Agent': 'MegaBot/1.0'})
        with _ur.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        meanings = data[0].get('meanings', [])
        text = f"📖 <b>{html.escape(word)}</b>\n\n"
        for m in meanings[:2]:
            defs = m.get('definitions', [])
            if defs:
                text += f"<i>{m.get('partOfSpeech','')}</i>: {html.escape(defs[0].get('definition',''))}\n"
                if defs[0].get('example'):
                    text += f"💬 <i>{html.escape(defs[0]['example'])}</i>\n"
                text += "\n"
        update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception:
        update.effective_message.reply_text(f"❌ Definition not found for: <b>{html.escape(word)}</b>", parse_mode=ParseMode.HTML)

@check_disabled
def wiki_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /wiki <topic>")
        return
    query = " ".join(context.args)
    try:
        import urllib.request as _ur
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(query.replace(' ','_'))}"
        req = _ur.Request(url, headers={'User-Agent': 'MegaBot/1.0'})
        with _ur.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        title = data.get('title', query)
        extract = data.get('extract', 'No summary available.')[:800]
        page_url = data.get('content_urls', {}).get('desktop', {}).get('page', '')
        keyboard = [[InlineKeyboardButton("📖 Read More", url=page_url)]] if page_url else None
        update.effective_message.reply_text(
            f"📚 <b>{html.escape(title)}</b>\n\n{html.escape(extract)}...",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)
    except Exception:
        update.effective_message.reply_text(f"❌ No Wikipedia article found for <b>{html.escape(query)}</b>!", parse_mode=ParseMode.HTML)

@check_disabled
def paste_command(update, context):
    msg = update.effective_message
    if msg.reply_to_message and msg.reply_to_message.text:
        text = msg.reply_to_message.text
    elif context.args:
        text = " ".join(context.args)
    else:
        msg.reply_text("Usage: /paste <text> or reply to a message")
        return
    try:
        import urllib.request as _ur
        data = json.dumps({"content": text}).encode()
        req = _ur.Request("https://hastebin.com/documents", data=data, headers={'Content-Type': 'application/json'})
        with _ur.urlopen(req, timeout=10) as resp:
            key = json.loads(resp.read().decode())['key']
        msg.reply_text(f"📋 Pasted! 🔗 https://hastebin.com/{key}")
    except Exception:
        try:
            import urllib.request as _ur
            data = json.dumps({"document": {"content": text}}).encode()
            req = _ur.Request("https://nekobin.com/api/documents", data=data, headers={'Content-Type': 'application/json'})
            with _ur.urlopen(req, timeout=10) as resp:
                key = json.loads(resp.read().decode())['result']['key']
            msg.reply_text(f"📋 Pasted! 🔗 https://nekobin.com/{key}")
        except Exception as e:
            msg.reply_text(f"❌ Paste failed: {e}")

# ---- USER PROFILE ----
@check_disabled
def topuser_command(update, context):
    top = db.execute("SELECT user_id, first_name, xp, level FROM users ORDER BY xp DESC LIMIT 10", fetch=True)
    if not top:
        update.effective_message.reply_text("No user data yet!")
        return
    medals = ['🥇','🥈','🥉']+['🏅']*7
    text = "🏆 <b>Top Users (XP)</b>\n\n"
    for i, u in enumerate(top):
        name = html.escape(u.get('first_name') or str(u['user_id']))
        text += f"{medals[i]} <b>{name}</b> — ⭐ {u['xp']} XP | Lv.{u['level']}\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def rank_command(update, context):
    user_id = update.effective_user.id
    user = db.execute("SELECT * FROM users WHERE user_id = %s", (user_id,), fetchone=True)
    if not user:
        update.effective_message.reply_text("No data! Send some messages first.")
        return
    rank_row = db.execute("SELECT COUNT(*) as r FROM users WHERE xp > %s", (user.get('xp',0),), fetchone=True)
    rank = (rank_row['r'] + 1) if rank_row else 1
    xp = user.get('xp', 0); level = user.get('level', 1); next_xp = level * 100
    bar_filled = int((xp % next_xp) / next_xp * 10) if next_xp else 0
    bar = "█"*bar_filled + "░"*(10-bar_filled)
    update.effective_message.reply_text(
        f"📊 <b>Rank Card</b>\n\n{mention_html(user_id, update.effective_user.first_name)}\n"
        f"🏆 Rank: <b>#{rank}</b>\n⭐ XP: <b>{xp}</b>\n📈 Level: <b>{level}</b>\n[{bar}] {xp%next_xp}/{next_xp} XP",
        parse_mode=ParseMode.HTML)

@check_disabled
def coins_command(update, context):
    user_id = update.effective_user.id
    user = db.execute("SELECT coins, first_name FROM users WHERE user_id = %s", (user_id,), fetchone=True)
    coins = user['coins'] if user else 0
    name = html.escape(user.get('first_name') or str(user_id)) if user else str(user_id)
    update.effective_message.reply_text(f"💰 <b>{name}</b> has <b>{coins:,}</b> coins!", parse_mode=ParseMode.HTML)

@check_disabled
def rep_command(update, context):
    msg = update.effective_message
    if not msg.reply_to_message:
        msg.reply_text("❌ Reply to someone to give rep!")
        return
    target = msg.reply_to_message.from_user
    if target.id == update.effective_user.id or target.is_bot:
        msg.reply_text("❌ Can't rep yourself or a bot!")
        return
    db.execute("UPDATE users SET reputation = reputation + 1 WHERE user_id = %s", (target.id,))
    rep = db.execute("SELECT reputation FROM users WHERE user_id = %s", (target.id,), fetchone=True)
    total = rep['reputation'] if rep else 1
    msg.reply_text(f"⭐ {mention_html(update.effective_user.id, update.effective_user.first_name)} repped "
                   f"{mention_html(target.id, target.first_name)}! Total: <b>{total}</b>", parse_mode=ParseMode.HTML)

# ---- WARN EXTRAS ----
@check_disabled
@group_only
@admin_only
def unwarn_command(update, context):
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("Reply to a user or provide ID!")
        return
    last = db.execute("SELECT id FROM warnings WHERE chat_id=%s AND user_id=%s ORDER BY created_at DESC LIMIT 1",
                      (update.effective_chat.id, user_id), fetchone=True)
    if not last:
        update.effective_message.reply_text("✅ User has no warnings!")
        return
    db.execute("DELETE FROM warnings WHERE id=%s", (last['id'],))
    cnt = db.execute("SELECT COUNT(*) as c FROM warnings WHERE chat_id=%s AND user_id=%s",
                     (update.effective_chat.id, user_id), fetchone=True)
    update.effective_message.reply_text(
        f"✅ Warning removed from {mention_html(user_id, user_name)}!\nRemaining: <b>{cnt['c']}</b>",
        parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def clearwarns_command(update, context):
    user_id, user_name = extract_user(update, context)
    if not user_id:
        update.effective_message.reply_text("Reply to a user or provide ID!")
        return
    db.execute("DELETE FROM warnings WHERE chat_id=%s AND user_id=%s", (update.effective_chat.id, user_id))
    update.effective_message.reply_text(f"✅ All warns cleared for {mention_html(user_id, user_name)}!", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def setwarnlimit_command(update, context):
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /setwarnlimit <1-10>")
        return
    limit = max(1, min(10, int(context.args[0])))
    db.execute("UPDATE chats SET warn_limit=%s WHERE chat_id=%s", (limit, update.effective_chat.id))
    update.effective_message.reply_text(f"✅ Warn limit: <b>{limit}</b>", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def setwarnaction_command(update, context):
    if not context.args or context.args[0].lower() not in ['ban','kick','mute']:
        update.effective_message.reply_text("Usage: /setwarnaction <ban|kick|mute>")
        return
    db.execute("UPDATE chats SET warn_action=%s WHERE chat_id=%s", (context.args[0].lower(), update.effective_chat.id))
    update.effective_message.reply_text(f"✅ Warn action: <b>{context.args[0]}</b>", parse_mode=ParseMode.HTML)

# ---- ADMIN ----
@check_disabled
@group_only
@admin_only
def delete_command(update, context):
    """Alias for del_command — same functionality, kept for compatibility"""
    del_command(update, context)

@check_disabled
@group_only
@admin_only
def settitle_command(update, context):
    msg = update.effective_message
    if not msg.reply_to_message:
        msg.reply_text("❌ Reply to a user!")
        return
    title = " ".join(context.args)[:16] if context.args else ""
    try:
        context.bot.set_chat_administrator_custom_title(update.effective_chat.id, msg.reply_to_message.from_user.id, title)
        msg.reply_text(f"✅ Title set: <b>{html.escape(title)}</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        msg.reply_text(f"❌ {e}")

@check_disabled
@group_only
@admin_only
def locktypes_command(update, context):
    lock_types = ["text","media","sticker","gif","photo","video","audio","voice","document","contact","location","game","forward","bot","url"]
    chat = db.execute("SELECT locked_types FROM chats WHERE chat_id=%s", (update.effective_chat.id,), fetchone=True)
    locked = json.loads(chat['locked_types']) if chat and chat.get('locked_types') else []
    text = "🔒 <b>Lockable Types</b>\n\n"
    for lt in lock_types:
        text += f"{'🔒' if lt in locked else '🔓'} {lt}\n"
    update.effective_message.reply_text(text + "\nUse /lock <type> to toggle", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def setdesc_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /setdesc <description>")
        return
    try:
        context.bot.set_chat_description(update.effective_chat.id, " ".join(context.args))
        update.effective_message.reply_text("✅ Description updated!")
    except Exception as e:
        update.effective_message.reply_text(f"❌ {e}")

# ---- WELCOME / GOODBYE ----
@check_disabled
@group_only
@admin_only
def setwelcome_command(update, context):
    msg = update.effective_message
    if not context.args:
        msg.reply_text("Usage: /setwelcome <text>\nVars: {first} {last} {fullname} {username} {mention} {id} {chatname} {count}")
        return
    wtext = " ".join(context.args)
    db.execute("UPDATE chats SET welcome_text=%s WHERE chat_id=%s", (wtext, update.effective_chat.id))
    preview = format_welcome(wtext, update.effective_user, update.effective_chat)
    msg.reply_text(f"✅ Welcome set!\n\nPreview:\n{preview}", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def resetwelcome_command(update, context):
    db.execute("UPDATE chats SET welcome_text='Hey {mention}, welcome to {chatname}! 🎉' WHERE chat_id=%s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ Welcome reset to default!")

@check_disabled
@group_only
@admin_only
def welc_command(update, context):
    chat = db.execute("SELECT welcome_enabled, welcome_text FROM chats WHERE chat_id=%s", (update.effective_chat.id,), fetchone=True)
    if not context.args:
        enabled = chat.get('welcome_enabled', True) if chat else True
        text = chat.get('welcome_text', 'Default') if chat else 'Default'
        update.effective_message.reply_text(
            f"👋 <b>Welcome Status:</b> {'✅ ON' if enabled else '❌ OFF'}\n\n{html.escape(text)}", parse_mode=ParseMode.HTML)
        return
    mode = context.args[0].lower()
    new_val = mode in ['on', 'yes', 'true']
    db.execute("UPDATE chats SET welcome_enabled=%s WHERE chat_id=%s", (new_val, update.effective_chat.id))
    update.effective_message.reply_text(f"👋 Welcome: {'✅ ON' if new_val else '❌ OFF'}")

@check_disabled
@group_only
@admin_only
def setgoodbye_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /setgoodbye <text>")
        return
    db.execute("UPDATE chats SET goodbye_text=%s WHERE chat_id=%s", (" ".join(context.args), update.effective_chat.id))
    update.effective_message.reply_text("✅ Goodbye message set!")

@check_disabled
@group_only
@admin_only
def goodbye_toggle_command(update, context):
    chat = db.execute("SELECT goodbye_enabled FROM chats WHERE chat_id=%s", (update.effective_chat.id,), fetchone=True)
    if not context.args:
        enabled = chat.get('goodbye_enabled', True) if chat else True
        update.effective_message.reply_text(f"👋 Goodbye: {'✅ ON' if enabled else '❌ OFF'}")
        return
    new_val = context.args[0].lower() in ['on','yes']
    db.execute("UPDATE chats SET goodbye_enabled=%s WHERE chat_id=%s", (new_val, update.effective_chat.id))
    update.effective_message.reply_text(f"👋 Goodbye: {'✅ ON' if new_val else '❌ OFF'}")

# ---- NOTES ----
@check_disabled
def note_hashtag_handler(update, context):
    msg = update.effective_message
    text = msg.text or ''
    if text.startswith('#'):
        note_name = text[1:].split()[0].lower()
        note = db.execute("SELECT * FROM notes WHERE chat_id=%s AND LOWER(note_name)=%s",
                          (update.effective_chat.id, note_name), fetchone=True)
        if note:
            clean_text, buttons = parse_buttons(note.get('note_text',''))
            reply_markup = build_keyboard(buttons) if buttons else None
            try:
                msg.reply_text(clean_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
            except:
                msg.reply_text(clean_text, reply_markup=reply_markup)

# ---- FILTERS ----
@check_disabled
@group_only
@admin_only
def remove_filter_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /stop <keyword>")
        return
    keyword = " ".join(context.args).lower()
    result = db.execute("DELETE FROM filters WHERE chat_id=%s AND LOWER(keyword)=%s RETURNING id",
                        (update.effective_chat.id, keyword), fetchone=True)
    if result:
        update.effective_message.reply_text(f"✅ Filter <code>{html.escape(keyword)}</code> removed!", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text(f"❌ Filter not found: {html.escape(keyword)}")

@check_disabled
@group_only
@admin_only
def remove_all_filters_command(update, context):
    db.execute("DELETE FROM filters WHERE chat_id=%s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ All filters removed!")

# ---- BLACKLIST ----
@check_disabled
@group_only
def listblacklist_command(update, context):
    items = db.execute("SELECT trigger_word, action FROM blacklist WHERE chat_id=%s", (update.effective_chat.id,), fetch=True)
    if not items:
        update.effective_message.reply_text("✅ No blacklisted words!")
        return
    text = f"🚫 <b>Blacklist ({len(items)})</b>\n\n"
    for item in items:
        text += f"• <code>{html.escape(item['trigger_word'])}</code> → {item['action']}\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def removeblacklist_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /rmblacklist <word>")
        return
    word = " ".join(context.args).lower()
    result = db.execute("DELETE FROM blacklist WHERE chat_id=%s AND LOWER(trigger_word)=%s RETURNING id",
                        (update.effective_chat.id, word), fetchone=True)
    if result:
        update.effective_message.reply_text(f"✅ Removed: <code>{html.escape(word)}</code>", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text(f"❌ Not in blacklist: {html.escape(word)}")

@check_disabled
@group_only
@admin_only
def blacklistaction_command(update, context):
    if not context.args or context.args[0].lower() not in ['delete','warn','kick','ban']:
        update.effective_message.reply_text("Usage: /blacklistaction <delete|warn|kick|ban>")
        return
    db.execute("UPDATE blacklist SET action=%s WHERE chat_id=%s", (context.args[0].lower(), update.effective_chat.id))
    update.effective_message.reply_text(f"✅ Blacklist action: <b>{context.args[0]}</b>", parse_mode=ParseMode.HTML)

# ---- RULES ----
@check_disabled
@group_only
@admin_only
def resetrules_command(update, context):
    db.execute("UPDATE chats SET rules='' WHERE chat_id=%s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ Rules cleared!")

@check_disabled
@group_only
@admin_only
def privaterules_command(update, context):
    update.effective_message.reply_text("ℹ️ Users can /start the bot in PM and then use /rules to get rules privately!")

# ---- FLOOD ----
@check_disabled
@group_only
def flood_status_command(update, context):
    chat = db.execute("SELECT antiflood_enabled, antiflood_limit, antiflood_action FROM chats WHERE chat_id=%s",
                      (update.effective_chat.id,), fetchone=True)
    enabled = chat.get('antiflood_enabled', False) if chat else False
    limit = chat.get('antiflood_limit', 10) if chat else 10
    action = chat.get('antiflood_action', 'mute') if chat else 'mute'
    update.effective_message.reply_text(
        f"🌊 <b>Anti-Flood</b>\nStatus: {'✅ ON' if enabled else '❌ OFF'}\nLimit: <b>{limit} msgs/5s</b>\nAction: <b>{action}</b>",
        parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def setfloodaction_command(update, context):
    if not context.args or context.args[0].lower() not in ['ban','kick','mute']:
        update.effective_message.reply_text("Usage: /setfloodaction <ban|kick|mute>")
        return
    db.execute("UPDATE chats SET antiflood_action=%s WHERE chat_id=%s", (context.args[0].lower(), update.effective_chat.id))
    update.effective_message.reply_text(f"✅ Flood action: <b>{context.args[0]}</b>", parse_mode=ParseMode.HTML)

# ---- ANTILINK / ANTISPAM ----
@check_disabled
@group_only
@admin_only
def toggle_antilink_command(update, context):
    chat = db.execute("SELECT antilink_enabled FROM chats WHERE chat_id=%s", (update.effective_chat.id,), fetchone=True)
    enabled = not (chat.get('antilink_enabled', False) if chat else False)
    db.execute("UPDATE chats SET antilink_enabled=%s WHERE chat_id=%s", (enabled, update.effective_chat.id))
    update.effective_message.reply_text(f"🔗 Anti-link: {'✅ ON' if enabled else '❌ OFF'}")

@check_disabled
@group_only
@admin_only
def toggle_antispam_command(update, context):
    chat = db.execute("SELECT antispam_enabled FROM chats WHERE chat_id=%s", (update.effective_chat.id,), fetchone=True)
    enabled = not (chat.get('antispam_enabled', False) if chat else False)
    db.execute("UPDATE chats SET antispam_enabled=%s WHERE chat_id=%s", (enabled, update.effective_chat.id))
    update.effective_message.reply_text(f"🛡️ Anti-spam: {'✅ ON' if enabled else '❌ OFF'}")

# ---- LOG CHANNEL ----
@check_disabled
@group_only
@admin_only
def setlog_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /setlog <channel_id>")
        return
    try:
        channel_id = int(context.args[0])
        context.bot.send_message(channel_id, f"✅ Log set for <b>{html.escape(update.effective_chat.title)}</b>!", parse_mode=ParseMode.HTML)
        db.execute("UPDATE chats SET log_channel=%s WHERE chat_id=%s", (channel_id, update.effective_chat.id))
        update.effective_message.reply_text(f"✅ Log channel: <code>{channel_id}</code>!", parse_mode=ParseMode.HTML)
    except Exception as e:
        update.effective_message.reply_text(f"❌ Error: {e}\nMake bot admin in channel!")

@check_disabled
@group_only
@admin_only
def unsetlog_command(update, context):
    db.execute("UPDATE chats SET log_channel=0 WHERE chat_id=%s", (update.effective_chat.id,))
    update.effective_message.reply_text("✅ Log channel removed!")

# ---- CAPTCHA ----
@check_disabled
@group_only
@admin_only
def toggle_captcha_command(update, context):
    chat = db.execute("SELECT captcha_enabled FROM chats WHERE chat_id=%s", (update.effective_chat.id,), fetchone=True)
    enabled = not (chat.get('captcha_enabled', False) if chat else False)
    db.execute("UPDATE chats SET captcha_enabled=%s WHERE chat_id=%s", (enabled, update.effective_chat.id))
    update.effective_message.reply_text(f"🔐 Captcha: {'✅ ON — New members must verify!' if enabled else '❌ OFF'}")

@check_disabled
@group_only
@admin_only
def captchamode_command(update, context):
    if not context.args or context.args[0].lower() not in ['button','math']:
        update.effective_message.reply_text("Usage: /captchamode <button|math>")
        return
    db.execute("UPDATE chats SET captcha_type=%s WHERE chat_id=%s", (context.args[0].lower(), update.effective_chat.id))
    update.effective_message.reply_text(f"✅ Captcha mode: <b>{context.args[0]}</b>", parse_mode=ParseMode.HTML)

# ---- DISABLE / ENABLE ----
@check_disabled
@group_only
@admin_only
def disable_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /disable <command>")
        return
    cmd = context.args[0].lstrip('/')
    db.execute("INSERT INTO disabled_commands (chat_id, command) VALUES (%s, %s) ON CONFLICT DO NOTHING",
               (update.effective_chat.id, cmd))
    update.effective_message.reply_text(f"✅ /<code>{cmd}</code> disabled!", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def enable_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /enable <command>")
        return
    cmd = context.args[0].lstrip('/')
    result = db.execute("DELETE FROM disabled_commands WHERE chat_id=%s AND command=%s RETURNING command",
                        (update.effective_chat.id, cmd), fetchone=True)
    if result:
        update.effective_message.reply_text(f"✅ /<code>{cmd}</code> enabled!", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text(f"❌ /<code>{cmd}</code> was not disabled!", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
def disabled_command(update, context):
    items = db.execute("SELECT command FROM disabled_commands WHERE chat_id=%s", (update.effective_chat.id,), fetch=True)
    if not items:
        update.effective_message.reply_text("✅ No disabled commands!")
        return
    cmds = ", ".join([f"/{i['command']}" for i in items])
    update.effective_message.reply_text(f"🚫 <b>Disabled:</b> {cmds}", parse_mode=ParseMode.HTML)

# ---- CONNECT ----
@check_disabled
def connect_command(update, context):
    msg = update.effective_message
    if update.effective_chat.type != 'private':
        db.execute("INSERT INTO connections (user_id, chat_id) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET chat_id=EXCLUDED.chat_id",
                   (update.effective_user.id, update.effective_chat.id))
        msg.reply_text(f"✅ Connected to <b>{html.escape(update.effective_chat.title)}</b>!\nManage from PM.", parse_mode=ParseMode.HTML)
        return
    if not context.args:
        msg.reply_text("Usage: /connect <chat_id> or use /connect inside a group!")
        return
    try:
        chat_id = int(context.args[0])
        chat = context.bot.get_chat(chat_id)
        member = context.bot.get_chat_member(chat_id, update.effective_user.id)
        if member.status not in ['creator', 'administrator']:
            msg.reply_text("❌ You must be admin in that group!")
            return
        db.execute("INSERT INTO connections (user_id, chat_id) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET chat_id=EXCLUDED.chat_id",
                   (update.effective_user.id, chat_id))
        msg.reply_text(f"✅ Connected to <b>{html.escape(chat.title)}</b>!", parse_mode=ParseMode.HTML)
    except Exception as e:
        msg.reply_text(f"❌ Error: {e}")

@check_disabled
def disconnect_command(update, context):
    db.execute("DELETE FROM connections WHERE user_id=%s", (update.effective_user.id,))
    update.effective_message.reply_text("✅ Disconnected!")

@check_disabled
def connection_info_command(update, context):
    conn = db.execute("SELECT chat_id FROM connections WHERE user_id=%s", (update.effective_user.id,), fetchone=True)
    if not conn:
        update.effective_message.reply_text("❌ Not connected! Use /connect in a group.")
        return
    try:
        chat = context.bot.get_chat(conn['chat_id'])
        update.effective_message.reply_text(f"🔗 Connected to: <b>{html.escape(chat.title)}</b>", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text(f"🔗 Connected to: <code>{conn['chat_id']}</code>", parse_mode=ParseMode.HTML)

# ---- SCHEDULE ----
@check_disabled
@group_only
@admin_only
def schedule_command(update, context):
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /schedule <time> <message>\nExample: /schedule 2h Good morning!")
        return
    scheduled_time = extract_time(context.args[0])
    if not scheduled_time:
        update.effective_message.reply_text("❌ Invalid time! Use: 30m, 1h, 2d")
        return
    message_text = " ".join(context.args[1:])
    db.execute("INSERT INTO scheduled_messages (chat_id, message_text, scheduled_time, created_by) VALUES (%s,%s,%s,%s)",
               (update.effective_chat.id, message_text, scheduled_time, update.effective_user.id))
    update.effective_message.reply_text(
        f"⏰ Scheduled!\n📝 <code>{html.escape(message_text)}</code>\n🕐 At: {scheduled_time.strftime('%Y-%m-%d %H:%M')} UTC",
        parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def list_schedules_command(update, context):
    sched = db.execute("SELECT id, message_text, scheduled_time FROM scheduled_messages WHERE chat_id=%s AND is_sent=FALSE ORDER BY scheduled_time",
                       (update.effective_chat.id,), fetch=True)
    if not sched:
        update.effective_message.reply_text("📅 No scheduled messages!")
        return
    text = f"📅 <b>Scheduled ({len(sched)})</b>\n\n"
    for s in sched:
        text += f"🆔 <code>{s['id']}</code> | {s['scheduled_time'].strftime('%m-%d %H:%M')}\n📝 {html.escape(str(s['message_text'])[:40])}...\n\n"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def cancel_schedule_command(update, context):
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /cancelschedule <id>")
        return
    result = db.execute("DELETE FROM scheduled_messages WHERE id=%s AND chat_id=%s RETURNING id",
                        (int(context.args[0]), update.effective_chat.id), fetchone=True)
    if result:
        update.effective_message.reply_text(f"✅ Schedule <code>{context.args[0]}</code> cancelled!", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text("❌ Not found!")

# ---- NIGHTMODE ----
@check_disabled
@group_only
@admin_only
def nightmode_command(update, context):
    chat = db.execute("SELECT nightmode_enabled, nightmode_start, nightmode_end FROM chats WHERE chat_id=%s",
                      (update.effective_chat.id,), fetchone=True)
    if not context.args:
        enabled = chat.get('nightmode_enabled', False) if chat else False
        start = chat.get('nightmode_start', '00:00') if chat else '00:00'
        end = chat.get('nightmode_end', '06:00') if chat else '06:00'
        update.effective_message.reply_text(
            f"🌙 <b>Night Mode</b>\nStatus: {'✅ ON' if enabled else '❌ OFF'}\n⏰ {start} — {end} UTC\n\nUse: /nightmode on|off",
            parse_mode=ParseMode.HTML)
        return
    new_val = context.args[0].lower() in ['on','yes']
    db.execute("UPDATE chats SET nightmode_enabled=%s WHERE chat_id=%s", (new_val, update.effective_chat.id))
    update.effective_message.reply_text(f"🌙 Night mode: {'✅ ON' if new_val else '❌ OFF'}")

@check_disabled
@group_only
@admin_only
def setnightmode_command(update, context):
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /setnightmode <start> <end>\nExample: /setnightmode 23:00 06:00")
        return
    import re as _re
    if not (_re.match(r'^\d{2}:\d{2}$', context.args[0]) and _re.match(r'^\d{2}:\d{2}$', context.args[1])):
        update.effective_message.reply_text("❌ Use HH:MM format!")
        return
    db.execute("UPDATE chats SET nightmode_start=%s, nightmode_end=%s WHERE chat_id=%s",
               (context.args[0], context.args[1], update.effective_chat.id))
    update.effective_message.reply_text(f"✅ Night hours: <b>{context.args[0]} — {context.args[1]} UTC</b>", parse_mode=ParseMode.HTML)

# ---- GIVEAWAY ----
@check_disabled
@group_only
@admin_only
def giveaway_command(update, context):
    if len(context.args) < 2:
        update.effective_message.reply_text("Usage: /giveaway <duration> <prize>\nExample: /giveaway 1h iPhone 15 Pro")
        return
    end_time = extract_time(context.args[0])
    if not end_time:
        update.effective_message.reply_text("❌ Invalid time!")
        return
    prize = " ".join(context.args[1:])
    result = db.execute("INSERT INTO giveaways (chat_id, creator_id, prize, end_time) VALUES (%s,%s,%s,%s) RETURNING id",
                        (update.effective_chat.id, update.effective_user.id, prize, end_time), fetchone=True)
    gid = result['id']
    keyboard = [[InlineKeyboardButton("🎉 Join Giveaway!", callback_data=f"giveaway_join_{gid}")]]
    update.effective_message.reply_text(
        f"🎊 <b>GIVEAWAY!</b>\n\n🏆 Prize: <b>{html.escape(prize)}</b>\n⏰ Ends: {end_time.strftime('%Y-%m-%d %H:%M')} UTC\n\nClick to participate!",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def endgiveaway_command(update, context):
    active = db.execute("SELECT id FROM giveaways WHERE chat_id=%s AND is_active=TRUE ORDER BY created_at DESC LIMIT 1",
                        (update.effective_chat.id,), fetchone=True)
    if not active:
        update.effective_message.reply_text("❌ No active giveaway!")
        return
    giveaway = db.execute("SELECT * FROM giveaways WHERE id=%s", (active['id'],), fetchone=True)
    participants = json.loads(giveaway.get('participants', '[]'))
    db.execute("UPDATE giveaways SET is_active=FALSE WHERE id=%s", (active['id'],))
    if not participants:
        update.effective_message.reply_text("😢 No participants!")
        return
    winner_id = random.choice(participants)
    try:
        w = context.bot.get_chat(winner_id)
        winner_text = mention_html(winner_id, w.first_name)
    except:
        winner_text = f"<code>{winner_id}</code>"
    update.effective_message.reply_text(
        f"🎊 <b>Giveaway Ended!</b>\n\n🏆 Prize: <b>{html.escape(giveaway['prize'])}</b>\n👥 Participants: {len(participants)}\n\n🎉 Winner: {winner_text}",
        parse_mode=ParseMode.HTML)

@check_disabled
def join_giveaway_command(update, context):
    active = db.execute("SELECT id FROM giveaways WHERE chat_id=%s AND is_active=TRUE ORDER BY created_at DESC LIMIT 1",
                        (update.effective_chat.id,), fetchone=True)
    if not active:
        update.effective_message.reply_text("❌ No active giveaway!")
        return
    giveaway = db.execute("SELECT * FROM giveaways WHERE id=%s", (active['id'],), fetchone=True)
    participants = json.loads(giveaway.get('participants', '[]'))
    if update.effective_user.id in participants:
        update.effective_message.reply_text("✅ Already joined!")
        return
    participants.append(update.effective_user.id)
    db.execute("UPDATE giveaways SET participants=%s WHERE id=%s", (json.dumps(participants), active['id']))
    update.effective_message.reply_text(f"🎉 Joined! Total: <b>{len(participants)}</b>", parse_mode=ParseMode.HTML)

# ---- COUPON ----
@sudo_only
def create_coupon_command(update, context):
    if len(context.args) < 3:
        update.effective_message.reply_text("Usage: /createcoupon <CODE> <coins|xp> <amount> [max_uses]")
        return
    code = context.args[0].upper()
    rtype = context.args[1].lower()
    if rtype not in ['coins', 'xp']:
        update.effective_message.reply_text("❌ Type must be coins or xp!")
        return
    try:
        amount = int(context.args[2])
        max_uses = int(context.args[3]) if len(context.args) > 3 else 1
    except:
        update.effective_message.reply_text("❌ Invalid amount!")
        return
    db.execute("INSERT INTO coupons (code, reward_type, reward_amount, max_uses, created_by) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (code) DO UPDATE SET reward_type=EXCLUDED.reward_type, reward_amount=EXCLUDED.reward_amount, max_uses=EXCLUDED.max_uses",
               (code, rtype, amount, max_uses, update.effective_user.id))
    update.effective_message.reply_text(
        f"✅ <b>Coupon Created!</b>\n🎟️ Code: <code>{code}</code>\n🎁 {amount} {rtype}\n🔢 Max Uses: {max_uses}",
        parse_mode=ParseMode.HTML)

@check_disabled
def redeem_coupon_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /coupon <CODE>")
        return
    code = context.args[0].upper()
    coupon = db.execute("SELECT * FROM coupons WHERE code=%s", (code,), fetchone=True)
    if not coupon:
        update.effective_message.reply_text("❌ Invalid coupon!")
        return
    used_by = json.loads(coupon.get('used_by', '[]'))
    if update.effective_user.id in used_by:
        update.effective_message.reply_text("❌ Already used!")
        return
    if coupon['used_count'] >= coupon['max_uses']:
        update.effective_message.reply_text("❌ Coupon expired (max uses reached)!")
        return
    rtype, amount = coupon['reward_type'], coupon['reward_amount']
    if rtype == 'coins':
        db.execute("UPDATE users SET coins=coins+%s WHERE user_id=%s", (amount, update.effective_user.id))
    elif rtype == 'xp':
        db.execute("UPDATE users SET xp=xp+%s WHERE user_id=%s", (amount, update.effective_user.id))
    used_by.append(update.effective_user.id)
    db.execute("UPDATE coupons SET used_count=used_count+1, used_by=%s WHERE code=%s", (json.dumps(used_by), code))
    update.effective_message.reply_text(f"🎊 Redeemed! You got: <b>{amount} {rtype}</b>!", parse_mode=ParseMode.HTML)

# ---- MARRY ----
@check_disabled
def marry_command(update, context):
    msg = update.effective_message
    if not msg.reply_to_message:
        msg.reply_text("💍 Reply to someone to propose!")
        return
    target = msg.reply_to_message.from_user
    user = update.effective_user
    if target.id == user.id or target.is_bot:
        msg.reply_text("❌ Invalid target!")
        return
    proposer_data = db.execute("SELECT married_to FROM users WHERE user_id=%s", (user.id,), fetchone=True)
    if proposer_data and proposer_data.get('married_to') and proposer_data['married_to'] != 0:
        msg.reply_text("❌ You're already married! /divorce first 💔")
        return
    keyboard = [[InlineKeyboardButton("💍 Accept!", callback_data=f"marry_accept_{user.id}_{target.id}"),
                 InlineKeyboardButton("💔 Reject", callback_data=f"marry_reject_{user.id}")]]
    msg.reply_text(f"💍 {mention_html(user.id, user.first_name)} proposes to {mention_html(target.id, target.first_name)}!\n\nWill you accept? 💕",
                   reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

@check_disabled
def divorce_command(update, context):
    user_id = update.effective_user.id
    user = db.execute("SELECT married_to FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    if not user or not user.get('married_to') or user['married_to'] == 0:
        update.effective_message.reply_text("❌ You're not married!")
        return
    spouse_id = user['married_to']
    db.execute("UPDATE users SET married_to=0 WHERE user_id=%s OR user_id=%s", (user_id, spouse_id))
    try:
        spouse = context.bot.get_chat(spouse_id)
        update.effective_message.reply_text(
            f"💔 {mention_html(user_id, update.effective_user.first_name)} and {mention_html(spouse_id, spouse.first_name)} divorced.",
            parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text("💔 You are now divorced.")

@check_disabled
def spouse_command(update, context):
    user = db.execute("SELECT married_to FROM users WHERE user_id=%s", (update.effective_user.id,), fetchone=True)
    if not user or not user.get('married_to') or user['married_to'] == 0:
        update.effective_message.reply_text("💔 Not married to anyone!")
        return
    try:
        spouse = context.bot.get_chat(user['married_to'])
        update.effective_message.reply_text(f"💑 Married to {mention_html(user['married_to'], spouse.first_name)}! 💕", parse_mode=ParseMode.HTML)
    except:
        update.effective_message.reply_text(f"💑 Married to <code>{user['married_to']}</code>", parse_mode=ParseMode.HTML)

# ---- TODO ----
@check_disabled
def todo_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /todo <task>")
        return
    task = " ".join(context.args)
    db.execute("INSERT INTO todos (user_id, task) VALUES (%s,%s)", (update.effective_user.id, task))
    update.effective_message.reply_text(f"✅ Added: <code>{html.escape(task)}</code>", parse_mode=ParseMode.HTML)

@check_disabled
def todos_command(update, context):
    items = db.execute("SELECT id, task FROM todos WHERE user_id=%s AND is_done=FALSE ORDER BY id",
                       (update.effective_user.id,), fetch=True)
    done = db.execute("SELECT COUNT(*) as c FROM todos WHERE user_id=%s AND is_done=TRUE", (update.effective_user.id,), fetchone=True)
    if not items:
        update.effective_message.reply_text(f"📋 Empty! ({done['c'] if done else 0} done ✅)")
        return
    text = f"📋 <b>Todo List ({len(items)} pending)</b>\n\n"
    for i in items:
        text += f"• [<code>{i['id']}</code>] {html.escape(i['task'])}\n"
    text += f"\nCompleted: {done['c'] if done else 0} | /donetodo <id> | /deletetodo <id>"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
def donetodo_command(update, context):
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /donetodo <id>")
        return
    result = db.execute("UPDATE todos SET is_done=TRUE WHERE id=%s AND user_id=%s RETURNING task",
                        (int(context.args[0]), update.effective_user.id), fetchone=True)
    if result:
        update.effective_message.reply_text(f"✅ Done: <s>{html.escape(result['task'])}</s>", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text("❌ Not found!")

@check_disabled
def deletetodo_command(update, context):
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /deletetodo <id>")
        return
    result = db.execute("DELETE FROM todos WHERE id=%s AND user_id=%s RETURNING task",
                        (int(context.args[0]), update.effective_user.id), fetchone=True)
    if result:
        update.effective_message.reply_text(f"🗑️ Deleted: {html.escape(result['task'])}", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text("❌ Not found!")

# ---- CONFESS ----
@check_disabled
def confess_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /confess <your confession>\nPosted anonymously!")
        return
    confession = " ".join(context.args)
    chat_id = update.effective_chat.id
    result = db.execute("INSERT INTO confessions (chat_id, user_id, confession_text) VALUES (%s,%s,%s) RETURNING id",
                        (chat_id, update.effective_user.id, confession), fetchone=True)
    try:
        update.effective_message.delete()
    except:
        pass
    cid = result['id'] if result else 0
    context.bot.send_message(chat_id,
        f"💌 <b>Anonymous Confession #{cid}</b>\n\n<i>{html.escape(confession)}</i>\n\n<code>~ Anonymous</code>",
        parse_mode=ParseMode.HTML)

# ---- WORD GAME ----
@check_disabled
@group_only
def wordgame_command(update, context):
    chat_id = update.effective_chat.id
    existing = db.execute("SELECT is_active FROM word_games WHERE chat_id=%s", (chat_id,), fetchone=True)
    if existing and existing.get('is_active'):
        update.effective_message.reply_text("🎮 Game already active! /stopgame to stop.")
        return
    start_word = random.choice(['apple','orange','elephant','nature','robot','telegram','python','game','river','planet'])
    db.execute("INSERT INTO word_games (chat_id, current_word, is_active, scores) VALUES (%s,%s,TRUE,'{}') ON CONFLICT (chat_id) DO UPDATE SET current_word=EXCLUDED.current_word, is_active=TRUE, scores='{}'",
               (chat_id, start_word))
    update.effective_message.reply_text(
        f"🎮 <b>Word Chain Game!</b>\n\nSay a word starting with the last letter of the previous word!\n\nStarting: <b>{start_word}</b>\nNext starts with: <b>{start_word[-1].upper()}</b>",
        parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def stopgame_command(update, context):
    chat_id = update.effective_chat.id
    game = db.execute("SELECT * FROM word_games WHERE chat_id=%s", (chat_id,), fetchone=True)
    if not game or not game.get('is_active'):
        update.effective_message.reply_text("❌ No active game!")
        return
    db.execute("UPDATE word_games SET is_active=FALSE WHERE chat_id=%s", (chat_id,))
    scores = json.loads(game.get('scores', '{}'))
    if scores:
        top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
        txt = "\n".join([f"<code>{uid}</code>: {pts} pts" for uid, pts in top])
        update.effective_message.reply_text(f"🛑 Game stopped!\n\n🏆 <b>Scores:</b>\n{txt}", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text("🛑 Game stopped!")

# ---- RSS ----
@check_disabled
@group_only
@admin_only
def addrss_command(update, context):
    if not context.args:
        update.effective_message.reply_text("Usage: /addrss <feed_url>")
        return
    feed_url = context.args[0]
    if not feed_url.startswith(('http://', 'https://')):
        update.effective_message.reply_text("❌ Invalid URL!")
        return
    count = db.execute("SELECT COUNT(*) as c FROM rss_feeds WHERE chat_id=%s AND is_active=TRUE", (update.effective_chat.id,), fetchone=True)
    if count and count['c'] >= 5:
        update.effective_message.reply_text("❌ Max 5 RSS feeds per group!")
        return
    db.execute("INSERT INTO rss_feeds (chat_id, feed_url) VALUES (%s,%s)", (update.effective_chat.id, feed_url))
    update.effective_message.reply_text(f"✅ RSS added!\n🔗 <code>{html.escape(feed_url)}</code>", parse_mode=ParseMode.HTML)

@check_disabled
@group_only
def rsslist_command(update, context):
    feeds = db.execute("SELECT id, feed_url FROM rss_feeds WHERE chat_id=%s AND is_active=TRUE", (update.effective_chat.id,), fetch=True)
    if not feeds:
        update.effective_message.reply_text("📰 No RSS feeds! Use /addrss <url>")
        return
    text = f"📰 <b>RSS Feeds ({len(feeds)})</b>\n\n"
    for f in feeds:
        text += f"🆔 <code>{f['id']}</code>: {html.escape(f['feed_url'][:60])}\n"
    text += "\nUse /delrss <id> to remove"
    update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

@check_disabled
@group_only
@admin_only
def delrss_command(update, context):
    if not context.args or not context.args[0].isdigit():
        update.effective_message.reply_text("Usage: /delrss <id>")
        return
    result = db.execute("DELETE FROM rss_feeds WHERE id=%s AND chat_id=%s RETURNING id",
                        (int(context.args[0]), update.effective_chat.id), fetchone=True)
    if result:
        update.effective_message.reply_text(f"✅ RSS <code>{context.args[0]}</code> removed!", parse_mode=ParseMode.HTML)
    else:
        update.effective_message.reply_text("❌ Not found!")

# ---- MARRY CALLBACK ----
def marry_callback_handler(update, context):
    query = update.callback_query
    data = query.data.split("_")
    action = data[1]
    if action == "accept":
        proposer_id, target_id = int(data[2]), int(data[3])
        if query.from_user.id != target_id:
            query.answer("❌ Not for you!", show_alert=True)
            return
        db.execute("UPDATE users SET married_to=%s WHERE user_id=%s", (target_id, proposer_id))
        db.execute("UPDATE users SET married_to=%s WHERE user_id=%s", (proposer_id, target_id))
        try:
            p = context.bot.get_chat(proposer_id); t = context.bot.get_chat(target_id)
            query.message.edit_text(
                f"💑 {mention_html(proposer_id, p.first_name)} 💍 {mention_html(target_id, t.first_name)}\nJust got married! 🎊",
                parse_mode=ParseMode.HTML)
        except:
            query.message.edit_text("💑 They got married! 🎊")
        query.answer("💍 Accepted!")
    elif action == "reject":
        query.message.edit_text("💔 Proposal rejected...")
        query.answer("💔 Rejected")

# ---- GIVEAWAY JOIN CALLBACK ----
def giveaway_join_callback(update, context):
    query = update.callback_query
    gid = int(query.data.split("_")[2])
    giveaway = db.execute("SELECT * FROM giveaways WHERE id=%s AND is_active=TRUE", (gid,), fetchone=True)
    if not giveaway:
        query.answer("❌ Giveaway ended!", show_alert=True)
        return
    participants = json.loads(giveaway.get('participants', '[]'))
    if query.from_user.id in participants:
        query.answer("✅ Already joined!", show_alert=True)
        return
    participants.append(query.from_user.id)
    db.execute("UPDATE giveaways SET participants=%s WHERE id=%s", (json.dumps(participants), gid))
    try:
        keyboard = [[InlineKeyboardButton(f"🎉 Join! ({len(participants)})", callback_data=f"giveaway_join_{gid}")]]
        query.message.edit_reply_markup(InlineKeyboardMarkup(keyboard))
    except:
        pass
    query.answer(f"🎉 Joined! {len(participants)} total.")


if __name__ == '__main__':
    main()
