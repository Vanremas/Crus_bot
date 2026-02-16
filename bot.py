import logging
import sqlite3
import uuid
import html
import os
from datetime import datetime, timedelta

from telegram import Update, ChatMember, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
)

# ========================== КОНСТАНТЫ И НАСТРОЙКИ ==========================
if os.environ.get('RAILWAY_ENVIRONMENT') or os.path.exists('/railway'):
    DB_PATH = '/data/clanbot.db'
    os.makedirs('/data', exist_ok=True)
else:
    DB_PATH = 'clanbot.db'

TOKEN = os.environ.get('TOKEN') or '8235761382:AAGil59hWQ_fcTefFAYqohFcVm6Lw9eu6oM'
if not TOKEN:
    raise ValueError("❌ Переменная окружения TOKEN не задана!")

CLAN_CHAT_ID = -1003378716036
ADMIN_IDS = [906717241]

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========================
def escape_markdown_v2(text):
    """Экранирует спецсимволы MarkdownV2 в тексте."""
    if not text:
        return text
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, '\\' + char)
    return text

# ========================== РАБОТА С БАЗОЙ ДАННЫХ ==========================
def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            nickname TEXT,
            verified_at TEXT
        )
    ''')
    try:
        cur.execute('ALTER TABLE users ADD COLUMN nickname TEXT')
    except sqlite3.OperationalError:
        pass

    cur.execute('''
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            broadcast_id TEXT,
            choice TEXT,
            voted_at TEXT,
            UNIQUE(user_id, broadcast_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS stats_messages (
            broadcast_id TEXT PRIMARY KEY,
            admin_id INTEGER,
            message_id INTEGER,
            created_at TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS broadcast_texts (
            broadcast_id TEXT PRIMARY KEY,
            text TEXT,
            created_at TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS user_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            broadcast_id TEXT,
            attended INTEGER DEFAULT 0,
            marked_at TEXT,
            UNIQUE(user_id, broadcast_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            total_events INTEGER DEFAULT 0,
            attended_events INTEGER DEFAULT 0,
            attendance_percent REAL DEFAULT 0,
            last_active TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS user_nickname_changes (
            user_id INTEGER PRIMARY KEY,
            last_change TEXT
        )
    ''')

    for col in [
        ('cooldown_minutes', 'INTEGER DEFAULT 0'),
        ('event_time', 'TEXT'),
        ('reminder_sent', 'INTEGER DEFAULT 0'),
        ('expired_notified', 'INTEGER DEFAULT 0')
    ]:
        try:
            cur.execute(f'ALTER TABLE broadcast_texts ADD COLUMN {col[0]} {col[1]}')
        except sqlite3.OperationalError:
            pass

    conn.commit()
    conn.close()

# ---------- Функции для работы с пользователями ----------
def get_user_nickname(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT nickname FROM users WHERE user_id = ?', (user_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def update_user_nickname(user_id, new_nickname):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET nickname = ? WHERE user_id = ?', (new_nickname, user_id))
    conn.commit()
    conn.close()

def get_last_nickname_change(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT last_change FROM user_nickname_changes WHERE user_id = ?', (user_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def set_last_nickname_change(user_id, timestamp):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO user_nickname_changes (user_id, last_change)
        VALUES (?, ?)
    ''', (user_id, timestamp))
    conn.commit()
    conn.close()

def can_change_nickname(user_id):
    last = get_last_nickname_change(user_id)
    if not last:
        return True, 0
    try:
        last_time = datetime.fromisoformat(last)
        now = datetime.now()
        delta = now - last_time
        if delta.total_seconds() >= 24 * 3600:
            return True, 0
        else:
            remaining = 24 * 3600 - delta.total_seconds()
            return False, int(remaining)
    except:
        return True, 0

def get_user_attended_count(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT attended_events FROM user_stats WHERE user_id = ?', (user_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else 0

def get_user_broadcasts(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT DISTINCT v.broadcast_id
        FROM votes v
        WHERE v.user_id = ?
        UNION
        SELECT DISTINCT ua.broadcast_id
        FROM user_activity ua
        WHERE ua.user_id = ?
        ORDER BY broadcast_id DESC
    ''', (user_id, user_id))
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows]

def get_broadcast_info(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT text, created_at, event_time FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {'text': row[0], 'created_at': row[1], 'event_time': row[2]}
    return None

def get_user_choice_and_attendance(user_id, broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT choice FROM votes WHERE user_id = ? AND broadcast_id = ?', (user_id, broadcast_id))
    vote = cur.fetchone()
    choice = vote[0] if vote else None
    cur.execute('SELECT attended FROM user_activity WHERE user_id = ? AND broadcast_id = ?', (user_id, broadcast_id))
    att = cur.fetchone()
    attended = att[0] if att else 0
    conn.close()
    return choice, attended

# ---------- Остальные функции базы данных ----------
def save_vote(user_id, broadcast_id, choice):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO votes (user_id, broadcast_id, choice, voted_at)
        VALUES (?, ?, ?, ?)
    ''', (user_id, broadcast_id, choice, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def save_broadcast_text(broadcast_id, text):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO broadcast_texts (broadcast_id, text, created_at)
        VALUES (?, ?, ?)
    ''', (broadcast_id, text, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    logger.info(f"Текст рассылки {broadcast_id} сохранён в БД: {text}")

def get_broadcast_text(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT text FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def update_user_attendance(user_id, broadcast_id, attended):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO user_activity (user_id, broadcast_id, attended, marked_at)
        VALUES (?, ?, ?, ?)
    ''', (user_id, broadcast_id, 1 if attended else 0, datetime.now().isoformat()))
    conn.commit()
    _update_user_stats(user_id)
    conn.close()

def _update_user_stats(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT DISTINCT broadcast_id FROM (
            SELECT broadcast_id FROM votes WHERE user_id = ?
            UNION
            SELECT broadcast_id FROM user_activity WHERE user_id = ?
        )
    ''', (user_id, user_id))
    total_events = len(cur.fetchall())
    cur.execute('SELECT COUNT(*) FROM user_activity WHERE user_id = ? AND attended = 1', (user_id,))
    attended_events = cur.fetchone()[0] or 0
    attendance_percent = (attended_events / total_events * 100) if total_events > 0 else 0

    cur.execute('''
        INSERT OR REPLACE INTO user_stats (user_id, total_events, attended_events, attendance_percent, last_active)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, total_events, attended_events, attendance_percent, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def recalc_all_stats():
    logger.info("Начинаю пересчёт статистики...")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT DISTINCT user_id FROM (
            SELECT user_id FROM votes
            UNION
            SELECT user_id FROM user_activity
        )
    ''')
    users = cur.fetchall()
    conn.close()
    for (uid,) in users:
        _update_user_stats(uid)
    logger.info(f"Статистика пересчитана для {len(users)} пользователей")
    return len(users)

def get_user_vote(user_id, broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT choice FROM votes WHERE user_id = ? AND broadcast_id = ?', (user_id, broadcast_id))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def get_vote_stats(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT choice, COUNT(*) FROM votes WHERE broadcast_id = ? GROUP BY choice', (broadcast_id,))
    results = cur.fetchall()
    conn.close()
    stats = {'going': 0, 'not_going': 0}
    for choice, count in results:
        if choice == 'going':
            stats['going'] = count
        elif choice == 'not_going':
            stats['not_going'] = count
    return stats

def get_formatted_stats(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute('SELECT cooldown_minutes, event_time FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
    binfo = cur.fetchone()
    cooldown = binfo[0] if binfo else 0
    event_time = binfo[1] if binfo else None

    cur.execute('''
        SELECT v.user_id, v.choice, u.username, u.first_name, u.nickname
        FROM votes v
        LEFT JOIN users u ON v.user_id = u.user_id
        WHERE v.broadcast_id = ?
        ORDER BY v.voted_at DESC
    ''', (broadcast_id,))
    votes = cur.fetchall()

    cur.execute('SELECT user_id, username, first_name, nickname FROM users ORDER BY verified_at DESC')
    all_users = cur.fetchall()
    conn.close()

    voted_user_ids = set()
    going_list = []
    not_going_list = []

    for uid, choice, username, first_name, nickname in votes:
        voted_user_ids.add(uid)
        display_name = nickname or first_name or "Unknown"
        safe_name = escape_markdown_v2(display_name)
        safe_username = escape_markdown_v2(username) if username else None
        display = f"👤 {safe_name}" + (f" (@{safe_username})" if safe_username else "")
        if choice == 'going':
            going_list.append(display)
        else:
            not_going_list.append(display)

    ignored_list = []
    for uid, username, first_name, nickname in all_users:
        if uid not in voted_user_ids:
            display_name = nickname or first_name or "Unknown"
            safe_name = escape_markdown_v2(display_name)
            safe_username = escape_markdown_v2(username) if username else None
            display = f"👤 {safe_name}" + (f" (@{safe_username})" if safe_username else "")
            ignored_list.append(display)

    text = f"📊 Статистика голосования\n"
    text += f"🆔 Рассылка: {broadcast_id}\n"
    if cooldown:
        text += f"⏱ Кулдаун: {cooldown} мин.\n"
    if event_time:
        try:
            dt = datetime.fromisoformat(event_time)
            text += f"🕒 Время события: {dt.strftime('%d.%m.%Y %H:%M')}\n"
        except:
            safe_event_time = escape_markdown_v2(event_time)
            text += f"🕒 Время события: {safe_event_time}\n"
    text += f"🕒 Обновлено: {datetime.now().strftime('%H:%M:%S')}\n\n"

    text += f"✅: {len(going_list)}\n"
    if going_list:
        for user in going_list:
            text += f"{user}\n"
    else:
        text += "— пока никого —\n"

    text += f"\n❌: {len(not_going_list)}\n"
    if not_going_list:
        for user in not_going_list:
            text += f"{user}\n"
    else:
        text += "— пока никого —\n"

    total_users = len(all_users)
    voted_count = len(voted_user_ids)
    ignored_count = total_users - voted_count

    text += f"\n⚠️ Проигнорировали: {ignored_count} из {total_users}\n"
    if ignored_list:
        text += "Список слишком длинный, используйте /ignored ID_рассылки для просмотра"
    else:
        text += "— все проголосовали —"

    return text

def add_user(user_id, username, first_name, nickname):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO users (user_id, username, first_name, nickname, verified_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, username, first_name, nickname, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()

def remove_user(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def get_all_users():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    users = cur.fetchall()
    conn.close()
    return [uid for (uid,) in users]

def is_user_verified(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    result = cur.fetchone()
    conn.close()
    return result is not None

def save_broadcast_with_params(broadcast_id, text, cooldown_minutes, event_time):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO broadcast_texts
        (broadcast_id, text, created_at, cooldown_minutes, event_time, reminder_sent, expired_notified)
        VALUES (?, ?, ?, ?, ?, 0, 0)
    ''', (broadcast_id, text, datetime.now().isoformat(), cooldown_minutes, event_time))
    conn.commit()
    conn.close()
    logger.info(f"Текст рассылки {broadcast_id} сохранён с параметрами: cooldown={cooldown_minutes}, event_time={event_time}")

def get_broadcast_cooldown(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT cooldown_minutes FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else 0

def get_broadcast_event_time(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT event_time FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def mark_reminder_sent(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('UPDATE broadcast_texts SET reminder_sent = 1 WHERE broadcast_id = ?', (broadcast_id,))
    conn.commit()
    conn.close()

def can_change_vote(user_id, broadcast_id, cooldown_minutes):
    if cooldown_minutes == 0:
        return True, 0

    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT voted_at FROM votes
        WHERE user_id = ? AND broadcast_id = ?
        ORDER BY voted_at DESC LIMIT 1
    ''', (user_id, broadcast_id))
    result = cur.fetchone()
    conn.close()

    if not result:
        return True, 0

    try:
        last_vote_time = datetime.fromisoformat(result[0])
        now = datetime.now()
        minutes_passed = (now - last_vote_time).total_seconds() / 60
        if minutes_passed >= cooldown_minutes:
            return True, 0
        else:
            remaining = cooldown_minutes - minutes_passed
            return False, round(remaining, 1)
    except:
        return True, 0

def parse_event_time(time_input):
    time_input = time_input.strip()
    if time_input == '0':
        return None

    if time_input.startswith('+'):
        try:
            hours = int(time_input[1:])
            event_time = datetime.now() + timedelta(hours=hours)
            return event_time.isoformat()
        except:
            return False

    if ':' in time_input and len(time_input) <= 5:
        try:
            hour, minute = map(int, time_input.split(':'))
            now = datetime.now()
            event_time = datetime(now.year, now.month, now.day, hour, minute)
            if event_time < now:
                event_time += timedelta(days=1)
            return event_time.isoformat()
        except:
            return False

    try:
        event_time = datetime.strptime(time_input, "%d.%m.%Y %H:%M")
        return event_time.isoformat()
    except:
        try:
            event_time = datetime.strptime(time_input, "%d.%m.%Y")
            return event_time.isoformat()
        except:
            return False

def save_stats_message(broadcast_id, admin_id, message_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT OR REPLACE INTO stats_messages (broadcast_id, admin_id, message_id, created_at)
        VALUES (?, ?, ?, ?)
    ''', (broadcast_id, admin_id, message_id, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    logger.info(f"Saved stats message {message_id} for broadcast {broadcast_id} in DB")

def get_stats_message(broadcast_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT message_id FROM stats_messages WHERE broadcast_id = ?', (broadcast_id,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

# ========================== КЛАВИАТУРЫ ==========================
def get_verify_keyboard():
    keyboard = [[InlineKeyboardButton("✅ Верифицироваться", callback_data='start_verify')]]
    return InlineKeyboardMarkup(keyboard)

def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("📅 Рассылка с событием", callback_data='admin_broadcast_event')],
        [
            InlineKeyboardButton("📊 Статистика", callback_data='admin_stats'),
            InlineKeyboardButton("👥 Пользователи", callback_data='admin_users')
        ],
        [
            InlineKeyboardButton("📋 Архив рассылок", callback_data='admin_broadcasts_list'),
            InlineKeyboardButton("🏆 Рейтинг", callback_data='admin_rating')
        ],
        [
            InlineKeyboardButton("🔄 Сбросить статистику", callback_data='admin_reset_stats'),
            InlineKeyboardButton("❌ Закрыть", callback_data='admin_close')
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_stats_keyboard(broadcast_id):
    logger.info(f"Функция get_stats_keyboard вызвана для {broadcast_id}")
    keyboard = [
        [
            InlineKeyboardButton("📊 Обновить", callback_data=f'refresh_stats_{broadcast_id}'),
            InlineKeyboardButton("📋 Копировать ID", callback_data=f'copy_id_{broadcast_id}')
        ],
        [
            InlineKeyboardButton("👥 Игнорируют", callback_data=f'ignored_list_{broadcast_id}'),
            InlineKeyboardButton("🗑 Удалить рассылку", callback_data=f'delete_broadcast_{broadcast_id}')
        ],
        [
            InlineKeyboardButton("❌ Закрыть", callback_data='close_stats')
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_me_keyboard(user_id):
    can_change, remaining = can_change_nickname(user_id)
    buttons = []
    if can_change:
        buttons.append([InlineKeyboardButton("✏️ Изменить ник", callback_data='change_nickname')])
    else:
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        time_str = f"{hours}ч {minutes}м" if hours else f"{minutes}м"
        buttons.append([InlineKeyboardButton(f"⏳ Изменить ник (доступно через {time_str})", callback_data='nickname_cooldown')])
    buttons.append([InlineKeyboardButton("📋 Мои рассылки", callback_data='my_broadcasts')])
    return InlineKeyboardMarkup(buttons)

def get_my_broadcasts_keyboard(broadcasts, page, total_pages):
    keyboard = []
    per_page = 5
    start = (page - 1) * per_page
    for i, bid in enumerate(broadcasts[start:start+per_page], start=start+1):
        short = bid[:6] + "..." if len(bid) > 6 else bid
        keyboard.append([InlineKeyboardButton(f"{i}. {short}", callback_data=f'my_broadcast_detail_{bid}')])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️", callback_data=f'my_broadcasts_page_{page-1}'))
    if page < total_pages:
        nav.append(InlineKeyboardButton("▶️", callback_data=f'my_broadcasts_page_{page+1}'))
    if nav:
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("◀️ Назад в профиль", callback_data='back_to_me')])
    return InlineKeyboardMarkup(keyboard)

# ========================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ-ОБРАБОТЧИКИ ==========================
async def show_ignored_list(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    """Показывает список проигнорировавших рассылку."""
    query = update.callback_query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT user_id FROM votes WHERE broadcast_id = ?', (broadcast_id,))
    voted_users = {row[0] for row in cur.fetchall()}
    cur.execute('SELECT user_id, username, first_name, nickname FROM users ORDER BY verified_at DESC')
    all_users = cur.fetchall()
    conn.close()

    if not all_users:
        await query.answer("📭 В базе нет пользователей", show_alert=True)
        return

    ignored_list = []
    for uid, username, first_name, nickname in all_users:
        if uid not in voted_users:
            display_name = nickname or first_name or "Unknown"
            # Экранируем для HTML
            safe_display_name = html.escape(display_name)
            safe_username = html.escape(username) if username else None
            display = f"👤 {safe_display_name}" + (f" (@{safe_username})" if safe_username else "")
            ignored_list.append(display)

    total = len(all_users)
    voted = len(voted_users)
    ignored = total - voted

    if not ignored_list:
        await query.answer("✅ Все пользователи проголосовали!", show_alert=True)
        return

    text = f"<b>📋 Проигнорировали рассылку</b> <code>{broadcast_id}</code>\n"
    text += f"📊 Всего: {total} | Проголосовало: {voted} | Игнор: {ignored}\n\n"

    if len(ignored_list) <= 10:
        for user in ignored_list:
            text += f"{user}\n"
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("◀️ Назад к статистике", callback_data=f'back_to_stats_{broadcast_id}')
        ]])
        await query.edit_message_text(text, reply_markup=markup, parse_mode='HTML')
    else:
        for user in ignored_list[:10]:
            text += f"{user}\n"
        text += f"\n... и еще {ignored - 10} пользователей"
        keyboard = [
            [InlineKeyboardButton("📥 Скачать полный список", callback_data=f'download_ignored_{broadcast_id}')],
            [InlineKeyboardButton("◀️ Назад к статистике", callback_data=f'back_to_stats_{broadcast_id}')]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    await query.answer()

async def show_broadcasts_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список всех рассылок с пагинацией (админский)."""
    query = update.callback_query
    parts = query.data.split('_')
    if len(parts) > 2 and parts[-2] == 'page':
        page = int(parts[-1])
    else:
        page = 1

    per_page = 5
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM stats_messages')
    total = cur.fetchone()[0]
    cur.execute('''
        SELECT s.broadcast_id, s.created_at, b.text, COUNT(v.id) as votes_count
        FROM stats_messages s
        LEFT JOIN broadcast_texts b ON s.broadcast_id = b.broadcast_id
        LEFT JOIN votes v ON s.broadcast_id = v.broadcast_id
        GROUP BY s.broadcast_id
        ORDER BY s.created_at DESC
        LIMIT ? OFFSET ?
    ''', (per_page, (page - 1) * per_page))
    broadcasts = cur.fetchall()
    conn.close()

    if not broadcasts:
        if page == 1:
            await query.answer("📭 Нет отправленных рассылок", show_alert=True)
        else:
            await query.answer("📭 Страница пуста", show_alert=True)
        return

    total_pages = (total - 1) // per_page + 1
    text = f"<b>📋 Архив рассылок</b> (стр. {page}/{total_pages})\n\n"

    for i, (bid, created_at, preview, votes_cnt) in enumerate(broadcasts, 1):
        date_str = created_at[:16] if created_at else "неизвестно"
        preview_text = (preview[:30] + "...") if preview and len(preview) > 30 else (preview or "Нет текста")
        text += f"{i}. <code>{bid}</code>\n"
        text += f"   📅 {date_str}\n"
        text += f"   📝 {preview_text}\n"
        text += f"   📊 Голосов: {votes_cnt}\n\n"

    keyboard = []
    for i, (bid, _, _, _) in enumerate(broadcasts, 1):
        short_id = bid[:6] + "..." if len(bid) > 6 else bid
        keyboard.append([InlineKeyboardButton(f"{i}. {short_id}", callback_data=f'select_broadcast_{bid}')])

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f'broadcasts_page_{page - 1}'))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f'broadcasts_page_{page + 1}'))
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([
        InlineKeyboardButton("🏠 В главное меню", callback_data='admin_back'),
        InlineKeyboardButton("🗑 Удалить все", callback_data='delete_all_broadcasts')
    ])

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    await query.answer()

async def show_broadcast_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    query = update.callback_query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT text FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
    text_result = cur.fetchone()
    broadcast_text = text_result[0] if text_result else "Текст не найден"
    broadcast_text = escape_markdown_v2(broadcast_text)
    
    cur.execute('SELECT created_at FROM stats_messages WHERE broadcast_id = ?', (broadcast_id,))
    date_result = cur.fetchone()
    created_at = date_result[0][:16] if date_result else "неизвестно"
    # Экранируем дату, так как она содержит дефисы
    created_at = escape_markdown_v2(created_at)

    cur.execute('''
        SELECT v.user_id, v.choice, u.nickname, u.username, COALESCE(ua.attended, 0) as attended
        FROM votes v
        LEFT JOIN users u ON v.user_id = u.user_id
        LEFT JOIN user_activity ua ON v.user_id = ua.user_id AND ua.broadcast_id = ?
        WHERE v.broadcast_id = ?
        ORDER BY v.choice, u.nickname
    ''', (broadcast_id, broadcast_id))
    votes = cur.fetchall()

    cur.execute('''
        SELECT u.user_id, u.nickname, u.username, COALESCE(ua.attended, 0) as attended
        FROM users u
        LEFT JOIN user_activity ua ON u.user_id = ua.user_id AND ua.broadcast_id = ?
        ORDER BY u.nickname
    ''', (broadcast_id,))
    all_users = cur.fetchall()
    conn.close()

    voted_ids = set()
    going = []
    not_going = []
    
    for uid, choice, nick, username, attended in votes:
        voted_ids.add(uid)
        status = "🟢" if attended else "🔴"
        safe_nick = escape_markdown_v2(nick) if nick else 'Без ника'
        safe_username = escape_markdown_v2(username) if username else None
        display = f"{status} {safe_nick}" + (f" | @{safe_username}" if safe_username else "")
        if choice == 'going':
            going.append(display)
        else:
            not_going.append(display)

    ignored = []
    for uid, nick, username, attended in all_users:
        if uid not in voted_ids:
            status = "🟢" if attended else "🔴"
            safe_nick = escape_markdown_v2(nick) if nick else 'Без ника'
            safe_username = escape_markdown_v2(username) if username else None
            display = f"{status} {safe_nick}" + (f" | @{safe_username}" if safe_username else "")
            ignored.append(display)

    text = f"📢 **{broadcast_text}**\n"
    text += f"🆔 `{broadcast_id}`\n"
    text += f"📅 {created_at}\n\n"
    text += f"✅ ** ({len(going)}):**\n"
    for i, user in enumerate(going, 1):
        text += f"{i}. {user}\n"
    text += "\n"
    text += f"❌ ** ({len(not_going)}):**\n"
    for i, user in enumerate(not_going, 1):
        text += f"{i}. {user}\n"
    text += "\n"
    text += f"⚠️ **Проигнорировали ({len(ignored)}):**\n"
    for i, user in enumerate(ignored, 1):
        text += f"{i}. {user}\n"

    keyboard = [
        [InlineKeyboardButton("✅ Отметить присутствие", callback_data=f'mark_attendance_{broadcast_id}'),
         InlineKeyboardButton("🗑 Удалить", callback_data=f'delete_broadcast_{broadcast_id}')],
        [InlineKeyboardButton("◀️ Назад к списку", callback_data='admin_broadcasts_list'),
         InlineKeyboardButton("❌ Закрыть", callback_data='close_stats')]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2')
    await query.answer()

async def mark_attendance(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    query = update.callback_query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT u.user_id, u.nickname, u.username,
               COALESCE(v.choice, 'ignored') as choice,
               COALESCE(ua.attended, 0) as attended
        FROM users u
        LEFT JOIN votes v ON u.user_id = v.user_id AND v.broadcast_id = ?
        LEFT JOIN user_activity ua ON u.user_id = ua.user_id AND ua.broadcast_id = ?
        ORDER BY
            CASE
                WHEN v.choice = 'going' THEN 1
                WHEN v.choice = 'not_going' THEN 2
                ELSE 3
            END,
            u.nickname
    ''', (broadcast_id, broadcast_id))
    all_users = cur.fetchall()
    conn.close()

    if not all_users:
        await query.answer("❌ В базе нет пользователей", show_alert=True)
        return

    text = f"📝 **Отметка присутствия**\nРассылка: `{broadcast_id}`\n\n"
    going, not_going, ignored = [], [], []
    
    for uid, nick, username, choice, attended in all_users:
        status = "✅" if attended else "⬜"
        safe_nick = escape_markdown_v2(nick) if nick else None
        safe_username = escape_markdown_v2(username) if username else None
        name = safe_nick or safe_username or f"ID {uid}"
        display = f"{status} {name}"
        if choice == 'going':
            going.append(display)
        elif choice == 'not_going':
            not_going.append(display)
        else:
            ignored.append(display)

    counter = 1
    user_map = {}
    
    if going:
        text += f"✅ ** ({len(going)}):**\n"
        for display in going:
            text += f"{counter}. {display}\n"
            user_map[str(counter)] = all_users[counter - 1][0]
            counter += 1
        text += "\n"
    if not_going:
        text += f"❌ ** ({len(not_going)}):**\n"
        for display in not_going:
            text += f"{counter}. {display}\n"
            user_map[str(counter)] = all_users[counter - 1][0]
            counter += 1
        text += "\n"
    if ignored:
        text += f"⚠️ **Проигнорировали ({len(ignored)}):**\n"
        for display in ignored:
            text += f"{counter}. {display}\n"
            user_map[str(counter)] = all_users[counter - 1][0]
            counter += 1

    context.user_data['attendance_map'] = user_map
    context.user_data['attendance_broadcast'] = broadcast_id
    context.user_data['attendance_total'] = len(all_users)

    keyboard = [
        [InlineKeyboardButton("✅ Отметить всех", callback_data=f'attend_all_{broadcast_id}'),
         InlineKeyboardButton("❌ Сбросить всех", callback_data=f'unattend_all_{broadcast_id}')],
        [InlineKeyboardButton("🔢 Ввести номера", callback_data=f'enter_numbers_{broadcast_id}'),
         InlineKeyboardButton("◀️ Назад", callback_data=f'broadcast_detail_{broadcast_id}')]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2')
    await query.answer()

async def enter_attendance_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    query = update.callback_query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT u.user_id, u.nickname, u.username,
               COALESCE(v.choice, 'ignored') as choice,
               COALESCE(ua.attended, 0) as attended
        FROM users u
        LEFT JOIN votes v ON u.user_id = v.user_id AND v.broadcast_id = ?
        LEFT JOIN user_activity ua ON u.user_id = ua.user_id AND ua.broadcast_id = ?
        ORDER BY
            CASE
                WHEN v.choice = 'going' THEN 1
                WHEN v.choice = 'not_going' THEN 2
                ELSE 3
            END,
            u.nickname
    ''', (broadcast_id, broadcast_id))
    all_users = cur.fetchall()
    conn.close()

    text = f"📝 **Отметка присутствия**\nРассылка: `{broadcast_id}`\n\n**Список пользователей:**\n\n"
    going, not_going, ignored = [], [], []
    
    for uid, nick, username, choice, attended in all_users:
        status = "✅" if attended else "⬜"
        safe_nick = escape_markdown_v2(nick) if nick else None
        safe_username = escape_markdown_v2(username) if username else None
        name = safe_nick or safe_username or f"ID {uid}"
        display = f"{status} {name}"
        if choice == 'going':
            going.append(display)
        elif choice == 'not_going':
            not_going.append(display)
        else:
            ignored.append(display)

    counter = 1
    if going:
        text += f"✅ ** ({len(going)}):**\n"
        for display in going:
            text += f"`{counter}.` {display}\n"
            counter += 1
        text += "\n"
    if not_going:
        text += f"❌ ** ({len(not_going)}):**\n"
        for display in not_going:
            text += f"`{counter}.` {display}\n"
            counter += 1
        text += "\n"
    if ignored:
        text += f"⚠️ **Проигнорировали ({len(ignored)}):**\n"
        for display in ignored:
            text += f"`{counter}.` {display}\n"
            counter += 1
        text += "\n"

    text += f"**Всего пользователей:** {len(all_users)}\n\n"
    text += "Введи номера присутствовавших в формате:\n`1-5-8-3-9-13`\n\n"

    keyboard = [
        [InlineKeyboardButton("◀️ Назад к рассылке", callback_data=f'broadcast_detail_{broadcast_id}')],
        [InlineKeyboardButton("❌ Закрыть", callback_data='close_stats')]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2')
    context.user_data['awaiting_attendance_numbers'] = broadcast_id
    await query.answer()

async def handle_attendance_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("=== HANDLE ATTENDANCE NUMBERS ===")
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        logger.info("User not admin, skipping")
        return False
    broadcast_id = context.user_data.get('awaiting_attendance_numbers')
    if not broadcast_id:
        logger.info("Not awaiting attendance numbers, skipping")
        return False

    numbers_text = update.message.text.strip()
    if numbers_text.lower() == '/cancel':
        context.user_data.pop('awaiting_attendance_numbers', None)
        keyboard = [
            [InlineKeyboardButton("◀️ Назад к списку", callback_data='admin_broadcasts_list'),
             InlineKeyboardButton("❌ Закрыть", callback_data='close_stats')]
        ]
        await update.message.reply_text(
            f"❌ Отметка отменена.\n\nВозврат к рассылке `{broadcast_id}`:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True

    try:
        parts = numbers_text.split('-')
        numbers = [int(p.strip()) for p in parts if p.strip()]
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат. Используй формат: 1-5-8-3-9-13\nИли отправь /cancel для отмены"
        )
        return True

    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT u.user_id, u.nickname, u.username, COALESCE(v.choice, 'ignored') as choice
        FROM users u
        LEFT JOIN votes v ON u.user_id = v.user_id AND v.broadcast_id = ?
        ORDER BY
            CASE
                WHEN v.choice = 'going' THEN 1
                WHEN v.choice = 'not_going' THEN 2
                ELSE 3
            END,
            u.nickname
    ''', (broadcast_id,))
    all_users = cur.fetchall()
    conn.close()

    if not all_users:
        await update.message.reply_text("❌ В базе нет пользователей")
        return True

    marked = 0
    errors = 0
    marked_list = []
    not_found = []
    for num in numbers:
        if 1 <= num <= len(all_users):
            uid, nick, username, choice = all_users[num - 1]
            try:
                update_user_attendance(uid, broadcast_id, True)
                marked += 1
                name = nick or username or f"ID {uid}"
                marked_list.append(f"  {num}. {name}")
            except Exception as e:
                logger.error(f"Error marking attendance for user {uid}: {e}")
                errors += 1
        else:
            not_found.append(str(num))

    context.user_data.pop('awaiting_attendance_numbers', None)
    result_text = f"📊 **Результат отметки**\nРассылка: `{broadcast_id}`\n\n✅ Успешно отмечено: {marked}\n"
    if marked_list:
        result_text += "Отмечены:\n" + "\n".join(marked_list) + "\n"
    if not_found:
        result_text += f"❌ Не найдены номера: {', '.join(not_found)}\n"
    if errors > 0:
        result_text += f"⚠️ Ошибок при отметке: {errors}\n"

    keyboard = [
        [InlineKeyboardButton("◀️ К рассылке", callback_data=f'broadcast_detail_{broadcast_id}'),
         InlineKeyboardButton("📋 К списку", callback_data='admin_broadcasts_list')]
    ]
    await update.message.reply_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard))
    return True

async def show_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT u.user_id, u.nickname, u.username, s.attended_events
        FROM user_stats s
        JOIN users u ON s.user_id = u.user_id
        WHERE s.attended_events > 0
        ORDER BY s.attended_events DESC
        LIMIT 20
    ''')
    stats = cur.fetchall()
    conn.close()

    if not stats:
        await query.answer("📊 Статистика пока пуста", show_alert=True)
        return

    text = "🏆 **Рейтинг активности**\n\n"
    for i, (uid, nick, username, attended) in enumerate(stats, 1):
        safe_nick = escape_markdown_v2(nick) if nick else f"ID {uid}"
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "📌"
        text += f"{medal} {i}. {safe_nick}\n"
        text += f"   🎯 Активность: {attended}\n\n"

    keyboard = [
        [InlineKeyboardButton("◀️ Назад в админ-панель", callback_data='admin_back')],
        [InlineKeyboardButton("❌ Закрыть", callback_data='close_stats')]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2')
    await query.answer()

async def delete_all_broadcasts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить всё", callback_data='confirm_delete_all'),
         InlineKeyboardButton("❌ Нет", callback_data='admin_broadcasts_list')]
    ]
    await query.edit_message_text(
        "🗑 <b>Удаление всех рассылок</b>\n\n"
        "Вы уверены? Это удалит:\n"
        "• Все сообщения со статистикой\n"
        "• Все голоса пользователей\n"
        "• Все тексты рассылок\n\n"
        "<b>Активность пользователей сохранится!</b>",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    await query.answer()

async def confirm_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT text FROM broadcast_texts')
    broadcasts = cur.fetchall()
    cur.execute('DELETE FROM votes')
    cur.execute('DELETE FROM stats_messages')
    cur.execute('DELETE FROM broadcast_texts')
    conn.commit()
    conn.close()

    users = get_all_users()
    for uid in users:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text="❌ <b>ВСЕ РАССЫЛКИ ОТМЕНЕНЫ</b>\n\nАдминистратор отменил все активные события.",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Failed to notify {uid} about cancelled broadcasts: {e}")

    await query.answer(f"✅ Все рассылки удалены, уведомлено {len(users)} пользователей", show_alert=True)
    keyboard = get_admin_keyboard()
    await query.edit_message_text(
        "<b>👑 Админ-панель</b>\n\nВыберите действие:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

async def delete_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f'confirm_delete_{broadcast_id}'),
         InlineKeyboardButton("❌ Нет, отмена", callback_data=f'back_to_stats_{broadcast_id}')]
    ]
    await query.edit_message_text(
        f"🗑 <b>Удаление рассылки</b>\n\n"
        f"Вы уверены, что хотите удалить рассылку <code>{broadcast_id}</code>?\n"
        f"Это действие нельзя отменить!",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    await query.answer()

async def confirm_delete_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        broadcast_text = get_broadcast_text(broadcast_id) or "Без текста"
        safe_text = escape_markdown_v2(broadcast_text)

        conn = get_connection()
        cur = conn.cursor()
        cur.execute('DELETE FROM votes WHERE broadcast_id = ?', (broadcast_id,))
        votes_deleted = cur.rowcount
        cur.execute('DELETE FROM stats_messages WHERE broadcast_id = ?', (broadcast_id,))
        cur.execute('DELETE FROM broadcast_texts WHERE broadcast_id = ?', (broadcast_id,))
        conn.commit()
        conn.close()

        users = get_all_users()
        for uid in users:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"❌ **РАССЫЛКА ОТМЕНЕНА**\n\n"
                         f"Событие:\n{safe_text}\n\n"
                         f"Администратор отменил это событие.",
                    parse_mode='MarkdownV2'
                )
            except Exception as e:
                logger.error(f"Failed to notify {uid} about cancelled broadcast: {e}")

        try:
            await query.delete_message()
        except:
            pass

        keyboard = get_admin_keyboard()
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Рассылка `{broadcast_id}` успешно удалена!\n"
                 f"Удалено голосов: {votes_deleted}\n"
                 f"Уведомлено пользователей: {len(users)}",
            reply_markup=keyboard,
            parse_mode='MarkdownV2'
        )
        await query.answer()
    except Exception as e:
        logger.error(f"Error deleting broadcast {broadcast_id}: {e}")
        await query.answer("❌ Ошибка при удалении", show_alert=True)

# ========================== ФОНОВЫЕ ЗАДАЧИ ==========================
async def send_reminder(context: ContextTypes.DEFAULT_TYPE, broadcast_id, text, event_time):
    users = get_all_users()
    try:
        dt = datetime.fromisoformat(event_time)
        time_str = dt.strftime("%d.%m.%Y в %H:%M")
    except:
        time_str = event_time

    safe_text = escape_markdown_v2(text)

    for uid in users:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"⏰ **НАПОМИНАНИЕ**\n\n"
                     f"Через 30 минут начинается событие:\n"
                     f"📢 {safe_text}\n\n"
                     f"🕒 Время начала: {time_str}\n\n"
                     f"Если ты ещё не выбрал вариант - самое время!",
                parse_mode='MarkdownV2'
            )
        except Exception as e:
            logger.error(f"Failed to send reminder to {uid}: {e}")

    for admin in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin,
                text=f"✅ Напоминание о событии `{broadcast_id}` отправлено {len(users)} пользователям!"
            )
        except:
            pass

async def check_reminders(context: ContextTypes.DEFAULT_TYPE):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()
    reminder_time = now + timedelta(minutes=30)
    reminder_end = reminder_time + timedelta(minutes=1)
    cur.execute('''
        SELECT broadcast_id, text, event_time FROM broadcast_texts
        WHERE event_time IS NOT NULL
        AND reminder_sent = 0
        AND datetime(event_time) BETWEEN datetime(?) AND datetime(?)
    ''', (reminder_time.isoformat(), reminder_end.isoformat()))
    events = cur.fetchall()
    conn.close()
    for bid, text, etime in events:
        await send_reminder(context, bid, text, etime)
        mark_reminder_sent(bid)

async def check_expired_events(context: ContextTypes.DEFAULT_TYPE):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()
    cur.execute('''
        SELECT broadcast_id, text FROM broadcast_texts
        WHERE event_time IS NOT NULL
        AND expired_notified = 0
        AND datetime(event_time) < datetime(?)
    ''', (now.isoformat(),))
    expired = cur.fetchall()
    for bid, text in expired:
        users = get_all_users()
        safe_text = escape_markdown_v2(text)
        for uid in users:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"⏰ **СОБЫТИЕ НАЧАЛОСЬ**\n\n"
                         f"📢 {safe_text}\n\n"
                         f"Голосование закрыто!",
                    parse_mode='MarkdownV2'
                )
            except:
                pass
        cur.execute('UPDATE broadcast_texts SET expired_notified = 1 WHERE broadcast_id = ?', (bid,))
        conn.commit()
        logger.info(f"Event {bid} has started, notifications sent")
    conn.close()

# ========================== ОБРАБОТЧИКИ КОМАНД ==========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = get_verify_keyboard()
    await update.message.reply_text(
        "Приветствую. Я бот-менеджер клана.\n\n"
        "Чтобы получать важные объявления, нажми кнопку ниже и укажи свой ник в игре.",
        reply_markup=keyboard
    )

async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>📱 Верификация теперь происходит по-новому!</b>\n\n"
        "Чтобы верифицироваться, нажми <b>/start</b> и используй кнопку <b>'✅ Верифицироваться'</b>.\n\n"
        "Там нужно будет указать свой ник в игре.",
        parse_mode='HTML'
    )

async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ У тебя нет доступа к админ-панели.")
        return
    keyboard = get_admin_keyboard()
    await update.message.reply_text(
        "<b>👑 Админ-панель</b>\n\nВыберите действие:",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("=== НАЧАЛО ФУНКЦИИ BROADCAST ===")
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("У тебя нет прав на рассылку.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /broadcast <текст сообщения>")
        return
    broadcast_text = " ".join(context.args)
    broadcast_id = str(uuid.uuid4())[:8]
    context.user_data['current_broadcast_id'] = broadcast_id
    keyboard = [
        [InlineKeyboardButton("✅", callback_data=f'going_{broadcast_id}'),
         InlineKeyboardButton("❌", callback_data=f'not_going_{broadcast_id}')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    users = get_all_users()
    if not users:
        await update.message.reply_text("В базе нет верифицированных пользователей.")
        return
    await update.message.reply_text(f"📢 Начинаю рассылку для {len(users)} пользователей...")
    successful = 0
    failed = 0
    for uid in users:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 РАССЫЛКА КЛАНА:\n\n{broadcast_text}\n\nВыбери свой вариант:",
                reply_markup=reply_markup
            )
            successful += 1
        except Exception as e:
            logger.error(f"Failed to send to {uid}: {e}")
            failed += 1
    stats_text = get_formatted_stats(broadcast_id)
    stats_message = await context.bot.send_message(
        chat_id=user_id,
        text=stats_text,
        reply_markup=get_stats_keyboard(broadcast_id)
    )
    save_stats_message(broadcast_id, user_id, stats_message.message_id)
    await update.message.reply_text(f"✅ Рассылка завершена. Успешно: {successful}, Ошибок: {failed}")
    logger.info("=== КОНЕЦ ФУНКЦИИ BROADCAST ===")

async def track_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != CLAN_CHAT_ID:
        return
    if update.message and update.message.left_chat_member:
        left_user = update.message.left_chat_member
        user_id = left_user.id
        if is_user_verified(user_id):
            remove_user(user_id)
            logger.info(f"User {user_id} left clan chat. Removed from broadcast list.")
            for admin in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin,
                        f"Пользователь {left_user.full_name} покинул клан и удален из рассылки."
                    )
                except:
                    pass

async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_user_verified(user.id):
        await update.message.reply_text("❌ Ты ещё не верифицирован. Используй /start для верификации.")
        return

    nickname = get_user_nickname(user.id) or "Не указан"
    safe_nickname = escape_markdown_v2(nickname)
    attended = get_user_attended_count(user.id)
    text = f"👤 **Твой профиль**\n\n"
    text += f"🎮 Ник в игре: **{safe_nickname}**\n"
    text += f"📊 Посещено мероприятий: **{attended}**\n"

    await update.message.reply_text(text, reply_markup=get_me_keyboard(user.id))

# ========================== ОСНОВНОЙ CALLBACK-ОБРАБОТЧИК ==========================
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    callback_data = query.data

    logger.info(f"Callback received: {callback_data} from user {user.id}")
    logger.info(f"Current user_data keys: {list(context.user_data.keys())}")

    # -------------------- КНОПКИ ПРОФИЛЯ --------------------
    if callback_data == 'change_nickname':
        await change_nickname_start(update, context)
        return
    if callback_data == 'nickname_cooldown':
        can, remaining = can_change_nickname(user.id)
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        time_str = f"{hours}ч {minutes}м" if hours else f"{minutes}м"
        await query.answer(f"⏳ Сменить ник можно будет через {time_str}.", show_alert=True)
        return
    if callback_data == 'my_broadcasts':
        await my_broadcasts_list(update, context)
        return
    if callback_data.startswith('my_broadcasts_page_'):
        page = int(callback_data.split('_')[-1])
        await my_broadcasts_list(update, context, page)
        return
    if callback_data.startswith('my_broadcast_detail_'):
        bid = callback_data.replace('my_broadcast_detail_', '')
        await my_broadcast_detail(update, context, bid)
        return
    if callback_data == 'back_to_me':
        nickname = get_user_nickname(user.id) or "Не указан"
        safe_nickname = escape_markdown_v2(nickname)
        attended = get_user_attended_count(user.id)
        text = f"👤 **Твой профиль**\n\n"
        text += f"🎮 Ник в игре: **{safe_nickname}**\n"
        text += f"📊 Посещено мероприятий: **{attended}**\n"
        await query.edit_message_text(text, reply_markup=get_me_keyboard(user.id), parse_mode='MarkdownV2')
        await query.answer()
        return

    # -------------------- АДМИНСКИЕ КНОПКИ --------------------
    if user.id in ADMIN_IDS:
        if callback_data == 'admin_broadcast':
            await query.answer()
            await query.edit_message_text(
                "📝 **Создание рассылки**\n\n"
                "Отправь мне текст, который хочешь разослать всем верифицированным пользователям.\n\n"
                "❌ Для отмены отправь /cancel"
            )
            context.user_data['awaiting_broadcast'] = True
            return

        if callback_data == 'admin_broadcast_event':
            await query.answer()
            await query.edit_message_text(
                "📅 **Создание рассылки с событием**\n\n"
                "Шаг 1/3: Отправь текст рассылки:\n\n"
                "❌ /cancel - отмена"
            )
            context.user_data['broadcast_step'] = 1
            return

        if callback_data == 'admin_stats':
            await query.answer()
            users_count = len(get_all_users())
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM stats_messages")
            broadcasts_count = cur.fetchone()[0]
            conn.close()
            await query.edit_message_text(
                f"<b>📊 Статистика бота</b>\n\n"
                f"👥 Верифицированных пользователей: {users_count}\n"
                f"📢 Всего рассылок: {broadcasts_count}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data='admin_back')]]),
                parse_mode='HTML'
            )
            return

        if callback_data == 'admin_users' or callback_data.startswith('admin_users_'):
            await query.answer()
            page = 1
            if '_' in callback_data and callback_data.split('_')[1].isdigit():
                try:
                    page = int(callback_data.split('_')[1])
                except:
                    page = 1
            per_page = 15
            offset = (page - 1) * per_page
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            total = cur.fetchone()[0]
            cur.execute("""
                SELECT first_name, username, nickname, verified_at
                FROM users
                ORDER BY verified_at DESC
                LIMIT ? OFFSET ?
            """, (per_page, offset))
            users = cur.fetchall()
            conn.close()
            if not users:
                text = "📭 Нет верифицированных пользователей" if page == 1 else "📭 Страница пуста"
            else:
                text = f"<b>👥 Пользователи ({total})</b> - Страница {page}\n\n"
                for i, (first_name, username, nickname, verified_at) in enumerate(users, offset + 1):
                    name = nickname or first_name or "Unknown"
                    # Экранируем все поля для MarkdownV2 (здесь мы используем HTML, но экранирование не нужно, оставляем для единообразия)
                    safe_name = escape_markdown_v2(name)
                    safe_username = escape_markdown_v2(username) if username else None
                    line = f"{i}. 👤 {safe_name}"
                    if safe_username:
                        line += f" (@{safe_username})"
                    if verified_at:
                        line += f" (с {verified_at[:10]})"
                    text += line + "\n"
            keyboard = []
            nav = []
            if page > 1:
                nav.append(InlineKeyboardButton("◀️", callback_data=f'admin_users_{page - 1}'))
            if offset + per_page < total:
                nav.append(InlineKeyboardButton("▶️", callback_data=f'admin_users_{page + 1}'))
            if nav:
                keyboard.append(nav)
            keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data='admin_back')])
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            return

        if callback_data == 'admin_back':
            await query.answer()
            await query.edit_message_text(
                "<b>👑 Админ-панель</b>\n\nВыберите действие:",
                reply_markup=get_admin_keyboard(),
                parse_mode='HTML'
            )
            return

        if callback_data == 'admin_close':
            await query.answer()
            await query.delete_message()
            return

        if callback_data == 'admin_broadcasts_list':
            await query.answer()
            await show_broadcasts_list(update, context)
            return

        if callback_data == 'admin_rating':
            await query.answer()
            await show_rating(update, context)
            return

        if callback_data == 'admin_reset_stats':
            await query.answer()
            keyboard = [
                [InlineKeyboardButton("✅ Да, сбросить всё", callback_data='confirm_reset_stats'),
                 InlineKeyboardButton("❌ Нет", callback_data='admin_back')]
            ]
            await query.edit_message_text(
                "<b>⚠️ Сброс статистики</b>\n\n"
                "Это удалит ВСЮ историю активности и рейтинги.\n"
                "Пользователи останутся в базе.\n\n"
                "Точно продолжить?",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return

        if callback_data == 'confirm_reset_stats':
            await query.answer()
            conn = get_connection()
            cur = conn.cursor()
            cur.execute('DELETE FROM user_activity')
            cur.execute('DELETE FROM user_stats')
            cur.execute('DELETE FROM votes')
            cur.execute('DELETE FROM stats_messages')
            cur.execute('DELETE FROM broadcast_texts')
            conn.commit()
            conn.close()
            users = get_all_users()
            for uid in users:
                try:
                    await context.bot.send_message(
                        chat_id=uid,
                        text="❌ <b>СТАТИСТИКА СБРОШЕНА</b>\n\nАдминистратор сбросил всю статистику. Все активные рассылки отменены.",
                        parse_mode='HTML'
                    )
                except:
                    pass
            await query.answer(f"✅ Статистика полностью сброшена, уведомлено {len(users)} пользователей", show_alert=True)
            await query.edit_message_text(
                "<b>👑 Админ-панель</b>\n\nСтатистика сброшена!",
                reply_markup=get_admin_keyboard(),
                parse_mode='HTML'
            )
            return

        if callback_data.startswith('broadcasts_page_'):
            await query.answer()
            await show_broadcasts_list(update, context)
            return

        if callback_data.startswith('select_broadcast_'):
            await query.answer()
            broadcast_id = callback_data.replace('select_broadcast_', '')
            await show_broadcast_detail(update, context, broadcast_id)
            return

        if callback_data.startswith('broadcast_detail_'):
            await query.answer()
            broadcast_id = callback_data.replace('broadcast_detail_', '')
            await show_broadcast_detail(update, context, broadcast_id)
            return

        if callback_data.startswith('mark_attendance_'):
            await query.answer()
            broadcast_id = callback_data.replace('mark_attendance_', '')
            await mark_attendance(update, context, broadcast_id)
            return

        if callback_data.startswith('attend_all_'):
            await query.answer()
            broadcast_id = callback_data.replace('attend_all_', '')
            conn = get_connection()
            cur = conn.cursor()
            cur.execute('SELECT user_id FROM users')
            users = cur.fetchall()
            conn.close()
            for (uid,) in users:
                update_user_attendance(uid, broadcast_id, True)
            await query.answer("✅ Все отмечены присутствующими", show_alert=True)
            await show_broadcast_detail(update, context, broadcast_id)
            return

        if callback_data.startswith('unattend_all_'):
            await query.answer()
            broadcast_id = callback_data.replace('unattend_all_', '')
            conn = get_connection()
            cur = conn.cursor()
            cur.execute('SELECT user_id FROM users')
            users = cur.fetchall()
            conn.close()
            for (uid,) in users:
                update_user_attendance(uid, broadcast_id, False)
            await query.answer("✅ Отметки сброшены у всех", show_alert=True)
            await show_broadcast_detail(update, context, broadcast_id)
            return

        if callback_data.startswith('enter_numbers_'):
            await query.answer()
            broadcast_id = callback_data.replace('enter_numbers_', '')
            await enter_attendance_numbers(update, context, broadcast_id)
            return

        if callback_data == 'delete_all_broadcasts':
            await query.answer()
            await delete_all_broadcasts(update, context)
            return

        if callback_data == 'confirm_delete_all':
            await query.answer()
            await confirm_delete_all(update, context)
            return

    # -------------------- КНОПКИ СТАТИСТИКИ (доступны админам) --------------------
    if callback_data.startswith('refresh_stats_'):
        await query.answer()
        broadcast_id = callback_data.replace('refresh_stats_', '')
        stats_text = get_formatted_stats(broadcast_id)
        try:
            await query.edit_message_text(stats_text, reply_markup=get_stats_keyboard(broadcast_id))
            await query.answer("✅ Статистика обновлена!")
        except Exception as e:
            if "Message is not modified" in str(e):
                await query.answer("📊 Статистика актуальна")
            else:
                logger.error(f"Error refreshing stats: {e}")
        return

    if callback_data.startswith('copy_id_'):
        broadcast_id = callback_data.replace('copy_id_', '')
        await query.answer(f"ID скопирован: {broadcast_id}", show_alert=True)
        return

    if callback_data.startswith('ignored_list_'):
        await query.answer()
        broadcast_id = callback_data.replace('ignored_list_', '')
        await show_ignored_list(update, context, broadcast_id)
        return

    if callback_data.startswith('back_to_stats_'):
        await query.answer()
        broadcast_id = callback_data.replace('back_to_stats_', '')
        stats_text = get_formatted_stats(broadcast_id)
        await query.edit_message_text(stats_text, reply_markup=get_stats_keyboard(broadcast_id))
        return

    if callback_data.startswith('delete_broadcast_'):
        await query.answer()
        broadcast_id = callback_data.replace('delete_broadcast_', '')
        await delete_broadcast(update, context, broadcast_id)
        return

    if callback_data.startswith('confirm_delete_'):
        await query.answer()
        broadcast_id = callback_data.replace('confirm_delete_', '')
        await confirm_delete_broadcast(update, context, broadcast_id)
        return

    if callback_data == 'close_stats':
        await query.answer()
        await query.delete_message()
        return

    # -------------------- ПОДТВЕРЖДЕНИЕ РАССЫЛКИ (старый метод) --------------------
    if callback_data == 'confirm_broadcast':
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Нет доступа!", show_alert=True)
            return
        await query.answer()
        broadcast_text = context.user_data.get('broadcast_text')
        if not broadcast_text:
            await query.edit_message_text("❌ Ошибка: текст не найден")
            return
        broadcast_id = str(uuid.uuid4())[:8]
        keyboard = [
            [InlineKeyboardButton("✅", callback_data=f'going_{broadcast_id}'),
             InlineKeyboardButton("❌", callback_data=f'not_going_{broadcast_id}')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        users = get_all_users()
        if not users:
            await query.edit_message_text("❌ В базе нет верифицированных пользователей.")
            return
        await query.edit_message_text(f"📢 Начинаю рассылку для {len(users)} пользователей...")
        successful = 0
        failed = 0
        safe_text = escape_markdown_v2(broadcast_text)
        for uid in users:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"📢 **НОВАЯ РАССЫЛКА КЛАНА**\n\n{safe_text}\n\nВыбери свой вариант:",
                    reply_markup=reply_markup,
                    parse_mode='MarkdownV2'
                )
                successful += 1
            except Exception as e:
                logger.error(f"Failed to send to {uid}: {e}")
                failed += 1
        save_broadcast_text(broadcast_id, broadcast_text)
        stats_text = get_formatted_stats(broadcast_id)
        stats_message = await context.bot.send_message(
            chat_id=user.id,
            text=stats_text,
            reply_markup=get_stats_keyboard(broadcast_id)
        )
        save_stats_message(broadcast_id, user.id, stats_message.message_id)
        context.user_data.pop('broadcast_text', None)
        await context.bot.send_message(
            chat_id=user.id,
            text=f"✅ Рассылка завершена. Успешно: {successful}, Ошибок: {failed}\n\n👑 Админ-панель:",
            reply_markup=get_admin_keyboard()
        )
        return

    if callback_data == 'cancel_broadcast':
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Нет доступа!", show_alert=True)
            return
        await query.answer()
        context.user_data.pop('broadcast_text', None)
        await query.edit_message_text(
            "❌ Рассылка отменена.\n\n👑 Админ-панель:",
            reply_markup=get_admin_keyboard()
        )
        return

    # -------------------- ВЕРИФИКАЦИЯ --------------------
    if callback_data == 'start_verify':
        if update.effective_chat.type != "private":
            await query.answer("Эту команду нужно использовать в личных сообщениях со мной!", show_alert=True)
            return
        if is_user_verified(user.id):
            await query.answer()
            await query.edit_message_text("✅ Ты уже верифицирован!")
            return
        try:
            member = await context.bot.get_chat_member(chat_id=CLAN_CHAT_ID, user_id=user.id)
            if member.status not in (ChatMember.OWNER, ChatMember.ADMINISTRATOR, ChatMember.MEMBER):
                await query.answer()
                await query.edit_message_text("❌ Ты не состоишь в чате клана!")
                return
        except Exception as e:
            logger.error(f"Error checking chat membership: {e}")
            await query.answer()
            await query.edit_message_text("❌ Ошибка проверки. Попробуй позже.")
            return
        await query.answer()
        await query.edit_message_text(
            "🎮 Отлично! Ты в клане.\n\n"
            "Напиши свой **ник в игре** (как тебя зовут в клане):"
        )
        context.user_data['awaiting_nickname'] = True
        return

    # -------------------- ГОЛОСОВАНИЕ --------------------
    if '_' not in callback_data:
        await query.answer()
        try:
            await query.edit_message_text(
                "❌ Это сообщение устарело. Пожалуйста, дождись новой рассылки.",
                reply_markup=InlineKeyboardMarkup([])
            )
        except:
            pass
        return

    try:
        last_underscore = callback_data.rfind('_')
        if last_underscore == -1:
            raise ValueError("No underscore found")
        action = callback_data[:last_underscore]
        broadcast_id = callback_data[last_underscore + 1:]
    except Exception as e:
        logger.error(f"Error parsing callback data {callback_data}: {e}")
        await query.answer()
        try:
            await query.edit_message_text("❌ Ошибка обработки кнопки.", reply_markup=InlineKeyboardMarkup([]))
        except:
            pass
        return

    action = action.strip()
    if action not in ['going', 'not_going']:
        logger.warning(f"Unknown action: '{action}' from user {user.id}")
        await query.answer()
        try:
            await query.edit_message_text("❌ Неизвестное действие", reply_markup=InlineKeyboardMarkup([]))
        except:
            pass
        return

    broadcast_text = get_broadcast_text(broadcast_id)
    if broadcast_text is None:
        await query.answer(
            text="❌ Эта рассылка была удалена администратором.",
            show_alert=True
        )
        await query.edit_message_text(
            text="❌ **Рассылка удалена**\n\nЭто событие было отменено администратором.",
            reply_markup=InlineKeyboardMarkup([])
        )
        return

    event_time = get_broadcast_event_time(broadcast_id)
    if event_time:
        try:
            event_dt = datetime.fromisoformat(event_time)
            if event_dt <= datetime.now():
                await query.answer(
                    text="❌ Время события уже истекло! Голосование закрыто.",
                    show_alert=True
                )
                await query.edit_message_text(
                    text=f"📢 {broadcast_text}\n\n⏰ Время события истекло!\nГолосование закрыто.",
                    reply_markup=InlineKeyboardMarkup([])
                )
                return
        except:
            pass

    previous_vote = get_user_vote(user.id, broadcast_id)
    cooldown = get_broadcast_cooldown(broadcast_id)

    if previous_vote and previous_vote != action:
        can_change, remaining = can_change_vote(user.id, broadcast_id, cooldown)
        if not can_change:
            if remaining % 10 == 1 and remaining % 100 != 11:
                minutes_text = "минуту"
            elif 2 <= remaining % 10 <= 4 and not (12 <= remaining % 100 <= 14):
                minutes_text = "минуты"
            else:
                minutes_text = "минут"
            await query.answer(
                text=f"⏳ Подожди ещё {remaining} {minutes_text} перед сменой голоса",
                show_alert=True
            )
            return

    save_vote(user.id, broadcast_id, action)

    new_stats = get_vote_stats(broadcast_id)

    choice_text = "✅" if action == 'going' else "❌"
    if previous_vote:
        old_choice = "✅" if previous_vote == 'going' else "❌"
        user_text = f"✅ Ты изменил решение!\nБыло: {old_choice}\nСтало: {choice_text}"
    else:
        user_text = f"✅ Твой выбор: {choice_text}"

    if cooldown > 0:
        user_text += f"\n\n⏱️ Менять голос можно раз в {cooldown} мин."

    if event_time:
        try:
            event_dt = datetime.fromisoformat(event_time)
            if event_dt > datetime.now():
                if action == 'going':
                    kb = [[InlineKeyboardButton("❌", callback_data=f'not_going_{broadcast_id}')]]
                else:
                    kb = [[InlineKeyboardButton("✅", callback_data=f'going_{broadcast_id}')]]
                reply_markup = InlineKeyboardMarkup(kb)
                user_text += "\n\n🔄 Нажми на другую кнопку, чтобы изменить решение."
            else:
                reply_markup = InlineKeyboardMarkup([])
        except:
            reply_markup = InlineKeyboardMarkup([])
    else:
        if action == 'going':
            kb = [[InlineKeyboardButton("❌", callback_data=f'not_going_{broadcast_id}')]]
        else:
            kb = [[InlineKeyboardButton("✅", callback_data=f'going_{broadcast_id}')]]
        reply_markup = InlineKeyboardMarkup(kb)
        user_text += "\n\n🔄 Нажми на другую кнопку, чтобы изменить решение."

    new_text = f"📢 {broadcast_text}\n\n{user_text}"

    try:
        await query.edit_message_text(text=new_text, reply_markup=reply_markup)
        await query.answer()
    except Exception as e:
        if "Message is not modified" in str(e):
            logger.info(f"Message not modified for user {user.id}")
        else:
            logger.error(f"Error editing message: {e}")

    logger.info(f"✅ Статистика изменилась! Новые значения: {new_stats}")
    for admin in ADMIN_IDS:
        try:
            stats_msg_id = get_stats_message(broadcast_id)
            if stats_msg_id:
                new_stats_text = get_formatted_stats(broadcast_id)
                await context.bot.edit_message_text(
                    chat_id=admin,
                    message_id=stats_msg_id,
                    text=new_stats_text,
                    reply_markup=get_stats_keyboard(broadcast_id)
                )
                logger.info(f"Stats updated for broadcast {broadcast_id}")
            else:
                new_stats_text = get_formatted_stats(broadcast_id)
                stats_message = await context.bot.send_message(
                    chat_id=admin,
                    text=new_stats_text
                )
                save_stats_message(broadcast_id, admin, stats_message.message_id)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Error updating stats: {e}")

# ========================== ОБРАБОТЧИКИ ТЕКСТОВЫХ СООБЩЕНИЙ ==========================
async def handle_nickname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.user_data.get('awaiting_nickname'):
        return False
    nickname = update.message.text.strip()
    if len(nickname) < 2 or len(nickname) > 30:
        await update.message.reply_text("❌ Ник должен быть от 2 до 30 символов. Попробуй еще раз:")
        return True
    try:
        member = await context.bot.get_chat_member(chat_id=CLAN_CHAT_ID, user_id=user.id)
        if member.status not in (ChatMember.OWNER, ChatMember.ADMINISTRATOR, ChatMember.MEMBER):
            await update.message.reply_text("❌ Ты не состоишь в чате клана!")
            context.user_data['awaiting_nickname'] = False
            return True
    except Exception as e:
        logger.error(f"Error checking chat membership: {e}")
        await update.message.reply_text("❌ Ошибка проверки. Попробуй позже.")
        context.user_data['awaiting_nickname'] = False
        return True
    add_user(user.id, user.username, user.first_name, nickname)
    context.user_data['awaiting_nickname'] = False
    await update.message.reply_text(
        f"✅ Верификация успешна!\n\n"
        f"Твой ник в игре: **{nickname}**\n"
        f"Теперь ты будешь получать все важные объявления клана."
    )
    for admin in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin,
                f"✅ Новый верифицированный:\n"
                f"👤 {user.first_name} (@{user.username})\n"
                f"🎮 Ник: {nickname}"
            )
        except:
            pass
    return True

async def handle_broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("=== HANDLE BROADCAST TEXT ===")
    user = update.effective_user

    if context.user_data.get('awaiting_broadcast_fast'):
        if user.id not in ADMIN_IDS:
            context.user_data.pop('awaiting_broadcast_fast', None)
            return False
        text = update.message.text.strip()
        if text.lower() == '/cancel':
            context.user_data.pop('awaiting_broadcast_fast', None)
            await update.message.reply_text("❌ Создание рассылки отменено.", reply_markup=get_admin_keyboard())
            return True
        if len(text) < 2:
            await update.message.reply_text("❌ Текст слишком короткий. Попробуй еще раз или отправь /cancel")
            return True
        broadcast_id = str(uuid.uuid4())[:8]
        save_broadcast_with_params(broadcast_id, text, 0, None)
        kb = [[InlineKeyboardButton("✅", callback_data=f'going_{broadcast_id}'),
               InlineKeyboardButton("❌", callback_data=f'not_going_{broadcast_id}')]]
        markup = InlineKeyboardMarkup(kb)
        users = get_all_users()
        if not users:
            await update.message.reply_text("❌ В базе нет верифицированных пользователей.")
            return True
        await update.message.reply_text(f"📢 Начинаю рассылку для {len(users)} пользователей...")
        successful = 0
        failed = 0
        safe_text = escape_markdown_v2(text)
        for uid in users:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"📢 **НОВАЯ РАССЫЛКА КЛАНА**\n\n{safe_text}\n\nВыбери свой вариант:",
                    reply_markup=markup,
                    parse_mode='MarkdownV2'
                )
                successful += 1
            except Exception as e:
                logger.error(f"Failed to send to {uid}: {e}")
                failed += 1
        stats_text = get_formatted_stats(broadcast_id)
        stats_msg = await context.bot.send_message(
            chat_id=user.id,
            text=stats_text,
            reply_markup=get_stats_keyboard(broadcast_id)
        )
        save_stats_message(broadcast_id, user.id, stats_msg.message_id)
        context.user_data.pop('awaiting_broadcast_fast', None)
        await update.message.reply_text(
            f"✅ Рассылка завершена. Успешно: {successful}, Ошибок: {failed}",
            reply_markup=get_admin_keyboard()
        )
        return True

    if not context.user_data.get('awaiting_broadcast'):
        return False
    if user.id not in ADMIN_IDS:
        context.user_data['awaiting_broadcast'] = False
        await update.message.reply_text("❌ У тебя нет прав на рассылку.")
        return True
    text = update.message.text.strip()
    if text.lower() == '/cancel':
        context.user_data['awaiting_broadcast'] = False
        await update.message.reply_text("❌ Создание рассылки отменено.", reply_markup=get_admin_keyboard())
        return True
    if len(text) < 2:
        await update.message.reply_text("❌ Текст слишком короткий. Попробуй еще раз или отправь /cancel")
        return True
    context.user_data['broadcast_text'] = text
    context.user_data['awaiting_broadcast'] = False
    kb = [[InlineKeyboardButton("✅ Отправить", callback_data='confirm_broadcast'),
           InlineKeyboardButton("❌ Отмена", callback_data='cancel_broadcast')]]
    await update.message.reply_text(
        f"📢 **Подтверждение рассылки**\n\n"
        f"Текст:\n```\n{text}\n```\n\n"
        f"Отправить всем верифицированным пользователям?",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode='MarkdownV2'
    )
    return True

async def handle_all_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("=== HANDLE ALL TEXT ===")
    logger.info(f"User data keys: {list(context.user_data.keys())}")

    user = update.effective_user

    if context.user_data.get('awaiting_nickname_change'):
        if await handle_nickname_change(update, context):
            return

    if user.id in ADMIN_IDS:
        step = context.user_data.get('broadcast_step')
        if step == 1:
            text = update.message.text.strip()
            if text.lower() == '/cancel':
                context.user_data.pop('broadcast_step', None)
                await update.message.reply_text("❌ Создание рассылки отменено.", reply_markup=get_admin_keyboard())
                return
            context.user_data['broadcast_text'] = text
            context.user_data['broadcast_step'] = 2
            await update.message.reply_text(
                "✅ Текст сохранён!\n\n"
                "Шаг 2/3: Укажи время начала события.\n"
                "Форматы:\n"
                "• 0 - без времени (просто рассылка)\n"
                "• 20:00 - сегодня в 20:00\n"
                "• 15.03.2024 18:30 - конкретная дата\n"
                "• +2 - через 2 часа\n\n"
                "❌ /cancel - отмена"
            )
            return

        if step == 2:
            time_input = update.message.text.strip()
            if time_input.lower() == '/cancel':
                context.user_data.pop('broadcast_step', None)
                context.user_data.pop('broadcast_text', None)
                await update.message.reply_text("❌ Создание рассылки отменено.", reply_markup=get_admin_keyboard())
                return
            event_time = parse_event_time(time_input)
            if event_time is False:
                await update.message.reply_text(
                    "❌ Неверный формат времени. Попробуй ещё раз:\n\n"
                    "• 0 - без времени\n"
                    "• 20:00 - сегодня в 20:00\n"
                    "• 15.03.2024 18:30 - дата и время\n"
                    "• +2 - через 2 часа"
                )
                return
            context.user_data['event_time'] = event_time
            context.user_data['broadcast_step'] = 3
            await update.message.reply_text(
                "✅ Время сохранено!\n\n"
                "Шаг 3/3: Укажи кулдаун смены голоса (в минутах).\n"
                "• 0 - без ограничений\n"
                "• 5 - можно менять раз в 5 минут\n"
                "• 30 - раз в полчаса\n"
                "• 60 - раз в час\n\n"
                "❌ /cancel - отмена"
            )
            return

        if step == 3:
            cooldown_input = update.message.text.strip()
            if cooldown_input.lower() == '/cancel':
                context.user_data.pop('broadcast_step', None)
                context.user_data.pop('broadcast_text', None)
                context.user_data.pop('event_time', None)
                await update.message.reply_text("❌ Создание рассылки отменено.", reply_markup=get_admin_keyboard())
                return
            try:
                cooldown = int(cooldown_input)
                if cooldown < 0:
                    raise ValueError
            except:
                await update.message.reply_text("❌ Введи число (0 или больше):")
                return

            broadcast_text = context.user_data['broadcast_text']
            event_time = context.user_data['event_time']
            broadcast_id = str(uuid.uuid4())[:8]

            save_broadcast_with_params(broadcast_id, broadcast_text, cooldown, event_time)

            kb = [[InlineKeyboardButton("✅", callback_data=f'going_{broadcast_id}'),
                   InlineKeyboardButton("❌", callback_data=f'not_going_{broadcast_id}')]]
            markup = InlineKeyboardMarkup(kb)

            users = get_all_users()
            if not users:
                await update.message.reply_text("❌ В базе нет верифицированных пользователей.")
                return

            await update.message.reply_text(f"📢 Начинаю рассылку для {len(users)} пользователей...")

            successful = 0
            failed = 0
            safe_text = escape_markdown_v2(broadcast_text)
            for uid in users:
                try:
                    event_text = ""
                    if event_time:
                        try:
                            dt = datetime.fromisoformat(event_time)
                            event_text = f"\n🕒 Время события: {dt.strftime('%d.%m.%Y %H:%M')}"
                        except:
                            pass
                    await context.bot.send_message(
                        chat_id=uid,
                        text=f"📢 **НОВАЯ РАССЫЛКА КЛАНА**{event_text}\n\n{safe_text}\n\nВыбери свой вариант:",
                        reply_markup=markup,
                        parse_mode='MarkdownV2'
                    )
                    successful += 1
                except Exception as e:
                    logger.error(f"Failed to send to {uid}: {e}")
                    failed += 1

            stats_text = get_formatted_stats(broadcast_id)
            stats_msg = await context.bot.send_message(
                chat_id=user.id,
                text=stats_text,
                reply_markup=get_stats_keyboard(broadcast_id)
            )
            save_stats_message(broadcast_id, user.id, stats_msg.message_id)

            context.user_data.pop('broadcast_step', None)
            context.user_data.pop('broadcast_text', None)
            context.user_data.pop('event_time', None)

            await update.message.reply_text(
                f"✅ Рассылка завершена. Успешно: {successful}, Ошибок: {failed}",
                reply_markup=get_admin_keyboard()
            )
            return

    if await handle_attendance_numbers(update, context):
        logger.info("Handled by attendance_numbers")
        return
    if await handle_nickname(update, context):
        logger.info("Handled by nickname")
        return
    if await handle_broadcast_text(update, context):
        logger.info("Handled by broadcast_text")
        return

    logger.info("No handler processed the message")

# ---------- НОВЫЕ ФУНКЦИИ ДЛЯ ПРОФИЛЯ ----------
async def my_broadcasts_list(update: Update, context: ContextTypes.DEFAULT_TYPE, page=1):
    query = update.callback_query
    user_id = query.from_user.id

    broadcasts = get_user_broadcasts(user_id)
    if not broadcasts:
        await query.answer("📭 Ты ещё не участвовал ни в одной рассылке.", show_alert=True)
        return

    per_page = 5
    total = len(broadcasts)
    total_pages = (total - 1) // per_page + 1
    if page < 1 or page > total_pages:
        page = 1

    text = f"📋 **Мои рассылки** (стр. {page}/{total_pages})\n\n"
    start = (page - 1) * per_page
    for i, bid in enumerate(broadcasts[start:start+per_page], start=start+1):
        info = get_broadcast_info(bid)
        if info:
            preview = info['text'][:30] + "..." if info['text'] and len(info['text']) > 30 else (info['text'] or "Нет текста")
            date_str = info['created_at'][:16] if info['created_at'] else "неизвестно"
            text += f"{i}. `{bid}`\n   📅 {date_str}\n   📝 {preview}\n\n"
        else:
            text += f"{i}. `{bid}`\n   (информация недоступна)\n\n"

    keyboard = get_my_broadcasts_keyboard(broadcasts, page, total_pages)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='MarkdownV2')
    await query.answer()

async def my_broadcast_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
    query = update.callback_query
    user_id = query.from_user.id

    info = get_broadcast_info(broadcast_id)
    if not info:
        await query.answer("❌ Рассылка не найдена.", show_alert=True)
        return

    choice, attended = get_user_choice_and_attendance(user_id, broadcast_id)
    choice_text = {
        'going': '✅ Пойду',
        'not_going': '❌ Не пойду',
        None: '❓ Не голосовал'
    }.get(choice, '❓ Не голосовал')
    attended_text = "✅ Был отмечен" if attended else "❌ Не отмечен"

    safe_text = escape_markdown_v2(info['text'])

    stats = get_vote_stats(broadcast_id)
    total_votes = stats['going'] + stats['not_going']

    text = f"📢 **{safe_text}**\n"
    text += f"🆔 `{broadcast_id}`\n"
    if info['created_at']:
        text += f"📅 Создана: {info['created_at'][:16]}\n"
    if info['event_time']:
        try:
            dt = datetime.fromisoformat(info['event_time'])
            text += f"🕒 Время события: {dt.strftime('%d.%m.%Y %H:%M')}\n"
        except:
            safe_event_time = escape_markdown_v2(info['event_time'])
            text += f"🕒 Время события: {safe_event_time}\n"
    text += f"\n**Твой выбор:** {choice_text}\n"
    text += f"**Твоя отметка:** {attended_text}\n"
    text += f"\n📊 Всего проголосовало: {total_votes}"

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("◀️ Назад к списку", callback_data='my_broadcasts')
    ]])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='MarkdownV2')
    await query.answer()

async def change_nickname_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id

    can, remaining = can_change_nickname(user_id)
    if not can:
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        time_str = f"{hours}ч {minutes}м" if hours else f"{minutes}м"
        await query.answer(f"⏳ Сменить ник можно будет через {time_str}.", show_alert=True)
        return

    await query.edit_message_text(
        "✏️ Введи новый ник (от 2 до 30 символов) или отправь /cancel для отмены."
    )
    context.user_data['awaiting_nickname_change'] = True
    await query.answer()

async def handle_nickname_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not context.user_data.get('awaiting_nickname_change'):
        return False

    new_nick = update.message.text.strip()
    if new_nick.lower() == '/cancel':
        context.user_data.pop('awaiting_nickname_change', None)
        await update.message.reply_text("❌ Смена ника отменена.")
        await me_command(update, context)
        return True

    if len(new_nick) < 2 or len(new_nick) > 30:
        await update.message.reply_text("❌ Ник должен быть от 2 до 30 символов. Попробуй ещё раз:")
        return True

    can, _ = can_change_nickname(user.id)
    if not can:
        await update.message.reply_text("❌ Ты уже менял ник недавно. Подожди 24 часа.")
        context.user_data.pop('awaiting_nickname_change', None)
        return True

    update_user_nickname(user.id, new_nick)
    set_last_nickname_change(user.id, datetime.now().isoformat())
    context.user_data.pop('awaiting_nickname_change', None)

    safe_new_nick = escape_markdown_v2(new_nick)
    await update.message.reply_text(f"✅ Ник успешно изменён на **{safe_new_nick}**!", parse_mode='MarkdownV2')
    await me_command(update, context)
    return True

# ========================== ЗАПУСК БОТА ==========================
def main():
    init_db()
    recalc_all_stats()
    application = Application.builder().token(TOKEN).build()

    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(check_reminders, interval=60, first=10)
        job_queue.run_repeating(check_expired_events, interval=60, first=20)
    else:
        logger.warning("Job queue not available – reminders and expired events disabled")

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("verify", verify))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("me", me_command))

    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, track_chat_members))

    application.add_handler(CallbackQueryHandler(button_callback))

    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_all_text
    ))

    print("Бот запущен...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
