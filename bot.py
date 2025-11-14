import os
import shutil
import sqlite3
import asyncio
import random
from datetime import datetime, timedelta, timezone

import libsql  # для Turso / libSQL

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatMemberStatus
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    FSInputFile
)
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter, TelegramBadRequest

# === БАЗОВЫЕ НАСТРОЙКИ/ПУТИ ДЛЯ ЛОКАЛЬНОГО И СЕРВЕРА ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# На Render ОБЯЗАТЕЛЬНО создаём переменную окружения API_TOKEN со значением токена.
API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("API_TOKEN is not set. Please configure it in environment variables (Render → Environment).")

# Путь к базе: рядом с файлом бота. Для Render диск может быть временный,
# но если включён Turso, это будет локальная реплика удалённой БД.
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "users.db"))

# Turso / libSQL (если заданы переменные — используем удалённую БД)
TURSO_URL = os.getenv("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN")

QUIET_LOGGING = True

if TURSO_URL and TURSO_AUTH_TOKEN:
    # Подключаемся к Turso как к embedded-реплике
    # Документация: https://docs.turso.tech/sdk/python/quickstart
    conn = libsql.connect(DB_PATH, sync_url=TURSO_URL, auth_token=TURSO_AUTH_TOKEN)
    try:
        # Подтягиваем данные из удалённой БД (если уже есть)
        conn.sync()
    except Exception:
        if not QUIET_LOGGING:
            print("WARN: Turso sync() failed on startup")
else:
    # Старый вариант — обычная локальная SQLite
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

cursor = conn.cursor()

# канал для уведомлений админа
NEW_ADMIN_CHANNEL = os.getenv("NEW_ADMIN_CHANNEL", "sdafsadfsdaf13")

# пути к картинкам (относительно проекта)
PROFILE_IMG_PATH = os.path.join(BASE_DIR, "images", "profile.png.png")
EARNINGS_IMG_PATH = os.path.join(BASE_DIR, "images", "earnings.png.png")
WITHDRAW_IMG_PATH = os.path.join(BASE_DIR, "images", "withdraw.png.png")
RATING_IMG_PATH = os.path.join(BASE_DIR, "images", "rating.png.png")

SPONSORS = [
    "@WhiteStarXInfo",
]
CHANNELS_TO_CHECK = SPONSORS

BOT_USERNAME = "WhiteStarXBot"

REFERRAL_REWARD = 4
REFERRAL_BONUS_EVERY = 10
REFERRAL_BONUS_AMOUNT = 10

YOUNG_ACCOUNT_THRESHOLD = 7_500_000_000

CHANNEL_FOR_WITHDRAW = -1003003114178
INSTRUCTION_LINK = "https://t.me/+JIE3W3PVNYdjYjM6"

# пароль можно тоже вынести в окружение, но оставим дефолт
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "jikolpkolp")

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    subscribed INTEGER DEFAULT 0,
    first_time INTEGER DEFAULT 1,
    balance REAL DEFAULT 0,
    referrals_count INTEGER DEFAULT 0,
    total_earned REAL DEFAULT 0,
    referrer_id INTEGER DEFAULT 0,
    referral_link TEXT,
    created_at TEXT,
    blocked INTEGER DEFAULT 0
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS referral_rewards (
    referrer_id INTEGER,
    referred_id INTEGER,
    rewarded INTEGER DEFAULT 0,
    rewarded_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(referrer_id, referred_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    to_username TEXT,
    status TEXT DEFAULT 'pending',
    created_at TEXT,
    user_msg_id INTEGER
)
""")

# Логи рассылок (оставил — может пригодиться дальше)
cursor.execute("""
CREATE TABLE IF NOT EXISTS broadcast_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT,
    finished_at TEXT,
    total INTEGER,
    sent INTEGER,
    forbidden INTEGER,
    failed INTEGER,
    sample_chat_id INTEGER,
    sample_message_id INTEGER
)
""")

conn.commit()

user_states = {}
admin_sessions = set()
admin_login_states = set()
last_rating_click = {}
admin_actions = {}

START_DATE = datetime(2025, 8, 28)
BASE_USERS = 3752
BASE_WITHDRAWN = 69835
_stats_cache_date = None
_stats_cache_users = BASE_USERS
_stats_cache_withdrawn = BASE_WITHDRAWN

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ---------------------- ВСПОМОГАТЕЛЬНЫЕ ----------------------

def _qwarn(msg: str):
    if not QUIET_LOGGING:
        print(msg)

async def safe_send_message(chat_id: int, text: str, **kwargs):
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramForbiddenError:
        return None
    except Exception as e:
        _qwarn(f"[WARN] send_message failed: {type(e).__name__}")
        return None

async def safe_answer_message(message: types.Message, text: str, **kwargs):
    try:
        return await message.answer(text, **kwargs)
    except TelegramForbiddenError:
        return None
    except Exception as e:
        _qwarn(f"[WARN] message.answer failed: {type(e).__name__}")
        return None

async def safe_edit_text(message: types.Message, new_text: str, **kwargs):
    try:
        return await message.edit_text(new_text, **kwargs)
    except TelegramForbiddenError:
        return None
    except Exception as e:
        _qwarn(f"[WARN] edit_text failed: {type(e).__name__}")
        return None

async def send_photo_caption(chat_id: int, image_path: str, caption: str, reply_markup=None, parse_mode="HTML"):
    try:
        if image_path and os.path.exists(image_path):
            photo = FSInputFile(image_path)
            return await bot.send_photo(
                chat_id, photo=photo, caption=caption,
                reply_markup=reply_markup, parse_mode=parse_mode
            )
        else:
            return await safe_send_message(chat_id, caption, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramForbiddenError:
        return None
    except Exception as e:
        _qwarn(f"[WARN] send_photo_caption failed ({os.path.basename(image_path) if image_path else 'no_image'}): {type(e).__name__}")
        return await safe_send_message(chat_id, caption, reply_markup=reply_markup, parse_mode=parse_mode)

def normalize_chat_target(target):
    if isinstance(target, int):
        return target
    s = str(target or "").strip()
    if s.startswith("-") and s[1:].isdigit():
        try:
            return int(s)
        except Exception:
            pass
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            pass
    if s.startswith("https://t.me/") or s.startswith("http://t.me/") or s.startswith("t.me/"):
        alias = s.split("/", maxsplit=1)[-1].strip()
        return alias if alias.startswith("@") else ("@" + alias if alias else s)
    return s if s.startswith("@") else "@" + s

async def notify_admin_channel(text: str):
    chat = normalize_chat_target(NEW_ADMIN_CHANNEL)
    try:
        await bot.send_message(chat, text, parse_mode="HTML")
    except Exception as e:
        _qwarn(f"[WARN] notify_admin_channel failed: {type(e).__name__}")

async def resolve_username_display(user_id: int) -> str:
    try:
        cursor.execute("SELECT username FROM users WHERE user_id=?", (user_id,))
        row = cursor.fetchone()
        if row and row[0]:
            return f"@{row[0]}"
    except Exception:
        pass
    try:
        chat = await bot.get_chat(user_id)
        if getattr(chat, "username", None):
            return f"@{chat.username}"
        name = " ".join([x for x in [getattr(chat, "first_name", None), getattr(chat, "last_name", None)] if x])
        return name or "—"
    except Exception:
        return "—"

# ====== РАССЫЛКА: надёжная реализация с троттлингом и 429 ======
async def do_broadcast(admin_id: int, sample_chat_id: int, sample_message_id: int):
    cursor.execute("SELECT user_id FROM users WHERE blocked=0")
    rows = cursor.fetchall()
    user_ids = [r[0] for r in rows if r and r[0]]

    total = len(user_ids)
    sent = 0
    forb = 0
    failed = 0

    # создаём запись лога заранее
    cursor.execute("""
        INSERT INTO broadcast_logs(started_at, finished_at, total, sent, forbidden, failed, sample_chat_id, sample_message_id)
        VALUES(?,?,?,?,?,?,?,?)
    """, (now_kyiv().isoformat(), None, total, 0, 0, 0, int(sample_chat_id), int(sample_message_id)))
    # для Turso/SQLite lastrowid одинаково работает
    log_id = cursor.lastrowid
    conn.commit()

    # Шагаем, уважая лимиты
    for i, uid in enumerate(user_ids, start=1):
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=sample_chat_id, message_id=sample_message_id)
            sent += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(getattr(e, "retry_after", 1) + 0.2)
            try:
                await bot.copy_message(chat_id=uid, from_chat_id=sample_chat_id, message_id=sample_message_id)
                sent += 1
            except TelegramForbiddenError:
                forb += 1
                try:
                    cursor.execute("UPDATE users SET blocked=1 WHERE user_id=?", (uid,))
                    conn.commit()
                except Exception:
                    pass
            except Exception:
                failed += 1
        except TelegramForbiddenError:
            forb += 1
            try:
                cursor.execute("UPDATE users SET blocked=1 WHERE user_id=?", (uid,))
                conn.commit()
            except Exception:
                pass
        except TelegramBadRequest:
            failed += 1
        except Exception:
            failed += 1

        if i % 25 == 0:
            await asyncio.sleep(0.3)

    # финальный апдейт лога
    cursor.execute("""
        UPDATE broadcast_logs
        SET finished_at=?, sent=?, forbidden=?, failed=?
        WHERE id=?
    """, (now_kyiv().isoformat(), sent, forb, failed, log_id))
    conn.commit()

    report = (
        "📢 <b>Рассылка завершена</b>\n\n"
        f"Всего получателей: <b>{total}</b>\n"
        f"Отправлено: <b>{sent}</b>\n"
        f"Заблокировали бота: <b>{forb}</b>\n"
        f"Других ошибок: <b>{failed}</b>"
    )
    await safe_send_message(admin_id, report, parse_mode="HTML")

# ----------------------------------------------------------------------------------

def now_kyiv():
    return datetime.now(timezone(timedelta(hours=3)))

def start_of_today_kyiv():
    now = now_kyiv()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)

def gender_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Муж🧑", callback_data="gender_boy"),
         InlineKeyboardButton(text="Жен👩", callback_data="gender_girl")]
    ])

def sponsor_keyboard(miss_channels: list) -> InlineKeyboardMarkup:
    rows = []
    temp_row = []
    for i, ch in enumerate(miss_channels, start=1):
        if isinstance(ch, str) and ch.startswith("http"):
            url = ch
        else:
            display = str(ch).lstrip('@')
            url = f"https://t.me/{display}"
        btn = InlineKeyboardButton(text=f"Канал {i}#", url=url)
        temp_row.append(btn)
        if len(temp_row) == 2:
            rows.append(temp_row)
            temp_row = []
    if temp_row:
        rows.append(temp_row)
    rows.append([InlineKeyboardButton(text="✅Проверить подписку", callback_data="check_subs")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Заработать звезды🌟")],
            [KeyboardButton(text="Профиль 👤"), KeyboardButton(text="Рейтинг 📊")],
            [KeyboardButton(text="Инструкция 📕"), KeyboardButton(text="Информация📚")],
            [KeyboardButton(text="Вывести звезды✨")]
        ],
        resize_keyboard=True
    )
    return kb

def back_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Назад")]], resize_keyboard=True)

def rating_keyboard_single_for(current_timeframe: str) -> InlineKeyboardMarkup:
    if current_timeframe == "24h":
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="За всё время", callback_data="rating_all")]])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="За 24 часа", callback_data="rating_24h")]])

def withdraw_amount_confirm_kb(user_id: int, amount: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтверждаю✅", callback_data=f"confirm_amount:{user_id}:{amount}"),
         InlineKeyboardButton(text="Назад", callback_data="withdraw_back")]
    ])

def withdraw_final_confirm_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подтверждаю✅", callback_data=f"create_withdraw:{user_id}"),
         InlineKeyboardButton(text="Переделать заявку📃", callback_data=f"redo_withdraw_user:{user_id}")]
    ])

def admin_withdraw_kb(withdraw_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Выплачено", callback_data=f"admin_paid:{withdraw_id}"),
            InlineKeyboardButton(text="Отказано", callback_data=f"admin_reject:{withdraw_id}")
        ]
    ])

def admin_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔄 Обнулить пользователя")],
            [KeyboardButton(text="🚫 Заблокировать / Разблокировать")],
            [KeyboardButton(text="💳 Начислить звезды")],
            [KeyboardButton(text="📢 Рассылка")],
            [KeyboardButton(text="📈 Статистика пользователей")],
            [KeyboardButton(text="🚪 Выйти из админки")]
        ],
        resize_keyboard=True
    )

# ---------------------- УТИЛИТЫ АДМИН-ДИАЛОГА ----------------------

def normalize_username(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("@"):
        u = u[1:]
    return u.lower()

def fetch_user_by_username(uname: str):
    uname_norm = normalize_username(uname)
    if not uname_norm:
        return None
    cursor.execute("SELECT user_id, username FROM users WHERE lower(username)=?", (uname_norm,))
    return cursor.fetchone()

def fetch_user_by_id(uid: int):
    cursor.execute("SELECT user_id, username FROM users WHERE user_id=?", (uid,))
    return cursor.fetchone()

def parse_user_ref(text: str):
    t = (text or "").strip()
    if not t:
        return None, None
    if t.startswith("@") or t.isalpha() or (t and not t[0].isdigit()):
        row = fetch_user_by_username(t)
        if row:
            return int(row[0]), row[1]
        return None, None
    try:
        uid = int(t)
    except Exception:
        return None, None
    row = fetch_user_by_id(uid)
    if row:
        return int(row[0]), row[1]
    return None, None

# ---------------------- ДОСТУП И АДМИН-КОМАНДЫ ----------------------

async def is_channel_admin(user_id: int, channel_id) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)
    except Exception:
        return False

async def has_admin_access(user_id: int) -> bool:
    if user_id in admin_sessions:
        return True
    if await is_channel_admin(user_id, CHANNEL_FOR_WITHDRAW):
        return True
    return False

@dp.message(Command("arisadminslipjiko"))
async def cmd_admin_login(message: types.Message):
    admin_login_states.add(message.from_user.id)
    await safe_answer_message(message, "🔑 Введите пароль для входа в админ-панель (отправьте пароль как обычное сообщение).")

@dp.message(Command("exitadmin"))
async def cmd_exit_admin(message: types.Message):
    if message.from_user.id in admin_sessions:
        admin_sessions.discard(message.from_user.id)
        admin_actions.pop(message.from_user.id, None)
        await safe_answer_message(message, "🚪 Вы вышли из админ-панели.", reply_markup=ReplyKeyboardRemove())
        await safe_send_message(message.from_user.id, "🔝 Главное меню", reply_markup=main_menu_keyboard())
    else:
        await safe_answer_message(message, "❌ Вы не в админ-панели.")

# Запасной вход в режим рассылки командой
@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    if not await has_admin_access(message.from_user.id):
        await safe_answer_message(message, "❌ У вас нет доступа. Войдите через /arisadminslipjiko.")
        return
    admin_actions[message.from_user.id] = {"mode": "broadcast", "await": "sample"}
    await safe_answer_message(
        message,
        "📢 Пришлите образец сообщения для рассылки всем пользователям. Затем напишите «да» для подтверждения.\n«отмена» — чтобы выйти.",
        reply_markup=admin_menu_kb()
    )

@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    await safe_answer_message(message, f"🆔 Твой user_id: {message.from_user.id}")

# ---------- НОВАЯ КОМАНДА: /backup_db (отправка базы админу) ----------
@dp.message(Command("backup_db"))
async def cmd_backup_db(message: types.Message):
    """
    Админ-команда: /backup_db
    Отправляет файл базы данных (DB_PATH) как документ.
    Если используется Turso, это локальная реплика удалённой БД.
    """
    if not await has_admin_access(message.from_user.id):
        await safe_answer_message(message, "❌ У вас нет доступа к этой команде.")
        return

    try:
        if not os.path.exists(DB_PATH):
            await safe_answer_message(
                message,
                f"⚠️ Файл базы не найден по пути:\n{DB_PATH}"
            )
            return

        backup_name = "db_backup_for_send.sqlite"
        shutil.copyfile(DB_PATH, backup_name)

        await message.answer_document(
            FSInputFile(backup_name),
            caption="Резервная копия базы данных"
        )
    except Exception as e:
        _qwarn(f"[WARN] backup_db failed: {type(e).__name__}: {e}")
        await safe_answer_message(
            message,
            "⚠️ Не удалось отправить файл базы. Смотри логи на Render."
        )

@dp.message(lambda m: m.from_user.id in admin_login_states)
async def admin_password_handler(message: types.Message):
    user_id = message.from_user.id
    text = (message.text or "").strip()
    admin_login_states.discard(user_id)

    if text == ADMIN_PASSWORD:
        admin_sessions.add(user_id)
        await safe_answer_message(
            message,
            "✅ Доступ разрешён. Вы в админ-панели.\n\n"
            "Доступные действия:\n"
            "🔄 Обнулить пользователя — кнопка или /restartpikslip <user_id>\n"
            "🚫 Заблокировать / Разблокировать — кнопка или /bensplip <user_id>\n"
            "💳 Начислить звезды — кнопка\n"
            "📢 Рассылка — кнопка или /broadcast\n"
            "📈 Статистика пользователей — кнопка\n"
            "🚪 Выйти из админки — кнопка или /exitadmin",
            reply_markup=admin_menu_kb()
        )
    else:
        await safe_answer_message(message, "❌ Неверный пароль. Вход в админ-панель отклонён.")

# ---------------------- /start ----------------------

@dp.message(CommandStart())
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "None"
    join_date = now_kyiv().isoformat()

    cursor.execute("SELECT blocked FROM users WHERE user_id=?", (user_id,))
    row_block = cursor.fetchone()
    if row_block and row_block[0] == 1:
        await safe_answer_message(message, "🚫 Вы заблокированы администратором.")
        return

    referrer_id = 0
    if message.text and len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
        except Exception:
            referrer_id = 0
    if referrer_id == user_id:
        referrer_id = 0

    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = cursor.fetchone()

    bot_username = BOT_USERNAME

    if not row:
        referral_link = f"https://t.me/{bot_username}?start={user_id}"
        cursor.execute(
            "INSERT INTO users(user_id, username, subscribed, first_time, balance, referrals_count, total_earned, referrer_id, referral_link, created_at, blocked) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (user_id, username, 0, 1, 0, 0, 0, referrer_id, referral_link, join_date, 0)
        )
        conn.commit()

        if referrer_id == 0:
            try:
                joined_disp = await resolve_username_display(user_id)
                admin_text = (
                    "🆕 <b>Новый вход</b>\n"
                    f"👤 Вошёл: {joined_disp} (ID: <code>{user_id}</code>)\n"
                    f"🤝 Пригласил: @— (ID: <code>—</code>)\n"
                    f"🕒 {join_date}"
                )
                await notify_admin_channel(admin_text)
            except Exception:
                pass

        await safe_answer_message(message, "Выберите пол:", reply_markup=gender_keyboard())
        return

    referral_link = row[8] if row and row[8] else f"https://t.me/{bot_username}?start={user_id}"
    cursor.execute("UPDATE users SET username=?, referral_link=? WHERE user_id=?", (username, referral_link, user_id))
    conn.commit()

    miss = await missing_channels(user_id)
    if not miss:
        cursor.execute("UPDATE users SET subscribed=1, first_time=0 WHERE user_id=?", (user_id,))
        conn.commit()
        await safe_answer_message(message, "🔝 Главное меню", reply_markup=main_menu_keyboard())
    else:
        cursor.execute("UPDATE users SET subscribed=0, first_time=0 WHERE user_id=?", (user_id,))
        conn.commit()
        await safe_answer_message(
            message,
            "Уважаемый пользователь, к сожалению, вы не подписаны на спонсоров😢, попробуйте снова",
            reply_markup=sponsor_keyboard(miss)
        )

# ---------------------- Подписки / реферал ----------------------

@dp.callback_query(lambda c: c.data and c.data.startswith("gender_"))
async def gender_chosen(callback: types.CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    miss = await missing_channels(callback.from_user.id)
    await safe_answer_message(callback.message, "Подпишись на спонсоров:", reply_markup=sponsor_keyboard(miss))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "check_subs")
async def check_subscriptions(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username or "None"

    cursor.execute("SELECT blocked FROM users WHERE user_id=?", (user_id,))
    b = cursor.fetchone()
    if b and b[0] == 1:
        await callback.answer("Вы заблокированы администратором.", show_alert=True)
        return

    cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row_user = cursor.fetchone()
    if not row_user:
        await callback.answer()
        return

    miss = await missing_channels(user_id)

    if await is_subscribed_all(user_id):
        cursor.execute("UPDATE users SET subscribed=1 WHERE user_id=?", (user_id,))
        conn.commit()
        try:
            await callback.message.delete()
        except Exception:
            pass

        await safe_send_message(user_id, "⭐️")
        await safe_send_message(user_id, "✅ Спасибо за подписку! Мы рады, что вы выбрали именно нас!", reply_markup=main_menu_keyboard())
        await safe_send_message(
            user_id,
            "<b>🤍Рады приветствовать тебя в нашем боте!\n\n С помощью нашего бота ты сможешь зарабатывать красивые подарки для себя,или же порадовать близких совершенно бесплатно!💫\n\nТвоя задача,просто приглашать друзей по своей реферальной ссылке и лутать звезды,ВСЕ!\n\n👤Скорее жми «Заработать звезды🌟» чтобы заработать звезды</b>",
            parse_mode="HTML"
        )

        referrer_id = row_user[7]
        ref_disp = await resolve_username_display(referrer_id) if referrer_id else "—"
        joined_disp = await resolve_username_display(user_id)

        if referrer_id and referrer_id != user_id:
            cursor.execute("SELECT rewarded FROM referral_rewards WHERE referrer_id=? AND referred_id=?", (referrer_id, user_id))
            reward_row = cursor.fetchone()
            if not reward_row:
                cursor.execute("SELECT user_id FROM users WHERE user_id=?", (referrer_id,))
                ref_exists = cursor.fetchone() is not None

                if ref_exists:
                    cursor.execute("""
                        UPDATE users
                        SET balance = balance + ?, total_earned = total_earned + ?, referrals_count = referrals_count + 1
                        WHERE user_id=?
                    """, (REFERRAL_REWARD, REFERRAL_REWARD, referrer_id))
                    cursor.execute("INSERT INTO referral_rewards(referrer_id, referred_id, rewarded, rewarded_at) VALUES(?,?,1,?)",
                                   (referrer_id, user_id, now_kyiv().isoformat()))
                    conn.commit()

                    cursor.execute("SELECT referrals_count FROM users WHERE user_id=?", (referrer_id,))
                    rref = cursor.fetchone()
                    if rref and rref[0] is not None:
                        ref_count = rref[0]
                        if ref_count % REFERRAL_BONUS_EVERY == 0:
                            cursor.execute("UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id=?",
                                           (REFERRAL_BONUS_AMOUNT, REFERRAL_BONUS_AMOUNT, referrer_id))
                            conn.commit()
                            await safe_send_message(referrer_id, f"🎉 Поздравляем! Вы пригласили {ref_count} новых пользователей!\nВ качестве бонуса начислено {REFERRAL_BONUS_AMOUNT}.0 ⭐️")

                    await safe_send_message(referrer_id, f"📲 Новый пользователь @{username} зарегистрировался по вашей ссылке!\n- Зачислено {REFERRAL_REWARD}.0 ⭐️")

                    await notify_admin_channel(
                        "👥 <b>Реф-подтверждение</b>\n"
                        f"🤝 Пригласил: {ref_disp} (ID: <code>{referrer_id}</code>)\n"
                        f"👤 Вошёл: {joined_disp} (ID: <code>{user_id}</code>)\n"
                        f"🕒 {now_kyiv().isoformat()}"
                    )
                else:
                    cursor.execute("INSERT INTO referral_rewards(referrer_id, referred_id, rewarded, rewarded_at) VALUES(?,?,0,?)",
                                   (referrer_id, user_id, now_kyiv().isoformat()))
                    conn.commit()
                    await notify_admin_channel(
                        "👥 <b>Реф-подтверждение</b>\n"
                        f"🤝 Пригласил: {ref_disp} (ID: <code>{referrer_id}</code>)\n"
                        f"👤 Вошёл: {joined_disp} (ID: <code>{user_id}</code>)\n"
                        f"🕒 {now_kyiv().isoformat()}"
                    )
    else:
        try:
            await callback.message.delete()
        except Exception:
            pass
        await safe_send_message(callback.from_user.id, "Уважаемый пользователь, к сожалению, вы не подписаны на спонсоров😢, попробуйте снова:", reply_markup=sponsor_keyboard(miss))
    await callback.answer()

# ---------------------- Прочее (рейтинг/кнопки/выводы) ----------------------

def classify_channel_sync(channel) -> str:
    try:
        if isinstance(channel, str) and channel.startswith("http"):
            return "invite"
        if isinstance(channel, str) and channel.startswith('-') and channel[1:].isdigit():
            return "private"
        display = str(channel).lstrip('@')
        if display.lower().endswith("bot"):
            return "bot"
        return "public"
    except Exception:
        return "unknown"

async def missing_channels(user_id: int) -> list:
    miss = []
    for channel in CHANNELS_TO_CHECK:
        try:
            if isinstance(channel, str) and channel.startswith("http"):
                miss.append(channel)
                continue
            if isinstance(channel, str) and channel.startswith('-') and channel[1:].isdigit():
                chat_id = int(channel)
            else:
                chat_id = channel
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
                miss.append(channel)
        except Exception:
            miss.append(channel)
    return miss

async def is_subscribed_all(user_id: int) -> bool:
    for channel in CHANNELS_TO_CHECK:
        try:
            ch_type = classify_channel_sync(channel)
            if ch_type in ("bot", "invite", "private"):
                continue
            if isinstance(channel, str) and channel.startswith('-') and channel[1:].isdigit():
                chat_id = int(channel)
            else:
                chat_id = channel
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
                return False
        except Exception:
            return False
    return True

def build_rating_text(time_frame: str):
    cur = conn.cursor()
    if time_frame == "24h":
        start_day = start_of_today_kyiv()
        end_day = start_day + timedelta(days=1)
        cur.execute("""
            SELECT u.username, COUNT(r.referred_id)
            FROM referral_rewards r
            JOIN users u ON r.referrer_id = u.user_id
            WHERE r.rewarded_at BETWEEN ? AND ?
            GROUP BY r.referrer_id
            ORDER BY COUNT(r.referred_id) DESC
            LIMIT 10
        """, (start_day.isoformat(), end_day.isoformat()))
        text = "🫂 Топ по рефералам за 24ч:\n\n"
    else:
        cur.execute("""
            SELECT u.username, COUNT(r.referred_id)
            FROM referral_rewards r
            JOIN users u ON r.referrer_id = u.user_id
            GROUP BY r.referrer_id
            ORDER BY COUNT(r.referred_id) DESC
            LIMIT 10
        """)
        text = "🫂 Топ по рефералам за всё время:\n\n"

    rows = cur.fetchall()
    if not rows:
        text += "Нет данных"
    else:
        for i, row in enumerate(rows, 1):
            uname = row[0] or "None"
            text += f"{i}. {uname} - {row[1]} рефералов\n"
    return text

async def send_rating(user_id: int, time_frame: str, old_msg: types.Message = None):
    now_dt = datetime.now()
    last_time = last_rating_click.get(user_id)
    if last_time and (now_dt - last_time).total_seconds() < 2:
        return
    last_rating_click[user_id] = now_dt

    text = build_rating_text(time_frame)
    kb = rating_keyboard_single_for(time_frame)

    if old_msg:
        try:
            await old_msg.delete()
        except Exception:
            pass

    await send_photo_caption(user_id, RATING_IMG_PATH, text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(lambda c: c.data in ["rating_24h", "rating_all"])
async def rating_callbacks(callback: types.CallbackQuery):
    tf = "24h" if callback.data == "rating_24h" else "all"
    text = build_rating_text(tf)
    kb = rating_keyboard_single_for(tf)
    try:
        await callback.message.edit_caption(caption=text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        try:
            await send_photo_caption(callback.from_user.id, RATING_IMG_PATH, text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await safe_answer_message(callback.message, text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

# ---------------------- Главный меню-хендлер ----------------------

@dp.message()
async def main_menu_handler(message: types.Message):
    uid = message.from_user.id
    text = (message.text or "").strip()

    if text == "🚪 Выйти из админки":
        if uid in admin_sessions:
            admin_sessions.discard(uid)
        admin_actions.pop(uid, None)
        await safe_answer_message(message, "🚪 Вы вышли из админ-панели.", reply_markup=ReplyKeyboardRemove())
        await safe_send_message(uid, "🔝 Главное меню", reply_markup=main_menu_keyboard())
        return

    if text.startswith("/"):
        admin_actions.pop(uid, None)
        return

    nav_buttons = {
        "Заработать звезды🌟",
        "Профиль 👤",
        "Рейтинг 📊",
        "Инструкция 📕",
        "Информация📚",
        "Вывести звезды✨",
        "Назад",
        "🔄 Обнулить пользователя",
        "🚫 Заблокировать / Разблокировать",
        "💳 Начислить звезды",
        "📢 Рассылка",
        "📈 Статистика пользователей",
    }
    if text in nav_buttons:
        admin_actions.pop(uid, None)

    # ----- Новая кнопка: статистика пользователей -----
    if text == "📈 Статистика пользователей":
        if not await has_admin_access(uid):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        cursor.execute("SELECT COUNT(*) FROM users")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE blocked=1")
        blocked = cursor.fetchone()[0]
        active = total - blocked
        msg = (
            f"Всего пользователей: {total}\n"
            f"Активных: {active}\n"
            f"Заблокировали бота: {blocked}"
        )
        await safe_answer_message(message, msg, reply_markup=admin_menu_kb())
        return

    if await maybe_handle_admin_dialog(message):
        return

    user_id = uid

    cursor.execute("SELECT blocked FROM users WHERE user_id=?", (user_id,))
    blk = cursor.fetchone()
    if blk and blk[0] == 1:
        await safe_answer_message(message, "🚫 Вы заблокированы администратором.")
        return

    if text == "📢 Рассылка":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "broadcast", "await": "sample"}
        await safe_answer_message(
            message,
            "📢 Пришлите сообщение, которое нужно разослать всем пользователям в базе (любой тип: текст/фото/видео/документ/голос/кружок и т.п.).\n\n"
            "Напишите «да» после — чтобы подтвердить рассылку. «отмена» — чтобы выйти.",
            reply_markup=admin_menu_kb()
        )
        return

    if text == "🔄 Обнулить пользователя":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "reset", "await": "user"}
        await safe_answer_message(message, "🧹 Кого обнулить? Пришлите @username или user_id.\nНапишите «отмена» для выхода.", reply_markup=admin_menu_kb())
        return

    if text == "🚫 Заблокировать / Разблокировать":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "toggle", "await": "user"}
        await safe_answer_message(message, "🚫 Кого заблокировать/разблокировать? Пришлите @username или user_id.\nНапишите «отмена» для выхода.", reply_markup=admin_menu_kk())
        return
# ---------------------- Главный меню-хендлер ----------------------

@dp.message()
async def main_menu_handler(message: types.Message):
    uid = message.from_user.id
    text = (message.text or "").strip()

    if text == "🚪 Выйти из админки":
        if uid in admin_sessions:
            admin_sessions.discard(uid)
        admin_actions.pop(uid, None)
        await safe_answer_message(message, "🚪 Вы вышли из админ-панели.", reply_markup=ReplyKeyboardRemove())
        await safe_send_message(uid, "🔝 Главное меню", reply_markup=main_menu_keyboard())
        return

    if text.startswith("/"):
        admin_actions.pop(uid, None)
        return

    nav_buttons = {
        "Заработать звезды🌟",
        "Профиль 👤",
        "Рейтинг 📊",
        "Инструкция 📕",
        "Информация📚",
        "Вывести звезды✨",
        "Назад",
        "🔄 Обнулить пользователя",
        "🚫 Заблокировать / Разблокировать",
        "💳 Начислить звезды",
        "📢 Рассылка",
        "📈 Статистика пользователей",
    }
    if text in nav_buttons:
        admin_actions.pop(uid, None)

    # ----- Новая кнопка: статистика пользователей -----
    if text == "📈 Статистика пользователей":
        if not await has_admin_access(uid):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        cursor.execute("SELECT COUNT(*) FROM users")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE blocked=1")
        blocked = cursor.fetchone()[0]
        active = total - blocked
        msg = (
            f"Всего пользователей: {total}\n"
            f"Активных: {active}\n"
            f"Заблокировали бота: {blocked}"
        )
        await safe_answer_message(message, msg, reply_markup=admin_menu_kb())
        return

    if await maybe_handle_admin_dialog(message):
        return

    user_id = uid

    cursor.execute("SELECT blocked FROM users WHERE user_id=?", (user_id,))
    blk = cursor.fetchone()
    if blk and blk[0] == 1:
        await safe_answer_message(message, "🚫 Вы заблокированы администратором.")
        return

    if text == "📢 Рассылка":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "broadcast", "await": "sample"}
        await safe_answer_message(
            message,
            "📢 Пришлите сообщение, которое нужно разослать всем пользователям в базе (любой тип: текст/фото/видео/документ/голос/кружок и т.п.).\n\n"
            "Напишите «да» после — чтобы подтвердить рассылку. «отмена» — чтобы выйти.",
            reply_markup=admin_menu_kb()
        )
        return

    if text == "🔄 Обнулить пользователя":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "reset", "await": "user"}
        await safe_answer_message(message, "🧹 Кого обнулить? Пришлите @username или user_id.\nНапишите «отмена» для выхода.", reply_markup=admin_menu_kb())
        return

    if text == "🚫 Заблокировать / Разблокировать":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "toggle", "await": "user"}
        await safe_answer_message(
            message,
            "🚫 Кого заблокировать/разблокировать? Пришлите @username или user_id.\nНапишите «отмена» для выхода.",
            reply_markup=admin_menu_kb()
        )
        return

    if text == "💳 Начислить звезды":
        if not await has_admin_access(user_id):
            await safe_answer_message(message, "❌ У вас нет доступа.")
            return
        admin_actions[user_id] = {"mode": "grant", "await": "user"}
        await safe_answer_message(
            message,
            "💳 Кому начислить звезды? Пришлите @username или user_id.\nНапишите «отмена» для выхода.",
            reply_markup=admin_menu_kb()
        )
        return

    if text == "Назад":
        if user_id in user_states:
            user_states.pop(user_id, None)
        await safe_answer_message(message, "🔝 Главное меню", reply_markup=main_menu_keyboard())
        return

    # ----- выводы / профиль / инфо -----
    state = user_states.get(user_id)
    if state:
        stage = state.get("stage")
        if stage == "await_amount":
            try:
                amount = int(text.strip())
            except Exception:
                await safe_answer_message(message, "Введите цифру: 15, 25, 50 или 100.", reply_markup=back_keyboard())
                return
            if amount not in (15, 25, 50, 100):
                await safe_answer_message(
                    message,
                    "Мы выводим только выводы на суммы: <b>15⭐️, 25⭐️, 50⭐️ и 100⭐️</b>",
                    reply_markup=back_keyboard(),
                    parse_mode="HTML"
                )
                return
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
            r = cursor.fetchone()
            balance = float(r[0]) if r and r[0] is not None else 0.0
            if amount > balance:
                await safe_answer_message(message, f"Недостаточно средств. Ваш баланс: {balance} ⭐️", reply_markup=back_keyboard())
                user_states.pop(user_id, None)
                return
            user_states[user_id] = {"stage": "awaiting_confirm_amount", "pending_amount": amount}
            await safe_answer_message(
                message,
                "⚠️ <b>ВАЖНО!</b> Перед подачей заявки на вывод необходимо отписать администратору @aaR1ss\n\n"
                "<b>Без этого ваша заявка не будет обработана и выведена!</b>",
                reply_markup=withdraw_amount_confirm_kb(user_id, amount),
                parse_mode="HTML"
            )
            return

        if stage == "await_username":
            to_username = text.strip()
            if not to_username:
                await safe_answer_message(
                    message,
                    "🗣 <b>Укажите свой юзернейм через @</b>\n\n"
                    "<b>Например: @aaR1ss</b>",
                    reply_markup=back_keyboard(),
                    parse_mode="HTML"
                )
                return
            if not to_username.startswith("@"):
                to_username = "@" + to_username
            state["pending_username"] = to_username
            user_states[user_id] = state
            await safe_answer_message(
                message,
                "🧑🏼‍💻 <b>Ваша заявка на вывод:</b>\n\n"
                f"<b>Указанный юзернейм:</b> {to_username}\n\n"
                f"<b>Сумма вывода:</b> {state['pending_amount']}⭐️",
                reply_markup=withdraw_final_confirm_kb(user_id),
                parse_mode="HTML"
            )
            return

    menu_buttons = [
        "Заработать звезды🌟",
        "Профиль 👤",
        "Рейтинг 📊",
        "Инструкция 📕",
        "Информация📚",
        "Вывести звезды✨",
    ]

    if text in menu_buttons:
        cursor.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        row = cursor.fetchone()
        if not row:
            await safe_answer_message(message, "Сначала начните работу с ботом через /start")
            return

        if row[2] == 0:
            miss = await missing_channels(user_id)
            await safe_answer_message(
                message,
                "Уважаемый пользователь, к сожалению, вы не подписаны на спонсоров😢, попробуйте снова",
                reply_markup=sponsor_keyboard(miss)
            )
            return

        if text == "Заработать звезды🌟":
            referral_link = row[8]
            caption = (
                f"<b>ЗАРАБОТАЙ ЗВЁЗДЫ ПРИГЛАШАЯ ДРУЗЕЙ В БОТА ПО СВОЕЙ РЕФЕРАЛЬНОЙ ССЫЛКЕ 🔗</b>\n\n"
                f"<b>├</b> <b>За каждого</b> приглашенного друга, ты получишь по <b>{REFERRAL_REWARD}.0⭐️</b>\n\n"
                f"<b>├</b> Также за каждых {REFERRAL_BONUS_EVERY} друзей — бонус <b>{REFERRAL_BONUS_AMOUNT}.0⭐️</b>\n\n"
                f"<b>├</b> Чтобы получить награду: друг должен зайти в бота по твоей ссылке и подписаться на спонсоров\n\n"
                f"<b>└</b> <b>Твоя реферальная ссылка 🔗</b> - {referral_link}"
            )
            await send_photo_caption(user_id, EARNINGS_IMG_PATH, caption, reply_markup=back_keyboard(), parse_mode="HTML")

        elif text == "Профиль 👤":
            caption = (
                f"👤 <b>Ник: @{row[1]}</b>\n\n"
                f"🫂 <b>Друзей приглашено: {row[5]}</b>\n\n"
                f"⭐️ <b>Заработано звезд: {row[6]}</b>\n\n"
                f"🏦 <b>Баланс: {row[4]} ⭐️</b>"
            )
            await send_photo_caption(user_id, PROFILE_IMG_PATH, caption, reply_markup=back_keyboard(), parse_mode="HTML")

        elif text == "Рейтинг 📊":
            await send_rating(user_id, "24h")

        elif text == "Инструкция 📕":
            kb_inst = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Полная инструкция🗂", url=INSTRUCTION_LINK)]
            ])
            faq_text = (
                "<b>- Что делать если не получается набрать минимальную сумму для вывода?\n\n"
                "Не обязательно делиться ссылкой только с друзьями — кидай её в чаты, свой канал, соцсети. Многим помогает тик-ток: ролик и ссылка в комментариях.\n\n"
                "- Почему списались ⭐️ после заявки, а на аккаунт не пришли?\n\n"
                "Заявка попадает администратору и ждёт подтверждения. После «выплачено» подарок приходит в течение нескольких минут.\n\n"
                "Частые вопросы 🔽</b>"
            )
            await safe_answer_message(message, faq_text, reply_markup=kb_inst, parse_mode="HTML")

        elif text == "Информация📚":
            global _stats_cache_date, _stats_cache_users, _stats_cache_withdrawn
            today = now_kyiv().date()
            if _stats_cache_date != today:
                days_passed = (today - START_DATE.date()).days
                users = BASE_USERS
                withdrawn = BASE_WITHDRAWN
                rnd = random.Random(days_passed)
                for _ in range(days_passed):
                    users += rnd.randint(3, 10)
                    withdrawn += rnd.randint(300, 700)
                _stats_cache_date = today
                _stats_cache_users = users
                _stats_cache_withdrawn = withdrawn

            info_text = (
                "<b>⭐️Старт бота: 28.08.2025\n"
                f"👥Всего пользователей: {_stats_cache_users}\n"
                f"📨Всего выведено: {_stats_cache_withdrawn}⭐️</b>"
            )
            await safe_answer_message(message, info_text, reply_markup=back_keyboard(), parse_mode="HTML")

        elif text == "Вывести звезды✨":
            cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
            rb = cursor.fetchone()
            balance = float(rb[0]) if rb and rb[0] is not None else 0.0
            caption = (
                "Введите какую сумму звёзд вы хотите вывести:\n\n"
                "Выводим только — <b>15⭐️, 25⭐️, 50⭐️, 100⭐️</b>"
            )
            await send_photo_caption(
                user_id, WITHDRAW_IMG_PATH, caption,
                reply_markup=back_keyboard(), parse_mode="HTML"
            )
            user_states[user_id] = {"stage": "await_amount"}
        else:
            await safe_answer_message(message, f"Вы нажали кнопку: {text}", reply_markup=back_keyboard())
        return

    await safe_answer_message(message, "🔝 Главное меню", reply_markup=main_menu_keyboard())

# ---------------------- Админ-диалог ----------------------

async def maybe_handle_admin_dialog(message: types.Message) -> bool:
    uid = message.from_user.id
    if uid not in admin_actions:
        return False
    if not await has_admin_access(uid):
        admin_actions.pop(uid, None)
        await safe_answer_message(message, "❌ Доступ закрыт.")
        return True

    state = admin_actions.get(uid) or {}
    mode = state.get("mode")
    step = state.get("await")

    text_lower = (message.text or "").strip().lower() if message.text else ""

    if text_lower in ("отмена", "cancel", "стоп"):
        admin_actions.pop(uid, None)
        await safe_answer_message(message, "❎ Отменено.", reply_markup=admin_menu_kb())
        return True

    # ===== Рассылка: sample -> confirm -> отправка
    if mode == "broadcast":
        if step == "sample":
            state["sample_chat_id"] = message.chat.id
            state["sample_message_id"] = message.message_id
            state["await"] = "confirm"
            admin_actions[uid] = state
            await safe_answer_message(
                message,
                "✅ Сообщение получено.\n\nНапишите «да» для подтверждения рассылки всем пользователям в базе, либо «отмена».",
                reply_markup=admin_menu_kb()
            )
            return True

        if step == "confirm":
            if text_lower in ("да", "yes", "y"):
                sample_chat_id = state.get("sample_chat_id")
                sample_message_id = state.get("sample_message_id")
                admin_actions.pop(uid, None)
                await safe_answer_message(message, "🚀 Запускаю рассылку…", reply_markup=admin_menu_kb())
                await do_broadcast(uid, sample_chat_id, sample_message_id)
                return True
            else:
                await safe_answer_message(
                    message,
                    "Не понял. Напишите «да» для запуска рассылки или «отмена».",
                    reply_markup=admin_menu_kb()
                )
                return True

    # ===== Остальные режимы (reset/toggle/grant)
    if step == "user":
        target_id, target_username = parse_user_ref(message.text or "")
        if not target_id:
            await safe_answer_message(
                message,
                "❗ Не нашёл такого пользователя. Пришлите корректный @username или user_id, либо напишите «отмена».",
                reply_markup=admin_menu_kb()
            )
            return True

        if mode == "reset":
            cursor.execute("UPDATE users SET balance=0, referrals_count=0, total_earned=0 WHERE user_id=?", (target_id,))
            cursor.execute("DELETE FROM referral_rewards WHERE referrer_id=? OR referred_id=?", (target_id, target_id))
            cursor.execute("DELETE FROM withdrawals WHERE user_id=?", (target_id,))
            conn.commit()
            admin_actions.pop(uid, None)
            await safe_answer_message(message, f"🧹 Пользователь {target_id} обнулён.", reply_markup=admin_menu_kb())
            return True

        if mode == "toggle":
            cursor.execute("SELECT blocked FROM users WHERE user_id=?", (target_id,))
            row = cursor.fetchone()
            if not row:
                await safe_answer_message(message, "❗ Такого пользователя нет в базе.", reply_markup=admin_menu_kb())
                admin_actions.pop(uid, None)
                return True
            new_status = 0 if row[0] == 1 else 1
            cursor.execute("UPDATE users SET blocked=? WHERE user_id=?", (new_status, target_id))
            conn.commit()
            status_text = "заблокирован" if new_status == 1 else "разблокирован"
            admin_actions.pop(uid, None)
            await safe_answer_message(message, f"🚫 Пользователь {target_id} {status_text}.", reply_markup=admin_menu_kb())
            return True

        if mode == "grant":
            state["await"] = "amount"
            state["target_id"] = target_id
            admin_actions[uid] = state
            await safe_answer_message(
                message,
                f"💳 Ок. Сколько ⭐️ начислить пользователю {target_id}? Напишите число. («отмена» для выхода)",
                reply_markup=admin_menu_kb()
            )
            return True

    if step == "amount" and mode == "grant":
        try:
            amount = float((message.text or "").replace(",", "."))
        except Exception:
            await safe_answer_message(
                message,
                "❗ Введите число (например: 10 или 25.0).",
                reply_markup=admin_menu_kb()
            )
            return True
        if amount <= 0:
            await safe_answer_message(message, "❗ Сумма должна быть положительной.", reply_markup=admin_menu_kb())
            return True
        target_id = state.get("target_id")
        if not target_id:
            admin_actions.pop(uid, None)
            await safe_answer_message(message, "⚠️ Ошибка контекста. Начните заново.", reply_markup=admin_menu_kb())
            return True

        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, target_id))
        conn.commit()
        admin_actions.pop(uid, None)

        await safe_send_message(
            target_id,
            f"🎁 <b>На ваш баланс начислено {amount}⭐️</b>",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
        await safe_answer_message(message, f"✅ Начислено {amount}⭐️ пользователю {target_id}.", reply_markup=admin_menu_kb())
        return True

    await safe_answer_message(
        message,
        "❗ Неверный ввод. Пришлите @username или user_id, либо «отмена».",
        reply_markup=admin_menu_kb()
    )
    return True

# ---------------------- Выводы (callbacks) ----------------------

@dp.callback_query(
    lambda c: c.data and (
        c.data.startswith("confirm_amount:") or
        c.data == "withdraw_back" or
        c.data.startswith("create_withdraw:") or
        c.data.startswith("redo_withdraw_user:")
    )
)
async def withdraw_confirm_handlers(callback: types.CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id

    if data == "withdraw_back":
        user_states.pop(user_id, None)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await safe_send_message(user_id, "🔝 Главное меню", reply_markup=main_menu_keyboard())
        await callback.answer()
        return

    if data.startswith("confirm_amount:"):
        parts = data.split(":")
        try:
            _, uid_s, amount_s = parts
            uid = int(uid_s)
            amount = int(amount_s)
        except Exception:
            await callback.answer()
            return
        if uid != user_id:
            await callback.answer("Это не ваша заявка.", show_alert=True)
            return
        user_states[user_id] = {"stage": "await_username", "pending_amount": amount}
        try:
            await callback.message.delete()
        except Exception:
            pass
        await safe_send_message(
            user_id,
            "🗣 <b>Укажите свой юзернейм через @</b>\n\n<b>Например: @aaR1ss</b>",
            reply_markup=back_keyboard(),
            parse_mode="HTML"
        )
        await callback.answer()
        return

    if data.startswith("create_withdraw:"):
        parts = data.split(":")
        try:
            _, uid_s = parts
            uid = int(uid_s)
        except Exception:
            await callback.answer()
            return
        if uid != user_id:
            await callback.answer("Это не ваша заявка.", show_alert=True)
            return
        state = user_states.get(user_id)
        if not state or "pending_amount" not in state or "pending_username" not in state:
            await callback.answer("Нет данных для создания заявки.", show_alert=True)
            return
        amount = float(state["pending_amount"])
        to_username = state["pending_username"]

        cursor.execute("SELECT balance FROM users WHERE user_id=?", (user_id,))
        r = cursor.fetchone()
        balance = float(r[0]) if r and r[0] is not None else 0.0
        if amount > balance:
            user_states.pop(user_id, None)
            try:
                await callback.message.delete()
            except Exception:
                pass
            await safe_send_message(user_id, f"Недостаточно средств. Ваш баланс: {balance} ⭐️", reply_markup=back_keyboard())
            await callback.answer()
            return

        cursor.execute(
            "INSERT INTO withdrawals(user_id, amount, to_username, status, created_at) VALUES(?,?,?,?,?)",
            (user_id, amount, to_username, "pending", now_kyiv().isoformat())
        )
        withdraw_id = cursor.lastrowid
        cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))
        conn.commit()

        kb_admin = admin_withdraw_kb(withdraw_id)
        cursor.execute("SELECT username FROM users WHERE user_id=?", (user_id,))
        usr_row = cursor.fetchone()
        usr = usr_row[0] if usr_row and usr_row[0] else "None"
        admin_text = (
            f"Заявка #{withdraw_id}\n"
            f"Пользователь: @{usr} ({user_id})\n"
            f"Сумма: {amount} ⭐️\n"
            f"Кому: {to_username}\n"
            f"Время: {now_kyiv().isoformat()}"
        )
        try:
            await bot.send_message(CHANNEL_FOR_WITHDRAW, admin_text, reply_markup=kb_admin)
        except Exception as e:
            _qwarn(f"[WARN] send to CHANNEL_FOR_WITHDRAW failed: {type(e).__name__}")

        try:
            await callback.message.delete()
        except Exception:
            pass

        msg = await safe_send_message(
            user_id,
            f"✅ <b>Заявка на вывод создана</b>\n\n"
            f"<b>Сумма вывода:</b> {amount}⭐️\n"
            f"<b>Юзернейм:</b> {to_username}",
            reply_markup=main_menu_keyboard(),
            parse_mode="HTML"
        )
        try:
            if msg and hasattr(msg, "message_id"):
                cursor.execute("UPDATE withdrawals SET user_msg_id=? WHERE id=?", (msg.message_id, withdraw_id))
                conn.commit()
        except Exception as e:
            _qwarn(f"[WARN] store user_msg_id failed: {type(e).__name__}")

        user_states.pop(user_id, None)
        await callback.answer()
        return

    if data.startswith("redo_withdraw_user:"):
        parts = data.split(":")
        try:
            _, uid_s = parts
            uid = int(uid_s)
        except Exception:
            await callback.answer()
            return
        if uid != user_id:
            await callback.answer("Это не ваша заявка.", show_alert=True)
            return
        prev_amount = user_states.get(user_id, {}).get("pending_amount", 0)
        user_states[user_id] = {"stage": "await_username", "pending_amount": prev_amount}
        try:
            await callback.message.delete()
        except Exception:
            pass
        await safe_send_message(
            user_id,
            "🗣 <b>Укажите свой юзернейм через @</b>\n\n<b>Например: @aaR1ss</b>",
            reply_markup=back_keyboard(),
            parse_mode="HTML"
        )
        await callback.answer()
        return

@dp.callback_query(lambda c: c.data and (c.data.startswith("admin_paid:") or c.data.startswith("admin_reject:")))
async def admin_withdraw_handlers(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not await has_admin_access(user_id):
        await callback.answer("❌ Нет доступа.", show_alert=True)
        return

    data = callback.data
    parts = data.split(":")
    try:
        _, withdraw_id_s = parts
        withdraw_id = int(withdraw_id_s)
    except Exception:
        await callback.answer()
        return

    cursor.execute("SELECT user_id, amount, status, user_msg_id FROM withdrawals WHERE id=?", (withdraw_id,))
    row = cursor.fetchone()
    if not row:
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    target_user_id, amount, status, user_msg_id = row

    if user_msg_id:
        try:
            await bot.delete_message(target_user_id, user_msg_id)
        except Exception:
            pass

    if data.startswith("admin_paid:"):
        cursor.execute("UPDATE withdrawals SET status='paid' WHERE id=?", (withdraw_id,))
        conn.commit()
        await safe_send_message(
            target_user_id,
            f"💸 <b>Ваша выплата в размере {amount}⭐️ была успешно отправлена вам!</b>",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
        await callback.answer("✅ Пометка как выплачено поставлена.")
        await safe_edit_text(callback.message, (callback.message.text or "") + "\n\n✅ Выплачено")
        return

    if data.startswith("admin_reject:"):
        cursor.execute("UPDATE withdrawals SET status='rejected' WHERE id=?", (withdraw_id,))
        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, target_user_id))
        conn.commit()
        await safe_send_message(
            target_user_id,
            f"❌ <b>Ваша заявка была отклонена. {amount}⭐️ возвращены на баланс</b>",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
        await callback.answer("❌ Заявка отклонена, средства возвращены.")
        await safe_edit_text(callback.message, (callback.message.text or "") + "\n\n❌ Отклонено")
        return

# ---------------------- MAIN ----------------------

async def main():
    if not QUIET_LOGGING:
        print("Бот запускается...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
