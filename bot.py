import asyncio
import logging
import sqlite3
import uuid
import os
import json
import sys
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, Tuple, List, Optional
import httpx
import qrcode
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton,
    ErrorEvent, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery, SuccessfulPayment, CallbackQuery
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums.parse_mode import ParseMode

load_dotenv()

# ====================== НАСТРОЙКИ ======================
BOT_TOKEN: str = os.getenv("BOT_TOKEN") or ""
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required")

ADMIN_IDS: List[int] = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]
ARBITRUM_WALLET: str = os.getenv("ARBITRUM_WALLET") or ""
USDT_CONTRACT: str = os.getenv("USDT_CONTRACT") or ""
USDC_CONTRACT: str = os.getenv("USDC_CONTRACT") or ""
ALCHEMY_API_KEY: str = os.getenv("ALCHEMY_API_KEY") or ""
SBP_DETAILS: str = os.getenv("SBP_DETAILS", "Номер карты: 2200 1234 5678 9012\nПолучатель: Иван Иванов\nБанк: Т-Банк")

SERVERS: Dict[int, dict] = {
    1: {
        "name": "🇫🇷 Франция",
        "ip": "185.193.89.183",
        "panel_url": os.getenv("PANEL_URL"),
        "panel_login": os.getenv("PANEL_LOGIN"),
        "panel_pass": os.getenv("PANEL_PASS"),
        "inbound_id": int(os.getenv("INBOUND_ID", 2)),
        "client_port": int(os.getenv("CLIENT_PORT", 2096)),
        "sub_path": os.getenv("SUB_PATH", "nkfnrwkrejkewtrewtg"),
        "pbk": os.getenv("PBK"),
        "sni": os.getenv("SNI"),
        "short_id": os.getenv("SHORT_ID"),
        "fp": os.getenv("FP"),
        "configured": True,
    },
}

COUNTRIES = [
    "🇺🇸 США", "🇬🇧 Великобритания", "🇩🇪 Германия", "🇫🇷 Франция",
    "🇨🇦 Канада", "🇯🇵 Япония", "🇦🇺 Австралия", "🇳🇱 Нидерланды",
    "🇸🇬 Сингапур", "🇨🇭 Швейцария", "🇸🇪 Швеция", "🇳🇴 Норвегия",
    "🇩🇰 Дания", "🇫🇮 Финляндия", "🇧🇪 Бельгия", "🇦🇹 Австрия",
    "🇮🇪 Ирландия", "🇮🇱 Израиль", "🇰🇷 Южная Корея", "🇧🇷 Бразилия"
]

# ====================== ЛОГИРОВАНИЕ ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
DB_FILE = "vpn_bot.db"

# ====================== ГЕНЕРАТОР УНИКАЛЬНОГО PAY-ID ======================
def generate_payment_uid(payment_id: int) -> str:
    date_str = datetime.now().strftime("%Y%m%d")
    return f"PAY-{date_str}-{str(payment_id).zfill(5)}"

# ====================== БАЗА ДАННЫХ ======================
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                server_id INTEGER,
                client_uuid TEXT UNIQUE,
                email TEXT,
                sub_id TEXT,
                expiry_date INTEGER,
                status TEXT DEFAULT "active",
                last_sync INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_uid TEXT UNIQUE,
                user_id INTEGER,
                tx_hash TEXT UNIQUE,
                currency TEXT,
                amount_usd REAL,
                amount_rub REAL,
                method TEXT,
                status TEXT DEFAULT "pending",
                created_at TEXT,
                confirmed_by_admin INTEGER DEFAULT 0,
                admin_note TEXT
            );
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                status TEXT DEFAULT "open",
                created_at TEXT,
                ticket_id TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS ticket_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT,
                sender_id INTEGER,
                message_text TEXT,
                created_at TEXT,
                is_admin BOOLEAN DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS country_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                country TEXT,
                status TEXT DEFAULT "open",
                created_at TEXT,
                request_id TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS tariffs (months INTEGER PRIMARY KEY, rub REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS pending_confirmations (
                user_id INTEGER,
                payment_id INTEGER PRIMARY KEY,
                confirm_type TEXT,
                data TEXT,
                created_at TEXT
            );
        """)
        # Миграции
        c.execute("PRAGMA table_info(subscriptions)")
        cols = [col[1] for col in c.fetchall()]
        if "last_sync" not in cols:
            c.execute("ALTER TABLE subscriptions ADD COLUMN last_sync INTEGER DEFAULT 0")
        c.execute("PRAGMA table_info(payments)")
        pay_cols = [col[1] for col in c.fetchall()]
        if "admin_note" not in pay_cols:
            c.execute("ALTER TABLE payments ADD COLUMN admin_note TEXT")
        if "payment_uid" not in pay_cols:
            c.execute("ALTER TABLE payments ADD COLUMN payment_uid TEXT UNIQUE")
            c.execute("""
                UPDATE payments
                SET payment_uid = 'PAY-' || strftime('%Y%m%d', 'now') || '-' || printf('%05d', id)
                WHERE payment_uid IS NULL OR payment_uid = ''
            """)
            logger.info("✅ Добавлена колонка payment_uid и выполнена миграция существующих платежей")
        # Создание таблиц для тикетов и запросов стран, если их нет
        c.execute("PRAGMA table_info(tickets)")
        ticket_cols = [col[1] for col in c.fetchall()]
        if "question" in ticket_cols:
            c.execute("ALTER TABLE tickets RENAME TO tickets_old")
            c.execute("""
                CREATE TABLE tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    status TEXT DEFAULT "open",
                    created_at TEXT,
                    ticket_id TEXT UNIQUE
                )
            """)
            c.execute("INSERT INTO tickets (id, user_id, status, created_at, ticket_id) SELECT id, user_id, status, created_at, ticket_id FROM tickets_old")
            c.execute("DROP TABLE tickets_old")
            logger.info("✅ Таблица tickets пересоздана без поля question")
        c.execute("PRAGMA table_info(ticket_messages)")
        if not c.fetchall():
            c.execute("""
                CREATE TABLE ticket_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT,
                    sender_id INTEGER,
                    message_text TEXT,
                    created_at TEXT,
                    is_admin BOOLEAN DEFAULT 0
                )
            """)
        c.execute("PRAGMA table_info(country_requests)")
        if not c.fetchall():
            c.execute("""
                CREATE TABLE country_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    country TEXT,
                    status TEXT DEFAULT "open",
                    created_at TEXT,
                    request_id TEXT UNIQUE
                )
            """)
        if c.execute("SELECT COUNT(*) FROM tariffs").fetchone()[0] == 0:
            c.executemany("INSERT INTO tariffs (months, rub) VALUES (?, ?)", [(1, 399), (3, 999), (6, 1799)])
        conn.commit()
    logger.info("✅ База данных инициализирована и готова к работе")

# ====================== КУРС И ТАРИФЫ ======================
USD_RUB_RATE: float = 81.5
RATE_CACHE_TIME: datetime = datetime.now()

async def update_usd_rub_rate() -> float:
    global USD_RUB_RATE, RATE_CACHE_TIME
    if (datetime.now() - RATE_CACHE_TIME).total_seconds() < 1800:
        return USD_RUB_RATE
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10)
            if r.status_code == 200:
                USD_RUB_RATE = float(r.json()["rates"]["RUB"])
                RATE_CACHE_TIME = datetime.now()
                logger.info(f"✅ Курс обновлён: 1 USD = {USD_RUB_RATE:.2f} RUB")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось обновить курс USD/RUB: {e}")
    return USD_RUB_RATE

TARIFFS: Dict[int, dict] = {}

async def load_tariffs():
    await update_usd_rub_rate()
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.cursor().execute("SELECT months, rub FROM tariffs").fetchall()
    global TARIFFS
    TARIFFS = {
        m: {
            "months": m,
            "rub": r,
            "usd": round(r / USD_RUB_RATE, 2),
            "label": f"{m} месяц" if m == 1 else f"{m} месяца" if m in (2, 3, 4) else f"{m} месяцев"
        }
        for m, r in rows
    }
    logger.info(f"✅ Загружено {len(TARIFFS)} тарифов")

# ====================== 3X-UI API ======================
class XUIApi:
    def __init__(self, server: dict):
        self.server = server
        self.base_url = server["panel_url"].rstrip("/")
        self.username = server["panel_login"]
        self.password = server["panel_pass"]
        self.client = httpx.AsyncClient(timeout=15, verify=False)
        self.cookies = None

    async def login(self) -> bool:
        try:
            r = await self.client.post(
                f"{self.base_url}/login",
                json={"username": self.username, "password": self.password}
            )
            if r.status_code == 200 and r.json().get("success"):
                self.cookies = r.cookies
                logger.info("✅ Успешный вход в 3x-ui панель")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка входа в панель: {e}")
        return False

    async def add_client(self, client: dict) -> bool:
        if not self.cookies and not await self.login():
            return False
        form = {"id": str(self.server["inbound_id"]), "settings": json.dumps({"clients": [client]})}
        try:
            r = await self.client.post(
                f"{self.base_url}/panel/api/inbounds/addClient",
                data=form,
                cookies=self.cookies,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            if r.status_code == 200 and r.json().get("success"):
                logger.info(f"✅ Клиент {client.get('email')} добавлен в панель")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка добавления клиента: {e}")
        return False

    async def remove_client(self, client_uuid: str) -> bool:
        if not self.cookies and not await self.login():
            return False
        try:
            r = await self.client.post(
                f"{self.base_url}/panel/api/inbounds/delClient",
                json={"id": self.server["inbound_id"], "clientId": client_uuid},
                cookies=self.cookies,
                headers={"Content-Type": "application/json"}
            )
            if r.status_code == 200 and r.json().get("success", False):
                logger.info(f"✅ Клиент {client_uuid} удалён из панели")
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка удаления клиента: {e}")
        return False

    async def get_clients(self) -> List[dict]:
        if not self.cookies and not await self.login():
            return []
        try:
            r = await self.client.post(
                f"{self.base_url}/panel/api/inbounds/getClientTraffics/{self.server['inbound_id']}",
                cookies=self.cookies
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    return data.get("obj", [])
        except Exception as e:
            logger.error(f"❌ Ошибка получения клиентов из панели: {e}")
        return []

# ====================== СИНХРОНИЗАЦИЯ ======================
async def sync_subscriptions_with_panel():
    with sqlite3.connect(DB_FILE) as conn:
        subs = conn.cursor().execute(
            "SELECT id, user_id, client_uuid, server_id FROM subscriptions WHERE status='active'"
        ).fetchall()
    for sub_id, user_id, client_uuid, server_id in subs:
        server = SERVERS.get(server_id, SERVERS[1])
        xui = XUIApi(server)
        clients = await xui.get_clients()
        found = any(c.get("id") == client_uuid for c in clients)
        if not found:
            with sqlite3.connect(DB_FILE) as conn:
                conn.cursor().execute("UPDATE subscriptions SET status='expired' WHERE id=?", (sub_id,))
                conn.commit()
            try:
                await bot.send_message(user_id, "⚠️ <b>Ваша подписка была деактивирована в панели.</b>\nОбратитесь в поддержку.", parse_mode=ParseMode.HTML)
            except:
                pass
            logger.info(f"🔄 Подписка {client_uuid} деактивирована (не найдена в панели)")

async def sync_task():
    while True:
        await asyncio.sleep(21600)
        await sync_subscriptions_with_panel()

# ====================== ВЕРИФИКАЦИЯ КРИПТО ======================
async def verify_arbitrum_tx(tx_hash: str, currency: str, expected_usd: float, retries: int = 12) -> Tuple[bool, str]:
    if not ALCHEMY_API_KEY:
        return False, "Alchemy API ключ не настроен"
    contract = USDT_CONTRACT if currency == "USDT" else USDC_CONTRACT
    decimals = 6
    delays = [5, 10, 15, 20, 30, 40, 50, 60, 80, 100, 120, 150]
    last_reason = "Транзакция не найдена или не подтверждена"
    for attempt in range(retries):
        if attempt > 0:
            wait_time = delays[attempt - 1] if attempt - 1 < len(delays) else 150
            logger.info(f"⏳ Ожидание {wait_time} сек перед попыткой {attempt+1}/{retries} для TX {tx_hash}")
            await asyncio.sleep(wait_time)
        try:
            url = f"https://arb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
            payload = {"jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [tx_hash], "id": 1}
            async with httpx.AsyncClient() as client:
                r = await client.post(url, json=payload, timeout=15)
                if r.status_code != 200:
                    continue
                data = r.json()
                receipt = data.get("result")
                if not receipt:
                    continue
                if receipt.get("status") != "0x1":
                    last_reason = "Транзакция завершилась с ошибкой (status != 1)"
                    return False, last_reason
                logs = receipt.get("logs", [])
                for log in logs:
                    if log.get("address", "").lower() != contract.lower():
                        continue
                    topics = log.get("topics", [])
                    if len(topics) < 3:
                        continue
                    to_topic = topics[2]
                    if len(to_topic) >= 42:
                        to_address = "0x" + to_topic[-40:]
                    else:
                        continue
                    if to_address.lower() != ARBITRUM_WALLET.lower():
                        continue
                    value_hex = log.get("data", "0x0")
                    try:
                        value = int(value_hex, 16) / (10 ** decimals)
                    except:
                        continue
                    if abs(value - expected_usd) < 0.01:
                        logger.info(f"✅ Платёж подтверждён | TX: {tx_hash} | Сумма: {value:.6f} {currency}")
                        return True, "✅ Платёж подтверждён"
                    else:
                        last_reason = f"Неверная сумма: {value:.6f} {currency}"
                        return False, last_reason
                last_reason = "Перевод на наш кошелёк не обнаружен"
                return False, last_reason
        except Exception as e:
            logger.error(f"⚠️ Ошибка при проверке (попытка {attempt+1}): {e}")
            continue
    return False, last_reason

# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def generate_vless_link(server: dict, client_uuid: str) -> str:
    params = {
        "security": "reality", "fp": server["fp"], "pbk": server["pbk"],
        "sni": server["sni"], "sid": server["short_id"],
        "flow": "xtls-rprx-vision", "type": "tcp", "headerType": "none", "encryption": "none"
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"vless://{client_uuid}@{server['ip']}:{server['client_port']}?{query}#{server['name']}"

def generate_subscription_link(server: dict, sub_id: str) -> str:
    return f"https://{server['ip']}:{server['client_port']}/{server['sub_path']}/{sub_id}"

async def create_subscription(user_id: int, server: dict, months: int, rub_amount: float, payment_id: Optional[int] = None) -> Optional[Tuple[str, str]]:
    client_uuid = str(uuid.uuid4())
    sub_id = uuid.uuid4().hex[:16]
    email = f"user_{user_id}_{months}m"
    expiry = int((datetime.now() + timedelta(days=30 * months)).timestamp() * 1000)
    payment_uid = None
    if payment_id:
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.cursor().execute("SELECT payment_uid FROM payments WHERE id=?", (payment_id,)).fetchone()
            if row:
                payment_uid = row[0]
    client_dict = {
        "id": client_uuid, "flow": "xtls-rprx-vision", "email": email,
        "limitIp": 2, "totalGB": 0, "expiryTime": expiry, "enable": True,
        "tgId": str(user_id), "subId": sub_id,
        "comment": f"Payment {payment_uid}" if payment_uid else "Admin created", "reset": 0
    }
    xui = XUIApi(server)
    if await xui.add_client(client_dict):
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT INTO subscriptions (user_id, server_id, client_uuid, email, sub_id, expiry_date, status, last_sync) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (user_id, 1, client_uuid, email, sub_id, expiry, "active", int(datetime.now().timestamp()))
            )
            if payment_id:
                c.execute("UPDATE payments SET status='completed' WHERE id=?", (payment_id,))
                c.execute("DELETE FROM pending_confirmations WHERE payment_id=?", (payment_id,))
            conn.commit()
        logger.info(f"✅ Подписка создана для {user_id} (PAY ID: {payment_uid})")
        return generate_vless_link(server, client_uuid), generate_subscription_link(server, sub_id)
    return None

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ====================== FSM ======================
class BuyStates(StatesGroup):
    select_server = State()
    select_tariff = State()
    select_method = State()
    select_crypto_currency = State()
    wait_crypto_hash = State()
    wait_sbp_confirm = State()

class ExtendSubscriptionStates(StatesGroup):
    select_tariff = State()
    select_method = State()
    select_crypto_currency = State()
    wait_crypto_hash = State()
    wait_sbp_confirm = State()

class TicketStates(StatesGroup):
    waiting_question = State()
    waiting_reply = State()          # ожидание ответа в тикете (для любой стороны)

class CountryRequestStates(StatesGroup):
    waiting_country = State()

class AdminPriceStates(StatesGroup):
    waiting_action = State()
    waiting_manual_input = State()

class AdminReplyStates(StatesGroup):
    waiting_ticket_id = State()
    waiting_reply_text = State()

class AdminBroadcastStates(StatesGroup):
    waiting_message = State()

class AdminCreateSubStates(StatesGroup):
    waiting_user_id = State()
    waiting_months = State()

class AdminTicketReplyStates(StatesGroup):
    waiting_reply_text = State()

class AdminCountryReplyStates(StatesGroup):
    waiting_reply_text = State()

# ====================== БОТ ======================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# ====================== КЛАВИАТУРЫ ======================
def main_keyboard(is_admin_flag: bool = False) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🛒 Купить подписку")],
        [KeyboardButton(text="👤 Личный кабинет"), KeyboardButton(text="📱 Как подключиться")],
        [KeyboardButton(text="❓ Поддержка"), KeyboardButton(text="🌍 Запросить новую страну")]
    ]
    if is_admin_flag:
        buttons.append([KeyboardButton(text="⚙️ Админ-панель"), KeyboardButton(text="🔄 Перезапустить бота")])
        buttons.append([KeyboardButton(text="🎫 Тикеты поддержки"), KeyboardButton(text="🌍 Запросы на новую страну")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, persistent=True)

def os_selection_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Android"), KeyboardButton(text="🍏 iOS")],
            [KeyboardButton(text="💻 Windows"), KeyboardButton(text="🍎 Mac")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def admin_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Изменить цены")],
            [KeyboardButton(text="👥 Управление пользователями")],
            [KeyboardButton(text="📢 Сделать рассылку")],
            [KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="✨ Создать подписку (админ)")],
            [KeyboardButton(text="◀️ Главное меню")]
        ],
        resize_keyboard=True
    )

def admin_users_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📋 Список всех пользователей")],
                  [KeyboardButton(text="◀️ Назад в админку")]],
        resize_keyboard=True
    )

def price_percent_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="+10%"), KeyboardButton(text="+20%")],
            [KeyboardButton(text="+30%"), KeyboardButton(text="+50%")],
            [KeyboardButton(text="✏️ Ввести вручную")],
            [KeyboardButton(text="◀️ Назад"), KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )

# ====================== НАВИГАЦИЯ ======================
@router.message(F.text == "◀️ Назад")
async def back_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state in (BuyStates.select_server, ExtendSubscriptionStates.select_tariff):
        await state.clear()
        await message.answer("👋 Главное меню", reply_markup=main_keyboard(is_admin(message.from_user.id)))
    elif current_state == BuyStates.select_tariff:
        await state.set_state(BuyStates.select_server)
        await show_servers(message)
    elif current_state == BuyStates.select_method:
        await state.set_state(BuyStates.select_tariff)
        await show_tariffs(message, state)
    elif current_state == BuyStates.select_crypto_currency:
        await state.set_state(BuyStates.select_method)
        await show_payment_methods(message)
    elif current_state == BuyStates.wait_crypto_hash:
        await state.set_state(BuyStates.select_crypto_currency)
        await show_crypto_currencies(message)
    elif current_state == BuyStates.wait_sbp_confirm:
        await state.set_state(BuyStates.select_method)
        await show_payment_methods(message)
    elif current_state == ExtendSubscriptionStates.select_method:
        await state.set_state(ExtendSubscriptionStates.select_tariff)
        await show_tariffs(message, state)
    elif current_state == ExtendSubscriptionStates.select_crypto_currency:
        await state.set_state(ExtendSubscriptionStates.select_method)
        await show_payment_methods(message)
    elif current_state == ExtendSubscriptionStates.wait_crypto_hash:
        await state.set_state(ExtendSubscriptionStates.select_crypto_currency)
        await show_crypto_currencies(message)
    elif current_state == ExtendSubscriptionStates.wait_sbp_confirm:
        await state.set_state(ExtendSubscriptionStates.select_method)
        await show_payment_methods(message)
    else:
        await state.clear()
        await message.answer("👋 Главное меню", reply_markup=main_keyboard(is_admin(message.from_user.id)))

@router.message(F.text == "❌ Отмена")
async def universal_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("✅ Действие отменено.", reply_markup=main_keyboard(is_admin(message.from_user.id)))

# ====================== /start ======================
@router.message(Command("start"))
async def cmd_start(message: Message):
    await load_tariffs()
    user_id = message.from_user.id
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute(
            "INSERT OR IGNORE INTO users (user_id, username, full_name, created_at) VALUES (?,?,?,?)",
            (user_id, message.from_user.username, message.from_user.full_name, datetime.now().isoformat())
        )
        conn.commit()
    await message.answer(
        "👋 <b>Добро пожаловать в VPN Pro</b>\n\n"
        "✨ Максимальная скорость\n"
        "🔒 Полная анонимность\n"
        "🛡️ Надёжная защита\n\n"
        "Выберите действие ниже 👇",
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(is_admin(user_id))
    )

GREETING_WORDS = {"привет", "hi", "hello", "здравствуй", "добрый день", "доброе утро", "добрый вечер", "хай", "всем привет"}
@router.message(F.text.lower().in_(GREETING_WORDS))
async def greeting_handler(message: Message):
    await cmd_start(message)

# ====================== ПОКУПКА ======================
async def show_servers(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=server["name"], callback_data=f"server_{server['name']}")]
        for server in SERVERS.values()
    ])
    await message.answer("🌍 <b>Выберите страну сервера</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

async def show_tariffs(message: Message, state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📅 {t['label']} — {t['rub']} ₽", callback_data=f"tariff_{t['months']}")]
        for t in TARIFFS.values()
    ])
    await message.answer("📦 <b>Выберите срок подписки</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

async def show_payment_methods(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Telegram Stars", callback_data="method_stars")],
        [InlineKeyboardButton(text="💳 СБП", callback_data="method_sbp")],
        [InlineKeyboardButton(text="₿ Криптовалюта (Arbitrum)", callback_data="method_crypto")]
    ])
    await message.answer("💵 <b>Выберите способ оплаты</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

async def show_crypto_currencies(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 USDT", callback_data="crypto_USDT"),
         InlineKeyboardButton(text="🔵 USDC", callback_data="crypto_USDC")]
    ])
    await message.answer("🪙 <b>Выберите криптовалюту</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

@router.message(F.text == "🛒 Купить подписку")
async def buy_start(message: Message, state: FSMContext):
    await state.set_state(BuyStates.select_server)
    await show_servers(message)

@router.callback_query(lambda c: c.data.startswith("server_"))
async def server_callback(callback: CallbackQuery, state: FSMContext):
    server_name = callback.data.split("_", 1)[1]
    server = next((s for s in SERVERS.values() if s["name"] == server_name), None)
    if not server:
        await callback.answer("Сервер не найден", show_alert=True)
        return
    await state.update_data(server=server, server_id=1)
    await state.set_state(BuyStates.select_tariff)
    await show_tariffs(callback.message, state)
    await callback.answer()

# ====================== ТАРИФ ======================
@router.callback_query(lambda c: c.data.startswith("tariff_"))
async def tariff_callback(callback: CallbackQuery, state: FSMContext):
    months = int(callback.data.split("_")[1])
    if months not in TARIFFS:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    tariff = TARIFFS[months]
    current_state = await state.get_state()
    await state.update_data(months=months, rub=tariff["rub"], usd=tariff["usd"])
    if current_state == AdminCreateSubStates.waiting_months:
        data = await state.get_data()
        target_user_id = data["target_user_id"]
        result = await create_subscription(target_user_id, SERVERS[1], months, 0, None)
        if result:
            vless, sub = result
            await callback.message.edit_text(
                f"🎉 <b>Подписка успешно создана для пользователя {target_user_id}</b>\n\n"
                f"🔗 <b>VLESS-ссылка:</b>\n<code>{vless}</code>\n\n"
                f"📡 <b>Ссылка на подписку:</b>\n<code>{sub}</code>",
                parse_mode=ParseMode.HTML
            )
            try:
                await bot.send_message(target_user_id, f"🎉 <b>Администратор выдал вам подписку!</b>\n\n🔗 <b>VLESS:</b>\n<code>{vless}</code>\n\n📡 <b>Подписка:</b>\n<code>{sub}</code>\n\nСпасибо за доверие!", parse_mode=ParseMode.HTML)
            except:
                pass
        else:
            await callback.message.edit_text("❌ Ошибка создания подписки.")
        await state.clear()
        await callback.answer()
        return
    if current_state == BuyStates.select_tariff:
        await state.set_state(BuyStates.select_method)
    elif current_state == ExtendSubscriptionStates.select_tariff:
        await state.set_state(ExtendSubscriptionStates.select_method)
    else:
        await callback.answer("Ошибка состояния", show_alert=True)
        return
    await show_payment_methods(callback.message)
    await callback.answer()

# ====================== МЕТОДЫ ОПЛАТЫ ======================
@router.callback_query(lambda c: c.data.startswith("method_"))
async def method_callback(callback: CallbackQuery, state: FSMContext):
    method = callback.data.split("_")[1]
    data = await state.get_data()
    user_id = callback.from_user.id
    current_state = await state.get_state()
    is_extend = current_state == ExtendSubscriptionStates.select_method
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        if method == "stars":
            stars = int(data["rub"])
            c.execute("INSERT INTO payments (user_id, amount_rub, method, status, created_at) VALUES (?,?,?,?,?)",
                      (user_id, data["rub"], "stars", "pending_stars", datetime.now().isoformat()))
            payment_id = c.lastrowid
            payment_uid = generate_payment_uid(payment_id)
            c.execute("UPDATE payments SET payment_uid = ? WHERE id = ?", (payment_uid, payment_id))
            confirm_type = "extend_stars" if is_extend else "stars"
            c.execute("INSERT OR REPLACE INTO pending_confirmations (user_id, payment_id, confirm_type, data, created_at) VALUES (?,?,?,?,?)",
                      (user_id, payment_id, confirm_type, json.dumps(data), datetime.now().isoformat()))
            conn.commit()
            title = "Продление подписки VPN" if is_extend else "Оплата подписки VPN"
            description = f"Продление на {data['months']} месяц(ев). Стоимость: {stars} Stars." if is_extend else f"Подписка на {data['months']} месяц(ев). Стоимость: {stars} Stars."
            payload = f"extend_{data['months']}_{data['rub']}" if is_extend else f"sub_{data['months']}_{data['rub']}"
            await bot.send_invoice(
                chat_id=callback.message.chat.id,
                title=title,
                description=description,
                payload=payload,
                provider_token="",
                currency="XTR",
                prices=[LabeledPrice(label="Подписка" if not is_extend else "Продление", amount=stars)],
                start_parameter="vpn_extend" if is_extend else "vpn_subscription",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⭐ Оплатить", pay=True)]])
            )
            await state.clear()
            await callback.answer()
        elif method == "sbp":
            await state.update_data(method="sbp")
            c.execute("INSERT INTO payments (user_id, amount_usd, amount_rub, method, status, created_at) VALUES (?,?,?,?,?,?)",
                      (user_id, data["usd"], data["rub"], "sbp", "pending_admin", datetime.now().isoformat()))
            payment_id = c.lastrowid
            payment_uid = generate_payment_uid(payment_id)
            c.execute("UPDATE payments SET payment_uid = ? WHERE id = ?", (payment_uid, payment_id))
            confirm_type = "extend_sbp" if is_extend else "sbp"
            c.execute("INSERT OR REPLACE INTO pending_confirmations (user_id, payment_id, confirm_type, data, created_at) VALUES (?,?,?,?,?)",
                      (user_id, payment_id, confirm_type, json.dumps(data), datetime.now().isoformat()))
            conn.commit()
            await state.update_data(payment_id=payment_id, payment_uid=payment_uid)
            await state.set_state(ExtendSubscriptionStates.wait_sbp_confirm if is_extend else BuyStates.wait_sbp_confirm)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Я оплатил", callback_data="sbp_paid"),
                 InlineKeyboardButton(text="❌ Отмена", callback_data="sbp_cancel")]
            ])
            text_prefix = " (продление)" if is_extend else ""
            await callback.message.answer(
                f"<b>💳 Оплата через СБП{text_prefix}</b>\n\n"
                f"Сумма: <b>{data['rub']} ₽</b>\n\n"
                f"<b>Реквизиты:</b>\n<code>{SBP_DETAILS}</code>\n\n"
                f"После перевода нажмите «✅ Я оплатил»",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
            await callback.answer()
        elif method == "crypto":
            await state.update_data(method="crypto")
            await state.set_state(ExtendSubscriptionStates.select_crypto_currency if is_extend else BuyStates.select_crypto_currency)
            await show_crypto_currencies(callback.message)
            await callback.answer()
        else:
            await callback.answer("Неизвестный способ оплаты", show_alert=True)

@router.callback_query(lambda c: c.data.startswith("crypto_"))
async def crypto_currency_callback(callback: CallbackQuery, state: FSMContext):
    currency = callback.data.split("_")[1]
    data = await state.get_data()
    user_id = callback.from_user.id
    current_state = await state.get_state()
    is_extend = current_state == ExtendSubscriptionStates.select_crypto_currency
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO payments (user_id, amount_usd, amount_rub, method, currency, status, created_at) VALUES (?,?,?,?,?,?,?)",
                  (user_id, data["usd"], data["rub"], "crypto", currency, "pending_crypto", datetime.now().isoformat()))
        payment_id = c.lastrowid
        payment_uid = generate_payment_uid(payment_id)
        c.execute("UPDATE payments SET payment_uid = ? WHERE id = ?", (payment_uid, payment_id))
        confirm_type = "extend_crypto" if is_extend else "crypto"
        c.execute("INSERT OR REPLACE INTO pending_confirmations (user_id, payment_id, confirm_type, data, created_at) VALUES (?,?,?,?,?)",
                  (user_id, payment_id, confirm_type, json.dumps(data), datetime.now().isoformat()))
        conn.commit()
        await state.update_data(payment_id=payment_id, payment_uid=payment_uid, crypto_currency=currency)
    await send_crypto_payment_details(callback.message, data["usd"], data["rub"], currency)
    await state.set_state(ExtendSubscriptionStates.wait_crypto_hash if is_extend else BuyStates.wait_crypto_hash)
    await callback.answer()

async def send_crypto_payment_details(message: Message, amount_usd: float, amount_rub: float, currency: str):
    crypto_amount = round(amount_usd, 4)
    text = (
        f"<b>₿ Оплата криптовалютой ({currency})</b>\n\n"
        f"🔹 <b>Сумма к оплате:</b> <code>{crypto_amount} {currency}</code>\n"
        f"🔹 <b>В рублях:</b> ≈ {amount_rub:.0f} ₽\n\n"
        f"🌐 <b>Сеть:</b> Arbitrum One\n"
        f"📄 <b>Контракт токена:</b>\n<code>{USDT_CONTRACT if currency == 'USDT' else USDC_CONTRACT}</code>\n"
        f"👛 <b>Кошелёк получателя:</b>\n<code>{ARBITRUM_WALLET}</code>\n\n"
        f"<b>После перевода нажмите «✅ Я оплатил» и пришлите TXID</b>"
    )
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(ARBITRUM_WALLET)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    bio = BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data="crypto_paid"),
         InlineKeyboardButton(text="❌ Отмена", callback_data="crypto_cancel")]
    ])
    await message.answer_photo(
        photo=BufferedInputFile(bio.getvalue(), filename="qr.png"),
        caption=text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )

# ====================== ИСПРАВЛЕННЫЙ «Я ОПЛАТИЛ» (крипта) ======================
@router.callback_query(lambda c: c.data == "crypto_paid")
async def crypto_paid_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Обрабатываю...")
    current_state = await state.get_state()
    if current_state not in (BuyStates.wait_crypto_hash, ExtendSubscriptionStates.wait_crypto_hash):
        await callback.answer("Ошибка: активный платёж не найден. Начните заново.", show_alert=True)
        return
    data = await state.get_data()
    if not data or "payment_id" not in data:
        await callback.answer("Платёж не найден", show_alert=True)
        return
    payment_id = data["payment_id"]
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        row = c.execute("SELECT status FROM payments WHERE id=?", (payment_id,)).fetchone()
        if not row:
            await callback.answer("Платёж не найден", show_alert=True)
            return
        if row[0] == "completed":
            await callback.answer("Этот платёж уже обработан.", show_alert=True)
            return
        if row[0] == "awaiting_hash":
            await callback.answer("Вы уже запросили ввод хеша. Пожалуйста, отправьте TXID.", show_alert=True)
            return
        if row[0] != "pending_crypto":
            await callback.answer("Невозможно подтвердить платёж в текущем статусе.", show_alert=True)
            return
        # Меняем статус на awaiting_hash, чтобы блокировать повторные нажатия
        c.execute("UPDATE payments SET status='awaiting_hash' WHERE id=?", (payment_id,))
        conn.commit()
    # Отправляем только сообщение с запросом хеша, без удаления предыдущего
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="crypto_cancel_confirm")]])
    await bot.send_message(
        chat_id=callback.message.chat.id,
        text="📎 <b>Отправьте хеш транзакции (TXID)</b>\n\nПример: <code>0x1234...abcd</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )
    await callback.answer()

@router.callback_query(lambda c: c.data == "sbp_paid")
async def sbp_paid_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "payment_id" not in data:
        await callback.answer("Платёж не найден", show_alert=True)
        return
    payment_id = data["payment_id"]
    user_id = callback.from_user.id
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        row = c.execute("SELECT status FROM payments WHERE id=? AND user_id=?", (payment_id, user_id)).fetchone()
        if not row or row[0] != "pending_admin":
            await callback.answer("Этот платёж уже обработан или отменён.", show_alert=True)
            return
        c.execute("UPDATE payments SET status='pending_admin' WHERE id=?", (payment_id,))
        conn.commit()
    for admin_id in ADMIN_IDS:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять", callback_data=f"sbp_accept_{user_id}"),
             InlineKeyboardButton(text="❌ Отклонить", callback_data=f"sbp_reject_{user_id}")]
        ])
        await bot.send_message(admin_id, f"💰 Новый платёж СБП от @{callback.from_user.username}\n💵 {data.get('rub')} ₽", parse_mode=ParseMode.HTML, reply_markup=kb)
    await callback.message.edit_text("✅ Заявка отправлена администратору.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
    await state.clear()
    await callback.answer()

@router.callback_query(lambda c: c.data in ("sbp_cancel", "crypto_cancel", "crypto_cancel_confirm"))
async def cancel_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("✅ Действие отменено.")
    await callback.answer()

# ====================== ОБРАБОТКА TXID ======================
@router.message(BuyStates.wait_crypto_hash)
@router.message(ExtendSubscriptionStates.wait_crypto_hash)
async def process_crypto_hash(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await universal_cancel(message, state)
        return
    tx_hash = message.text.strip()
    if len(tx_hash) < 64 or not tx_hash.startswith("0x"):
        await message.answer("❌ Неверный формат TXID. Хеш должен начинаться с 0x и содержать 66 символов.", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(is_admin(message.from_user.id)))
        return
    data = await state.get_data()
    user_id = message.from_user.id
    payment_id = data.get("payment_id")
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        row = c.execute("SELECT status FROM payments WHERE id=?", (payment_id,)).fetchone()
        if not row or row[0] != "awaiting_hash":
            await message.answer("❌ Этот платёж уже обработан или не ожидает хеша.", reply_markup=main_keyboard(is_admin(user_id)))
            await state.clear()
            return
    waiting_msg = await message.answer("⏳ Проверяем транзакцию... Пожалуйста, подождите.")
    success, reason = await verify_arbitrum_tx(tx_hash, data["crypto_currency"], data["usd"])
    await waiting_msg.delete()
    if not success:
        # Возвращаем статус обратно на pending_crypto, чтобы можно было повторить попытку
        with sqlite3.connect(DB_FILE) as conn:
            conn.cursor().execute("UPDATE payments SET status='pending_crypto' WHERE id=?", (payment_id,))
            conn.commit()
        await message.answer(f"❌ {reason}\n\nПроверьте корректность TXID и попробуйте снова.", reply_markup=main_keyboard(is_admin(user_id)))
        return
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        if c.execute("SELECT id FROM payments WHERE tx_hash=?", (tx_hash,)).fetchone():
            await message.answer("❌ Этот TXID уже использован для другого платежа.", reply_markup=main_keyboard(is_admin(user_id)))
            await state.clear()
            return
        c.execute("UPDATE payments SET tx_hash=?, status='confirmed' WHERE id=?", (tx_hash, payment_id))
        if await state.get_state() == ExtendSubscriptionStates.wait_crypto_hash:
            sub_id = data.get("sub_id")
            months = data["months"]
            c.execute("UPDATE subscriptions SET expiry_date = expiry_date + ? WHERE sub_id=?", (months * 30 * 24 * 3600 * 1000, sub_id))
            c.execute("UPDATE payments SET status='completed' WHERE id=?", (payment_id,))
            c.execute("DELETE FROM pending_confirmations WHERE payment_id=?", (payment_id,))
            conn.commit()
            await message.answer("✅ <b>Подписка успешно продлена!</b>\n\nВаша защита активна. Приятного использования! 🚀", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(is_admin(user_id)))
        else:
            conn.commit()
            result = await create_subscription(user_id, data["server"], data["months"], data["rub"], payment_id)
            if result:
                vless, sub = result
                await message.answer(
                    "🎉 <b>Оплата успешно подтверждена!</b>\n\n"
                    f"🔗 <b>VLESS-ссылка:</b>\n<code>{vless}</code>\n\n"
                    f"📡 <b>Ссылка на подписку:</b>\n<code>{sub}</code>\n\n"
                    "<i>Спасибо, что выбрали VPN Pro. Надёжная защита гарантирована.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_keyboard(is_admin(user_id))
                )
            else:
                await message.answer("❌ Ошибка создания подписки. Обратитесь в поддержку.", reply_markup=main_keyboard(is_admin(user_id)))
    await state.clear()

# ====================== TELEGRAM STARS ======================
@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout: PreCheckoutQuery):
    await pre_checkout.answer(ok=True)

@router.message(F.successful_payment)
async def successful_payment_handler(message: Message, state: FSMContext):
    user_id = message.from_user.id
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT payment_id, data, confirm_type FROM pending_confirmations WHERE user_id=? AND confirm_type IN ('stars', 'extend_stars') ORDER BY created_at DESC LIMIT 1", (user_id,))
        row = c.fetchone()
        if row:
            payment_id, data_json, confirm_type = row
            data = json.loads(data_json)
            is_extend = confirm_type == "extend_stars"
            pay_row = c.execute("SELECT status FROM payments WHERE id=?", (payment_id,)).fetchone()
            if pay_row and pay_row[0] == "completed":
                await state.clear()
                return
            if is_extend:
                months = data["months"]
                c.execute("UPDATE payments SET status='completed', tx_hash=? WHERE id=?", (message.successful_payment.provider_payment_charge_id, payment_id))
                c.execute("UPDATE subscriptions SET expiry_date = expiry_date + ? WHERE sub_id=?", (months * 30 * 24 * 3600 * 1000, data.get("sub_id")))
                c.execute("DELETE FROM pending_confirmations WHERE user_id=?", (user_id,))
                conn.commit()
                await message.answer("✅ <b>Подписка успешно продлена!</b>\n\nВаша защита активна. Приятного использования! 🚀", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(is_admin(user_id)))
            else:
                result = await create_subscription(user_id, data["server"], data["months"], data["rub"], payment_id)
                if result:
                    vless, sub = result
                    c.execute("UPDATE payments SET status='completed', tx_hash=? WHERE id=?", (message.successful_payment.provider_payment_charge_id, payment_id))
                    c.execute("DELETE FROM pending_confirmations WHERE user_id=?", (user_id,))
                    conn.commit()
                    await message.answer(
                        "🎉 <b>Оплата Telegram Stars подтверждена!</b>\n\n"
                        f"🔗 <b>VLESS-ссылка:</b>\n<code>{vless}</code>\n\n"
                        f"📡 <b>Ссылка на подписку:</b>\n<code>{sub}</code>\n\n"
                        "<i>Спасибо, что выбрали VPN Pro!</i>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=main_keyboard(is_admin(user_id))
                    )
                else:
                    await message.answer("❌ Ошибка создания подписки.")
            await state.clear()
            return
    await message.answer("❌ Не найден ожидающий платёж.", reply_markup=main_keyboard(is_admin(user_id)))

# ====================== АДМИН ДЕЙСТВИЯ СБП ======================
@router.callback_query(lambda c: c.data.startswith("sbp_accept_") or c.data.startswith("sbp_reject_"))
async def sbp_admin_action(callback: CallbackQuery, state: FSMContext):
    action, user_id_str = callback.data.split("_", 2)[1:]
    user_id = int(user_id_str)
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        row = c.execute("""
            SELECT p.id, p.amount_rub, pc.data, pc.confirm_type
            FROM payments p JOIN pending_confirmations pc ON p.id = pc.payment_id
            WHERE p.user_id=? AND p.method='sbp' AND p.status='pending_admin'
        """, (user_id,)).fetchone()
        if not row:
            await callback.answer("Платёж уже обработан.", show_alert=True)
            await callback.message.delete()
            return
        payment_id, amount_rub, data_json, confirm_type = row
        data = json.loads(data_json)
        is_extend = confirm_type == "extend_sbp"
        if action == "accept":
            if is_extend:
                sub_id = data.get("sub_id")
                months = data["months"]
                c.execute("UPDATE subscriptions SET expiry_date = expiry_date + ? WHERE sub_id=?", (months * 30 * 24 * 3600 * 1000, sub_id))
                c.execute("UPDATE payments SET status='completed', admin_note='Принято админом (продление)' WHERE id=?", (payment_id,))
                c.execute("DELETE FROM pending_confirmations WHERE payment_id=?", (payment_id,))
                conn.commit()
                await callback.message.edit_text(f"✅ Продление для {user_id} принято.")
                await bot.send_message(user_id, f"✅ <b>Ваш платёж через СБП подтверждён!</b>\nПодписка продлена на {months} месяц(ев).", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(False))
            else:
                result = await create_subscription(user_id, SERVERS[1], data["months"], amount_rub, payment_id)
                if result:
                    vless, sub = result
                    c.execute("DELETE FROM pending_confirmations WHERE user_id=?", (user_id,))
                    c.execute("UPDATE payments SET status='completed', admin_note='Принято админом' WHERE id=?", (payment_id,))
                    conn.commit()
                    await callback.message.edit_text(f"✅ Платёж для {user_id} принят.")
                    await bot.send_message(user_id, f"🎉 <b>Ваш платёж подтверждён!</b>\n\n🔗 <b>VLESS:</b>\n<code>{vless}</code>\n\n📡 <b>Подписка:</b>\n<code>{sub}</code>\n\nСпасибо!", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(False))
        else:
            c.execute("UPDATE payments SET status='rejected', admin_note='Отклонено админом' WHERE id=?", (payment_id,))
            c.execute("DELETE FROM pending_confirmations WHERE user_id=?", (user_id,))
            conn.commit()
            await callback.message.edit_text(f"❌ Платёж для {user_id} отклонён.")
            await bot.send_message(user_id, "❌ Ваш платёж отклонён. Свяжитесь с поддержкой.", reply_markup=main_keyboard(False))
    await callback.answer()

# ====================== ЛИЧНЫЙ КАБИНЕТ ======================
async def cabinet_entry(message: Message):
    user_id = message.from_user.id
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Активные подписки", callback_data="cabinet_active")],
        [InlineKeyboardButton(text="⏳ Ожидающие платежи", callback_data="cabinet_pending")],
        [InlineKeyboardButton(text="📜 История платежей", callback_data="cabinet_history")]
    ])
    await message.answer("👤 <b>Личный кабинет</b>\n\nВыберите раздел:", parse_mode=ParseMode.HTML, reply_markup=kb)

@router.message(F.text == "👤 Личный кабинет")
async def cabinet(message: Message):
    await cabinet_entry(message)

@router.callback_query(lambda c: c.data.startswith("cabinet_"))
async def cabinet_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    section = callback.data.split("_")[1]
    if section == "active":
        with sqlite3.connect(DB_FILE) as conn:
            subs = conn.cursor().execute("SELECT server_id, expiry_date, sub_id FROM subscriptions WHERE user_id=? AND status='active'", (user_id,)).fetchall()
        if not subs:
            await callback.message.edit_text("У вас пока нет активных подписок.")
            await callback.answer()
            return
        await callback.message.delete()
        for server_id, expiry_ts, sub_id in subs:
            expiry = datetime.fromtimestamp(expiry_ts / 1000).strftime("%d.%m.%Y %H:%M")
            server_name = SERVERS.get(server_id, {}).get("name", "Сервер")
            text = f"🌍 <b>{server_name}</b>\n📅 Действует до: <code>{expiry}</code>\n🆔 <code>{sub_id}</code>\n\n📡 <b>Ссылка на подписку:</b>\n<code>{generate_subscription_link(SERVERS.get(server_id, SERVERS[1]), sub_id)}</code>"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Продлить подписку", callback_data=f"extend_{sub_id}")]])
            await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        await callback.answer()
    elif section == "pending":
        with sqlite3.connect(DB_FILE) as conn:
            pending = conn.cursor().execute(
                "SELECT p.id, p.payment_uid, p.amount_rub, p.currency, p.method, p.created_at, p.status FROM payments p WHERE p.user_id=? AND p.status IN ('pending_crypto', 'pending_admin', 'awaiting_hash')",
                (user_id,)
            ).fetchall()
        if not pending:
            await callback.message.edit_text("⏳ У вас нет ожидающих платежей.")
            await callback.answer()
            return
        await callback.message.delete()
        for pid, uid, rub, curr, method, created, status in pending:
            dt = datetime.fromisoformat(created).strftime("%d.%m.%Y %H:%M")
            status_display = "Ожидает TXID" if status == "awaiting_hash" else ("Ожидает подтверждения" if status == "pending_admin" else "Ожидает оплаты")
            text = f"💸 <b>{uid}</b>\n💰 {rub} ₽ • {method.upper()}\n📅 {dt}\nСтатус: {status_display}"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📎 Отправить TXID", callback_data=f"resend_txid_{pid}"), InlineKeyboardButton(text="❌ Удалить", callback_data=f"delete_payment_{pid}")]
            ]) if method == "crypto" and status != "awaiting_hash" else InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Удалить", callback_data=f"delete_payment_{pid}")]])
            await callback.message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        await callback.answer()
    elif section == "history":
        with sqlite3.connect(DB_FILE) as conn:
            hist = conn.cursor().execute(
                "SELECT payment_uid, amount_rub, amount_usd, method, currency, status, created_at FROM payments WHERE user_id=? AND status='completed' ORDER BY created_at DESC",
                (user_id,)
            ).fetchall()
        if not hist:
            await callback.message.edit_text("📭 История платежей пуста.")
            await callback.answer()
            return
        await callback.message.delete()
        status_map = {"completed": "✅ Завершён"}
        method_map = {"crypto": "Криптовалюта", "sbp": "СБП", "stars": "Telegram Stars"}
        for uid, rub, usd, method, curr, status, dt in hist:
            dt_str = datetime.fromisoformat(dt).strftime("%d.%m.%Y %H:%M")
            text = f"<b>🧾 {uid}</b>\n"
            if method == "crypto" and curr and usd:
                text += f"💰 Сумма: <b>{usd} {curr}</b>\n"
                text += f"💵 Эквивалент: ≈ {rub:.0f} ₽\n"
            else:
                text += f"💰 Сумма: <b>{rub:.0f} ₽</b>\n"
            text += f"💳 Способ: {method_map.get(method, method.upper())}\n"
            text += f"📅 Дата: {dt_str}\n"
            text += f"📊 Статус: {status_map.get(status, status)}\n"
            text += "─" * 40
            await callback.message.answer(text, parse_mode=ParseMode.HTML)
        await callback.answer()

@router.callback_query(lambda c: c.data.startswith("resend_txid_"))
async def resend_txid_callback(callback: CallbackQuery, state: FSMContext):
    payment_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.cursor().execute("SELECT data, confirm_type FROM pending_confirmations WHERE user_id=? AND payment_id=? AND confirm_type IN ('crypto','extend_crypto')", (user_id, payment_id)).fetchone()
    if not row:
        await callback.answer("Платёж не найден", show_alert=True)
        return
    data_json, confirm_type = row
    data = json.loads(data_json)
    # Устанавливаем статус платежа обратно в awaiting_hash, если он был pending_crypto
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("UPDATE payments SET status='awaiting_hash' WHERE id=?", (payment_id,))
        conn.commit()
    if confirm_type == "crypto":
        await state.set_state(BuyStates.wait_crypto_hash)
    else:
        await state.set_state(ExtendSubscriptionStates.wait_crypto_hash)
    await state.update_data(data, payment_id=payment_id, crypto_currency=data.get("crypto_currency"))
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="crypto_cancel_confirm")]])
    await callback.message.answer("📎 <b>Отправьте TXID</b>", parse_mode=ParseMode.HTML, reply_markup=kb)
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("delete_payment_"))
async def delete_payment_callback(callback: CallbackQuery):
    pid = int(callback.data.split("_")[2])
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().executescript("DELETE FROM pending_confirmations WHERE payment_id=?; DELETE FROM payments WHERE id=?;", (pid, pid))
        conn.commit()
    await callback.message.edit_text("✅ Платёж успешно удалён.")
    await callback.answer()

@router.callback_query(lambda c: c.data.startswith("extend_"))
async def extend_subscription(callback: CallbackQuery, state: FSMContext):
    sub_id = callback.data.split("_")[1]
    with sqlite3.connect(DB_FILE) as conn:
        sub = conn.cursor().execute("SELECT user_id, server_id FROM subscriptions WHERE sub_id=? AND status='active'", (sub_id,)).fetchone()
    if not sub or sub[0] != callback.from_user.id:
        await callback.answer("Подписка не найдена", show_alert=True)
        return
    server = SERVERS.get(sub[1], SERVERS[1])
    await state.update_data(server=server, server_id=sub[1], sub_id=sub_id)
    await state.set_state(ExtendSubscriptionStates.select_tariff)
    await show_tariffs(callback.message, state)
    await callback.answer()

# ====================== ИНСТРУКЦИИ ======================
@router.message(F.text == "📱 Как подключиться")
async def instructions_os(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Android", callback_data="os_android")],
        [InlineKeyboardButton(text="🍏 iOS", callback_data="os_ios")],
        [InlineKeyboardButton(text="💻 Windows", callback_data="os_windows")],
        [InlineKeyboardButton(text="🍎 Mac", callback_data="os_mac")]
    ])
    await message.answer("📱 <b>Выберите вашу операционную систему</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

@router.callback_query(lambda c: c.data.startswith("os_"))
async def os_instructions_callback(callback: CallbackQuery):
    os_type = callback.data.split("_")[1]
    texts = {
        "android": "📱 <b>Android</b>\n\n1. Установите Nekobox / v2rayNG\n2. Импортируйте VLESS-ссылку\n3. Подключитесь",
        "ios": "🍏 <b>iOS</b>\n\n1. Установите Shadowrocket / Streisand\n2. Импортируйте VLESS-ссылку\n3. Подключитесь",
        "windows": "💻 <b>Windows</b>\n\n1. Установите v2rayN\n2. Импортируйте VLESS-ссылку\n3. Активируйте",
        "mac": "🍎 <b>Mac</b>\n\n1. Установите V2RayX / Nekoray\n2. Импортируйте VLESS-ссылку\n3. Активируйте"
    }
    await callback.message.edit_text(texts.get(os_type, "Инструкция в разработке"), parse_mode=ParseMode.HTML)

# ====================== ПОДДЕРЖКА (ТИКЕТЫ) ======================
@router.message(F.text == "❓ Поддержка")
async def support_start(message: Message, state: FSMContext):
    await state.set_state(TicketStates.waiting_question)
    await message.answer("✍️ <b>Опишите вашу проблему</b>\n\nМы ответим в ближайшее время.", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(is_admin(message.from_user.id)))

@router.message(TicketStates.waiting_question)
async def save_ticket(message: Message, state: FSMContext):
    ticket_id = "TICKET-" + uuid.uuid4().hex[:8]
    user_id = message.from_user.id
    question = message.text
    created_at = datetime.now().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO tickets (user_id, status, created_at, ticket_id) VALUES (?,?,?,?)",
                  (user_id, "open", created_at, ticket_id))
        c.execute("INSERT INTO ticket_messages (ticket_id, sender_id, message_text, created_at, is_admin) VALUES (?,?,?,?,?)",
                  (ticket_id, user_id, question, created_at, 0))
        conn.commit()
    # Уведомляем пользователя
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
         InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"ticket_close_{ticket_id}")]
    ])
    await message.answer(f"✅ <b>Тикет {ticket_id} создан</b>\n\nВы можете отправить дополнительные сообщения или закрыть тикет.", parse_mode=ParseMode.HTML, reply_markup=kb)
    # Уведомляем всех админов
    for admin_id in ADMIN_IDS:
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
             InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"ticket_close_{ticket_id}")]
        ])
        await bot.send_message(admin_id, f"🆕 Новый тикет {ticket_id}\nОт: @{message.from_user.username}\n\n{question}", parse_mode=ParseMode.HTML, reply_markup=admin_kb)
    await state.clear()

# Обработчик кнопки "Ответить" в тикете (как для админа, так и для пользователя)
@router.callback_query(lambda c: c.data.startswith("ticket_reply_"))
async def ticket_reply_callback(callback: CallbackQuery, state: FSMContext):
    ticket_id = callback.data.split("_")[2]
    # Проверяем, открыт ли тикет
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        row = c.execute("SELECT status FROM tickets WHERE ticket_id=?", (ticket_id,)).fetchone()
        if not row or row[0] != "open":
            await callback.answer("Тикет уже закрыт.", show_alert=True)
            return
    await state.update_data(ticket_id=ticket_id)
    await state.set_state(TicketStates.waiting_reply)
    await callback.message.answer("✏️ Введите ваш ответ:")
    await callback.answer()

@router.message(TicketStates.waiting_reply)
async def process_ticket_reply(message: Message, state: FSMContext):
    data = await state.get_data()
    ticket_id = data["ticket_id"]
    sender_id = message.from_user.id
    is_admin_sender = is_admin(sender_id)
    reply_text = message.text
    created_at = datetime.now().isoformat()
    # Сохраняем сообщение
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO ticket_messages (ticket_id, sender_id, message_text, created_at, is_admin) VALUES (?,?,?,?,?)",
                  (ticket_id, sender_id, reply_text, created_at, 1 if is_admin_sender else 0))
        # Получаем user_id, кому принадлежит тикет
        ticket = c.execute("SELECT user_id FROM tickets WHERE ticket_id=?", (ticket_id,)).fetchone()
        if not ticket:
            await message.answer("Ошибка: тикет не найден.")
            await state.clear()
            return
        user_id = ticket[0]
        conn.commit()
    # Отправляем сообщение получателю
    recipient_id = user_id if is_admin_sender else ADMIN_IDS[0]  # если ответ от пользователя, шлём первому админу (или всем)
    # Формируем клавиатуру с кнопками для получателя
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
         InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"ticket_close_{ticket_id}")]
    ])
    if is_admin_sender:
        # Отправляем ответ пользователю
        await bot.send_message(user_id, f"📬 <b>Ответ на тикет {ticket_id}</b>\n\n{reply_text}", parse_mode=ParseMode.HTML, reply_markup=kb)
        await message.answer("✅ Ваш ответ отправлен пользователю.")
    else:
        # Отправляем ответ админам
        for admin_id in ADMIN_IDS:
            await bot.send_message(admin_id, f"📬 <b>Новое сообщение в тикете {ticket_id}</b>\nОт: @{message.from_user.username}\n\n{reply_text}", parse_mode=ParseMode.HTML, reply_markup=kb)
        await message.answer("✅ Ваше сообщение отправлено администраторам.")
    await state.clear()

# Обработчик кнопки "Закрыть" в тикете
@router.callback_query(lambda c: c.data.startswith("ticket_close_"))
async def ticket_close_callback(callback: CallbackQuery):
    ticket_id = callback.data.split("_")[2]
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("UPDATE tickets SET status='closed' WHERE ticket_id=?", (ticket_id,))
        # Получаем user_id тикета
        row = c.execute("SELECT user_id FROM tickets WHERE ticket_id=?", (ticket_id,)).fetchone()
        conn.commit()
    if row:
        user_id = row[0]
        await bot.send_message(user_id, f"🔒 <b>Тикет {ticket_id} закрыт</b>\n\nСпасибо за обращение!", parse_mode=ParseMode.HTML)
    await callback.message.edit_text(f"✅ Тикет {ticket_id} закрыт.")
    await callback.answer()

# ====================== ЗАПРОС НОВОЙ СТРАНЫ ======================
@router.message(F.text == "🌍 Запросить новую страну")
async def request_country(message: Message, state: FSMContext):
    await state.set_state(CountryRequestStates.waiting_country)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=c, callback_data=f"country_{c}")] for c in COUNTRIES])
    await message.answer("🌏 <b>Выберите страну или напишите свою</b>", parse_mode=ParseMode.HTML, reply_markup=kb)

@router.callback_query(lambda c: c.data.startswith("country_"))
async def country_callback(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split("_", 1)[1]
    user_id = callback.from_user.id
    request_id = "REQ-" + uuid.uuid4().hex[:8]
    created_at = datetime.now().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("INSERT INTO country_requests (user_id, country, status, created_at, request_id) VALUES (?,?,?,?,?)",
                              (user_id, country, "open", created_at, request_id))
        conn.commit()
    await callback.message.edit_text("✅ Запрос отправлен администратору. Спасибо!")
    # Уведомляем админов с кнопкой "Ответить"
    for admin_id in ADMIN_IDS:
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"country_reply_{request_id}")]
        ])
        await bot.send_message(admin_id, f"🌍 Запрос новой страны от @{callback.from_user.username}\nСтрана: {country}", parse_mode=ParseMode.HTML, reply_markup=admin_kb)
    await state.clear()
    await callback.answer()

@router.message(CountryRequestStates.waiting_country)
async def custom_country(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await universal_cancel(message, state)
        return
    country = message.text
    user_id = message.from_user.id
    request_id = "REQ-" + uuid.uuid4().hex[:8]
    created_at = datetime.now().isoformat()
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("INSERT INTO country_requests (user_id, country, status, created_at, request_id) VALUES (?,?,?,?,?)",
                              (user_id, country, "open", created_at, request_id))
        conn.commit()
    await message.answer("✅ Запрос отправлен! Спасибо.", reply_markup=main_keyboard(is_admin(message.from_user.id)))
    for admin_id in ADMIN_IDS:
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"country_reply_{request_id}")]
        ])
        await bot.send_message(admin_id, f"🌍 Запрос новой страны от @{message.from_user.username}\nСтрана: {country}", parse_mode=ParseMode.HTML, reply_markup=admin_kb)
    await state.clear()

# Обработчик ответа админа на запрос страны
@router.callback_query(lambda c: c.data.startswith("country_reply_"))
async def country_reply_callback(callback: CallbackQuery, state: FSMContext):
    request_id = callback.data.split("_")[2]
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        row = c.execute("SELECT user_id, status FROM country_requests WHERE request_id=?", (request_id,)).fetchone()
        if not row or row[1] != "open":
            await callback.answer("Запрос уже обработан или не найден.", show_alert=True)
            return
        user_id = row[0]
    await state.update_data(request_id=request_id, user_id=user_id)
    await state.set_state(AdminCountryReplyStates.waiting_reply_text)
    await callback.message.answer("✏️ Введите ответ для пользователя:")
    await callback.answer()

@router.message(AdminCountryReplyStates.waiting_reply_text)
async def process_country_reply(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    request_id = data["request_id"]
    user_id = data["user_id"]
    reply_text = message.text
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("UPDATE country_requests SET status='closed' WHERE request_id=?", (request_id,))
        conn.commit()
    await bot.send_message(user_id, f"📬 <b>Ответ на запрос новой страны</b>\n\n{reply_text}", parse_mode=ParseMode.HTML)
    await message.answer("✅ Ответ отправлен пользователю.", reply_markup=admin_main_keyboard())
    await state.clear()

# ====================== АДМИН-ПАНЕЛЬ ======================
@router.message(F.text == "⚙️ Админ-панель")
async def admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("⚙️ <b>Админ-панель</b>", parse_mode=ParseMode.HTML, reply_markup=admin_main_keyboard())

@router.message(F.text == "💰 Изменить цены")
async def admin_edit_prices(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminPriceStates.waiting_action)
    await message.answer("💰 Выберите действие:", reply_markup=price_percent_keyboard())

@router.message(AdminPriceStates.waiting_action)
async def save_new_prices(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if message.text in ("◀️ Назад", "❌ Отмена"):
        await universal_cancel(message, state)
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            if message.text.startswith("+"):
                percent = int(message.text[1:].replace("%", ""))
                current = c.execute("SELECT months, rub FROM tariffs").fetchall()
                for m, r in current:
                    c.execute("UPDATE tariffs SET rub=? WHERE months=?", (round(r * (1 + percent / 100)), m))
            elif message.text == "✏️ Ввести вручную":
                await message.answer("Введите цены:\n1:399\n3:999\n6:1799")
                await state.set_state(AdminPriceStates.waiting_manual_input)
                return
            else:
                for line in message.text.strip().splitlines():
                    if ":" in line:
                        m, r = line.split(":")
                        c.execute("REPLACE INTO tariffs (months, rub) VALUES (?, ?)", (int(m), float(r)))
            conn.commit()
        await load_tariffs()
        await message.answer("✅ Цены обновлены!", reply_markup=admin_main_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}", reply_markup=price_percent_keyboard())
    await state.clear()

@router.message(AdminPriceStates.waiting_manual_input)
async def manual_prices_input(message: Message, state: FSMContext):
    await save_new_prices(message, state)

@router.message(F.text == "👥 Управление пользователями")
async def admin_users_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("👥 Управление пользователями", reply_markup=admin_users_keyboard())

@router.message(F.text == "📋 Список всех пользователей")
async def admin_list_users(message: Message):
    if not is_admin(message.from_user.id):
        return
    with sqlite3.connect(DB_FILE) as conn:
        users = conn.cursor().execute("""
            SELECT u.user_id, u.username, u.full_name, COUNT(s.id) as subs, MAX(s.expiry_date) as last_exp
            FROM users u LEFT JOIN subscriptions s ON u.user_id = s.user_id AND s.status='active'
            GROUP BY u.user_id ORDER BY u.user_id DESC
        """).fetchall()
    text = "<b>👥 Пользователи:</b>\n\n"
    for uid, uname, fname, subs, exp in users:
        exp_str = datetime.fromtimestamp(exp/1000).strftime("%d.%m.%Y") if exp else "—"
        text += f"🆔 <code>{uid}</code> | @{uname or fname} | Подписок: {subs} | До: {exp_str}\n"
    await message.answer(text[:4000], parse_mode=ParseMode.HTML, reply_markup=admin_users_keyboard())
    await message.answer("Удалить: /deluser &lt;client_uuid&gt;")

@router.message(Command("deluser"))
async def admin_delete_user(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /deluser &lt;client_uuid&gt;")
        return
    client_uuid = parts[1]
    with sqlite3.connect(DB_FILE) as conn:
        sub = conn.cursor().execute("SELECT user_id, server_id FROM subscriptions WHERE client_uuid=?", (client_uuid,)).fetchone()
        if not sub:
            await message.answer("UUID не найден.")
            return
        user_id, server_id = sub
    server = SERVERS.get(server_id, SERVERS[1])
    xui = XUIApi(server)
    if await xui.remove_client(client_uuid):
        with sqlite3.connect(DB_FILE) as conn:
            conn.cursor().execute("UPDATE subscriptions SET status='disabled' WHERE client_uuid=?", (client_uuid,))
            conn.commit()
        await message.answer(f"✅ Пользователь {user_id} удалён.")
    else:
        await message.answer("❌ Ошибка удаления.")

@router.message(F.text == "📢 Сделать рассылку")
async def admin_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminBroadcastStates.waiting_message)
    await message.answer("📢 Введите текст рассылки:")

@router.message(AdminBroadcastStates.waiting_message)
async def admin_do_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if message.text == "❌ Отмена":
        await universal_cancel(message, state)
        return
    with sqlite3.connect(DB_FILE) as conn:
        users = conn.cursor().execute("SELECT user_id FROM users").fetchall()
    count = 0
    for (uid,) in users:
        try:
            await bot.send_message(uid, message.text, parse_mode=ParseMode.HTML)
            count += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await message.answer(f"✅ Рассылка завершена. Отправлено {count} пользователям.", reply_markup=admin_main_keyboard())
    await state.clear()

@router.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        stats = {
            "users": c.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "active": c.execute("SELECT COUNT(*) FROM subscriptions WHERE status='active'").fetchone()[0],
            "revenue": c.execute("SELECT COALESCE(SUM(amount_rub),0) FROM payments WHERE status='completed'").fetchone()[0],
            "pending": c.execute("SELECT COUNT(*) FROM payments WHERE status IN ('pending_crypto','pending_admin','awaiting_hash')").fetchone()[0],
            "tickets": c.execute("SELECT COUNT(*) FROM tickets WHERE status='open'").fetchone()[0]
        }
    await message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{stats['users']}</b>\n"
        f"✅ Активных: <b>{stats['active']}</b>\n"
        f"💰 Выручка: <b>{stats['revenue']:.0f} ₽</b>\n"
        f"⏳ Ожидающих: <b>{stats['pending']}</b>\n"
        f"🎫 Тикетов: <b>{stats['tickets']}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_main_keyboard()
    )

@router.message(F.text == "✨ Создать подписку (админ)")
async def admin_create_subscription_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminCreateSubStates.waiting_user_id)
    await message.answer("👤 Введите user_id:")

@router.message(AdminCreateSubStates.waiting_user_id)
async def admin_create_subscription_get_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        uid = int(message.text.strip())
    except:
        await message.answer("❌ Неверный user_id.")
        return
    with sqlite3.connect(DB_FILE) as conn:
        if not conn.cursor().execute("SELECT user_id FROM users WHERE user_id=?", (uid,)).fetchone():
            await message.answer("❌ Пользователь не найден.")
            return
    await state.update_data(target_user_id=uid)
    await show_tariffs(message, state)
    await state.set_state(AdminCreateSubStates.waiting_months)

# ====================== НОВЫЕ КНОПКИ В ГЛАВНОМ МЕНЮ АДМИНА ======================
@router.message(F.text == "🎫 Тикеты поддержки")
async def admin_tickets_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    with sqlite3.connect(DB_FILE) as conn:
        tickets = conn.cursor().execute("SELECT ticket_id, user_id FROM tickets WHERE status='open' ORDER BY id DESC").fetchall()
    if not tickets:
        await message.answer("Нет открытых тикетов.", reply_markup=main_keyboard(True))
        return
    for ticket_id, user_id in tickets:
        # Получаем первое сообщение из тикета для отображения
        msg_row = conn.cursor().execute("SELECT message_text FROM ticket_messages WHERE ticket_id=? ORDER BY id ASC LIMIT 1", (ticket_id,)).fetchone()
        first_msg = msg_row[0] if msg_row else "Нет сообщений"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"ticket_reply_{ticket_id}"),
             InlineKeyboardButton(text="🔒 Закрыть", callback_data=f"ticket_close_{ticket_id}")]
        ])
        await message.answer(f"🎫 <b>{ticket_id}</b> от {user_id}\n\n{first_msg[:200]}", parse_mode=ParseMode.HTML, reply_markup=kb)
    await message.answer("Все открытые тикеты показаны выше.", reply_markup=main_keyboard(True))

@router.message(F.text == "🌍 Запросы на новую страну")
async def admin_country_requests(message: Message):
    if not is_admin(message.from_user.id):
        return
    with sqlite3.connect(DB_FILE) as conn:
        requests = conn.cursor().execute("SELECT request_id, user_id, country FROM country_requests WHERE status='open' ORDER BY id DESC").fetchall()
    if not requests:
        await message.answer("Нет открытых запросов на новые страны.", reply_markup=main_keyboard(True))
        return
    for req_id, user_id, country in requests:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Ответить", callback_data=f"country_reply_{req_id}")]
        ])
        await message.answer(f"🌍 <b>{req_id}</b> от {user_id}\nСтрана: {country}", parse_mode=ParseMode.HTML, reply_markup=kb)
    await message.answer("Все открытые запросы показаны выше.", reply_markup=main_keyboard(True))

@router.message(F.text == "🔄 Перезапустить бота")
async def restart_bot(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🔄 Бот перезагружается...")
    logger.info("🔄 Бот перезапущен администратором")
    os._exit(0)

# ====================== FALLBACK ======================
@router.message()
async def unknown_message(message: Message, state: FSMContext):
    if await state.get_state() is None:
        await message.answer("❓ Неизвестная команда. Используйте меню.", reply_markup=main_keyboard(is_admin(message.from_user.id)))

@router.errors()
async def error_handler(event: ErrorEvent):
    logger.error(f"❌ Критическая ошибка: {event.exception}", exc_info=True)

# ====================== ЗАПУСК ======================
async def main():
    init_db()
    await load_tariffs()
    await sync_subscriptions_with_panel()
    asyncio.create_task(sync_task())
    logger.info("🚀 Бот запущен и готов к работе")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())