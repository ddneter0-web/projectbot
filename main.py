import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import os

from dotenv import load_dotenv
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)
from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Numeric, String, Text,
    create_engine, delete, func, select
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip()
}
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    DATABASE_URL = f"sqlite+aiosqlite:///{(BASE_DIR / 'data' / 'bot.db').as_posix()}"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in .env")

if DATABASE_URL.startswith("sqlite"):
    db_part = DATABASE_URL.rsplit("///", 1)[-1].split("?", 1)[0]
    db_path = Path(db_part)
    if not db_path.is_absolute():
        db_path = (BASE_DIR / db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

engine_kwargs = {
    "echo": False,
    "pool_pre_ping": True,
}
if DATABASE_URL.startswith("postgresql"):
    engine_kwargs.update(pool_size=20, max_overflow=30)
else:
    engine_kwargs.update(pool_size=5, max_overflow=0)

engine = create_async_engine(DATABASE_URL, **engine_kwargs)
Session = async_sessionmaker(engine, expire_on_commit=False)

# ============================================================
# DATABASE
# ============================================================

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(255))
    balance: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0)
    referred_by: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("users.id")
    )
    referral_rewarded: Mapped[bool] = mapped_column(Boolean, default=False)
    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


class Sponsor(Base):
    __tablename__ = "sponsors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    invite_link: Mapped[str] = mapped_column(String(500))


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    description: Mapped[str] = mapped_column(Text)
    data: Mapped[str] = mapped_column(Text)
    sold: Mapped[bool] = mapped_column(Boolean, default=False)
    sold_to: Mapped[int | None] = mapped_column(BigInteger)
    sold_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    amount_rub: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    stars: Mapped[int] = mapped_column()
    telegram_charge_id: Mapped[str] = mapped_column(String(255), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if DATABASE_URL.startswith("sqlite"):
            await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
            await conn.exec_driver_sql("PRAGMA synchronous=NORMAL")


async def upsert_user(session: AsyncSession, tg_user, referred_by: int | None = None):
    user = await session.get(User, tg_user.id)
    now = datetime.now(timezone.utc)

    if user is None:
        user = User(
            id=tg_user.id,
            username=tg_user.username,
            referred_by=(
                referred_by
                if referred_by and referred_by != tg_user.id
                else None
            ),
            first_seen=now,
            last_seen=now,
        )
        session.add(user)
    else:
        user.username = tg_user.username
        user.last_seen = now
        if (
            user.referred_by is None
            and referred_by
            and referred_by != tg_user.id
        ):
            user.referred_by = referred_by

    await session.commit()
    return user


async def count_users(session: AsyncSession, since=None):
    q = select(func.count(User.id))
    if since:
        q = q.where(User.first_seen >= since)
    return int((await session.scalar(q)) or 0)

# ============================================================
# UI
# ============================================================

IMAGES_DIR = BASE_DIR / "images"


def valid_image(image_name):
    if not image_name:
        return None
    path = IMAGES_DIR / image_name
    if path.is_file() and path.stat().st_size > 0:
        return path
    return None


async def send_or_edit(target, text, keyboard, image_name=None):
    path = valid_image(image_name)

    if isinstance(target, Message):
        if path:
            return await target.answer_photo(
                FSInputFile(path),
                caption=text,
                reply_markup=keyboard,
            )
        return await target.answer(text, reply_markup=keyboard)

    if isinstance(target, CallbackQuery):
        msg = target.message

        if path:
            try:
                await msg.delete()
            except Exception:
                pass
            return await msg.answer_photo(
                FSInputFile(path),
                caption=text,
                reply_markup=keyboard,
            )

        try:
            return await msg.edit_text(
                text,
                reply_markup=keyboard,
            )
        except Exception:
            return await msg.answer(
                text,
                reply_markup=keyboard,
            )

# ============================================================
# KEYBOARDS
# ============================================================

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛍 Аккаунты", callback_data="products")],
        [InlineKeyboardButton(text="💰 Баланс", callback_data="balance")],
        [InlineKeyboardButton(text="👥 Рефералы", callback_data="referrals")],
    ])


def balance_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ 100 ₽ — 75 Stars", callback_data="topup:100:75")],
        [InlineKeyboardButton(text="⭐ 500 ₽ — 340 Stars", callback_data="topup:500:340")],
        [InlineKeyboardButton(text="⭐ 1000 ₽ — 890 Stars", callback_data="topup:1000:890")],
        [InlineKeyboardButton(text="⭐ Своя сумма", callback_data="topup_custom")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")],
    ])


def back_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")]
    ])


def products_keyboard(products):
    rows = [
        [InlineKeyboardButton(
            text=f"{p.name} — {p.price} ₽",
            callback_data=f"product:{p.id}",
        )]
        for p in products
    ]
    rows.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def product_detail_keyboard(product_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🛒 Купить",
            callback_data=f"buy:{product_id}",
        )],
        [InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data="products",
        )],
    ])


def sponsors_keyboard(sponsors):
    rows = []
    for sponsor in sponsors:
        rows.append([
            InlineKeyboardButton(
                text=f"📢 {sponsor.title}",
                url=sponsor.invite_link,
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text="✅ Проверить подписку",
            callback_data="check_subs",
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ============================================================
# SPONSORS / SUBSCRIPTIONS
# ============================================================

async def get_sponsors():
    async with Session() as s:
        result = await s.scalars(select(Sponsor).order_by(Sponsor.id))
        return list(result.all())


async def check_subscription(bot: Bot, user_id: int, sponsor: Sponsor) -> bool:
    try:
        member = await bot.get_chat_member(sponsor.chat_id, user_id)

        if member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }:
            return True

        if member.status == ChatMemberStatus.RESTRICTED:
            return bool(getattr(member, "is_member", False))

        return False
    except Exception as e:
        print(
            f"[SPONSOR CHECK ERROR] "
            f"chat={sponsor.chat_id} user={user_id}: {e}"
        )
        return False


async def missing_sponsors(bot: Bot, user_id: int):
    missing = []
    for sponsor in await get_sponsors():
        if not await check_subscription(bot, user_id, sponsor):
            missing.append(sponsor)
    return missing


async def all_subscribed(bot: Bot, user_id: int):
    return not await missing_sponsors(bot, user_id)

# ============================================================
# ADMIN
# ============================================================

def is_admin(user_id):
    return user_id in ADMIN_IDS


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📦 Товары", callback_data="admin:products")],
        [InlineKeyboardButton(text="📢 Спонсоры", callback_data="admin:sponsors")],
    ])


admin_router = Router()


@admin_router.message(Command("admin"))
async def admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "⚙️ <b>Админ-панель</b>",
        reply_markup=admin_kb(),
    )


@admin_router.callback_query(lambda c: c.data == "admin:stats")
async def stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    now = datetime.now(timezone.utc)

    async with Session() as s:
        day = await count_users(s, now - timedelta(days=1))
        week = await count_users(s, now - timedelta(days=7))
        month = await count_users(s, now - timedelta(days=30))
        total = await count_users(s)

    await callback.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"За день: {day}\n"
        f"За неделю: {week}\n"
        f"За месяц: {month}\n"
        f"За всё время: {total}",
        reply_markup=admin_kb(),
    )
    await callback.answer()


@admin_router.callback_query(lambda c: c.data == "admin:products")
async def admin_products(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    async with Session() as s:
        ps = (
            await s.scalars(
                select(Product)
                .order_by(Product.id.desc())
                .limit(30)
            )
        ).all()

    text = "📦 <b>Последние товары</b>\n\n"

    if not ps:
        text += "Нет товаров."
    else:
        text += "\n".join(
            f"#{p.id} {p.name} — {p.price} ₽ — "
            f"{'продан' if p.sold else 'в наличии'}"
            for p in ps
        )

    text += (
        "\n\n/product_add Название | Цена | Описание | Данные"
        "\n/product_delete ID"
    )

    await callback.message.edit_text(
        text,
        reply_markup=admin_kb(),
    )
    await callback.answer()


@admin_router.callback_query(lambda c: c.data == "admin:sponsors")
async def admin_sponsors(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return

    async with Session() as s:
        ss = (
            await s.scalars(
                select(Sponsor).order_by(Sponsor.id)
            )
        ).all()

    text = "📢 <b>Спонсоры</b>\n\n"
    text += (
        "\n".join(
            f"#{x.id} {x.title} ({x.chat_id})"
            for x in ss
        )
        or "Спонсоров нет."
    )
    text += (
        "\n\n/sponsor_add @channel | Название"
        "\n/sponsor_delete ID"
    )

    await callback.message.edit_text(
        text,
        reply_markup=admin_kb(),
    )
    await callback.answer()


@admin_router.message(Command("product_add"))
async def product_add(message: Message):
    if not is_admin(message.from_user.id):
        return

    raw = message.text.partition(" ")[2]
    parts = [x.strip() for x in raw.split("|")]

    if len(parts) != 4:
        await message.answer(
            "Формат: /product_add Название | Цена | Описание | Данные"
        )
        return

    name, price, desc, data = parts

    try:
        price_value = Decimal(price.replace(",", "."))
        if price_value < 0:
            raise ValueError
    except ValueError:
        await message.answer("Цена указана неверно.")
        return

    async with Session.begin() as s:
        s.add(Product(
            name=name,
            price=price_value,
            description=desc,
            data=data,
        ))

    await message.answer("✅ Товар добавлен.")


@admin_router.message(Command("product_delete"))
async def product_delete(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        pid = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("Формат: /product_delete ID")
        return

    async with Session.begin() as s:
        p = await s.get(Product, pid)
        if p:
            await s.delete(p)

    await message.answer("Удалено.")


@admin_router.message(Command("product_list"))
async def product_list(message: Message):
    if not is_admin(message.from_user.id):
        return

    async with Session() as s:
        ps = (
            await s.scalars(
                select(Product).order_by(Product.id)
            )
        ).all()

    await message.answer(
        "\n".join(
            f"#{p.id} {p.name} — {p.price} ₽ — "
            f"{'SOLD' if p.sold else 'IN STOCK'}"
            for p in ps
        )
        or "Нет товаров."
    )


@admin_router.message(Command("sponsor_add"))
async def sponsor_add(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return

    raw = message.text.partition(" ")[2]
    parts = [x.strip() for x in raw.split("|")]

    if len(parts) != 2:
        await message.answer(
            "Формат: /sponsor_add @channel | Название"
        )
        return

    target, title = parts

    try:
        chat = await bot.get_chat(target)
        chat_id = chat.id

        me = await bot.get_me()
        bot_member = await bot.get_chat_member(chat_id, me.id)

        if bot_member.status not in {
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }:
            await message.answer(
                "❌ Бот должен быть администратором "
                "канала-спонсора, иначе Telegram не даст "
                "проверять подписку пользователей."
            )
            return

        if getattr(chat, "username", None):
            link = f"https://t.me/{chat.username}"
        else:
            link_obj = await bot.create_chat_invite_link(
                chat_id,
                name="Sponsor",
            )
            link = link_obj.invite_link

    except Exception as e:
        await message.answer(
            "Не удалось получить чат/создать ссылку. "
            "Добавьте бота администратором.\n"
            f"{e}"
        )
        return

    async with Session.begin() as s:
        old = await s.scalar(
            select(Sponsor).where(Sponsor.chat_id == chat_id)
        )

        if old:
            old.title = title
            old.invite_link = link
        else:
            s.add(Sponsor(
                chat_id=chat_id,
                title=title,
                invite_link=link,
            ))

    await message.answer("✅ Спонсор добавлен.")


@admin_router.message(Command("sponsor_delete"))
async def sponsor_delete(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        sid = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("Формат: /sponsor_delete ID")
        return

    async with Session.begin() as s:
        sp = await s.get(Sponsor, sid)
        if sp:
            await s.delete(sp)

    await message.answer("Удалено.")


@admin_router.message(Command("sponsor_list"))
async def sponsor_list(message: Message):
    if not is_admin(message.from_user.id):
        return

    async with Session() as s:
        ss = (
            await s.scalars(
                select(Sponsor).order_by(Sponsor.id)
            )
        ).all()

    await message.answer(
        "\n".join(
            f"#{x.id} {x.title} — {x.chat_id}"
            for x in ss
        )
        or "Спонсоров нет."
    )

# ============================================================
# START
# ============================================================

start_router = Router()


async def process_start(message, bot, referred_by=None):
    async with Session() as s:
        await upsert_user(
            s,
            message.from_user,
            referred_by,
        )

    missing = await missing_sponsors(
        bot,
        message.from_user.id,
    )

    if missing:
        await send_or_edit(
            message,
            "👋 <b>Добро пожаловать!</b>\n\n"
            "Чтобы продолжить, подпишитесь на всех спонсоров:",
            sponsors_keyboard(missing),
            "main.png",
        )
        return

    await send_or_edit(
        message,
        "🏠 <b>Главное меню</b>\n\nВыберите действие:",
        main_menu(),
        "main.png",
    )


@start_router.message(CommandStart())
async def start(message: Message, bot: Bot):
    args = (message.text or "").split(maxsplit=1)
    ref = None

    if len(args) == 2 and args[1].startswith("ref_"):
        try:
            ref = int(args[1][4:])
        except ValueError:
            pass

    await process_start(message, bot, ref)


@start_router.callback_query(lambda c: c.data == "check_subs")
async def check_subs(callback: CallbackQuery, bot: Bot):
    missing = await missing_sponsors(
        bot,
        callback.from_user.id,
    )

    if missing:
        await callback.answer(
            "Вы подписаны не на всех спонсоров.",
            show_alert=True,
        )
        await send_or_edit(
            callback,
            "❌ <b>Не все подписки выполнены.</b>\n\n"
            "Подпишитесь на оставшиеся:",
            sponsors_keyboard(missing),
            "main.png",
        )
        return

    async with Session() as s:
        user = await s.get(User, callback.from_user.id)

        if user and user.referred_by and not user.referral_rewarded:
            ref = await s.get(User, user.referred_by)

            if ref:
                ref.balance = (ref.balance or 0) + 20
                user.referral_rewarded = True
                await s.commit()

    await callback.answer("Подписка подтверждена!")

    await send_or_edit(
        callback,
        "🏠 <b>Главное меню</b>\n\nВыберите действие:",
        main_menu(),
        "main.png",
    )

# ============================================================
# MENU
# ============================================================

menu_router = Router()


@menu_router.callback_query(lambda c: c.data == "menu")
async def menu(callback: CallbackQuery):
    await send_or_edit(
        callback,
        "🏠 <b>Главное меню</b>\n\nВыберите действие:",
        main_menu(),
        "main.png",
    )
    await callback.answer()


@menu_router.callback_query(lambda c: c.data == "balance")
async def balance(callback: CallbackQuery):
    async with Session() as s:
        user = await s.get(User, callback.from_user.id)
        amount = user.balance if user else 0

    await send_or_edit(
        callback,
        f"💰 <b>Баланс</b>\n\n"
        f"Ваш баланс: <b>{amount} ₽</b>",
        balance_keyboard(),
        "balance.png",
    )
    await callback.answer()

# ============================================================
# PRODUCTS
# ============================================================

products_router = Router()


@products_router.callback_query(lambda c: c.data == "products")
async def products(callback: CallbackQuery):
    async with Session() as s:
        products_list = (
            await s.scalars(
                select(Product)
                .where(Product.sold == False)
                .order_by(Product.id)
            )
        ).all()

    if not products_list:
        await send_or_edit(
            callback,
            "🛍 <b>Аккаунты</b>\n\n"
            "Товаров пока нет. Скоро появятся!",
            back_menu(),
            "products.png",
        )
    else:
        await send_or_edit(
            callback,
            "🛍 <b>Аккаунты в наличии</b>\n\n"
            "Выберите товар:",
            products_keyboard(products_list),
            "products.png",
        )

    await callback.answer()


@products_router.callback_query(
    lambda c: c.data and c.data.startswith("product:")
)
async def product_detail(callback: CallbackQuery):
    try:
        pid = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("Неверный товар.", show_alert=True)
        return

    async with Session() as s:
        p = await s.get(Product, pid)

    if not p or p.sold:
        await callback.answer(
            "Товар уже продан или отсутствует.",
            show_alert=True,
        )
        return

    text = (
        f"📦 <b>{p.name}</b>\n\n"
        f"{p.description}\n\n"
        f"💵 Цена: <b>{p.price} ₽</b>"
    )

    await send_or_edit(
        callback,
        text,
        product_detail_keyboard(p.id),
        "products.png",
    )
    await callback.answer()


@products_router.callback_query(
    lambda c: c.data and c.data.startswith("buy:")
)
async def buy(callback: CallbackQuery):
    try:
        pid = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("Неверный товар.", show_alert=True)
        return

    async with Session.begin() as s:
        user = await s.get(
            User,
            callback.from_user.id,
        )
        p = await s.get(Product, pid)

        if not p or p.sold:
            await callback.answer(
                "Товар уже продан.",
                show_alert=True,
            )
            return

        if not user or user.balance < p.price:
            await callback.answer(
                "Недостаточно средств.",
                show_alert=True,
            )
            return

        user.balance -= p.price
        p.sold = True
        p.sold_to = user.id
        p.sold_at = datetime.now(timezone.utc)

        data = p.data
        product_name = p.name

    await callback.message.answer(
        "✅ <b>Покупка успешна!</b>\n\n"
        f"Товар: <b>{product_name}</b>\n"
        f"Данные:\n<code>{data}</code>"
    )
    await callback.answer("Покупка выполнена!")

# ============================================================
# PAYMENTS
# ============================================================

payments_router = Router()


@payments_router.callback_query(
    lambda c: c.data and c.data.startswith("topup:")
)
async def fixed_topup(callback: CallbackQuery, bot: Bot):
    _, rub, stars = callback.data.split(":")

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=f"Пополнение на {rub} ₽",
        description=f"Пополнение баланса на {rub} ₽",
        payload=(
            f"topup:{rub}:{stars}:"
            f"{callback.from_user.id}"
        ),
        currency="XTR",
        prices=[
            LabeledPrice(
                label=f"{rub} ₽",
                amount=int(stars),
            )
        ],
    )
    await callback.answer()


@payments_router.callback_query(
    lambda c: c.data == "topup_custom"
)
async def custom_topup(callback: CallbackQuery):
    await callback.message.answer(
        "Введите сумму в рублях целым числом одним сообщением.\n"
        "Например: <code>250</code>.\n\n"
        "Для отмены нажмите /cancel."
    )
    await callback.answer()


@payments_router.message(
    lambda m: (
        m.text
        and m.text.isdigit()
        and 1 <= int(m.text) <= 100000
    )
)
async def custom_amount(message: Message, bot: Bot):
    rub = int(message.text)
    stars = rub

    await bot.send_invoice(
        chat_id=message.from_user.id,
        title=f"Пополнение на {rub} ₽",
        description=f"Пополнение баланса на {rub} ₽",
        payload=(
            f"topup:{rub}:{stars}:"
            f"{message.from_user.id}"
        ),
        currency="XTR",
        prices=[
            LabeledPrice(
                label=f"{rub} ₽",
                amount=stars,
            )
        ],
    )


@payments_router.pre_checkout_query()
async def pre_checkout(
    query: PreCheckoutQuery,
):
    await query.answer(ok=True)


@payments_router.message(F.successful_payment)
async def successful_payment(message: Message):
    sp = message.successful_payment
    parts = sp.invoice_payload.split(":")

    if len(parts) != 4 or parts[0] != "topup":
        return

    try:
        rub = int(parts[1])
    except ValueError:
        return

    async with Session.begin() as s:
        user = await s.get(
            User,
            message.from_user.id,
        )

        if not user:
            user = User(id=message.from_user.id)
            s.add(user)
            await s.flush()

        existing = await s.scalar(
            select(Payment).where(
                Payment.telegram_charge_id
                == sp.telegram_payment_charge_id
            )
        )

        if existing:
            return

        user.balance += rub

        s.add(Payment(
            user_id=user.id,
            amount_rub=rub,
            stars=sp.total_amount,
            telegram_charge_id=(
                sp.telegram_payment_charge_id
            ),
        ))

    await message.answer(
        f"✅ Баланс пополнен на <b>{rub} ₽</b>."
    )

# ============================================================
# REFERRALS
# ============================================================

referrals_router = Router()


@referrals_router.callback_query(
    lambda c: c.data == "referrals"
)
async def referrals(callback: CallbackQuery):
    async with Session() as s:
        count = await s.scalar(
            select(func.count(User.id)).where(
                User.referred_by == callback.from_user.id
            )
        )
        user = await s.get(
            User,
            callback.from_user.id,
        )

    link = (
        f"https://t.me/{BOT_USERNAME}"
        f"?start=ref_{callback.from_user.id}"
        if BOT_USERNAME
        else f"/start ref_{callback.from_user.id}"
    )

    text = (
        "👥 <b>Реферальная система</b>\n\n"
        "Приглашайте друзей и получайте <b>20 ₽</b> "
        "за каждого реферала после прохождения "
        "обязательной подписки.\n\n"
        f"Приглашено: <b>{count}</b>\n"
        f"Ваш баланс: <b>{user.balance if user else 0} ₽</b>\n\n"
        f"Ваша ссылка:\n<code>{link}</code>"
    )

    await send_or_edit(
        callback,
        text,
        back_menu(),
        "refferals.png",
    )
    await callback.answer()

# ============================================================
# SUBSCRIPTION MIDDLEWARE
# ============================================================

class SubscriptionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            cb = event

            if (
                cb.data == "check_subs"
                or (
                    cb.data
                    and cb.data.startswith("admin:")
                )
            ):
                return await handler(event, data)

        if (
            isinstance(event, Message)
            and event.text
            and event.text.startswith("/")
        ):
            text = event.text

            if (
                text.startswith("/start")
                or text.startswith("/admin")
                or text.startswith("/product_")
                or text.startswith("/sponsor_")
            ):
                return await handler(event, data)

        tg_user = event.from_user

        async with Session() as s:
            await upsert_user(s, tg_user)

        missing = await missing_sponsors(
            data["bot"],
            tg_user.id,
        )

        if missing:
            text = (
                "🔒 <b>Доступ ограничен</b>\n\n"
                "Подпишитесь на всех спонсоров и "
                "нажмите «Проверить подписку»."
            )

            await send_or_edit(
                event,
                text,
                sponsors_keyboard(missing),
                "main.png",
            )

            if isinstance(event, CallbackQuery):
                await event.answer(
                    "Сначала подпишитесь на всех спонсоров.",
                    show_alert=True,
                )

            return

        return await handler(event, data)

# ============================================================
# STARTUP
# ============================================================

async def main():
    logging.basicConfig(level=logging.INFO)

    await init_db()

    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML
        ),
    )

    dp = Dispatcher()

    # Middleware ставим перед пользовательскими обработчиками.
    dp.message.middleware(SubscriptionMiddleware())
    dp.callback_query.middleware(SubscriptionMiddleware())

    # Регистрация всех бывших handlers в одном файле.
    dp.include_router(start_router)
    dp.include_router(menu_router)
    dp.include_router(products_router)
    dp.include_router(payments_router)
    dp.include_router(referrals_router)
    dp.include_router(admin_router)

    await bot.delete_webhook(
        drop_pending_updates=True
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
