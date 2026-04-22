from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from tg_game.config import get_settings


def _session_name(session_name: str = "") -> str:
    settings = get_settings()
    return (session_name or settings.telegram_session_name).strip()


def _session_candidates(session_name: str = "") -> list[str]:
    settings = get_settings()
    candidates = [
        (session_name or "").strip(),
        (settings.telegram_login_session_name or "").strip(),
        (settings.telegram_session_name or "").strip(),
    ]
    ordered = []
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


def build_client(session_name: str = "", save_entities: bool = True) -> TelegramClient:
    settings = get_settings()
    client = TelegramClient(
        _session_name(session_name),
        int(settings.telegram_api_id),
        settings.telegram_api_hash,
    )
    if hasattr(client, "session") and hasattr(client.session, "save_entities"):
        client.session.save_entities = bool(save_entities)
    return client


async def get_authorized_account_info(session_name: str = "") -> dict:
    for candidate in _session_candidates(session_name):
        client = build_client(candidate, save_entities=False)
        await client.connect()
        try:
            if not await client.is_user_authorized():
                continue
            me = await client.get_me()
            return {
                "id": getattr(me, "id", None),
                "username": getattr(me, "username", "") or "",
                "first_name": getattr(me, "first_name", "") or "",
                "last_name": getattr(me, "last_name", "") or "",
                "phone": getattr(me, "phone", "") or "",
                "session_name": candidate,
            }
        finally:
            await client.disconnect()
    raise RuntimeError("Telegram session is not authorized")


async def has_authorized_session(session_name: str = "") -> bool:
    for candidate in _session_candidates(session_name):
        client = build_client(candidate, save_entities=False)
        await client.connect()
        try:
            if await client.is_user_authorized():
                return True
        finally:
            await client.disconnect()
    return False


async def resolve_authorized_session_name(session_name: str = "") -> str:
    for candidate in _session_candidates(session_name):
        client = build_client(candidate, save_entities=False)
        await client.connect()
        try:
            if await client.is_user_authorized():
                return candidate
        finally:
            await client.disconnect()
    return _session_name(session_name)


async def send_login_code(phone: str, session_name: str = "") -> dict:
    client = build_client(session_name)
    await client.connect()
    try:
        sent = await client.send_code_request((phone or "").strip())
        return {
            "phone": (phone or "").strip(),
            "phone_code_hash": getattr(sent, "phone_code_hash", "") or "",
            "session_name": _session_name(session_name),
        }
    finally:
        await client.disconnect()


async def verify_login_code(
    phone: str, code: str, phone_code_hash: str, session_name: str = ""
) -> dict:
    client = build_client(session_name)
    await client.connect()
    try:
        try:
            me = await client.sign_in(
                phone=(phone or "").strip(),
                code=(code or "").strip(),
                phone_code_hash=(phone_code_hash or "").strip(),
            )
        except SessionPasswordNeededError:
            return {
                "requires_password": True,
                "session_name": _session_name(session_name),
            }
        return {
            "requires_password": False,
            "account": {
                "id": getattr(me, "id", None),
                "username": getattr(me, "username", "") or "",
                "first_name": getattr(me, "first_name", "") or "",
                "last_name": getattr(me, "last_name", "") or "",
                "phone": getattr(me, "phone", "") or "",
                "session_name": _session_name(session_name),
            },
        }
    finally:
        await client.disconnect()


async def verify_login_password(password: str, session_name: str = "") -> dict:
    client = build_client(session_name)
    await client.connect()
    try:
        me = await client.sign_in(password=(password or "").strip())
        return {
            "id": getattr(me, "id", None),
            "username": getattr(me, "username", "") or "",
            "first_name": getattr(me, "first_name", "") or "",
            "last_name": getattr(me, "last_name", "") or "",
            "phone": getattr(me, "phone", "") or "",
            "session_name": _session_name(session_name),
        }
    finally:
        await client.disconnect()


async def logout_account(session_name: str = "") -> None:
    client = build_client(session_name)
    await client.connect()
    try:
        if await client.is_user_authorized():
            await client.log_out()
    finally:
        await client.disconnect()
