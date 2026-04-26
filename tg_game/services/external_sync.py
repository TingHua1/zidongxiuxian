import json
import time
from typing import Optional

from tg_game.clients.asc_client import (
    AscAuthError,
    AscNotFoundError,
    get_cultivator,
)
from tg_game.config import get_settings
from tg_game.storage import Storage


ASC_PROVIDER = "asc_aiopenai"


def normalize_external_cookie(raw_cookie: str) -> str:
    text = (raw_cookie or "").strip()
    if not text:
        return ""
    if text.lower().startswith("cookie:"):
        text = text.split(":", 1)[1].strip()
    parts = [
        part.strip() for part in text.replace("\n", ";").split(";") if part.strip()
    ]
    session_part = next(
        (part for part in parts if part.lower().startswith("session=")), ""
    )
    return session_part or text


def resolve_external_cookie(cookie_text: str, refreshed_cookie: str = "") -> str:
    candidate = normalize_external_cookie(refreshed_cookie or cookie_text)
    return (
        candidate
        if candidate.startswith("session=")
        else normalize_external_cookie(cookie_text)
    )


def get_effective_external_cookie(storage: Storage) -> str:
    settings = get_settings()
    authorized_user_id = str(settings.authorized_user_id or "").strip()
    if authorized_user_id:
        profile = storage.get_profile_by_telegram_user_id(authorized_user_id)
        if profile:
            external_account = (
                storage.get_external_account(profile.id, ASC_PROVIDER) or {}
            )
            cookie_text = normalize_external_cookie(
                external_account.get("cookie_text") or ""
            )
            if cookie_text.startswith("session="):
                return cookie_text
    override_cookie = storage.get_external_cookie_override()
    return normalize_external_cookie(override_cookie or "")


def is_authorized_profile(storage: Storage, profile) -> bool:
    if not profile:
        return False
    authorized_user_id = str(get_settings().authorized_user_id or "").strip()
    if not authorized_user_id:
        return False
    return (
        str(getattr(profile, "telegram_user_id", "") or "").strip()
        == authorized_user_id
    )


def get_external_keepalive_seconds() -> int:
    return max(int(get_settings().external_keepalive_seconds or 0), 60)


def get_external_keepalive_poll_seconds() -> int:
    return max(int(get_settings().external_keepalive_poll_seconds or 0), 5)


def get_external_account_status(external_account: Optional[dict]) -> str:
    return str((external_account or {}).get("status") or "").strip().lower()


def is_external_account_expired(external_account: Optional[dict]) -> bool:
    return get_external_account_status(external_account) in {
        "expired",
        "logged_out",
        "disconnected",
    }


def get_external_account_touch_time(external_account: Optional[dict]) -> float:
    account = external_account or {}
    return max(
        float(account.get("last_verified_at") or 0),
        float(account.get("updated_at") or 0),
    )


def should_keep_external_session_fresh(
    profile, external_account: Optional[dict]
) -> bool:
    if not profile or not getattr(profile, "telegram_verified_at", 0):
        return False
    status = get_external_account_status(external_account)
    if is_external_account_expired(external_account):
        return False
    if status in {"logged_out", "disconnected"}:
        return False
    if not external_account:
        return True
    if not str((external_account or {}).get("me_json") or "").strip():
        return True
    if status not in {"", "connected", "error"}:
        return True
    last_touch_at = get_external_account_touch_time(external_account)
    if not last_touch_at:
        return True
    return time.time() - last_touch_at >= get_external_keepalive_seconds()


def clear_external_cookie_override_if_matches(
    storage: Storage, cookie_text: str
) -> None:
    override_cookie = storage.get_external_cookie_override()
    if override_cookie is None:
        return
    if normalize_external_cookie(override_cookie) != normalize_external_cookie(
        cookie_text
    ):
        return
    storage.clear_external_cookie_override()


def get_cultivator_username(profile) -> str:
    return (
        (profile.telegram_username or profile.account_name.lstrip("@") or "")
        .strip()
        .lstrip("@")
    )


def get_cultivator_lookup_candidates(profile) -> list[str]:
    if not profile:
        return []
    candidates = []
    seen = set()
    for raw_value in [
        profile.telegram_username,
        profile.account_name,
        profile.game_name,
        profile.display_name,
    ]:
        candidate = str(raw_value or "").strip().lstrip("@")
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def fetch_cultivator_payload(cookie_text: str, profile) -> tuple[dict, str, str]:
    candidates = get_cultivator_lookup_candidates(profile)
    if not candidates:
        raise RuntimeError(
            "当前 Telegram 账号未绑定用户名或姓名，无法调用 /api/cultivator/<identifier>"
        )

    last_error: Optional[Exception] = None
    for candidate in candidates:
        try:
            payload, _status, refreshed_cookie = get_cultivator(candidate, cookie_text)
            return payload, candidate, refreshed_cookie
        except AscAuthError:
            raise
        except AscNotFoundError as exc:
            last_error = exc
            continue

    if last_error:
        raise last_error
    raise RuntimeError("调用 /api/cultivator 失败")


def read_cached_external_payload(
    storage: Storage, profile_id: int, provider: str = ASC_PROVIDER
) -> dict:
    external_account = storage.get_external_account(profile_id, provider) or {}
    try:
        payload = json.loads(external_account.get("me_json") or "{}")
    except json.JSONDecodeError:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def sync_external_account(
    storage: Storage,
    profile_id: int,
    *,
    cookie_text: str = "",
    provider: str = ASC_PROVIDER,
) -> dict:
    profile = storage.get_profile(profile_id)
    if not profile:
        raise RuntimeError("Profile not found")
    is_admin = is_authorized_profile(storage, profile)
    normalized_cookie = normalize_external_cookie(
        cookie_text or get_effective_external_cookie(storage)
    )
    if not normalized_cookie:
        raise RuntimeError("缺少天机阁登录 Cookie")
    if not normalized_cookie.startswith("session="):
        raise RuntimeError("只识别 session=... 形式的天机阁登录 Cookie")
    payload, resolved_identifier, refreshed_cookie = fetch_cultivator_payload(
        normalized_cookie, profile
    )
    persisted_cookie = resolve_external_cookie(normalized_cookie, refreshed_cookie)
    stored_cookie = persisted_cookie if is_admin else ""
    storage.upsert_external_account(
        profile_id=profile_id,
        provider=provider,
        telegram_user_id=str(profile.telegram_user_id or ""),
        telegram_username=(profile.telegram_username or resolved_identifier or ""),
        status="connected",
        cookie_text=stored_cookie,
        me_payload=payload,
    )
    if is_admin:
        storage.set_external_cookie_override(persisted_cookie)
    return payload if isinstance(payload, dict) else {}


def mark_external_account_failure(
    storage: Storage,
    profile_id: int,
    exc: Exception,
    *,
    provider: str = ASC_PROVIDER,
    cookie_text: str = "",
) -> None:
    profile = storage.get_profile(profile_id)
    storage.mark_external_account_error(
        profile_id,
        provider,
        str(exc),
        status="expired" if isinstance(exc, AscAuthError) else "error",
    )
    if (
        isinstance(exc, AscAuthError)
        and cookie_text
        and is_authorized_profile(storage, profile)
    ):
        clear_external_cookie_override_if_matches(storage, cookie_text)
