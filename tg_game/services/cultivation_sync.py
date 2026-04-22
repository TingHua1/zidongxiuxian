from datetime import datetime
from typing import Optional

import fanren_game
from tg_game.clients.asc_client import AscAuthError
from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_cultivator_username,
    get_effective_external_cookie,
    mark_external_account_failure,
    sync_external_account,
)
from tg_game.storage import Storage


def _parse_iso_timestamp(value: str) -> float:
    text = (value or "").strip()
    if not text:
        return 0
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0


def sync_cultivation_session(
    storage: Storage, profile_id: int, chat_id: int, db=None
) -> Optional[dict]:
    profile = storage.get_profile(profile_id)
    if not profile:
        return None
    external_account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
    default_cookie = get_effective_external_cookie(storage)
    cookie_text = (external_account.get("cookie_text") or default_cookie).strip()
    username = get_cultivator_username(profile)
    if not cookie_text or not username:
        return None
    try:
        cultivator = sync_external_account(storage, profile_id, cookie_text=cookie_text)
    except AscAuthError as exc:
        mark_external_account_failure(storage, profile_id, exc, cookie_text=cookie_text)
        raise
    now = fanren_game.time.time()
    cooldown_until = _parse_iso_timestamp(
        cultivator.get("cultivation_cooldown_until") or ""
    )
    meditation_end = _parse_iso_timestamp(cultivator.get("meditation_end_time") or "")
    deep_start = _parse_iso_timestamp(cultivator.get("deep_seclusion_start_time") or "")
    deep_end = _parse_iso_timestamp(cultivator.get("deep_seclusion_end_time") or "")

    status_event = "idle"
    status_summary = "未开始修炼"
    next_check_time = 0
    next_check_source = None

    runtime_db = db or fanren_game.RuntimeDb(storage)
    try:
        fanren_game.ensure_tables(runtime_db)
        session = fanren_game.get_session(runtime_db, chat_id)
        if (
            deep_start
            and deep_end
            and deep_end <= now
            and fanren_game.has_pending_deep_settlement(session)
        ):
            status_event = "deep_settlement_due"
            status_summary = "深度闭关已到时，等待发送检查消息触发结算"
            next_check_time = now
            next_check_source = "deep_seclusion_end_time 已到，需触发深度结算"
        elif deep_start and deep_end and deep_end > now:
            status_event = "deep_cultivating"
            status_summary = "深度闭关中"
            next_check_time = deep_end
            next_check_source = "deep_seclusion_end_time"
        else:
            normal_unlock = max(cooldown_until, meditation_end)
            if normal_unlock > now:
                status_event = "cultivating"
                status_summary = "闭关修炼中"
                next_check_time = normal_unlock
                next_check_source = (
                    "meditation_end_time"
                    if meditation_end >= cooldown_until
                    else "cultivation_cooldown_until"
                )
        fanren_game.update_session(
            runtime_db,
            chat_id,
            last_event=status_event,
            last_summary=status_summary,
            next_check_time=next_check_time,
            next_check_source=next_check_source,
        )
        session = fanren_game.get_session(runtime_db, chat_id)
    finally:
        if db is None:
            runtime_db.close()
    return session
