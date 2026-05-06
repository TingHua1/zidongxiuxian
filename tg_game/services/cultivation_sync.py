from datetime import datetime
from typing import Optional

import fanren_game
from tg_game.clients.asc_client import AscAuthError
from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_cultivator_lookup_candidates,
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
    storage: Storage,
    profile_id: int,
    chat_id: int,
    db=None,
    cultivator_payload: Optional[dict] = None,
) -> Optional[dict]:
    profile = storage.get_profile(profile_id)
    if not profile:
        return None
    external_account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
    default_cookie = get_effective_external_cookie(storage)
    cookie_text = (external_account.get("cookie_text") or default_cookie).strip()
    identifiers = get_cultivator_lookup_candidates(profile)
    if not cookie_text or not identifiers:
        return None
    try:
        cultivator = cultivator_payload or sync_external_account(
            storage, profile_id, cookie_text=cookie_text
        )
    except AscAuthError as exc:
        mark_external_account_failure(storage, profile_id, exc, cookie_text=cookie_text)
        raise
    now = fanren_game.time.time()
    cooldown_until = _parse_iso_timestamp(
        cultivator.get("cultivation_cooldown_until") or ""
    )
    deep_start = _parse_iso_timestamp(cultivator.get("deep_seclusion_start_time") or "")
    deep_end = _parse_iso_timestamp(cultivator.get("deep_seclusion_end_time") or "")

    status_event = "idle"
    status_summary = "未开始修炼"
    next_check_time = 0
    next_check_source = None

    runtime_db = db or fanren_game.RuntimeDb(storage)
    try:
        fanren_game.ensure_tables(runtime_db)
        session = fanren_game.get_session(runtime_db, chat_id, profile_id=profile_id)
        preserve_deep_next_check_time = 0.0
        preserve_deep_next_check_source = None
        if session:
            existing_next_check_time = float(session.get("next_check_time") or 0)
            existing_next_check_source = str(
                session.get("next_check_source") or ""
            ).strip()
            retreat_mode = str(session.get("retreat_mode") or "").strip().lower()
            if (
                retreat_mode == "deep"
                and existing_next_check_time > now
                and (
                    existing_next_check_source == "deep_seclusion_end_time"
                    or (session.get("last_event") or "")
                    in fanren_game.FANREN_DEEP_PENDING_EVENTS
                )
            ):
                preserve_deep_next_check_time = existing_next_check_time
                preserve_deep_next_check_source = (
                    existing_next_check_source or "deep_seclusion_end_time"
                )
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
            normal_unlock = cooldown_until
            if normal_unlock > now:
                status_event = "cultivating"
                status_summary = "闭关修炼中"
                next_check_time = normal_unlock
                next_check_source = "cultivation_cooldown_until"
        if preserve_deep_next_check_time and (
            next_check_time == 0 or next_check_source == "cultivation_cooldown_until"
        ):
            next_check_time = preserve_deep_next_check_time
            next_check_source = preserve_deep_next_check_source
            status_event = "deep_cultivating"
            status_summary = "深度闭关中"
        fanren_game.update_session(
            runtime_db,
            chat_id,
            profile_id=profile_id,
            last_event=status_event,
            last_summary=status_summary,
            next_check_time=next_check_time,
            next_check_source=next_check_source,
        )
        session = fanren_game.get_session(runtime_db, chat_id, profile_id=profile_id)
    finally:
        if db is None:
            runtime_db.close()
    return session
