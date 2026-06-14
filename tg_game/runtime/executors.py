import asyncio
import json
import logging
import re
import secrets
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Optional

import artifact_game
import basic_game
import battle_feature_game
import breakthrough_game
import companion_game
import diplomacy_game
import dungeon_feature_game
import estate_game
import fanren_game
import market_trade_game
import sect_game
import shop_game
import stock_trade_game
import inventory_feature_game

from tg_game.runtime.context import EventContext
from tg_game.services.cultivation_sync import sync_cultivation_session
from tg_game.services.external_sync import read_cached_external_payload
from tg_game.storage import CompatDb as SQLiteCompatDb, Storage
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)

DIVINATION_COMMAND = ".卜筮问天"
DIVINATION_BATCH_COMMAND_INTERVAL_SECONDS = 60
DIVINATION_BATCH_POLL_SECONDS = 5
FANREN_RECENT_REPLY_WINDOW_SECONDS = 30
COMPANION_AUTO_POLL_SECONDS = 5
COMPANION_HEART_TRIBULATION_EMPTY_SLEEP_SECONDS = 5
COMPANION_HEART_TRIBULATION_ACTIVE_POLL_SECONDS = 2
COMPANION_HEART_TRIBULATION_IDLE_SLEEP_MAX_SECONDS = 60
COMPANION_AUTO_POST_SEND_GRACE_SECONDS = 1800
COMPANION_PANEL_COMMAND = ".我的侍妾"
COMPANION_HEART_TRIBULATION_COMMAND = ".共历心劫"
COMPANION_HEART_TRIBULATION_ALLOWED_BOT_IDS = set()
COMPANION_HEART_TRIBULATION_STEP_TIMEOUT_SECONDS = 300
COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS = 600
COMPANION_HEART_TRIBULATION_SETTLEMENT_KEYWORD = "【坠魔心劫·结算】"
COMPANION_HEART_TRIBULATION_ROUND1_LOCK_KEYWORD = "【坠魔心劫·第1轮已定】"
COMPANION_HEART_TRIBULATION_ROUND2_LOCK_KEYWORD = "【坠魔心劫·第2轮已定】"
COMPANION_HEART_TRIBULATION_IDLE_STATE = "idle"
COMPANION_HEART_TRIBULATION_SENDING_PANEL_STATE = "sending_panel_command"
COMPANION_HEART_TRIBULATION_AWAIT_PANEL_STATE = "await_panel_reply"
COMPANION_HEART_TRIBULATION_AWAIT_TRIBULATION_STATE = "await_tribulation_reply"
COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE = "await_round1_edit"
COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE = "await_round2_edit"
COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE = "await_settlement_edit"
COMPANION_HEART_TRIBULATION_FAILED_STATE = "failed_stopped"
COMPANION_HEART_TRIBULATION_ROUND_RETRY_SECONDS = 20
COMPANION_HEART_TRIBULATION_ROUND_RETRY_MAX = 1
COMPANION_HEART_TRIBULATION_ACTIVE_STATES = {
    COMPANION_HEART_TRIBULATION_SENDING_PANEL_STATE,
    COMPANION_HEART_TRIBULATION_AWAIT_PANEL_STATE,
    COMPANION_HEART_TRIBULATION_AWAIT_TRIBULATION_STATE,
    COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE,
    COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE,
    COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE,
}
COMPANION_AUTO_FEATURES = {
    "dream_seek": {
        "command": ".入梦寻图",
        "payload_field": "last_dream_map_seek_time",
        "cooldown_hours": 8,
        "payload_scope": "companion",
    },
    "divination_chain": {
        "command": ".天机代卜",
        "payload_field": "last_divination_chain_time",
        "cooldown_hours": 12,
        "payload_scope": "companion",
    },
    "wild_experience": {
        "command": ".野外历练",
        "payload_field": "last_wild_experience_time",
        "cooldown_hours": 2,
        "payload_scope": "root",
    },
}
WILD_EXPERIENCE_ALLOWED_STRATEGIES = {"谨慎", "均衡", "深入"}


def _refresh_companion_payload(storage: Storage, profile_id: int):
    from tg_game.clients.asc_client import AscAuthError
    from tg_game.services.external_sync import (
        ASC_PROVIDER,
        get_effective_external_cookie,
        mark_external_account_failure,
        sync_external_account,
    )

    external_account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
    cookie_text = (
        (external_account or {}).get("cookie_text") or get_effective_external_cookie(storage)
    ).strip()
    if not cookie_text:
        logger.warning(
            "Force refresh companion payload skipped profile=%s reason=no_cookie",
            profile_id,
        )
        return None

    try:
        return sync_external_account(storage, profile_id, cookie_text=cookie_text)
    except AscAuthError as exc:
        mark_external_account_failure(
            storage, profile_id, exc, cookie_text=cookie_text
        )
        logger.warning(
            "Force refresh companion payload auth failed profile=%s error=%s",
            profile_id,
            exc,
        )
        return None
    except Exception as exc:
        logger.warning(
            "Force refresh companion payload failed profile=%s error=%s",
            profile_id,
            exc,
        )
        return None


def _refresh_divination_payload(storage: Storage, profile_id: int):
    from tg_game.clients.asc_client import AscAuthError
    from tg_game.services.external_sync import (
        ASC_PROVIDER,
        get_effective_external_cookie,
        mark_external_account_failure,
        sync_external_account,
    )

    external_account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
    cookie_text = (
        (external_account or {}).get("cookie_text") or get_effective_external_cookie(storage)
    ).strip()
    if not cookie_text:
        logger.warning(
            "Force refresh divination payload skipped profile=%s reason=no_cookie",
            profile_id,
        )
        return None

    try:
        return sync_external_account(storage, profile_id, cookie_text=cookie_text)
    except AscAuthError as exc:
        mark_external_account_failure(
            storage, profile_id, exc, cookie_text=cookie_text
        )
        logger.warning(
            "Force refresh divination payload auth failed profile=%s error=%s",
            profile_id,
            exc,
        )
        return None
    except Exception as exc:
        logger.warning(
            "Force refresh divination payload failed profile=%s error=%s",
            profile_id,
            exc,
        )
        return None


def _binding_bot_ids(context: EventContext) -> list[int]:
    bot_ids = list(getattr(context.chat_binding, "bot_ids", None) or [])
    primary_bot_id = getattr(context.chat_binding, "bot_id", None)
    try:
        normalized_primary = int(primary_bot_id) if primary_bot_id is not None else None
    except (TypeError, ValueError):
        normalized_primary = None
    if not bot_ids and normalized_primary is not None and normalized_primary not in bot_ids:
        bot_ids = [normalized_primary, *bot_ids]
    deduped = []
    for bot_id in bot_ids:
        try:
            normalized = int(bot_id)
        except (TypeError, ValueError):
            continue
        if normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _is_context_sender_allowed_bot(context: EventContext) -> bool:
    try:
        return int(context.sender_id or 0) in _binding_bot_ids(context)
    except (TypeError, ValueError):
        return False


def _is_edited_event(context: EventContext) -> bool:
    if context.is_outgoing:
        return False
    if getattr(context.event, "edit_date", None):
        return True
    message = getattr(context.event, "message", None)
    if message is not None and getattr(message, "edit_date", None):
        return True
    event_type = type(context.event).__name__.lower()
    return "edited" in event_type


def _has_pending_outgoing_command(
    storage: Storage,
    *,
    profile_id: int,
    chat_id: int,
    text: str,
    thread_id: Optional[int],
) -> bool:
    latest_command = storage.get_latest_outgoing_command(
        chat_id,
        profile_id=profile_id,
        text=text,
        thread_id=thread_id,
    )
    if not latest_command:
        return False
    return str(latest_command.get("status") or "").strip() in {"pending", "sending"}


def _queue_companion_command(
    storage: Storage,
    *,
    profile_id: int,
    chat_id: int,
    text: str,
    thread_id: Optional[int],
    chat_type: str,
    bot_username: str,
    reply_to_msg_id: Optional[int] = None,
) -> None:
    storage.enqueue_outgoing_command(
        profile_id=profile_id,
        chat_id=chat_id,
        text=text,
        thread_id=thread_id,
        reply_to_msg_id=reply_to_msg_id,
        chat_type=chat_type,
        bot_username=bot_username,
    )


def _is_allowed_companion_heart_tribulation_bot_id(sender_id: object) -> bool:
    return False


def _normalize_companion_heart_tribulation_action(value: object) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in {"稳", "狠", "骗"} else "稳"


def _resolve_companion_heart_tribulation_next_run_at(payload: dict) -> Optional[float]:
    companion_payload = payload.get("companion") or {}
    if not isinstance(companion_payload, dict):
        companion_payload = {}
    dongfu = payload.get("dongfu") or {}
    if isinstance(dongfu, str):
        try:
            dongfu = json.loads(dongfu)
        except Exception:
            dongfu = {}
    companion_residence = {}
    if isinstance(dongfu, dict):
        companion_residence = dongfu.get("companion_residence") or {}
        if isinstance(companion_residence, str):
            try:
                companion_residence = json.loads(companion_residence)
            except Exception:
                companion_residence = {}
    if not isinstance(companion_residence, dict):
        companion_residence = {}
    raw_value = companion_payload.get("last_companion_heart_tribulation_time")
    if raw_value is None:
        raw_value = companion_residence.get("last_companion_heart_tribulation_time")
    last_ts = _parse_iso_to_ts(raw_value)
    if last_ts <= 0:
        return None
    return last_ts + 10 * 3600


def _build_companion_heart_tribulation_action_command(task: dict, round_number: int) -> str:
    normalized_round = max(int(round_number or 1), 1)
    if normalized_round <= 1:
        action = _normalize_companion_heart_tribulation_action(task.get("round1_reply"))
    elif normalized_round == 2:
        action = _normalize_companion_heart_tribulation_action(task.get("round2_reply"))
    else:
        action = _normalize_companion_heart_tribulation_action(task.get("round3_reply"))
    return f".{action}"


def _build_companion_heart_tribulation_event_fingerprint(
    *,
    message_id: int,
    text: str,
    event_kind: str,
) -> str:
    return f"{event_kind}:{int(message_id or 0)}:{str(text or '').strip()[:900]}"


def _append_companion_heart_tribulation_log(
    storage: Storage,
    task: dict,
    *,
    step: str,
    event_type: str,
    message_id: int = 0,
    reply_to_msg_id: int = 0,
    sender_id: int = 0,
    sender_username: str = "",
    text: str = "",
    detail: Optional[dict] = None,
) -> None:
    storage.append_companion_heart_tribulation_log(
        profile_id=int(task.get("profile_id") or 0),
        chat_id=int(task.get("chat_id") or 0),
        thread_id=int(task.get("thread_id")) if task.get("thread_id") else None,
        task_id=int(task.get("id") or 0),
        run_id=str(task.get("run_id") or ""),
        step=step,
        event_type=event_type,
        message_id=int(message_id or 0),
        reply_to_msg_id=int(reply_to_msg_id or 0),
        sender_id=int(sender_id or 0),
        sender_username=sender_username,
        text=text,
        detail=detail or {},
    )


def _stop_companion_heart_tribulation_task(
    storage: Storage,
    task: dict,
    *,
    last_error: str,
    step: str,
    detail: Optional[dict] = None,
) -> Optional[dict]:
    _append_companion_heart_tribulation_log(
        storage,
        task,
        step=step,
        event_type="failed_stop",
        text=last_error,
        detail=detail or {},
    )
    profile_id = int(task.get("profile_id") or 0)
    chat_id = int(task.get("chat_id") or 0)
    if profile_id and chat_id:
        task_thread_id = int(task.get("thread_id")) if task.get("thread_id") else None
        for command_text in [
            COMPANION_PANEL_COMMAND,
            COMPANION_HEART_TRIBULATION_COMMAND,
            ".稳",
            ".狠",
            ".骗",
        ]:
            storage.cancel_pending_outgoing_commands(
                profile_id,
                chat_id,
                text=command_text,
                thread_id=task_thread_id,
                require_exact_thread=True,
            )
    updated_task = storage.disable_companion_heart_tribulation_task(
        profile_id,
        chat_id,
        thread_id=int(task.get("thread_id")) if task.get("thread_id") else None,
        last_error=last_error,
    )
    if updated_task:
        storage.update_companion_heart_tribulation_task(
            int(updated_task.get("id") or 0),
            workflow_state=COMPANION_HEART_TRIBULATION_FAILED_STATE,
        )
        return storage.get_companion_heart_tribulation_task(
            profile_id,
            chat_id,
            thread_id=int(task.get("thread_id")) if task.get("thread_id") else None,
        )
    return updated_task


async def _send_companion_heart_tribulation_command(
    client: object,
    storage: Storage,
    task: dict,
    *,
    text: str,
    reply_to_msg_id: Optional[int] = None,
) -> object:
    chat_id = int(task.get("chat_id") or 0)
    if not chat_id:
        raise RuntimeError("Heart tribulation chat_id missing")
    return await send_message_with_thread_fallback(
        client,
        chat_id,
        text,
        thread_id=int(task.get("thread_id")) if task.get("thread_id") else None,
        storage=storage,
        profile_id=int(task.get("profile_id") or 0),
        bot_username=str(task.get("bot_username") or ""),
        log_prefix="Heart tribulation",
    ) if reply_to_msg_id is None else await client.send_message(
        chat_id,
        text,
        reply_to=int(reply_to_msg_id),
    )


async def _poll_companion_heart_tribulation_message(
    client: object,
    storage: Storage,
    task: dict,
) -> bool:
    workflow_state = str(task.get("workflow_state") or "").strip()
    if workflow_state not in {
        COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE,
        COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE,
        COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE,
    }:
        return False
    task_id = int(task.get("id") or 0)
    profile_id = int(task.get("profile_id") or 0)
    chat_id = int(task.get("chat_id") or 0)
    thread_id = int(task.get("thread_id")) if task.get("thread_id") else None
    tribulation_msg_id = int(task.get("tribulation_msg_id") or 0)
    if not task_id or not profile_id or not chat_id or tribulation_msg_id <= 0:
        return False

    try:
        message = await client.get_messages(chat_id, ids=tribulation_msg_id)
    except Exception as exc:
        _append_companion_heart_tribulation_log(
            storage,
            task,
            step=workflow_state,
            event_type="poll_message_failed",
            message_id=tribulation_msg_id,
            detail={"error": str(exc)},
        )
        return False
    if not message:
        return False
    current_text = (
        getattr(message, "raw_text", "") or getattr(message, "text", "") or ""
    ).strip()
    if not current_text:
        return False

    last_fingerprint = str(task.get("last_progress_fingerprint") or "")
    if any(
        _build_companion_heart_tribulation_event_fingerprint(
            message_id=tribulation_msg_id,
            text=current_text,
            event_kind=kind,
        )
        == last_fingerprint
        for kind in {"tribulation_reply", "edited", "polled_edit"}
    ):
        return False

    sender_id = int(getattr(message, "sender_id", None) or task.get("matched_bot_id") or 0)
    if sender_id:
        allowed_bot_ids = storage.get_chat_binding_bot_ids(
            profile_id, chat_id, thread_id=thread_id
        )
        if sender_id not in allowed_bot_ids:
            return False
    sender_username = ""
    try:
        sender = await message.get_sender()
        sender_username = (getattr(sender, "username", "") or "").strip()
    except Exception:
        existing_message = storage.get_bound_message(
            chat_id, tribulation_msg_id, profile_id=profile_id
        )
        sender_username = str((existing_message or {}).get("sender_username") or "")

    current_fingerprint = _build_companion_heart_tribulation_event_fingerprint(
        message_id=tribulation_msg_id,
        text=current_text,
        event_kind="polled_edit",
    )
    storage.upsert_bound_message(
        profile_id=profile_id,
        chat_id=chat_id,
        thread_id=thread_id,
        message_id=tribulation_msg_id,
        reply_to_msg_id=int(task.get("tribulation_command_msg_id") or 0),
        sender_id=sender_id,
        sender_username=sender_username,
        direction="incoming",
        is_bot=True,
        text=current_text,
    )
    _append_companion_heart_tribulation_log(
        storage,
        task,
        step=workflow_state,
        event_type="message_polled_edited",
        message_id=tribulation_msg_id,
        reply_to_msg_id=int(task.get("tribulation_command_msg_id") or 0),
        sender_id=sender_id,
        sender_username=sender_username,
        text=current_text,
    )
    storage.update_companion_heart_tribulation_task(
        task_id,
        step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
        last_progress_at=time.time(),
        last_progress_fingerprint=current_fingerprint,
    )
    task = storage.get_companion_heart_tribulation_task(
        profile_id,
        chat_id,
        thread_id=thread_id,
    ) or task

    if COMPANION_HEART_TRIBULATION_SETTLEMENT_KEYWORD in current_text:
        previous_settlement_text = str(task.get("last_settlement_text") or "")
        previous_settlement_at = float(task.get("last_settlement_at") or 0)
        updated_task = storage.update_companion_heart_tribulation_task(
            task_id,
            workflow_state=COMPANION_HEART_TRIBULATION_IDLE_STATE,
            step_deadline_at=0,
            matched_bot_id=0,
            anchor_command_msg_id=0,
            anchor_bot_msg_id=0,
            tribulation_command_msg_id=0,
            tribulation_msg_id=0,
            panel_reply_msg_id=0,
            last_action_round_sent=0,
            last_tribulation_command_at=0,
            last_progress_at=time.time(),
            last_progress_fingerprint=current_fingerprint,
            last_stable_sent_at=0,
            last_settlement_text=current_text,
            last_settlement_at=time.time(),
            previous_settlement_text=previous_settlement_text,
            previous_settlement_at=previous_settlement_at,
            last_error="",
        )
        _append_companion_heart_tribulation_log(
            storage,
            updated_task or task,
            step="completed",
            event_type="settlement_recorded",
            message_id=tribulation_msg_id,
            sender_id=sender_id,
            sender_username=sender_username,
            text=current_text,
            detail={"source": "poll"},
        )
        return True

    if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE:
        if COMPANION_HEART_TRIBULATION_ROUND1_LOCK_KEYWORD not in current_text:
            return True
        next_state = COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE
        next_round = 2
    elif workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE:
        if COMPANION_HEART_TRIBULATION_ROUND2_LOCK_KEYWORD not in current_text:
            return True
        next_state = COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE
        next_round = 3
    else:
        return True

    command = _build_companion_heart_tribulation_action_command(task, next_round)
    try:
        action_message = await _send_companion_heart_tribulation_command(
            client,
            storage,
            task,
            text=command,
            reply_to_msg_id=tribulation_msg_id,
        )
    except Exception as exc:
        _stop_companion_heart_tribulation_task(
            storage,
            task,
            last_error=f"发送第{next_round}轮心劫策略失败，已停止自动共历心劫。",
            step=f"send_round{next_round}",
            detail={"error": str(exc), "command": command, "source": "poll"},
        )
        return True

    storage.update_companion_heart_tribulation_task(
        task_id,
        workflow_state=next_state,
        step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
        last_action_round_sent=next_round,
        last_progress_at=time.time(),
        last_progress_fingerprint=current_fingerprint,
        last_stable_sent_at=time.time(),
        round_retry_count=0,
        round_retry_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_ROUND_RETRY_SECONDS,
        last_error="",
    )
    updated_task = storage.get_companion_heart_tribulation_task(
        profile_id,
        chat_id,
        thread_id=thread_id,
    ) or task
    _append_companion_heart_tribulation_log(
        storage,
        updated_task,
        step=next_state,
        event_type=f"send_round{next_round}",
        message_id=int(getattr(action_message, "id", 0) or 0),
        reply_to_msg_id=tribulation_msg_id,
        text=command,
        detail={"source": "poll"},
    )
    return True


async def _run_companion_heart_tribulation_scheduler(
    client: object, storage: Storage
) -> None:
    profile_id = getattr(client, "_tg_game_profile_id", None)
    if not profile_id:
        return

    while True:
        try:
            tasks = storage.list_active_companion_heart_tribulation_tasks(int(profile_id))
            now = time.time()
            if not tasks:
                await asyncio.sleep(COMPANION_HEART_TRIBULATION_EMPTY_SLEEP_SECONDS)
                continue

            has_active_workflow = False
            earliest_idle_next_run_at: Optional[float] = None

            for task in tasks:
                task_id = int(task.get("id") or 0)
                if not task_id:
                    continue
                workflow_state = str(task.get("workflow_state") or "").strip()
                next_run_at = float(task.get("next_run_at") or 0)
                step_deadline_at = float(task.get("step_deadline_at") or 0)

                if workflow_state == COMPANION_HEART_TRIBULATION_FAILED_STATE:
                    continue

                if workflow_state in COMPANION_HEART_TRIBULATION_ACTIVE_STATES:
                    has_active_workflow = True

                if workflow_state in {
                    COMPANION_HEART_TRIBULATION_SENDING_PANEL_STATE,
                    COMPANION_HEART_TRIBULATION_AWAIT_PANEL_STATE,
                    COMPANION_HEART_TRIBULATION_AWAIT_TRIBULATION_STATE,
                }:
                    if step_deadline_at > 0 and now >= step_deadline_at:
                        _stop_companion_heart_tribulation_task(
                            storage,
                            task,
                            last_error="自动共历心劫等待超时，已停止自动。",
                            step=workflow_state,
                            detail={
                                "step_deadline_at": step_deadline_at,
                                "now": now,
                            },
                        )
                    continue

                if workflow_state in {
                    COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE,
                    COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE,
                    COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE,
                }:
                    if await _poll_companion_heart_tribulation_message(client, storage, task):
                        continue
                    round_retry_deadline_at = float(task.get("round_retry_deadline_at") or 0)
                    round_retry_count = int(task.get("round_retry_count") or 0)
                    if round_retry_deadline_at > 0 and now >= round_retry_deadline_at:
                        if round_retry_count < COMPANION_HEART_TRIBULATION_ROUND_RETRY_MAX:
                            round_map = {
                                COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE: 1,
                                COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE: 2,
                                COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE: 3,
                            }
                            round_num = round_map.get(workflow_state, 0)
                            if round_num <= 0:
                                _stop_companion_heart_tribulation_task(
                                    storage,
                                    task,
                                    last_error="自动共历心劫重试时无法确定轮次，已停止自动。",
                                    step=workflow_state,
                                )
                                continue
                            command = _build_companion_heart_tribulation_action_command(task, round_num)
                            tribulation_msg_id = int(task.get("tribulation_msg_id") or 0)
                            if tribulation_msg_id <= 0:
                                _stop_companion_heart_tribulation_task(
                                    storage,
                                    task,
                                    last_error="自动共历心劫重试时缺少心劫消息锚点，已停止自动。",
                                    step=workflow_state,
                                )
                                continue
                            try:
                                await _send_companion_heart_tribulation_command(
                                    client,
                                    storage,
                                    task,
                                    text=command,
                                    reply_to_msg_id=tribulation_msg_id,
                                )
                            except Exception as exc:
                                _stop_companion_heart_tribulation_task(
                                    storage,
                                    task,
                                    last_error=f"自动共历心劫重试发送第{round_num}轮策略失败，已停止自动。",
                                    step=workflow_state,
                                    detail={"error": str(exc), "round": round_num},
                                )
                                continue
                            new_retry_count = round_retry_count + 1
                            storage.update_companion_heart_tribulation_task(
                                task_id,
                                round_retry_count=new_retry_count,
                                round_retry_deadline_at=now + COMPANION_HEART_TRIBULATION_ROUND_RETRY_SECONDS,
                                step_deadline_at=now + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
                                last_error="",
                            )
                            _append_companion_heart_tribulation_log(
                                storage,
                                task,
                                step=workflow_state,
                                event_type="round_retry_sent",
                                text=command,
                                detail={
                                    "round": round_num,
                                    "retry_count": new_retry_count,
                                    "tribulation_msg_id": tribulation_msg_id,
                                },
                            )
                        else:
                            tribulation_msg_id = int(task.get("tribulation_msg_id") or 0)
                            matched_bot_id = int(task.get("matched_bot_id") or 0)
                            _stop_companion_heart_tribulation_task(
                                storage,
                                task,
                                last_error="自动共历心劫轮次编辑重试耗尽，已停止自动。",
                                step=workflow_state,
                                detail={
                                    "step_deadline_at": step_deadline_at,
                                    "now": now,
                                    "tribulation_msg_id": tribulation_msg_id,
                                    "matched_bot_id": matched_bot_id,
                                    "round_retry_count": round_retry_count,
                                    "workflow_state": workflow_state,
                                },
                            )
                        continue
                    if step_deadline_at > 0 and now >= step_deadline_at:
                        tribulation_msg_id = int(task.get("tribulation_msg_id") or 0)
                        matched_bot_id = int(task.get("matched_bot_id") or 0)
                        _stop_companion_heart_tribulation_task(
                            storage,
                            task,
                            last_error="自动共历心劫等待超时，已停止自动。",
                            step=workflow_state,
                            detail={
                                "step_deadline_at": step_deadline_at,
                                "now": now,
                                "tribulation_msg_id": tribulation_msg_id,
                                "matched_bot_id": matched_bot_id,
                                "round_retry_count": round_retry_count,
                                "workflow_state": workflow_state,
                            },
                        )
                    continue

                if workflow_state not in {"", COMPANION_HEART_TRIBULATION_IDLE_STATE}:
                    continue

                if next_run_at > now:
                    if earliest_idle_next_run_at is None or next_run_at < earliest_idle_next_run_at:
                        earliest_idle_next_run_at = next_run_at
                    continue

                has_active_workflow = True
                fresh_payload = await asyncio.to_thread(
                    _refresh_companion_payload, storage, int(profile_id)
                )
                if not fresh_payload or not isinstance(fresh_payload, dict):
                    _stop_companion_heart_tribulation_task(
                        storage,
                        task,
                        last_error="刷新侍妾冷却失败，已停止自动共历心劫。",
                        step="refresh_payload",
                    )
                    continue

                resolved_next_run_at = _resolve_companion_heart_tribulation_next_run_at(
                    fresh_payload
                )
                if resolved_next_run_at is None:
                    _stop_companion_heart_tribulation_task(
                        storage,
                        task,
                        last_error="最新侍妾信息缺少共历心劫冷却字段，已停止自动。",
                        step="resolve_cooldown",
                    )
                    continue
                if resolved_next_run_at > now:
                    storage.update_companion_heart_tribulation_task(
                        task_id,
                        workflow_state=COMPANION_HEART_TRIBULATION_IDLE_STATE,
                        next_run_at=resolved_next_run_at,
                        step_deadline_at=0,
                        last_error="",
                    )
                    if earliest_idle_next_run_at is None or resolved_next_run_at < earliest_idle_next_run_at:
                        earliest_idle_next_run_at = resolved_next_run_at
                    continue

                run_id = secrets.token_hex(8)
                updated_task = storage.update_companion_heart_tribulation_task(
                    task_id,
                    enabled=1,
                    run_id=run_id,
                    workflow_state=COMPANION_HEART_TRIBULATION_SENDING_PANEL_STATE,
                    next_run_at=0,
                    step_deadline_at=now + COMPANION_HEART_TRIBULATION_STEP_TIMEOUT_SECONDS,
                    last_run_at=now,
                    matched_bot_id=0,
                    anchor_command_msg_id=0,
                    anchor_bot_msg_id=0,
                    tribulation_command_msg_id=0,
                    tribulation_msg_id=0,
                    panel_reply_msg_id=0,
                    last_action_round_sent=0,
                    last_tribulation_command_at=0,
                    last_progress_at=0,
                    last_progress_fingerprint="",
                    last_stable_sent_at=0,
                    last_error="",
                    retry_count=0,
                )
                if not updated_task:
                    continue
                task = updated_task
                _append_companion_heart_tribulation_log(
                    storage,
                    task,
                    step="launch",
                    event_type="cooldown_ready",
                    detail={"resolved_next_run_at": resolved_next_run_at},
                )
                try:
                    command_message = await _send_companion_heart_tribulation_command(
                        client,
                        storage,
                        task,
                        text=COMPANION_PANEL_COMMAND,
                    )
                except Exception as exc:
                    _stop_companion_heart_tribulation_task(
                        storage,
                        task,
                        last_error=f"发送{COMPANION_PANEL_COMMAND}失败，已停止自动共历心劫。",
                        step="send_panel_command",
                        detail={"error": str(exc)},
                    )
                    continue
                storage.update_companion_heart_tribulation_task(
                    task_id,
                    workflow_state=COMPANION_HEART_TRIBULATION_AWAIT_PANEL_STATE,
                    anchor_command_msg_id=int(getattr(command_message, "id", 0) or 0),
                    step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_STEP_TIMEOUT_SECONDS,
                    last_run_at=time.time(),
                )
                task = storage.get_companion_heart_tribulation_task(
                    int(profile_id),
                    int(task.get("chat_id") or 0),
                    thread_id=int(task.get("thread_id")) if task.get("thread_id") else None,
                ) or task
                _append_companion_heart_tribulation_log(
                    storage,
                    task,
                    step=COMPANION_HEART_TRIBULATION_AWAIT_PANEL_STATE,
                    event_type="send_panel_command",
                    message_id=int(getattr(command_message, "id", 0) or 0),
                    text=COMPANION_PANEL_COMMAND,
                    detail={"run_id": run_id},
                )
            if has_active_workflow:
                sleep_seconds = COMPANION_HEART_TRIBULATION_ACTIVE_POLL_SECONDS
            elif earliest_idle_next_run_at is not None:
                sleep_seconds = min(
                    COMPANION_HEART_TRIBULATION_IDLE_SLEEP_MAX_SECONDS,
                    max(1.0, earliest_idle_next_run_at - time.time()),
                )
            else:
                sleep_seconds = COMPANION_HEART_TRIBULATION_EMPTY_SLEEP_SECONDS
            await asyncio.sleep(sleep_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Companion heart tribulation scheduler error for profile=%s: %s",
                profile_id,
                exc,
            )
            await asyncio.sleep(10)


def _register_client_background_task(
    client: object, task: asyncio.Task
) -> asyncio.Task:
    tasks = getattr(client, "_tg_game_background_tasks", None)
    if tasks is None:
        tasks = set()
        setattr(client, "_tg_game_background_tasks", tasks)

    tasks.add(task)

    def _discard_done(done_task: asyncio.Task) -> None:
        current_tasks = getattr(client, "_tg_game_background_tasks", None)
        if current_tasks is not None:
            current_tasks.discard(done_task)

    task.add_done_callback(_discard_done)
    return task


def _get_divination_today_count_from_payload(payload: dict) -> int:
    last_divination_text = str(payload.get("last_divination_date") or "").strip()
    last_divination_ts = sect_game._parse_iso_timestamp(last_divination_text)
    last_divination_day = ""
    if last_divination_ts:
        last_divination_day = time.strftime(
            "%Y-%m-%d", time.localtime(last_divination_ts)
        )
    elif last_divination_text:
        last_divination_day = last_divination_text[:10]
    today_text = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    raw_today_count = max(int(payload.get("divination_count_today") or 0), 0)
    return raw_today_count if last_divination_day == today_text else 0


def _get_cached_divination_today_count(storage: Storage, profile_id: int) -> int:
    payload = read_cached_external_payload(storage, profile_id)
    return _get_divination_today_count_from_payload(payload)


def _parse_iso_to_ts(raw_value: object) -> float:
    text = str(raw_value or "").strip()
    if not text:
        return 0.0
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _resolve_companion_next_run_at(payload: dict, feature_key: str) -> Optional[float]:
    feature = COMPANION_AUTO_FEATURES.get(feature_key) or {}
    payload_field = str(feature.get("payload_field") or "").strip()
    cooldown_hours = int(feature.get("cooldown_hours") or 0)
    payload_scope = str(feature.get("payload_scope") or "companion").strip()
    if cooldown_hours <= 0 or not payload_field:
        return None
    if payload_scope == "root":
        last_ts = _parse_iso_to_ts(payload.get(payload_field))
        if last_ts <= 0:
            return None
        return last_ts + cooldown_hours * 3600
    companion = payload.get("companion") or {}
    if not isinstance(companion, dict):
        companion = {}
    dongfu = payload.get("dongfu") or {}
    if isinstance(dongfu, str):
        try:
            dongfu = json.loads(dongfu)
        except Exception:
            dongfu = {}
    if isinstance(dongfu, dict):
        companion_residence = dongfu.get("companion_residence") or {}
        if isinstance(companion_residence, str):
            try:
                companion_residence = json.loads(companion_residence)
            except Exception:
                companion_residence = {}
        if not isinstance(companion_residence, dict):
            companion_residence = {}
    if payload_field in companion:
        companion_payload = companion
    elif payload_field in companion_residence:
        companion_payload = companion_residence
    else:
        return None
    last_ts = _parse_iso_to_ts(companion_payload.get(payload_field))
    if last_ts <= 0:
        return None
    return last_ts + cooldown_hours * 3600


def _normalize_wild_experience_strategy(value: object) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in WILD_EXPERIENCE_ALLOWED_STRATEGIES else "均衡"


async def _run_companion_auto_scheduler(client: object, storage: Storage) -> None:
    profile_id = getattr(client, "_tg_game_profile_id", None)
    if not profile_id:
        return

    while True:
        try:
            tasks = storage.list_active_companion_auto_tasks(int(profile_id))
            if not tasks:
                await asyncio.sleep(COMPANION_AUTO_POLL_SECONDS)
                continue

            payload = read_cached_external_payload(storage, int(profile_id))
            now = time.time()
            for task in tasks:
                task_id = int(task.get("id") or 0)
                feature_key = str(task.get("feature_key") or "").strip()
                feature = COMPANION_AUTO_FEATURES.get(feature_key)
                if not feature or not task_id:
                    continue

                resolved_next_run_at = _resolve_companion_next_run_at(
                    payload, feature_key
                )
                if resolved_next_run_at is None:
                    cancel_text = str(feature.get("command") or "")
                    if feature_key == "wild_experience":
                        cancel_text = f".野外历练 {_normalize_wild_experience_strategy(task.get('strategy'))}"
                    storage.cancel_pending_outgoing_commands(
                        int(profile_id),
                        int(task.get("chat_id") or 0),
                        text=cancel_text,
                    )
                    storage.update_companion_auto_task(
                        task_id,
                        enabled=0,
                        next_run_at=0,
                        last_error=f"最新 payload 缺少{feature.get('command') or feature_key}冷却字段，已停止自动。",
                    )
                    continue
                if feature_key == "wild_experience" and resolved_next_run_at <= now:
                    fresh_payload = await asyncio.to_thread(
                        _refresh_companion_payload, storage, int(profile_id)
                    )
                    if not fresh_payload or not isinstance(fresh_payload, dict):
                        storage.update_companion_auto_task(
                            task_id,
                            next_run_at=now + 300,
                            last_error="刷新野外历练冷却失败，5分钟后重试。",
                        )
                        continue
                    payload = fresh_payload
                    resolved_next_run_at = _resolve_companion_next_run_at(
                        fresh_payload, feature_key
                    )
                    if resolved_next_run_at is None:
                        cancel_text = f".野外历练 {_normalize_wild_experience_strategy(task.get('strategy'))}"
                        storage.cancel_pending_outgoing_commands(
                            int(profile_id),
                            int(task.get("chat_id") or 0),
                            text=cancel_text,
                        )
                        storage.update_companion_auto_task(
                            task_id,
                            enabled=0,
                            next_run_at=0,
                            last_error="最新 payload 缺少野外历练冷却字段，已停止自动。",
                        )
                        continue
                if resolved_next_run_at > now:
                    storage.update_companion_auto_task(
                        task_id,
                        next_run_at=resolved_next_run_at,
                        last_error="",
                    )
                    continue

                chat_id = int(task.get("chat_id") or 0)
                if not chat_id:
                    storage.update_companion_auto_task(
                        task_id,
                        enabled=0,
                        last_error="Chat ID missing",
                    )
                    continue

                thread_id = (
                    int(task.get("thread_id")) if task.get("thread_id") else None
                )
                command_text = str(feature.get("command") or "").strip()
                if feature_key == "wild_experience":
                    command_text = f".野外历练 {_normalize_wild_experience_strategy(task.get('strategy'))}"

                latest_command = storage.get_latest_outgoing_command(
                    chat_id,
                    profile_id=int(profile_id),
                    text=command_text,
                    thread_id=thread_id,
                )
                if latest_command and str(
                    latest_command.get("status") or ""
                ).strip() in {"pending", "sending"}:
                    continue

                last_run_at = float(task.get("last_run_at") or 0)
                if (
                    last_run_at
                    and (now - last_run_at) < COMPANION_AUTO_POST_SEND_GRACE_SECONDS
                ):
                    continue

                storage.enqueue_outgoing_command(
                    profile_id=int(profile_id),
                    chat_id=chat_id,
                    text=command_text,
                    thread_id=thread_id,
                    chat_type=str(task.get("chat_type") or "group"),
                    bot_username=str(task.get("bot_username") or ""),
                )
                storage.update_companion_auto_task(
                    task_id,
                    last_run_at=now,
                    next_run_at=now + COMPANION_AUTO_POST_SEND_GRACE_SECONDS,
                    last_error="",
                )
            await asyncio.sleep(COMPANION_AUTO_POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Companion auto scheduler error for profile=%s: %s", profile_id, exc
            )
            await asyncio.sleep(10)


async def _run_divination_batch_scheduler(client: object, storage: Storage) -> None:
    profile_id = getattr(client, "_tg_game_profile_id", None)
    if not profile_id:
        return

    while True:
        try:
            batch = storage.get_active_divination_batch(int(profile_id))
            if not batch:
                await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                continue

            batch_id = int(batch["id"])
            chat_id = int(batch.get("chat_id") or 0)
            thread_id = int(batch.get("thread_id")) if batch.get("thread_id") else None
            target_count = max(int(batch.get("target_count") or 0), 0)
            initial_count = max(int(batch.get("initial_count") or 0), 0)
            planned_rounds = max(target_count - initial_count, 0)
            sent_count = max(int(batch.get("sent_count") or 0), 0)
            last_dispatch_at = float(batch.get("last_dispatch_at") or 0)

            current_count = _get_cached_divination_today_count(storage, int(profile_id))
            completed_count = max(current_count - initial_count, 0)
            stored_completed = max(int(batch.get("completed_count") or 0), 0)
            if completed_count != stored_completed:
                batch = (
                    storage.update_divination_batch(
                        batch_id,
                        completed_count=completed_count,
                        pending_command_msg_id=0,
                        last_error="",
                    )
                    or batch
                )

            if current_count >= target_count:
                storage.cancel_pending_outgoing_commands(
                    int(profile_id), chat_id, text=DIVINATION_COMMAND
                )
                storage.finish_divination_batch(batch_id, status="completed")
                await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                continue

            latest_command = storage.get_latest_outgoing_command(
                chat_id,
                profile_id=int(profile_id),
                text=DIVINATION_COMMAND,
                thread_id=thread_id,
            )
            if latest_command:
                latest_status = str(latest_command.get("status") or "").strip()
                if latest_status in {"pending", "sending"}:
                    await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                    continue

            now = time.time()
            effective_last_dispatch_at = last_dispatch_at
            if not effective_last_dispatch_at and latest_command:
                effective_last_dispatch_at = float(
                    latest_command.get("created_at") or 0
                )

            if effective_last_dispatch_at and (
                now - effective_last_dispatch_at
                < DIVINATION_BATCH_COMMAND_INTERVAL_SECONDS
            ):
                await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                continue

            # 计划次数已发完后，主动刷新天机阁缓存再决定补发与否
            needs_makeup = current_count < target_count
            if sent_count >= planned_rounds and needs_makeup:
                try:
                    fresh_payload = await asyncio.to_thread(
                        _refresh_divination_payload, storage, int(profile_id)
                    )
                    if fresh_payload and isinstance(fresh_payload, dict):
                        fresh_count = _get_divination_today_count_from_payload(
                            fresh_payload
                        )
                        fresh_completed = max(fresh_count - initial_count, 0)
                        storage.update_divination_batch(
                            batch_id,
                            completed_count=fresh_completed,
                            last_error="",
                        )
                        if fresh_count >= target_count:
                            storage.cancel_pending_outgoing_commands(
                                int(profile_id), chat_id, text=DIVINATION_COMMAND
                            )
                            storage.finish_divination_batch(
                                batch_id, status="completed"
                            )
                            await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                            continue
                        current_count = fresh_count
                        completed_count = fresh_completed
                        needs_makeup = current_count < target_count
                    else:
                        # 接口返回None或非dict，终止补发
                        storage.cancel_pending_outgoing_commands(
                            int(profile_id), chat_id, text=DIVINATION_COMMAND
                        )
                        storage.finish_divination_batch(
                            batch_id,
                            status="failed",
                            last_error="天机阁接口刷新失败，终止补发",
                        )
                        await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                        continue
                except Exception:
                    storage.cancel_pending_outgoing_commands(
                        int(profile_id), chat_id, text=DIVINATION_COMMAND
                    )
                    storage.finish_divination_batch(
                        batch_id,
                        status="failed",
                        last_error="天机阁接口刷新异常，终止补发",
                    )
                    await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                    continue
            if sent_count >= planned_rounds and not needs_makeup:
                await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
                continue

            storage.enqueue_outgoing_command(
                profile_id=int(profile_id),
                chat_id=chat_id,
                text=DIVINATION_COMMAND,
                thread_id=thread_id,
                chat_type=str(batch.get("chat_type") or "group"),
                bot_username=str(batch.get("bot_username") or ""),
            )
            storage.update_divination_batch(
                batch_id,
                pending_command_msg_id=0,
                last_dispatch_at=now,
                last_error="",
            )
            await asyncio.sleep(DIVINATION_BATCH_POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Divination batch scheduler error for profile=%s: %s", profile_id, exc
            )
            await asyncio.sleep(10)


SECT_FEATURE_REPLY_WHITELISTS = {
    "huangfeng": {
        ".小药园",
        ".播种",
        ".采药",
        ".除草",
        ".除虫",
        ".浇水",
        ".扩建药园",
    },
    "xingong": {
        ".启阵",
        ".助阵",
        ".观星台",
        ".牵引星辰",
        ".收集精华",
        ".安抚星辰",
        ".观星",
        ".改换星移",
        ".我的侍妾",
        ".每日问安",
    },
    "lingxiao": {
        ".凌霄宫",
        ".天阶状态",
        ".问心台",
        ".登天阶",
        ".引九天罡风",
        ".借天门势",
    },
    "taiyi": {".引道", ".神识冲击"},
    "wanling": {
        ".寻觅灵兽",
        ".我的灵兽",
        ".喂养",
        ".灵兽出战",
        ".灵兽休息",
        ".一键放养",
        ".灵兽偷菜",
        ".探渊",
    },
    "luoyun": {".灵树状态", ".灵树灌溉", ".协同守山", ".采摘灵果"},
    "yinluo": {
        ".我的阴罗幡",
        ".升级阴罗幡",
        ".每日献祭",
        ".化功为煞",
        ".血洗山林",
        ".召唤魔影",
        ".囚禁魂魄",
        ".安抚幡灵",
        ".收取精华",
        ".下咒",
        ".收割",
    },
    "yuanying": {
        ".元婴状态",
        ".元婴出窍",
        ".元婴闭关",
        ".元婴归窍",
        ".问道",
        ".参悟功法",
    },
    "hehuan": {
        ".闭关双修",
        ".缔结同参",
        ".双修 温养",
        ".种下心印",
        ".双修 采补",
        ".挣脱心印",
        ".结印",
    },
}


class BaseExecutor(ABC):
    key = "base"

    async def startup(self, client: object, storage: Storage) -> None:
        return None

    def _expected_profile_user_id(self, context: EventContext) -> str:
        binding_user_id = (
            context.chat_binding.telegram_user_id if context.chat_binding else ""
        )
        return binding_user_id or (
            context.profile.telegram_user_id if context.profile else ""
        )

    async def _bot_message_targets_profile(
                        self, context: EventContext, storage: Storage
    ) -> bool:
        if await context.bot_message_targets_profile():
            return True
        return False

    def _get_stored_reply_message(
        self, context: EventContext, storage: Storage
    ) -> Optional[dict]:
        return None

    async def _get_reply_message_text(
        self, context: EventContext, storage: Storage
    ) -> str:
        reply_text = await context.get_reply_message_text()
        if reply_text:
            return reply_text.strip()
        return ""

    @abstractmethod
    async def handle(self, context: EventContext, storage: Storage) -> bool:
        raise NotImplementedError


class FanrenExecutor(BaseExecutor):
    key = "fanren"

    def __init__(self) -> None:
        self._runner_started = False

    async def startup(self, client: object, storage: Storage) -> None:
        if self._runner_started:
            return
        self._runner_started = True
        db = SQLiteCompatDb(storage)
        fanren_game.ensure_tables(db)
        db.close()
        _register_client_background_task(
            client,
            asyncio.create_task(
                fanren_game.runner(
                    client,
                    storage,
                    profile_id=getattr(client, "_tg_game_profile_id", None),
                )
            ),
        )
        logger.info("Fanren executor runner started")

    async def _bot_message_targets_profile(
        self, context: EventContext, storage: Storage
    ) -> bool:
        if await super()._bot_message_targets_profile(context, storage):
            return True
        if not context.profile or context.chat_id is None:
            return False
        db = SQLiteCompatDb(storage)
        try:
            session = fanren_game.get_session(
                db, context.chat_id, profile_id=context.profile.id
            )
        finally:
            db.close()
        if not session:
            return False
        if session.get("thread_id") and context.thread_id:
            if int(session.get("thread_id") or 0) != int(context.thread_id or 0):
                return False
        last_action = str(session.get("last_action") or "").strip()
        if not last_action:
            return False
        if last_action not in {
            fanren_game.YUANYING_OUTING_COMMAND,
            fanren_game.YUANYING_STATUS_COMMAND,
        }:
            return False
        last_action_time = float(session.get("last_action_time") or 0)
        if not last_action_time:
            return False
        if (time.time() - last_action_time) > FANREN_RECENT_REPLY_WINDOW_SECONDS:
            return False
        raw_text = context.text
        has_yuanying_anchor = any(
            keyword in raw_text
            for keyword in ("元婴", "本命元婴", "元神归窍", "窍中温养")
        )
        if not has_yuanying_anchor:
            return False
        if last_action == fanren_game.YUANYING_STATUS_COMMAND:
            yy_status, _yy_cd = fanren_game.parse_yuanying_status_reply(raw_text)
            return yy_status != "unknown"
        yy_success, yy_cd = fanren_game.parse_yuanying_reply(raw_text)
        return yy_success or yy_cd is not None

    async def handle(self, context: EventContext, storage: Storage) -> bool:
        if not context.chat_binding:
            return False

        db = SQLiteCompatDb(storage)
        try:
            if context.text.startswith(".fanren") and context.is_profile_owner():
                if context.profile:
                    if context.thread_id is not None:
                        storage.set_chat_binding_thread_id(
                            context.profile.id, context.chat_id, context.thread_id
                        )
                        fanren_game.update_session(
                            db,
                            context.chat_id,
                            profile_id=context.profile.id if context.profile else None,
                            thread_id=context.thread_id,
                        )
                return await self._handle_command(context, db)

            if (
                context.is_bot_sender
                and _is_context_sender_allowed_bot(context)
                and await self._bot_message_targets_profile(context, storage)
            ):
                session = fanren_game.get_session(
                    db,
                    context.chat_id,
                    profile_id=context.profile.id if context.profile else None,
                )
                reply_text = await self._get_reply_message_text(context, storage)
                allowed_reply_commands = {
                    fanren_game.FANREN_CHECK_COMMAND,
                    fanren_game.FANREN_NORMAL_COMMAND,
                    fanren_game.FANREN_DEEP_COMMAND,
                    ".强行出关",
                    fanren_game.RIFT_EXPLORE_COMMAND,
                    fanren_game.YUANYING_OUTING_COMMAND,
                    fanren_game.YUANYING_STATUS_COMMAND,
                }
                if session:
                    allowed_reply_commands.add(fanren_game.build_check_command(session))
                if reply_text and reply_text not in allowed_reply_commands:
                    return False
                stored_reply_message = self._get_stored_reply_message(context, storage)
                reply_message_id = context.reply_to_msg_id or int(
                    (stored_reply_message or {}).get("message_id") or 0
                )
                parsed = await fanren_game.handle_bot_message(
                    context.event,
                    db,
                    client=context.client,
                    profile_id=context.profile.id if context.profile else None,
                )
                if parsed is not None:
                    session = fanren_game.get_session(
                        db,
                        context.chat_id,
                        profile_id=context.profile.id if context.profile else None,
                    )
                    await fanren_game.maybe_delete_normal_command_message(
                        context.event,
                        session,
                        context.client,
                        reply_text,
                        reply_message_id=reply_message_id or None,
                    )
                    if context.profile and context.chat_id is not None:
                        try:
                            sync_cultivation_session(
                                storage, context.profile.id, context.chat_id, db
                            )
                        except Exception as exc:
                            logger.warning(
                                "Cultivation API sync failed in chat %s: %s",
                                context.chat_id,
                                exc,
                            )
                    self._record_result(context, storage, parsed.event)
                return parsed is not None
            if (
                context.is_bot_sender
                and _is_context_sender_allowed_bot(context)
                and await context.bot_message_targets_profile()
            ):
                reply_text = await self._get_reply_message_text(context, storage)
                fallback_allowed_reply_commands = {
                    fanren_game.FANREN_CHECK_COMMAND,
                    fanren_game.FANREN_NORMAL_COMMAND,
                    fanren_game.FANREN_DEEP_COMMAND,
                    ".强行出关",
                    fanren_game.RIFT_EXPLORE_COMMAND,
                    fanren_game.YUANYING_OUTING_COMMAND,
                    fanren_game.YUANYING_STATUS_COMMAND,
                }
                if reply_text and reply_text not in fallback_allowed_reply_commands:
                    return False
                parsed = await fanren_game.handle_bot_message(
                    context.event,
                    db,
                    client=context.client,
                    profile_id=context.profile.id if context.profile else None,
                )
                if parsed is not None:
                    if context.profile and context.chat_id is not None:
                        try:
                            sync_cultivation_session(
                                storage, context.profile.id, context.chat_id, db
                            )
                        except Exception as exc:
                            logger.warning(
                                "Cultivation API sync failed in chat %s: %s",
                                context.chat_id,
                                exc,
                            )
                    return True
            return False
        finally:
            db.close()

    def _record_result(
        self, context: EventContext, storage: Storage, event_name: str
    ) -> None:
        if not event_name:
            return
        # 记录所有闭关/元婴/裂缝相关事件，不只是里程碑
        if event_name in {"empty", "ignored", "blocked", "resource_blocked", "unknown"}:
            return
        if event_name.endswith("_edited"):
            pass  # 编辑事件总是记录
        elif not any(
            event_name.startswith(prefix)
            for prefix in (
                "retreat_",
                "deep_",
                "cultivat",
                "cooldown",
                "rift_",
                "yuanying_",
                "soul_",
                "meditation",
            )
        ) and event_name not in {
            "cultivation_full",
            "soul_returning",
            "jie_dan",
            "jie_dan_complete",
        }:
            return
        session_setting = context.get_setting("cultivation") or context.get_setting(
            "basic"
        )
        gain_value = fanren_game.parse_gain_value(context.text)
        stage_name, progress_text = fanren_game.extract_stage_progress(context.text)
        mode = "normal"
        if context.chat_id is not None:
            db = SQLiteCompatDb(storage)
            try:
                session = fanren_game.get_session(
                    db,
                    context.chat_id,
                    profile_id=context.profile.id if context.profile else None,
                )
                mode = (
                    (session.get("retreat_mode") or "normal") if session else "normal"
                )
            finally:
                db.close()
        elif (
            session_setting
            and session_setting.command_template == fanren_game.FANREN_CHECK_COMMAND
        ):
            mode = "deep"
        storage.record_cultivation_result(
            profile_id=context.profile.id if context.profile else None,
            chat_id=context.chat_id or 0,
            mode=mode,
            event=event_name,
            gain_value=gain_value,
            stage_name=stage_name,
            progress_text=progress_text,
            summary=fanren_game.parse_message(context.text).summary,
            raw_text=context.text,
        )

    async def _handle_command(self, context: EventContext, db: SQLiteCompatDb) -> bool:
        parts = context.text.split(maxsplit=2)
        action = parts[1].lower() if len(parts) > 1 else "status"
        payload = parts[2].strip() if len(parts) > 2 else ""
        chat_id = context.chat_id
        if chat_id is None:
            return False

        setting = context.get_setting("cultivation") or context.get_setting("basic")
        if setting:
            fanren_game.set_interval(
                db,
                chat_id,
                setting.check_interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            if setting.command_template:
                fanren_game.set_check_command(
                    db,
                    chat_id,
                    setting.command_template,
                    profile_id=context.profile.id if context.profile else None,
                )

        if action == "on":
            if payload in {"normal", "deep"}:
                fanren_game.set_mode(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            if context.profile:
                sync_cultivation_session(storage, context.profile.id, chat_id, db)
            fanren_game.set_enabled(
                db,
                chat_id,
                True,
                reset_failure=True,
                profile_id=context.profile.id if context.profile else None,
            )
            session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(
                f"凡人修仙自动化已开启，当前模式为 {'深度闭关' if session.get('retreat_mode') == 'deep' else '普通闭关'}，将按接口冷却时间自动调度。"
            )
            return True
        if action == "off":
            fanren_game.set_enabled(
                db,
                chat_id,
                False,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("凡人修仙自动化已关闭。")
            return True
        if action == "status":
            session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(fanren_game.build_status_text(session))
            return True
        if action == "dry-run":
            enabled = payload.lower() == "on"
            fanren_game.set_dry_run(
                db,
                chat_id,
                enabled,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"凡人修仙 dry-run 已{'开启' if enabled else '关闭'}。")
            return True
        if action == "interval":
            try:
                interval_seconds = fanren_game.parse_interval_input(payload)
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            fanren_game.set_interval(
                db,
                chat_id,
                interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(
                f"凡人修仙检查间隔已设置为 {fanren_game.format_duration(interval_seconds)}。"
            )
            return True
        if action == "check":
            try:
                check_command = fanren_game.set_check_command(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            await context.reply(f"凡人修仙检查指令已设置为: {check_command}")
            return True
        if action == "mode":
            try:
                retreat_mode = fanren_game.set_mode(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            if context.profile:
                sync_cultivation_session(storage, context.profile.id, chat_id, db)
            await context.reply(
                f"凡人修仙模式已设置为 {'深度闭关' if retreat_mode == 'deep' else '普通闭关'}，将按接口冷却时间自动调度。"
            )
            return True
        if action == "run":
            if context.profile:
                sync_cultivation_session(storage, context.profile.id, chat_id, db)
            _ok, status = await fanren_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=False,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "reset":
            fanren_game.reset_failures(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("凡人修仙失败计数已重置。")
            return True
        if action == "rift":
            rift_action = payload.lower() if payload else "status"
            rift_session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            if rift_action == "on":
                fanren_game.set_auto_rift(
                    db,
                    chat_id,
                    True,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动探寻裂缝已开启，CD 12 小时。")
                return True
            if rift_action == "off":
                fanren_game.set_auto_rift(
                    db,
                    chat_id,
                    False,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动探寻裂缝已关闭。")
                return True
            if rift_action == "status":
                await context.reply(
                    "\n".join(
                        [
                            "自动探寻裂缝状态",
                            f"开关: {'开启' if rift_session.get('auto_rift_enabled') else '关闭'}",
                            f"状态: {rift_session.get('rift_state') or '-'}",
                            f"下次: {fanren_game.format_timestamp(rift_session.get('rift_next_check_time') or 0)}",
                            f"重试: {rift_session.get('rift_retry_count') or 0}/{fanren_game.RIFT_RETRY_MAX}",
                        ]
                    )
                )
                return True
            if rift_action == "log":
                if not context.profile:
                    await context.reply("当前未绑定角色，无法查看裂缝日志。")
                    return True
                logs = fanren_game.get_rift_execution_logs(
                    storage,
                    profile_id=context.profile.id,
                    chat_id=chat_id,
                    limit=12,
                )
                if not logs:
                    await context.reply("最近没有自动探寻裂缝执行日志。")
                    return True
                lines = ["自动探寻裂缝日志（最近12条）"]
                for entry in reversed(logs):
                    lines.append(
                        f"[{fanren_game.format_timestamp(entry.get('created_at') or 0)}] "
                        f"{entry.get('step') or '-'} / {entry.get('event_type') or '-'} / "
                        f"{entry.get('rift_state') or '-'}"
                    )
                await context.reply("\n".join(lines))
                return True
        if action == "yuanying":
            yy_action = payload.lower() if payload else "status"
            yy_session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            if yy_action == "on":
                fanren_game.set_auto_yuanying(
                    db,
                    chat_id,
                    True,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动元婴出窍已开启，CD 8 小时。")
                return True
            if yy_action == "off":
                fanren_game.set_auto_yuanying(
                    db,
                    chat_id,
                    False,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动元婴出窍已关闭。")
                return True
            if yy_action == "status":
                await context.reply(
                    "\n".join(
                        [
                            "自动元婴出窍状态",
                            f"开关: {'开启' if yy_session.get('auto_yuanying_enabled') else '关闭'}",
                            f"状态: {yy_session.get('yuanying_state') or '-'}",
                            f"下次: {fanren_game.format_timestamp(yy_session.get('yuanying_next_check_time') or 0)}",
                        ]
                    )
                )
                return True

        await context.reply(
            "用法: .fanren status|on [normal|deep]|off|mode normal|deep|dry-run on|off|interval 5m|check 指令|run|reset|rift on|off|status|yuanying on|off|status"
        )
        return True


class SectExecutor(BaseExecutor):
    key = "sect"

    def __init__(self) -> None:
        self._runner_started = False

    def _reply_matches_whitelist(self, reply_text: str, feature_key: str) -> bool:
        reply_text = (reply_text or "").strip()
        if not reply_text:
            return False
        for command in SECT_FEATURE_REPLY_WHITELISTS.get(feature_key, set()):
            if reply_text == command or reply_text.startswith(f"{command} "):
                return True
        return False

    async def startup(self, client: object, storage: Storage) -> None:
        if self._runner_started:
            return
        self._runner_started = True
        db = SQLiteCompatDb(storage)
        sect_game.ensure_tables(db)
        db.close()
        _register_client_background_task(
            client,
            asyncio.create_task(
                sect_game.runner(
                    client,
                    storage,
                    profile_id=getattr(client, "_tg_game_profile_id", None),
                )
            ),
        )
        logger.info("Sect executor runner started")

    async def handle(self, context: EventContext, storage: Storage) -> bool:
        if not context.chat_binding:
            return False

        db = SQLiteCompatDb(storage)
        try:
            if context.text.startswith(".sect") and context.is_profile_owner():
                if context.profile:
                    if context.thread_id is not None:
                        storage.set_chat_binding_thread_id(
                            context.profile.id, context.chat_id, context.thread_id
                        )
                        sect_game.update_session(
                            db,
                            context.chat_id,
                            profile_id=context.profile.id if context.profile else None,
                            thread_id=context.thread_id,
                        )
                return await self._handle_command(context, db)

            if context.is_bot_sender and _is_context_sender_allowed_bot(context):
                preview_parsed = sect_game.parse_message(context.text)
                bot_targets_profile = await self._bot_message_targets_profile(
                    context, storage
                )
                allow_companion_assist_observation = False
                if (
                    not bot_targets_profile
                    and preview_parsed.get("event")
                    in {"xinggong_star_array_open", "xinggong_star_array_complete"}
                    and context.profile
                    and context.chat_id is not None
                ):
                    session = sect_game.get_session(
                        db,
                        context.chat_id,
                        profile_id=context.profile.id,
                    )
                    allow_companion_assist_observation = bool(
                        session
                        and session.get("enabled")
                        and session.get("auto_companion_assist_enabled")
                    )
                if not bot_targets_profile and not allow_companion_assist_observation:
                    return False
                reply_text = await self._get_reply_message_text(context, storage)
                if reply_text:
                    if (
                        preview_parsed.get("event")
                        in {
                            "sect_panel",
                            "sect_panel_pending",
                            "sect_info",
                        }
                        and reply_text != ".我的宗门"
                    ):
                        return False
                    if (
                        preview_parsed.get("event") == "lingxiao_step"
                        and reply_text != ".登天阶"
                    ):
                        return False
                parsed = await sect_game.handle_bot_message(
                    context.event,
                    db,
                    client=context.client,
                    profile_id=context.profile.id if context.profile else None,
                    profile=context.profile,
                )
                if parsed is not None:
                    return True
                return parsed is not None
            return False
        finally:
            db.close()

    async def _handle_command(self, context: EventContext, db: SQLiteCompatDb) -> bool:
        parts = context.text.split(maxsplit=2)
        action = parts[1].lower() if len(parts) > 1 else "status"
        payload = parts[2].strip() if len(parts) > 2 else ""
        chat_id = context.chat_id
        if chat_id is None:
            return False

        setting = context.get_setting("sect") or context.get_setting("basic")
        if setting:
            sect_game.set_interval(
                db,
                chat_id,
                setting.check_interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            if setting.command_template:
                sect_game.set_check_command(
                    db,
                    chat_id,
                    setting.command_template,
                    profile_id=context.profile.id if context.profile else None,
                )

        if action == "on":
            sect_game.set_enabled(
                db,
                chat_id,
                True,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("宗门模块已开启。")
            return True
        if action == "off":
            sect_game.set_enabled(
                db,
                chat_id,
                False,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("宗门模块已关闭。")
            return True
        if action == "status":
            session = sect_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(sect_game.build_status_text(session))
            return True
        if action == "dry-run":
            enabled = payload.lower() == "on"
            sect_game.set_dry_run(
                db,
                chat_id,
                enabled,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"宗门 dry-run 已{'开启' if enabled else '关闭'}。")
            return True
        if action == "interval":
            try:
                interval_seconds = fanren_game.parse_interval_input(payload)
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            sect_game.set_interval(
                db,
                chat_id,
                interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(
                f"宗门检查间隔已设置为 {fanren_game.format_duration(interval_seconds)}。"
            )
            return True
        if action == "check":
            try:
                check_command = sect_game.set_check_command(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            await context.reply(f"宗门查询指令已设置为: {check_command}")
            return True
        if action == "panel":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".我的宗门",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "sign":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".宗门点卯",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "teach":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".宗门传功",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "bounty":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".宗门悬赏",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "submit":
            if not payload:
                await context.reply("用法: .sect submit 问候")
                return True
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=f".提交任务 {payload}",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "hf":
            return await self._handle_huangfeng_command(context, db, payload)
        if action == "xg":
            return await self._handle_xingong_command(context, db, payload)
        if action == "lx":
            return await self._handle_lingxiao_command(context, db, payload)
        if action == "ty":
            return await self._handle_taiyi_command(context, db, payload)
        if action == "wl":
            return await self._handle_wanling_command(context, db, payload)
        if action == "ly":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "status": ".灵树状态",
                    "water": ".灵树灌溉",
                    "guard": ".协同守山",
                    "harvest": ".采摘灵果",
                },
                "用法: .sect ly status|water|guard|harvest",
            )
        if action == "yl":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "banner": ".我的阴罗幡",
                    "upgrade": ".升级阴罗幡",
                    "daily": ".每日献祭",
                    "convert": ".化功为煞",
                    "hunt": ".血洗山林",
                    "summon": ".召唤魔影",
                    "prison": ".囚禁魂魄",
                    "soothe": ".安抚幡灵",
                    "collect": ".收取精华",
                    "curse": ".下咒",
                    "reap": ".收割",
                },
                "用法: .sect yl banner|upgrade|daily|convert|hunt|summon|prison|soothe|collect|curse|reap",
            )
        if action == "yy":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "status": ".元婴状态",
                    "trip": ".元婴出窍",
                    "retreat": ".元婴闭关",
                    "return": ".元婴归窍",
                    "seek": ".问道",
                    "skill": ".参悟功法",
                },
                "用法: .sect yy status|trip|retreat|return|seek|skill",
            )
        if action == "hh":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "dual": ".闭关双修",
                    "contract": ".缔结同参",
                    "warm": ".双修 温养",
                    "mark": ".种下心印",
                    "harvest": ".双修 采补",
                    "break": ".挣脱心印",
                    "seal": ".结印",
                },
                "用法: .sect hh dual|contract|warm|mark|harvest|break|seal",
            )
        if action == "run":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True

        await context.reply(
            "用法: .sect status|on|off|dry-run on|off|interval 30m|check 指令|panel|sign|teach|bounty|submit 内容|hf/xg/lx/ty/wl/ly/yl/yy/hh 子命令|run"
        )
        return True

    async def _handle_simple_feature_command(
        self,
        context: EventContext,
        db: SQLiteCompatDb,
        payload: str,
        action_map: dict,
        usage: str,
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        action = (payload or "").strip().lower()
        command_text = action_map.get(action)
        if not command_text:
            await context.reply(usage)
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True


class GeneralGameExecutor(BaseExecutor):
    key = "game"

    def __init__(self) -> None:
        self._runner_started = False
        self._parsers = [
            ("basic", basic_game.parse_message),
            ("breakthrough", breakthrough_game.parse_message),
            ("battle", battle_feature_game.parse_message),
            ("inventory", inventory_feature_game.parse_message),
            ("artifact", artifact_game.parse_message),
            ("estate", estate_game.parse_message),
            ("companion", companion_game.parse_message),
            ("dungeon", dungeon_feature_game.parse_message),
            ("market", market_trade_game.parse_message),
            ("stock", stock_trade_game.parse_message),
            ("diplomacy", diplomacy_game.parse_message),
            ("shop", shop_game.parse_message),
        ]

    async def startup(self, client: object, storage: Storage) -> None:
        if self._runner_started:
            return
        self._runner_started = True
        _register_client_background_task(
            client,
            asyncio.create_task(_run_divination_batch_scheduler(client, storage)),
        )
        _register_client_background_task(
            client,
            asyncio.create_task(_run_companion_auto_scheduler(client, storage)),
        )
        _register_client_background_task(
            client,
            asyncio.create_task(_run_companion_heart_tribulation_scheduler(client, storage)),
        )
        return

    async def handle(self, context: EventContext, storage: Storage) -> bool:
        if context.text.strip() == ".chatid" and context.is_profile_owner():
            binding_ref = (
                f"{context.chat_id}_{context.thread_id}"
                if context.thread_id
                else f"{context.chat_id}"
            )
            await context.reply(
                "\n".join(
                    [
                        "当前聊天信息",
                        f"绑定 ID: {binding_ref}",
                        f"Chat ID: {context.chat_id}",
                        f"Thread ID: {context.thread_id or '无'}",
                        f"类型: {'私聊' if context.is_private else '群组/频道'}",
                        f"发送者 ID: {context.sender_id}",
                        f"线程状态: {'话题线程' if context.thread_id else '主会话'}",
                    ]
                )
            )
            return True
        if not context.chat_binding:
            return False

        await self._maybe_advance_divination_batch(context, storage)

        if context.is_bot_sender and await self._bot_message_targets_profile(
            context, storage
        ):
            reply_text = await self._get_reply_message_text(context, storage)
            if (
                context.profile
                and context.chat_id is not None
                and reply_text
                in {
                    ".我的持仓",
                    ".股市任务",
                }
                and context.text
            ):
                # 校验回包内容确实是股票相关，过滤误匹配
                stock_keywords = (
                    "持仓",
                    "股票",
                    "浮盈",
                    "市值",
                    "仓位",
                    "股息",
                    "融资",
                )
                is_stock_reply = any(
                    kw in (context.text or "") for kw in stock_keywords
                )
                if reply_text == ".我的持仓":
                    is_stock_reply = is_stock_reply or "我的股票账户" in (
                        context.text or ""
                    )
                elif reply_text == ".股市任务":
                    is_stock_reply = is_stock_reply or "股市任务" in (
                        context.text or ""
                    )
                if is_stock_reply:
                    storage.upsert_stock_player_reply(
                        context.profile.id,
                        context.chat_id,
                        reply_text,
                        context.text,
                        thread_id=context.thread_id,
                        source_message_id=int(context.message_id or 0),
                        reply_to_msg_id=int(context.reply_to_msg_id or 0),
                    )
            for module_key, parser in self._parsers:
                parsed = parser(context.text)
                if parsed is not None:
                    if module_key == "basic" and parsed.get("event") in {
                        "basic_profile",
                        "basic_profile_pending",
                    }:
                        continue
                    if (
                        module_key == "battle"
                        and parsed.get("event") == "battle_profile"
                    ):
                        continue
                    if (
                        module_key == "artifact"
                        and parsed.get("event") == "artifact_status_profile"
                    ):
                        continue
                    return True
        return False


    async def _maybe_advance_companion_heart_tribulation(
        self, context: EventContext, storage: Storage
    ) -> bool:
        if not context.profile or context.chat_id is None:
            return False
        task = storage.get_companion_heart_tribulation_task(
            context.profile.id,
            context.chat_id,
            thread_id=context.thread_id,
        )
        if not task or not bool(task.get("enabled")):
            return False

        task_id = int(task.get("id") or 0)
        if not task_id:
            return False

        workflow_state = str(task.get("workflow_state") or "").strip()
        if workflow_state in {
            "",
            COMPANION_HEART_TRIBULATION_IDLE_STATE,
            COMPANION_HEART_TRIBULATION_SENDING_PANEL_STATE,
            COMPANION_HEART_TRIBULATION_FAILED_STATE,
        }:
            return False

        task_thread_id = int(task.get("thread_id")) if task.get("thread_id") else None
        if (
            task_thread_id is not None
            and context.thread_id is not None
            and context.thread_id != task_thread_id
        ):
            return False

        if not _is_context_sender_allowed_bot(context):
            return False

        sender = getattr(context.event, "sender", None)
        sender_username = (getattr(sender, "username", "") or "").strip()
        current_message_id = int(context.message_id or 0)
        current_reply_to_msg_id = int(context.reply_to_msg_id or 0)
        current_sender_id = int(context.sender_id or 0)
        current_text = context.text or ""
        is_edited_event = _is_edited_event(context)

        if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_PANEL_STATE:
            expected_reply_to = int(task.get("anchor_command_msg_id") or 0)
            if expected_reply_to <= 0:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="自动共历心劫缺少侍妾命令锚点，已停止自动。",
                    step=workflow_state,
                )
                return True
            if current_reply_to_msg_id != expected_reply_to:
                return False
            if not current_message_id:
                return False
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step=workflow_state,
                event_type="panel_reply_received",
                message_id=current_message_id,
                reply_to_msg_id=current_reply_to_msg_id,
                sender_id=current_sender_id,
                sender_username=sender_username,
                text=current_text,
            )
            try:
                command_message = await _send_companion_heart_tribulation_command(
                    context.client,
                    storage,
                    task,
                    text=COMPANION_HEART_TRIBULATION_COMMAND,
                    reply_to_msg_id=current_message_id,
                )
            except Exception as exc:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error=f"发送{COMPANION_HEART_TRIBULATION_COMMAND}失败，已停止自动共历心劫。",
                    step="send_tribulation_command",
                    detail={"error": str(exc)},
                )
                return True
            storage.update_companion_heart_tribulation_task(
                task_id,
                workflow_state=COMPANION_HEART_TRIBULATION_AWAIT_TRIBULATION_STATE,
                step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_STEP_TIMEOUT_SECONDS,
                last_run_at=time.time(),
                matched_bot_id=current_sender_id,
                anchor_bot_msg_id=current_message_id,
                panel_reply_msg_id=current_message_id,
                tribulation_command_msg_id=int(getattr(command_message, "id", 0) or 0),
                last_tribulation_command_at=time.time(),
                last_error="",
            )
            task = storage.get_companion_heart_tribulation_task(
                context.profile.id,
                context.chat_id,
                thread_id=context.thread_id,
            ) or task
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step=COMPANION_HEART_TRIBULATION_AWAIT_TRIBULATION_STATE,
                event_type="send_tribulation_command",
                message_id=int(getattr(command_message, "id", 0) or 0),
                reply_to_msg_id=current_message_id,
                text=COMPANION_HEART_TRIBULATION_COMMAND,
            )
            return True

        if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_TRIBULATION_STATE:
            expected_reply_to = int(task.get("tribulation_command_msg_id") or 0)
            if expected_reply_to <= 0:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="自动共历心劫缺少心劫命令锚点，已停止自动。",
                    step=workflow_state,
                )
                return True
            if current_reply_to_msg_id != expected_reply_to:
                return False
            if not current_message_id:
                return False
            round1_command = _build_companion_heart_tribulation_action_command(task, 1)
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step=workflow_state,
                event_type="tribulation_reply_received",
                message_id=current_message_id,
                reply_to_msg_id=current_reply_to_msg_id,
                sender_id=current_sender_id,
                sender_username=sender_username,
                text=current_text,
            )
            try:
                action_message = await _send_companion_heart_tribulation_command(
                    context.client,
                    storage,
                    task,
                    text=round1_command,
                    reply_to_msg_id=current_message_id,
                )
            except Exception as exc:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="发送第一轮心劫策略失败，已停止自动共历心劫。",
                    step="send_round1",
                    detail={"error": str(exc), "command": round1_command},
                )
                return True
            fingerprint = _build_companion_heart_tribulation_event_fingerprint(
                message_id=current_message_id,
                text=current_text,
                event_kind="tribulation_reply",
            )
            storage.update_companion_heart_tribulation_task(
                task_id,
                workflow_state=COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE,
                step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
                last_run_at=time.time(),
                matched_bot_id=current_sender_id,
                tribulation_msg_id=current_message_id,
                anchor_bot_msg_id=current_message_id,
                last_action_round_sent=1,
                last_progress_at=time.time(),
                last_progress_fingerprint=fingerprint,
                last_stable_sent_at=time.time(),
                round_retry_count=0,
                round_retry_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_ROUND_RETRY_SECONDS,
                last_error="",
            )
            task = storage.get_companion_heart_tribulation_task(
                context.profile.id,
                context.chat_id,
                thread_id=context.thread_id,
            ) or task
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step=COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE,
                event_type="send_round1",
                message_id=int(getattr(action_message, "id", 0) or 0),
                reply_to_msg_id=current_message_id,
                text=round1_command,
            )
            return True

        tribulation_msg_id = int(task.get("tribulation_msg_id") or 0)
        if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE and not is_edited_event:
            if COMPANION_HEART_TRIBULATION_SETTLEMENT_KEYWORD in current_text and current_sender_id in _binding_bot_ids(context):
                _append_companion_heart_tribulation_log(
                    storage,
                    task,
                    step=workflow_state,
                    event_type="settlement_received",
                    message_id=current_message_id,
                    reply_to_msg_id=current_reply_to_msg_id,
                    sender_id=current_sender_id,
                    sender_username=sender_username,
                    text=current_text,
                )
                previous_settlement_text = str(task.get("last_settlement_text") or "")
                previous_settlement_at = float(task.get("last_settlement_at") or 0)
                updated_task = storage.update_companion_heart_tribulation_task(
                    task_id,
                    workflow_state=COMPANION_HEART_TRIBULATION_IDLE_STATE,
                    step_deadline_at=0,
                    matched_bot_id=0,
                    anchor_command_msg_id=0,
                    anchor_bot_msg_id=0,
                    tribulation_command_msg_id=0,
                    tribulation_msg_id=0,
                    panel_reply_msg_id=0,
                    last_action_round_sent=0,
                    last_tribulation_command_at=0,
                    last_progress_at=time.time(),
                    last_progress_fingerprint="",
                    last_stable_sent_at=0,
                    last_settlement_text=current_text,
                    last_settlement_at=time.time(),
                    previous_settlement_text=previous_settlement_text,
                    previous_settlement_at=previous_settlement_at,
                    last_error="",
                )
                task = updated_task or task
                _append_companion_heart_tribulation_log(
                    storage,
                    task,
                    step="completed",
                    event_type="settlement_recorded",
                    message_id=current_message_id,
                    sender_id=current_sender_id,
                    sender_username=sender_username,
                    text=current_text,
                )
                return True
            return False
        if tribulation_msg_id <= 0 or current_message_id != tribulation_msg_id or not is_edited_event:
            return False
        if matched_bot_id > 0 and current_sender_id != matched_bot_id:
            # 星宫/心劫链路里不同阶段的回包可能由不同的允许 bot 发出，
            # 编辑阶段只要求仍然是允许的星宫 bot，避免把真实的成功编辑误过滤。
            if current_sender_id not in _binding_bot_ids(context):
                return False

        current_fingerprint = _build_companion_heart_tribulation_event_fingerprint(
            message_id=current_message_id,
            text=current_text,
            event_kind="edited",
        )
        if current_fingerprint == str(task.get("last_progress_fingerprint") or ""):
            return True

        _append_companion_heart_tribulation_log(
            storage,
            task,
            step=workflow_state,
            event_type="message_edited",
            message_id=current_message_id,
            reply_to_msg_id=current_reply_to_msg_id,
            sender_id=current_sender_id,
            sender_username=sender_username,
            text=current_text,
        )

        storage.update_companion_heart_tribulation_task(
            task_id,
            step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
            last_progress_at=time.time(),
            last_progress_fingerprint=current_fingerprint,
        )
        task = storage.update_companion_heart_tribulation_task(task_id) or task

        if COMPANION_HEART_TRIBULATION_SETTLEMENT_KEYWORD in current_text:
            previous_settlement_text = str(task.get("last_settlement_text") or "")
            previous_settlement_at = float(task.get("last_settlement_at") or 0)
            completed_run_id = str(task.get("run_id") or "")
            updated_task = storage.update_companion_heart_tribulation_task(
                task_id,
                workflow_state=COMPANION_HEART_TRIBULATION_IDLE_STATE,
                step_deadline_at=0,
                matched_bot_id=0,
                anchor_command_msg_id=0,
                anchor_bot_msg_id=0,
                tribulation_command_msg_id=0,
                tribulation_msg_id=0,
                panel_reply_msg_id=0,
                last_action_round_sent=0,
                last_tribulation_command_at=0,
                last_progress_at=time.time(),
                last_progress_fingerprint=current_fingerprint,
                last_stable_sent_at=0,
                last_settlement_text=current_text,
                last_settlement_at=time.time(),
                previous_settlement_text=previous_settlement_text,
                previous_settlement_at=previous_settlement_at,
                last_error="",
            )
            task = updated_task or task
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step="completed",
                event_type="settlement_recorded",
                message_id=current_message_id,
                sender_id=current_sender_id,
                sender_username=sender_username,
                text=current_text,
            )
            fresh_payload = await asyncio.to_thread(
                _refresh_companion_payload, storage, context.profile.id
            )
            if not fresh_payload or not isinstance(fresh_payload, dict):
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="结算后刷新侍妾冷却失败，已停止自动共历心劫。",
                    step="post_settlement_refresh",
                )
                return True
            next_run_at = _resolve_companion_heart_tribulation_next_run_at(fresh_payload)
            if next_run_at is None:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="结算后无法解析最新共历心劫冷却，已停止自动。",
                    step="post_settlement_cooldown",
                )
                return True
            storage.update_companion_heart_tribulation_task(
                task_id,
                enabled=1,
                run_id="",
                workflow_state=COMPANION_HEART_TRIBULATION_IDLE_STATE,
                next_run_at=next_run_at,
                step_deadline_at=0,
            )
            return True

        if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_ROUND1_EDIT_STATE:
            if COMPANION_HEART_TRIBULATION_ROUND1_LOCK_KEYWORD not in current_text:
                return True
            round2_command = _build_companion_heart_tribulation_action_command(task, 2)
            try:
                action_message = await _send_companion_heart_tribulation_command(
                    context.client,
                    storage,
                    task,
                    text=round2_command,
                    reply_to_msg_id=current_message_id,
                )
            except Exception as exc:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="发送第二轮心劫策略失败，已停止自动共历心劫。",
                    step="send_round2",
                    detail={"error": str(exc), "command": round2_command},
                )
                return True
            storage.update_companion_heart_tribulation_task(
                task_id,
                workflow_state=COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE,
                step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
                last_action_round_sent=2,
                last_progress_at=time.time(),
                last_progress_fingerprint=current_fingerprint,
                last_stable_sent_at=time.time(),
                round_retry_count=0,
                round_retry_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_ROUND_RETRY_SECONDS,
                last_error="",
            )
            task = storage.get_companion_heart_tribulation_task(
                context.profile.id,
                context.chat_id,
                thread_id=context.thread_id,
            ) or task
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step=COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE,
                event_type="send_round2",
                message_id=int(getattr(action_message, "id", 0) or 0),
                reply_to_msg_id=current_message_id,
                text=round2_command,
            )
            return True

        if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_ROUND2_EDIT_STATE:
            if COMPANION_HEART_TRIBULATION_ROUND2_LOCK_KEYWORD not in current_text:
                return True
            round3_command = _build_companion_heart_tribulation_action_command(task, 3)
            try:
                action_message = await _send_companion_heart_tribulation_command(
                    context.client,
                    storage,
                    task,
                    text=round3_command,
                    reply_to_msg_id=current_message_id,
                )
            except Exception as exc:
                _stop_companion_heart_tribulation_task(
                    storage,
                    task,
                    last_error="发送第三轮心劫策略失败，已停止自动共历心劫。",
                    step="send_round3",
                    detail={"error": str(exc), "command": round3_command},
                )
                return True
            storage.update_companion_heart_tribulation_task(
                task_id,
                workflow_state=COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE,
                step_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_EDIT_STALL_SECONDS,
                last_action_round_sent=3,
                last_progress_at=time.time(),
                last_progress_fingerprint=current_fingerprint,
                last_stable_sent_at=time.time(),
                round_retry_count=0,
                round_retry_deadline_at=time.time() + COMPANION_HEART_TRIBULATION_ROUND_RETRY_SECONDS,
                last_error="",
            )
            task = storage.get_companion_heart_tribulation_task(
                context.profile.id,
                context.chat_id,
                thread_id=context.thread_id,
            ) or task
            _append_companion_heart_tribulation_log(
                storage,
                task,
                step=COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE,
                event_type="send_round3",
                message_id=int(getattr(action_message, "id", 0) or 0),
                reply_to_msg_id=current_message_id,
                text=round3_command,
            )
            return True

        if workflow_state == COMPANION_HEART_TRIBULATION_AWAIT_SETTLEMENT_STATE:
            return True

        return False

    async def _maybe_advance_divination_batch(
        self, context: EventContext, storage: Storage
    ) -> None:
        if not context.profile or context.chat_id is None:
            return
        batch = storage.get_active_divination_batch(context.profile.id, context.chat_id)
        if not batch:
            return

        if context.is_outgoing:
            if context.text.strip() != DIVINATION_COMMAND or not context.message_id:
                return
            storage.update_divination_batch(
                int(batch["id"]),
                thread_id=context.thread_id or batch.get("thread_id"),
                sent_count=max(int(batch.get("sent_count") or 0), 0) + 1,
                pending_command_msg_id=0,
            )
        return

    async def _maybe_resume_idle_divination_batch(
        self,
        context: EventContext,
        storage: Storage,
        batch: dict,
        planned_rounds: int,
    ) -> bool:
        if planned_rounds <= 0:
            storage.finish_divination_batch(int(batch["id"]), status="completed")
            return True

        pending_command_msg_id = int(batch.get("pending_command_msg_id") or 0)
        if pending_command_msg_id:
            return False

        completed_count = max(int(batch.get("completed_count") or 0), 0)
        if completed_count >= planned_rounds:
            storage.finish_divination_batch(int(batch["id"]), status="completed")
            return True

        thread_id = int(batch.get("thread_id")) if batch.get("thread_id") else None
        latest_command = storage.get_latest_outgoing_command(
            int(batch.get("chat_id") or context.chat_id),
            profile_id=context.profile.id,
            text=DIVINATION_COMMAND,
            thread_id=thread_id,
        )
        if latest_command:
            latest_status = str(latest_command.get("status") or "").strip()
            latest_updated_at = float(latest_command.get("updated_at") or 0)
            batch_updated_at = float(batch.get("updated_at") or 0)
            if latest_status in {"pending", "sending"}:
                return True
            if latest_status == "sent" and latest_updated_at >= batch_updated_at:
                return True

        storage.enqueue_outgoing_command(
            profile_id=context.profile.id,
            chat_id=int(batch.get("chat_id") or context.chat_id),
            text=DIVINATION_COMMAND,
            thread_id=thread_id,
            chat_type=str(batch.get("chat_type") or "group"),
            bot_username=str(batch.get("bot_username") or ""),
        )
        return True

    async def _send(self, context: EventContext, command_text: str) -> bool:
        await send_message_with_thread_fallback(
            context.client,
            context.chat_id,
            command_text,
            thread_id=context.thread_id,
            storage=None,
            profile_id=context.profile.id if context.profile else None,
            bot_username=(
                context.chat_binding.bot_username if context.chat_binding else ""
            ),
            log_prefix="Runtime executor",
        )
        await context.reply(f"执行结果: sent `{command_text}`")
        return True

    async def _handle_huangfeng_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        session = sect_game.get_session(
            db,
            chat_id,
            profile_id=context.profile.id if context.profile else None,
        )
        parts = (payload or "").split(maxsplit=2)
        action = parts[0].lower() if parts else ""
        command_texts = []
        if action == "garden":
            command_texts = [".小药园"]
        elif action == "sow":
            if len(parts) >= 3:
                command_texts = [f".播种 {parts[1]} {parts[2]}"]
            elif len(parts) >= 2:
                command_texts = [f".播种 {parts[1]}"]
        elif action == "harvest":
            if len(parts) >= 2:
                command_texts = [f".采药 {parts[1]}"]
            else:
                command_texts = [".采药"]
        elif action == "weed":
            if len(parts) >= 2:
                command_texts = [f".除草 {parts[1]}"]
            else:
                command_texts = [".除草"]
        elif action == "bug":
            if len(parts) >= 2:
                command_texts = [f".除虫 {parts[1]}"]
            else:
                command_texts = [".除虫"]
        elif action == "water":
            if len(parts) >= 2:
                command_texts = [f".浇水 {parts[1]}"]
            else:
                command_texts = [".浇水"]
        elif action == "expand":
            command_texts = [".扩建药园"]
        elif action == "auto":
            auto_body = parts[1] if len(parts) >= 2 else ""
            if len(parts) >= 3:
                auto_body = f"{parts[1]} {parts[2]}".strip()
            auto_parts = auto_body.split(maxsplit=1)
            auto_action = auto_parts[0].lower() if auto_parts else "status"
            auto_payload = auto_parts[1].strip() if len(auto_parts) > 1 else ""
            if auto_action == "on":
                seed_name = (
                    auto_payload
                    or str((session or {}).get("huangfeng_seed_name") or "").strip()
                )
                if not seed_name:
                    await context.reply("用法: .sect hf auto on 种子名")
                    return True
                sect_game.configure_huangfeng_auto(
                    db,
                    chat_id,
                    True,
                    seed_name=seed_name,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(f"黄枫谷自动化已开启，播种种子为 {seed_name}。")
                return True
            if auto_action == "off":
                sect_game.configure_huangfeng_auto(
                    db,
                    chat_id,
                    False,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("黄枫谷自动化已关闭。")
                return True
            if auto_action == "seed":
                if not auto_payload:
                    await context.reply("用法: .sect hf auto seed 种子名")
                    return True
                sect_game.set_huangfeng_seed(
                    db,
                    chat_id,
                    auto_payload,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(f"黄枫谷自动播种种子已设置为 {auto_payload}。")
                return True
            if auto_action == "exchange":
                enabled = auto_payload.lower() == "on"
                sect_game.set_huangfeng_exchange_auto(
                    db,
                    chat_id,
                    enabled,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(
                    f"黄枫谷自动兑换种子已{'开启' if enabled else '关闭'}。"
                )
                return True
            if auto_action == "status":
                refreshed_session = sect_game.get_session(
                    db,
                    chat_id,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(
                    "\n".join(
                        [
                            "黄枫谷自动化状态",
                            f"开关: {'开启' if refreshed_session.get('auto_huangfeng_enabled') else '关闭'}",
                            f"播种种子: {refreshed_session.get('huangfeng_seed_name') or '-'}",
                            f"自动兑换: {'开启' if refreshed_session.get('auto_huangfeng_exchange_enabled') else '关闭'}",
                            f"下次检查: {sect_game.format_timestamp(refreshed_session.get('huangfeng_next_check_time') or 0)}",
                            f"状态来源: {refreshed_session.get('huangfeng_next_check_source') or '-'}",
                        ]
                    )
                )
                return True
        if not command_texts:
            await context.reply(
                "用法: .sect hf garden|sow [地块] 种子|harvest [地块]|weed [地块]|bug [地块]|water [地块]|expand|auto on 种子|off|seed 种子|exchange on|off|status"
            )
            return True
        if len(command_texts) > 1 and not session:
            await context.reply(
                "黄枫谷会话未初始化，请先执行 .sect on 或 .sect hf garden。"
            )
            return True
        if len(command_texts) > 1 and not sect_game._get_huangfeng_known_plots(session):
            await context.reply(
                "缺少最近药园状态，请先执行 .sect hf garden 后再省略地块。"
            )
            return True
        command_text = command_texts[0]
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        if status == "sent" and len(command_texts) > 1:
            storage = getattr(context.client, "_tg_game_storage", None)
            if storage and context.profile:
                for index, extra_command in enumerate(command_texts[1:], start=1):
                    storage.enqueue_outgoing_command(
                        profile_id=context.profile.id,
                        chat_id=chat_id,
                        text=extra_command,
                        thread_id=session.get("thread_id")
                        if session
                        else context.thread_id,
                        chat_type="group",
                        bot_username=(
                            context.chat_binding.bot_username
                            if context.chat_binding
                            else ""
                        ),
                        delay_seconds=index * 3,
                    )
                await context.reply(
                    f"执行结果: {status}，已按最近药园状态为全部地块排队 {len(command_texts)} 条命令。"
                )
                return True
        await context.reply(f"执行结果: {status}")
        return True
    async def _handle_taiyi_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        parts = (payload or "").split(maxsplit=1)
        action = parts[0].lower() if parts else ""
        argument = parts[1] if len(parts) > 1 else ""
        command_text = None
        if action == "guide":
            if argument not in {"金", "木", "水", "火", "土"}:
                await context.reply("用法: .sect ty guide 金|木|水|火|土")
                return True
            command_text = f".引道 {argument}"
        elif action == "shock":
            command_text = ".神识冲击"
        if not command_text:
            await context.reply("用法: .sect ty guide 金|木|水|火|土|shock")
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True
    async def _handle_wanling_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        parts = (payload or "").split(maxsplit=2)
        action = parts[0].lower() if parts else ""
        command_text = None
        if action == "search":
            command_text = ".寻觅灵兽"
        elif action == "status":
            command_text = ".我的灵兽"
        elif action == "feed" and len(parts) >= 3:
            command_text = f".喂养 {parts[1]} {parts[2]}"
        elif action == "battle" and len(parts) >= 2:
            command_text = f".灵兽出战 {parts[1]}"
        elif action == "rest":
            command_text = ".灵兽休息"
        elif action == "farm":
            command_text = ".一键放养"
        elif action == "steal":
            command_text = ".灵兽偷菜"
        elif action == "abyss":
            command_text = ".探渊"
        if not command_text:
            await context.reply(
                "用法: .sect wl search|status|feed 灵兽 物品*数量|battle 灵兽|rest|farm|steal|abyss"
            )
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True
    async def _handle_lingxiao_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        action = (payload or "").strip().lower()
        command_text = None
        if action == "status":
            command_text = ".天阶状态"
        elif action == "mind":
            command_text = ".问心台"
        elif action == "step":
            command_text = ".登天阶"
        elif action == "wind":
            command_text = ".引九天罡风"
        elif action == "gate":
            command_text = ".借天门势"
        elif action == "overview":
            command_text = ".凌霄宫"
        if not command_text:
            await context.reply("用法: .sect lx overview|status|mind|step|wind|gate")
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True
    async def _handle_xingong_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        parts = payload.split(maxsplit=2)
        if not parts:
            await context.reply(
                "用法: .sect xg matrix|assist|starboard|pull 编号 星辰|collect 编号|soothe 编号|divine|shift @目标|companion"
            )
            return True
        action = parts[0].lower()
        command_text = None
        if action == "matrix":
            command_text = ".启阵"
        elif action == "assist":
            command_text = ".助阵"
        elif action == "starboard":
            command_text = ".观星台"
        elif action == "pull" and len(parts) >= 3:
            command_text = f".牵引星辰 {parts[1]} {parts[2]}"
        elif action == "collect" and len(parts) >= 2:
            command_text = f".收集精华 {parts[1]}"
        elif action == "soothe" and len(parts) >= 2:
            command_text = f".安抚星辰 {parts[1]}"
        elif action == "divine":
            command_text = ".观星"
        elif action == "shift" and len(parts) >= 2:
            command_text = f".改换星移 {parts[1]}"
        elif action == "companion":
            command_text = ".我的侍妾"
        if not command_text:
            await context.reply(
                "用法: .sect xg matrix|assist|starboard|pull 编号 星辰|collect 编号|soothe 编号|divine|shift @目标|companion"
            )
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True


async def observe_companion_heart_tribulation_event(
    context: EventContext, storage: Storage
) -> bool:
    observer = GeneralGameExecutor()
    return await observer._maybe_advance_companion_heart_tribulation(context, storage)
