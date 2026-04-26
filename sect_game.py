import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta

from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_cultivator_username,
    get_effective_external_cookie,
    mark_external_account_failure,
    read_cached_external_payload,
    sync_external_account,
)
from tg_game.storage import CompatDb as RuntimeDb
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)


SECT_BOT_USERNAME = "fanrenxiuxian_bot"
SECT_CHECK_COMMAND = ".我的宗门"
SECT_DEFAULT_INTERVAL = 1800
SECT_COMMAND_COOLDOWN = 15
SECT_RUNNER_POLL_SECONDS = 5
SECT_DAILY_TEACH_LIMIT = 3
SECT_AUTO_CHECK_IN_TIME = "02:00"
SECT_AUTO_TEACH_TIME = "02:10"
YINLUO_AUTO_SACRIFICE_TIME = "02:20"
SECT_AUTO_TEACH_REPLY_RECHECK_SECONDS = 30
LINGXIAO_STEP_DEFAULT_SECONDS = 7200
LINGXIAO_STEP_SECONDS = 14400
LINGXIAO_ELDER_STEP_SECONDS = 10800
LINGXIAO_GANGFENG_SECONDS = 12 * 3600
LINGXIAO_BORROW_SECONDS = 18 * 3600
LINGXIAO_GANGFENG_HEART_RECHECK_SECONDS = 10 * 60
LINGXIAO_QUESTION_RECHECK_SECONDS = 2 * 3600
LINGXIAO_COMMAND_REFRESH_SECONDS = 180
LINGXIAO_ACTION_SYNC_TIMEOUT_SECONDS = 15 * 60
YINLUO_BLOOD_WASH_SECONDS = 4 * 3600

SECT_NAME_PATTERNS = [
    re.compile(r"(?:所在宗门|宗门名称|宗门)[:：]\s*(?P<value>[^\n]+)"),
]
SECT_POSITION_PATTERNS = [
    re.compile(r"(?:宗门职位|职位|身份)[:：]\s*(?P<value>[^\n]+)"),
]
SECT_MASTER_PATTERN = re.compile(r"掌门[:：]\s*(?P<value>[^\n]+)")
SECT_DESC_PATTERN = re.compile(r"描述[:：]\s*(?P<value>[^\n]+)")
SECT_BONUS_PATTERN = re.compile(r"修炼加成[:：]\s*(?P<value>[^\n]+)")
SECT_CONTRIBUTION_PATTERNS = [
    re.compile(r"(?:宗门贡献|贡献)[:：]\s*(?P<value>\d+)"),
    re.compile(r"获得了\s*(?P<value>\d+)\s*点宗门贡献"),
    re.compile(r"获得\s*(?P<value>\d+)\s*点宗门贡献"),
]
SECT_BONUS_PATTERNS = [
    re.compile(r"获得了\s*(?P<value>\d+)\s*点宗门贡献"),
    re.compile(r"获得\s*(?P<value>\d+)\s*点宗门贡献"),
    re.compile(r"获得了\s*(?P<value>\d+)\s*点宗门贡献加成"),
]
SECT_DAYS_PATTERN = re.compile(r"你已连续点卯\s*(?P<value>\d+)\s*天")
SECT_TEACH_USAGE_PATTERN = re.compile(
    r"今日已传功\s*(?P<value>\d+)\s*/\s*(?P<limit>\d+)\s*次"
)
YINLUO_BANNER_OWNER_PATTERN = re.compile(r"【(?P<owner>[^】]+)的阴罗幡】")
YINLUO_BANNER_RANK_PATTERN = re.compile(r"等阶[:：]\s*(?P<rank>[^\n]+)")
YINLUO_BANNER_POOL_PATTERN = re.compile(
    r"煞气池[:：]\s*(?P<current>\d+)\s*/\s*(?P<capacity>\d+)"
)
YINLUO_BANNER_SOUL_PATTERN = re.compile(
    r"-\s*(?P<name>[^:：\n]+)[:：]\s*(?P<count>\d+)\s*缕"
)
YINLUO_REFINING_SLOT_PATTERN = re.compile(
    r"(?P<index>\d+)号槽[:：]\s*\[(?P<state>[^\]]+)\](?:\s*-\s*(?P<detail>[^\n\(]+))?(?:\s*\(剩余[:：]\s*(?P<remaining>[^\)]+)\))?"
)
YINLUO_SUMMON_SHADOW_SECONDS = 24 * 3600


def _normalize_bool(value):
    return 1 if bool(value) else 0


def format_timestamp(timestamp):
    if not timestamp:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def current_date_key(now=None):
    return time.strftime("%Y-%m-%d", time.localtime(now or time.time()))


def _current_time_text(now=None):
    return time.strftime("%H:%M", time.localtime(now or time.time()))


def _time_today_timestamp(time_text, now=None):
    now = now or time.time()
    base = time.localtime(now)
    try:
        hour_text, minute_text = str(time_text or "00:00").split(":", 1)
        hour = max(0, min(int(hour_text), 23))
        minute = max(0, min(int(minute_text), 59))
    except (TypeError, ValueError):
        hour = 0
        minute = 0
    return time.mktime(
        (
            base.tm_year,
            base.tm_mon,
            base.tm_mday,
            hour,
            minute,
            0,
            base.tm_wday,
            base.tm_yday,
            base.tm_isdst,
        )
    )


def _next_daily_run_timestamp(time_text, now=None):
    now = now or time.time()
    today_run = _time_today_timestamp(time_text, now)
    if now < today_run:
        return today_run
    return today_run + 86400


def _parse_iso_timestamp(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10**12:
            return number / 1000.0
        return number
    text = str(value or "").strip()
    if not text:
        return 0
    if text.isdigit():
        number = float(text)
        if number > 10**12:
            return number / 1000.0
        return number
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return 0


def _parse_date_key(value):
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return time.strftime("%Y-%m-%d", time.localtime(float(value)))
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10:
        return text[:10]
    return text


def _parse_int(value, default=0):
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return default


def _parse_duration_seconds(value):
    text = str(value or "").strip()
    if not text:
        return 0
    total = 0
    matched = False
    for pattern, multiplier in [
        (r"(\d+)\s*天", 86400),
        (r"(\d+)\s*小?时", 3600),
        (r"(\d+)\s*分(?:钟)?", 60),
        (r"(\d+)\s*秒", 1),
    ]:
        match = re.search(pattern, text)
        if match:
            matched = True
            total += int(match.group(1)) * multiplier
    return total if matched else 0


def _read_cached_profile_payload(storage, profile_id):
    return read_cached_external_payload(storage, profile_id, ASC_PROVIDER)


def _parse_json_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def parse_yinluo_banner_text(text):
    raw_text = str(text or "").strip()
    if not raw_text:
        return {
            "owner_name": "",
            "rank_text": "",
            "sha_pool_current": 0,
            "sha_pool_capacity": 0,
            "soul_entries": [],
            "refining_slots": [],
        }
    owner_match = YINLUO_BANNER_OWNER_PATTERN.search(raw_text)
    rank_match = YINLUO_BANNER_RANK_PATTERN.search(raw_text)
    pool_match = YINLUO_BANNER_POOL_PATTERN.search(raw_text)
    soul_entries = [
        {
            "name": str(match.group("name") or "").strip(),
            "quantity": max(_parse_int(match.group("count"), 0), 0),
        }
        for match in YINLUO_BANNER_SOUL_PATTERN.finditer(raw_text)
        if str(match.group("name") or "").strip()
    ]
    refining_slots = []
    for match in YINLUO_REFINING_SLOT_PATTERN.finditer(raw_text):
        remaining_text = str(match.group("remaining") or "").strip()
        remaining_seconds = _parse_duration_seconds(remaining_text)
        refining_slots.append(
            {
                "index": _parse_int(match.group("index"), 0),
                "state": str(match.group("state") or "").strip(),
                "detail": str(match.group("detail") or "").strip(),
                "remaining_text": remaining_text,
                "remaining_seconds": remaining_seconds,
            }
        )
    refining_slots.sort(key=lambda item: item["index"])
    return {
        "owner_name": str(
            (owner_match.group("owner") if owner_match else "") or ""
        ).strip(),
        "rank_text": str(
            (rank_match.group("rank") if rank_match else "") or ""
        ).strip(),
        "sha_pool_current": _parse_int(
            pool_match.group("current") if pool_match else 0, 0
        ),
        "sha_pool_capacity": _parse_int(
            pool_match.group("capacity") if pool_match else 0, 0
        ),
        "soul_entries": soul_entries,
        "refining_slots": refining_slots,
    }


def _is_guard_one(heart_state):
    return (heart_state or "").strip() == "守一"


def _is_clear_heart(heart_state):
    return (heart_state or "").strip() == "澄明"


def _has_heart_state(heart_state):
    return bool(str(heart_state or "").strip())


def _next_day_start(last_question_date, now=None):
    date_key = _parse_date_key(last_question_date)
    if not date_key:
        return 0
    base_date = None
    try:
        base_date = datetime.strptime(date_key, "%Y-%m-%d")
    except ValueError:
        return 0
    return (base_date + timedelta(days=1)).timestamp()


def _extract_trial_payload(payload):
    if not isinstance(payload, dict):
        return {}
    trial_state = payload.get("lingxiao_trial_state") or {}
    if isinstance(trial_state, str) and trial_state.strip():
        try:
            parsed = json.loads(trial_state)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return trial_state if isinstance(trial_state, dict) else {}


def _extract_sect_daily_state(payload, now=None):
    now = now or time.time()
    payload = payload if isinstance(payload, dict) else {}
    last_check_in_time = _parse_iso_timestamp(payload.get("last_sect_check_in"))
    last_check_in_date = _parse_date_key(payload.get("last_sect_check_in"))
    checked_in_today = last_check_in_date == current_date_key(now)
    consecutive_days = _parse_int(payload.get("consecutive_check_in_days"), 0)
    last_teach_date = _parse_date_key(payload.get("last_teach_date"))
    teach_count = _parse_int(payload.get("teach_count"), 0)
    if last_teach_date != current_date_key(now):
        teach_count = 0
    return {
        "last_check_in_time": last_check_in_time,
        "checked_in_today": checked_in_today,
        "consecutive_days": consecutive_days,
        "last_teach_date": last_teach_date,
        "teach_count": max(teach_count, 0),
    }


def _resolve_lingxiao_step_seconds(payload, sect_position=""):
    if _parse_int((payload or {}).get("is_grand_elder")):
        return LINGXIAO_ELDER_STEP_SECONDS
    if _parse_int((payload or {}).get("is_sect_elder")):
        return LINGXIAO_ELDER_STEP_SECONDS
    position_text = str(sect_position or "")
    if "长老" in position_text:
        return LINGXIAO_ELDER_STEP_SECONDS
    return LINGXIAO_STEP_SECONDS


def build_lingxiao_view(payload, session=None, sect_position="", now=None):
    now = now or time.time()
    if not isinstance(payload, dict):
        payload = {}
    trial_state = _extract_trial_payload(payload)
    if not trial_state:
        return None
    step = _parse_int(trial_state.get("step"))
    cycles = _parse_int(trial_state.get("cycles"))
    body_temper = _parse_int(trial_state.get("body_temper"))
    heart_state = str(trial_state.get("heart_state") or "").strip()
    last_climb_time = _parse_iso_timestamp(trial_state.get("last_climb_time"))
    last_gangfeng_time = _parse_iso_timestamp(trial_state.get("last_gangfeng_art_time"))
    last_borrow_time = _parse_iso_timestamp(trial_state.get("last_borrow_tianmen_time"))
    last_question_date = _parse_date_key(trial_state.get("last_question_date"))
    step_cooldown_seconds = _resolve_lingxiao_step_seconds(payload, sect_position)
    climb_ready_time = last_climb_time + step_cooldown_seconds if last_climb_time else 0
    gangfeng_ready_time = (
        last_gangfeng_time + LINGXIAO_GANGFENG_SECONDS if last_gangfeng_time else 0
    )
    borrow_ready_time = (
        last_borrow_time + LINGXIAO_BORROW_SECONDS if last_borrow_time else 0
    )
    questioned_today = last_question_date == current_date_key(now)
    question_ready_time = (
        0 if not questioned_today else _next_day_start(last_question_date, now)
    )
    return {
        "step": step,
        "step_display": f"{step}/12",
        "cycles": cycles,
        "body_temper": body_temper,
        "body_temper_display": f"{body_temper}/12",
        "heart_state": heart_state,
        "last_climb_time": last_climb_time,
        "climb_ready_time": climb_ready_time,
        "last_gangfeng_time": last_gangfeng_time,
        "gangfeng_ready_time": gangfeng_ready_time,
        "last_borrow_time": last_borrow_time,
        "borrow_ready_time": borrow_ready_time,
        "last_question_date": last_question_date,
        "questioned_today": questioned_today,
        "question_ready_time": question_ready_time,
        "step_cooldown_seconds": step_cooldown_seconds,
        "auto_step_enabled": bool((session or {}).get("auto_lingxiao_enabled")),
        "auto_gangfeng_enabled": bool(
            (session or {}).get("auto_lingxiao_gangfeng_enabled")
        ),
        "auto_borrow_enabled": bool(
            (session or {}).get("auto_lingxiao_borrow_enabled")
        ),
        "auto_question_enabled": bool(
            (session or {}).get("auto_lingxiao_question_enabled")
        ),
    }


def build_yinluo_view(
    payload,
    session=None,
    now=None,
    banner_text="",
    summon_shadow_reply=None,
):
    now = now or time.time()
    if not isinstance(payload, dict):
        payload = {}
    banner_view = parse_yinluo_banner_text(banner_text)
    soul_pouch = _parse_json_dict(payload.get("soul_pouch"))
    payload_soul_entries = [
        {"name": str(name or "").strip(), "quantity": max(_parse_int(quantity, 0), 0)}
        for name, quantity in soul_pouch.items()
        if str(name or "").strip() and _parse_int(quantity, 0) > 0
    ]
    payload_soul_entries.sort(key=lambda item: (-item["quantity"], item["name"]))
    soul_pouch_entries = banner_view["soul_entries"] or payload_soul_entries
    last_blood_wash_time = _parse_iso_timestamp(payload.get("last_blood_wash_time"))
    blood_wash_ready_time = (
        last_blood_wash_time + YINLUO_BLOOD_WASH_SECONDS if last_blood_wash_time else 0
    )
    last_summon_shadow_time = _parse_iso_timestamp(
        payload.get("last_summon_shadow_time")
    )
    summon_shadow_reply = (
        summon_shadow_reply if isinstance(summon_shadow_reply, dict) else {}
    )
    if not last_summon_shadow_time:
        last_summon_shadow_time = _parse_iso_timestamp(
            summon_shadow_reply.get("created_at")
        )
    summon_shadow_ready_time = (
        last_summon_shadow_time + YINLUO_SUMMON_SHADOW_SECONDS
        if last_summon_shadow_time
        else 0
    )
    last_battle_date = _parse_date_key(payload.get("last_battle_date"))
    daily_battle_stamina = max(_parse_int(payload.get("daily_battle_stamina"), 0), 0)
    last_sacrifice_date = _parse_date_key(
        (session or {}).get("last_yinluo_sacrifice_date")
    )
    sacrificed_today = last_sacrifice_date == current_date_key(now)
    refining_slots = banner_view["refining_slots"]
    refining_slot_state_counts = {}
    for slot in refining_slots:
        if slot["remaining_seconds"] > 0:
            slot["ready_time"] = now + slot["remaining_seconds"]
        else:
            slot["ready_time"] = 0
        state_name = slot["state"] or "未知"
        refining_slot_state_counts[state_name] = (
            int(refining_slot_state_counts.get(state_name) or 0) + 1
        )
    return {
        "owner_name": banner_view["owner_name"],
        "rank_text": banner_view["rank_text"],
        "sha_pool_current": banner_view["sha_pool_current"],
        "sha_pool_capacity": banner_view["sha_pool_capacity"],
        "soul_pouch_entries": soul_pouch_entries,
        "payload_soul_pouch_entries": payload_soul_entries,
        "daily_battle_stamina": daily_battle_stamina,
        "last_battle_date": last_battle_date,
        "last_blood_wash_time": last_blood_wash_time,
        "blood_wash_ready_time": blood_wash_ready_time,
        "refining_slots": refining_slots,
        "refining_slot_total": len(refining_slots),
        "refining_slot_ready_count": sum(
            1 for slot in refining_slots if slot["state"] == "精华已成"
        ),
        "refining_slot_idle_count": sum(
            1 for slot in refining_slots if slot["state"] == "空闲"
        ),
        "refining_slot_exhausted_count": sum(
            1 for slot in refining_slots if slot["state"] == "魂力枯竭"
        ),
        "refining_slot_state_counts": refining_slot_state_counts,
        "sacrificed_today": sacrificed_today,
        "last_sacrifice_date": last_sacrifice_date,
        "banner_text": str(banner_text or "").strip(),
        "last_summon_shadow_time": last_summon_shadow_time,
        "summon_shadow_ready_time": summon_shadow_ready_time,
        "summon_shadow_reply_text": str(summon_shadow_reply.get("text") or "").strip(),
        "summon_shadow_reply_time": _parse_iso_timestamp(
            summon_shadow_reply.get("created_at")
        ),
        "auto_blood_wash_enabled": bool(
            (session or {}).get("auto_yinluo_blood_wash_enabled")
        ),
        "auto_sacrifice_enabled": bool(
            (session or {}).get("auto_yinluo_sacrifice_enabled")
        ),
    }


def _lingxiao_sync_error_updates(message, now=None, session=None):
    now = now or time.time()
    retry_time = now + 1800
    updates = {
        "last_summary": message,
        "next_check_time": retry_time,
        "next_check_source": message,
    }
    session = session or {}
    for enabled_key, next_key, source_key in [
        (
            "auto_lingxiao_enabled",
            "lingxiao_next_check_time",
            "lingxiao_next_check_source",
        ),
        (
            "auto_lingxiao_gangfeng_enabled",
            "lingxiao_gangfeng_next_check_time",
            "lingxiao_gangfeng_next_check_source",
        ),
        (
            "auto_lingxiao_borrow_enabled",
            "lingxiao_borrow_next_check_time",
            "lingxiao_borrow_next_check_source",
        ),
        (
            "auto_lingxiao_question_enabled",
            "lingxiao_question_next_check_time",
            "lingxiao_question_next_check_source",
        ),
    ]:
        if not session.get(enabled_key):
            continue
        updates[next_key] = retry_time
        updates[source_key] = message
    return updates


def _active_lingxiao_auto_keys(session):
    keys = []
    if session.get("auto_lingxiao_enabled"):
        keys.append("step")
    if session.get("auto_lingxiao_gangfeng_enabled"):
        keys.append("gangfeng")
    if session.get("auto_lingxiao_borrow_enabled"):
        keys.append("borrow")
    if session.get("auto_lingxiao_question_enabled"):
        keys.append("question")
    return keys


def _active_common_auto_keys(session):
    keys = []
    if session.get("auto_sect_checkin_enabled"):
        keys.append("checkin")
    if session.get("auto_sect_teach_enabled"):
        keys.append("teach")
    return keys


def _active_yinluo_auto_keys(session):
    keys = []
    if session.get("auto_yinluo_sacrifice_enabled"):
        keys.append("sacrifice")
    if session.get("auto_yinluo_blood_wash_enabled"):
        keys.append("blood_wash")
    return keys


def _has_any_auto_keys(session):
    return bool(
        _active_common_auto_keys(session)
        or _active_yinluo_auto_keys(session)
        or _active_lingxiao_auto_keys(session)
    )


def _recompute_overall_next_check(session, updates, now=None):
    now = now or time.time()
    merged = dict(session or {})
    merged.update(updates or {})
    candidates = []
    for enabled_key, next_key in [
        ("auto_sect_checkin_enabled", "sect_checkin_next_check_time"),
        ("auto_sect_teach_enabled", "sect_teach_next_check_time"),
        ("auto_yinluo_sacrifice_enabled", "yinluo_sacrifice_next_check_time"),
        ("auto_yinluo_blood_wash_enabled", "yinluo_blood_wash_next_check_time"),
        ("auto_lingxiao_enabled", "lingxiao_next_check_time"),
        ("auto_lingxiao_gangfeng_enabled", "lingxiao_gangfeng_next_check_time"),
        ("auto_lingxiao_borrow_enabled", "lingxiao_borrow_next_check_time"),
        ("auto_lingxiao_question_enabled", "lingxiao_question_next_check_time"),
    ]:
        if not merged.get(enabled_key):
            continue
        next_time = float(merged.get(next_key) or 0)
        if not next_time or next_time <= now:
            return 0
        candidates.append(next_time)
    return min(candidates) if candidates else merged.get("next_check_time") or 0


def _lingxiao_action_still_syncing(
    session,
    now,
    *,
    command_text: str,
    observed_time: float = 0,
) -> bool:
    last_action = str((session or {}).get("last_action") or "").strip()
    last_action_time = float((session or {}).get("last_action_time") or 0)
    if last_action != str(command_text or "").strip() or not last_action_time:
        return False
    if float(observed_time or 0) >= max(last_action_time - 1, 0):
        return False
    return now - last_action_time < LINGXIAO_ACTION_SYNC_TIMEOUT_SECONDS


def _lingxiao_sync_retry_time(session, now) -> float:
    last_action_time = float((session or {}).get("last_action_time") or 0)
    hard_deadline = (
        last_action_time + LINGXIAO_ACTION_SYNC_TIMEOUT_SECONDS
        if last_action_time
        else now + LINGXIAO_COMMAND_REFRESH_SECONDS
    )
    return min(hard_deadline, now + LINGXIAO_COMMAND_REFRESH_SECONDS)


def sync_common_sect_state(storage, db, profile_id, chat_id, payload=None, now=None):
    now = now or time.time()
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session:
        return None, None
    if payload is None:
        payload = _read_cached_profile_payload(storage, profile_id)
    daily = _extract_sect_daily_state(payload, now)
    session_teach_date = _parse_date_key(session.get("last_teach_date"))
    session_teach_count = max(_parse_int(session.get("last_teach_count"), 0), 0)
    if (
        session_teach_date == current_date_key(now)
        and session_teach_count > daily["teach_count"]
    ):
        daily["last_teach_date"] = session_teach_date
        daily["teach_count"] = session_teach_count
    updates = {
        "last_sign_date": _parse_date_key(payload.get("last_sect_check_in")),
        "last_teach_date": daily["last_teach_date"] or None,
        "last_teach_count": daily["teach_count"],
    }

    if session.get("auto_sect_checkin_enabled"):
        next_check_in_time = _next_daily_run_timestamp(SECT_AUTO_CHECK_IN_TIME, now)
        if daily["checked_in_today"]:
            updates["sect_checkin_next_check_time"] = next_check_in_time
            updates["sect_checkin_next_check_source"] = "今日已点卯，等待次日 02:00"
        else:
            today_check_in_time = _time_today_timestamp(SECT_AUTO_CHECK_IN_TIME, now)
            if now < today_check_in_time:
                updates["sect_checkin_next_check_time"] = today_check_in_time
                updates["sect_checkin_next_check_source"] = "等待 02:00 执行宗门点卯"
            else:
                updates["sect_checkin_next_check_time"] = 0
                updates["sect_checkin_next_check_source"] = "可执行宗门点卯"

    if session.get("auto_sect_teach_enabled"):
        next_teach_time = _next_daily_run_timestamp(SECT_AUTO_TEACH_TIME, now)
        if daily["teach_count"] >= SECT_DAILY_TEACH_LIMIT:
            updates["sect_teach_next_check_time"] = next_teach_time
            updates["sect_teach_next_check_source"] = (
                f"今日已传功 {daily['teach_count']}/{SECT_DAILY_TEACH_LIMIT}，等待次日 02:10"
            )
        else:
            today_teach_time = _time_today_timestamp(SECT_AUTO_TEACH_TIME, now)
            if now < today_teach_time:
                updates["sect_teach_next_check_time"] = today_teach_time
                updates["sect_teach_next_check_source"] = "等待 02:10 执行宗门传功"
            else:
                updates["sect_teach_next_check_time"] = 0
                updates["sect_teach_next_check_source"] = (
                    f"可执行宗门传功 ({daily['teach_count']}/{SECT_DAILY_TEACH_LIMIT})"
                )

    updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
    if _has_any_auto_keys(session):
        updates["next_check_source"] = "已同步宗门缓存状态"
    update_session(db, chat_id, profile_id=profile_id, **updates)
    return get_session(db, chat_id, profile_id=profile_id), daily


def sync_yinluo_state(storage, db, profile_id, chat_id, payload=None, now=None):
    now = now or time.time()
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session:
        return None, None
    if payload is None:
        payload = _read_cached_profile_payload(storage, profile_id)
    view = build_yinluo_view(payload, session=session, now=now)
    updates = {}
    if session.get("auto_yinluo_sacrifice_enabled"):
        if view["sacrificed_today"]:
            updates["yinluo_sacrifice_next_check_time"] = _next_daily_run_timestamp(
                YINLUO_AUTO_SACRIFICE_TIME, now
            )
            updates["yinluo_sacrifice_next_check_source"] = (
                f"今日已献祭，等待次日 {YINLUO_AUTO_SACRIFICE_TIME}"
            )
        else:
            today_sacrifice_time = _time_today_timestamp(
                YINLUO_AUTO_SACRIFICE_TIME, now
            )
            if now < today_sacrifice_time:
                updates["yinluo_sacrifice_next_check_time"] = today_sacrifice_time
                updates["yinluo_sacrifice_next_check_source"] = (
                    f"等待 {YINLUO_AUTO_SACRIFICE_TIME} 执行每日献祭"
                )
            else:
                updates["yinluo_sacrifice_next_check_time"] = 0
                updates["yinluo_sacrifice_next_check_source"] = "可执行每日献祭"
    if session.get("auto_yinluo_blood_wash_enabled"):
        if view["daily_battle_stamina"] <= 0 and view[
            "last_battle_date"
        ] == current_date_key(now):
            updates["yinluo_blood_wash_next_check_time"] = _next_day_start(
                view["last_battle_date"], now
            )
            updates["yinluo_blood_wash_next_check_source"] = (
                "今日剩余斗法次数为 0，等待次日恢复"
            )
        elif view["blood_wash_ready_time"] and view["blood_wash_ready_time"] > now:
            updates["yinluo_blood_wash_next_check_time"] = view["blood_wash_ready_time"]
            updates["yinluo_blood_wash_next_check_source"] = "血洗山林冷却中"
        else:
            updates["yinluo_blood_wash_next_check_time"] = 0
            updates["yinluo_blood_wash_next_check_source"] = "可执行血洗山林"
    updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
    if _has_any_auto_keys(session):
        updates["next_check_source"] = "已同步宗门缓存状态"
    update_session(db, chat_id, profile_id=profile_id, **updates)
    return get_session(db, chat_id, profile_id=profile_id), view


def sync_lingxiao_trial_state(storage, db, profile_id, chat_id, payload=None):
    now = time.time()
    profile = storage.get_profile(profile_id)
    if not profile:
        session = get_session(db, chat_id, profile_id=profile_id)
        update_session(
            db,
            chat_id,
            profile_id=profile_id,
            **_lingxiao_sync_error_updates("角色不存在", now, session),
        )
        return get_session(db, chat_id, profile_id=profile_id), None
    session = get_session(db, chat_id, profile_id=profile_id)
    external_account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
    external_status = str(external_account.get("status") or "").strip().lower()
    if payload is not None and external_status and external_status != "connected":
        message = "天机阁会话已失效，无法同步凌霄宫状态"
        update_session(
            db,
            chat_id,
            profile_id=profile_id,
            **_lingxiao_sync_error_updates(message, now, session),
        )
        return get_session(db, chat_id, profile_id=profile_id), None
    if payload is None:
        username = get_cultivator_username(profile)
        default_cookie = get_effective_external_cookie(storage)
        cookie_text = (external_account.get("cookie_text") or default_cookie).strip()
        if not username or not cookie_text:
            message = "缺少天机阁 cookie 或 Telegram 用户名，无法同步凌霄宫状态"
            update_session(
                db,
                chat_id,
                profile_id=profile_id,
                **_lingxiao_sync_error_updates(message, now, session),
            )
            return get_session(db, chat_id, profile_id=profile_id), None
        try:
            payload = sync_external_account(
                storage, profile_id, cookie_text=cookie_text
            )
        except Exception as exc:
            mark_external_account_failure(
                storage, profile_id, exc, cookie_text=cookie_text
            )
            update_session(
                db,
                chat_id,
                profile_id=profile_id,
                **_lingxiao_sync_error_updates(
                    f"凌霄宫状态同步失败: {exc}", now, session
                ),
            )
            return get_session(db, chat_id, profile_id=profile_id), None

    view = build_lingxiao_view(
        payload, session=session, sect_position=profile.sect_position, now=now
    )
    if not view:
        message = "天机阁未返回 lingxiao_trial_state"
        update_session(
            db,
            chat_id,
            profile_id=profile_id,
            **_lingxiao_sync_error_updates(message, now, session),
        )
        return get_session(db, chat_id, profile_id=profile_id), None

    updates = {
        "last_panel_time": now,
    }

    if session.get("auto_lingxiao_enabled"):
        step_next = float(view["climb_ready_time"] or 0)
        if _lingxiao_action_still_syncing(
            session,
            now,
            command_text=".登天阶",
            observed_time=float(view["last_climb_time"] or 0),
        ):
            pending_time = _lingxiao_sync_retry_time(session, now)
            updates["lingxiao_next_check_time"] = pending_time
            updates["lingxiao_next_check_source"] = (
                "已发送 .登天阶，等待天机阁同步云阶状态"
            )
        else:
            updates["lingxiao_next_check_time"] = step_next if step_next > now else 0
            updates["lingxiao_next_check_source"] = (
                f"登天阶冷却至 {format_timestamp(step_next)}"
                if step_next > now
                else "登天阶已到时，可执行"
            )

    if session.get("auto_lingxiao_gangfeng_enabled"):
        gangfeng_ready = float(view["gangfeng_ready_time"] or 0)
        existing_gangfeng_next = float(
            session.get("lingxiao_gangfeng_next_check_time") or 0
        )
        existing_gangfeng_source = str(
            session.get("lingxiao_gangfeng_next_check_source") or ""
        ).strip()
        if _lingxiao_action_still_syncing(
            session,
            now,
            command_text=".引九天罡风",
            observed_time=float(view["last_gangfeng_time"] or 0),
        ):
            pending_time = _lingxiao_sync_retry_time(session, now)
            updates["lingxiao_gangfeng_next_check_time"] = pending_time
            updates["lingxiao_gangfeng_next_check_source"] = (
                "已发送 .引九天罡风，等待天机阁同步淬体状态"
            )
        elif gangfeng_ready > now:
            updates["lingxiao_gangfeng_next_check_time"] = gangfeng_ready
            updates["lingxiao_gangfeng_next_check_source"] = (
                f"引九天罡风冷却至 {format_timestamp(gangfeng_ready)}"
            )
        elif existing_gangfeng_next > now and existing_gangfeng_source.startswith(
            "引九天罡风冷却至"
        ):
            updates["lingxiao_gangfeng_next_check_time"] = existing_gangfeng_next
            updates["lingxiao_gangfeng_next_check_source"] = existing_gangfeng_source
        elif _has_heart_state(view["heart_state"]):
            updates["lingxiao_gangfeng_next_check_time"] = (
                now + LINGXIAO_GANGFENG_HEART_RECHECK_SECONDS
            )
            updates["lingxiao_gangfeng_next_check_source"] = (
                f"当前心境为 {view['heart_state']}，等待清空后再引九天罡风"
            )
        else:
            updates["lingxiao_gangfeng_next_check_time"] = 0
            updates["lingxiao_gangfeng_next_check_source"] = "可执行引九天罡风"

    if session.get("auto_lingxiao_borrow_enabled"):
        borrow_ready = float(view["borrow_ready_time"] or 0)
        if _lingxiao_action_still_syncing(
            session,
            now,
            command_text=".借天门势",
            observed_time=float(view["last_borrow_time"] or 0),
        ):
            pending_time = _lingxiao_sync_retry_time(session, now)
            updates["lingxiao_borrow_next_check_time"] = pending_time
            updates["lingxiao_borrow_next_check_source"] = (
                "已发送 .借天门势，等待天机阁同步借势状态"
            )
        else:
            updates["lingxiao_borrow_next_check_time"] = (
                borrow_ready if borrow_ready > now else 0
            )
            updates["lingxiao_borrow_next_check_source"] = (
                f"借天门势冷却至 {format_timestamp(borrow_ready)}"
                if borrow_ready > now
                else "可执行借天门势"
            )

    if session.get("auto_lingxiao_question_enabled"):
        if view["questioned_today"]:
            next_question = float(view["question_ready_time"] or 0)
            updates["lingxiao_question_next_check_time"] = next_question
            updates["lingxiao_question_next_check_source"] = (
                "今日已问心，今日停止自动问心检测"
            )
        elif _has_heart_state(view["heart_state"]):
            updates["lingxiao_question_next_check_time"] = (
                now + LINGXIAO_QUESTION_RECHECK_SECONDS
            )
            updates["lingxiao_question_next_check_source"] = (
                f"当前心境为 {view['heart_state']}，等待清空后再问心"
            )
        else:
            updates["lingxiao_question_next_check_time"] = 0
            updates["lingxiao_question_next_check_source"] = "可执行问心台"

    updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
    if _active_lingxiao_auto_keys(session):
        updates["next_check_source"] = "已同步天机阁凌霄宫状态"
    update_session(db, chat_id, profile_id=profile_id, **updates)
    return get_session(db, chat_id, profile_id=profile_id), view


def _daily_time_due(session, now=None):
    now_text = _current_time_text(now)
    run_text = session.get("daily_run_time") or "00:00"
    return now_text >= run_text


def ensure_tables(db):
    db.cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sect_sessions (
            profile_id INTEGER NOT NULL DEFAULT 0,
            chat_id INTEGER NOT NULL,
            bot_username TEXT NOT NULL,
            enabled INTEGER DEFAULT 0,
            interval_seconds INTEGER DEFAULT 1800,
            command_text TEXT DEFAULT '.我的宗门',
            thread_id INTEGER,
            last_command_time REAL DEFAULT 0,
            next_check_time REAL DEFAULT 0,
            next_check_source TEXT,
            last_event TEXT,
            last_summary TEXT,
            last_bot_text TEXT,
            last_bot_msg_id INTEGER DEFAULT 0,
            last_action TEXT,
            last_action_time REAL DEFAULT 0,
            dry_run INTEGER DEFAULT 0,
            auto_lingxiao_enabled INTEGER DEFAULT 0,
            lingxiao_next_check_time REAL DEFAULT 0,
            lingxiao_next_check_source TEXT,
            auto_lingxiao_gangfeng_enabled INTEGER DEFAULT 0,
            lingxiao_gangfeng_next_check_time REAL DEFAULT 0,
            lingxiao_gangfeng_next_check_source TEXT,
            auto_lingxiao_borrow_enabled INTEGER DEFAULT 0,
            lingxiao_borrow_next_check_time REAL DEFAULT 0,
            lingxiao_borrow_next_check_source TEXT,
            auto_lingxiao_question_enabled INTEGER DEFAULT 0,
            lingxiao_question_next_check_time REAL DEFAULT 0,
            lingxiao_question_next_check_source TEXT,
            auto_sect_checkin_enabled INTEGER DEFAULT 0,
            sect_checkin_next_check_time REAL DEFAULT 0,
            sect_checkin_next_check_source TEXT,
            auto_sect_teach_enabled INTEGER DEFAULT 0,
            sect_teach_next_check_time REAL DEFAULT 0,
            sect_teach_next_check_source TEXT,
            auto_yinluo_sacrifice_enabled INTEGER DEFAULT 0,
            yinluo_sacrifice_next_check_time REAL DEFAULT 0,
            yinluo_sacrifice_next_check_source TEXT,
            auto_yinluo_blood_wash_enabled INTEGER DEFAULT 0,
            yinluo_blood_wash_next_check_time REAL DEFAULT 0,
            yinluo_blood_wash_next_check_source TEXT,
            yinluo_batch_mode TEXT,
            yinluo_batch_commands TEXT,
            yinluo_batch_index INTEGER DEFAULT 0,
            yinluo_batch_pending_msg_id INTEGER DEFAULT 0,
            yinluo_batch_started_at REAL DEFAULT 0,
            last_panel_time REAL DEFAULT 0,
            last_bounty_time REAL DEFAULT 0,
            last_sign_date TEXT,
            last_teach_date TEXT,
            last_teach_count INTEGER DEFAULT 0,
            last_yinluo_sacrifice_date TEXT,
            last_command_msg_id INTEGER DEFAULT 0,
            PRIMARY KEY (profile_id, chat_id, bot_username)
        )
        """
    )
    columns = {
        row[1] for row in db.cur.execute("PRAGMA table_info(sect_sessions)").fetchall()
    }
    alter_columns = {
        "thread_id": "INTEGER",
        "next_check_source": "TEXT",
        "auto_lingxiao_enabled": "INTEGER DEFAULT 0",
        "lingxiao_next_check_time": "REAL DEFAULT 0",
        "lingxiao_next_check_source": "TEXT",
        "auto_lingxiao_gangfeng_enabled": "INTEGER DEFAULT 0",
        "lingxiao_gangfeng_next_check_time": "REAL DEFAULT 0",
        "lingxiao_gangfeng_next_check_source": "TEXT",
        "auto_lingxiao_borrow_enabled": "INTEGER DEFAULT 0",
        "lingxiao_borrow_next_check_time": "REAL DEFAULT 0",
        "lingxiao_borrow_next_check_source": "TEXT",
        "auto_lingxiao_question_enabled": "INTEGER DEFAULT 0",
        "lingxiao_question_next_check_time": "REAL DEFAULT 0",
        "lingxiao_question_next_check_source": "TEXT",
        "auto_sect_checkin_enabled": "INTEGER DEFAULT 0",
        "sect_checkin_next_check_time": "REAL DEFAULT 0",
        "sect_checkin_next_check_source": "TEXT",
        "auto_sect_teach_enabled": "INTEGER DEFAULT 0",
        "sect_teach_next_check_time": "REAL DEFAULT 0",
        "sect_teach_next_check_source": "TEXT",
        "auto_yinluo_sacrifice_enabled": "INTEGER DEFAULT 0",
        "yinluo_sacrifice_next_check_time": "REAL DEFAULT 0",
        "yinluo_sacrifice_next_check_source": "TEXT",
        "auto_yinluo_blood_wash_enabled": "INTEGER DEFAULT 0",
        "yinluo_blood_wash_next_check_time": "REAL DEFAULT 0",
        "yinluo_blood_wash_next_check_source": "TEXT",
        "yinluo_batch_mode": "TEXT",
        "yinluo_batch_commands": "TEXT",
        "yinluo_batch_index": "INTEGER DEFAULT 0",
        "yinluo_batch_pending_msg_id": "INTEGER DEFAULT 0",
        "yinluo_batch_started_at": "REAL DEFAULT 0",
        "last_panel_time": "REAL DEFAULT 0",
        "last_bounty_time": "REAL DEFAULT 0",
        "last_sign_date": "TEXT",
        "last_teach_date": "TEXT",
        "last_teach_count": "INTEGER DEFAULT 0",
        "last_yinluo_sacrifice_date": "TEXT",
        "last_command_msg_id": "INTEGER DEFAULT 0",
        "profile_id": "INTEGER NOT NULL DEFAULT 0",
    }
    for column_name, column_type in alter_columns.items():
        if column_name not in columns:
            db.cur.execute(
                f"ALTER TABLE sect_sessions ADD COLUMN {column_name} {column_type}"
            )
    db.conn.commit()


def ensure_session(db, chat_id, bot_username=SECT_BOT_USERNAME, profile_id=None):
    ensure_tables(db)
    resolved_profile_id = int(profile_id or 0)
    db.cur.execute(
        """
        INSERT OR IGNORE INTO sect_sessions
            (profile_id, chat_id, bot_username, interval_seconds, command_text)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            resolved_profile_id,
            chat_id,
            bot_username,
            SECT_DEFAULT_INTERVAL,
            SECT_CHECK_COMMAND,
        ),
    )
    if resolved_profile_id:
        db.cur.execute(
            "UPDATE sect_sessions SET profile_id=? WHERE chat_id=? AND bot_username=? AND (profile_id IS NULL OR profile_id=0)",
            (resolved_profile_id, chat_id, bot_username),
        )
    db.conn.commit()


def get_session(db, chat_id, bot_username=SECT_BOT_USERNAME, profile_id=None):
    ensure_session(db, chat_id, bot_username, profile_id=profile_id)
    resolved_profile_id = int(profile_id or 0)
    if resolved_profile_id:
        db.cur.execute(
            "SELECT * FROM sect_sessions WHERE profile_id=? AND chat_id=? AND bot_username=?",
            (resolved_profile_id, chat_id, bot_username),
        )
    else:
        db.cur.execute(
            "SELECT * FROM sect_sessions WHERE chat_id=? AND bot_username=? ORDER BY profile_id DESC LIMIT 1",
            (chat_id, bot_username),
        )
    row = db.cur.fetchone()
    return dict(zip([col[0] for col in db.cur.description], row)) if row else None


def update_session(
    db, chat_id, bot_username=SECT_BOT_USERNAME, profile_id=None, **fields
):
    if not fields:
        return
    ensure_session(db, chat_id, bot_username, profile_id=profile_id)
    resolved_profile_id = int(profile_id or 0)
    assignments = ", ".join(f"{key}=?" for key in fields)
    if resolved_profile_id:
        values = list(fields.values()) + [resolved_profile_id, chat_id, bot_username]
        db.cur.execute(
            f"UPDATE sect_sessions SET {assignments} WHERE profile_id=? AND chat_id=? AND bot_username=?",
            values,
        )
    else:
        values = list(fields.values()) + [chat_id, bot_username]
        db.cur.execute(
            f"UPDATE sect_sessions SET {assignments} WHERE chat_id=? AND bot_username=?",
            values,
        )
    db.conn.commit()


def list_sessions(db, profile_id=None):
    ensure_tables(db)
    if profile_id:
        db.cur.execute(
            "SELECT * FROM sect_sessions WHERE profile_id=? ORDER BY chat_id",
            (int(profile_id),),
        )
    else:
        db.cur.execute("SELECT * FROM sect_sessions ORDER BY profile_id, chat_id")
    return [
        dict(zip([col[0] for col in db.cur.description], row))
        for row in db.cur.fetchall()
    ]


def _restore_session_thread_from_binding(storage, db, profile_id, session):
    if not storage or not db or not profile_id or not session:
        return session
    if session.get("thread_id"):
        return session
    chat_id = int(session.get("chat_id") or 0)
    if not chat_id:
        return session
    binding_thread_id = None
    for binding in storage.list_chat_bindings(profile_id):
        binding_chat_id = int(getattr(binding, "chat_id", 0) or 0)
        binding_thread = getattr(binding, "thread_id", None)
        binding_bot = (
            str(getattr(binding, "bot_username", "") or "").strip().lower().lstrip("@")
        )
        if binding_chat_id != chat_id:
            continue
        if binding_bot and binding_bot != SECT_BOT_USERNAME:
            continue
        if binding_thread:
            binding_thread_id = int(binding_thread)
            break
    if not binding_thread_id:
        return session
    updates = {"thread_id": binding_thread_id}
    last_summary = str(session.get("last_summary") or "")
    if "TOPIC_CLOSED" in last_summary:
        updates["next_check_time"] = 0
        updates["next_check_source"] = "已恢复话题线程，准备重试宗门自动任务"
        updates["last_summary"] = "检测到有效话题线程，已恢复自动发送目标"
    update_session(db, chat_id, profile_id=profile_id, **updates)
    refreshed_session = get_session(db, chat_id, profile_id=profile_id)
    return refreshed_session or session


async def send_message_in_session(
    client,
    session,
    chat_id,
    command_text,
    reply_to_msg_id=None,
    *,
    storage=None,
    profile_id=None,
):
    thread_id = session.get("thread_id")
    reply_to_target = reply_to_msg_id or thread_id
    logger.info(
        "Sect send attempt chat=%s thread=%s reply_to=%s command=%s",
        chat_id,
        thread_id,
        reply_to_target,
        command_text,
    )
    message = await send_message_with_thread_fallback(
        client,
        chat_id,
        command_text,
        thread_id=reply_to_target,
        storage=storage or getattr(client, "_tg_game_storage", None),
        profile_id=profile_id,
        bot_username=session.get("bot_username") or SECT_BOT_USERNAME,
        log_prefix="Sect auto",
    )
    logger.info(
        "Sect send success chat=%s thread=%s reply_to=%s command=%s",
        chat_id,
        thread_id,
        reply_to_target,
        command_text,
    )
    return message


def _extract_first(patterns, text):
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return match.group("value").strip()
    return None


def _extract_bonus(text):
    for pattern in SECT_BONUS_PATTERNS:
        match = pattern.search(text)
        if match:
            return int(match.group("value"))
    return None


def _extract_teach_progress(text):
    match = SECT_TEACH_USAGE_PATTERN.search(text)
    if not match:
        return None
    return int(match.group("value")), int(match.group("limit"))


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return {"event": "empty", "summary": "empty message"}
    if "正在推演天机" in text or "锁定道友神魂" in text:
        return {
            "event": "sect_panel_pending",
            "summary": "宗门信息推演中，等待机器人完成编辑",
            "sect_name": None,
            "leader_name": None,
            "description_text": None,
            "bonus_text": None,
            "position_name": None,
            "contribution_text": None,
        }

    sect_name = _extract_first(SECT_NAME_PATTERNS, text)
    position = _extract_first(SECT_POSITION_PATTERNS, text)
    leader = _extract_first([SECT_MASTER_PATTERN], text)
    description = _extract_first([SECT_DESC_PATTERN], text)
    sect_bonus = _extract_first([SECT_BONUS_PATTERN], text)
    contribution = _extract_first(SECT_CONTRIBUTION_PATTERNS, text)
    bonus = _extract_bonus(text)
    streak_match = SECT_DAYS_PATTERN.search(text)
    streak_days = int(streak_match.group("value")) if streak_match else None
    teach_progress = _extract_teach_progress(text)

    if "你所属的宗门" in text or "修炼加成" in text:
        parts = ["收到宗门面板"]
        if sect_name:
            parts.append(f"宗门 {sect_name}")
        if leader:
            parts.append(f"掌门 {leader}")
        if sect_bonus:
            parts.append(f"加成 {sect_bonus}")
        if contribution:
            parts.append(f"贡献 {contribution}")
        return {
            "event": "sect_panel",
            "summary": "，".join(parts),
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if "点卯成功" in text:
        parts = ["宗门点卯完成"]
        if bonus is not None:
            parts.append(f"贡献 +{bonus}")
        if streak_days is not None:
            parts.append(f"连续 {streak_days} 天")
        return {
            "event": "sect_sign",
            "summary": "，".join(parts),
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if "传功道意已记录" in text or "今日已传功" in text:
        parts = ["宗门传功完成"]
        if bonus is not None:
            parts.append(f"贡献 +{bonus}")
        usage_match = re.search(r"今日已传功\s*(?P<value>\d+/\d+)", text)
        if usage_match:
            parts.append(f"今日已传功 {usage_match.group('value')}")
        return {
            "event": "sect_teach",
            "summary": "，".join(parts),
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if "任务板" in text or "宗门悬赏" in text:
        task_name = "问候宗门长老" if "问候宗门长老" in text else None
        parts = ["收到宗门悬赏"]
        if task_name:
            parts.append(task_name)
        if bonus is not None:
            parts.append(f"奖励 {bonus} 贡献")
        return {
            "event": "sect_task_board",
            "summary": "，".join(parts),
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if "任务完成" in text and "宗门贡献" in text:
        parts = ["宗门任务完成"]
        if bonus is not None:
            parts.append(f"贡献 +{bonus}")
        return {
            "event": "sect_task_done",
            "summary": "，".join(parts),
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if sect_name or position or contribution:
        parts = ["收到宗门信息"]
        if sect_name:
            parts.append(f"宗门 {sect_name}")
        if position:
            parts.append(f"职位 {position}")
        if contribution:
            parts.append(f"贡献 {contribution}")
        return {
            "event": "sect_info",
            "summary": "，".join(parts),
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if "宗门任务" in text or "任务堂" in text:
        return {
            "event": "sect_task",
            "summary": "收到宗门任务相关信息",
            "sect_name": sect_name,
            "leader_name": leader,
            "description_text": description,
            "bonus_text": sect_bonus,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    if any(
        keyword in text
        for keyword in ["宗门宝库", "兑换", "宗门点卯", "宗门传功", "宗门捐献"]
    ):
        return {
            "event": "sect_daily",
            "summary": "收到宗门日常相关信息",
            "sect_name": sect_name,
            "position_name": position,
            "contribution_text": contribution,
            "bonus_value": bonus,
            "streak_days": streak_days,
            "teach_progress": teach_progress,
        }

    return {
        "event": "unknown",
        "summary": text[:80],
        "sect_name": sect_name,
        "leader_name": leader,
        "description_text": description,
        "bonus_text": sect_bonus,
        "position_name": position,
        "contribution_text": contribution,
        "bonus_value": bonus,
        "streak_days": streak_days,
    }


def build_status_text(session):
    if not session:
        return "宗门模块未初始化。"
    return "\n".join(
        [
            "宗门模块状态",
            f"开关: {'开启' if session.get('enabled') else '关闭'}",
            f"Dry-run: {'开启' if session.get('dry_run') else '关闭'}",
            f"自动登天阶: {'开启' if session.get('auto_lingxiao_enabled') else '关闭'}",
            f"下次登天阶: {format_timestamp(session.get('lingxiao_next_check_time') or 0)}",
            f"登阶倒计时来源: {session.get('lingxiao_next_check_source') or '-'}",
            f"自动引九天罡风: {'开启' if session.get('auto_lingxiao_gangfeng_enabled') else '关闭'}",
            f"下次引罡风: {format_timestamp(session.get('lingxiao_gangfeng_next_check_time') or 0)}",
            f"罡风倒计时来源: {session.get('lingxiao_gangfeng_next_check_source') or '-'}",
            f"自动借天门势: {'开启' if session.get('auto_lingxiao_borrow_enabled') else '关闭'}",
            f"下次借天门势: {format_timestamp(session.get('lingxiao_borrow_next_check_time') or 0)}",
            f"借势倒计时来源: {session.get('lingxiao_borrow_next_check_source') or '-'}",
            f"自动问心台: {'开启' if session.get('auto_lingxiao_question_enabled') else '关闭'}",
            f"下次问心检查: {format_timestamp(session.get('lingxiao_question_next_check_time') or 0)}",
            f"问心倒计时来源: {session.get('lingxiao_question_next_check_source') or '-'}",
            f"自动宗门点卯: {'开启' if session.get('auto_sect_checkin_enabled') else '关闭'}",
            f"下次点卯检查: {format_timestamp(session.get('sect_checkin_next_check_time') or 0)}",
            f"点卯倒计时来源: {session.get('sect_checkin_next_check_source') or '-'}",
            f"自动宗门传功: {'开启' if session.get('auto_sect_teach_enabled') else '关闭'}",
            f"下次传功检查: {format_timestamp(session.get('sect_teach_next_check_time') or 0)}",
            f"传功倒计时来源: {session.get('sect_teach_next_check_source') or '-'}",
            f"自动每日献祭: {'开启' if session.get('auto_yinluo_sacrifice_enabled') else '关闭'}",
            f"下次每日献祭: {format_timestamp(session.get('yinluo_sacrifice_next_check_time') or 0)}",
            f"每日献祭来源: {session.get('yinluo_sacrifice_next_check_source') or '-'}",
            f"自动血洗山林: {'开启' if session.get('auto_yinluo_blood_wash_enabled') else '关闭'}",
            f"下次血洗山林: {format_timestamp(session.get('yinluo_blood_wash_next_check_time') or 0)}",
            f"血洗山林来源: {session.get('yinluo_blood_wash_next_check_source') or '-'}",
            f"阴罗批次: {session.get('yinluo_batch_mode') or '-'}",
            f"阴罗批次进度: {int(session.get('yinluo_batch_index') or 0)} / {len(_load_yinluo_batch_commands(session))}",
            f"查询指令: {session.get('command_text') or SECT_CHECK_COMMAND}",
            f"轮询间隔: {session.get('interval_seconds') or SECT_DEFAULT_INTERVAL} 秒",
            f"下次检查: {format_timestamp(session.get('next_check_time') or 0)}",
            f"检查倒计时来源: {session.get('next_check_source') or '-'}",
            f"最后事件: {session.get('last_event') or '-'}",
            f"最后摘要: {session.get('last_summary') or '-'}",
        ]
    )


def set_enabled(db, chat_id, enabled, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        enabled=_normalize_bool(enabled),
        next_check_time=0 if enabled else 0,
    )


def set_dry_run(db, chat_id, enabled, profile_id=None):
    update_session(db, chat_id, profile_id=profile_id, dry_run=_normalize_bool(enabled))


def set_interval(db, chat_id, interval_seconds, profile_id=None):
    interval_seconds = max(int(interval_seconds), 30)
    update_session(
        db, chat_id, profile_id=profile_id, interval_seconds=interval_seconds
    )
    return interval_seconds


def set_check_command(db, chat_id, command_text, profile_id=None):
    command_text = (command_text or "").strip()
    if not command_text:
        raise ValueError("宗门查询指令不能为空")
    update_session(db, chat_id, profile_id=profile_id, command_text=command_text)
    return command_text


def configure_lingxiao_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_lingxiao_enabled=_normalize_bool(enabled),
        lingxiao_next_check_time=0,
        lingxiao_next_check_source=(
            "已开启自动登天阶，等待首轮执行" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(session, {"auto_lingxiao_enabled": 0})
        ),
        next_check_source=("已开启自动登天阶，等待首轮同步" if enabled else None),
    )


def configure_sect_checkin_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_sect_checkin_enabled=_normalize_bool(enabled),
        sect_checkin_next_check_time=0,
        sect_checkin_next_check_source=(
            "已开启自动宗门点卯，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_sect_checkin_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动宗门点卯，等待首轮同步" if enabled else None),
    )


def configure_sect_teach_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_sect_teach_enabled=_normalize_bool(enabled),
        sect_teach_next_check_time=0,
        sect_teach_next_check_source=(
            "已开启自动宗门传功，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_sect_teach_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动宗门传功，等待首轮同步" if enabled else None),
    )


def configure_yinluo_sacrifice_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_yinluo_sacrifice_enabled=_normalize_bool(enabled),
        yinluo_sacrifice_next_check_time=0,
        yinluo_sacrifice_next_check_source=(
            "已开启自动每日献祭，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_yinluo_sacrifice_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动每日献祭，等待首轮同步" if enabled else None),
    )


def configure_yinluo_blood_wash_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_yinluo_blood_wash_enabled=_normalize_bool(enabled),
        yinluo_blood_wash_next_check_time=0,
        yinluo_blood_wash_next_check_source=(
            "已开启自动血洗山林，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_yinluo_blood_wash_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动血洗山林，等待首轮同步" if enabled else None),
    )


def _load_yinluo_batch_commands(session):
    raw_value = str((session or {}).get("yinluo_batch_commands") or "").strip()
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item or "").strip() for item in parsed if str(item or "").strip()]


def has_active_yinluo_batch(session):
    return bool(_load_yinluo_batch_commands(session))


def start_yinluo_batch(db, chat_id, mode, commands, profile_id=None):
    normalized_commands = [
        str(command or "").strip()
        for command in (commands or [])
        if str(command or "").strip()
    ]
    if not normalized_commands:
        raise ValueError("阴罗批次命令不能为空")
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        yinluo_batch_mode=str(mode or "imprison").strip() or "imprison",
        yinluo_batch_commands=json.dumps(normalized_commands, ensure_ascii=False),
        yinluo_batch_index=0,
        yinluo_batch_pending_msg_id=0,
        yinluo_batch_started_at=time.time(),
        next_check_time=0,
        next_check_source=f"已创建阴罗批次，共 {len(normalized_commands)} 条命令",
        last_summary=f"阴罗批次已启动，共 {len(normalized_commands)} 条命令",
    )


def clear_yinluo_batch(db, chat_id, summary="", profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        yinluo_batch_mode=None,
        yinluo_batch_commands=None,
        yinluo_batch_index=0,
        yinluo_batch_pending_msg_id=0,
        yinluo_batch_started_at=0,
        last_summary=summary or None,
    )


def configure_lingxiao_gangfeng_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_lingxiao_gangfeng_enabled=_normalize_bool(enabled),
        lingxiao_gangfeng_next_check_time=0,
        lingxiao_gangfeng_next_check_source=(
            "已开启自动引九天罡风，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_lingxiao_gangfeng_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动引九天罡风，等待首轮同步" if enabled else None),
    )


def configure_lingxiao_borrow_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_lingxiao_borrow_enabled=_normalize_bool(enabled),
        lingxiao_borrow_next_check_time=0,
        lingxiao_borrow_next_check_source=(
            "已开启自动借天门势，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_lingxiao_borrow_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动借天门势，等待首轮同步" if enabled else None),
    )


def configure_lingxiao_question_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_lingxiao_question_enabled=_normalize_bool(enabled),
        lingxiao_question_next_check_time=0,
        lingxiao_question_next_check_source=(
            "已开启自动问心台，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_lingxiao_question_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动问心台，等待首轮同步" if enabled else None),
    )


def build_auto_command(session, now=None):
    now = now or time.time()
    if session.get("auto_sect_checkin_enabled"):
        next_time = float(session.get("sect_checkin_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".宗门点卯",
                "next_field": "sect_checkin_next_check_time",
                "source_field": "sect_checkin_next_check_source",
                "pending_source": "已发送 .宗门点卯，等待机器人回复",
            }
    if session.get("auto_sect_teach_enabled"):
        next_time = float(session.get("sect_teach_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".宗门传功",
                "next_field": "sect_teach_next_check_time",
                "source_field": "sect_teach_next_check_source",
                "pending_source": "已发送 .宗门传功，等待机器人回复",
                "requires_reply_target": True,
                "pending_delay_seconds": SECT_AUTO_TEACH_REPLY_RECHECK_SECONDS,
            }
    if session.get("auto_yinluo_sacrifice_enabled"):
        next_time = float(session.get("yinluo_sacrifice_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".每日献祭",
                "next_field": "yinluo_sacrifice_next_check_time",
                "source_field": "yinluo_sacrifice_next_check_source",
                "pending_source": "已发送 .每日献祭，等待宗门状态刷新",
            }
    if session.get("auto_yinluo_blood_wash_enabled"):
        next_time = float(session.get("yinluo_blood_wash_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".血洗山林",
                "next_field": "yinluo_blood_wash_next_check_time",
                "source_field": "yinluo_blood_wash_next_check_source",
                "pending_source": "已发送 .血洗山林，等待宗门状态刷新",
            }
    if session.get("auto_lingxiao_question_enabled"):
        next_time = float(session.get("lingxiao_question_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".问心台",
                "next_field": "lingxiao_question_next_check_time",
                "source_field": "lingxiao_question_next_check_source",
                "pending_source": "已发送 .问心台，等待天机阁同步问心状态",
            }
    if session.get("auto_lingxiao_gangfeng_enabled"):
        next_time = float(session.get("lingxiao_gangfeng_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".引九天罡风",
                "next_field": "lingxiao_gangfeng_next_check_time",
                "source_field": "lingxiao_gangfeng_next_check_source",
                "pending_source": "已发送 .引九天罡风，等待天机阁同步淬体状态",
            }
    if session.get("auto_lingxiao_borrow_enabled"):
        next_time = float(session.get("lingxiao_borrow_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".借天门势",
                "next_field": "lingxiao_borrow_next_check_time",
                "source_field": "lingxiao_borrow_next_check_source",
                "pending_source": "已发送 .借天门势，等待天机阁同步借势状态",
            }
    if session.get("auto_lingxiao_enabled"):
        next_time = float(session.get("lingxiao_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".登天阶",
                "next_field": "lingxiao_next_check_time",
                "source_field": "lingxiao_next_check_source",
                "pending_source": "已发送 .登天阶，等待天机阁同步云阶状态",
            }
        return None
    return None


async def maybe_send_check(
    client,
    db,
    chat_id,
    *,
    force=False,
    command_text=None,
    reply_to_msg_id=None,
    storage=None,
    profile_id=None,
):
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session or not session["enabled"]:
        return False, "disabled", 0
    now = time.time()
    if not force and session["next_check_time"] and now < session["next_check_time"]:
        return False, "not_due", 0
    if (
        not force
        and session["last_command_time"]
        and now - session["last_command_time"] < SECT_COMMAND_COOLDOWN
    ):
        return False, "cooldown", 0

    command_text = command_text or session["command_text"] or SECT_CHECK_COMMAND
    if session["dry_run"]:
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            last_action=f"dry-run:{command_text}",
            last_action_time=now,
            next_check_time=now + session["interval_seconds"],
            next_check_source=f"dry-run 已模拟发送 {command_text}",
            last_summary=f"dry-run 模式，未实际发送指令: {command_text}",
        )
        return True, "dry_run", 0

    sent_message = await send_message_in_session(
        client,
        session,
        chat_id,
        command_text,
        reply_to_msg_id=reply_to_msg_id,
        storage=storage,
        profile_id=profile_id,
    )
    update_session(
        db,
        chat_id,
        profile_id=session.get("profile_id"),
        last_command_time=now,
        last_command_msg_id=getattr(sent_message, "id", 0),
        last_action=command_text,
        last_action_time=now,
        next_check_time=now + session["interval_seconds"],
        next_check_source=f"已发送 {command_text}，等待机器人回复",
        last_summary=f"已发送宗门指令: {command_text}",
    )
    if command_text == ".登天阶":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            lingxiao_next_check_time=now + LINGXIAO_COMMAND_REFRESH_SECONDS,
            lingxiao_next_check_source="已发送 .登天阶，等待机器人回复",
        )
    elif command_text == ".引九天罡风":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            lingxiao_gangfeng_next_check_time=now + LINGXIAO_COMMAND_REFRESH_SECONDS,
            lingxiao_gangfeng_next_check_source="已发送 .引九天罡风，等待机器人回复",
        )
    elif command_text == ".借天门势":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            lingxiao_borrow_next_check_time=now + LINGXIAO_COMMAND_REFRESH_SECONDS,
            lingxiao_borrow_next_check_source="已发送 .借天门势，等待机器人回复",
        )
    elif command_text == ".问心台":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            lingxiao_question_next_check_time=now + LINGXIAO_COMMAND_REFRESH_SECONDS,
            lingxiao_question_next_check_source="已发送 .问心台，等待机器人回复",
        )
    elif command_text == ".每日献祭":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            last_yinluo_sacrifice_date=current_date_key(now),
            yinluo_sacrifice_next_check_source="已发送 .每日献祭，等待机器人回复",
        )
    elif command_text == ".血洗山林":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            yinluo_blood_wash_next_check_source="已发送 .血洗山林，等待机器人回复",
        )
    return True, "sent", getattr(sent_message, "id", 0)


async def maybe_run_yinluo_batch(client, db, session, *, storage=None, profile_id=None):
    commands = _load_yinluo_batch_commands(session)
    if not commands:
        return False
    current_index = int(session.get("yinluo_batch_index") or 0)
    pending_msg_id = int(session.get("yinluo_batch_pending_msg_id") or 0)
    chat_id = int(session.get("chat_id") or 0)
    if pending_msg_id:
        return True
    if current_index >= len(commands):
        clear_yinluo_batch(
            db,
            chat_id,
            summary="阴罗批次已完成",
            profile_id=session.get("profile_id"),
        )
        return True
    command_text = commands[current_index]
    _ok, status, sent_message_id = await maybe_send_check(
        client,
        db,
        chat_id,
        force=True,
        command_text=command_text,
        storage=storage,
        profile_id=profile_id,
    )
    if status == "sent" and sent_message_id:
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            yinluo_batch_pending_msg_id=int(sent_message_id),
            next_check_time=time.time() + LINGXIAO_COMMAND_REFRESH_SECONDS,
            next_check_source=f"阴罗批次等待回复: {command_text}",
        )
    return True


async def handle_bot_message(event, db, client=None, profile_id=None):
    sender = await event.get_sender()
    username = (getattr(sender, "username", "") or "").lower()
    if username != SECT_BOT_USERNAME:
        return None

    session = get_session(db, event.chat_id, profile_id=profile_id)
    if not session or not session["enabled"]:
        return None
    raw_text = (event.raw_text or "").strip()
    last_bot_text = (session.get("last_bot_text") or "").strip()
    if session["last_bot_msg_id"] == event.id and last_bot_text == raw_text[:1000]:
        return None

    parsed = parse_message(raw_text)
    now = time.time()
    message = getattr(event, "message", None)
    reply_to = getattr(message, "reply_to", None) if message else None
    reply_to_msg_id = int(getattr(reply_to, "reply_to_msg_id", None) or 0)
    update_fields = {
        "last_event": parsed["event"],
        "last_summary": parsed["summary"],
        "last_bot_text": raw_text[:1000],
        "last_bot_msg_id": event.id,
        "next_check_source": parsed["summary"],
    }
    batch_commands = _load_yinluo_batch_commands(session)
    batch_pending_msg_id = int(session.get("yinluo_batch_pending_msg_id") or 0)
    batch_index = int(session.get("yinluo_batch_index") or 0)
    if (
        batch_commands
        and batch_pending_msg_id
        and reply_to_msg_id == batch_pending_msg_id
    ):
        if client is not None:
            try:
                await client.delete_messages(
                    event.chat_id, [int(batch_pending_msg_id)], revoke=True
                )
            except Exception as exc:
                logger.warning(
                    "Yinluo batch failed deleting command chat=%s message_id=%s error=%s",
                    event.chat_id,
                    batch_pending_msg_id,
                    exc,
                )
        next_index = batch_index + 1
        if next_index >= len(batch_commands):
            update_fields["yinluo_batch_mode"] = None
            update_fields["yinluo_batch_commands"] = None
            update_fields["yinluo_batch_index"] = 0
            update_fields["yinluo_batch_pending_msg_id"] = 0
            update_fields["yinluo_batch_started_at"] = 0
            update_fields["last_summary"] = "阴罗批次已完成"
        else:
            update_fields["yinluo_batch_index"] = next_index
            update_fields["yinluo_batch_pending_msg_id"] = 0
            update_fields["next_check_time"] = 0
            update_fields["next_check_source"] = (
                f"阴罗批次已完成 {next_index}/{len(batch_commands)}，准备下一条"
            )
    if parsed["event"] == "sect_panel":
        update_fields["last_panel_time"] = now
        if not session.get("auto_lingxiao_enabled"):
            update_fields["next_check_time"] = now + session["interval_seconds"]
    teach_progress = parsed.get("teach_progress") or ()
    if teach_progress:
        teach_count = int(teach_progress[0])
        update_fields["last_teach_count"] = teach_count
        if teach_count > 0:
            update_fields["last_teach_date"] = current_date_key(now)
    if parsed["event"] == "lingxiao_step":
        cooldown_seconds = (
            parsed.get("cooldown_seconds") or LINGXIAO_STEP_DEFAULT_SECONDS
        )
        update_fields["lingxiao_next_check_time"] = now + cooldown_seconds
        update_fields["lingxiao_next_check_source"] = parsed["summary"]
        update_fields["next_check_time"] = now + cooldown_seconds
    elif parsed["event"] == "sect_sign":
        update_fields["last_sign_date"] = current_date_key(now)
        if session.get("auto_sect_checkin_enabled"):
            update_fields["sect_checkin_next_check_time"] = _next_daily_run_timestamp(
                SECT_AUTO_CHECK_IN_TIME, now
            )
            update_fields["sect_checkin_next_check_source"] = (
                "今日已点卯，等待次日 02:00"
            )
    elif parsed["event"] == "sect_teach":
        teach_count = int(teach_progress[0]) if teach_progress else 0
        update_fields["last_teach_date"] = current_date_key(now)
        update_fields["last_teach_count"] = teach_count
        if session.get("auto_sect_teach_enabled"):
            if teach_count >= SECT_DAILY_TEACH_LIMIT:
                update_fields["sect_teach_next_check_time"] = _next_daily_run_timestamp(
                    SECT_AUTO_TEACH_TIME, now
                )
                update_fields["sect_teach_next_check_source"] = (
                    f"今日已传功 {teach_count}/{SECT_DAILY_TEACH_LIMIT}，等待次日 02:10"
                )
            else:
                update_fields["sect_teach_next_check_time"] = 0
                update_fields["sect_teach_next_check_source"] = (
                    f"收到传功回复，可继续执行 ({teach_count}/{SECT_DAILY_TEACH_LIMIT})"
                )
    elif parsed["event"] not in {"sect_panel_pending", "unknown"}:
        update_fields["next_check_time"] = now + session["interval_seconds"]
    if _has_any_auto_keys(session):
        update_fields["next_check_time"] = _recompute_overall_next_check(
            session, update_fields, now
        )
    update_session(
        db,
        event.chat_id,
        profile_id=session.get("profile_id"),
        **update_fields,
    )
    return parsed


async def runner(client, storage):
    while True:
        try:
            db = RuntimeDb(storage)
            now = time.time()
            for session in list_sessions(db):
                if not session["enabled"]:
                    continue
                session_profile_id = int(session.get("profile_id") or 0) or None
                if session_profile_id:
                    session = _restore_session_thread_from_binding(
                        storage, db, session_profile_id, session
                    )
                if has_active_yinluo_batch(session):
                    try:
                        handled = await maybe_run_yinluo_batch(
                            client,
                            db,
                            session,
                            storage=storage,
                            profile_id=session_profile_id,
                        )
                        if handled:
                            continue
                    except Exception as exc:
                        logger.warning(
                            "Yinluo batch failed in chat %s: %s",
                            session["chat_id"],
                            exc,
                        )
                        clear_yinluo_batch(
                            db,
                            session["chat_id"],
                            summary=f"阴罗批次失败: {exc}",
                        )
                        continue
                if session["next_check_time"] and now < session["next_check_time"]:
                    continue
                try:
                    payload = None
                    if session_profile_id and (
                        _active_common_auto_keys(session)
                        or _active_yinluo_auto_keys(session)
                        or _active_lingxiao_auto_keys(session)
                    ):
                        payload = _read_cached_profile_payload(
                            storage, session_profile_id
                        )
                    if session_profile_id and _active_common_auto_keys(session):
                        session, _daily_state = sync_common_sect_state(
                            storage,
                            db,
                            session_profile_id,
                            session["chat_id"],
                            payload=payload,
                            now=now,
                        )
                        now = time.time()
                    if session_profile_id and _active_yinluo_auto_keys(session):
                        session, _view = sync_yinluo_state(
                            storage,
                            db,
                            session_profile_id,
                            session["chat_id"],
                            payload=payload,
                            now=now,
                        )
                        now = time.time()
                    if session_profile_id and _active_lingxiao_auto_keys(session):
                        session, _view = sync_lingxiao_trial_state(
                            storage,
                            db,
                            session_profile_id,
                            session["chat_id"],
                            payload=None,
                        )
                        now = time.time()
                    command_info = build_auto_command(session, now)
                    if not command_info:
                        continue
                    reply_to_msg_id = None
                    if command_info.get("requires_reply_target") and session_profile_id:
                        latest_command = storage.get_latest_outgoing_command_message(
                            session_profile_id,
                            session["chat_id"],
                            thread_id=session.get("thread_id"),
                        )
                        reply_to_msg_id = int(
                            (latest_command or {}).get("message_id")
                            or session.get("last_command_msg_id")
                            or 0
                        )
                        if not reply_to_msg_id:
                            pending_time = now + SECT_AUTO_TEACH_REPLY_RECHECK_SECONDS
                            pending_source = "缺少可回复的最近命令，稍后重试宗门传功"
                            update_session(
                                db,
                                session["chat_id"],
                                profile_id=session_profile_id,
                                sect_teach_next_check_time=pending_time,
                                sect_teach_next_check_source=pending_source,
                                next_check_time=pending_time,
                                next_check_source=pending_source,
                            )
                            continue
                    _ok, _status, sent_message_id = await maybe_send_check(
                        client,
                        db,
                        session["chat_id"],
                        command_text=command_info["command"],
                        reply_to_msg_id=reply_to_msg_id,
                        storage=storage,
                        profile_id=session_profile_id,
                    )
                    current_session = (
                        get_session(
                            db,
                            session["chat_id"],
                            profile_id=session_profile_id,
                        )
                        or session
                    )
                    if _status == "sent":
                        pending_time = now + int(
                            command_info.get("pending_delay_seconds")
                            or LINGXIAO_COMMAND_REFRESH_SECONDS
                        )
                        pending_source = command_info.get("pending_source") or (
                            f"已发送 {command_info['command']}，等待天机阁状态刷新"
                        )
                    elif _status == "cooldown":
                        last_command_time = float(
                            current_session.get("last_command_time") or 0
                        )
                        retry_seconds = max(
                            int(
                                SECT_COMMAND_COOLDOWN - max(now - last_command_time, 0)
                            ),
                            3,
                        )
                        pending_time = now + retry_seconds
                        pending_source = f"命令冷却中，{retry_seconds} 秒后重试 {command_info['command']}"
                    elif _status == "not_due":
                        pending_time = float(
                            current_session.get("next_check_time") or 0
                        ) or (now + SECT_RUNNER_POLL_SECONDS)
                        pending_source = str(
                            current_session.get("next_check_source")
                            or "未到执行时间，稍后重试"
                        )
                    elif _status == "disabled":
                        continue
                    else:
                        pending_time = float(
                            current_session.get("next_check_time") or 0
                        ) or (now + SECT_RUNNER_POLL_SECONDS)
                        pending_source = str(
                            current_session.get("next_check_source")
                            or f"{command_info['command']} 当前未发送，稍后重试"
                        )
                    update_fields = {
                        command_info["next_field"]: pending_time,
                        command_info["source_field"]: pending_source,
                    }
                    update_fields["next_check_time"] = pending_time
                    update_fields["next_check_source"] = pending_source
                    update_session(
                        db,
                        session["chat_id"],
                        profile_id=session_profile_id,
                        **update_fields,
                    )
                except Exception as exc:
                    logger.warning(
                        "Sect runner failed in chat %s: %s", session["chat_id"], exc
                    )
                    update_session(
                        db,
                        session["chat_id"],
                        profile_id=session_profile_id,
                        next_check_time=now + max(session["interval_seconds"], 60),
                        next_check_source="runner 异常后退避等待",
                        last_summary=f"runner failed: {exc}",
                    )
            db.close()
            await asyncio.sleep(SECT_RUNNER_POLL_SECONDS)
        except Exception as exc:
            logger.exception("Sect runner error: %s", exc)
            await asyncio.sleep(10)
