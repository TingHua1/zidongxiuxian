import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta

from tg_game.clients.asc_client import AscAuthError
from tg_game.config import ALLOWED_GAME_BOT_IDS
from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_cultivator_lookup_candidates,
    get_effective_external_cookie,
    mark_external_account_failure,
    read_cached_external_payload,
    sync_external_account,
)
from tg_game.storage import CompatDb as RuntimeDb
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)


SECT_BOT_USERNAME = "fanrenxiuxian_bot"
SECT_BOT_IDS = set(ALLOWED_GAME_BOT_IDS)
SECT_CHECK_COMMAND = ".我的宗门"
SECT_DEFAULT_INTERVAL = 1800
SECT_COMMAND_COOLDOWN = 15
SECT_RUNNER_POLL_SECONDS = 5
SECT_DAILY_TEACH_LIMIT = 3
SECT_AUTO_WINDOW_START_TIME = "02:00"
SECT_AUTO_WINDOW_END_TIME = "05:00"
YINLUO_AUTO_SACRIFICE_TIME = "02:20"
COMPANION_GREET_WINDOW_END = "03:00"
SECT_AUTO_TEACH_REPLY_RECHECK_SECONDS = 30
HUANGFENG_AUTO_CHECK_SECONDS = 30 * 60
LUOYUN_IRRIGATION_COOLDOWN_SECONDS = 2 * 3600
LUOYUN_COMMAND_REFRESH_SECONDS = 180
LUOYUN_BATCH_TIMEOUT_SECONDS = 10 * 60
LUOYUN_BATCH_MAX_RETRIES = 1
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
HUANGFENG_BATCH_TIMEOUT_SECONDS = 10 * 60
HUANGFENG_BATCH_MAX_RETRIES = 2
HUANGFENG_PAYLOAD_REFRESH_MAX_RETRIES = 2

HUANGFENG_PLOT_PATTERN = re.compile(
    r"(?P<plot>\d+)\s*(?:号)?(?:药田|地块|灵田|田)", re.IGNORECASE
)
HUANGFENG_PLOT_STATUS_PATTERN = re.compile(
    r"(?P<plot>\d+)\s*(?:号)?(?:药田|地块|灵田|田)[^\n]*", re.IGNORECASE
)
HUANGFENG_SEED_SHORTAGE_KEYWORDS = (
    "种子不足",
    "没有该种子",
    "缺少种子",
    "数量不足",
    "不足以播种",
)
HUANGFENG_FAILURE_KEYWORDS = ("失败", "不可", "无法", "没有权限")

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

HUANGFENG_SECT_NAME = "黄枫谷"
YINLUO_SECT_NAME = "阴罗宗"
LINGXIAO_SECT_NAME = "凌霄宫"
LUOYUN_SECT_NAME = "落云宗"
XINGGONG_SECT_NAME = "星宫"


def _normalize_bool(value):
    return 1 if bool(value) else 0


def _normalize_sect_name_text(value: str) -> str:
    return str(value or "").replace("【", "").replace("】", "").strip()


def _is_same_sect_name(current_name: str, expected_name: str) -> bool:
    current = _normalize_sect_name_text(current_name)
    expected = _normalize_sect_name_text(expected_name)
    if not current or not expected:
        return False
    return current == expected or current in expected or expected in current


def _build_sect_auto_guard_updates(session, sect_name: str, now=None) -> dict:
    now = now or time.time()
    normalized_sect_name = _normalize_sect_name_text(sect_name)
    updates = {}
    reasons = []

    if not normalized_sect_name:
        if any(
            session.get(key)
            for key in [
                "auto_sect_checkin_enabled",
                "auto_sect_teach_enabled",
                "auto_yinluo_sacrifice_enabled",
                "auto_yinluo_blood_wash_enabled",
                "auto_huangfeng_enabled",
                "auto_huangfeng_exchange_enabled",
                "auto_luoyun_enabled",
                "auto_companion_greet_enabled",
                "auto_lingxiao_enabled",
                "auto_lingxiao_gangfeng_enabled",
                "auto_lingxiao_borrow_enabled",
                "auto_lingxiao_question_enabled",
            ]
        ):
            reasons.append("人物已无宗门，已关闭全部宗门自动任务")
        updates.update(
            {
                "auto_sect_checkin_enabled": 0,
                "auto_sect_teach_enabled": 0,
                "auto_yinluo_sacrifice_enabled": 0,
                "auto_yinluo_blood_wash_enabled": 0,
                "auto_huangfeng_enabled": 0,
                "auto_huangfeng_exchange_enabled": 0,
                "auto_lingxiao_enabled": 0,
                "auto_lingxiao_gangfeng_enabled": 0,
                "auto_lingxiao_borrow_enabled": 0,
                "auto_lingxiao_question_enabled": 0,
                "yinluo_batch_mode": None,
                "yinluo_batch_commands": None,
                "yinluo_batch_index": 0,
                "yinluo_batch_pending_msg_id": 0,
                "yinluo_batch_started_at": 0,
                "huangfeng_pending_commands": None,
                "huangfeng_pending_index": 0,
                "huangfeng_pending_msg_id": 0,
                "huangfeng_pending_retry": 0,
                "huangfeng_payload_refresh_retry": 0,
                "huangfeng_batch_just_completed": 0,
                "auto_luoyun_enabled": 0,
                "luoyun_pending_commands": None,
                "luoyun_pending_index": 0,
                "luoyun_pending_msg_id": 0,
                "luoyun_pending_retry": 0,
                "luoyun_batch_just_completed": 0,
                "luoyun_force_refresh": 0,
                "luoyun_invasion_active": 0,
                "luoyun_frozen_irrigation_ready_time": 0,
                "sect_checkin_next_check_time": 0,
                "sect_teach_next_check_time": 0,
                "yinluo_sacrifice_next_check_time": 0,
                "yinluo_blood_wash_next_check_time": 0,
                "huangfeng_next_check_time": 0,
                "luoyun_next_check_time": 0,
                "lingxiao_next_check_time": 0,
                "lingxiao_gangfeng_next_check_time": 0,
                "lingxiao_borrow_next_check_time": 0,
                "lingxiao_question_next_check_time": 0,
                "next_check_time": 0,
            }
        )
    else:
        if (
            session.get("auto_huangfeng_enabled")
            or session.get("auto_huangfeng_exchange_enabled")
        ) and not _is_same_sect_name(normalized_sect_name, HUANGFENG_SECT_NAME):
            reasons.append(f"当前宗门已不是{HUANGFENG_SECT_NAME}，已关闭黄枫谷自动")
            updates.update(
                {
                    "auto_huangfeng_enabled": 0,
                    "auto_huangfeng_exchange_enabled": 0,
                    "huangfeng_pending_commands": None,
                    "huangfeng_pending_index": 0,
                    "huangfeng_pending_msg_id": 0,
                    "huangfeng_pending_retry": 0,
                    "huangfeng_payload_refresh_retry": 0,
                    "huangfeng_batch_just_completed": 0,
                    "huangfeng_next_check_time": 0,
                }
            )
        if session.get("auto_luoyun_enabled") and not _is_same_sect_name(
            normalized_sect_name, LUOYUN_SECT_NAME
        ):
            reasons.append(f"当前宗门已不是{LUOYUN_SECT_NAME}，已关闭落云宗自动")
            updates.update(
                {
                "auto_luoyun_enabled": 0,
                "auto_companion_greet_enabled": 0,
                "companion_greet_next_check_time": 0,
                    "luoyun_pending_commands": None,
                    "luoyun_pending_index": 0,
                    "luoyun_pending_msg_id": 0,
                    "luoyun_pending_retry": 0,
                    "luoyun_batch_just_completed": 0,
                    "luoyun_force_refresh": 0,
                    "luoyun_invasion_active": 0,
                    "luoyun_frozen_irrigation_ready_time": 0,
                    "luoyun_next_check_time": 0,
                }
            )
        if session.get("auto_companion_greet_enabled") and not _is_same_sect_name(
            normalized_sect_name, XINGGONG_SECT_NAME
        ):
            reasons.append(f"当前宗门已不是{XINGGONG_SECT_NAME}，已关闭自动问安")
            updates.update(
                {
                    "auto_companion_greet_enabled": 0,
                    "companion_greet_next_check_time": 0,
                }
            )
        if (
            session.get("auto_yinluo_sacrifice_enabled")
            or session.get("auto_yinluo_blood_wash_enabled")
        ) and not _is_same_sect_name(normalized_sect_name, YINLUO_SECT_NAME):
            reasons.append(f"当前宗门已不是{YINLUO_SECT_NAME}，已关闭阴罗宗自动")
            updates.update(
                {
                    "auto_yinluo_sacrifice_enabled": 0,
                    "auto_yinluo_blood_wash_enabled": 0,
                    "yinluo_batch_mode": None,
                    "yinluo_batch_commands": None,
                    "yinluo_batch_index": 0,
                    "yinluo_batch_pending_msg_id": 0,
                    "yinluo_batch_started_at": 0,
                    "yinluo_sacrifice_next_check_time": 0,
                    "yinluo_blood_wash_next_check_time": 0,
                }
            )
        if any(
            session.get(key)
            for key in [
                "auto_lingxiao_enabled",
                "auto_lingxiao_gangfeng_enabled",
                "auto_lingxiao_borrow_enabled",
                "auto_lingxiao_question_enabled",
            ]
        ) and not _is_same_sect_name(normalized_sect_name, LINGXIAO_SECT_NAME):
            reasons.append(f"当前宗门已不是{LINGXIAO_SECT_NAME}，已关闭凌霄宫自动")
            updates.update(
                {
                    "auto_lingxiao_enabled": 0,
                    "auto_lingxiao_gangfeng_enabled": 0,
                    "auto_lingxiao_borrow_enabled": 0,
                    "auto_lingxiao_question_enabled": 0,
                    "lingxiao_next_check_time": 0,
                    "lingxiao_gangfeng_next_check_time": 0,
                    "lingxiao_borrow_next_check_time": 0,
                    "lingxiao_question_next_check_time": 0,
                }
            )

    if updates:
        updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
        reason_text = "；".join(reasons).strip()
        if reason_text:
            updates["next_check_source"] = reason_text
            updates["last_summary"] = reason_text
    return updates


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


def _sect_daily_window_bounds(now=None):
    now = now or time.time()
    window_start = _time_today_timestamp(SECT_AUTO_WINDOW_START_TIME, now)
    window_end = _time_today_timestamp(SECT_AUTO_WINDOW_END_TIME, now)
    if window_end <= window_start:
        window_end = window_start + 3 * 3600
    return window_start, window_end


def _stable_daily_window_seed(session, action_key, now=None):
    date_key = current_date_key(now)
    seed_text = "|".join(
        [
            str(action_key or ""),
            date_key,
            str((session or {}).get("profile_id") or 0),
            str((session or {}).get("chat_id") or 0),
            str((session or {}).get("bot_username") or ""),
        ]
    )
    return int(hashlib.sha256(seed_text.encode("utf-8")).hexdigest()[:16], 16)


def _daily_random_window_timestamp(session, action_key, now=None):
    now = now or time.time()
    window_start, window_end = _sect_daily_window_bounds(now)
    window_seconds = max(int(window_end - window_start), 1)
    offset_seconds = _stable_daily_window_seed(session, action_key, now) % (
        window_seconds + 1
    )
    return window_start + offset_seconds


def _next_daily_random_window_timestamp(session, action_key, now=None):
    now = now or time.time()
    today_target = _daily_random_window_timestamp(session, action_key, now)
    if now < today_target:
        return today_target
    return _daily_random_window_timestamp(session, action_key, now + 86400)


def _common_action_still_syncing(session, now, *, command_text: str) -> bool:
    last_action = str((session or {}).get("last_action") or "").strip()
    last_action_time = float((session or {}).get("last_action_time") or 0)
    if last_action != str(command_text or "").strip() or not last_action_time:
        return False
    return now - last_action_time < LINGXIAO_ACTION_SYNC_TIMEOUT_SECONDS


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


def _parse_json_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _find_nested_value(payload, target_key):
    normalized_key = str(target_key or "").strip()
    if not normalized_key:
        return None
    if isinstance(payload, dict):
        if normalized_key in payload:
            return payload.get(normalized_key)
        for value in payload.values():
            nested = _find_nested_value(value, normalized_key)
            if nested is not None:
                return nested
    elif isinstance(payload, list):
        for item in payload:
            nested = _find_nested_value(item, normalized_key)
            if nested is not None:
                return nested
    return None


def _normalize_luoyun_stage(value: object) -> str:
    normalized = str(value or "").strip()
    return normalized or "未知"


def _resolve_luoyun_last_irrigation_time(payload):
    raw_value = _find_nested_value(payload if isinstance(payload, dict) else {}, "last_irrigation_time")
    return _parse_iso_timestamp(raw_value)


def _resolve_luoyun_last_defend_time(payload):
    raw_value = _find_nested_value(payload if isinstance(payload, dict) else {}, "last_defend_time")
    return _parse_iso_timestamp(raw_value)


def parse_luoyun_tree_text(text, now=None):
    now = now or time.time()
    raw_text = str(text or "").strip()
    if not raw_text:
        return {
            "raw_text": "",
            "updated_at": now,
            "stage": "未知",
            "can_irrigate": False,
            "can_harvest": False,
            "already_picked": False,
            "remaining_text": "",
            "remaining_seconds": 0,
            "next_ready_time": 0,
            "current_points": 0,
        }

    remaining_match = re.search(r"⏳\s*剩余[:：]\s*(?P<value>[^\n]+)", raw_text)
    remaining_text = str((remaining_match.group("value") if remaining_match else "") or "").strip()
    remaining_seconds = _parse_duration_seconds(remaining_text)
    next_ready_time = now + remaining_seconds if remaining_seconds > 0 else 0

    current_status_match = re.search(r"👤\s*你的当前状态[:：]\s*(?P<value>[^\n]+)", raw_text)
    current_status_text = str((current_status_match.group("value") if current_status_match else "") or "").strip()
    current_points_match = re.search(r"(?P<value>\d+)\s*点", current_status_text)
    current_points = _parse_int(current_points_match.group("value") if current_points_match else 0, 0)

    progress_match = re.search(r"🌲\s*进度[:：][^\n]*\n?[^\n]*?(?P<percent>\d+(?:\.\d+)?)%", raw_text)
    progress_percent = float(progress_match.group("percent")) if progress_match else 0.0
    stage_match = re.search(r"🔄\s*阶段[:：]\s*(?P<current>\d+)\s*/\s*(?P<total>\d+)", raw_text)
    current_stage = _parse_int(stage_match.group("current") if stage_match else 0, 0)
    total_stage = _parse_int(stage_match.group("total") if stage_match else 0, 0)

    is_mature = "✨ 状态: 成熟采摘期" in raw_text or "✨ 状态：成熟采摘期" in raw_text
    invasion_detected = "⚔️ 警报: 古剑门入侵中！" in raw_text or "⚔️ 警报：古剑门入侵中！" in raw_text
    already_picked = "已采摘" in current_status_text or "奖励已入袋" in current_status_text
    has_numeric_status = current_points >= 0 and not already_picked
    is_growth = (bool(progress_match) or stage_match is not None) and not is_mature and has_numeric_status

    stage = "未知"
    if is_mature:
        stage = "成熟采摘期"
    elif already_picked:
        stage = "已采摘"
    elif is_growth:
        stage = "生长阶段"

    return {
        "raw_text": raw_text[:4000],
        "updated_at": now,
        "stage": stage,
        "progress_percent": progress_percent,
        "current_stage": current_stage,
        "total_stage": total_stage,
        "can_irrigate": is_growth,
        "can_harvest": is_mature and has_numeric_status,
        "already_picked": already_picked,
        "remaining_text": remaining_text,
        "remaining_seconds": remaining_seconds,
        "next_ready_time": next_ready_time,
        "current_status_text": current_status_text,
        "current_points": current_points,
        "is_mature": is_mature,
        "invasion_detected": invasion_detected,
    }


def _load_luoyun_state(session):
    return _parse_json_dict((session or {}).get("luoyun_last_tree_state"))


def _load_luoyun_pending_commands(session):
    return [
        str(item or "").strip()
        for item in _parse_json_list((session or {}).get("luoyun_pending_commands"))
        if str(item or "").strip()
    ]


def _save_luoyun_pending_commands(commands):
    normalized = [
        str(command or "").strip()
        for command in (commands or [])
        if str(command or "").strip()
    ]
    return json.dumps(normalized, ensure_ascii=False) if normalized else None


def has_active_luoyun_batch(session):
    return bool(_load_luoyun_pending_commands(session))


def build_luoyun_view(payload, session=None, now=None):
    now = now or time.time()
    session = session or {}
    state = _load_luoyun_state(session)
    last_irrigation_time = _resolve_luoyun_last_irrigation_time(payload)
    if not last_irrigation_time:
        last_irrigation_time = float(state.get("last_irrigation_time") or 0)
    irrigation_ready_time = (
        last_irrigation_time + LUOYUN_IRRIGATION_COOLDOWN_SECONDS if last_irrigation_time else 0
    )
    last_defend_time = _resolve_luoyun_last_defend_time(payload)
    defend_ready_time = last_defend_time + 300 if last_defend_time else 0
    next_ready_time = float(state.get("next_ready_time") or 0)
    current_stage = _normalize_luoyun_stage(state.get("stage"))
    current_status_text = str(state.get("current_status_text") or "").strip()
    current_points = _parse_int(state.get("current_points"), 0)
    can_irrigate = bool(state.get("can_irrigate"))
    can_harvest = bool(state.get("can_harvest"))
    already_picked = bool(state.get("already_picked"))
    remaining_text = str(state.get("remaining_text") or "").strip()
    invasion_active = bool(session.get("luoyun_invasion_active"))
    frozen_irrigation_ready_time = float(session.get("luoyun_frozen_irrigation_ready_time") or 0)
    if irrigation_ready_time > now and not can_irrigate and not can_harvest and not next_ready_time:
        next_ready_time = irrigation_ready_time
    if invasion_active and frozen_irrigation_ready_time > 0:
        irrigation_ready_time = frozen_irrigation_ready_time
    return {
        "auto_enabled": bool(session.get("auto_luoyun_enabled")),
        "last_irrigation_time": last_irrigation_time,
        "irrigation_ready_time": irrigation_ready_time,
        "last_defend_time": last_defend_time,
        "defend_ready_time": defend_ready_time,
        "next_check_time": float(session.get("luoyun_next_check_time") or 0),
        "next_check_source": str(session.get("luoyun_next_check_source") or "").strip(),
        "pending_count": len(_load_luoyun_pending_commands(session)),
        "pending_index": int(session.get("luoyun_pending_index") or 0),
        "remaining_text": remaining_text,
        "remaining_seconds": _parse_int(state.get("remaining_seconds"), 0),
        "next_ready_time": next_ready_time,
        "stage": current_stage,
        "current_status_text": current_status_text,
        "current_points": current_points,
        "can_irrigate": can_irrigate,
        "can_harvest": can_harvest,
        "already_picked": already_picked,
        "invasion_active": invasion_active,
        "frozen_irrigation_ready_time": frozen_irrigation_ready_time,
        "source": str(state.get("source") or "session").strip() or "session",
        "updated_at": float(state.get("updated_at") or 0),
        "status_text": str((session or {}).get("luoyun_last_tree_text") or "").strip(),
    }


def build_luoyun_auto_commands(session):
    state = _load_luoyun_state(session)
    if bool((session or {}).get("luoyun_invasion_active")):
        return [".协同守山"]
    commands = []
    if bool(state.get("can_irrigate")):
        commands.append(".灵树灌溉")
    if bool(state.get("can_harvest")):
        commands.append(".采摘灵果")
    return commands


def _normalize_plot_value(value) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(\d+)", text)
    return match.group(1) if match else text


def _normalize_huangfeng_status(status) -> str:
    return str(status or "").strip().lower()


def _huangfeng_status_meta(status: str) -> dict:
    normalized = _normalize_huangfeng_status(status)
    mapping = {
        "growing": {"label": "生长中", "action": "无需处理"},
        "dry": {"label": "干旱", "action": "需浇水"},
        "pests": {"label": "虫害", "action": "需除虫"},
        "weeds": {"label": "杂草", "action": "需除草"},
        "mature": {"label": "已成熟", "action": "可采药"},
        "ready": {"label": "已成熟", "action": "可采药"},
        "idle": {"label": "空闲", "action": "可播种"},
    }
    if not normalized or normalized == "null":
        return {"label": "空闲", "action": "可播种"}
    return mapping.get(normalized, {"label": normalized or "未知", "action": "待确认"})


def _resolve_huangfeng_seed_names(payload, game_items=None) -> dict:
    inventory = (payload or {}).get("inventory") or {}
    items = inventory.get("items") or []
    seed_name_map = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip() != "seed":
            continue
        item_id = str(item.get("item_id") or "").strip()
        item_name = str(item.get("name") or "").strip()
        if item_id and item_name:
            seed_name_map[item_id] = item_name
    if game_items:
        for seed_id, item_info in (game_items or {}).items():
            if seed_id.startswith("seed_") and seed_id not in seed_name_map:
                seed_name_map[seed_id] = str(item_info.get("name") or seed_id).strip()
    return seed_name_map


def _resolve_huangfeng_seed_options(payload) -> list[dict]:
    inventory = (payload or {}).get("inventory") or {}
    items = inventory.get("items") or []
    options = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip() != "seed":
            continue
        name = str(item.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        quantity = _parse_int(item.get("quantity"), 0)
        options.append(
            {
                "value": name,
                "label": f"{name}（{quantity}）" if quantity >= 0 else name,
                "quantity": quantity,
            }
        )
    options.sort(key=lambda item: item.get("label") or item.get("value") or "")
    return options


def parse_huangfeng_garden_payload(payload, game_items=None):
    herb_garden = (payload or {}).get("herb_garden") or {}
    if isinstance(herb_garden, str) and herb_garden.strip():
        try:
            herb_garden = json.loads(herb_garden)
        except json.JSONDecodeError:
            herb_garden = {}
    if not isinstance(herb_garden, dict):
        return {"size": 0, "plots": [], "updated_at": time.time(), "source": "payload"}
    raw_plots = herb_garden.get("plots") or {}
    if not isinstance(raw_plots, dict):
        raw_plots = {}
    seed_name_map = _resolve_huangfeng_seed_names(payload or {}, game_items=game_items)
    seed_options = _resolve_huangfeng_seed_options(payload or {})
    plots = []
    for raw_plot, raw_state in raw_plots.items():
        plot_id = _normalize_plot_value(raw_plot)
        state = raw_state if isinstance(raw_state, dict) else {}
        status = _normalize_huangfeng_status(state.get("status"))
        meta = _huangfeng_status_meta(status)
        seed_id = str(state.get("seed_id") or "").strip()
        plots.append(
            {
                "plot": plot_id,
                "status": status,
                "status_label": meta["label"],
                "suggested_action": meta["action"],
                "seed_id": seed_id,
                "seed_name": seed_name_map.get(seed_id) or seed_id,
                "plant_time": str(state.get("plant_time") or "").strip(),
                "is_idle": status in {"", "idle", "null"} and not seed_id,
                "has_weeds": status == "weeds",
                "has_insects": status == "pests",
                "is_dry": status == "dry",
                "is_mature": status in {"mature", "ready"},
                "is_growing": status == "growing",
            }
        )
    plots.sort(
        key=lambda item: (_parse_int(item.get("plot"), 9999), item.get("plot") or "")
    )
    return {
        "size": _parse_int(herb_garden.get("size"), len(plots)),
        "plots": plots,
        "updated_at": time.time(),
        "source": "payload",
        "seed_options": seed_options,
    }


def parse_huangfeng_garden_text(text):
    raw_text = str(text or "").strip()
    if not raw_text:
        return {"plots": []}
    plots = []
    seen = set()
    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        match = HUANGFENG_PLOT_STATUS_PATTERN.search(stripped)
        if not match:
            continue
        plot = _normalize_plot_value(match.group("plot"))
        if not plot or plot in seen:
            continue
        seen.add(plot)
        is_idle = any(
            keyword in stripped for keyword in ["空闲", "未播种", "暂无作物", "可播种"]
        )
        has_weeds = any(keyword in stripped for keyword in ["杂草", "长草", "荒草"])
        has_insects = any(
            keyword in stripped for keyword in ["虫", "虫害", "害虫", "生虫"]
        )
        is_dry = any(keyword in stripped for keyword in ["干", "干涸", "缺水", "干旱"])
        is_mature = any(
            keyword in stripped
            for keyword in ["成熟", "可采", "可收获", "已成熟", "ready"]
        )
        status = ""
        if is_mature:
            status = "ready"
        elif has_weeds:
            status = "weeds"
        elif has_insects:
            status = "pests"
        elif is_dry:
            status = "dry"
        elif is_idle:
            status = "idle"
        else:
            status = "growing"
        meta = _huangfeng_status_meta(status)
        plots.append(
            {
                "plot": plot,
                "text": stripped,
                "status": status,
                "status_label": meta["label"],
                "suggested_action": meta["action"],
                "is_idle": is_idle,
                "has_weeds": has_weeds,
                "has_insects": has_insects,
                "is_dry": is_dry,
                "is_mature": is_mature,
            }
        )
    plots.sort(
        key=lambda item: (_parse_int(item.get("plot"), 9999), item.get("plot") or "")
    )
    return {
        "plots": plots,
        "updated_at": time.time(),
        "raw_text": raw_text[:4000],
    }


def _load_huangfeng_state(session):
    return _parse_json_dict((session or {}).get("huangfeng_last_garden_state"))


def _load_huangfeng_pending_commands(session):
    return [
        str(item or "").strip()
        for item in _parse_json_list((session or {}).get("huangfeng_pending_commands"))
        if str(item or "").strip()
    ]


def _save_huangfeng_pending_commands(commands):
    normalized = [
        str(command or "").strip()
        for command in (commands or [])
        if str(command or "").strip()
    ]
    return json.dumps(normalized, ensure_ascii=False) if normalized else None


def _get_huangfeng_known_plots(session) -> list[str]:
    state = _load_huangfeng_state(session)
    plots = []
    for entry in state.get("plots") or []:
        plot = _normalize_plot_value((entry or {}).get("plot"))
        if plot and plot not in plots:
            plots.append(plot)
    return plots


def has_active_huangfeng_batch(session):
    return bool(_load_huangfeng_pending_commands(session))


def _is_huangfeng_seed_shortage(text: str) -> bool:
    normalized = str(text or "").strip()
    return any(keyword in normalized for keyword in HUANGFENG_SEED_SHORTAGE_KEYWORDS)


def _build_huangfeng_exchange_command(seed_name: str) -> str:
    normalized_seed = str(seed_name or "").strip()
    return f".兑换 {normalized_seed}*3" if normalized_seed else ""


def build_huangfeng_auto_commands(session):
    seed_name = str((session or {}).get("huangfeng_seed_name") or "").strip()
    state = _load_huangfeng_state(session)
    has_mature = False
    has_weeds = False
    has_insects = False
    has_dry = False
    has_idle = False
    for plot in state.get("plots") or []:
        if plot.get("is_mature"):
            has_mature = True
        if plot.get("has_weeds"):
            has_weeds = True
        if plot.get("has_insects"):
            has_insects = True
        if plot.get("is_dry"):
            has_dry = True
        if plot.get("is_idle"):
            has_idle = True
    commands = []
    if has_weeds:
        commands.append(".除草")
    if has_insects:
        commands.append(".除虫")
    if has_dry:
        commands.append(".浇水")
    if has_mature:
        commands.append(".采药")
    if has_idle and seed_name:
        commands.append(f".播种 {seed_name}")
    return commands


def build_huangfeng_view(payload, session=None, now=None, game_items=None):
    now = now or time.time()
    state = parse_huangfeng_garden_payload(payload, game_items=game_items)
    if not (state.get("plots") or []):
        state = _load_huangfeng_state(session)
    plots = list(state.get("plots") or [])
    seed_options = state.get("seed_options") or _resolve_huangfeng_seed_options(
        payload or {}
    )
    counts = {"growing": 0, "dry": 0, "pests": 0, "weeds": 0, "mature": 0, "idle": 0}
    for plot in plots:
        status = _normalize_huangfeng_status(plot.get("status"))
        if status in {"mature", "ready"}:
            counts["mature"] += 1
        elif status in counts:
            counts[status] += 1
        elif plot.get("is_mature"):
            counts["mature"] += 1
        elif plot.get("is_idle"):
            counts["idle"] += 1
    return {
        "size": _parse_int(state.get("size"), len(plots)),
        "plots": plots,
        "plot_count": len(plots),
        "source": state.get("source") or "session",
        "updated_at": float(state.get("updated_at") or now),
        "counts": counts,
        "auto_enabled": bool((session or {}).get("auto_huangfeng_enabled")),
        "seed_name": str((session or {}).get("huangfeng_seed_name") or "").strip(),
        "seed_options": seed_options,
        "exchange_enabled": bool(
            (session or {}).get("auto_huangfeng_exchange_enabled")
        ),
        "next_check_time": float((session or {}).get("huangfeng_next_check_time") or 0),
        "next_check_source": str(
            (session or {}).get("huangfeng_next_check_source") or ""
        ).strip(),
        "pending_count": len(_load_huangfeng_pending_commands(session)),
    }


def sync_huangfeng_state(storage, db, profile_id, chat_id, payload=None, now=None):
    now = now or time.time()
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session:
        return None, None

    if bool(session.get("huangfeng_batch_just_completed")):
        try:
            # A batch just finished, force a refresh of the external payload
            payload = sync_external_account(storage, profile_id)
            logger.info("强制刷新天机阁 payload 成功 profile=%s", profile_id)
        except Exception as exc:
            logger.warning("强制刷新天机阁 payload 失败 profile=%s: %s", profile_id, exc)
            # Fallback to cached payload
            if payload is None:
                payload = _read_cached_profile_payload(storage, profile_id)
        finally:
            # Clear the flag regardless of success or failure
            update_session(
                db, chat_id, profile_id=profile_id, huangfeng_batch_just_completed=0
            )
            session["huangfeng_batch_just_completed"] = 0
    elif payload is None:
        payload = _read_cached_profile_payload(storage, profile_id)
    else:
        if payload is None:
            payload = _read_cached_profile_payload(storage, profile_id)
    game_items = storage.get_game_items() if storage else None
    view = build_huangfeng_view(payload, session=session, now=now, game_items=game_items)
    updates = {"last_panel_time": now}
    if view.get("plots"):
        state_payload = {
            "size": view.get("size"),
            "plots": view.get("plots"),
            "updated_at": view.get("updated_at") or now,
            "source": view.get("source") or "payload",
        }
        updates["huangfeng_last_garden_state"] = json.dumps(
            state_payload, ensure_ascii=False
        )
        updates["huangfeng_last_garden_text"] = "\n".join(
            [
                f"{plot.get('plot')}号地：{plot.get('status_label')}"
                + (f"（{plot.get('seed_name')}）" if plot.get("seed_name") else "")
                for plot in view.get("plots") or []
            ]
        )[:4000]
        updates["huangfeng_payload_refresh_retry"] = 0
    elif (
        session.get("auto_huangfeng_enabled")
        and not has_active_huangfeng_batch(session)
        and (force_refresh or not (view.get("plots") or []))
    ):
        refresh_retry = int(session.get("huangfeng_payload_refresh_retry") or 0)
        if refresh_retry < HUANGFENG_PAYLOAD_REFRESH_MAX_RETRIES:
            try:
                fresh_payload = sync_external_account(storage, profile_id)
            except Exception as exc:
                logger.warning(
                    "黄枫谷天机阁接口刷新失败（第%d次） profile=%s: %s",
                    refresh_retry + 1,
                    profile_id,
                    exc,
                )
                if isinstance(exc, AscAuthError):
                    account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
                    storage.upsert_external_account(
                        profile_id=profile_id,
                        provider=ASC_PROVIDER,
                        telegram_user_id=account.get("telegram_user_id", ""),
                        telegram_username=account.get("telegram_username", ""),
                        status=account.get("status", ""),
                        cookie_text=account.get("cookie_text", ""),
                        me_payload=json.loads(account.get("me_json", "{}")),
                        api_token="",  # 清除 api_token 强制重刷
                    )
                mark_external_account_failure(storage, profile_id, exc)
                fresh_payload = None
            if fresh_payload and isinstance(fresh_payload, dict):
                view = build_huangfeng_view(fresh_payload, session=session, now=now, game_items=game_items)
                if view.get("plots"):
                    state_payload = {
                        "size": view.get("size"),
                        "plots": view.get("plots"),
                        "updated_at": view.get("updated_at") or now,
                        "source": "payload",
                    }
                    updates["huangfeng_last_garden_state"] = json.dumps(
                        state_payload, ensure_ascii=False
                    )
                    updates["huangfeng_last_garden_text"] = "\n".join(
                        [
                            f"{plot.get('plot')}号地：{plot.get('status_label')}"
                            + (
                                f"（{plot.get('seed_name')}）"
                                if plot.get("seed_name")
                                else ""
                            )
                            for plot in view.get("plots") or []
                        ]
                    )[:4000]
                    updates["huangfeng_payload_refresh_retry"] = 0
                else:
                    updates["huangfeng_payload_refresh_retry"] = refresh_retry + 1
            else:
                updates["huangfeng_payload_refresh_retry"] = refresh_retry + 1
        if int(updates.get("huangfeng_payload_refresh_retry") or 0) >= HUANGFENG_PAYLOAD_REFRESH_MAX_RETRIES:
            logger.warning(
                "黄枫谷天机阁接口连续%d次刷新失败 profile=%s，停止自动化",
                HUANGFENG_PAYLOAD_REFRESH_MAX_RETRIES,
                profile_id,
            )
            updates["auto_huangfeng_enabled"] = 0
            updates["huangfeng_payload_refresh_retry"] = 0
            updates["huangfeng_next_check_source"] = (
                f"天机阁接口连续{HUANGFENG_PAYLOAD_REFRESH_MAX_RETRIES}次刷新失败，已停止黄枫谷自动化"
            )
    if session.get("auto_huangfeng_enabled") and not has_active_huangfeng_batch(
        session
    ):
        simulated_session = dict(session)
        simulated_session.update(updates)
        auto_commands = build_huangfeng_auto_commands(simulated_session)
        if auto_commands:
            updates["huangfeng_pending_commands"] = (
                _save_huangfeng_pending_commands(auto_commands)
            )
            updates["huangfeng_pending_index"] = 0
            updates["huangfeng_pending_msg_id"] = 0
            updates["huangfeng_pending_retry"] = 0
            updates["huangfeng_next_check_time"] = 0
            updates["huangfeng_next_check_source"] = (
                f"药园存在 {len(auto_commands)} 条待处理动作，可立即执行"
            )
        else:
            updates["huangfeng_next_check_time"] = now + HUANGFENG_AUTO_CHECK_SECONDS
            updates["huangfeng_next_check_source"] = "药园状态稳定，30 分钟后复查"
    updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
    if _has_any_auto_keys(session):
        updates["next_check_source"] = "已同步黄枫谷药园状态"
    update_session(db, chat_id, profile_id=profile_id, **updates)
    return get_session(db, chat_id, profile_id=profile_id), view


def configure_huangfeng_auto(
    db,
    chat_id,
    enabled,
    *,
    seed_name=None,
    exchange_enabled=None,
    profile_id=None,
):
    session = get_session(db, chat_id, profile_id=profile_id)
    updates = {
        "auto_huangfeng_enabled": _normalize_bool(enabled),
        "huangfeng_next_check_time": 0,
        "huangfeng_next_check_source": (
            "已开启黄枫谷自动化，等待首轮药园检查" if enabled else "已关闭黄枫谷自动化"
        ),
        "next_check_time": 0
        if enabled
        else _recompute_overall_next_check(
            session, {"auto_huangfeng_enabled": 0}, time.time()
        ),
        "next_check_source": (
            "已开启黄枫谷自动化，等待首轮药园检查" if enabled else "已关闭黄枫谷自动化"
        ),
    }
    if seed_name is not None:
        updates["huangfeng_seed_name"] = str(seed_name or "").strip()
    if exchange_enabled is not None:
        updates["auto_huangfeng_exchange_enabled"] = _normalize_bool(exchange_enabled)
    if not enabled:
        updates.update(
            {
                "huangfeng_pending_commands": None,
                "huangfeng_pending_index": 0,
                "huangfeng_pending_msg_id": 0,
                "huangfeng_pending_retry": 0,
                "huangfeng_payload_refresh_retry": 0,
                "huangfeng_next_check_time": 0,
                "huangfeng_next_check_source": "已关闭黄枫谷自动化",
            }
        )
    update_session(db, chat_id, profile_id=profile_id, **updates)


def set_huangfeng_seed(db, chat_id, seed_name, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        huangfeng_seed_name=str(seed_name or "").strip(),
    )


def set_huangfeng_exchange_auto(db, chat_id, enabled, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_huangfeng_exchange_enabled=_normalize_bool(enabled),
    )


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


def _active_huangfeng_auto_keys(session):
    keys = []
    if session.get("auto_huangfeng_enabled"):
        keys.append("garden")
    return keys


def _active_luoyun_auto_keys(session):
    keys = []
    if session.get("auto_luoyun_enabled"):
        keys.append("tree")
    return keys


def _has_any_auto_keys(session):
    return bool(
        _active_common_auto_keys(session)
        or _active_yinluo_auto_keys(session)
        or _active_huangfeng_auto_keys(session)
        or _active_luoyun_auto_keys(session)
        or _active_lingxiao_auto_keys(session)
        or bool(session.get("auto_companion_greet_enabled"))
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
        ("auto_huangfeng_enabled", "huangfeng_next_check_time"),
        ("auto_luoyun_enabled", "luoyun_next_check_time"),
        ("auto_lingxiao_enabled", "lingxiao_next_check_time"),
        ("auto_lingxiao_gangfeng_enabled", "lingxiao_gangfeng_next_check_time"),
        ("auto_lingxiao_borrow_enabled", "lingxiao_borrow_next_check_time"),
        ("auto_lingxiao_question_enabled", "lingxiao_question_next_check_time"),
        ("auto_companion_greet_enabled", "companion_greet_next_check_time"),
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


def _luoyun_action_still_syncing(session, now, *, command_text: str) -> bool:
    last_action = str((session or {}).get("last_action") or "").strip()
    last_action_time = float((session or {}).get("last_action_time") or 0)
    if last_action != str(command_text or "").strip() or not last_action_time:
        return False
    if command_text == ".灵树状态":
        next_check_source = str((session or {}).get("luoyun_next_check_source") or "").strip()
        pending_msg_id = int((session or {}).get("luoyun_pending_msg_id") or 0)
        waiting_source_matches = (
            ".灵树状态" in next_check_source and "等待机器人回包" in next_check_source
        )
        if not waiting_source_matches and pending_msg_id <= 0:
            return False
    return now - last_action_time < LUOYUN_BATCH_TIMEOUT_SECONDS


def _luoyun_sync_retry_time(session, now) -> float:
    last_action_time = float((session or {}).get("last_action_time") or 0)
    hard_deadline = (
        last_action_time + LUOYUN_BATCH_TIMEOUT_SECONDS if last_action_time else now + LUOYUN_COMMAND_REFRESH_SECONDS
    )
    return min(hard_deadline, now + LUOYUN_COMMAND_REFRESH_SECONDS)


def sync_common_sect_state(storage, db, profile_id, chat_id, payload=None, now=None):
    now = now or time.time()
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session:
        return None, None
    force_refresh = bool(session.get("sect_common_force_refresh"))
    if force_refresh:
        try:
            payload = sync_external_account(storage, profile_id)
            logger.info("宗门自动任务强制刷新天机阁 payload 成功 profile=%s", profile_id)
        except Exception as exc:
            logger.warning("宗门自动任务强制刷新天机阁 payload 失败 profile=%s: %s", profile_id, exc)
            if payload is None:
                payload = _read_cached_profile_payload(storage, profile_id)
    elif payload is None:
        payload = _read_cached_profile_payload(storage, profile_id)
    daily = _extract_sect_daily_state(payload, now)
    today_key = current_date_key(now)
    checkin_pending_date = _parse_date_key(session.get("sect_checkin_pending_date"))
    teach_pending_date = _parse_date_key(session.get("sect_teach_pending_date"))
    teach_pending_target_count = max(
        _parse_int(session.get("sect_teach_pending_target_count"), 0), 0
    )
    checkin_syncing = _common_action_still_syncing(
        session, now, command_text=".宗门点卯"
    )
    teach_syncing = _common_action_still_syncing(
        session, now, command_text=".宗门传功"
    )
    session_teach_date = _parse_date_key(session.get("last_teach_date"))
    session_teach_count = max(_parse_int(session.get("last_teach_count"), 0), 0)
    if session_teach_date == today_key and session_teach_count > daily["teach_count"]:
        daily["last_teach_date"] = session_teach_date
        daily["teach_count"] = session_teach_count
    if (
        teach_pending_date == today_key
        and teach_syncing
        and teach_pending_target_count > daily["teach_count"]
    ):
        daily["last_teach_date"] = today_key
        daily["teach_count"] = teach_pending_target_count
    updates = {
        "last_sign_date": _parse_date_key(payload.get("last_sect_check_in")),
        "last_teach_date": daily["last_teach_date"] or None,
        "last_teach_count": daily["teach_count"],
        "sect_common_force_refresh": 0,
    }
    if checkin_pending_date and (checkin_pending_date != today_key or not checkin_syncing):
        updates["sect_checkin_pending_date"] = None
        checkin_pending_date = ""
        checkin_syncing = False
    if teach_pending_date and (teach_pending_date != today_key or not teach_syncing):
        updates["sect_teach_pending_date"] = None
        updates["sect_teach_pending_target_count"] = 0
        teach_pending_date = ""
        teach_pending_target_count = 0
        teach_syncing = False

    if session.get("auto_sect_checkin_enabled"):
        next_check_in_time = _next_daily_random_window_timestamp(
            session, "sect_checkin", now
        )
        if daily["checked_in_today"]:
            updates["sect_checkin_next_check_time"] = next_check_in_time
            updates["sect_checkin_next_check_source"] = (
                f"今日已点卯，等待次日 {SECT_AUTO_WINDOW_START_TIME}-{SECT_AUTO_WINDOW_END_TIME} 随机执行"
            )
        else:
            today_check_in_time = _daily_random_window_timestamp(
                session, "sect_checkin", now
            )
            if checkin_pending_date == today_key and checkin_syncing:
                retry_time = min(
                    float(session.get("last_action_time") or 0)
                    + LINGXIAO_ACTION_SYNC_TIMEOUT_SECONDS,
                    now + LINGXIAO_COMMAND_REFRESH_SECONDS,
                )
                updates["sect_checkin_next_check_time"] = max(retry_time, now + 3)
                updates["sect_checkin_next_check_source"] = (
                    "已发送 .宗门点卯，等待确认或缓存刷新"
                )
            elif now < today_check_in_time:
                updates["sect_checkin_next_check_time"] = today_check_in_time
                updates["sect_checkin_next_check_source"] = (
                    f"等待 {SECT_AUTO_WINDOW_START_TIME}-{SECT_AUTO_WINDOW_END_TIME} 随机时间执行宗门点卯"
                )
            else:
                updates["sect_checkin_next_check_time"] = 0
                updates["sect_checkin_next_check_source"] = "可执行宗门点卯"

    if session.get("auto_sect_teach_enabled"):
        next_teach_time = _next_daily_random_window_timestamp(session, "sect_teach", now)
        if daily["teach_count"] >= SECT_DAILY_TEACH_LIMIT:
            updates["sect_teach_next_check_time"] = next_teach_time
            updates["sect_teach_next_check_source"] = (
                f"今日已传功 {daily['teach_count']}/{SECT_DAILY_TEACH_LIMIT}，等待次日 {SECT_AUTO_WINDOW_START_TIME}-{SECT_AUTO_WINDOW_END_TIME} 随机执行"
            )
        else:
            today_teach_time = _daily_random_window_timestamp(session, "sect_teach", now)
            if teach_pending_date == today_key and teach_syncing:
                retry_time = min(
                    float(session.get("last_action_time") or 0)
                    + LINGXIAO_ACTION_SYNC_TIMEOUT_SECONDS,
                    now + SECT_AUTO_TEACH_REPLY_RECHECK_SECONDS,
                )
                updates["sect_teach_next_check_time"] = max(retry_time, now + 3)
                updates["sect_teach_next_check_source"] = (
                    f"已发送 .宗门传功，等待确认或缓存刷新 ({daily['teach_count']}/{SECT_DAILY_TEACH_LIMIT})"
                )
            elif now < today_teach_time:
                updates["sect_teach_next_check_time"] = today_teach_time
                updates["sect_teach_next_check_source"] = (
                    f"等待 {SECT_AUTO_WINDOW_START_TIME}-{SECT_AUTO_WINDOW_END_TIME} 随机时间执行宗门传功"
                )
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
    if not session:
        return None, None
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
        identifiers = get_cultivator_lookup_candidates(profile)
        default_cookie = get_effective_external_cookie(storage)
        cookie_text = (external_account.get("cookie_text") or default_cookie).strip()
        if not identifiers or not cookie_text:
            message = "缺少天机阁 cookie 或可用的用户名/姓名，无法同步凌霄宫状态"
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
            auto_huangfeng_enabled INTEGER DEFAULT 0,
            auto_huangfeng_exchange_enabled INTEGER DEFAULT 0,
            huangfeng_seed_name TEXT,
            huangfeng_next_check_time REAL DEFAULT 0,
            huangfeng_next_check_source TEXT,
            huangfeng_last_garden_text TEXT,
            huangfeng_last_garden_state TEXT,
            huangfeng_pending_commands TEXT,
            huangfeng_pending_index INTEGER DEFAULT 0,
            huangfeng_pending_msg_id INTEGER DEFAULT 0,
            huangfeng_pending_retry INTEGER DEFAULT 0,
            huangfeng_payload_refresh_retry INTEGER DEFAULT 0,
            huangfeng_batch_just_completed INTEGER DEFAULT 0,
            auto_luoyun_enabled INTEGER DEFAULT 0,
            luoyun_next_check_time REAL DEFAULT 0,
            luoyun_next_check_source TEXT,
            luoyun_last_tree_text TEXT,
            luoyun_last_tree_state TEXT,
            luoyun_pending_commands TEXT,
            luoyun_pending_index INTEGER DEFAULT 0,
            luoyun_pending_msg_id INTEGER DEFAULT 0,
            luoyun_pending_retry INTEGER DEFAULT 0,
            luoyun_batch_just_completed INTEGER DEFAULT 0,
            luoyun_force_refresh INTEGER DEFAULT 0,
            luoyun_invasion_active INTEGER DEFAULT 0,
            luoyun_frozen_irrigation_ready_time REAL DEFAULT 0,
            yinluo_batch_mode TEXT,
            yinluo_batch_commands TEXT,
            yinluo_batch_index INTEGER DEFAULT 0,
            yinluo_batch_pending_msg_id INTEGER DEFAULT 0,
            yinluo_batch_started_at REAL DEFAULT 0,
            last_panel_time REAL DEFAULT 0,
            last_bounty_time REAL DEFAULT 0,
            last_sign_date TEXT,
            sect_checkin_pending_date TEXT,
            last_teach_date TEXT,
            last_teach_count INTEGER DEFAULT 0,
            sect_teach_pending_date TEXT,
            sect_teach_pending_target_count INTEGER DEFAULT 0,
            sect_common_force_refresh INTEGER DEFAULT 0,
            last_yinluo_sacrifice_date TEXT,
            last_command_msg_id INTEGER DEFAULT 0,
            auto_companion_greet_enabled INTEGER DEFAULT 0,
            companion_greet_next_check_time REAL DEFAULT 0,
            companion_greet_next_check_source TEXT,
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
        "auto_huangfeng_enabled": "INTEGER DEFAULT 0",
        "auto_huangfeng_exchange_enabled": "INTEGER DEFAULT 0",
        "huangfeng_seed_name": "TEXT",
        "huangfeng_next_check_time": "REAL DEFAULT 0",
        "huangfeng_next_check_source": "TEXT",
        "huangfeng_last_garden_text": "TEXT",
        "huangfeng_last_garden_state": "TEXT",
        "huangfeng_pending_commands": "TEXT",
        "huangfeng_pending_index": "INTEGER DEFAULT 0",
        "huangfeng_pending_msg_id": "INTEGER DEFAULT 0",
        "huangfeng_pending_retry": "INTEGER DEFAULT 0",
        "huangfeng_payload_refresh_retry": "INTEGER DEFAULT 0",
        "huangfeng_batch_just_completed": "INTEGER DEFAULT 0",
        "auto_luoyun_enabled": "INTEGER DEFAULT 0",
        "luoyun_next_check_time": "REAL DEFAULT 0",
        "luoyun_next_check_source": "TEXT",
        "luoyun_last_tree_text": "TEXT",
        "luoyun_last_tree_state": "TEXT",
        "luoyun_pending_commands": "TEXT",
        "luoyun_pending_index": "INTEGER DEFAULT 0",
        "luoyun_pending_msg_id": "INTEGER DEFAULT 0",
        "luoyun_pending_retry": "INTEGER DEFAULT 0",
        "luoyun_batch_just_completed": "INTEGER DEFAULT 0",
        "luoyun_force_refresh": "INTEGER DEFAULT 0",
        "luoyun_invasion_active": "INTEGER DEFAULT 0",
        "luoyun_frozen_irrigation_ready_time": "REAL DEFAULT 0",
        "luoyun_last_passive_tree_check": "REAL DEFAULT 0",
        "yinluo_batch_mode": "TEXT",
        "yinluo_batch_commands": "TEXT",
        "yinluo_batch_index": "INTEGER DEFAULT 0",
        "yinluo_batch_pending_msg_id": "INTEGER DEFAULT 0",
        "yinluo_batch_started_at": "REAL DEFAULT 0",
        "last_panel_time": "REAL DEFAULT 0",
        "last_bounty_time": "REAL DEFAULT 0",
        "last_sign_date": "TEXT",
        "sect_checkin_pending_date": "TEXT",
        "last_teach_date": "TEXT",
        "last_teach_count": "INTEGER DEFAULT 0",
        "sect_teach_pending_date": "TEXT",
        "sect_teach_pending_target_count": "INTEGER DEFAULT 0",
        "sect_common_force_refresh": "INTEGER DEFAULT 0",
        "last_yinluo_sacrifice_date": "TEXT",
        "last_command_msg_id": "INTEGER DEFAULT 0",
        "auto_companion_greet_enabled": "INTEGER DEFAULT 0",
        "companion_greet_next_check_time": "REAL DEFAULT 0",
        "companion_greet_next_check_source": "TEXT",
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

    if ".灵树状态" in text or "✨ 状态:" in text or "🌲 进度:" in text or "👤 你的当前状态:" in text:
        luoyun_state = parse_luoyun_tree_text(text)
        return {
            "event": "luoyun_tree_status",
            "summary": f"收到灵树状态：{luoyun_state.get('stage') or '未知'}",
            "luoyun_tree_state": luoyun_state,
        }

    if "当前并无外敌入侵，无需加固大阵。" in text:
        return {
            "event": "luoyun_invasion_clear",
            "summary": "当前无外敌入侵，恢复灵树循环",
        }

    if "地脉灵气尚未恢复" in text:
        return {
            "event": "luoyun_tree_water",
            "summary": "灵树灌溉冷却中",
        }
    if "灌溉" in text and any(keyword in text for keyword in ["灵树", "浇灌", "灌溉成功"]):
        return {
            "event": "luoyun_tree_water",
            "summary": "灵树灌溉完成",
        }

    if "【守山成功】" in text or ".协同守山" in text or "无需加固大阵" in text:
        return {
            "event": "luoyun_guard",
            "summary": "协同守山完成",
        }

    if "采摘" in text and any(keyword in text for keyword in ["灵果", "奖励", "已入袋"]):
        return {
            "event": "luoyun_tree_harvest",
            "summary": "灵果采摘完成",
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
            f"自动黄枫谷: {'开启' if session.get('auto_huangfeng_enabled') else '关闭'}",
            f"黄枫谷种子: {session.get('huangfeng_seed_name') or '-'}",
            f"自动兑换种子: {'开启' if session.get('auto_huangfeng_exchange_enabled') else '关闭'}",
            f"下次黄枫检查: {format_timestamp(session.get('huangfeng_next_check_time') or 0)}",
            f"黄枫检查来源: {session.get('huangfeng_next_check_source') or '-'}",
            f"黄枫待执行批次: {int(session.get('huangfeng_pending_index') or 0)} / {len(_load_huangfeng_pending_commands(session))}",
            f"自动落云灵树: {'开启' if session.get('auto_luoyun_enabled') else '关闭'}",
            f"下次灵树检查: {format_timestamp(session.get('luoyun_next_check_time') or 0)}",
            f"灵树检查来源: {session.get('luoyun_next_check_source') or '-'}",
            f"灵树待执行批次: {int(session.get('luoyun_pending_index') or 0)} / {len(_load_luoyun_pending_commands(session))}",
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


def stop_all_automation(db, chat_id, reason="", profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        enabled=0,
        next_check_time=0,
        next_check_source=reason or None,
        auto_lingxiao_enabled=0,
        auto_lingxiao_gangfeng_enabled=0,
        auto_lingxiao_borrow_enabled=0,
        auto_lingxiao_question_enabled=0,
        auto_sect_checkin_enabled=0,
        auto_sect_teach_enabled=0,
        auto_yinluo_sacrifice_enabled=0,
        auto_yinluo_blood_wash_enabled=0,
        auto_huangfeng_enabled=0,
        auto_huangfeng_exchange_enabled=0,
        auto_luoyun_enabled=0,
        lingxiao_next_check_time=0,
        lingxiao_gangfeng_next_check_time=0,
        lingxiao_borrow_next_check_time=0,
        lingxiao_question_next_check_time=0,
        sect_checkin_next_check_time=0,
        sect_teach_next_check_time=0,
        yinluo_sacrifice_next_check_time=0,
        yinluo_blood_wash_next_check_time=0,
        huangfeng_next_check_time=0,
        luoyun_next_check_time=0,
        last_summary=reason or None,
    )


def apply_sect_auto_guard(storage, db, session, profile_id=None, payload=None, now=None):
    now = now or time.time()
    if not session or not session.get("enabled"):
        return session, False
    if payload is None and profile_id:
        payload = _read_cached_profile_payload(storage, profile_id)
    current_sect_name = _normalize_sect_name_text((payload or {}).get("sect_name") or "")
    updates = _build_sect_auto_guard_updates(session, current_sect_name, now)
    if not updates:
        return session, False
    update_session(
        db,
        session["chat_id"],
        profile_id=profile_id,
        **updates,
    )
    refreshed = get_session(db, session["chat_id"], profile_id=profile_id) or session
    return refreshed, True


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


def configure_companion_greet_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_companion_greet_enabled=_normalize_bool(enabled),
        companion_greet_next_check_time=0,
        companion_greet_next_check_source=(
            "已开启自动问安，等待首轮同步" if enabled else None
        ),
        next_check_time=(
            0
            if enabled
            else _recompute_overall_next_check(
                session, {"auto_companion_greet_enabled": 0}, time.time()
            )
        ),
        next_check_source=("已开启自动问安，等待首轮同步" if enabled else None),
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


def clear_huangfeng_batch(
    db, chat_id, summary="", profile_id=None, *, next_check_time=0
):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        huangfeng_pending_commands=None,
        huangfeng_pending_index=0,
        huangfeng_pending_msg_id=0,
        huangfeng_pending_retry=0,
        huangfeng_payload_refresh_retry=0,
        huangfeng_next_check_time=next_check_time,
        huangfeng_next_check_source=summary or None,
        last_summary=summary or None,
    )


def clear_luoyun_batch(
    db, chat_id, summary="", profile_id=None, *, next_check_time=0, keep_state=True
):
    updates = {
        "luoyun_pending_commands": None,
        "luoyun_pending_index": 0,
        "luoyun_pending_msg_id": 0,
        "luoyun_pending_retry": 0,
        "luoyun_batch_just_completed": 0,
        "luoyun_force_refresh": 0,
        "luoyun_next_check_time": next_check_time,
        "luoyun_next_check_source": summary or None,
        "last_summary": summary or None,
        "last_action": None,
        "last_action_time": 0,
        "last_command_time": 0,
        "last_command_msg_id": 0,
    }
    if not keep_state:
        updates["luoyun_last_tree_text"] = None
        updates["luoyun_last_tree_state"] = None
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        **updates,
    )


def configure_luoyun_auto(db, chat_id, enabled, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    updates = {
        "auto_luoyun_enabled": _normalize_bool(enabled),
        "luoyun_next_check_time": 0,
        "luoyun_next_check_source": (
            "已开启落云宗灵树自动，等待首轮灵树检查" if enabled else "已关闭落云宗灵树自动"
        ),
        "luoyun_force_refresh": 1 if enabled else 0,
        "next_check_time": 0
        if enabled
        else _recompute_overall_next_check(session, {"auto_luoyun_enabled": 0}, time.time()),
        "next_check_source": (
            "已开启落云宗灵树自动，等待首轮灵树检查" if enabled else "已关闭落云宗灵树自动"
        ),
        "luoyun_pending_commands": None,
        "luoyun_pending_index": 0,
        "luoyun_pending_msg_id": 0,
        "luoyun_pending_retry": 0,
        "luoyun_batch_just_completed": 0,
        "luoyun_last_tree_text": None,
        "luoyun_last_tree_state": None,
        "luoyun_invasion_active": 0,
        "luoyun_frozen_irrigation_ready_time": 0,
        "last_action": None,
        "last_action_time": 0,
        "last_command_time": 0,
        "last_command_msg_id": 0,
    }
    update_session(db, chat_id, profile_id=profile_id, **updates)


def sync_luoyun_state(storage, db, profile_id, chat_id, payload=None, now=None):
    now = now or time.time()
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session:
        return None, None
    force_refresh = bool(session.get("luoyun_force_refresh")) or bool(
        session.get("luoyun_batch_just_completed")
    )
    if force_refresh:
        try:
            payload = sync_external_account(storage, profile_id)
            logger.info("落云宗自动强制刷新天机阁 payload 成功 profile=%s", profile_id)
        except Exception as exc:
            logger.warning(
                "落云宗自动强制刷新天机阁 payload 失败 profile=%s: %s",
                profile_id,
                exc,
            )
            if payload is None:
                payload = _read_cached_profile_payload(storage, profile_id)
    elif payload is None:
        payload = _read_cached_profile_payload(storage, profile_id)

    updates = {"luoyun_force_refresh": 0}
    if bool(session.get("luoyun_batch_just_completed")):
        updates["luoyun_batch_just_completed"] = 0
        updates["luoyun_last_tree_text"] = None
        updates["luoyun_last_tree_state"] = None
    if bool(session.get("luoyun_invasion_active")):
        session_for_view = dict(session)
        session_for_view.update(updates)
        invasion_view = build_luoyun_view(payload, session=session_for_view, now=now)
        defend_ready_time = float(invasion_view.get("defend_ready_time") or 0)
        if defend_ready_time > now:
            updates["luoyun_next_check_time"] = defend_ready_time
            updates["luoyun_next_check_source"] = (
                f"古剑门入侵中，协同守山冷却至 {format_timestamp(defend_ready_time)}"
            )
        else:
            updates["luoyun_next_check_time"] = 0
            updates["luoyun_next_check_source"] = "古剑门入侵中，可执行协同守山"
        updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
        if _has_any_auto_keys(session):
            updates["next_check_source"] = "已同步宗门缓存状态"
        update_session(db, chat_id, profile_id=profile_id, **updates)
        return get_session(db, chat_id, profile_id=profile_id), build_luoyun_view(
            payload,
            session=get_session(db, chat_id, profile_id=profile_id),
            now=now,
        )

    session_for_view = dict(session)
    session_for_view.update(updates)
    if force_refresh and not has_active_luoyun_batch(session_for_view):
        refreshed_state = _load_luoyun_state(session_for_view)
        if refreshed_state:
            refreshed_state = dict(refreshed_state)
            refreshed_state.pop("next_ready_time", None)
            refreshed_state.pop("remaining_seconds", None)
            session_for_view["luoyun_last_tree_state"] = json.dumps(
                refreshed_state, ensure_ascii=False
            )
    view = build_luoyun_view(payload, session=session_for_view, now=now)

    if session.get("auto_luoyun_enabled"):
        if _luoyun_action_still_syncing(session, now, command_text=".灵树状态"):
            pending_time = _luoyun_sync_retry_time(session, now)
            updates["luoyun_next_check_time"] = pending_time
            updates["luoyun_next_check_source"] = "已发送 .灵树状态，等待机器人回包"
        elif not has_active_luoyun_batch(session_for_view):
            last_passive = float(session.get("luoyun_last_passive_tree_check") or 0)
            if last_passive <= 0:
                updates["luoyun_last_passive_tree_check"] = now
                last_passive = now
            if now - last_passive > 3600:
                updates["luoyun_last_passive_tree_check"] = now
                updates["luoyun_next_check_time"] = 0
                updates["luoyun_next_check_source"] = "被动监听超时1小时，主动发送灵树状态"
            else:
                simulated_session = dict(session_for_view)
                simulated_session.update(updates)
                auto_commands = build_luoyun_auto_commands(simulated_session)
                irrigation_ready = float(view.get("irrigation_ready_time") or 0)
                if irrigation_ready > now:
                    auto_commands = [
                        cmd for cmd in auto_commands if cmd != ".灵树灌溉"
                    ]
                if auto_commands:
                    updates["luoyun_pending_commands"] = _save_luoyun_pending_commands(auto_commands)
                    updates["luoyun_pending_index"] = 0
                    updates["luoyun_pending_msg_id"] = 0
                    updates["luoyun_pending_retry"] = 0
                    updates["luoyun_force_refresh"] = 0
                    updates["luoyun_next_check_time"] = 0
                    updates["luoyun_next_check_source"] = (
                        f"已根据灵树状态生成 {len(auto_commands)} 条落云宗命令"
                    )
                elif view.get("next_ready_time") and float(view.get("next_ready_time") or 0) > now:
                    updates["luoyun_next_check_time"] = float(view.get("next_ready_time") or 0)
                    updates["luoyun_next_check_source"] = (
                        f"灵树状态等待至 {format_timestamp(view.get('next_ready_time') or 0)}"
                    )
                    updates["luoyun_force_refresh"] = 0
                elif view.get("irrigation_ready_time") and float(view.get("irrigation_ready_time") or 0) > now:
                    updates["luoyun_next_check_time"] = float(view.get("irrigation_ready_time") or 0)
                    updates["luoyun_next_check_source"] = (
                        f"灵树灌溉冷却至 {format_timestamp(view.get('irrigation_ready_time') or 0)}"
                    )
                    updates["luoyun_force_refresh"] = 0
                else:
                    updates["luoyun_next_check_time"] = 0
                    updates["luoyun_next_check_source"] = "可发送 .灵树状态 检查当前灵树状态"

    updates["next_check_time"] = _recompute_overall_next_check(session, updates, now)
    if _has_any_auto_keys(session_for_view):
        updates["next_check_source"] = "已同步宗门缓存状态"
    update_session(db, chat_id, profile_id=profile_id, **updates)
    return get_session(db, chat_id, profile_id=profile_id), build_luoyun_view(
        payload,
        session=get_session(db, chat_id, profile_id=profile_id),
        now=now,
    )


async def maybe_run_luoyun_batch(client, db, session, *, storage=None, profile_id=None):
    commands = _load_luoyun_pending_commands(session)
    if not commands:
        return False
    current_index = int(session.get("luoyun_pending_index") or 0)
    pending_msg_id = int(session.get("luoyun_pending_msg_id") or 0)
    chat_id = int(session.get("chat_id") or 0)
    if pending_msg_id:
        now = time.time()
        sent_at = float(session.get("luoyun_next_check_time") or 0) - LUOYUN_COMMAND_REFRESH_SECONDS
        if sent_at <= 0 or now - sent_at < LUOYUN_BATCH_TIMEOUT_SECONDS:
            return True
        retry_count = int(session.get("luoyun_pending_retry") or 0)
        if retry_count < LUOYUN_BATCH_MAX_RETRIES:
            update_session(
                db,
                chat_id,
                profile_id=session.get("profile_id"),
                luoyun_pending_retry=retry_count + 1,
                luoyun_pending_msg_id=0,
                luoyun_next_check_time=0,
                luoyun_next_check_source=f"灵树指令超时未回复，准备重试 {commands[current_index] if current_index < len(commands) else ''}",
                next_check_time=0,
                next_check_source="灵树指令超时未回复，准备重试",
            )
        else:
            clear_luoyun_batch(
                db,
                chat_id,
                summary="灵树指令重试后仍未收到回复，已停止本轮自动执行",
                profile_id=session.get("profile_id"),
                next_check_time=time.time() + LUOYUN_IRRIGATION_COOLDOWN_SECONDS,
            )
            update_session(
                db,
                chat_id,
                profile_id=session.get("profile_id"),
        auto_luoyun_enabled=0,
        auto_companion_greet_enabled=0,
        companion_greet_next_check_time=0,
                luoyun_pending_retry=0,
                next_check_source="灵树指令连续超时，已停止落云宗自动",
                next_check_time=time.time() + LUOYUN_IRRIGATION_COOLDOWN_SECONDS,
            )
        return True
    if current_index >= len(commands):
        clear_luoyun_batch(
            db,
            chat_id,
            summary="落云宗批次已完成，等待重新检查灵树状态",
            profile_id=session.get("profile_id"),
            next_check_time=0,
        )
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            luoyun_batch_just_completed=1,
            luoyun_force_refresh=1,
            luoyun_last_tree_text=None,
            luoyun_last_tree_state=None,
            next_check_time=0,
            next_check_source="落云宗批次已完成，准备重新获取灵树状态",
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
            luoyun_pending_msg_id=int(sent_message_id),
            luoyun_next_check_time=time.time() + LUOYUN_COMMAND_REFRESH_SECONDS,
            luoyun_next_check_source=f"落云宗批次等待回复: {command_text}",
            next_check_time=time.time() + LUOYUN_COMMAND_REFRESH_SECONDS,
            next_check_source=f"落云宗批次等待回复: {command_text}",
        )
    return True


async def maybe_run_huangfeng_batch(
    client, db, session, *, storage=None, profile_id=None
):
    commands = _load_huangfeng_pending_commands(session)
    if not commands:
        return False
    current_index = int(session.get("huangfeng_pending_index") or 0)
    pending_msg_id = int(session.get("huangfeng_pending_msg_id") or 0)
    chat_id = int(session.get("chat_id") or 0)
    if pending_msg_id:
        now = time.time()
        sent_at = (
            float(session.get("huangfeng_next_check_time") or 0)
            - LINGXIAO_COMMAND_REFRESH_SECONDS
        )
        if sent_at <= 0 or now - sent_at < HUANGFENG_BATCH_TIMEOUT_SECONDS:
            return True
        retry_count = int(session.get("huangfeng_pending_retry") or 0)
        if retry_count < HUANGFENG_BATCH_MAX_RETRIES:
            update_session(
                db,
                chat_id,
                profile_id=session.get("profile_id"),
                huangfeng_pending_retry=retry_count + 1,
                huangfeng_pending_msg_id=0,
                huangfeng_next_check_time=0,
                huangfeng_next_check_source=(
                    f"黄枫谷指令超时未回复（第{retry_count + 1}次重试），重新发送{commands[current_index] if current_index < len(commands) else ''}"
                ),
                next_check_time=0,
                next_check_source=(
                    f"黄枫谷指令超时未回复（第{retry_count + 1}次重试）"
                ),
            )
        else:
            logger.warning(
                "Huangfeng batch command timed out %d times for chat %d, stopping auto",
                HUANGFENG_BATCH_MAX_RETRIES,
                chat_id,
            )
            clear_huangfeng_batch(
                db,
                chat_id,
                summary=(
                    f"黄枫谷指令连续{retry_count}次超时未回复（各等待30分钟），已停止自动化"
                ),
                profile_id=session.get("profile_id"),
            )
            update_session(
                db,
                chat_id,
                profile_id=session.get("profile_id"),
                auto_huangfeng_enabled=0,
                huangfeng_pending_retry=0,
                huangfeng_next_check_source=(
                    f"黄枫谷指令连续{retry_count}次超时未回复，已停止自动化"
                ),
                next_check_time=(
                    time.time() + HUANGFENG_AUTO_CHECK_SECONDS
                ),
                next_check_source=(
                    f"黄枫谷指令连续{retry_count}次超时未回复，已停止自动化"
                ),
            )
        return True
    if current_index >= len(commands):
        clear_huangfeng_batch(
            db,
            chat_id,
            summary="黄枫谷批次已完成，等待 30 分钟后复查",
            profile_id=session.get("profile_id"),
            next_check_time=time.time() + HUANGFENG_AUTO_CHECK_SECONDS,
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
            huangfeng_pending_msg_id=int(sent_message_id),
            huangfeng_next_check_time=time.time() + LINGXIAO_COMMAND_REFRESH_SECONDS,
            huangfeng_next_check_source=f"黄枫谷批次等待回复: {command_text}",
            next_check_time=time.time() + LINGXIAO_COMMAND_REFRESH_SECONDS,
            next_check_source=f"黄枫谷批次等待回复: {command_text}",
        )
    return True


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
        if _parse_date_key(session.get("sect_checkin_pending_date")) == current_date_key(
            now
        ) and _common_action_still_syncing(session, now, command_text=".宗门点卯"):
            next_time = max(next_time, now + 3)
        if not next_time or now >= next_time:
            return {
                "command": ".宗门点卯",
                "next_field": "sect_checkin_next_check_time",
                "source_field": "sect_checkin_next_check_source",
                "pending_source": "已发送 .宗门点卯，等待机器人回复",
            }
    if session.get("auto_sect_teach_enabled"):
        next_time = float(session.get("sect_teach_next_check_time") or 0)
        if _parse_date_key(session.get("sect_teach_pending_date")) == current_date_key(
            now
        ) and _common_action_still_syncing(session, now, command_text=".宗门传功"):
            next_time = max(next_time, now + 3)
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
    if session.get("auto_luoyun_enabled"):
        if bool(session.get("luoyun_invasion_active")):
            next_time = float(session.get("luoyun_next_check_time") or 0)
            if not next_time or now >= next_time:
                return {
                    "command": ".协同守山",
                    "next_field": "luoyun_next_check_time",
                    "source_field": "luoyun_next_check_source",
                    "pending_source": "已发送 .协同守山，等待机器人回包",
                    "pending_delay_seconds": LUOYUN_COMMAND_REFRESH_SECONDS,
                }
            return None
        if has_active_luoyun_batch(session):
            return None
        next_time = float(session.get("luoyun_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".灵树状态",
                "next_field": "luoyun_next_check_time",
                "source_field": "luoyun_next_check_source",
                "pending_source": "已发送 .灵树状态，等待机器人回包",
                "pending_delay_seconds": LUOYUN_COMMAND_REFRESH_SECONDS,
            }
            return None
    if session.get("auto_companion_greet_enabled"):
        next_time = float(session.get("companion_greet_next_check_time") or 0)
        if not next_time or now >= next_time:
            return {
                "command": ".每日问安",
                "next_field": "companion_greet_next_check_time",
                "source_field": "companion_greet_next_check_source",
                "pending_source": "已发送 .每日问安，等待回复",
                "pending_delay_seconds": LUOYUN_COMMAND_REFRESH_SECONDS,
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
    today_key = current_date_key(now)
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
    elif command_text == ".宗门点卯":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            sect_checkin_pending_date=today_key,
            sect_checkin_next_check_time=now + LINGXIAO_COMMAND_REFRESH_SECONDS,
            sect_checkin_next_check_source="已发送 .宗门点卯，等待确认或缓存刷新",
        )
    elif command_text == ".宗门传功":
        current_teach_count = 0
        if _parse_date_key(session.get("last_teach_date")) == today_key:
            current_teach_count = max(
                current_teach_count, _parse_int(session.get("last_teach_count"), 0)
            )
        if _parse_date_key(session.get("sect_teach_pending_date")) == today_key:
            current_teach_count = max(
                current_teach_count,
                _parse_int(session.get("sect_teach_pending_target_count"), 0),
            )
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            sect_teach_pending_date=today_key,
            sect_teach_pending_target_count=min(
                current_teach_count + 1, SECT_DAILY_TEACH_LIMIT
            ),
            sect_teach_next_check_time=now + SECT_AUTO_TEACH_REPLY_RECHECK_SECONDS,
            sect_teach_next_check_source="已发送 .宗门传功，等待确认或缓存刷新",
        )
    elif command_text == ".灵树状态":
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            luoyun_force_refresh=0,
            luoyun_next_check_time=now + LUOYUN_COMMAND_REFRESH_SECONDS,
            luoyun_next_check_source="已发送 .灵树状态，等待机器人回包",
        )
    elif command_text in {".灵树灌溉", ".采摘灵果"}:
        update_session(
            db,
            chat_id,
            profile_id=session.get("profile_id"),
            luoyun_next_check_time=now + LUOYUN_COMMAND_REFRESH_SECONDS,
            luoyun_next_check_source=f"已发送 {command_text}，等待机器人回包",
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
    sender_id = getattr(sender, "id", None)
    if sender_id not in SECT_BOT_IDS:
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
    reply_text = ""
    if getattr(event, "is_reply", False):
        try:
            reply_message = await event.get_reply_message()
        except Exception:
            reply_message = None
        reply_text = (getattr(reply_message, "raw_text", "") or "").strip()
    update_fields = {
        "last_event": parsed["event"],
        "last_summary": parsed["summary"],
        "last_bot_text": raw_text[:1000],
        "last_bot_msg_id": event.id,
        "next_check_source": parsed["summary"],
    }
    if reply_text == ".小药园" or HUANGFENG_PLOT_PATTERN.search(raw_text):
        garden_state = parse_huangfeng_garden_text(raw_text)
        if garden_state.get("plots"):
            update_fields["huangfeng_last_garden_text"] = raw_text[:4000]
            update_fields["huangfeng_last_garden_state"] = json.dumps(
                garden_state, ensure_ascii=False
            )
    luoyun_pending_commands = _load_luoyun_pending_commands(session)
    luoyun_pending_msg_id = int(session.get("luoyun_pending_msg_id") or 0)
    luoyun_pending_index = int(session.get("luoyun_pending_index") or 0)
    if parsed.get("event") == "luoyun_tree_status":
        luoyun_state = parsed.get("luoyun_tree_state") or {}
        luoyun_state["source"] = "status_text"
        existing_luoyun_state = _load_luoyun_state(session)
        cached_last_irrigation_time = float(existing_luoyun_state.get("last_irrigation_time") or 0)
        if cached_last_irrigation_time > 0:
            luoyun_state["last_irrigation_time"] = cached_last_irrigation_time
        update_fields["luoyun_last_tree_text"] = raw_text[:4000]
        update_fields["luoyun_last_tree_state"] = json.dumps(luoyun_state, ensure_ascii=False)
        update_fields["luoyun_force_refresh"] = 0
        if bool(luoyun_state.get("invasion_detected")):
            frozen_ready_time = float(session.get("luoyun_frozen_irrigation_ready_time") or 0)
            if frozen_ready_time <= 0:
                existing_ready = float(existing_luoyun_state.get("next_ready_time") or 0)
                frozen_ready_time = existing_ready
            update_fields["luoyun_invasion_active"] = 1
            update_fields["luoyun_frozen_irrigation_ready_time"] = frozen_ready_time
            update_fields["luoyun_pending_commands"] = None
            update_fields["luoyun_pending_index"] = 0
            update_fields["luoyun_pending_msg_id"] = 0
            update_fields["luoyun_pending_retry"] = 0
            update_fields["luoyun_next_check_time"] = 0
            update_fields["luoyun_next_check_source"] = "检测到古剑门入侵，切换为协同守山"
            update_fields["next_check_time"] = 0
            update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
        if session.get("auto_luoyun_enabled"):
            own_last_cmd = int(session.get("last_command_msg_id") or 0)
            is_own_reply = bool(own_last_cmd and reply_to_msg_id == own_last_cmd)
            if not is_own_reply and ".灵树状态" in reply_text:
                update_fields["luoyun_last_passive_tree_check"] = now
                if luoyun_state.get("invasion_detected") and not session.get("luoyun_invasion_active"):
                    update_fields["luoyun_invasion_active"] = 1
                    update_fields["luoyun_pending_commands"] = None
                    update_fields["luoyun_pending_index"] = 0
                    update_fields["luoyun_pending_msg_id"] = 0
                    update_fields["luoyun_pending_retry"] = 0
                    update_fields["luoyun_next_check_time"] = 0
                    update_fields["luoyun_next_check_source"] = "被动监听到古剑门入侵，切换为协同守山"
                    update_fields["next_check_time"] = 0
                    update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
        syncing_tree_status = _luoyun_action_still_syncing(
            session, now, command_text=".灵树状态"
        )
        expected_status_reply = reply_to_msg_id == int(session.get("last_command_msg_id") or 0)
        if session.get("auto_luoyun_enabled") and (expected_status_reply or syncing_tree_status):
            refreshed_session = dict(session)
            refreshed_session.update(update_fields)
            auto_commands = build_luoyun_auto_commands(refreshed_session)
            if auto_commands:
                update_fields["luoyun_pending_commands"] = _save_luoyun_pending_commands(auto_commands)
                update_fields["luoyun_pending_index"] = 0
                update_fields["luoyun_pending_msg_id"] = 0
                update_fields["luoyun_pending_retry"] = 0
                update_fields["luoyun_next_check_time"] = 0
                update_fields["luoyun_next_check_source"] = (
                    f"已根据灵树状态生成 {len(auto_commands)} 条落云宗命令"
                )
                update_fields["next_check_time"] = 0
                update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
            else:
                update_fields["luoyun_force_refresh"] = 1
                update_fields["luoyun_next_check_time"] = 0
                update_fields["luoyun_next_check_source"] = (
                    "灵树状态已更新，准备刷新天机阁冷却后进入下一轮等待"
                )
                update_fields["next_check_time"] = 0
                update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
        elif syncing_tree_status:
            update_fields["luoyun_force_refresh"] = 1
            update_fields["luoyun_pending_msg_id"] = 0
            update_fields["luoyun_pending_retry"] = 0
            update_fields["luoyun_next_check_time"] = 0
            update_fields["luoyun_next_check_source"] = "已收到灵树状态回包，准备按最新状态重新判断"
            update_fields["next_check_time"] = 0
            update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
    if parsed.get("event") == "luoyun_invasion_clear":
        update_fields["luoyun_invasion_active"] = 0
        update_fields["luoyun_pending_commands"] = None
        update_fields["luoyun_pending_index"] = 0
        update_fields["luoyun_pending_msg_id"] = 0
        update_fields["luoyun_pending_retry"] = 0
        update_fields["luoyun_force_refresh"] = 1
        update_fields["luoyun_last_tree_text"] = None
        update_fields["luoyun_last_tree_state"] = None
        update_fields["luoyun_next_check_time"] = 0
        update_fields["luoyun_next_check_source"] = "外敌已退，准备重新获取灵树状态"
        update_fields["next_check_time"] = 0
        update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
    if (
        luoyun_pending_commands
        and luoyun_pending_msg_id
        and reply_to_msg_id == luoyun_pending_msg_id
        and parsed.get("summary") == "灵树灌溉冷却中"
    ):
        update_fields["luoyun_pending_commands"] = None
        update_fields["luoyun_pending_index"] = 0
        update_fields["luoyun_pending_msg_id"] = 0
        update_fields["luoyun_pending_retry"] = 0
        update_fields["luoyun_force_refresh"] = 1
        update_fields["luoyun_next_check_time"] = 0
        update_fields["luoyun_next_check_source"] = "灵树尚在灌溉冷却中，准备刷新天机阁后重新判断"
        update_fields["next_check_time"] = 0
        update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
    elif (
        luoyun_pending_commands
        and luoyun_pending_msg_id
        and reply_to_msg_id == luoyun_pending_msg_id
        and parsed.get("event") in {"luoyun_tree_water", "luoyun_tree_harvest", "luoyun_tree_status", "luoyun_guard", "luoyun_invasion_clear"}
    ):
        next_index = luoyun_pending_index + 1
        if next_index >= len(luoyun_pending_commands):
            update_fields["luoyun_pending_commands"] = None
            update_fields["luoyun_pending_index"] = 0
            update_fields["luoyun_pending_msg_id"] = 0
            update_fields["luoyun_pending_retry"] = 0
            if bool(session.get("luoyun_invasion_active")) or parsed.get("event") == "luoyun_guard":
                update_fields["luoyun_force_refresh"] = 1
                update_fields["luoyun_next_check_time"] = 0
                update_fields["luoyun_next_check_source"] = "守山完成，等待根据最新守山冷却重新判断"
            else:
                update_fields["luoyun_batch_just_completed"] = 1
                update_fields["luoyun_force_refresh"] = 1
                update_fields["luoyun_last_tree_text"] = None
                update_fields["luoyun_last_tree_state"] = None
                update_fields["luoyun_next_check_time"] = 0
                update_fields["luoyun_next_check_source"] = "落云宗批次已完成，准备重新获取灵树状态"
            update_fields["last_summary"] = update_fields["luoyun_next_check_source"]
            update_fields["next_check_time"] = update_fields["luoyun_next_check_time"] or 0
            update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
        else:
            update_fields["luoyun_pending_index"] = next_index
            update_fields["luoyun_pending_msg_id"] = 0
            update_fields["luoyun_pending_retry"] = 0
            update_fields["luoyun_next_check_time"] = 0
            update_fields["luoyun_next_check_source"] = (
                f"落云宗批次已完成 {next_index}/{len(luoyun_pending_commands)}，准备下一条"
            )
            update_fields["next_check_time"] = 0
            update_fields["next_check_source"] = update_fields["luoyun_next_check_source"]
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
    huangfeng_commands = _load_huangfeng_pending_commands(session)
    huangfeng_pending_msg_id = int(session.get("huangfeng_pending_msg_id") or 0)
    huangfeng_index = int(session.get("huangfeng_pending_index") or 0)
    if (
        huangfeng_commands
        and huangfeng_pending_msg_id
        and reply_to_msg_id == huangfeng_pending_msg_id
    ):
        current_huangfeng_command = (
            huangfeng_commands[huangfeng_index]
            if 0 <= huangfeng_index < len(huangfeng_commands)
            else ""
        )
        if current_huangfeng_command.startswith(
            ".播种 "
        ) and _is_huangfeng_seed_shortage(raw_text):
            seed_name = str(session.get("huangfeng_seed_name") or "").strip()
            if session.get("auto_huangfeng_exchange_enabled") and seed_name:
                exchange_command = _build_huangfeng_exchange_command(seed_name)
                remaining_commands = [
                    command
                    for command in [
                        exchange_command,
                        current_huangfeng_command,
                        *huangfeng_commands[huangfeng_index + 1 :],
                    ]
                    if command
                ]
                update_fields["huangfeng_pending_commands"] = (
                    _save_huangfeng_pending_commands(remaining_commands)
                )
                update_fields["huangfeng_pending_index"] = 0
                update_fields["huangfeng_pending_msg_id"] = 0
                update_fields["huangfeng_next_check_time"] = 0
                update_fields["huangfeng_next_check_source"] = (
                    f"{seed_name} 不足，准备先兑换后重试播种"
                )
                update_fields["next_check_time"] = 0
                update_fields["next_check_source"] = update_fields[
                    "huangfeng_next_check_source"
                ]
            else:
                update_fields["auto_huangfeng_enabled"] = 0
                update_fields["huangfeng_pending_commands"] = None
                update_fields["huangfeng_pending_index"] = 0
                update_fields["huangfeng_pending_msg_id"] = 0
                update_fields["huangfeng_next_check_time"] = 0
                update_fields["huangfeng_next_check_source"] = (
                    "播种缺少种子，已停止黄枫谷自动化"
                )
                update_fields["next_check_source"] = update_fields[
                    "huangfeng_next_check_source"
                ]
        else:
            next_index = huangfeng_index + 1
            if next_index >= len(huangfeng_commands):
                update_fields["huangfeng_pending_commands"] = None
                update_fields["huangfeng_pending_index"] = 0
                update_fields["huangfeng_pending_msg_id"] = 0
                update_fields["huangfeng_pending_retry"] = 0
                update_fields["huangfeng_payload_refresh_retry"] = 0
                update_fields["huangfeng_last_garden_state"] = None
                update_fields["huangfeng_batch_just_completed"] = 1
                update_fields["huangfeng_next_check_time"] = 0
                update_fields["huangfeng_next_check_source"] = (
                    "黄枫谷批次已完成，即将刷新药园状态"
                )
                update_fields["last_summary"] = update_fields[
                    "huangfeng_next_check_source"
                ]
            else:
                update_fields["huangfeng_pending_index"] = next_index
                update_fields["huangfeng_pending_msg_id"] = 0
                update_fields["huangfeng_pending_retry"] = 0
                update_fields["huangfeng_next_check_time"] = 0
                update_fields["huangfeng_next_check_source"] = (
                    f"黄枫谷批次已完成 {next_index}/{len(huangfeng_commands)}，准备下一条"
                )
                update_fields["next_check_time"] = 0
                update_fields["next_check_source"] = update_fields[
                    "huangfeng_next_check_source"
                ]
    if session.get("auto_huangfeng_enabled") and reply_text == ".小药园":
        refreshed_session = dict(session)
        refreshed_session.update(update_fields)
        auto_commands = build_huangfeng_auto_commands(refreshed_session)
        if auto_commands:
            update_fields["huangfeng_pending_commands"] = (
                _save_huangfeng_pending_commands(auto_commands)
            )
            update_fields["huangfeng_pending_index"] = 0
            update_fields["huangfeng_pending_msg_id"] = 0
            update_fields["huangfeng_pending_retry"] = 0
            update_fields["huangfeng_next_check_time"] = 0
            update_fields["huangfeng_next_check_source"] = (
                f"已根据药园状态生成 {len(auto_commands)} 条黄枫谷命令"
            )
            update_fields["next_check_time"] = 0
            update_fields["next_check_source"] = update_fields[
                "huangfeng_next_check_source"
            ]
        else:
            update_fields["huangfeng_pending_commands"] = None
            update_fields["huangfeng_pending_index"] = 0
            update_fields["huangfeng_pending_msg_id"] = 0
            update_fields["huangfeng_next_check_time"] = (
                now + HUANGFENG_AUTO_CHECK_SECONDS
            )
            update_fields["huangfeng_next_check_source"] = "药园状态正常，30 分钟后复查"
    if parsed["event"] == "luoyun_tree_status":
        update_fields["last_panel_time"] = now
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
        update_fields["sect_checkin_pending_date"] = None
        update_fields["sect_common_force_refresh"] = 1
        if session.get("auto_sect_checkin_enabled"):
            update_fields["sect_checkin_next_check_time"] = _next_daily_random_window_timestamp(
                session, "sect_checkin", now
            )
            update_fields["sect_checkin_next_check_source"] = (
                f"今日已点卯，等待次日 {SECT_AUTO_WINDOW_START_TIME}-{SECT_AUTO_WINDOW_END_TIME} 随机执行"
            )
    elif parsed["event"] == "sect_teach":
        teach_count = int(teach_progress[0]) if teach_progress else 0
        update_fields["last_teach_date"] = current_date_key(now)
        update_fields["last_teach_count"] = teach_count
        update_fields["sect_teach_pending_date"] = None
        update_fields["sect_teach_pending_target_count"] = 0
        if session.get("auto_sect_teach_enabled"):
            if teach_count >= SECT_DAILY_TEACH_LIMIT:
                update_fields["sect_common_force_refresh"] = 1
                update_fields["sect_teach_next_check_time"] = _next_daily_random_window_timestamp(
                    session, "sect_teach", now
                )
                update_fields["sect_teach_next_check_source"] = (
                    f"今日已传功 {teach_count}/{SECT_DAILY_TEACH_LIMIT}，等待次日 {SECT_AUTO_WINDOW_START_TIME}-{SECT_AUTO_WINDOW_END_TIME} 随机执行"
                )
            else:
                update_fields["sect_teach_next_check_time"] = 0
                update_fields["sect_teach_next_check_source"] = (
                    f"收到传功回复，可继续执行 ({teach_count}/{SECT_DAILY_TEACH_LIMIT})"
                )
    elif parsed["event"] in {"luoyun_tree_water", "luoyun_tree_harvest", "luoyun_guard", "luoyun_invasion_clear"}:
        update_fields["next_check_time"] = 0
        update_fields["luoyun_force_refresh"] = 1
    elif parsed["event"] not in {"sect_panel_pending", "unknown"}:
        update_fields["next_check_time"] = now + session["interval_seconds"]
    if _has_any_auto_keys(session):
        update_fields["next_check_time"] = _recompute_overall_next_check(
            session, update_fields, now
        )
    if session.get("auto_companion_greet_enabled") and str(session.get("last_action") or "").strip() == ".每日问安":
        update_fields["companion_greet_next_check_time"] = _next_daily_random_window_timestamp(
            session, "companion_greet", now
        )
        update_fields["companion_greet_next_check_source"] = (
            f"今日已问安，等待次日 {SECT_AUTO_WINDOW_START_TIME}-{COMPANION_GREET_WINDOW_END} 随机执行"
        )
    update_session(
        db,
        event.chat_id,
        profile_id=session.get("profile_id"),
        **update_fields,
    )
    return parsed


async def runner(client, storage, profile_id=None):
    while True:
        try:
            db = RuntimeDb(storage)
            now = time.time()
            for session in list_sessions(db, profile_id=profile_id):
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
                            profile_id=session_profile_id,
                        )
                        continue
                if has_active_huangfeng_batch(session):
                    try:
                        handled = await maybe_run_huangfeng_batch(
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
                            "Huangfeng batch failed in chat %s: %s",
                            session["chat_id"],
                            exc,
                        )
                        clear_huangfeng_batch(
                            db,
                            session["chat_id"],
                            summary=f"黄枫谷批次失败: {exc}",
                            profile_id=session_profile_id,
                            next_check_time=now + max(session["interval_seconds"], 60),
                        )
                        continue
                if has_active_luoyun_batch(session):
                    try:
                        handled = await maybe_run_luoyun_batch(
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
                            "Luoyun batch failed in chat %s: %s",
                            session["chat_id"],
                            exc,
                        )
                        clear_luoyun_batch(
                            db,
                            session["chat_id"],
                            summary=f"落云宗批次失败: {exc}",
                            profile_id=session_profile_id,
                            next_check_time=now + max(session["interval_seconds"], 60),
                            keep_state=False,
                        )
                        continue
                if session["next_check_time"] and now < session["next_check_time"]:
                    continue
                try:
                    payload = None
                    if session_profile_id and (
                        _active_common_auto_keys(session)
                        or _active_yinluo_auto_keys(session)
                        or _active_huangfeng_auto_keys(session)
                        or _active_luoyun_auto_keys(session)
                        or _active_lingxiao_auto_keys(session)
                    ):
                        payload = _read_cached_profile_payload(
                            storage, session_profile_id
                        )
                    if session_profile_id and payload is not None:
                        session, guard_applied = apply_sect_auto_guard(
                            storage,
                            db,
                            session,
                            profile_id=session_profile_id,
                            payload=payload,
                            now=now,
                        )
                        if guard_applied and not _has_any_auto_keys(session):
                            continue
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
                    if session_profile_id and _active_huangfeng_auto_keys(session):
                        session, _view = sync_huangfeng_state(
                            storage,
                            db,
                            session_profile_id,
                            session["chat_id"],
                            payload=payload,
                            now=now,
                        )
                        now = time.time()
                    if session_profile_id and _active_luoyun_auto_keys(session):
                        session, _view = sync_luoyun_state(
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
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Sect runner error: %s", exc)
            await asyncio.sleep(10)
