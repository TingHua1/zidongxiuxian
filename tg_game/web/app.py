import asyncio
import math
import json
import logging
import re
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

import fanren_game
import sect_game
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from tg_game.clients.asc_client import AscAuthError
from tg_game.config import get_settings
from tg_game.module_commands import MODULE_COMMANDS
from tg_game.sect_features import SECT_FEATURES
from tg_game.services import module_registry
from tg_game.services.cultivation_sync import sync_cultivation_session
from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_cultivator_lookup_candidates,
    get_effective_external_cookie,
    get_external_keepalive_poll_seconds,
    is_authorized_profile,
    is_external_account_expired,
    mark_external_account_failure,
    read_cached_external_payload,
    should_keep_external_session_fresh,
    sync_external_account,
)
from tg_game.storage import CompatDb, Storage
from tg_game.telegram.account import (
    get_authorized_account_info,
    has_authorized_session,
    logout_account,
    send_login_code,
    verify_login_code,
    verify_login_password,
)


BASE_DIR = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
APP_SESSION_COOKIE = "tg_game_app_session"
TG_LOGIN_CHALLENGE_COOKIE = "tg_game_login_challenge"
EXTERNAL_PROFILE_REFRESH_TTL_SECONDS = get_settings().external_keepalive_seconds
EXTERNAL_REFRESH_LOOP_SECONDS = get_external_keepalive_poll_seconds()


logger = logging.getLogger(__name__)
SHANGHAI_TZ = timezone(timedelta(hours=8))
COMPANION_AUTO_FEATURES = {
    "dream_seek": {
        "label": "入梦寻图",
        "command": ".入梦寻图",
        "cooldown_hours": 8,
        "payload_field": "last_dream_map_seek_time",
    },
    "divination_chain": {
        "label": "天机代卜",
        "command": ".天机代卜",
        "cooldown_hours": 12,
        "payload_field": "last_divination_chain_time",
    },
}
COMPANION_AUTO_MANUAL_DELAY_SECONDS = 1800


def _coerce_json_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _coerce_json_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _parse_iso_datetime(raw_value) -> Optional[datetime]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_datetime_display(raw_value) -> str:
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return "-"
    return parsed.astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M")


def _format_remaining_delta(end_time: Optional[datetime]) -> str:
    if not end_time:
        return "可施展"
    now = datetime.now(timezone.utc)
    remaining_seconds = int((end_time.astimezone(timezone.utc) - now).total_seconds())
    if remaining_seconds <= 0:
        return "可施展"
    total_minutes = math.ceil(remaining_seconds / 60)
    hours, minutes = divmod(total_minutes, 60)
    if hours <= 0:
        return f"{minutes}分钟"
    if minutes == 0:
        return f"{hours}小时"
    return f"{hours}小时{minutes}分钟"


def _format_cooldown_from_last(raw_value, cooldown_hours: int) -> str:
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return "可施展"
    end_time = parsed + timedelta(hours=max(int(cooldown_hours or 0), 0))
    return _format_remaining_delta(end_time)


def _cooldown_target_timestamp(raw_value, cooldown_hours: int) -> float:
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return 0.0
    end_time = parsed + timedelta(hours=max(int(cooldown_hours or 0), 0))
    return end_time.astimezone(timezone.utc).timestamp()


def _resolve_latest_companion_payload(payload: dict) -> dict:
    dongfu = _coerce_json_dict(payload.get("dongfu"))
    companion_residence = _coerce_json_dict(dongfu.get("companion_residence"))
    return companion_residence if companion_residence else {}


def _resolve_latest_companion_cooldown_target(
    companion_payload: dict,
    field_name: str,
    cooldown_hours: int,
) -> Optional[float]:
    normalized_field_name = str(field_name or "").strip()
    if not normalized_field_name or normalized_field_name not in companion_payload:
        return None
    raw_value = companion_payload.get(normalized_field_name)
    parsed = _parse_iso_datetime(raw_value)
    if not parsed:
        return None
    end_time = parsed + timedelta(hours=max(int(cooldown_hours or 0), 0))
    return end_time.astimezone(timezone.utc).timestamp()


def _format_companion_cooldown_display(target: Optional[float]) -> str:
    if target is None:
        return "接口未提供"
    now_ts = fanren_game.time.time()
    if target <= now_ts:
        return "可施展"
    return _format_remaining_delta(datetime.fromtimestamp(target, tz=timezone.utc))


def _extract_reply_field(reply_text: str, label: str) -> str:
    if not reply_text:
        return ""
    pattern = rf"-\s*{re.escape(label)}:\s*([^\n]+)"
    match = re.search(pattern, reply_text)
    return match.group(1).strip() if match else ""


def _build_companion_view(payload: dict, companion_reply_text: str = "") -> dict:
    companion_payload = _resolve_latest_companion_payload(payload)
    heart_vow = _coerce_json_dict(companion_payload.get("heart_vow"))
    fragment_bag = _coerce_json_dict(companion_payload.get("xutian_fragment_bag"))

    fragment_entries = [
        ("xutian_chart_east", "东"),
        ("xutian_chart_south", "南"),
        ("xutian_chart_west", "西"),
        ("xutian_chart_north", "北"),
    ]
    fragment_count = sum(
        1 for key, _label in fragment_entries if int(fragment_bag.get(key) or 0) > 0
    )
    fragment_detail = " / ".join(
        f"{label}{int(fragment_bag.get(key) or 0)}" for key, label in fragment_entries
    )

    reply_divination_chain = _extract_reply_field(companion_reply_text, "天机代卜链")
    reply_abyss_guard = _extract_reply_field(companion_reply_text, "坠魔谷护持")

    divination_chain_text = str(companion_payload.get("divination_chain") or "").strip()
    abyss_guard_text = str(companion_payload.get("abyss_guard") or "").strip()

    if not divination_chain_text:
        divination_chain_text = reply_divination_chain or "接口未提供"
    if not abyss_guard_text:
        abyss_guard_text = reply_abyss_guard or "接口未提供"

    current_vow_text = str(heart_vow.get("type") or "").strip() or "无"
    relation_title = "侍妾同行"
    companion_name = str(companion_payload.get("name") or "-").strip() or "-"
    status_text = "-"
    affection_value = int(companion_payload.get("affection") or 0)
    heart_demon_value = payload.get("companion_heart_demon_value")
    dream_seek_target = _resolve_latest_companion_cooldown_target(
        companion_payload,
        "last_dream_map_seek_time",
        8,
    )
    heart_tribulation_target = _resolve_latest_companion_cooldown_target(
        companion_payload,
        "last_companion_heart_tribulation_time",
        10,
    )
    divination_chain_target = _resolve_latest_companion_cooldown_target(
        companion_payload,
        "last_divination_chain_time",
        12,
    )
    now_ts = fanren_game.time.time()

    return {
        "available": bool(companion_payload),
        "relation_title": relation_title,
        "name": companion_name,
        "status": status_text,
        "affection": affection_value,
        "heart_demon_value": (
            "-" if heart_demon_value is None else str(heart_demon_value).strip() or "-"
        ),
        "current_vow": current_vow_text,
        "sworn_at_display": _format_datetime_display(heart_vow.get("sworn_at")),
        "divination_chain": divination_chain_text,
        "abyss_guard": abyss_guard_text,
        "dream_seek_display": _format_companion_cooldown_display(dream_seek_target),
        "dream_seek_cooldown_target": float(dream_seek_target or 0),
        "heart_tribulation_display": _format_companion_cooldown_display(
            heart_tribulation_target
        ),
        "heart_tribulation_cooldown_target": float(heart_tribulation_target or 0),
        "divination_chain_display": _format_companion_cooldown_display(
            divination_chain_target
        ),
        "divination_chain_cooldown_target": float(divination_chain_target or 0),
        "fragment_progress": f"{fragment_count}/4",
        "fragment_detail": fragment_detail,
        "heart_tribulation_command": ".共历心劫",
    }


def _build_companion_auto_view(raw_task: Optional[dict], feature_key: str) -> dict:
    feature = COMPANION_AUTO_FEATURES.get(feature_key) or {}
    task = raw_task or {}
    next_run_at = float(task.get("next_run_at") or 0)
    return {
        "feature_key": feature_key,
        "label": str(feature.get("label") or feature_key),
        "command": str(feature.get("command") or "").strip(),
        "enabled": bool(task) and bool(task.get("enabled")),
        "active": bool(task) and bool(task.get("enabled")),
        "next_run_at": next_run_at,
        "next_run_display": (
            _format_remaining_delta(
                datetime.fromtimestamp(next_run_at, tz=timezone.utc)
            )
            if next_run_at > 0
            else "待命"
        ),
        "status_display": (
            _format_remaining_delta(
                datetime.fromtimestamp(next_run_at, tz=timezone.utc)
            )
            if next_run_at > 0 and bool(task) and bool(task.get("enabled"))
            else ("已停止" if str(task.get("last_error") or "").strip() else "未开启")
        ),
        "last_error": str(task.get("last_error") or "").strip(),
    }


def _resolve_companion_auto_next_run_at(
    payload: dict, feature_key: str
) -> Optional[float]:
    feature = COMPANION_AUTO_FEATURES.get(feature_key) or {}
    companion_payload = _resolve_latest_companion_payload(payload)
    cooldown_hours = int(feature.get("cooldown_hours") or 0)
    payload_field = str(feature.get("payload_field") or "").strip()
    if not payload_field or cooldown_hours <= 0:
        return None
    return _resolve_latest_companion_cooldown_target(
        companion_payload,
        payload_field,
        cooldown_hours,
    )


OTHER_PLAY_DEFINITIONS = [
    {
        "key": "divination",
        "title": "卜筮问天",
        "command": ".卜筮问天",
        "description": "直接占一次气运与吉凶，适合日常顺手点。",
        "type": "button",
    },
    {
        "key": "pagoda",
        "title": "琉璃古塔",
        "command": ".闯塔",
        "description": "按当前战力一路闯塔，页面会额外展示 payload 里的古塔进度。",
        "type": "button",
    },
    {
        "key": "wheel",
        "title": "六道轮回盘",
        "command": ".六道轮回盘",
        "description": "先用 `.六道轮回盘` 查看下注情况，再用 `.卜卦` 按机选或自选下注。",
        "type": "button",
    },
    {
        "key": "stone",
        "title": "赌石坊",
        "command": ".赌石",
        "description": "赌石入口，历史消息里已有真实 `.赌石` 指令样例。",
        "type": "button",
    },
    {
        "key": "tianji_dice",
        "title": "天机骰",
        "template": ".押 {bet_type} {amount}",
        "description": "鬼赌坊的三骰玩法，使用 `.押 <类型> <金额>` 押大小单双、点数或豹子。",
        "type": "form",
        "fields": [
            {
                "name": "bet_type",
                "label": "押注类型",
                "type": "text",
                "placeholder": "大 / 小 / 点数7 / 豹子 / 豹子1",
            },
            {
                "name": "amount",
                "label": "灵石",
                "type": "number",
                "placeholder": "例如 100",
            },
        ],
    },
    {
        "key": "linglong_dice",
        "title": "玲珑骰",
        "template": ".对赌 {amount}",
        "description": "一对一掷骰子。历史消息样例为 `.对赌 500`，对方再用 `.应战` 接局。",
        "type": "form",
        "fields": [
            {
                "name": "amount",
                "label": "赌注灵石",
                "type": "number",
                "placeholder": "例如 500",
            }
        ],
        "extra_commands": [".应战"],
    },
    {
        "key": "mind_duel",
        "title": "神识对决",
        "template": ".神识对决 {amount}",
        "description": "21 点玩法。常见过程指令包含 `.应战`、`.凝神`、`.固元`。",
        "type": "form",
        "fields": [
            {
                "name": "amount",
                "label": "赌注灵石",
                "type": "number",
                "placeholder": "例如 500",
            }
        ],
        "extra_commands": [".应战", ".凝神", ".固元"],
    },
]


from tg_game.dungeon_defs import DUNGEON_DEFINITIONS


def _get_dungeon_definition(dungeon_key: str) -> dict:
    normalized_key = str(dungeon_key or "").strip().lower()
    for entry in DUNGEON_DEFINITIONS:
        if entry["key"] == normalized_key:
            return entry
    return DUNGEON_DEFINITIONS[0]


def _build_pagoda_view(payload: dict) -> dict:
    raw_progress = _coerce_json_dict(payload.get("pagoda_progress"))
    highest_floor = int(raw_progress.get("highest_floor") or 0)
    last_attempt_date = str(raw_progress.get("last_attempt_date") or "").strip()
    today_text = fanren_game.time.strftime(
        "%Y-%m-%d", fanren_game.time.localtime(fanren_game.time.time())
    )
    return {
        "highest_floor": highest_floor,
        "highest_floor_text": f"第 {highest_floor} 层" if highest_floor else "-",
        "is_in_pagoda": bool(raw_progress.get("is_in_pagoda")),
        "last_attempt_date": last_attempt_date,
        "attempted_today": bool(last_attempt_date[:10] == today_text),
        "failed_floor": int(payload.get("pagoda_failed_floor") or 0),
        "resets_today": int(payload.get("pagoda_resets_today") or 0),
        "claimed_floors": _coerce_json_list(payload.get("pagoda_claimed_floors")),
    }


def _collect_display_names(value, game_items_dict: Optional[dict] = None) -> list[str]:
    names = []
    seen = set()
    for raw_name in _payload_name_list(value):
        name = str(raw_name or "").strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    for entry in _payload_named_entries(value, game_items_dict or {}):
        name = str(entry.get("name") or "").strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    return names


SCENERY_CODE_NAME_MAP = {
    "scenery_001": "一柄青竹蜂云剑的剑影",
    "scenery_002": "嗜血妖蝠的头骨",
    "scenery_003": "天道金榜的拓印",
    "scenery_004": "风希的一缕残念",
    "scenery_005": "琉璃塔顶的刻痕",
    "scenery_006": "异界商人的信物",
    "scenery_007": "伏诛妖兽的精魄",
    "scenery_008": "虚天殿的残垣",
    "scenery_009": "通天仙门",
    "scenery_010": "坠魔谷封魔碑",
}


def _resolve_scenery_display_name(
    raw_value, game_items_dict: Optional[dict] = None
) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    normalized = text.lower()
    mapped = SCENERY_CODE_NAME_MAP.get(normalized)
    if mapped:
        return mapped
    return _resolve_payload_display_name(text, game_items_dict or {})


def _build_scenery_entries(value, game_items_dict: Optional[dict] = None) -> list[dict]:
    entries = []
    seen = set()
    for item in _coerce_json_list(value):
        if isinstance(item, dict):
            raw_id = str(
                item.get("item_id")
                or item.get("id")
                or item.get("name")
                or item.get("item_name")
                or ""
            ).strip()
            name = str(item.get("name") or item.get("item_name") or "").strip()
            if raw_id and not name:
                name = _resolve_scenery_display_name(raw_id, game_items_dict)
            elif name:
                name = _resolve_scenery_display_name(name, game_items_dict)
            if name and name not in seen:
                seen.add(name)
                entries.append({"id": raw_id or name, "name": name})
            continue

        raw_id = str(item or "").strip()
        name = _resolve_scenery_display_name(raw_id, game_items_dict)
        if name and name not in seen:
            seen.add(name)
            entries.append({"id": raw_id or name, "name": name})
    return entries


def _build_dongfu_view(payload: dict, game_items_dict: Optional[dict] = None) -> dict:
    dongfu = _coerce_json_dict(payload.get("dongfu"))
    inventory = _coerce_json_dict(payload.get("inventory"))
    storage_bag_options = [
        name
        for name in [
            *_collect_display_names(inventory.get("items"), game_items_dict),
            *[
                name
                for name in _collect_display_names(
                    inventory.get("materials"), game_items_dict
                )
                if name != "灵石"
            ],
        ]
        if name
    ]
    unlocked_scenery_entries = _build_scenery_entries(
        dongfu.get("unlocked_scenery"), game_items_dict
    )
    scenery_slot_entries = _build_scenery_entries(
        dongfu.get("scenery_slots"), game_items_dict
    )
    scenery_options = [
        entry.get("name")
        for entry in [*unlocked_scenery_entries, *scenery_slot_entries]
        if entry.get("name")
    ]
    return {
        "raw": dongfu,
        "lingmai_level": int(dongfu.get("lingmai_level") or 0),
        "jingshi_level": int(dongfu.get("jingshi_level") or 0),
        "danfang_level": int(dongfu.get("danfang_level") or 0),
        "qishi_level": int(dongfu.get("qishi_level") or 0),
        "shouyuan_level": int(dongfu.get("shouyuan_level") or 0),
        "dazhen_level": int(dongfu.get("dazhen_level") or 0),
        "dazhen_active": bool(int(dongfu.get("dazhen_active") or 0)),
        "dazhen_mode": str(dongfu.get("dazhen_mode") or "").strip(),
        "lingqi_pool": round(float(dongfu.get("lingqi_pool") or 0), 2),
        "pavilion_slots": _build_dongfu_pavilion_slots_view(
            dongfu.get("pavilion_slots"), game_items_dict
        ),
        "scenery_slots": scenery_slot_entries,
        "unlocked_scenery": unlocked_scenery_entries,
        "storage_bag_options": sorted(set(storage_bag_options)),
        "scenery_options": scenery_options,
        "messages": _coerce_json_list(dongfu.get("messages")),
        "last_update_time": str(dongfu.get("last_update_time") or "").strip(),
    }


def _build_dongfu_pavilion_slots_view(
    raw_value, game_items_dict: Optional[dict] = None
) -> dict[str, str]:
    raw_slots = _coerce_json_dict(raw_value)
    if not raw_slots:
        return {}

    def sort_key(item) -> tuple[int, str]:
        slot_key = str(item[0] or "").strip()
        return (0, f"{int(slot_key):09d}") if slot_key.isdigit() else (1, slot_key)

    slots = {}
    for slot_key, slot_value in sorted(raw_slots.items(), key=sort_key):
        normalized_slot = str(slot_key or "").strip() or "?"
        slot_label = (
            f"{normalized_slot}号位" if normalized_slot.isdigit() else normalized_slot
        )
        slot_dict = _coerce_json_dict(slot_value)
        item_payload = _coerce_json_dict(slot_dict.get("item_json")) or slot_dict
        item_id = str(
            item_payload.get("item_id") or slot_dict.get("item_id") or ""
        ).strip()
        item_name = str(
            item_payload.get("name")
            or slot_dict.get("name")
            or _resolve_payload_display_name(item_id, game_items_dict or {})
            or ""
        ).strip()
        quantity = int(item_payload.get("quantity") or slot_dict.get("quantity") or 0)
        if item_name and quantity > 1:
            item_name = f"{item_name}*{quantity}"
        slots[slot_label] = item_name or item_id or "空"
    return slots


def _build_estate_reply_messages(
    storage: Storage,
    profile_id: int,
    chat_id: Optional[int],
    thread_id: Optional[int] = None,
    sender_id: Optional[int] = None,
    sender_username: str = "",
    fallback_messages: Optional[list] = None,
) -> list[dict]:
    fallback = []
    for message in fallback_messages or []:
        text = str(
            message if not isinstance(message, dict) else message.get("text") or ""
        ).strip()
        if text:
            fallback.append(
                {
                    "command_text": "洞府缓存",
                    "text": text,
                    "created_at": 0,
                    "created_at_display": "-",
                }
            )
    return fallback[:3]


def _stringify_payload_stat_value(value) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip() or "-"
    if isinstance(value, list):
        return (
            "、".join(str(item).strip() for item in value if str(item).strip()) or "-"
        )
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value or "").strip() or "-"


def _payload_stat_label(key: str) -> str:
    normalized = str(key or "").strip()
    labels = {
        "total_plays": "总局数",
        "wins": "胜场",
        "losses": "负场",
        "draws": "平局",
        "total_won": "累计赢取",
        "total_lost": "累计亏损",
        "win_streak": "连胜",
        "loss_streak": "连败",
        "today_loss": "今日亏损",
        "today_lost": "今日亏损",
        "daily_loss": "今日亏损",
        "loss_today": "今日亏损",
        "today_profit": "今日盈利",
        "daily_profit": "今日盈利",
        "guard_limit": "道心守护上限",
        "daily_loss_limit": "道心守护上限",
        "loss_limit": "道心守护上限",
        "protection_limit": "道心守护上限",
        "guard_used": "道心守护已亏损",
        "loss_used": "道心守护已亏损",
        "selected_dice": "指定骰子",
        "豹子次数": "豹子次数",
    }
    if normalized in labels:
        return labels[normalized]
    return normalized.replace("_", " ").strip() or "-"


def _build_payload_stat_items(raw_stats: dict) -> list[dict]:
    items = []
    for key, value in (raw_stats or {}).items():
        if value in (None, "", [], {}):
            continue
        items.append(
            {
                "key": str(key or "").strip(),
                "label": _payload_stat_label(key),
                "value": _stringify_payload_stat_value(value),
            }
        )
    return items


def _build_payload_stat_items_with_defaults(
    raw_stats: dict, default_keys: Optional[list[str]] = None
) -> list[dict]:
    defaults = [
        str(key or "").strip() for key in (default_keys or []) if str(key or "").strip()
    ]
    if not defaults:
        return _build_payload_stat_items(raw_stats)
    items = []
    seen = set()
    for key in defaults:
        seen.add(key)
        value = raw_stats.get(key)
        items.append(
            {
                "key": key,
                "label": _payload_stat_label(key),
                "value": (
                    _stringify_payload_stat_value(value)
                    if value not in (None, "", [], {})
                    else ""
                ),
            }
        )
    for item in _build_payload_stat_items(raw_stats):
        if item["key"] in seen:
            continue
        items.append(item)
    return items


def _build_dice_state(
    raw_value, default_summary_keys: Optional[list[str]] = None
) -> dict:
    raw_stats = _coerce_json_dict(raw_value)
    total_won = int(raw_stats.get("total_won") or 0)
    total_lost = int(raw_stats.get("total_lost") or 0)
    return {
        "raw": raw_stats,
        "total_plays": int(raw_stats.get("total_plays") or 0),
        "wins": int(raw_stats.get("wins") or 0),
        "losses": int(raw_stats.get("losses") or 0),
        "total_won": total_won,
        "total_lost": total_lost,
        "net_total": total_won - total_lost,
        "summary_items": _build_payload_stat_items_with_defaults(
            raw_stats,
            default_keys=default_summary_keys,
        ),
    }


def _build_ghost_gambling_view(payload: dict) -> dict:
    last_bet_time_text = str(payload.get("last_bet_time") or "").strip()
    last_bet_time_ts = sect_game._parse_iso_timestamp(last_bet_time_text)
    return {
        "daily_loss_amount": int(payload.get("daily_loss_amount") or 0),
        "last_bet_date": str(payload.get("last_bet_date") or "").strip(),
        "last_bet_time": last_bet_time_text,
        "last_bet_time_ts": last_bet_time_ts,
        "last_bet_time_display": fanren_game.format_timestamp(last_bet_time_ts),
    }


def _build_divination_view(payload: dict) -> dict:
    last_divination_text = str(payload.get("last_divination_date") or "").strip()
    last_divination_ts = sect_game._parse_iso_timestamp(last_divination_text)
    last_divination_day = ""
    if last_divination_ts:
        last_divination_day = fanren_game.time.strftime(
            "%Y-%m-%d", fanren_game.time.localtime(last_divination_ts)
        )
    elif last_divination_text:
        last_divination_day = last_divination_text[:10]
    today_text = fanren_game.time.strftime(
        "%Y-%m-%d", fanren_game.time.localtime(fanren_game.time.time())
    )
    raw_today_count = max(int(payload.get("divination_count_today") or 0), 0)
    return {
        "last_divination_date": last_divination_text,
        "last_divination_ts": last_divination_ts,
        "last_divination_display": last_divination_day,
        "today_count": raw_today_count if last_divination_day == today_text else 0,
    }


def _build_character_view(payload: dict) -> dict:
    return {
        "shenshi_points": int(payload.get("shenshi_points") or 0),
    }


def _build_taiyi_view(payload: dict) -> dict:
    return {
        "taiyi_shenshi_points": int(payload.get("taiyi_shenshi_points") or 0),
    }


def _build_tianji_encounter_state(
    storage: Storage,
    profile_id: int,
    chat_id: Optional[int],
) -> dict:
    state = {
        "strategy": "未知",
        "today_count": "0/2",
        "last_encounter": "暂无",
        "records": [],
    }
    if not chat_id:
        return state

    # 1. 提取状态：仅认天机遭遇战面板/策略变更回包，必要时回退到最近的 outgoing 策略指令
    status_messages = storage.list_bound_messages(
        profile_id=profile_id,
        chat_id=chat_id,
        search_query="天机遭遇战",
        limit=80,
    )

    latest_strategy = ""
    for msg in status_messages:
        text = str(msg.get("text") or "").strip()
        is_bot = bool(msg.get("is_bot"))

        if (
            is_bot
            and text.startswith("【天机遭遇战】")
            and "当前策略:" in text
            and "今日遭遇:" in text
        ):
            strategy_match = re.search(r"当前策略:\s*([^\n]+)", text)
            panel_strategy = strategy_match.group(1).strip() if strategy_match else ""
            if panel_strategy and not latest_strategy:
                latest_strategy = panel_strategy
            count_match = re.search(r"今日遭遇:\s*([^\n]+)", text)
            if count_match:
                state["today_count"] = count_match.group(1).strip()
            last_match = re.search(r"上次遭遇:\s*([^\n]+)", text)
            if last_match:
                state["last_encounter"] = last_match.group(1).strip()
            if state["today_count"] != "0/2" or state["last_encounter"] != "暂无":
                break
        elif (
            is_bot
            and text.startswith("【天机遭遇战】")
            and "策略已改为" in text
            and not latest_strategy
        ):
            strategy_match = re.search(r"策略已改为：([^\n。]+)", text)
            if strategy_match:
                latest_strategy = strategy_match.group(1).strip()
        elif not is_bot and text.startswith(".天机遭遇战 ") and not latest_strategy:
            parts = text.split()
            if len(parts) >= 2 and parts[1] in ("谨慎", "均衡", "夺宝", "关闭"):
                latest_strategy = parts[1]

    if latest_strategy:
        state["strategy"] = latest_strategy

    # 2. 提取记录：只认【天机遭遇战记录】回包；这是 PVP 随机遭遇玩家玩法的唯一卷宗来源
    record_messages = storage.list_bound_messages(
        profile_id=profile_id,
        chat_id=chat_id,
        search_query="天机遭遇战记录",
        limit=20,
    )

    records = []
    for msg in record_messages:
        if not msg.get("is_bot"):
            continue
        text = str(msg.get("text") or "").strip()
        if not text.startswith("【天机遭遇战记录】"):
            continue
        time_display = fanren_game.format_timestamp(msg.get("created_at") or 0)

        lines = text.split("\n")[1:]  # 去掉标题
        for line in lines:
            line = line.strip()
            if line and line != "暂未留下遭遇因果。":
                records.append(
                    {
                        "text": line,
                        "time": time_display,
                    }
                )

    # 去重并限制数量
    unique_records = []
    seen_texts = set()
    for r in records:
        if r["text"] not in seen_texts:
            seen_texts.add(r["text"])
            unique_records.append(r)

    state["records"] = unique_records[:5]
    return state


def _build_other_play_view(payload: dict) -> dict:
    gambling_stats = _coerce_json_dict(payload.get("gambling_stats"))
    divination = _build_divination_view(payload)
    tianji_dice = _coerce_json_dict(payload.get("tianji_dice")) or _coerce_json_dict(
        gambling_stats.get("tianji_dice")
    )
    linglong_dice = _coerce_json_dict(
        payload.get("linglong_dice")
    ) or _coerce_json_dict(gambling_stats.get("linglong_dice"))
    return {
        "pagoda": _build_pagoda_view(payload),
        "gambling_karma": float(payload.get("gambling_karma") or 0),
        "divination": divination,
        "divination_count_today": divination["today_count"],
        "ghost_gambling": _build_ghost_gambling_view(payload),
        "tianji_dice": _build_dice_state(
            tianji_dice,
            default_summary_keys=[
                "total_plays",
                "wins",
                "losses",
                "total_won",
                "total_lost",
            ],
        ),
        "linglong_dice": _build_dice_state(
            linglong_dice,
            default_summary_keys=[
                "total_plays",
                "wins",
                "losses",
                "total_won",
            ],
        ),
    }


def _build_divination_batch_view(raw_batch: Optional[dict]) -> dict:
    batch = raw_batch or {}
    initial_count = max(int(batch.get("initial_count") or 0), 0)
    target_count = max(int(batch.get("target_count") or 0), 0)
    sent_count = max(int(batch.get("sent_count") or 0), 0)
    completed_count = max(int(batch.get("completed_count") or 0), 0)
    planned_rounds = max(target_count - initial_count, 0)
    return {
        "raw": batch,
        "active": bool(batch) and str(batch.get("status") or "") == "active",
        "status": str(batch.get("status") or "").strip(),
        "initial_count": initial_count,
        "target_count": target_count,
        "sent_count": sent_count,
        "completed_count": completed_count,
        "planned_rounds": planned_rounds,
        "remaining_rounds": max(planned_rounds - completed_count, 0),
        "pending_command_msg_id": int(batch.get("pending_command_msg_id") or 0),
        "last_error": str(batch.get("last_error") or "").strip(),
        "created_at": float(batch.get("created_at") or 0),
    }


def _list_dungeon_feed_source_messages(
    storage: Storage, chat_id: int, dungeon_key: str, profile_id: Optional[int] = None
) -> list[dict]:
    # 每个用户只看自己 worker 捕获的副本消息（profile 隔离）
    messages = storage.list_bound_messages(
        profile_id=profile_id,
        chat_id=chat_id,
        limit=300,
    )
    dungeon_def = _get_dungeon_definition(dungeon_key)
    prefixes = dungeon_def.get("command_prefixes") or []
    prefixes = [
        str(prefix or "").strip() for prefix in prefixes if str(prefix or "").strip()
    ]
    messages_by_id = {
        int(msg.get("message_id") or 0): msg
        for msg in messages
        if int(msg.get("message_id") or 0)
    }
    allowed_command_ids = {
        int(msg.get("message_id") or 0)
        for msg in messages
        if not bool(msg.get("is_bot"))
        and any(
            str(msg.get("text") or "").strip().startswith(prefix) for prefix in prefixes
        )
    }

    def _has_allowed_ancestor(msg: dict) -> bool:
        message_id = int(msg.get("message_id") or 0)
        if message_id in allowed_command_ids:
            return True
        if not bool(msg.get("is_bot")):
            return False
        reply_to = int(msg.get("reply_to_msg_id") or 0)
        depth = 0
        while reply_to and depth < 8:
            parent = messages_by_id.get(reply_to) or storage.get_bound_message(
                chat_id, reply_to
            )
            if not parent:
                return False
            parent_id = int(parent.get("message_id") or 0)
            parent_text = str(parent.get("text") or "").strip()
            if parent_id in allowed_command_ids:
                return True
            if not bool(parent.get("is_bot")):
                return any(parent_text.startswith(prefix) for prefix in prefixes)
            reply_to = int(parent.get("reply_to_msg_id") or 0)
            depth += 1
        return False

    dungeon_messages = [msg for msg in messages if _has_allowed_ancestor(msg)]
    dungeon_messages.sort(
        key=lambda msg: float(msg.get("created_at") or 0), reverse=True
    )
    return dungeon_messages


def _build_dungeon_messages(
    storage: Storage,
    chat_id: int,
    dungeon_key: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    filtered = _list_dungeon_feed_source_messages(
        storage, chat_id, dungeon_key, profile_id=profile_id
    )

    rows = []
    for message in filtered[:80]:
        text = str(message.get("text") or "").strip()
        is_bot = bool(message.get("is_bot"))
        reply_preview = ""
        sender_username = str(message.get("sender_username") or "").strip()
        sender_display = (
            "机器人"
            if is_bot
            else (f"@{sender_username.lstrip('@')}" if sender_username else "队伍消息")
        )
        rows.append(
            {
                **message,
                "message_id": int(message.get("message_id") or 0),
                "chat_id": int(message.get("chat_id") or chat_id),
                "text": text,
                "reply_preview": reply_preview,
                "sender_display": sender_display,
                "created_at_display": fanren_game.format_timestamp(
                    message.get("created_at") or 0
                ),
            }
        )
    return rows


def _extract_dungeon_command_buttons(dungeon_def: dict) -> list[str]:
    buttons = []
    seen = set()
    for line in dungeon_def.get("help_lines") or []:
        text = str(line or "").strip()
        match = re.search(r"`([^`]+)`", text)
        command_text = (match.group(1) if match else "").strip()
        if not command_text or command_text in seen:
            continue
        seen.add(command_text)
        buttons.append(command_text)
    return buttons


def _extract_dungeon_cleanup_targets(dungeon_messages: list[dict]) -> list[dict]:
    team_keywords = ("队伍", "队长", "成员", "房间")
    usernames = []
    seen = set()
    for message in dungeon_messages:
        text = str(message.get("text") or "")
        reply_preview = str(message.get("reply_preview") or "")
        haystack = f"{text}\n{reply_preview}"
        if not any(keyword in haystack for keyword in team_keywords):
            continue
        for username in re.findall(r"@([A-Za-z0-9_]{3,})", haystack):
            normalized = username.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            usernames.append(username)
    for message in dungeon_messages:
        if bool(message.get("is_bot")):
            continue
        sender_username = str(message.get("sender_username") or "").strip().lstrip("@")
        normalized = sender_username.lower()
        if sender_username and normalized not in seen:
            seen.add(normalized)
            usernames.append(sender_username)
    return [
        {"value": username, "label": f"@{username}", "command": f".请离 @{username}"}
        for username in usernames[:12]
    ]


_STOCK_BATCH_RE = re.compile(
    r"IDX_(\w+)\s+(.+?)\s*([🟢🔴⚡🌙\ufe0f]+)\s*\n"
    r"([\d.]+)\s*\|\s*([+\-]?[\d.]+)%\s*\(额:(\d+)\)\n"
    r"(.+?)/(.+?)/(.+?)/(.*)"
)


def _clean_stock_name(raw: str) -> str:
    """去掉股票名称末尾的方向 emoji 和多余的空白"""
    name = raw.strip()
    # 去掉末尾的 emoji / variation-selector（🟢🔴⚡🌙 等）
    while name and (
        name[-1] in "🟢🔴⚡🌙" or ord(name[-1]) > 0x2000  # 大部分 emoji 和多字节符号
    ):
        name = name[:-1].strip()
    return name


def _parse_stock_market_batch(text: str, observed_at: float) -> list[dict]:
    """从 天道股市·实时行情 消息文本中逐股解析"""
    results = []
    for m in _STOCK_BATCH_RE.finditer(text):
        try:
            code = m.group(1).upper()
            name = _clean_stock_name(m.group(2))
            price = float(m.group(4))
            chg_pct = float(m.group(5))
            volume = int(m.group(6))
            sector = m.group(7).strip()
            trend = m.group(8).strip()
            heat = m.group(9).strip()
            liquidity = m.group(10).strip().rstrip(")").rstrip("额")
            results.append(
                {
                    "stock_code": f"IDX_{code}",
                    "stock_name": name,
                    "current_price": price,
                    "change_percent": chg_pct,
                    "volume": volume,
                    "sector": sector,
                    "trend": trend,
                    "heat": heat,
                    "liquidity": liquidity,
                    "observed_at": observed_at,
                }
            )
        except (ValueError, IndexError):
            continue
    return results


def _build_stock_trend_points(
    history_rows: list[dict], width: int = 220, height: int = 72
) -> str:
    prices = [float(row.get("current_price") or 0) for row in history_rows]
    if not prices:
        return ""
    if len(prices) == 1:
        y = height / 2
        return f"0,{y:.2f} {width:.2f},{y:.2f}"
    min_price = min(prices)
    max_price = max(prices)
    spread = max(max_price - min_price, 1e-9)
    step_x = width / max(len(prices) - 1, 1)
    points = []
    for index, price in enumerate(prices):
        x = index * step_x
        normalized = (price - min_price) / spread
        y = height - (normalized * (height - 8)) - 4
        points.append(f"{x:.2f},{y:.2f}")
    return " ".join(points)


def _decorate_stock_history(
    history_rows: list[dict], max_points: Optional[int] = 16
) -> dict:
    trimmed_rows = (
        history_rows[-max_points:]
        if max_points is not None and int(max_points) > 0
        else list(history_rows)
    )
    latest = trimmed_rows[-1] if trimmed_rows else None
    # delta 始终以时间区间起点（history_rows[0]）为基准
    range_start = history_rows[0] if history_rows else None
    latest_price = float((latest or {}).get("current_price") or 0)
    earliest_price = float((range_start or {}).get("current_price") or 0)
    delta_price = latest_price - earliest_price
    delta_percent = (
        round((delta_price / earliest_price * 100), 2) if earliest_price > 0 else 0
    )
    return {
        "rows": [
            {
                **row,
                "observed_at_display": fanren_game.format_timestamp(
                    row.get("observed_at") or row.get("created_at") or 0
                ),
            }
            for row in trimmed_rows
        ],
        "count": len(history_rows),
        "sparkline_points": _build_stock_trend_points(trimmed_rows),
        "latest_price": latest_price,
        "earliest_price": earliest_price,
        "delta_price": delta_price,
        "delta_percent": delta_percent,
    }


def _latest_stock_player_reply_view(
    storage: Storage, profile_id: int, command_text: str
) -> dict:
    reply = storage.get_stock_player_reply(profile_id, command_text) or {}
    created_at = float(
        (reply or {}).get("updated_at") or (reply or {}).get("created_at") or 0
    )
    return {
        "text": str((reply or {}).get("reply_text") or "").strip(),
        "created_at": created_at,
        "created_at_display": fanren_game.format_timestamp(created_at)
        if created_at
        else "-",
    }


def _build_stock_view(
    storage: Storage,
    profile_id: int,
    chat_id: Optional[int],
    thread_id: Optional[int] = None,
    command_sender_id: Optional[int] = None,
    command_sender_username: str = "",
) -> dict:
    settings = get_settings()
    authorized_user_id = str(settings.authorized_user_id or "").strip()
    source_profile_id = profile_id
    if authorized_user_id:
        admin_profile = storage.get_profile_by_telegram_user_id(authorized_user_id)
        if admin_profile:
            source_profile_id = admin_profile.id
    rows = storage.list_stock_market_info(source_profile_id)
    if not rows:
        fallback_rows = storage.list_stock_source_messages(limit=200)
        if fallback_rows:
            rows = []
            for msg in fallback_rows:
                raw_text = str(msg.get("text") or "")
                observed_at = float(msg.get("created_at") or 0)
                msg_id = int(msg.get("message_id") or 0)
                chat_id = int(msg.get("chat_id") or 0)
                profile_id = int(msg.get("profile_id") or 0)
                # 尝试解析天道股市批量消息
                batch_stocks = _parse_stock_market_batch(raw_text, observed_at)
                if batch_stocks:
                    for stock in batch_stocks:
                        try:
                            storage.upsert_stock_market_history(
                                profile_id or None,
                                chat_id,
                                msg_id,
                                stock["stock_code"],
                                observed_at=stock["observed_at"],
                                **{
                                    k: v
                                    for k, v in stock.items()
                                    if k not in ("stock_code", "observed_at")
                                },
                            )
                        except Exception:
                            pass
                        rows.append(
                            {
                                "stock_code": stock["stock_code"],
                                "stock_name": stock["stock_name"],
                                "current_price": stock["current_price"],
                                "previous_price": stock["current_price"],
                                "change_percent": stock["change_percent"],
                                "sector": stock["sector"],
                                "trend": stock["trend"],
                                "heat": stock["heat"],
                                "liquidity": stock["liquidity"],
                                "volume": stock["volume"],
                                "updated_at": observed_at,
                            }
                        )
                else:
                    price = float(msg.get("current_price") or 0)
                    prev = float(msg.get("previous_price") or price)
                    rows.append(
                        {
                            "stock_code": str(msg.get("stock_code") or ""),
                            "stock_name": str(msg.get("stock_name") or ""),
                            "current_price": price,
                            "previous_price": prev,
                            "change_percent": round((price - prev) / prev * 100, 2)
                            if prev > 0
                            else 0,
                            "updated_at": float(
                                msg.get("observed_at") or msg.get("created_at") or 0
                            ),
                        }
                    )
    for row in rows:
        latest_updated_at = float(row.get("updated_at") or 0)
        row["data_time"] = latest_updated_at
        row["data_time_display"] = fanren_game.format_timestamp(latest_updated_at)
    gainers = sorted(
        rows, key=lambda row: float(row.get("change_percent") or 0), reverse=True
    )
    losers = sorted(rows, key=lambda row: float(row.get("change_percent") or 0))
    latest_updated_at = max(
        (float(row.get("data_time") or 0) for row in rows), default=0
    )
    latest_account = _latest_stock_player_reply_view(storage, profile_id, ".我的持仓")
    latest_task = _latest_stock_player_reply_view(storage, profile_id, ".股市任务")
    return {
        "rows": rows,
        "count": len(rows),
        "top_gainer": gainers[0] if gainers else None,
        "top_loser": losers[0] if losers else None,
        "latest_updated_at": latest_updated_at,
        "latest_updated_display": fanren_game.format_timestamp(latest_updated_at),
        "latest_account_text": latest_account["text"],
        "latest_account_time_display": latest_account["created_at_display"],
        "latest_task_text": latest_task["text"],
        "latest_task_time_display": latest_task["created_at_display"],
        "tracked_stocks": [
            {
                "stock_code": str(row.get("stock_code") or ""),
                "stock_name": str(row.get("stock_name") or "").strip(),
            }
            for row in rows
            if row.get("stock_code")
        ],
        "tracked_codes": [
            str(row.get("stock_code") or "") for row in rows if row.get("stock_code")
        ],
    }


STOCK_HISTORY_RANGE_OPTIONS = {
    "12h": {"label": "最近 12 小时", "seconds": 12 * 3600, "limit": 160},
    "24h": {"label": "最近 24 小时", "seconds": 24 * 3600, "limit": 220},
    "3d": {"label": "最近 3 天", "seconds": 3 * 86400, "limit": 320},
    "7d": {"label": "最近 7 天", "seconds": 7 * 86400, "limit": 420},
    "30d": {"label": "最近 30 天", "seconds": 30 * 86400, "limit": 520},
    "all": {"label": "全部", "seconds": None, "limit": 800},
}


def _resolve_stock_history_range(range_key: str) -> tuple[str, dict]:
    normalized_key = str(range_key or "7d").strip().lower()
    if normalized_key not in STOCK_HISTORY_RANGE_OPTIONS:
        normalized_key = "7d"
    return normalized_key, STOCK_HISTORY_RANGE_OPTIONS[normalized_key]


def _build_stock_history_response(
    storage: Storage, stock_code: str, range_key: str
) -> dict:
    normalized_code = str(stock_code or "").strip().upper()
    normalized_range, range_meta = _resolve_stock_history_range(range_key)
    since_observed_at = None
    if range_meta["seconds"]:
        since_observed_at = fanren_game.time.time() - float(range_meta["seconds"])
    history_rows = storage.list_stock_market_history(
        normalized_code,
        limit=int(range_meta["limit"]),
        since_observed_at=since_observed_at,
    )
    decorated = _decorate_stock_history(history_rows, max_points=None)
    latest_row = history_rows[-1] if history_rows else None
    return {
        "stock_code": normalized_code,
        "stock_name": str((latest_row or {}).get("stock_name") or normalized_code),
        "range_key": normalized_range,
        "range_label": str(range_meta["label"]),
        "rows": decorated["rows"],
        "count": decorated["count"],
        "sparkline_points": decorated["sparkline_points"],
        "latest_price": decorated["latest_price"],
        "earliest_price": decorated["earliest_price"],
        "delta_price": decorated["delta_price"],
        "delta_percent": decorated.get("delta_percent", 0),
        "latest_observed_at": float((latest_row or {}).get("observed_at") or 0),
        "latest_observed_at_display": fanren_game.format_timestamp(
            (latest_row or {}).get("observed_at") or 0
        ),
    }


def _format_external_artifacts(character: dict) -> str:
    equipped_ids = _coerce_json_list(character.get("equipped_treasure_id"))
    inventory = character.get("inventory") or {}
    items = inventory.get("items") or []
    treasure_by_id = {
        str(item.get("item_id") or ""): item for item in items if isinstance(item, dict)
    }
    lines = []
    for treasure_id in equipped_ids:
        item = treasure_by_id.get(str(treasure_id)) or {}
        name = (item.get("name") or str(treasure_id or "")).strip()
        durability = item.get("durability")
        max_durability = item.get("max_durability")
        if durability is not None and max_durability is not None:
            lines.append(f"- {name}: {durability}/{max_durability}")
        elif name:
            lines.append(f"- {name}")
    return "\n".join(lines)


def _first_equipped_artifact_name(character: dict) -> str:
    equipped_ids = _coerce_json_list(character.get("equipped_treasure_id"))
    inventory = character.get("inventory") or {}
    items = inventory.get("items") or []
    treasure_by_id = {
        str(item.get("item_id") or ""): item for item in items if isinstance(item, dict)
    }
    for treasure_id in equipped_ids:
        item = treasure_by_id.get(str(treasure_id)) or {}
        name = str(item.get("name") or "").strip()
        if name:
            return name
    return ""


def _equipped_artifact_names_text(character: dict) -> str:
    equipped_ids = _coerce_json_list(character.get("equipped_treasure_id"))
    inventory = character.get("inventory") or {}
    items = inventory.get("items") or []
    treasure_by_id = {
        str(item.get("item_id") or ""): item for item in items if isinstance(item, dict)
    }
    names = []
    for treasure_id in equipped_ids:
        item = treasure_by_id.get(str(treasure_id)) or {}
        name = str(item.get("name") or treasure_id or "").strip()
        if name:
            names.append(name)
    return "、".join(names)


def _build_equipped_artifact_details(character: dict) -> str:
    details = _format_external_artifacts(character).strip()
    return details or "未装备法宝"


def _build_recent_player_options(
    storage: Storage,
    chat_id: Optional[int],
    profile_id: Optional[int] = None,
    exclude_usernames: Optional[list[str]] = None,
    limit: int = 12,
) -> list[dict]:
    return []


def _build_sect_recent_reply_text(
    storage: Storage,
    profile_id: int,
    sect_chat,
    current_sect_feature: Optional[dict],
    active_profile,
    fallback_text: str = "",
) -> str:
    if not sect_chat or not profile_id:
        return str(fallback_text or "").strip()
    messages = storage.list_bound_messages(
        profile_id=profile_id,
        chat_id=int(sect_chat.chat_id) if sect_chat else None,
        limit=60,
    )
    sect_texts = []
    for msg in messages:
        if not msg.get("is_bot"):
            continue
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        if not _is_sect_related_message(text, current_sect_feature):
            continue
        sect_texts.append(text[:400])
    if sect_texts:
        return "\n\n---\n\n".join(sect_texts[:8])
    return str(fallback_text or "").strip()


def _is_sect_related_message(text: str, current_sect_feature: Optional[dict]) -> bool:
    sect_keywords = {
        "宗门",
        "大殿",
        "贡献",
        "传功",
        "签到",
        "俸禄",
        "宝库",
        "兑换",
        "小药园",
        "播种",
        "采药",
        "除草",
        "除虫",
        "浇水",
        "黄枫谷",
        "凌霄宫",
        "登天阶",
        "问心台",
        "借宝阁",
        "天罡风",
        "阴罗宗",
        "献祭",
        "血洗",
    }
    if current_sect_feature and current_sect_feature.get("name"):
        sect_keywords.add(current_sect_feature["name"])
    return any(kw in text for kw in sect_keywords)


def _format_sect_position(character: dict) -> str:
    positions = []
    if int(character.get("is_sect_elder") or 0):
        positions.append("长老")
    if int(character.get("is_grand_elder") or 0):
        positions.append("太上长老")
    return " / ".join(positions)


def _payload_name_list(value) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") or text.startswith("{"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                return _payload_name_list(parsed)
        return [text] if text else []
    if isinstance(value, dict):
        name = (
            value.get("name")
            or value.get("title")
            or value.get("item_name")
            or value.get("technique_name")
            or value.get("badge_name")
            or ""
        )
        text = str(name or "").strip()
        return [text] if text else []
    if isinstance(value, list):
        names = []
        for item in value:
            names.extend(_payload_name_list(item))
        return names
    return []


def _resolve_payload_display_name(raw_value, game_items_dict: dict) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    meta = game_items_dict.get(text) or {}
    return str(meta.get("name") or text).strip()


def _payload_named_entries(value, game_items_dict: dict) -> list[dict]:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") or text.startswith("{"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                return _payload_named_entries(parsed, game_items_dict)
        display = _resolve_payload_display_name(text, game_items_dict)
        return [{"id": text, "name": display}] if display else []
    if isinstance(value, dict):
        named_keys = {
            "name",
            "title",
            "item_name",
            "technique_name",
            "badge_name",
            "formation_name",
        }
        id_keys = {"item_id", "id", "badge_id", "technique_id", "formation_id"}
        if not any(value.get(key) for key in named_keys | id_keys):
            entries = []
            for raw_key, raw_item in value.items():
                nested_entries = _payload_named_entries(raw_item, game_items_dict)
                if nested_entries:
                    entries.extend(nested_entries)
                    continue
                if isinstance(raw_item, (int, float)) and int(raw_item) <= 0:
                    continue
                if isinstance(raw_item, str) and not raw_item.strip():
                    continue
                key_text = str(raw_key or "").strip()
                if not key_text:
                    continue
                display_name = _resolve_payload_display_name(key_text, game_items_dict)
                entries.append({"id": key_text, "name": display_name or key_text})
            return entries
        raw_id = str(
            value.get("item_id")
            or value.get("id")
            or value.get("badge_id")
            or value.get("technique_id")
            or value.get("formation_id")
            or ""
        ).strip()
        display_name = str(
            value.get("name")
            or value.get("title")
            or value.get("item_name")
            or value.get("technique_name")
            or value.get("badge_name")
            or value.get("formation_name")
            or _resolve_payload_display_name(raw_id, game_items_dict)
            or ""
        ).strip()
        return [{"id": raw_id, "name": display_name}] if display_name else []
    if isinstance(value, list):
        entries = []
        for item in value:
            entries.extend(_payload_named_entries(item, game_items_dict))
        return entries
    return []


def _payload_name_summary(value, game_items_dict: dict) -> str:
    names = []
    seen = set()
    for entry in _payload_named_entries(value, game_items_dict):
        name = str(entry.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return ", ".join(names) if names else "-"


def _recipe_craft_name(recipe_name: str) -> str:
    text = str(recipe_name or "").strip()
    for suffix in ["丹方", "单方", "图纸", "配方"]:
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)].strip()
    return text


def _build_sect_daily_view(payload: dict, now=None) -> dict:
    now = now or fanren_game.time.time()
    last_check_in_time = sect_game._parse_iso_timestamp(
        payload.get("last_sect_check_in")
    )
    checked_in_today = sect_game._parse_date_key(
        payload.get("last_sect_check_in")
    ) == sect_game.current_date_key(now)
    last_teach_date = sect_game._parse_date_key(payload.get("last_teach_date"))
    teach_count = max(sect_game._parse_int(payload.get("teach_count"), 0), 0)
    if last_teach_date != sect_game.current_date_key(now):
        teach_count = 0
    return {
        "last_check_in_time": last_check_in_time,
        "checked_in_today": checked_in_today,
        "consecutive_check_in_days": sect_game._parse_int(
            payload.get("consecutive_check_in_days"), 0
        ),
        "last_teach_date": last_teach_date,
        "teach_count": teach_count,
        "teach_progress_text": f"{teach_count}/{sect_game.SECT_DAILY_TEACH_LIMIT}",
    }


def _merge_sect_daily_view_with_session(
    daily_view: dict, sect_session: Optional[dict], now=None
) -> dict:
    merged = dict(daily_view or {})
    if not sect_session:
        return merged
    now = now or fanren_game.time.time()
    session_teach_date = sect_game._parse_date_key(sect_session.get("last_teach_date"))
    session_teach_count = max(
        sect_game._parse_int(sect_session.get("last_teach_count"), 0), 0
    )
    if session_teach_date == sect_game.current_date_key(
        now
    ) and session_teach_count > int(merged.get("teach_count") or 0):
        merged["last_teach_date"] = session_teach_date
        merged["teach_count"] = session_teach_count
        merged["teach_progress_text"] = (
            f"{session_teach_count}/{sect_game.SECT_DAILY_TEACH_LIMIT}"
        )
    return merged


def _format_payload_display_text(raw_value, game_items_dict: dict) -> str:
    summary = _payload_name_summary(raw_value, game_items_dict)
    return summary if summary and summary != "-" else ""


def _format_market_effects(raw_value) -> str:
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return ""
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
        return _format_market_effects(parsed)
    if isinstance(raw_value, list):
        parts = [
            part
            for part in (_format_market_effects(item) for item in raw_value)
            if part
        ]
        return "；".join(parts)
    if isinstance(raw_value, dict):
        parts = []
        for key, value in raw_value.items():
            key_text = str(key or "").strip()
            value_text = _format_market_effects(value)
            if key_text and value_text:
                parts.append(f"{key_text}: {value_text}")
            elif key_text:
                parts.append(key_text)
            elif value_text:
                parts.append(value_text)
        return "；".join(parts)
    text = str(raw_value or "").strip()
    return text


def _format_market_price(raw_value, game_items_dict: dict) -> str:
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return "-"
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
        return _format_market_price(parsed, game_items_dict)
    if isinstance(raw_value, dict):
        parts = []
        for item_id, quantity in raw_value.items():
            display_name = _resolve_payload_display_name(item_id, game_items_dict)
            qty = sect_game._parse_int(quantity, 0)
            if qty > 0:
                parts.append(f"{display_name}*{qty}")
            elif display_name:
                parts.append(display_name)
        return "、".join(parts) if parts else "-"
    if isinstance(raw_value, list):
        parts = []
        for item in raw_value:
            formatted = _format_market_price(item, game_items_dict)
            if formatted != "-":
                parts.append(formatted)
        return "、".join(parts) if parts else "-"
    text = str(raw_value or "").strip()
    return text or "-"


def _market_price_parts(raw_value, game_items_dict: dict) -> list[dict]:
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [{"name": text, "quantity": 0}]
        return _market_price_parts(parsed, game_items_dict)
    if isinstance(raw_value, dict):
        parts = []
        for item_id, quantity in raw_value.items():
            display_name = _resolve_payload_display_name(item_id, game_items_dict)
            parts.append(
                {
                    "name": display_name or str(item_id or "").strip(),
                    "quantity": max(sect_game._parse_int(quantity, 0), 0),
                }
            )
        return sorted(parts, key=lambda item: (item["name"], item["quantity"]))
    if isinstance(raw_value, list):
        parts = []
        for item in raw_value:
            parts.extend(_market_price_parts(item, game_items_dict))
        return parts
    text = str(raw_value or "").strip()
    return [{"name": text, "quantity": 0}] if text else []


def _market_price_sort_key(raw_value, game_items_dict: dict) -> tuple:
    parts = _market_price_parts(raw_value, game_items_dict)
    if not parts:
        return ((1, "", 0),)
    normalized_parts = sorted(
        parts,
        key=lambda part: (
            0 if str(part.get("name") or "").strip() == "灵石" else 1,
            str(part.get("name") or "").strip(),
            int(part.get("quantity") or 0),
        ),
    )
    return tuple(
        (
            0 if str(part.get("name") or "").strip() == "灵石" else 1,
            str(part.get("name") or "").strip(),
            int(part.get("quantity") or 0),
        )
        for part in normalized_parts
    )


def _reverse_market_price_sort_key(sort_key: tuple) -> tuple:
    reversed_parts = []
    for priority, name, quantity in sort_key or ():
        reversed_parts.append((priority, name, -int(quantity or 0)))
    return tuple(reversed_parts)


def _market_price_preview(raw_value, game_items_dict: dict, max_parts: int = 3) -> dict:
    parts = _market_price_parts(raw_value, game_items_dict)
    full_parts = []
    for part in parts:
        name = str(part.get("name") or "").strip()
        quantity = int(part.get("quantity") or 0)
        if not name:
            continue
        full_parts.append(f"{name}*{quantity}" if quantity > 0 else name)
    if not full_parts:
        return {"preview_text": "-", "full_text": "-", "item_count": 0}
    preview_parts = full_parts[:max_parts]
    preview_text = "、".join(preview_parts)
    if len(full_parts) > max_parts:
        preview_text = f"{preview_text} 等{len(full_parts)}项"
    return {
        "preview_text": preview_text,
        "full_text": "、".join(full_parts),
        "item_count": len(full_parts),
    }


def _normalize_sect_name_text(value: str) -> str:
    return str(value or "").replace("【", "").replace("】", "").strip()


def _sect_matches_current(item_sect_name: str, current_sect_name: str) -> bool:
    current = _normalize_sect_name_text(current_sect_name)
    item_sect = _normalize_sect_name_text(item_sect_name)
    if not item_sect:
        return True
    if not current:
        return False
    return current in item_sect or item_sect in current


def _item_type_label(raw_type: str, *, is_material: bool = False) -> str:
    item_type_map = {
        "material": "材料",
        "elixir": "丹药",
        "recipe": "图纸",
        "quest_item": "任务道具",
        "treasure": "法宝",
        "badge": "徽章",
        "talisman": "符箓",
        "formation": "阵法",
        "seed": "种子",
        "special_item": "特殊物品",
        "special_tool": "特殊工具",
        "recipe_internal": "特殊配方",
        "loot_box": "宝箱",
    }
    normalized = str(raw_type or "").strip()
    if normalized:
        return item_type_map.get(normalized, normalized)
    return "材料" if is_material else "-"


CULTIVATION_STAGE_CAPS = {
    "练气初期": 2000,
    "练气中期": 5000,
    "练气后期": 10000,
    "筑基初期": 30000,
    "筑基中期": 60000,
    "筑基后期": 100000,
    "结丹初期": 120000,
    "结丹中期": 160000,
    "结丹后期": 200000,
    "元婴初期": 300000,
    "元婴中期": 400000,
    "元婴后期": 500000,
    "化神初期": 700000,
    "化神中期": 900000,
    "化神后期": 1200000,
}


SECT_METADATA = {
    "元婴宗": {
        "description": "修真界顶尖门派之一，实力雄厚，门中元婴修士众多，常为修真界权力中心。",
        "bonus": "55",
    },
    "魔道血刹宗": {
        "description": "以修炼血煞之气闻名的魔道宗门，门人修炼嗜血功法，战力凶悍。",
        "bonus": "35",
    },
    "天星宗": {
        "description": "号称以星辰推演大道的宗门，善于占星与演算，门人修炼独特的星辰功法。",
        "bonus": "42",
    },
    "太一门": {
        "description": "修真界一流宗门，门中长老大多神通广大，底蕴深厚。",
        "bonus": "38",
    },
    "万灵宗": {
        "description": "以驭兽术著称的宗门，门人善于培养灵兽作战，宗内灵兽种类繁多。",
        "bonus": "45",
    },
    "凌霄宫": {
        "description": "仙风道骨的道门大派，修炼纯正仙道之法，弟子心性清高。",
        "bonus": "62",
    },
    "合欢宗": {
        "description": "双修宗门，男女修士以合欢之术快速提升修为，功法独具特色。",
        "bonus": "50",
    },
    "阴罗宗": {
        "description": "以煞气为根基的魔道势力，修士常以残忍功法迅速提升实力。",
        "bonus": "38",
    },
    "星宫": {
        "description": "神秘的女修宗门，弟子清一色皆为女修，修炼星辰玄功。",
        "bonus": "35",
    },
    "黄枫谷": {
        "description": "韩立早期加入的小型宗门，门人以勤修苦练著称，资源有限但氛围和谐。",
        "bonus": "25",
    },
    "落云宗": {
        "description": "落云宗是天南地区云梦山脉三大宗门之一，以炼制丹灵闻名。该宗门招收弟子门槛低但缺乏顶尖战力，在韩立加入后，凭借其实力和资源改造，成为天南第一大宗门。",
        "bonus": "62",
    },
}


def _format_cultivation_progress(
    stage_name: str, cultivation_points, stage_caps: Optional[dict] = None
) -> str:
    points_text = str(cultivation_points or "").strip()
    if not points_text:
        return ""
    caps = stage_caps or CULTIVATION_STAGE_CAPS
    cap = caps.get((stage_name or "").strip())
    if not cap:
        return points_text
    return f"({points_text} / {cap})"


_ITEM_DELTA_PATTERNS = (
    re.compile(r"(?:奇遇)?获得[^\n:：]*[:：]?\s*(?P<value>.+)"),
    re.compile(r"(?:奇遇)?减少[^\n:：]*[:：]?\s*(?P<value>.+)"),
    re.compile(r"(?:奇遇)?失去[^\n:：]*[:：]?\s*(?P<value>.+)"),
    re.compile(r"(?:奇遇)?消耗[^\n:：]*[:：]?\s*(?P<value>.+)"),
)


def _extract_item_delta_lines(raw_text: str) -> list[str]:
    lines = []
    for raw_line in (raw_text or "").splitlines():
        line = raw_line.strip().lstrip("-• ")
        if not line or "修为" in line:
            continue
        for pattern in _ITEM_DELTA_PATTERNS:
            match = pattern.search(line)
            if match:
                lines.append(match.group(0).strip("，。；; "))
                break
    return lines


def _extract_adventure_lines(raw_text: str) -> list[str]:
    lines = []
    for raw_line in (raw_text or "").splitlines():
        line = raw_line.strip().lstrip("-• ")
        if line and "奇遇" in line:
            lines.append(line.strip("，。；; "))
    return lines


def _build_cultivation_result_view(result: dict) -> dict:
    row = dict(result or {})
    gain_value = row.get("gain_value")
    if gain_value is None:
        gain_text = "修为变化未识别"
    elif int(gain_value) >= 0:
        gain_text = f"+{int(gain_value)}"
    else:
        gain_text = f"-{abs(int(gain_value))}"

    item_lines = _extract_item_delta_lines(row.get("raw_text") or "")
    adventure_lines = _extract_adventure_lines(row.get("raw_text") or "")
    row["gain_text"] = gain_text
    row["item_lines"] = item_lines[:3]
    row["item_summary"] = "；".join(item_lines[:2]) if item_lines else "无明显物品变化"
    row["adventure_lines"] = adventure_lines[:3]
    row["adventure_summary"] = (
        "；".join(adventure_lines[:2]) if adventure_lines else "无奇遇信息"
    )
    row["stage_display"] = (row.get("stage_name") or "-").strip() or "-"
    row["progress_display"] = (row.get("progress_text") or "-").strip() or "-"
    row["mode_label"] = "深度闭关" if row.get("mode") == "deep" else "普通闭关"
    return row


def _build_pagination_numbers(
    current_page: int, total_pages: int
) -> list[Optional[int]]:
    total_pages = max(int(total_pages or 1), 1)
    current_page = min(max(int(current_page or 1), 1), total_pages)
    if total_pages <= 6:
        return list(range(1, total_pages + 1))
    if current_page <= 3:
        return [1, 2, 3, 4, None, total_pages]
    if current_page >= total_pages - 2:
        return [1, None, total_pages - 3, total_pages - 2, total_pages - 1, total_pages]
    return [
        1,
        None,
        current_page - 1,
        current_page,
        current_page + 1,
        None,
        total_pages,
    ]


def _sync_all_items_if_needed(storage: Storage, cookie_text: str):
    last_sync = float(storage.get_runtime_state("last_all_items_sync") or 0)
    now = fanren_game.time.time()
    if now - last_sync > 86400:
        from tg_game.clients.asc_client import get_all_items

        try:
            payload, _status = get_all_items(cookie_text)
            items = (
                payload
                if isinstance(payload, list)
                else (
                    payload.get("items")
                    or payload.get("data")
                    or list(payload.values())
                    if isinstance(payload, dict)
                    else []
                )
            )
            if items:
                storage.upsert_game_items(items)
                storage.set_runtime_state("last_all_items_sync", str(now))
        except Exception as exc:
            pass


def _sync_bootstrap_if_needed(storage: Storage, cookie_text: str):
    last_sync = float(storage.get_runtime_state("last_bootstrap_sync") or 0)
    now = fanren_game.time.time()
    if now - last_sync <= 86400:
        return
    from tg_game.clients.asc_client import get_bootstrap

    try:
        payload, _status = get_bootstrap(cookie_text)
        if not isinstance(payload, dict):
            return

        wrote_items = False
        wrote_thresholds = False

        game_items_payload = payload.get("game_items") or {}
        if isinstance(game_items_payload, dict):
            items = []
            for item_id, meta in game_items_payload.items():
                if not isinstance(meta, dict):
                    continue
                items.append({"id": item_id, **meta})
            if items:
                storage.upsert_game_items_partial(items)
                wrote_items = True

        level_thresholds = payload.get("level_thresholds") or {}
        if isinstance(level_thresholds, dict) and level_thresholds:
            storage.replace_level_thresholds(level_thresholds)
            wrote_thresholds = True

        if wrote_thresholds and (wrote_items or isinstance(game_items_payload, dict)):
            storage.set_runtime_state("last_bootstrap_sync", str(now))
    except Exception:
        pass


def _sync_shop_items_if_needed(storage: Storage, cookie_text: str):
    last_sync = float(storage.get_runtime_state("last_shop_items_sync") or 0)
    now = fanren_game.time.time()
    if now - last_sync <= 86400:
        return
    from tg_game.clients.asc_client import get_shop_items

    try:
        payload, _status = get_shop_items(cookie_text)
        items = (
            payload
            if isinstance(payload, list)
            else (
                payload.get("items") or payload.get("data") or list(payload.values())
                if isinstance(payload, dict)
                else []
            )
        )
        if items:
            storage.replace_shop_items(items)
            storage.set_runtime_state("last_shop_items_sync", str(now))
    except Exception:
        pass


def _sync_marketplace_listings_if_needed(storage: Storage, cookie_text: str):
    last_sync = float(storage.get_runtime_state("last_marketplace_listings_sync") or 0)
    now = fanren_game.time.time()
    if now - last_sync <= 300:
        return
    from tg_game.clients.asc_client import get_all_marketplace_listings

    try:
        game_items_dict = storage.get_game_items()
        items = []
        for item in get_all_marketplace_listings(cookie_text):
            item_id = str(item.get("item_id") or "").strip()
            meta = game_items_dict.get(item_id) or {}
            items.append(
                {
                    **item,
                    "item_type": str(meta.get("type") or "").strip()
                    or ("material" if item.get("is_material") else ""),
                }
            )
        storage.replace_marketplace_listings(items)
        storage.set_runtime_state("last_marketplace_listings_sync", str(now))
    except Exception:
        pass


def _sync_profile_from_cultivator(
    storage: Storage, profile_id: int, cultivator_payload: dict
) -> None:
    profile = storage.get_profile(profile_id)
    if not profile:
        return
    game_username = (
        cultivator_payload.get("username") or profile.telegram_username or ""
    ).strip()
    sect_name = (cultivator_payload.get("sect_name") or "").strip()
    sect_position = _format_sect_position(cultivator_payload)
    sect_meta = SECT_METADATA.get(sect_name, {})
    stage_caps = {**CULTIVATION_STAGE_CAPS, **(storage.get_level_thresholds() or {})}
    storage.update_profile_game_info(
        profile_id=profile_id,
        display_name=(cultivator_payload.get("dao_name") or "").strip(),
        artifact_text=_format_external_artifacts(cultivator_payload),
        spirit_root=(cultivator_payload.get("spirit_root") or "").strip(),
        stage_name=(cultivator_payload.get("cultivation_level") or "").strip(),
        cultivation_text=_format_cultivation_progress(
            (cultivator_payload.get("cultivation_level") or "").strip(),
            cultivator_payload.get("cultivation_points"),
            stage_caps,
        ),
        poison_text=str(cultivator_payload.get("drug_poison_points") or "").strip(),
        kill_count_text=str(cultivator_payload.get("kill_count") or "").strip(),
        game_name=(cultivator_payload.get("dao_name") or "").strip(),
        account_name=(f"@{game_username.lstrip('@')}" if game_username else ""),
    )
    storage.update_profile_sect_info(
        profile_id=profile_id,
        sect_name=sect_name,
        sect_leader="",
        sect_position=sect_position,
        sect_description=sect_meta.get("description", ""),
        sect_bonus_text=sect_meta.get("bonus", ""),
        sect_contribution_text=str(
            cultivator_payload.get("sect_contribution") or ""
        ).strip(),
    )
    if game_username:
        storage.bind_profile_telegram_account(
            profile_id,
            telegram_user_id=profile.telegram_user_id,
            telegram_username=game_username,
            telegram_phone=profile.telegram_phone,
            telegram_session_name=profile.telegram_session_name,
        )


def _build_rift_failure_profile_state(
    payload: dict, cultivation_session: Optional[dict]
) -> Optional[dict]:
    status = str((payload or {}).get("status") or "").strip().upper()
    if status != "ESCAPED_SOUL":
        return None
    reason = str((cultivation_session or {}).get("stopped_reason") or "").strip()
    return {
        "title": "元婴遁逃·虚弱",
        "summary": reason or "当前为残魂状态，探寻裂缝已触发自动任务熔断。",
        "status": status,
        "dao_name": str((payload or {}).get("dao_name") or "").strip(),
        "stage_name": str((payload or {}).get("cultivation_level") or "").strip(),
    }


def create_app() -> FastAPI:
    settings = get_settings()
    storage = Storage(settings.database_path)
    application = FastAPI(title=settings.app_name, version=settings.app_version)
    application.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    visible_module_keys = {
        "cultivation",
        "sect",
        "inventory",
        "other",
        "estate",
        "market",
        "stock",
        "dungeon",
    }

    def _sync_env_binding(profile_id: int, telegram_user_id: str = "") -> None:
        storage.sync_env_chat_binding(
            profile_id=profile_id,
            chat_id=settings.bound_chat_id,
            thread_id=settings.bound_thread_id,
            chat_type=settings.bound_chat_type,
            bot_username=settings.bound_bot_username,
            bot_id=settings.bound_bot_id,
            telegram_user_id=telegram_user_id,
        )

    def _get_authorized_user_id_text() -> str:
        return str(settings.authorized_user_id or "").strip()

    def _get_admin_profile():
        authorized_user_id = _get_authorized_user_id_text()
        if not authorized_user_id:
            return None
        return storage.get_profile_by_telegram_user_id(authorized_user_id)

    def _is_admin_profile(profile) -> bool:
        return is_authorized_profile(storage, profile)

    def _get_global_market_cookie() -> str:
        admin_profile = _get_admin_profile()
        if not admin_profile:
            return ""
        return get_effective_external_cookie(storage)

    def _sync_global_reference_data_if_needed() -> None:
        cookie_text = _get_global_market_cookie()
        if not cookie_text:
            return
        _sync_bootstrap_if_needed(storage, cookie_text)
        _sync_all_items_if_needed(storage, cookie_text)
        _sync_shop_items_if_needed(storage, cookie_text)
        _sync_marketplace_listings_if_needed(storage, cookie_text)

    def _build_command_target_context(active_profile) -> dict:
        command_chat = None
        if active_profile:
            command_chat = _get_primary_command_chat(
                active_profile.id, fanren_game.FANREN_BOT_USERNAME
            )
        return {
            "bound_chat_id": command_chat.chat_id
            if command_chat
            else settings.bound_chat_id,
            "bound_thread_id": command_chat.thread_id
            if command_chat
            else settings.bound_thread_id,
            "bound_chat_type": command_chat.chat_type
            if command_chat
            else settings.bound_chat_type,
            "bound_bot_username": command_chat.bot_username
            if command_chat
            else settings.bound_bot_username,
            "bound_bot_id": command_chat.bot_id
            if command_chat and command_chat.bot_id is not None
            else settings.bound_bot_id,
            "command_chat_ready": bool(command_chat or settings.bound_chat_id),
        }

    def _build_sect_command_target_context(active_profile, sect_chat=None) -> dict:
        fallback_context = _build_command_target_context(active_profile)
        if not sect_chat and active_profile:
            sect_chat = _get_primary_command_chat(
                active_profile.id, sect_game.SECT_BOT_USERNAME
            )
        return {
            "sect_bound_chat_id": sect_chat.chat_id
            if sect_chat
            else fallback_context["bound_chat_id"],
            "sect_bound_thread_id": sect_chat.thread_id
            if sect_chat
            else fallback_context["bound_thread_id"],
            "sect_bound_chat_type": sect_chat.chat_type
            if sect_chat
            else fallback_context["bound_chat_type"],
            "sect_bound_bot_username": (
                sect_chat.bot_username
                if sect_chat and sect_chat.bot_username
                else sect_game.SECT_BOT_USERNAME
            ),
            "sect_command_chat_ready": bool(
                (sect_chat and sect_chat.chat_id) or fallback_context["bound_chat_id"]
            ),
        }

    def _build_sect_treasury_items(active_profile) -> list[dict]:
        if not active_profile:
            return []
        current_sect_name = _normalize_sect_name_text(active_profile.sect_name)
        game_items_dict = storage.get_game_items()
        all_entries = []
        for item in storage.get_shop_items():
            item_id = str(item.get("item_id") or "").strip()
            meta = game_items_dict.get(item_id) or {}
            sect_exclusive_name = (
                _format_payload_display_text(
                    item.get("sect_exclusive"), game_items_dict
                )
                or str(item.get("sect_exclusive") or "").strip()
            )
            display_name = str(meta.get("name") or item.get("name") or item_id).strip()
            if not display_name:
                continue
            all_entries.append(
                {
                    **item,
                    "display_name": display_name,
                    "display_type": _item_type_label(
                        item.get("type") or meta.get("type")
                    ),
                    "shop_price_text": f"{int(item.get('shop_price') or 0)} 贡献",
                    "sect_exclusive_name": sect_exclusive_name,
                    "sect_exclusive_label": sect_exclusive_name or "通用",
                }
            )
        return sorted(
            [
                entry
                for entry in all_entries
                if _sect_matches_current(
                    entry.get("sect_exclusive_name") or "", current_sect_name
                )
            ],
            key=lambda entry: (
                int(entry.get("shop_price") or 0),
                entry.get("display_name") or "",
            ),
        )

    def _build_shared_template_context(active_profile) -> dict:
        external_account = (
            storage.get_external_account(active_profile.id, ASC_PROVIDER)
            if active_profile
            else None
        )
        return {
            **_build_command_target_context(active_profile),
            "current_sect_name": active_profile.sect_name if active_profile else "",
            "sect_treasury_items": _build_sect_treasury_items(active_profile),
            "external_account": external_account,
            "is_admin_profile": _is_admin_profile(active_profile),
            "authorized_user_id": _get_authorized_user_id_text(),
        }

    def _is_public_path(path: str) -> bool:
        if path.startswith("/static"):
            return True
        return path in {
            "/login",
            "/logout",
            "/health",
            "/auth/external/connect",
            "/auth/external/logout",
            "/auth/external/refresh",
            "/auth/telegram/local-login",
            "/auth/telegram/start",
            "/auth/telegram/verify",
            "/auth/telegram/password",
            "/auth/telegram/logout",
        }

    def _sign_in_profile(
        request: Request, profile_id: int, redirect_url: str = "/"
    ) -> RedirectResponse:
        current_token = request.cookies.get(APP_SESSION_COOKIE, "")
        session_token = storage.create_app_session(
            profile_id, session_token=current_token
        )
        response = RedirectResponse(url=redirect_url or "/", status_code=303)
        response.set_cookie(
            APP_SESSION_COOKIE,
            session_token,
            httponly=True,
            samesite="lax",
            secure=False,
            max_age=86400 * 7,
        )
        return response

    def _login_session_name() -> str:
        return settings.telegram_login_session_name or settings.telegram_session_name

    def _login_session_name_for_phone(phone: str = "") -> str:
        base_name = (_login_session_name() or "tg_game_login").strip()
        digits = re.sub(r"\D+", "", str(phone or "").strip())
        if not digits:
            return base_name
        return f"{base_name}_{digits}"

    def _build_tianji_login_redirect(message: str = "") -> RedirectResponse:
        normalized_message = (
            message or ""
        ).strip() or "天机阁会话已失效，请重新粘贴 session Cookie 后再继续。"
        return RedirectResponse(
            url="/login?error=" + quote_plus(normalized_message), status_code=303
        )

    def _get_external_account_for_profile(profile_id: int) -> Optional[dict]:
        if not profile_id:
            return None
        return storage.get_external_account(profile_id, ASC_PROVIDER)

    def _is_external_session_expired_for_profile(profile_id: int) -> bool:
        return is_external_account_expired(
            _get_external_account_for_profile(profile_id)
        )

    def _ensure_external_session_active(profile) -> Optional[RedirectResponse]:
        if not profile:
            return None
        if not _is_external_session_expired_for_profile(profile.id):
            return None
        return _build_tianji_login_redirect()

    def _connect_external_cookie(profile_id: int, cookie_text: str) -> None:
        profile = storage.get_profile(profile_id)
        if not profile:
            raise RuntimeError("Profile not found")
        is_admin = _is_admin_profile(profile)
        normalized_cookie_text = (cookie_text or "").strip()
        global_cookie_text = _get_global_market_cookie()
        if (
            normalized_cookie_text
            and not is_admin
            and normalized_cookie_text != global_cookie_text
        ):
            raise RuntimeError("只有管理员可以替换天机阁 Cookie")
        cultivator_payload = sync_external_account(
            storage,
            profile_id,
            cookie_text=(
                normalized_cookie_text
                if is_admin
                else (global_cookie_text or normalized_cookie_text)
            ),
        )
        telegram_user_id = str(profile.telegram_user_id or "").strip()
        telegram_username = (profile.telegram_username or "").strip().lstrip("@")
        telegram_session_name = (
            profile.telegram_session_name
            or _login_session_name()
            or settings.telegram_session_name
        ).strip()
        storage.bind_profile_telegram_account(
            profile_id,
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            telegram_phone=profile.telegram_phone,
            telegram_session_name=telegram_session_name,
        )
        storage.activate_profile(profile_id)
        if is_admin:
            _sync_global_reference_data_if_needed()
        _sync_profile_from_cultivator(storage, profile_id, cultivator_payload)
        _sync_env_binding(profile_id, telegram_user_id)
        for binding in storage.list_chat_bindings(profile_id):
            if not binding.is_active:
                continue
            try:
                sync_cultivation_session(storage, profile_id, binding.chat_id)
            except Exception as exc:
                logger.warning(
                    "Cultivation session resync failed after external login profile=%s chat=%s: %s",
                    profile_id,
                    binding.chat_id,
                    exc,
                )
        storage.request_sect_refresh(profile_id, cooldown_seconds=0)

    def _build_external_session_notice(
        external_account: Optional[dict],
    ) -> Optional[dict]:
        if not external_account:
            return None
        status = (external_account.get("status") or "").strip().lower()
        last_error = (external_account.get("last_error") or "").strip()
        if status == "logged_out":
            return None
        if status == "expired":
            return {
                "level": "error",
                "title": "天机阁会话已失效",
                "message": "天机阁 session 已失效，请重新获取 Cookie 后在天机阁页重新验证。",
                "detail": last_error,
            }
        if status == "error" and last_error:
            return {
                "level": "error",
                "title": "天机阁同步失败",
                "message": "最近一次天机阁接口校验失败，请检查 Cookie 或稍后重试。",
                "detail": last_error,
            }
        return None

    def _should_refresh_cultivator_payload(
        profile, external_account: Optional[dict]
    ) -> bool:
        return should_keep_external_session_fresh(profile, external_account)

    def _get_request_profile(request: Request):
        return getattr(request.state, "auth_profile", None)

    def _get_primary_command_chat(profile_id: int, bot_username: str = ""):
        return storage.get_primary_chat_binding(
            profile_id, bot_username=bot_username
        ) or storage.get_primary_chat_binding(profile_id)

    def _load_cached_page_state(
        request: Request,
        *,
        include_chats: bool = False,
        include_profile_state: bool = True,
    ) -> dict:
        active_profile = _get_request_profile(request)
        chats = []
        external_account = None
        profile_state = {
            "active_profile": None,
            "external_account": None,
            "payload": {},
            "current_sect_feature": None,
            "sect_chat": None,
            "sect_session": None,
            "lingxiao_state": None,
            "yinluo_state": None,
            "huangfeng_state": None,
        }
        if active_profile:
            _sync_env_binding(active_profile.id, active_profile.telegram_user_id)
            if include_profile_state:
                profile_state = _load_profile_card_state(
                    active_profile, refresh_external=False
                )
                active_profile = profile_state["active_profile"]
                external_account = profile_state["external_account"]
            else:
                external_account = storage.get_external_account(
                    active_profile.id, ASC_PROVIDER
                )
                profile_state["active_profile"] = active_profile
                profile_state["external_account"] = external_account
            if include_chats:
                chats = storage.list_chat_bindings(active_profile.id)
        return {
            "active_profile": active_profile,
            "external_account": external_account,
            "chats": chats,
            "profile_state": profile_state,
        }

    async def _background_refresh_external_profiles() -> None:
        while True:
            try:
                await asyncio.to_thread(_sync_global_reference_data_if_needed)
                profiles = storage.list_profiles()
                for profile in profiles:
                    if not profile.telegram_verified_at:
                        continue
                    external_account = storage.get_external_account(
                        profile.id, ASC_PROVIDER
                    )
                    if is_external_account_expired(external_account):
                        continue
                    if not _should_refresh_cultivator_payload(
                        profile, external_account
                    ):
                        continue
                    await asyncio.to_thread(_refresh_cultivator_payload, profile.id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Background external profile refresh failed")
            await asyncio.sleep(EXTERNAL_REFRESH_LOOP_SECONDS)

    def _refresh_cultivator_payload(profile_id: int) -> dict:
        profile = storage.get_profile(profile_id)
        if not profile:
            return {}
        external_account = storage.get_external_account(profile_id, ASC_PROVIDER) or {}
        cookie_text = (
            external_account.get("cookie_text")
            or get_effective_external_cookie(storage)
        ).strip()
        identifiers = get_cultivator_lookup_candidates(profile)
        if not cookie_text or not identifiers:
            return read_cached_external_payload(storage, profile_id, ASC_PROVIDER)
        try:
            payload = sync_external_account(
                storage, profile_id, cookie_text=cookie_text
            )
            if _is_admin_profile(profile):
                _sync_global_reference_data_if_needed()
            _sync_profile_from_cultivator(storage, profile_id, payload)
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:
            mark_external_account_failure(
                storage, profile_id, exc, cookie_text=cookie_text
            )
            return read_cached_external_payload(storage, profile_id, ASC_PROVIDER)

    def _get_sect_feature_by_name(sect_name: str) -> Optional[dict]:
        normalized = _normalize_sect_name_text(sect_name)
        if not normalized:
            return None
        for feature in SECT_FEATURES:
            if feature["name"] in normalized:
                return feature
        return None

    def _resolve_current_sect_feature(profile) -> Optional[dict]:
        if not profile or not profile.sect_name:
            return None
        return _get_sect_feature_by_name(profile.sect_name)

    def _build_sect_action_command(action: dict, form_data) -> str:
        command_text = str(action.get("command") or "").strip()
        if command_text:
            return command_text
        template = str(action.get("template") or "").strip()
        if not template:
            raise HTTPException(status_code=400, detail="Sect action template missing")
        values = {}
        for field in action.get("fields") or []:
            field_name = str(field.get("name") or "").strip()
            if not field_name:
                continue
            raw_value = str(form_data.get(field_name) or "").strip()
            if field.get("required", True) and not raw_value:
                raise HTTPException(
                    status_code=400,
                    detail=f"{field.get('label') or field_name} is required",
                )
            if field.get("type") == "select":
                allowed_values = {
                    str(option.get("value") or "").strip()
                    for option in field.get("options") or []
                }
                if raw_value and allowed_values and raw_value not in allowed_values:
                    raise HTTPException(
                        status_code=400, detail="Invalid sect action option"
                    )
            values[field_name] = raw_value
        return template.format(**values).strip()

    def _load_profile_card_state(active_profile, refresh_external: bool = True) -> dict:
        if not active_profile:
            return {
                "active_profile": None,
                "external_account": None,
                "payload": {},
                "cultivation_session": None,
                "rift_failure_state": None,
                "current_sect_feature": None,
                "sect_chat": None,
                "sect_session": None,
                "lingxiao_state": None,
                "yinluo_state": None,
            }

        profile = storage.get_profile(active_profile.id) or active_profile
        external_account = storage.get_external_account(profile.id, ASC_PROVIDER)
        should_refresh = refresh_external and _should_refresh_cultivator_payload(
            profile, external_account
        )
        payload = (
            _refresh_cultivator_payload(profile.id)
            if should_refresh
            else read_cached_external_payload(storage, profile.id, ASC_PROVIDER)
        )
        profile = storage.get_profile(profile.id) or profile
        external_account = storage.get_external_account(profile.id, ASC_PROVIDER)
        cultivation_chat = _get_primary_command_chat(
            profile.id, fanren_game.FANREN_BOT_USERNAME
        )
        cultivation_session = (
            storage.get_cultivation_session(
                cultivation_chat.chat_id, profile_id=profile.id
            )
            if cultivation_chat
            else None
        )
        rift_failure_state = _build_rift_failure_profile_state(
            payload, cultivation_session
        )
        current_sect_feature = _resolve_current_sect_feature(profile)
        sect_chat = storage.get_primary_chat_binding(
            profile.id, bot_username=sect_game.SECT_BOT_USERNAME
        ) or storage.get_primary_chat_binding(profile.id)
        sect_session = (
            storage.get_sect_session(sect_chat.chat_id, profile_id=profile.id)
            if sect_chat
            else None
        )
        lingxiao_state = None
        yinluo_state = None
        huangfeng_state = None
        if current_sect_feature and current_sect_feature["name"] == "凌霄宫":
            if sect_chat:
                db = CompatDb(storage)
                try:
                    sect_game.ensure_tables(db)
                    sect_session, _ = sect_game.sync_lingxiao_trial_state(
                        storage,
                        db,
                        profile.id,
                        sect_chat.chat_id,
                        payload=payload,
                    )
                finally:
                    db.close()
            lingxiao_state = sect_game.build_lingxiao_view(
                payload,
                session=sect_session,
                sect_position=profile.sect_position,
            )
        if current_sect_feature and current_sect_feature["name"] == "阴罗宗":
            banner_reply = None
            summon_shadow_reply = None
            if sect_chat:
                db = CompatDb(storage)
                try:
                    sect_game.ensure_tables(db)
                    sect_session, yinluo_state = sect_game.sync_yinluo_state(
                        storage,
                        db,
                        profile.id,
                        sect_chat.chat_id,
                        payload=payload,
                    )
                finally:
                    db.close()
            if yinluo_state is None:
                yinluo_state = sect_game.build_yinluo_view(
                    payload,
                    session=sect_session,
                    banner_text=(banner_reply or {}).get("text") or "",
                    summon_shadow_reply=summon_shadow_reply,
                )
            else:
                yinluo_state = sect_game.build_yinluo_view(
                    payload,
                    session=sect_session,
                    banner_text=(banner_reply or {}).get("text") or "",
                    summon_shadow_reply=summon_shadow_reply,
                )
        if current_sect_feature and current_sect_feature["name"] == "黄枫谷":
            if sect_chat:
                db = CompatDb(storage)
                try:
                    sect_game.ensure_tables(db)
                    sect_session, huangfeng_state = sect_game.sync_huangfeng_state(
                        storage,
                        db,
                        profile.id,
                        sect_chat.chat_id,
                        payload=payload,
                    )
                finally:
                    db.close()
            if huangfeng_state is None:
                huangfeng_state = sect_game.build_huangfeng_view(
                    payload,
                    session=sect_session,
                )

        return {
            "active_profile": profile,
            "external_account": external_account,
            "payload": payload,
            "cultivation_session": cultivation_session,
            "rift_failure_state": rift_failure_state,
            "current_sect_feature": current_sect_feature,
            "sect_chat": sect_chat,
            "sect_session": sect_session,
            "lingxiao_state": lingxiao_state,
            "yinluo_state": yinluo_state,
            "huangfeng_state": huangfeng_state,
        }

    def _get_or_create_profile_for_telegram(
        telegram_user_id: str, telegram_username: str, telegram_first_name: str
    ):
        profile = storage.get_profile_by_telegram_user_id(telegram_user_id)
        if profile:
            return profile
        base_name = telegram_username or telegram_first_name or "tg"
        profile_name = f"{base_name}-{telegram_user_id[-6:]}"
        profile = storage.create_profile(name=profile_name, activate=False)
        storage.ensure_module_settings(profile.id, module_registry.list_modules())
        return profile

    def _get_authenticated_profile(request: Request):
        session_token = request.cookies.get(APP_SESSION_COOKIE, "")
        return storage.get_profile_by_session_token(session_token)

    def _list_session_profiles(request: Request) -> list:
        return storage.list_profiles_by_session_token(
            request.cookies.get(APP_SESSION_COOKIE, "")
        )

    def _profile_belongs_to_session(request: Request, profile_id: int) -> bool:
        return any(
            profile.id == int(profile_id) for profile in _list_session_profiles(request)
        )

    async def _discover_authorized_account(request: Request) -> Optional[dict]:
        session_names = []
        seen = set()

        def _push_session_name(value: str) -> None:
            normalized = str(value or "").strip()
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            session_names.append(normalized)

        for profile in _list_session_profiles(request):
            _push_session_name(profile.telegram_session_name)
        for profile in storage.list_profiles():
            _push_session_name(profile.telegram_session_name)
        _push_session_name(_login_session_name())
        _push_session_name(settings.telegram_session_name)

        for session_name in session_names:
            if await has_authorized_session(session_name, allow_fallback=False):
                return await get_authorized_account_info(
                    session_name, allow_fallback=False
                )
        return None

    def _finalize_telegram_login(request: Request, account: dict):
        telegram_user_id = str(account.get("id") or "").strip()
        telegram_username = (account.get("username") or "").strip()
        telegram_first_name = (account.get("first_name") or "").strip()
        telegram_phone = (account.get("phone") or "").strip()
        telegram_session_name = (
            account.get("session_name") or _login_session_name()
        ).strip()
        profile = _get_or_create_profile_for_telegram(
            telegram_user_id, telegram_username, telegram_first_name
        )
        storage.bind_profile_telegram_account(
            profile.id,
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            telegram_phone=telegram_phone,
            telegram_session_name=telegram_session_name,
        )
        storage.activate_profile(profile.id)
        _sync_env_binding(profile.id, telegram_user_id)
        default_cookie = get_effective_external_cookie(storage)
        if default_cookie:
            try:
                _connect_external_cookie(profile.id, default_cookie)
            except Exception as exc:
                mark_external_account_failure(
                    storage, profile.id, exc, cookie_text=default_cookie
                )
                return _sign_in_profile(
                    request,
                    profile.id,
                    redirect_url="/login?error="
                    + quote_plus("默认天机阁会话已失效，请重新获取 session 粘贴导入"),
                )
        return _sign_in_profile(
            request,
            profile.id,
            redirect_url="/login?success="
            + quote_plus("TG 登录成功，已自动绑定当前账号"),
        )

    def _switch_session_profile(
        request: Request, profile_id: int, redirect_url: str = "/profile"
    ) -> RedirectResponse:
        session_token = request.cookies.get(APP_SESSION_COOKIE, "")
        profile = storage.set_current_profile_by_session_token(
            session_token, profile_id
        )
        if not profile:
            raise HTTPException(
                status_code=404, detail="Profile not available in session"
            )
        storage.activate_profile(profile.id)
        _sync_env_binding(profile.id, profile.telegram_user_id)
        storage.request_sect_refresh(profile.id, cooldown_seconds=0)
        return RedirectResponse(url=redirect_url or "/profile", status_code=303)

    @application.middleware("http")
    async def require_app_session(request: Request, call_next):
        profile = _get_authenticated_profile(request)
        request.state.auth_profile = profile
        if not profile and not _is_public_path(request.url.path):
            return RedirectResponse(url="/login", status_code=303)
        if (
            profile
            and not _is_public_path(request.url.path)
            and _is_external_session_expired_for_profile(profile.id)
        ):
            return _build_tianji_login_redirect()
        return await call_next(request)

    @application.on_event("startup")
    async def on_startup() -> None:
        storage.init_schema()
        storage.maybe_cleanup_bound_messages(min_interval_seconds=0)
        active_profile = storage.get_active_profile()
        if active_profile:
            _sync_env_binding(active_profile.id, active_profile.telegram_user_id)
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            sect_game.ensure_tables(db)
        finally:
            db.close()
        application.state.external_refresh_task = asyncio.create_task(
            _background_refresh_external_profiles()
        )

    @application.on_event("shutdown")
    async def on_shutdown() -> None:
        task = getattr(application.state, "external_refresh_task", None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @application.get("/login", response_class=HTMLResponse)
    async def login_page(
        request: Request, error: str = "", success: str = ""
    ) -> HTMLResponse:
        auth_profile = getattr(request.state, "auth_profile", None)
        if not auth_profile and not error and not success:
            try:
                account = await _discover_authorized_account(request)
                if account:
                    return _finalize_telegram_login(request, account)
            except Exception:
                logger.exception("Auto Telegram login bind failed")
        active_profile = auth_profile
        session_profiles = _list_session_profiles(request)
        external_account = None
        if active_profile:
            external_account = storage.get_external_account(
                active_profile.id, ASC_PROVIDER
            )
        telegram_account = None
        has_telegram_session = False
        if active_profile and active_profile.telegram_verified_at:
            telegram_account = {
                "id": active_profile.telegram_user_id,
                "username": active_profile.telegram_username,
                "first_name": active_profile.name,
                "phone": active_profile.telegram_phone,
                "session_name": active_profile.telegram_session_name,
            }
            has_telegram_session = True
        login_challenge = None
        raw_challenge_id = request.cookies.get(TG_LOGIN_CHALLENGE_COOKIE, "")
        if raw_challenge_id.isdigit():
            login_challenge = storage.get_telegram_login_challenge(
                int(raw_challenge_id)
            )
        me_payload = {}
        if external_account and external_account.get("me_json"):
            try:
                me_payload = json.loads(external_account.get("me_json") or "{}")
            except json.JSONDecodeError:
                me_payload = {}
        external_session_notice = _build_external_session_notice(external_account)
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "app_name": settings.app_name,
                "active_profile": active_profile,
                "external_account": external_account,
                "external_character_count": len(me_payload.get("characters") or []),
                "login_error": error,
                "login_success": success,
                "has_telegram_session": has_telegram_session,
                "login_challenge": login_challenge,
                "telegram_account": telegram_account,
                "session_profiles": session_profiles,
                "is_admin_profile": _is_admin_profile(active_profile),
                "has_global_external_cookie": bool(_get_global_market_cookie()),
                "external_session_notice": external_session_notice,
                "format_timestamp": fanren_game.format_timestamp,
            },
        )

    @application.post("/auth/telegram/local-login")
    async def local_telegram_login(request: Request) -> RedirectResponse:
        active_profile = getattr(request.state, "auth_profile", None)
        if active_profile and active_profile.telegram_verified_at:
            return _finalize_telegram_login(
                request,
                {
                    "id": active_profile.telegram_user_id,
                    "username": active_profile.telegram_username,
                    "first_name": active_profile.name,
                    "phone": active_profile.telegram_phone,
                    "session_name": active_profile.telegram_session_name
                    or _login_session_name(),
                },
            )
        try:
            account = await _discover_authorized_account(request)
            if not account:
                raise RuntimeError(
                    "当前没有可直接复用的 Telegram 会话，请先走手机号验证码登录"
                )
        except Exception as exc:
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )
        return _finalize_telegram_login(request, account)

    @application.post("/auth/telegram/start")
    async def start_telegram_login(phone: str = Form(...)) -> RedirectResponse:
        try:
            session_name = _login_session_name_for_phone(phone)
            result = await send_login_code(phone, session_name)
            challenge_id = storage.create_telegram_login_challenge(
                phone=result.get("phone") or "",
                phone_code_hash=result.get("phone_code_hash") or "",
                session_name=result.get("session_name") or session_name,
            )
            response = RedirectResponse(
                url="/login?success="
                + quote_plus("验证码已发送，请输入验证码完成登录"),
                status_code=303,
            )
            response.set_cookie(
                TG_LOGIN_CHALLENGE_COOKIE,
                str(challenge_id),
                httponly=True,
                samesite="lax",
                secure=False,
                max_age=600,
            )
            return response
        except Exception as exc:
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )

    @application.post("/auth/telegram/verify")
    async def verify_telegram_login(
        request: Request, code: str = Form(...)
    ) -> RedirectResponse:
        raw_challenge_id = request.cookies.get(TG_LOGIN_CHALLENGE_COOKIE, "")
        if not raw_challenge_id.isdigit():
            return RedirectResponse(
                url="/login?error=" + quote_plus("请先发送 Telegram 验证码"),
                status_code=303,
            )
        challenge = storage.get_telegram_login_challenge(int(raw_challenge_id))
        if not challenge:
            return RedirectResponse(
                url="/login?error=" + quote_plus("登录挑战已失效，请重新发送验证码"),
                status_code=303,
            )
        try:
            result = await verify_login_code(
                challenge.get("phone") or "",
                code,
                challenge.get("phone_code_hash") or "",
                challenge.get("session_name") or _login_session_name(),
            )
            if result.get("requires_password"):
                storage.update_telegram_login_challenge_status(
                    challenge["id"], "password_required"
                )
                return RedirectResponse(
                    url="/login?success="
                    + quote_plus("检测到二步验证，请输入 Telegram 密码"),
                    status_code=303,
                )
            storage.delete_telegram_login_challenge(challenge["id"])
            response = _finalize_telegram_login(request, result.get("account") or {})
            response.delete_cookie(TG_LOGIN_CHALLENGE_COOKIE)
            return response
        except Exception as exc:
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )

    @application.post("/auth/telegram/password")
    async def verify_telegram_password(
        request: Request, password: str = Form(...)
    ) -> RedirectResponse:
        raw_challenge_id = request.cookies.get(TG_LOGIN_CHALLENGE_COOKIE, "")
        if not raw_challenge_id.isdigit():
            return RedirectResponse(
                url="/login?error=" + quote_plus("请先发送 Telegram 验证码"),
                status_code=303,
            )
        challenge = storage.get_telegram_login_challenge(int(raw_challenge_id))
        if not challenge:
            return RedirectResponse(
                url="/login?error=" + quote_plus("登录挑战已失效，请重新发送验证码"),
                status_code=303,
            )
        try:
            account = await verify_login_password(
                password,
                challenge.get("session_name") or _login_session_name(),
            )
            storage.delete_telegram_login_challenge(challenge["id"])
            response = _finalize_telegram_login(request, account)
            response.delete_cookie(TG_LOGIN_CHALLENGE_COOKIE)
            return response
        except Exception as exc:
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )

    @application.post("/auth/telegram/logout")
    async def telegram_logout(request: Request) -> RedirectResponse:
        profile = getattr(request.state, "auth_profile", None)
        session_token = request.cookies.get(APP_SESSION_COOKIE, "")
        session_name = (
            profile.telegram_session_name if profile else ""
        ) or _login_session_name()
        try:
            await logout_account(session_name)
        except Exception:
            pass
        if profile:
            storage.clear_profile_telegram_account(profile.id)
        has_remaining_profiles = (
            storage.remove_profile_from_session_token(session_token, profile.id)
            if profile and session_token
            else False
        )
        if has_remaining_profiles:
            next_profile = storage.get_profile_by_session_token(session_token)
            if next_profile:
                storage.activate_profile(next_profile.id)
                _sync_env_binding(next_profile.id, next_profile.telegram_user_id)
            response = RedirectResponse(
                url="/login?success="
                + quote_plus("当前 TG 账号已退出，已切换到浏览器会话中的其他档案"),
                status_code=303,
            )
        else:
            storage.revoke_app_session(session_token)
            response = RedirectResponse(
                url="/login?success="
                + quote_plus("TG 账号已退出，请重新走标准登录流程"),
                status_code=303,
            )
            response.delete_cookie(APP_SESSION_COOKIE)
        response.delete_cookie(TG_LOGIN_CHALLENGE_COOKIE)
        return response

    @application.post("/auth/external/connect")
    async def connect_external(
        request: Request, cookie_text: str = Form("")
    ) -> RedirectResponse:
        try:
            profile = _get_request_profile(request)
            if not profile:
                raise RuntimeError("请先完成 Telegram Web 登录")
            normalized_cookie_text = (cookie_text or "").strip()
            if _is_admin_profile(profile):
                if not normalized_cookie_text:
                    raise RuntimeError("管理员提交的 Cookie 不能为空")
                _connect_external_cookie(profile.id, normalized_cookie_text)
            else:
                if not _get_global_market_cookie():
                    raise RuntimeError("管理员尚未配置可用的天机阁 Cookie")
                _connect_external_cookie(profile.id, "")
        except AscAuthError as exc:
            profile = _get_request_profile(request)
            if profile:
                mark_external_account_failure(
                    storage, profile.id, exc, cookie_text=cookie_text
                )
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )
        except Exception as exc:
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )
        return _sign_in_profile(
            request,
            profile.id,
            redirect_url="/login?success="
            + quote_plus("天机阁登录成功，已同步人物卡并恢复自动调度"),
        )

    @application.post("/auth/external/logout")
    async def external_logout(request: Request) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            return RedirectResponse(url="/login", status_code=303)
        storage.clear_external_account(profile.id, ASC_PROVIDER)
        if _is_admin_profile(profile):
            storage.clear_external_cookie_override()
        return RedirectResponse(
            url="/login?success=" + quote_plus("天机阁登录已退出"),
            status_code=303,
        )

    @application.post("/logout")
    async def logout(request: Request) -> RedirectResponse:
        storage.revoke_app_session(request.cookies.get(APP_SESSION_COOKIE, ""))
        response = RedirectResponse(url="/login", status_code=303)
        response.delete_cookie(APP_SESSION_COOKIE)
        return response

    @application.post("/auth/external/refresh")
    async def refresh_external_session(request: Request) -> RedirectResponse:
        profile = getattr(request.state, "auth_profile", None)
        if not profile:
            return RedirectResponse(url="/login", status_code=303)
        external_account = storage.get_external_account(profile.id, ASC_PROVIDER)
        cookie_text = get_effective_external_cookie(storage)
        if not cookie_text:
            return RedirectResponse(url="/login", status_code=303)
        try:
            _connect_external_cookie(profile.id, cookie_text)
            return RedirectResponse(
                url=f"/login?success={quote_plus('天机阁会话验证成功，已恢复自动调度并同步人物卡')}",
                status_code=303,
            )
        except Exception as exc:
            mark_external_account_failure(
                storage, profile.id, exc, cookie_text=cookie_text
            )
            return RedirectResponse(
                url=f"/login?error={quote_plus(str(exc))}", status_code=303
            )

    @application.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        modules = [
            module
            for module in module_registry.list_modules()
            if module.key in visible_module_keys
        ]
        page_state = _load_cached_page_state(request, include_chats=True)
        active_profile = page_state["active_profile"]
        chats = page_state["chats"]
        cultivation_results = []
        cultivation_session = None
        profile_state = page_state["profile_state"]
        payload = profile_state.get("payload") or {}
        character_state = _build_character_view(payload)
        sect_session = profile_state["sect_session"]
        current_sect_feature = profile_state["current_sect_feature"]
        lingxiao_state = profile_state["lingxiao_state"]
        yinluo_state = profile_state["yinluo_state"]
        huangfeng_state = profile_state.get("huangfeng_state")
        sect_chat = profile_state["sect_chat"]
        external_account = page_state["external_account"]
        if active_profile:
            storage.ensure_module_settings(active_profile.id, modules)
            cultivation_results = [
                _build_cultivation_result_view(result)
                for result in storage.list_cultivation_results(
                    active_profile.id, limit=5
                )
            ]
            primary_chat = _get_primary_command_chat(
                active_profile.id, fanren_game.FANREN_BOT_USERNAME
            )
            if primary_chat:
                cultivation_session = storage.get_cultivation_session(
                    primary_chat.chat_id, profile_id=active_profile.id
                )
        external_session_notice = _build_external_session_notice(external_account)
        shared_template_context = _build_shared_template_context(active_profile)
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "app_name": settings.app_name,
                "modules": modules,
                "active_profile": active_profile,
                "chats": chats,
                "cultivation_results": cultivation_results,
                "cultivation_session": cultivation_session,
                "character_state": character_state,
                "sect_session": sect_session,
                "sect_features": SECT_FEATURES,
                "current_sect_feature": current_sect_feature,
                "lingxiao_state": lingxiao_state,
                "format_timestamp": fanren_game.format_timestamp,
                "now_ts": fanren_game.time.time(),
                "external_account": external_account,
                "external_session_notice": external_session_notice,
                **shared_template_context,
            },
        )

    @application.get("/profile", response_class=HTMLResponse)
    async def profile_page(request: Request) -> HTMLResponse:
        profiles = storage.list_profiles_by_session_token(
            request.cookies.get(APP_SESSION_COOKIE, "")
        )
        page_state = _load_cached_page_state(request, include_chats=True)
        active_profile = page_state["active_profile"]
        chats = page_state["chats"]
        external_account = page_state["external_account"]
        profile_state = page_state["profile_state"]
        character_state = _build_character_view(profile_state.get("payload") or {})
        rift_failure_state = profile_state.get("rift_failure_state")
        external_session_notice = _build_external_session_notice(external_account)
        shared_template_context = _build_shared_template_context(active_profile)
        return templates.TemplateResponse(
            request,
            "profile.html",
            {
                "app_name": settings.app_name,
                "profiles": profiles,
                "active_profile": active_profile,
                "chats": chats,
                "format_timestamp": fanren_game.format_timestamp,
                "now_ts": fanren_game.time.time(),
                "external_account": external_account,
                "character_state": character_state,
                "rift_failure_state": rift_failure_state,
                "external_session_notice": external_session_notice,
                **shared_template_context,
            },
        )

    @application.post("/profiles/{profile_id}/switch")
    async def switch_profile(
        request: Request,
        profile_id: int,
        redirect_to: str = Form("/profile"),
    ) -> RedirectResponse:
        return _switch_session_profile(request, profile_id, redirect_to)

    @application.get("/messages", response_class=HTMLResponse)
    async def messages_page(
        request: Request,
        chat_id: str = "",
        limit: int = 200,
        q: str = "",
        focus_msg_id: str = "",
    ) -> HTMLResponse:
        page_state = _load_cached_page_state(request, include_chats=True)
        active_profile = page_state["active_profile"]
        if not _is_admin_profile(active_profile):
            raise HTTPException(
                status_code=403, detail="Only admin can access messages"
            )
        chats = page_state["chats"]
        external_account = page_state["external_account"]
        safe_limit = max(20, min(int(limit or 200), 500))
        normalized_chat_id_text = str(chat_id or "").strip()
        selected_chat_id = (
            int(normalized_chat_id_text)
            if normalized_chat_id_text.lstrip("-").isdigit()
            else None
        )
        search_query = str(q or "").strip()
        normalized_focus_msg_id = str(focus_msg_id or "").strip()
        focused_message_id = (
            int(normalized_focus_msg_id) if normalized_focus_msg_id.isdigit() else None
        )
        profile_id = active_profile.id if active_profile else None
        if selected_chat_id is not None and focused_message_id is not None:
            messages = storage.get_bound_message_context(
                chat_id=selected_chat_id,
                message_id=focused_message_id,
                profile_id=profile_id,
            )
        else:
            messages = storage.list_bound_messages(
                profile_id=profile_id,
                chat_id=selected_chat_id,
                search_query=search_query,
                limit=safe_limit,
            )
        for message in messages:
            reply_preview = ""
            if message.get("reply_to_msg_id"):
                reply_message = storage.get_bound_message(
                    message.get("chat_id") or 0, int(message["reply_to_msg_id"])
                )
                reply_preview = ((reply_message or {}).get("text") or "").strip()[:160]
            message["reply_preview"] = reply_preview
            message["is_focused"] = bool(
                focused_message_id is not None
                and int(message.get("message_id") or 0) == focused_message_id
                and int(message.get("chat_id") or 0) == int(selected_chat_id or 0)
            )
        shared_template_context = _build_shared_template_context(active_profile)
        return templates.TemplateResponse(
            request,
            "messages.html",
            {
                "app_name": settings.app_name,
                "active_profile": active_profile,
                "chats": chats,
                "messages": messages,
                "selected_chat_id": selected_chat_id,
                "limit": safe_limit,
                "search_query": search_query,
                "search_query_qs": quote_plus(search_query) if search_query else "",
                "focused_message_id": focused_message_id,
                "format_timestamp": fanren_game.format_timestamp,
                "external_session_notice": _build_external_session_notice(
                    external_account
                ),
                **shared_template_context,
            },
        )

    @application.get("/modules/{module_key}", response_class=HTMLResponse)
    async def module_detail(
        request: Request,
        module_key: str,
        page: int = 1,
        dungeon_key: str = "",
        q: str = "",
        q_exchange: str = "",
        sort: str = "",
        inv_page: int = 1,
    ) -> HTMLResponse:
        if module_key not in visible_module_keys:
            raise HTTPException(status_code=404, detail="Module not available")
        module = module_registry.get_module(module_key)
        if not module:
            raise HTTPException(status_code=404, detail="Module not found")
        page_state = _load_cached_page_state(request)
        active_profile = page_state["active_profile"]
        module_setting = None
        cultivation_results = []
        cultivation_session = None
        command_chat = None
        profile_state = page_state["profile_state"]
        sect_session = profile_state["sect_session"]
        current_sect_feature = profile_state["current_sect_feature"]
        lingxiao_state = profile_state["lingxiao_state"]
        yinluo_state = profile_state["yinluo_state"]
        huangfeng_state = profile_state.get("huangfeng_state")
        sect_chat = profile_state["sect_chat"]
        external_account = page_state["external_account"]
        cultivation_page = max(int(page or 1), 1)
        cultivation_page_size = 4
        cultivation_total = 0
        cultivation_total_pages = 1
        cultivation_page_numbers = [1]
        payload = {}
        game_items_dict = storage.get_game_items()
        character_state = _build_character_view(payload)
        taiyi_state = _build_taiyi_view(payload)
        other_play_state = _build_other_play_view(payload)
        divination_batch_state = _build_divination_batch_view(None)
        dongfu_state = _build_dongfu_view(payload, game_items_dict)
        stock_state = {
            "rows": [],
            "count": 0,
            "top_gainer": None,
            "top_loser": None,
            "latest_updated_at": 0,
            "latest_updated_display": "-",
            "latest_account_text": "",
            "latest_account_time_display": "-",
            "latest_task_text": "",
            "latest_task_time_display": "-",
            "tracked_stocks": [],
            "tracked_codes": [],
        }
        selected_dungeon = _get_dungeon_definition(dungeon_key)
        dungeon_command_buttons = _extract_dungeon_command_buttons(selected_dungeon)
        dungeon_cleanup_targets = []
        dungeon_messages = []
        market_listings = []
        market_query = str(q or "").strip()
        market_exchange_query = str(q_exchange or "").strip()
        market_sort = str(sort or "").strip().lower()
        market_page = max(int(page or 1), 1)
        market_page_size = 20
        market_total = 0
        market_total_pages = 1
        market_page_numbers = [1]
        inventory_trade_options = []
        inventory_page = max(int(inv_page or 1), 1)
        inventory_page_size = 24
        inventory_total = 0
        inventory_total_pages = 1
        inventory_page_numbers = [1]
        inventory_all_items = []
        sect_recent_reply_text = ""
        equipped_artifact_details = "未装备法宝"
        other_opponent_options = []
        sect_daily_state = {
            "last_check_in_time": 0,
            "checked_in_today": False,
            "consecutive_check_in_days": 0,
            "teach_count": 0,
            "teach_progress_text": f"0/{sect_game.SECT_DAILY_TEACH_LIMIT}",
        }
        active_badge_text = "-"
        recipes_known_text = "-"
        formations_known_text = "-"
        learned_techniques_text = "-"
        equipped_artifact_name = ""
        recipes_known_entries = []
        tianji_encounter_state = {
            "strategy": "未知",
            "today_count": "0/2",
            "last_encounter": "暂无",
            "records": [],
        }
        companion_state = {
            "available": False,
            "relation_title": "侍妾同行",
            "name": "-",
            "status": "-",
            "affection": 0,
            "heart_demon_value": "-",
            "current_vow": "无",
            "sworn_at_display": "-",
            "divination_chain": "-",
            "abyss_guard": "-",
            "dream_seek_display": "接口未提供",
            "dream_seek_cooldown_target": 0.0,
            "heart_tribulation_display": "接口未提供",
            "heart_tribulation_cooldown_target": 0.0,
            "divination_chain_display": "接口未提供",
            "divination_chain_cooldown_target": 0.0,
            "fragment_progress": "0/4",
            "fragment_detail": "东0 / 南0 / 西0 / 北0",
            "heart_tribulation_command": ".共历心劫",
        }
        companion_auto_state = {
            "dream_seek": _build_companion_auto_view(None, "dream_seek"),
            "divination_chain": _build_companion_auto_view(None, "divination_chain"),
        }
        if active_profile:
            storage.ensure_module_settings(
                active_profile.id, module_registry.list_modules()
            )
            module_setting = storage.get_module_setting(active_profile.id, module_key)
            payload = profile_state.get("payload") or {}
            character_state = _build_character_view(payload)
            taiyi_state = _build_taiyi_view(payload)
            other_play_state = _build_other_play_view(payload)
            dongfu_state = _build_dongfu_view(payload, game_items_dict)
            if module_key in {"sect", "inventory"}:
                sect_daily_state = _merge_sect_daily_view_with_session(
                    _build_sect_daily_view(payload), sect_session
                )
                active_badge_text = _payload_name_summary(
                    payload.get("active_badge"), game_items_dict
                )
                equipped_artifact_name = _equipped_artifact_names_text(payload)
                recipes_known_entries = [
                    {
                        **entry,
                        "craft_name": _recipe_craft_name(entry["name"]),
                    }
                    for entry in _payload_named_entries(
                        payload.get("recipes_known"), game_items_dict
                    )
                ]
                recipes_known_text = (
                    "、".join(entry["name"] for entry in recipes_known_entries)
                    if recipes_known_entries
                    else "-"
                )
                formations_known_text = _payload_name_summary(
                    payload.get("formations_known"), game_items_dict
                )
                learned_techniques_text = _payload_name_summary(
                    payload.get("learned_techniques"), game_items_dict
                )
                equipped_artifact_details = _build_equipped_artifact_details(payload)
            if module_key == "sect":
                sect_recent_reply_text = _build_sect_recent_reply_text(
                    storage,
                    active_profile.id,
                    sect_chat,
                    current_sect_feature,
                    active_profile,
                    fallback_text=(sect_session or {}).get("last_summary") or "",
                )
            if module_key == "cultivation":
                cultivation_total = storage.count_cultivation_results(
                    active_profile.id, since_seconds=86400
                )
                cultivation_total_pages = max(
                    (cultivation_total + cultivation_page_size - 1)
                    // cultivation_page_size,
                    1,
                )
                cultivation_page = min(cultivation_page, cultivation_total_pages)
                cultivation_page_numbers = _build_pagination_numbers(
                    cultivation_page, cultivation_total_pages
                )
                cultivation_results = [
                    _build_cultivation_result_view(result)
                    for result in storage.list_cultivation_results(
                        active_profile.id,
                        limit=cultivation_page_size,
                        offset=(cultivation_page - 1) * cultivation_page_size,
                        since_seconds=86400,
                    )
                ]
                command_chat = _get_primary_command_chat(
                    active_profile.id, fanren_game.FANREN_BOT_USERNAME
                )
                if command_chat:
                    cultivation_session = storage.get_cultivation_session(
                        command_chat.chat_id, profile_id=active_profile.id
                    )
            elif module_key in {"other", "estate", "dungeon", "stock"}:
                command_chat = _get_primary_command_chat(
                    active_profile.id, fanren_game.FANREN_BOT_USERNAME
                )
                if module_key == "other":
                    divination_batch_state = _build_divination_batch_view(
                        storage.get_active_divination_batch(
                            active_profile.id,
                            chat_id=command_chat.chat_id if command_chat else None,
                        )
                        or storage.get_latest_divination_batch(
                            active_profile.id,
                            chat_id=command_chat.chat_id if command_chat else None,
                        )
                    )
                    other_opponent_options = _build_recent_player_options(
                        storage,
                        command_chat.chat_id if command_chat else None,
                        profile_id=active_profile.id,
                        exclude_usernames=[
                            getattr(active_profile, "telegram_username", "")
                        ],
                    )
                    tianji_encounter_state = _build_tianji_encounter_state(
                        storage,
                        active_profile.id,
                        command_chat.chat_id if command_chat else None,
                    )
                    command_sender_text = str(
                        getattr(command_chat, "telegram_user_id", "")
                        or getattr(active_profile, "telegram_user_id", "")
                        or ""
                    ).strip()
                    companion_reply = storage.get_latest_bot_reply_for_command(
                        command_chat.chat_id if command_chat else 0,
                        ".我的侍妾",
                        profile_id=active_profile.id,
                        thread_id=command_chat.thread_id if command_chat else None,
                        sender_id=(
                            int(command_sender_text)
                            if command_sender_text.isdigit()
                            else None
                        ),
                        sender_username=(
                            getattr(active_profile, "telegram_username", "") or ""
                        ),
                    )
                    companion_state = _build_companion_view(
                        payload,
                        str((companion_reply or {}).get("text") or "").strip(),
                    )
                    companion_auto_state = {
                        feature_key: _build_companion_auto_view(
                            storage.get_companion_auto_task(
                                active_profile.id,
                                command_chat.chat_id if command_chat else 0,
                                feature_key,
                            ),
                            feature_key,
                        )
                        for feature_key in COMPANION_AUTO_FEATURES
                    }
                if module_key == "estate":
                    command_sender_text = str(
                        getattr(command_chat, "telegram_user_id", "")
                        or getattr(active_profile, "telegram_user_id", "")
                        or ""
                    ).strip()
                    dongfu_state["messages"] = _build_estate_reply_messages(
                        storage,
                        active_profile.id,
                        command_chat.chat_id if command_chat else None,
                        thread_id=command_chat.thread_id if command_chat else None,
                        sender_id=(
                            int(command_sender_text)
                            if command_sender_text.isdigit()
                            else None
                        ),
                        sender_username=(
                            getattr(active_profile, "telegram_username", "") or ""
                        ),
                        fallback_messages=dongfu_state.get("messages") or [],
                    )
                if module_key == "dungeon" and command_chat:
                    dungeon_messages = _build_dungeon_messages(
                        storage,
                        command_chat.chat_id,
                        selected_dungeon["key"],
                        profile_id=active_profile.id,
                    )
                    dungeon_cleanup_targets = _extract_dungeon_cleanup_targets(
                        dungeon_messages
                    )
                if module_key == "stock":
                    command_sender_text = str(
                        getattr(command_chat, "telegram_user_id", "")
                        or getattr(active_profile, "telegram_user_id", "")
                        or ""
                    ).strip()
                    stock_state = _build_stock_view(
                        storage,
                        active_profile.id,
                        command_chat.chat_id if command_chat else None,
                        command_chat.thread_id if command_chat else None,
                        command_sender_id=(
                            int(command_sender_text)
                            if command_sender_text.isdigit()
                            else None
                        ),
                        command_sender_username=(
                            getattr(active_profile, "telegram_username", "") or ""
                        ),
                    )
        inventory_materials = {}
        inventory_items = []
        equipped_id = ""
        spirit_stones = 0
        if module_key == "inventory" and active_profile:
            inventory_data = payload.get("inventory") or {}
            raw_materials = inventory_data.get("materials") or {}
            raw_items = inventory_data.get("items") or []
            equipped_id_list = payload.get("equipped_treasure_id")
            equipped_ids = (
                {
                    str(item_id or "").strip()
                    for item_id in (equipped_id_list or [])
                    if str(item_id or "").strip()
                }
                if isinstance(equipped_id_list, list)
                else set()
            )
            equipped_id = (
                equipped_id_list[0]
                if equipped_id_list and isinstance(equipped_id_list, list)
                else ""
            )

            spirit_stones = raw_materials.get("mat_001", 0)

            inventory_trade_options = sorted(
                {
                    name.strip()
                    for name in [
                        *(meta.get("name", "") for meta in game_items_dict.values()),
                        "灵石",
                    ]
                    if name and name.strip()
                }
            )

            command_chat = _get_primary_command_chat(
                active_profile.id, fanren_game.FANREN_BOT_USERNAME
            )

            # Convert materials to items
            for mat_id, count in raw_materials.items():
                if mat_id == "mat_001":
                    continue
                meta = game_items_dict.get(mat_id, {})
                inventory_items.append(
                    {
                        "item_id": mat_id,
                        "name": meta.get("name", mat_id),
                        "description": meta.get("description", ""),
                        "type": _item_type_label("material"),
                        "raw_type": "material",
                        "quantity": count,
                        "durability": None,
                        "max_durability": None,
                    }
                )

            # Map standard items
            for it in raw_items:
                raw_t = it.get("type", "")
                it["raw_type"] = raw_t
                it["type"] = _item_type_label(raw_t)
                inventory_items.append(it)

            # Equip mapping
            for it in inventory_items:
                if str(it.get("item_id") or "").strip() in equipped_ids:
                    it["is_equipped"] = True
            inventory_items.sort(
                key=lambda item: (
                    0 if item.get("is_equipped") else 1,
                    str(item.get("type") or ""),
                    str(item.get("name") or item.get("item_id") or ""),
                )
            )
            inventory_all_items = list(inventory_items)
            inventory_total = len(inventory_all_items)
            inventory_total_pages = max(
                (inventory_total + inventory_page_size - 1) // inventory_page_size,
                1,
            )
            inventory_page = min(inventory_page, inventory_total_pages)
            inventory_page_numbers = _build_pagination_numbers(
                inventory_page, inventory_total_pages
            )
            inventory_start = (inventory_page - 1) * inventory_page_size
            inventory_items = inventory_all_items[
                inventory_start : inventory_start + inventory_page_size
            ]
        if module_key == "market":
            for item in storage.get_marketplace_listings():
                item_id = str(item.get("item_id") or "")
                meta = game_items_dict.get(item_id) or {}
                display_name = str(
                    meta.get("name") or item.get("item_name") or item_id
                ).strip()
                price_preview = _market_price_preview(
                    item.get("price_json"), game_items_dict
                )
                market_listings.append(
                    {
                        **item,
                        "display_name": display_name or item_id,
                        "display_type": _item_type_label(
                            item.get("item_type") or meta.get("type") or "",
                            is_material=bool(item.get("is_material")),
                        ),
                        "display_raw_type": str(item.get("item_type") or "").strip(),
                        "price_text": _format_market_price(
                            item.get("price_json"), game_items_dict
                        ),
                        "price_sort_key": _market_price_sort_key(
                            item.get("price_json"), game_items_dict
                        ),
                        "price_preview_text": price_preview["preview_text"],
                        "price_full_text": price_preview["full_text"],
                        "price_item_count": price_preview["item_count"],
                        "seller_display": str(
                            item.get("seller_username") or "-"
                        ).strip()
                        or "-",
                        "listing_time_ts": sect_game._parse_iso_timestamp(
                            item.get("listing_time")
                        ),
                        "listing_time_display": fanren_game.format_timestamp(
                            sect_game._parse_iso_timestamp(item.get("listing_time"))
                        ),
                        "is_bundle_text": "是" if item.get("is_bundle") else "否",
                        "quantity_selectable": not bool(item.get("is_bundle"))
                        and int(item.get("quantity") or 0) > 1,
                    }
                )
            normalized_market_query = market_query.lower()
            if normalized_market_query:
                market_listings = [
                    item
                    for item in market_listings
                    if normalized_market_query
                    in " ".join(
                        [
                            str(item.get("id") or ""),
                            str(item.get("display_name") or ""),
                            str(item.get("display_type") or ""),
                            str(item.get("seller_display") or ""),
                        ]
                    ).lower()
                ]
            normalized_exchange_query = market_exchange_query.lower()
            if normalized_exchange_query:
                market_listings = [
                    item
                    for item in market_listings
                    if normalized_exchange_query
                    in str(
                        item.get("price_full_text") or item.get("price_text") or ""
                    ).lower()
                ]
            if market_sort == "price_desc":
                market_listings.sort(
                    key=lambda item: _reverse_market_price_sort_key(
                        item.get("price_sort_key") or ()
                    )
                )
            elif market_sort == "price_asc":
                market_listings.sort(key=lambda item: item.get("price_sort_key") or ())
            market_total = len(market_listings)
            market_total_pages = max(
                (market_total + market_page_size - 1) // market_page_size,
                1,
            )
            market_page = min(market_page, market_total_pages)
            market_page_numbers = _build_pagination_numbers(
                market_page, market_total_pages
            )
            start_index = (market_page - 1) * market_page_size
            end_index = start_index + market_page_size
            market_listings = market_listings[start_index:end_index]

        shared_template_context = _build_shared_template_context(active_profile)

        return templates.TemplateResponse(
            request,
            "module.html",
            {
                "app_name": settings.app_name,
                "module": module,
                "active_profile": active_profile,
                "module_setting": module_setting,
                "cultivation_results": cultivation_results,
                "cultivation_session": cultivation_session,
                "sect_session": sect_session,
                "cultivation_page": cultivation_page,
                "cultivation_page_size": cultivation_page_size,
                "cultivation_total": cultivation_total,
                "cultivation_total_pages": cultivation_total_pages,
                "cultivation_page_numbers": cultivation_page_numbers,
                "module_commands": MODULE_COMMANDS.get(module_key, []),
                "sect_features": SECT_FEATURES,
                "current_sect_feature": current_sect_feature,
                "sect_daily_state": sect_daily_state,
                "lingxiao_state": lingxiao_state,
                "yinluo_state": yinluo_state,
                "huangfeng_state": huangfeng_state,
                "character_state": character_state,
                "taiyi_state": taiyi_state,
                "other_play_definitions": OTHER_PLAY_DEFINITIONS,
                "other_play_state": other_play_state,
                "other_opponent_options": other_opponent_options,
                "divination_batch_state": divination_batch_state,
                "tianji_encounter_state": tianji_encounter_state,
                "companion_state": companion_state,
                "companion_auto_state": companion_auto_state,
                "dongfu_state": dongfu_state,
                "stock_state": stock_state,
                "dungeon_definitions": DUNGEON_DEFINITIONS,
                "selected_dungeon": selected_dungeon,
                "dungeon_command_buttons": dungeon_command_buttons,
                "dungeon_cleanup_targets": dungeon_cleanup_targets,
                "dungeon_messages": dungeon_messages,
                "inventory_materials": inventory_materials,
                "inventory_items": inventory_items,
                "inventory_page": inventory_page,
                "inventory_page_size": inventory_page_size,
                "inventory_total": inventory_total,
                "inventory_total_pages": inventory_total_pages,
                "inventory_page_numbers": inventory_page_numbers,
                "inventory_trade_options": inventory_trade_options,
                "recipes_known_entries": recipes_known_entries,
                "market_listings": market_listings,
                "market_query": market_query,
                "market_exchange_query": market_exchange_query,
                "market_query_qs": quote_plus(market_query) if market_query else "",
                "market_exchange_query_qs": quote_plus(market_exchange_query)
                if market_exchange_query
                else "",
                "market_sort": market_sort,
                "market_page": market_page,
                "market_page_size": market_page_size,
                "market_total": market_total,
                "market_total_pages": market_total_pages,
                "market_page_numbers": market_page_numbers,
                "spirit_stones": spirit_stones,
                "active_badge_text": active_badge_text,
                "equipped_artifact_name": equipped_artifact_name,
                "equipped_artifact_details": equipped_artifact_details,
                "sect_recent_reply_text": sect_recent_reply_text,
                "recipes_known_text": recipes_known_text,
                "formations_known_text": formations_known_text,
                "learned_techniques_text": learned_techniques_text,
                "equipped_id": equipped_id,
                "format_timestamp": fanren_game.format_timestamp,
                "now_ts": fanren_game.time.time(),
                "external_session_notice": _build_external_session_notice(
                    external_account
                ),
                **_build_sect_command_target_context(active_profile, sect_chat),
                **shared_template_context,
            },
        )

    @application.get("/api/dungeon-feed")
    async def dungeon_feed(
        request: Request, dungeon_key: str = "", chat_id: str = ""
    ) -> dict:
        active_profile = _get_request_profile(request)
        selected_dungeon = _get_dungeon_definition(dungeon_key)
        normalized_chat_id = str(chat_id or "").strip()
        resolved_chat_id = (
            int(normalized_chat_id)
            if normalized_chat_id.lstrip("-").isdigit()
            else None
        )
        if resolved_chat_id is None and active_profile:
            command_chat = _get_primary_command_chat(
                active_profile.id, fanren_game.FANREN_BOT_USERNAME
            )
            if command_chat:
                resolved_chat_id = int(command_chat.chat_id)
        return {
            "chat_ready": bool(resolved_chat_id),
            "chat_id": resolved_chat_id,
            "dungeon": {
                "key": selected_dungeon["key"],
                "title": selected_dungeon["title"],
            },
            "messages": _build_dungeon_messages(
                storage,
                resolved_chat_id or 0,
                selected_dungeon["key"],
                profile_id=active_profile.id if active_profile else None,
            ),
        }

    @application.get("/api/stock-history")
    async def stock_history(
        request: Request, stock_code: str = "", range_key: str = "7d"
    ) -> dict:
        if not _get_request_profile(request):
            raise HTTPException(status_code=401, detail="Profile not active")
        normalized_code = str(stock_code or "").strip().upper()
        if not normalized_code:
            raise HTTPException(status_code=400, detail="stock_code is required")
        return _build_stock_history_response(storage, normalized_code, range_key)

    @application.post("/runtime/dungeon/clear-messages")
    async def runtime_clear_dungeon_messages(
        request: Request,
        dungeon_key: str = Form(...),
        chat_id: str = Form(...),
        redirect_to: str = Form("/modules/dungeon"),
    ) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id:
            raise HTTPException(
                status_code=400, detail="Dungeon chat is not configured"
            )
        resolved_chat_id = int(normalized_chat_id)
        message_ids = [
            int(message.get("message_id") or 0)
            for message in _list_dungeon_feed_source_messages(
                storage,
                resolved_chat_id,
                dungeon_key,
                profile_id=profile.id,
            )
            if int(message.get("message_id") or 0)
        ]
        storage.delete_bound_messages(resolved_chat_id, message_ids)
        return RedirectResponse(url=redirect_to, status_code=303)

    @application.post("/profiles")
    async def create_profile(
        name: str = Form(...),
    ) -> RedirectResponse:
        profile = storage.create_profile(
            name=name,
            activate=True,
        )
        _sync_env_binding(profile.id, profile.telegram_user_id)
        storage.ensure_module_settings(profile.id, module_registry.list_modules())
        return RedirectResponse(url="/", status_code=303)

    @application.post("/profiles/{profile_id}/bind-current-telegram")
    async def bind_current_telegram_account(
        request: Request, profile_id: int
    ) -> RedirectResponse:
        profile = storage.get_profile(profile_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        if not _profile_belongs_to_session(request, profile_id):
            raise HTTPException(
                status_code=403, detail="Profile not available in current session"
            )
        profile_session_name = (profile.telegram_session_name or "").strip()
        if profile_session_name:
            account = await get_authorized_account_info(
                profile_session_name, allow_fallback=False
            )
        else:
            account = await _discover_authorized_account(request)
            if not account:
                raise HTTPException(
                    status_code=400,
                    detail="No authorized Telegram session available for binding",
                )
        telegram_user_id = str(account.get("id") or "").strip()
        if not telegram_user_id:
            raise HTTPException(status_code=400, detail="Telegram account unavailable")
        telegram_username = (account.get("username") or "").strip()
        telegram_phone = (account.get("phone") or "").strip()
        telegram_session_name = (account.get("session_name") or "").strip()
        storage.bind_profile_telegram_account(
            profile_id,
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            telegram_phone=telegram_phone,
            telegram_session_name=telegram_session_name,
        )
        _sync_env_binding(profile_id, telegram_user_id)
        storage.request_sect_refresh(profile_id, cooldown_seconds=0)
        return RedirectResponse(url="/profile", status_code=303)

    @application.post("/profiles/{profile_id}/refresh-info")
    async def refresh_profile_info(profile_id: int) -> RedirectResponse:
        profile = storage.get_profile(profile_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        if not profile.telegram_verified_at:
            return RedirectResponse(url="/profile", status_code=303)
        expired_redirect = _ensure_external_session_active(profile)
        if expired_redirect:
            return expired_redirect
        external_account = storage.get_external_account(profile_id, ASC_PROVIDER)
        cookie_text = (external_account or {}).get(
            "cookie_text"
        ) or get_effective_external_cookie(storage)
        if not cookie_text:
            return RedirectResponse(url="/profile", status_code=303)
        try:
            _connect_external_cookie(profile_id, cookie_text)
        except Exception as exc:
            mark_external_account_failure(
                storage, profile_id, exc, cookie_text=cookie_text
            )
        return RedirectResponse(url="/profile", status_code=303)

    @application.post("/modules/{module_key}/settings")
    async def save_module_setting(
        module_key: str,
        profile_id: int = Form(...),
        enabled: str = Form("0"),
        cooldown_seconds: int = Form(30),
        check_interval_seconds: int = Form(300),
        command_template: str = Form(""),
        notes: str = Form(""),
    ) -> RedirectResponse:
        if module_key in ("sect", "inventory"):
            return RedirectResponse(url=f"/modules/{module_key}", status_code=303)
        if not module_registry.get_module(module_key):
            raise HTTPException(status_code=404, detail="Module not found")
        if not storage.get_profile(profile_id):
            raise HTTPException(status_code=404, detail="Profile not found")
        storage.save_module_setting(
            profile_id=profile_id,
            module_key=module_key,
            enabled=enabled == "1",
            cooldown_seconds=cooldown_seconds,
            check_interval_seconds=check_interval_seconds,
            command_template=command_template,
            notes=notes,
        )
        return RedirectResponse(url=f"/modules/{module_key}", status_code=303)

    @application.post("/modules/{module_key}/toggle")
    async def toggle_module_setting(
        module_key: str,
        profile_id: int = Form(...),
        enabled: str = Form(...),
    ) -> RedirectResponse:
        if module_key in ("sect", "inventory"):
            return RedirectResponse(url=f"/modules/{module_key}", status_code=303)
        if not storage.get_profile(profile_id):
            raise HTTPException(status_code=404, detail="Profile not found")
        storage.ensure_module_settings(profile_id, module_registry.list_modules())
        storage.set_module_enabled(profile_id, module_key, enabled == "1")
        return RedirectResponse(url=f"/modules/{module_key}", status_code=303)

    @application.post("/runtime/commands/send-raw")
    async def runtime_send_raw_command(
        request: Request,
        chat_id: str = Form(...),
        text: str = Form(...),
        thread_id: Optional[str] = Form(None),
        chat_type: str = Form("group"),
        bot_username: str = Form("fanrenxiuxian_bot"),
        redirect_to: str = Form("/"),
    ) -> RedirectResponse:
        profile = getattr(request.state, "auth_profile", None)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        expired_redirect = _ensure_external_session_active(profile)
        if expired_redirect:
            return expired_redirect

        normalized_text = (text or "").strip()
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id:
            raise HTTPException(status_code=400, detail="Chat ID not configured")
        if not normalized_text:
            raise HTTPException(status_code=400, detail="Command text is required")

        storage.enqueue_outgoing_command(
            profile_id=profile.id,
            chat_id=int(normalized_chat_id),
            text=normalized_text,
            thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
            chat_type=chat_type,
            bot_username=bot_username,
        )
        return RedirectResponse(url=redirect_to, status_code=303)

    @application.post("/runtime/commands/divination-batch")
    async def runtime_start_divination_batch(
        request: Request,
        chat_id: str = Form(...),
        target_count: int = Form(...),
        thread_id: Optional[str] = Form(None),
        chat_type: str = Form("group"),
        bot_username: str = Form("fanrenxiuxian_bot"),
        redirect_to: str = Form("/modules/other"),
    ) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        expired_redirect = _ensure_external_session_active(profile)
        if expired_redirect:
            return expired_redirect

        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id:
            raise HTTPException(status_code=400, detail="Chat ID not configured")

        normalized_target_count = max(int(target_count or 0), 0)
        if normalized_target_count <= 0:
            raise HTTPException(status_code=400, detail="Target count is required")

        resolved_chat_id = int(normalized_chat_id)
        payload = read_cached_external_payload(storage, profile.id)
        current_count = _build_divination_view(payload)["today_count"]
        remaining_rounds = max(normalized_target_count - current_count, 0)
        if remaining_rounds <= 0:
            return RedirectResponse(url=redirect_to, status_code=303)

        active_batch = storage.get_active_divination_batch(profile.id, resolved_chat_id)
        if active_batch:
            return RedirectResponse(url=redirect_to, status_code=303)

        batch_id = storage.start_divination_batch(
            profile_id=profile.id,
            chat_id=resolved_chat_id,
            target_count=normalized_target_count,
            initial_count=current_count,
            thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
            chat_type=chat_type,
            bot_username=bot_username,
        )
        try:
            storage.enqueue_outgoing_command(
                profile_id=profile.id,
                chat_id=resolved_chat_id,
                text=".卜筮问天",
                thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
                chat_type=chat_type,
                bot_username=bot_username,
                delay_seconds=0,
            )
            storage.update_divination_batch(
                batch_id,
                last_dispatch_at=fanren_game.time.time(),
            )
        except Exception as exc:
            storage.finish_divination_batch(
                batch_id, status="failed", last_error=str(exc)
            )
            raise

        return RedirectResponse(url=redirect_to, status_code=303)

    @application.post("/runtime/commands/divination-batch/cancel")
    async def runtime_cancel_divination_batch(
        request: Request,
        chat_id: str = Form(...),
        redirect_to: str = Form("/modules/other"),
    ) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        normalized_chat_id = str(chat_id or "").strip()
        if not normalized_chat_id:
            raise HTTPException(status_code=400, detail="Chat ID not configured")
        resolved_chat_id = int(normalized_chat_id)
        active_batch = storage.get_active_divination_batch(profile.id, resolved_chat_id)
        if active_batch:
            storage.finish_divination_batch(
                int(active_batch["id"]),
                status="cancelled",
                last_error="Cancelled by user",
            )
        storage.cancel_pending_outgoing_commands(
            profile.id,
            resolved_chat_id,
            text=".卜筮问天",
        )
        return RedirectResponse(url=redirect_to, status_code=303)

    @application.post("/runtime/commands/companion-auto")
    async def runtime_toggle_companion_auto(
        request: Request,
        chat_id: str = Form(...),
        feature_key: str = Form(...),
        thread_id: Optional[str] = Form(None),
        chat_type: str = Form("group"),
        bot_username: str = Form("fanrenxiuxian_bot"),
        redirect_to: str = Form("/modules/other"),
    ) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        expired_redirect = _ensure_external_session_active(profile)
        if expired_redirect:
            return expired_redirect

        normalized_chat_id = str(chat_id or "").strip()
        normalized_feature_key = str(feature_key or "").strip()
        feature = COMPANION_AUTO_FEATURES.get(normalized_feature_key)
        if not normalized_chat_id:
            raise HTTPException(status_code=400, detail="Chat ID not configured")
        if not feature:
            raise HTTPException(
                status_code=400, detail="Invalid companion auto feature"
            )

        resolved_chat_id = int(normalized_chat_id)
        existing_task = storage.get_companion_auto_task(
            profile.id, resolved_chat_id, normalized_feature_key
        )
        if existing_task and bool(existing_task.get("enabled")):
            storage.disable_companion_auto_task(
                profile.id, resolved_chat_id, normalized_feature_key
            )
            storage.cancel_pending_outgoing_commands(
                profile.id,
                resolved_chat_id,
                text=str(feature.get("command") or ""),
            )
            return RedirectResponse(url=redirect_to, status_code=303)

        payload = read_cached_external_payload(storage, profile.id)
        next_run_at = _resolve_companion_auto_next_run_at(
            payload, normalized_feature_key
        )
        if next_run_at is None:
            storage.upsert_companion_auto_task(
                profile_id=profile.id,
                chat_id=resolved_chat_id,
                feature_key=normalized_feature_key,
                enabled=False,
                thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
                chat_type=chat_type,
                bot_username=bot_username,
                next_run_at=0,
                last_error=f"最新 payload 缺少{feature.get('label') or normalized_feature_key}冷却字段，已停止自动。",
            )
            storage.cancel_pending_outgoing_commands(
                profile.id,
                resolved_chat_id,
                text=str(feature.get("command") or ""),
            )
            return RedirectResponse(url=redirect_to, status_code=303)
        storage.upsert_companion_auto_task(
            profile_id=profile.id,
            chat_id=resolved_chat_id,
            feature_key=normalized_feature_key,
            enabled=True,
            thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
            chat_type=chat_type,
            bot_username=bot_username,
            next_run_at=next_run_at,
            last_error="",
        )
        if next_run_at <= fanren_game.time.time():
            now_ts = fanren_game.time.time()
            storage.enqueue_outgoing_command(
                profile_id=profile.id,
                chat_id=resolved_chat_id,
                text=str(feature.get("command") or "").strip(),
                thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
                chat_type=chat_type,
                bot_username=bot_username,
                delay_seconds=0,
            )
            storage.upsert_companion_auto_task(
                profile_id=profile.id,
                chat_id=resolved_chat_id,
                feature_key=normalized_feature_key,
                enabled=True,
                thread_id=int(thread_id) if thread_id and thread_id.isdigit() else None,
                chat_type=chat_type,
                bot_username=bot_username,
                next_run_at=now_ts + COMPANION_AUTO_MANUAL_DELAY_SECONDS,
                last_run_at=now_ts,
                last_error="",
            )
        return RedirectResponse(url=redirect_to, status_code=303)

    @application.post("/runtime/sect/action")
    async def runtime_send_sect_action(request: Request) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        expired_redirect = _ensure_external_session_active(profile)
        if expired_redirect:
            return expired_redirect
        form_data = await request.form()
        sect_name = str(form_data.get("sect_name") or "").strip()
        action_key = str(form_data.get("action_key") or "").strip()
        redirect_to = str(form_data.get("redirect_to") or "/modules/sect").strip()
        chat_id_text = str(form_data.get("chat_id") or "").strip()
        if not sect_name or not action_key:
            raise HTTPException(status_code=400, detail="Sect action is required")
        if not chat_id_text:
            raise HTTPException(status_code=400, detail="Sect chat is not configured")

        feature = _get_sect_feature_by_name(sect_name)
        if not feature:
            raise HTTPException(status_code=404, detail="Sect feature not found")
        action = next(
            (
                item
                for item in feature.get("actions") or []
                if str(item.get("key") or "").strip() == action_key
            ),
            None,
        )
        if not action:
            raise HTTPException(status_code=404, detail="Sect action not found")

        command_text = _build_sect_action_command(action, form_data)
        thread_id_text = str(form_data.get("thread_id") or "").strip()
        chat_type = str(form_data.get("chat_type") or "group").strip() or "group"
        bot_username = (
            str(form_data.get("bot_username") or "").strip()
            or sect_game.SECT_BOT_USERNAME
        )
        storage.enqueue_outgoing_command(
            profile_id=profile.id,
            chat_id=int(chat_id_text),
            text=command_text,
            thread_id=int(thread_id_text) if thread_id_text.isdigit() else None,
            chat_type=chat_type,
            bot_username=bot_username,
        )
        return RedirectResponse(url=redirect_to or "/modules/sect", status_code=303)

    @application.post("/runtime/sect/yinluo-batch")
    async def runtime_start_yinluo_batch(request: Request) -> RedirectResponse:
        profile = _get_request_profile(request)
        if not profile:
            raise HTTPException(status_code=401, detail="Profile not active")
        expired_redirect = _ensure_external_session_active(profile)
        if expired_redirect:
            return expired_redirect
        form_data = await request.form()
        batch_mode = str(form_data.get("batch_mode") or "").strip().lower()
        redirect_to = str(form_data.get("redirect_to") or "/modules/sect").strip()
        chat_id_text = str(form_data.get("chat_id") or "").strip()
        if not chat_id_text:
            raise HTTPException(status_code=400, detail="Sect chat is not configured")
        thread_id_text = str(form_data.get("thread_id") or "").strip()
        chat_type = str(form_data.get("chat_type") or "group").strip() or "group"
        bot_username = (
            str(form_data.get("bot_username") or "").strip()
            or sect_game.SECT_BOT_USERNAME
        )

        if batch_mode in {"soothe", "collect"}:
            command_text = (
                ".一键安抚幡灵" if batch_mode == "soothe" else ".一键收取精华"
            )
            storage.enqueue_outgoing_command(
                profile_id=profile.id,
                chat_id=int(chat_id_text),
                text=command_text,
                thread_id=int(thread_id_text) if thread_id_text.isdigit() else None,
                chat_type=chat_type,
                bot_username=bot_username,
            )
            return RedirectResponse(url=redirect_to or "/modules/sect", status_code=303)

        if batch_mode != "imprison":
            raise HTTPException(status_code=400, detail="Invalid yinluo batch mode")

        commands = []
        for key in sorted(form_data.keys()):
            if not key.startswith("slot_soul_"):
                continue
            slot_index_text = key.split("slot_soul_", 1)[1].strip()
            if not slot_index_text.isdigit():
                continue
            slot_state = str(
                form_data.get(f"slot_state_{slot_index_text}") or ""
            ).strip()
            soul_name = str(form_data.get(key) or "").strip()
            if slot_state != "空闲" or not soul_name:
                continue
            commands.append(f".囚禁魂魄 {int(slot_index_text)} {soul_name}")
        if not commands:
            raise HTTPException(
                status_code=400, detail="No available yinluo imprison commands"
            )

        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            sect_game.set_enabled(
                db,
                int(chat_id_text),
                True,
                profile_id=profile.id,
            )
            sect_game.start_yinluo_batch(
                db,
                int(chat_id_text),
                "imprison",
                commands,
                profile_id=profile.id,
            )
        finally:
            db.close()
        return RedirectResponse(url=redirect_to or "/modules/sect", status_code=303)

    @application.post("/runtime/cultivation/toggle")
    async def toggle_cultivation_runtime(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            binding = (
                storage.get_chat_binding(active_profile.id, chat_id)
                if active_profile
                else None
            )
            if enabled == "1":
                if active_profile:
                    sync_cultivation_session(storage, active_profile.id, chat_id, db)
                fanren_game.update_session(
                    db,
                    chat_id,
                    profile_id=active_profile.id if active_profile else None,
                    thread_id=getattr(binding, "thread_id", None),
                )
                fanren_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    reset_failure=True,
                    profile_id=active_profile.id if active_profile else None,
                )
            else:
                fanren_game.set_enabled(
                    db,
                    chat_id,
                    False,
                    profile_id=active_profile.id if active_profile else None,
                )
            session = fanren_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/cultivation/mode")
    async def set_cultivation_mode(
        request: Request, chat_id: int = Form(...), mode: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            current_session = fanren_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
            preserve_next_check_time = 0
            if (
                (mode or "").strip().lower() == "deep"
                and current_session
                and float(current_session.get("next_check_time") or 0)
                > fanren_game.time.time()
            ):
                preserve_next_check_time = float(
                    current_session.get("next_check_time") or 0
                )
            fanren_game.set_mode(
                db,
                chat_id,
                mode,
                preserve_next_check_time=preserve_next_check_time,
                profile_id=active_profile.id if active_profile else None,
            )
            if active_profile:
                sync_cultivation_session(storage, active_profile.id, chat_id, db)
            fanren_game.update_session(
                db,
                chat_id,
                profile_id=active_profile.id if active_profile else None,
                last_summary=f"已切换为{'深度闭关' if mode == 'deep' else '普通闭关'}，将按接口冷却时间自动调度。",
            )
            session = fanren_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/cultivation/delete-command-toggle")
    async def toggle_cultivation_delete_command(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            fanren_game.set_delete_normal_command_message(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            session = fanren_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/cultivation/jiyin-toggle")
    async def toggle_cultivation_jiyin_auto(
        request: Request,
        chat_id: int = Form(...),
        enabled: str = Form(...),
        choice: str = Form(""),
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            if not active_profile:
                raise HTTPException(status_code=401, detail="Profile not active")
            fanren_game.set_auto_jiyin(
                db,
                chat_id,
                enabled == "1",
                choice,
                profile_id=active_profile.id,
            )
            session = fanren_game.get_session(db, chat_id, profile_id=active_profile.id)
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/cultivation/nanlong-toggle")
    async def toggle_cultivation_nanlong_auto(
        request: Request,
        chat_id: int = Form(...),
        enabled: str = Form(...),
        choice: str = Form(""),
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            if not active_profile:
                raise HTTPException(status_code=401, detail="Profile not active")
            fanren_game.set_auto_nanlong(
                db,
                chat_id,
                enabled == "1",
                choice,
                profile_id=active_profile.id,
            )
            session = fanren_game.get_session(db, chat_id, profile_id=active_profile.id)
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/cultivation/rift-toggle")
    async def toggle_cultivation_rift_auto(
        request: Request,
        chat_id: int = Form(...),
        enabled: str = Form(...),
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            if not active_profile:
                raise HTTPException(status_code=401, detail="Profile not active")
            fanren_game.set_auto_rift(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id,
            )
            session = fanren_game.get_session(db, chat_id, profile_id=active_profile.id)
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/cultivation/yuanying-toggle")
    async def toggle_cultivation_yuanying_auto(
        request: Request,
        chat_id: int = Form(...),
        enabled: str = Form(...),
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            fanren_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            if not active_profile:
                raise HTTPException(status_code=401, detail="Profile not active")
            fanren_game.set_auto_yuanying(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id,
            )
            session = fanren_game.get_session(db, chat_id, profile_id=active_profile.id)
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Cultivation session not found")
        return RedirectResponse(url="/modules/cultivation", status_code=303)

    @application.post("/runtime/sect/lingxiao-toggle")
    async def toggle_lingxiao_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_lingxiao_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_lingxiao_trial_state(
                        storage, db, active_profile.id, chat_id
                    )
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/checkin-toggle")
    async def toggle_sect_checkin_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_sect_checkin_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_common_sect_state(
                        storage, db, active_profile.id, chat_id
                    )
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/teach-toggle")
    async def toggle_sect_teach_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_sect_teach_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_common_sect_state(
                        storage, db, active_profile.id, chat_id
                    )
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/yinluo-sacrifice-toggle")
    async def toggle_yinluo_sacrifice_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_yinluo_sacrifice_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_yinluo_state(storage, db, active_profile.id, chat_id)
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/yinluo-blood-wash-toggle")
    async def toggle_yinluo_blood_wash_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_yinluo_blood_wash_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_yinluo_state(storage, db, active_profile.id, chat_id)
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/huangfeng-auto")
    async def configure_huangfeng_auto(
        request: Request,
        chat_id: int = Form(...),
        enabled: str = Form(...),
        seed_name: str = Form(""),
        exchange_enabled: str = Form("0"),
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            if not active_profile:
                raise HTTPException(status_code=401, detail="Profile not active")
            session = sect_game.get_session(db, chat_id, profile_id=active_profile.id)
            normalized_seed_name = (
                str(seed_name or "").strip()
                or str((session or {}).get("huangfeng_seed_name") or "").strip()
            )
            exchange_flag = exchange_enabled == "1"
            if enabled == "1" and not normalized_seed_name:
                raise HTTPException(status_code=400, detail="Seed name required")
            sect_game.configure_huangfeng_auto(
                db,
                chat_id,
                enabled == "1",
                seed_name=normalized_seed_name if enabled == "1" else None,
                exchange_enabled=exchange_flag,
                profile_id=active_profile.id,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id,
                )
            session = sect_game.get_session(db, chat_id, profile_id=active_profile.id)
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/lingxiao-gangfeng-toggle")
    async def toggle_lingxiao_gangfeng_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_lingxiao_gangfeng_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_lingxiao_trial_state(
                        storage, db, active_profile.id, chat_id
                    )
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/lingxiao-borrow-toggle")
    async def toggle_lingxiao_borrow_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_lingxiao_borrow_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_lingxiao_trial_state(
                        storage, db, active_profile.id, chat_id
                    )
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.post("/runtime/sect/lingxiao-question-toggle")
    async def toggle_lingxiao_question_auto(
        request: Request, chat_id: int = Form(...), enabled: str = Form(...)
    ) -> RedirectResponse:
        db = CompatDb(storage)
        try:
            sect_game.ensure_tables(db)
            active_profile = _get_request_profile(request)
            sect_game.configure_lingxiao_question_auto(
                db,
                chat_id,
                enabled == "1",
                profile_id=active_profile.id if active_profile else None,
            )
            if enabled == "1":
                sect_game.set_enabled(
                    db,
                    chat_id,
                    True,
                    profile_id=active_profile.id if active_profile else None,
                )
                if active_profile:
                    sect_game.sync_lingxiao_trial_state(
                        storage, db, active_profile.id, chat_id
                    )
            session = sect_game.get_session(
                db, chat_id, profile_id=active_profile.id if active_profile else None
            )
        finally:
            db.close()
        if not session:
            raise HTTPException(status_code=404, detail="Sect session not found")
        return RedirectResponse(url="/modules/sect", status_code=303)

    @application.get("/health")
    async def health() -> dict:
        active_profile = storage.get_active_profile()
        return {
            "status": "ok",
            "modules": len(module_registry.list_modules()),
            "profiles": len(storage.list_profiles()),
            "active_profile": active_profile.name if active_profile else None,
        }

    return application


app = create_app()
