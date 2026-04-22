import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_external_keepalive_poll_seconds,
    is_external_account_expired,
)
from tg_game.storage import CompatDb as RuntimeDb
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)


FANREN_BOT_USERNAME = (
    os.getenv("FANREN_BOT_USERNAME", "fanrenxiuxian_bot").lstrip("@").lower()
)
FANREN_CHECK_COMMAND = os.getenv("FANREN_CHECK_COMMAND", ".查看闭关")
FANREN_NORMAL_COMMAND = os.getenv("FANREN_NORMAL_COMMAND", ".闭关修炼")
FANREN_DEEP_COMMAND = os.getenv("FANREN_DEEP_COMMAND", ".深度闭关")
FANREN_DEFAULT_MODE = os.getenv("FANREN_DEFAULT_MODE", "normal")
FANREN_DEFAULT_INTERVAL = int(os.getenv("FANREN_DEFAULT_INTERVAL", "300"))
FANREN_COMMAND_COOLDOWN = int(os.getenv("FANREN_COMMAND_COOLDOWN", "30"))
FANREN_MAX_FAILURES = int(os.getenv("FANREN_MAX_FAILURES", "3"))
FANREN_MIN_INTERVAL = int(os.getenv("FANREN_MIN_INTERVAL", "30"))
FANREN_RUNNER_POLL_SECONDS = int(os.getenv("FANREN_RUNNER_POLL_SECONDS", "5"))

FANREN_FAILURE_EVENTS = {"blocked", "resource_blocked", "unknown"}
FANREN_DEEP_PENDING_EVENTS = {"deep_cultivating", "deep_started", "deep_settlement_due"}
FANREN_DEEP_RESOLVED_EVENTS = {"deep_retreat_summary", "deep_idle"}


@dataclass
class FanrenParseResult:
    event: str
    summary: str
    cooldown_seconds: Optional[int] = None


def _normalize_bool(value):
    return 1 if bool(value) else 0


def format_timestamp(timestamp):
    if not timestamp:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def format_duration(seconds):
    seconds = max(int(seconds or 0), 0)
    if seconds == 0:
        return "0秒"

    parts = []
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        parts.append(f"{hours}小时")
    if minutes:
        parts.append(f"{minutes}分钟")
    if secs or not parts:
        parts.append(f"{secs}秒")
    return "".join(parts)


def clamp_interval(seconds):
    return max(int(seconds), FANREN_MIN_INTERVAL)


COOLDOWN_PATTERNS = [
    re.compile(r"(?P<value>\d+)\s*小时"),
    re.compile(r"(?P<value>\d+)\s*分钟"),
    re.compile(r"(?P<value>\d+)\s*秒"),
]

GAIN_PATTERNS = [
    re.compile(r"修为最终增加了\s*(?P<value>\d+)\s*点"),
    re.compile(r"修为增加了\s*(?P<value>\d+)\s*点"),
    re.compile(r"修为增长变化了\s*(?P<value>\d+)\s*点"),
]

LOSS_PATTERNS = [
    re.compile(r"修为倒退了\s*(?P<value>\d+)\s*点"),
    re.compile(r"修为减少了\s*(?P<value>\d+)\s*点"),
]

STAGE_PATTERN = re.compile(r"当前境界[:：]\s*(?P<value>[^\n]+)")
PROGRESS_PATTERN = re.compile(r"当前修为[:：]\s*(?P<value>\d+\s*/\s*\d+)")


def ensure_tables(db):
    db.cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fanren_sessions (
            chat_id INTEGER NOT NULL,
            bot_username TEXT NOT NULL,
            enabled INTEGER DEFAULT 0,
            interval_seconds INTEGER DEFAULT 300,
            command_text TEXT DEFAULT '.查看闭关',
            last_command_time REAL DEFAULT 0,
            next_check_time REAL DEFAULT 0,
            next_check_source TEXT,
            last_event TEXT,
            last_summary TEXT,
            last_bot_text TEXT,
            last_bot_msg_id INTEGER DEFAULT 0,
            last_action TEXT,
            last_action_time REAL DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            dry_run INTEGER DEFAULT 0,
            stopped_reason TEXT,
            retreat_mode TEXT DEFAULT 'normal',
            thread_id INTEGER,
            delete_normal_command_message INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, bot_username)
        )
        """
    )
    columns = {
        row[1]
        for row in db.cur.execute("PRAGMA table_info(fanren_sessions)").fetchall()
    }
    if "stopped_reason" not in columns:
        db.cur.execute("ALTER TABLE fanren_sessions ADD COLUMN stopped_reason TEXT")
    if "retreat_mode" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN retreat_mode TEXT DEFAULT 'normal'"
        )
    if "thread_id" not in columns:
        db.cur.execute("ALTER TABLE fanren_sessions ADD COLUMN thread_id INTEGER")
    if "next_check_source" not in columns:
        db.cur.execute("ALTER TABLE fanren_sessions ADD COLUMN next_check_source TEXT")
    if "delete_normal_command_message" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN delete_normal_command_message INTEGER DEFAULT 0"
        )
    db.conn.commit()


def ensure_session(db, chat_id, bot_username=FANREN_BOT_USERNAME):
    ensure_tables(db)
    db.cur.execute(
        """
        INSERT OR IGNORE INTO fanren_sessions
            (chat_id, bot_username, interval_seconds, command_text, retreat_mode)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            chat_id,
            bot_username,
            FANREN_DEFAULT_INTERVAL,
            FANREN_CHECK_COMMAND,
            FANREN_DEFAULT_MODE,
        ),
    )
    db.conn.commit()


def get_session(db, chat_id, bot_username=FANREN_BOT_USERNAME):
    ensure_session(db, chat_id, bot_username)
    db.cur.execute(
        "SELECT * FROM fanren_sessions WHERE chat_id=? AND bot_username=?",
        (chat_id, bot_username),
    )
    row = db.cur.fetchone()
    return dict(zip([col[0] for col in db.cur.description], row)) if row else None


def update_session(db, chat_id, bot_username=FANREN_BOT_USERNAME, **fields):
    if not fields:
        return
    ensure_session(db, chat_id, bot_username)
    assignments = ", ".join(f"{key}=?" for key in fields)
    values = list(fields.values()) + [chat_id, bot_username]
    db.cur.execute(
        f"UPDATE fanren_sessions SET {assignments} WHERE chat_id=? AND bot_username=?",
        values,
    )
    db.conn.commit()


async def send_message_in_session(
    client,
    session,
    chat_id,
    command_text,
    *,
    storage=None,
    profile_id=None,
):
    thread_id = session.get("thread_id")
    logger.info(
        "Fanren send attempt chat=%s thread=%s mode=%s command=%s",
        chat_id,
        thread_id,
        session.get("retreat_mode"),
        command_text,
    )
    await send_message_with_thread_fallback(
        client,
        chat_id,
        command_text,
        thread_id=thread_id,
        storage=storage or getattr(client, "_tg_game_storage", None),
        profile_id=profile_id,
        bot_username=session.get("bot_username") or FANREN_BOT_USERNAME,
        log_prefix="Fanren auto",
    )
    logger.info(
        "Fanren send success chat=%s thread=%s command=%s",
        chat_id,
        thread_id,
        command_text,
    )


def list_sessions(db):
    ensure_tables(db)
    db.cur.execute("SELECT * FROM fanren_sessions ORDER BY chat_id")
    return [
        dict(zip([col[0] for col in db.cur.description], row))
        for row in db.cur.fetchall()
    ]


def parse_cooldown_seconds(text):
    total = 0
    matched = False
    for pattern in COOLDOWN_PATTERNS:
        for match in pattern.finditer(text):
            value = int(match.group("value"))
            unit = match.group(0)
            matched = True
            if "小时" in unit:
                total += value * 3600
            elif "分钟" in unit:
                total += value * 60
            else:
                total += value
    return total if matched else None


def parse_interval_input(raw_value):
    value = (raw_value or "").strip().lower()
    if not value:
        raise ValueError("间隔不能为空")

    match = re.fullmatch(r"(\d+)([hms]|分钟|分|秒|小时)?", value)
    if not match:
        raise ValueError("间隔格式不正确，示例：300 / 5m / 1h")

    amount = int(match.group(1))
    unit = match.group(2) or "s"
    if unit in {"h", "小时"}:
        seconds = amount * 3600
    elif unit in {"m", "分钟", "分"}:
        seconds = amount * 60
    else:
        seconds = amount

    return clamp_interval(seconds)


def parse_gain_value(text):
    for pattern in LOSS_PATTERNS:
        match = pattern.search(text)
        if match:
            return -int(match.group("value"))
    for pattern in GAIN_PATTERNS:
        match = pattern.search(text)
        if match:
            return int(match.group("value"))
    return None


def extract_stage_progress(text):
    stage_match = STAGE_PATTERN.search(text)
    progress_match = PROGRESS_PATTERN.search(text)
    stage = stage_match.group("value").strip() if stage_match else None
    progress = (
        progress_match.group("value").replace(" ", "") if progress_match else None
    )
    return stage, progress


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return FanrenParseResult("empty", "empty message")

    lowered = text.lower()
    cooldown = parse_cooldown_seconds(text)
    gain_value = parse_gain_value(text)
    stage, progress = extract_stage_progress(text)

    if "深度闭关总结" in text:
        summary = "收到深度闭关总结"
        if gain_value is not None:
            summary = f"深度闭关总结，修为变化 {gain_value} 点"
        return FanrenParseResult("deep_retreat_summary", summary, cooldown)

    if "正在推演天机" in text or "锁定道友神魂" in text or "大命玉璞" in text:
        return FanrenParseResult("ignored", "非修炼消息，忽略", cooldown)

    if "你已进入深度闭关状态" in text:
        summary = "已进入深度闭关"
        if cooldown:
            summary = f"已进入深度闭关，预计 {format_duration(cooldown)} 后结算"
        return FanrenParseResult("deep_started", summary, cooldown)

    if "你正在深度闭关" in text or "你已在深度闭关之中" in text:
        summary = "深度闭关进行中"
        if cooldown:
            summary = f"深度闭关中，还需 {format_duration(cooldown)}"
        return FanrenParseResult("deep_cultivating", summary, cooldown)

    if "并未处于深度闭关之中" in text:
        return FanrenParseResult("deep_idle", "当前未在深度闭关，可立即开始", cooldown)

    if "闭关成功" in text or "本次闭关" in text:
        summary_parts = ["闭关完成"]
        if gain_value is not None:
            if gain_value >= 0:
                summary_parts.append(f"修为增加 {gain_value} 点")
            else:
                summary_parts.append(f"修为倒退 {abs(gain_value)} 点")
        if stage:
            summary_parts.append(f"境界 {stage}")
        if progress:
            summary_parts.append(f"进度 {progress}")
        return FanrenParseResult("retreat_complete", "，".join(summary_parts), cooldown)

    if "走火入魔" in text or "道心受损" in text:
        summary_parts = ["闭关受挫"]
        if gain_value is not None and gain_value < 0:
            summary_parts.append(f"修为倒退 {abs(gain_value)} 点")
        if stage:
            summary_parts.append(f"境界 {stage}")
        if progress:
            summary_parts.append(f"进度 {progress}")
        return FanrenParseResult("retreat_setback", "，".join(summary_parts), cooldown)

    if "灵气尚未平复" in text or "需要打坐调息" in text:
        summary = "闭关后调息冷却中"
        if cooldown:
            summary = f"闭关后调息中，还需 {format_duration(cooldown)}"
        return FanrenParseResult("cooldown", summary, cooldown)

    if "功法圆满" in text:
        if "神魂正在归位" in text:
            return FanrenParseResult(
                "cultivation_full", "功法圆满，等待归位完成", cooldown
            )
        return FanrenParseResult("cultivation_full", "功法圆满，可准备下一步", cooldown)

    if "闭关中" in text or "正在闭关" in text or "修炼中" in text:
        return FanrenParseResult("cultivating", "仍在闭关中", cooldown)

    if "神魂正在归位" in text:
        return FanrenParseResult("soul_returning", "神魂归位中", cooldown)

    if "冷却" in text or "稍后再试" in text or "还需等待" in text:
        return FanrenParseResult("cooldown", "动作冷却中", cooldown)

    if "灵石不足" in text or "资源不足" in text or "材料不足" in text:
        return FanrenParseResult("resource_blocked", "资源不足，需要人工处理", cooldown)

    if "突破成功" in text or "出关成功" in text or "成功" in lowered:
        summary = "收到成功反馈"
        if gain_value is not None:
            summary = f"收到成功反馈，修为增加 {gain_value} 点"
        return FanrenParseResult("success", summary, cooldown)

    if "失败" in text or "不可" in text or "无法" in text:
        return FanrenParseResult("blocked", "当前步骤失败或受阻", cooldown)

    return FanrenParseResult("unknown", text[:80], cooldown)


def build_status_text(session):
    if not session:
        return "凡人修仙自动化未初始化。"

    now = time.time()
    next_check_time = session.get("next_check_time") or 0
    remaining = max(int(next_check_time - now), 0) if next_check_time else 0
    enabled = bool(session.get("enabled"))
    dry_run = bool(session.get("dry_run"))
    failure_count = int(session.get("failure_count") or 0)
    stopped_reason = session.get("stopped_reason") or "-"

    return "\n".join(
        [
            "凡人修仙自动化状态",
            f"开关: {'开启' if enabled else '关闭'}",
            f"Dry-run: {'开启' if dry_run else '关闭'}",
            f"模式: {'深度闭关' if session.get('retreat_mode') == 'deep' else '普通闭关'}",
            f"普通闭关删原消息: {'开启' if session.get('delete_normal_command_message') else '关闭'}",
            f"检查指令: {session.get('command_text') or FANREN_CHECK_COMMAND}",
            f"普通闭关指令: {FANREN_NORMAL_COMMAND}",
            f"深度闭关指令: {FANREN_DEEP_COMMAND}",
            f"检查间隔: {format_duration(session.get('interval_seconds') or FANREN_DEFAULT_INTERVAL)}",
            f"下次检查: {format_timestamp(next_check_time)}",
            f"剩余等待: {format_duration(remaining) if next_check_time else '-'}",
            f"倒计时来源: {session.get('next_check_source') or '-'}",
            f"最后事件: {session.get('last_event') or '-'}",
            f"最后摘要: {session.get('last_summary') or '-'}",
            f"最后动作: {session.get('last_action') or '-'}",
            f"最后动作时间: {format_timestamp(session.get('last_action_time') or 0)}",
            f"连续失败: {failure_count}/{FANREN_MAX_FAILURES}",
            f"熔断原因: {stopped_reason}",
        ]
    )


def set_enabled(db, chat_id, enabled, *, reset_failure=False):
    fields = {"enabled": _normalize_bool(enabled)}
    if enabled:
        # Keep the schedule that was just synced from Tianjige instead of
        # forcing an immediate send on enable.
        fields["stopped_reason"] = None
    if reset_failure:
        fields["failure_count"] = 0
    update_session(db, chat_id, **fields)


def reset_runtime_state(db, chat_id):
    update_session(
        db,
        chat_id,
        next_check_time=0,
        next_check_source=None,
        last_event=None,
        last_summary=None,
        last_bot_text=None,
        last_bot_msg_id=0,
        last_action=None,
        last_action_time=0,
        last_command_time=0,
        failure_count=0,
        stopped_reason=None,
    )


def set_dry_run(db, chat_id, enabled):
    update_session(db, chat_id, dry_run=_normalize_bool(enabled))


def set_interval(db, chat_id, interval_seconds):
    interval_seconds = clamp_interval(interval_seconds)
    update_session(db, chat_id, interval_seconds=interval_seconds)
    return interval_seconds


def set_check_command(db, chat_id, command_text):
    command_text = (command_text or "").strip()
    if not command_text:
        raise ValueError("检查指令不能为空")
    update_session(db, chat_id, command_text=command_text)
    return command_text


def set_mode(db, chat_id, retreat_mode, preserve_next_check_time=0):
    retreat_mode = (retreat_mode or "").strip().lower()
    if retreat_mode not in {"normal", "deep"}:
        raise ValueError("模式只支持 normal 或 deep")
    update_session(
        db,
        chat_id,
        retreat_mode=retreat_mode,
        next_check_time=preserve_next_check_time or 0,
        next_check_source=(
            "从深度闭关同步剩余倒计时" if preserve_next_check_time else None
        ),
        stopped_reason=None,
    )
    return retreat_mode


def set_delete_normal_command_message(db, chat_id, enabled):
    update_session(db, chat_id, delete_normal_command_message=_normalize_bool(enabled))
    return bool(_normalize_bool(enabled))


async def maybe_delete_normal_command_message(
    event, session, client, reply_text, reply_message_id=None
):
    if client is None or not session:
        return False
    if (session.get("retreat_mode") or FANREN_DEFAULT_MODE).lower() != "normal":
        return False
    if not bool(session.get("delete_normal_command_message")):
        return False
    if (reply_text or "").strip() != FANREN_NORMAL_COMMAND:
        return False
    message = getattr(event, "message", None)
    reply_to = getattr(message, "reply_to", None) if message else None
    reply_to_msg_id = reply_message_id or getattr(reply_to, "reply_to_msg_id", None)
    if not reply_to_msg_id:
        return False
    try:
        await client.delete_messages(event.chat_id, [int(reply_to_msg_id)], revoke=True)
        logger.info(
            "Fanren deleted replied normal command chat=%s message_id=%s",
            event.chat_id,
            reply_to_msg_id,
        )
        return True
    except Exception as exc:
        logger.warning(
            "Fanren failed deleting replied normal command chat=%s message_id=%s error=%s",
            event.chat_id,
            reply_to_msg_id,
            exc,
        )
        return False


def reset_failures(db, chat_id):
    update_session(db, chat_id, failure_count=0, stopped_reason=None)


def trip_circuit_breaker(db, chat_id, reason):
    update_session(
        db,
        chat_id,
        enabled=0,
        stopped_reason=reason,
        next_check_time=0,
    )
    logger.warning("Fanren circuit breaker tripped in chat %s: %s", chat_id, reason)


def record_failure(db, chat_id, reason):
    session = get_session(db, chat_id)
    failure_count = int(session.get("failure_count") or 0) + 1
    update_session(
        db,
        chat_id,
        failure_count=failure_count,
        last_summary=reason,
    )
    if failure_count >= FANREN_MAX_FAILURES:
        trip_circuit_breaker(db, chat_id, f"连续失败达到 {failure_count} 次: {reason}")
    return failure_count


def _resolve_runtime_profile_id(storage=None, profile_id=None):
    if profile_id:
        return int(profile_id)
    if not storage:
        return None
    active_profile = storage.get_active_profile()
    return int(getattr(active_profile, "id", 0) or 0) or None


def _build_external_expired_pause_fields(now):
    retry_seconds = max(int(get_external_keepalive_poll_seconds() or 0), 5)
    message = "天机阁会话已失效，暂停凡人修仙自动发送，等待重新登录"
    return {
        "next_check_time": now + retry_seconds,
        "next_check_source": message,
        "last_summary": message,
    }


def _pause_if_external_session_expired(
    db, chat_id, *, storage=None, profile_id=None, now=None
):
    runtime_storage = storage
    resolved_profile_id = _resolve_runtime_profile_id(runtime_storage, profile_id)
    if not runtime_storage or not resolved_profile_id:
        return False
    external_account = runtime_storage.get_external_account(
        resolved_profile_id, ASC_PROVIDER
    )
    if not is_external_account_expired(external_account):
        return False
    update_session(
        db, chat_id, **_build_external_expired_pause_fields(now or time.time())
    )
    return True


async def maybe_send_check(
    client, db, chat_id, *, force=False, storage=None, profile_id=None
):
    session = get_session(db, chat_id)
    if not session or not session["enabled"]:
        return False, "disabled"
    if session.get("stopped_reason"):
        return False, "stopped"

    now = time.time()
    resolved_storage = storage or getattr(client, "_tg_game_storage", None)
    if _pause_if_external_session_expired(
        db,
        chat_id,
        storage=resolved_storage,
        profile_id=profile_id,
        now=now,
    ):
        return False, "external_expired"
    if not force and session["next_check_time"] and now < session["next_check_time"]:
        return False, "not_due"
    if (
        not force
        and session["last_command_time"]
        and now - session["last_command_time"] < FANREN_COMMAND_COOLDOWN
    ):
        return False, "cooldown"

    command_text, is_status_check = resolve_cycle_command(session)
    next_check_time = compute_cycle_next_check(
        time.time(), session, is_status_check=is_status_check
    )
    if session["dry_run"]:
        update_session(
            db,
            chat_id,
            last_action=f"dry-run:{command_text}",
            last_action_time=now,
            next_check_time=next_check_time,
            next_check_source=f"dry-run 已模拟发送 {command_text}",
            last_summary=f"dry-run 模式，未实际发送指令: {command_text}",
        )
        return True, "dry_run"

    await send_message_in_session(
        client,
        session,
        chat_id,
        command_text,
        storage=storage,
        profile_id=profile_id,
    )
    update_session(
        db,
        chat_id,
        last_command_time=now,
        last_action=command_text,
        last_action_time=now,
        next_check_time=next_check_time,
        next_check_source=f"已发送 {command_text}，等待机器人回复",
        last_summary=f"已发送自动指令: {command_text}",
    )
    logger.info("Fanren cycle command sent to chat %s: %s", chat_id, command_text)
    return True, "sent"


def build_cycle_command(session):
    mode = (session.get("retreat_mode") or FANREN_DEFAULT_MODE).lower()
    if mode == "deep":
        return FANREN_DEEP_COMMAND
    return FANREN_NORMAL_COMMAND


def build_check_command(session):
    command_text = (session.get("command_text") or FANREN_CHECK_COMMAND).strip()
    return command_text or FANREN_CHECK_COMMAND


def has_pending_deep_settlement(session):
    if not session:
        return False
    last_event = (session.get("last_event") or "").strip()
    if last_event in FANREN_DEEP_PENDING_EVENTS:
        return True
    last_action = (session.get("last_action") or "").strip()
    if last_action == build_check_command(session):
        return last_event not in FANREN_DEEP_RESOLVED_EVENTS
    return False


def resolve_cycle_command(session):
    if has_pending_deep_settlement(session):
        return build_check_command(session), True
    return build_cycle_command(session), False


def compute_cycle_next_check(now, session, *, is_status_check=False):
    if is_status_check:
        return now + (session.get("interval_seconds") or FANREN_DEFAULT_INTERVAL)
    mode = (session.get("retreat_mode") or FANREN_DEFAULT_MODE).lower()
    if mode == "deep":
        return now + (session.get("interval_seconds") or FANREN_DEFAULT_INTERVAL)
    return now + max(FANREN_COMMAND_COOLDOWN, FANREN_MIN_INTERVAL)


def normal_retry_seconds(cooldown_seconds, fallback_seconds):
    base = cooldown_seconds or fallback_seconds
    return max(int(base), 0) + 60


async def send_retreat_command(
    client,
    db,
    chat_id,
    *,
    mode=None,
    bypass_cooldown=False,
    storage=None,
    profile_id=None,
):
    session = get_session(db, chat_id)
    now = time.time()
    resolved_storage = storage or getattr(client, "_tg_game_storage", None)
    if _pause_if_external_session_expired(
        db,
        chat_id,
        storage=resolved_storage,
        profile_id=profile_id,
        now=now,
    ):
        return False, "external_expired"
    if not bypass_cooldown and session.get("last_command_time"):
        if now - session["last_command_time"] < FANREN_COMMAND_COOLDOWN:
            return False, "cooldown"

    retreat_mode = (mode or session.get("retreat_mode") or FANREN_DEFAULT_MODE).lower()
    command_text = (
        FANREN_DEEP_COMMAND if retreat_mode == "deep" else FANREN_NORMAL_COMMAND
    )
    if session.get("dry_run"):
        update_session(
            db,
            chat_id,
            last_action=f"dry-run:{command_text}",
            last_action_time=now,
            last_summary=f"dry-run 模式，模拟发送 {command_text}",
            next_check_time=compute_cycle_next_check(now, session),
            next_check_source=f"dry-run 已模拟发送 {command_text}",
        )
        return True, "dry_run"

    await send_message_in_session(
        client,
        session,
        chat_id,
        command_text,
        storage=storage,
        profile_id=profile_id,
    )
    update_session(
        db,
        chat_id,
        last_command_time=now,
        last_action=command_text,
        last_action_time=now,
        last_summary=f"已发送闭关指令: {command_text}",
        next_check_time=compute_cycle_next_check(now, session),
        next_check_source=f"已发送 {command_text}，等待机器人回复",
    )
    logger.info("Fanren retreat command sent to chat %s: %s", chat_id, command_text)
    return True, "sent"


async def handle_bot_message(event, db, client=None):
    sender = await event.get_sender()
    username = (getattr(sender, "username", "") or "").lower()
    if username != FANREN_BOT_USERNAME:
        return None

    session = get_session(db, event.chat_id)
    if not session or not session["enabled"]:
        return None
    raw_text = (event.raw_text or "").strip()
    last_bot_text = (session.get("last_bot_text") or "").strip()
    if session["last_bot_msg_id"] == event.id and last_bot_text == raw_text[:1000]:
        return None

    parsed = parse_message(raw_text)
    if parsed.event == "ignored":
        return None
    retreat_mode = (session.get("retreat_mode") or FANREN_DEFAULT_MODE).lower()
    now = time.time()
    next_check = session.get("next_check_time") or 0
    if parsed.cooldown_seconds:
        next_check = now + parsed.cooldown_seconds
    elif parsed.event not in {"unknown", "blocked", "resource_blocked"}:
        next_check = now + session["interval_seconds"]
    if session.get("last_action") == (
        session.get("command_text") or FANREN_CHECK_COMMAND
    ):
        if parsed.cooldown_seconds:
            next_check = now + parsed.cooldown_seconds
    failure_count = (
        0
        if parsed.event not in FANREN_FAILURE_EVENTS
        else int(session["failure_count"] or 0) + 1
    )
    update_session(
        db,
        event.chat_id,
        last_event=parsed.event,
        last_summary=parsed.summary,
        last_bot_text=raw_text[:1000],
        last_bot_msg_id=event.id,
        next_check_time=next_check,
        next_check_source=parsed.summary,
        failure_count=failure_count,
        stopped_reason=None
        if parsed.event not in FANREN_FAILURE_EVENTS
        else session.get("stopped_reason"),
    )
    should_resume_after_deep_settlement = (
        parsed.event in FANREN_DEEP_RESOLVED_EVENTS
        and client is not None
        and (
            has_pending_deep_settlement(session)
            or (session.get("last_action") or "").strip()
            == build_check_command(session)
        )
    )
    if should_resume_after_deep_settlement:
        await send_retreat_command(
            client,
            db,
            event.chat_id,
            mode=retreat_mode,
            bypass_cooldown=True,
            storage=getattr(client, "_tg_game_storage", None),
        )
    if retreat_mode == "normal":
        if parsed.event == "retreat_complete" and parsed.cooldown_seconds:
            wait_seconds = normal_retry_seconds(
                parsed.cooldown_seconds, session["interval_seconds"]
            )
            update_session(
                db,
                event.chat_id,
                next_check_time=now + wait_seconds,
                next_check_source=f"普通闭关完成冷却 {format_duration(wait_seconds)}",
                last_summary=f"普通闭关完成，下次将在 {format_duration(wait_seconds)} 后尝试",
            )
        elif parsed.event == "retreat_setback" and parsed.cooldown_seconds:
            wait_seconds = normal_retry_seconds(
                parsed.cooldown_seconds, session["interval_seconds"]
            )
            update_session(
                db,
                event.chat_id,
                next_check_time=now + wait_seconds,
                next_check_source=f"普通闭关受挫后等待 {format_duration(wait_seconds)}",
                last_summary=f"普通闭关受挫，下次将在 {format_duration(wait_seconds)} 后尝试",
            )
        elif parsed.event == "cooldown" and parsed.cooldown_seconds:
            wait_seconds = normal_retry_seconds(
                parsed.cooldown_seconds, session["interval_seconds"]
            )
            update_session(
                db,
                event.chat_id,
                next_check_time=now + wait_seconds,
                next_check_source=f"普通闭关冷却 {format_duration(wait_seconds)}",
                last_summary=f"普通闭关冷却中，还需 {format_duration(wait_seconds)}",
            )
        elif parsed.event in {"deep_cultivating", "deep_started"}:
            wait_seconds = normal_retry_seconds(
                parsed.cooldown_seconds, session["interval_seconds"]
            )
            update_session(
                db,
                event.chat_id,
                next_check_time=now + wait_seconds,
                next_check_source=f"深度闭关占用中，等待 {format_duration(wait_seconds)}",
                last_summary=f"当前处于深度闭关中，普通闭关将在 {format_duration(wait_seconds)} 后重试",
            )
    if failure_count >= FANREN_MAX_FAILURES:
        trip_circuit_breaker(
            db,
            event.chat_id,
            f"收到机器人失败事件 {parsed.event}，连续 {failure_count} 次",
        )
    logger.info(
        "Fanren event in chat %s: %s (%s)", event.chat_id, parsed.event, parsed.summary
    )
    return parsed


async def runner(client, storage):
    while True:
        try:
            db = RuntimeDb(storage)
            now = time.time()
            active_profile = storage.get_active_profile()
            for session in list_sessions(db):
                if not session["enabled"]:
                    continue
                if session.get("stopped_reason"):
                    continue
                if session["next_check_time"] and now < session["next_check_time"]:
                    continue
                try:
                    logger.info(
                        "Fanren runner due chat=%s mode=%s next_check=%s now=%s",
                        session["chat_id"],
                        session.get("retreat_mode"),
                        format_timestamp(session.get("next_check_time") or 0),
                        format_timestamp(now),
                    )
                    await maybe_send_check(
                        client,
                        db,
                        session["chat_id"],
                        storage=storage,
                        profile_id=active_profile.id if active_profile else None,
                    )
                except Exception as exc:
                    record_failure(db, session["chat_id"], f"check failed: {exc}")
                    update_session(
                        db,
                        session["chat_id"],
                        next_check_time=now + max(session["interval_seconds"], 60),
                        next_check_source="runner 异常后退避等待",
                    )
                    logger.warning(
                        "Fanren runner failed in chat %s: %s", session["chat_id"], exc
                    )
            db.close()
            await asyncio.sleep(FANREN_RUNNER_POLL_SECONDS)
        except Exception as exc:
            logger.exception("Fanren runner error: %s", exc)
            await asyncio.sleep(10)
