import asyncio
import logging
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


FANREN_BOT_USERNAME = "fanrenxiuxian_bot"
FANREN_CHECK_COMMAND = ".查看闭关"
FANREN_NORMAL_COMMAND = ".闭关修炼"
FANREN_DEEP_COMMAND = ".深度闭关"
FANREN_DEFAULT_MODE = "normal"
FANREN_DEFAULT_INTERVAL = 300
FANREN_COMMAND_COOLDOWN = 30
FANREN_MAX_FAILURES = 3
FANREN_MIN_INTERVAL = 30
FANREN_RUNNER_POLL_SECONDS = 5
FANREN_REPLY_SYNC_GRACE_SECONDS = 300
FANREN_AUTO_JIYIN_KEYWORD = "神念直入脑海，一个苍老的声音"
FANREN_AUTO_JIYIN_KEYWORDS = [
    FANREN_AUTO_JIYIN_KEYWORD,
    "极阴祖师",
    "无尽魔海的恐怖神识扫过天地",
    "他的目光，似乎在修士",
    "乱星海霸主【极阴祖师】",
]
FANREN_AUTO_NANLONG_KEYWORD = "你感到一股无法抗拒的意志锁定了你的神魂"
FANREN_AUTO_JIYIN_CHOICES = {
    "献上魂魄": ".献上魂魄",
    "收敛气息": ".收敛气息",
}

# 自动探寻裂缝
RIFT_EXPLORE_COMMAND = ".探寻裂缝"
RIFT_EXPLORE_COOLDOWN_SECONDS = 43200  # 12 小时
RIFT_RETRY_INTERVAL_SECONDS = 600  # 10 分钟
RIFT_RETRY_MAX = 1
RIFT_SUCCESS_KEYWORD = "探寻裂缝"  # 成功回包关键词

# 自动元婴出窍
YUANYING_STATUS_COMMAND = ".元婴状态"
YUANYING_OUTING_COMMAND = ".元婴出窍"
YUANYING_OUTING_COOLDOWN_SECONDS = 28800  # 8 小时（兜底值，优先从回包解析）
YUANYING_SUCCESS_KEYWORDS = [
    "元婴化作一道流光飞出",
    "元婴出窍",
    "云游",
]
# 元婴状态回包中表示已归来/结算完成的关键词
YUANYING_RETURNED_KEYWORDS = [
    "归窍总结",
    "元神归窍",
    "已归来",
]
# 元婴状态回包中表示元婴在家可直接出窍的关键词
YUANYING_READY_KEYWORDS = [
    "窍中温养",
]
# 元婴状态回包中表示元婴仍在外的关键词
YUANYING_STILL_OUT_KEYWORDS = [
    "无法分身",
    "正在执行",
]
FANREN_AUTO_NANLONG_CHOICES = {
    "交换 法宝": ".交换 法宝",
    "交换 功法": ".交换 功法",
    "拒绝交易": ".拒绝交易",
}


def _normalize_special_choice(choice: str) -> str:
    return str(choice or "").strip().lstrip(".").strip()


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
            profile_id INTEGER NOT NULL DEFAULT 0,
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
            auto_jiyin_enabled INTEGER DEFAULT 0,
            auto_jiyin_choice TEXT DEFAULT '',
            auto_nanlong_enabled INTEGER DEFAULT 0,
            auto_nanlong_choice TEXT DEFAULT '',
            PRIMARY KEY (profile_id, chat_id, bot_username)
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
    if "auto_jiyin_enabled" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN auto_jiyin_enabled INTEGER DEFAULT 0"
        )
    if "auto_jiyin_choice" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN auto_jiyin_choice TEXT DEFAULT ''"
        )
    if "auto_nanlong_enabled" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN auto_nanlong_enabled INTEGER DEFAULT 0"
        )
    if "auto_nanlong_choice" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN auto_nanlong_choice TEXT DEFAULT ''"
        )
    if "profile_id" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN profile_id INTEGER NOT NULL DEFAULT 0"
        )
    if "auto_rift_enabled" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN auto_rift_enabled INTEGER DEFAULT 0"
        )
    if "rift_next_check_time" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN rift_next_check_time REAL DEFAULT 0"
        )
    if "rift_retry_count" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN rift_retry_count INTEGER DEFAULT 0"
        )
    if "rift_state" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN rift_state TEXT DEFAULT ''"
        )
    if "auto_yuanying_enabled" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN auto_yuanying_enabled INTEGER DEFAULT 0"
        )
    if "yuanying_next_check_time" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN yuanying_next_check_time REAL DEFAULT 0"
        )
    if "yuanying_state" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN yuanying_state TEXT DEFAULT ''"
        )
    if "rift_last_asc_time" not in columns:
        db.cur.execute(
            "ALTER TABLE fanren_sessions ADD COLUMN rift_last_asc_time TEXT DEFAULT ''"
        )
    db.conn.commit()


def ensure_session(db, chat_id, bot_username=FANREN_BOT_USERNAME, profile_id=None):
    ensure_tables(db)
    resolved_profile_id = int(profile_id or 0)
    db.cur.execute(
        """
        INSERT OR IGNORE INTO fanren_sessions
            (profile_id, chat_id, bot_username, interval_seconds, command_text, retreat_mode)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            resolved_profile_id,
            chat_id,
            bot_username,
            FANREN_DEFAULT_INTERVAL,
            FANREN_CHECK_COMMAND,
            FANREN_DEFAULT_MODE,
        ),
    )
    if resolved_profile_id:
        db.cur.execute(
            "UPDATE fanren_sessions SET profile_id=? WHERE chat_id=? AND bot_username=? AND (profile_id IS NULL OR profile_id=0)",
            (resolved_profile_id, chat_id, bot_username),
        )
    db.conn.commit()


def get_session(db, chat_id, bot_username=FANREN_BOT_USERNAME, profile_id=None):
    ensure_session(db, chat_id, bot_username, profile_id=profile_id)
    resolved_profile_id = int(profile_id or 0)
    if resolved_profile_id:
        db.cur.execute(
            "SELECT * FROM fanren_sessions WHERE profile_id=? AND chat_id=? AND bot_username=?",
            (resolved_profile_id, chat_id, bot_username),
        )
    else:
        db.cur.execute(
            "SELECT * FROM fanren_sessions WHERE chat_id=? AND bot_username=? ORDER BY profile_id DESC LIMIT 1",
            (chat_id, bot_username),
        )
    row = db.cur.fetchone()
    return dict(zip([col[0] for col in db.cur.description], row)) if row else None


def update_session(
    db, chat_id, bot_username=FANREN_BOT_USERNAME, profile_id=None, **fields
):
    if not fields:
        return
    ensure_session(db, chat_id, bot_username, profile_id=profile_id)
    resolved_profile_id = int(profile_id or 0)
    assignments = ", ".join(f"{key}=?" for key in fields)
    if resolved_profile_id:
        values = list(fields.values()) + [resolved_profile_id, chat_id, bot_username]
        db.cur.execute(
            f"UPDATE fanren_sessions SET {assignments} WHERE profile_id=? AND chat_id=? AND bot_username=?",
            values,
        )
    else:
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


def list_sessions(db, profile_id=None):
    ensure_tables(db)
    if profile_id:
        db.cur.execute(
            "SELECT * FROM fanren_sessions WHERE profile_id=? ORDER BY chat_id",
            (int(profile_id),),
        )
    else:
        db.cur.execute("SELECT * FROM fanren_sessions ORDER BY profile_id, chat_id")
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
            f"自动极阴祖师: {'开启' if session.get('auto_jiyin_enabled') else '关闭'} / {session.get('auto_jiyin_choice') or '-'}",
            f"自动南陇侯: {'开启' if session.get('auto_nanlong_enabled') else '关闭'} / {session.get('auto_nanlong_choice') or '-'}",
            f"自动探寻裂缝: {'开启' if session.get('auto_rift_enabled') else '关闭'}",
            f"  裂缝状态: {session.get('rift_state') or '-'}",
            f"  裂缝下次: {format_timestamp(session.get('rift_next_check_time') or 0)}",
            f"  裂缝重试: {session.get('rift_retry_count') or 0}/{RIFT_RETRY_MAX}",
            f"自动元婴出窍: {'开启' if session.get('auto_yuanying_enabled') else '关闭'}",
            f"  出窍状态: {session.get('yuanying_state') or '-'}",
            f"  出窍下次: {format_timestamp(session.get('yuanying_next_check_time') or 0)}",
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


def set_enabled(db, chat_id, enabled, *, reset_failure=False, profile_id=None):
    fields = {"enabled": _normalize_bool(enabled)}
    if enabled:
        # Keep the schedule that was just synced from Tianjige instead of
        # forcing an immediate send on enable.
        fields["stopped_reason"] = None
    if reset_failure:
        fields["failure_count"] = 0
    update_session(db, chat_id, profile_id=profile_id, **fields)


def reset_runtime_state(db, chat_id, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
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


def set_dry_run(db, chat_id, enabled, profile_id=None):
    update_session(db, chat_id, profile_id=profile_id, dry_run=_normalize_bool(enabled))


def set_interval(db, chat_id, interval_seconds, profile_id=None):
    interval_seconds = clamp_interval(interval_seconds)
    update_session(
        db, chat_id, profile_id=profile_id, interval_seconds=interval_seconds
    )
    return interval_seconds


def set_check_command(db, chat_id, command_text, profile_id=None):
    command_text = (command_text or "").strip()
    if not command_text:
        raise ValueError("检查指令不能为空")
    update_session(db, chat_id, profile_id=profile_id, command_text=command_text)
    return command_text


def set_mode(db, chat_id, retreat_mode, preserve_next_check_time=0, profile_id=None):
    retreat_mode = (retreat_mode or "").strip().lower()
    if retreat_mode not in {"normal", "deep"}:
        raise ValueError("模式只支持 normal 或 deep")
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        retreat_mode=retreat_mode,
        next_check_time=preserve_next_check_time or 0,
        next_check_source=(
            "从深度闭关同步剩余倒计时" if preserve_next_check_time else None
        ),
        stopped_reason=None,
    )
    return retreat_mode


def set_delete_normal_command_message(db, chat_id, enabled, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        delete_normal_command_message=_normalize_bool(enabled),
    )
    return bool(_normalize_bool(enabled))


def set_auto_jiyin(db, chat_id, enabled, choice, profile_id=None):
    normalized_choice = _normalize_special_choice(choice)
    if enabled and normalized_choice not in FANREN_AUTO_JIYIN_CHOICES:
        raise ValueError("极阴祖师自动选项无效")
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_jiyin_enabled=_normalize_bool(enabled),
        auto_jiyin_choice=normalized_choice,
    )
    return normalized_choice


def set_auto_nanlong(db, chat_id, enabled, choice, profile_id=None):
    normalized_choice = _normalize_special_choice(choice)
    if enabled and normalized_choice not in FANREN_AUTO_NANLONG_CHOICES:
        raise ValueError("南陇侯自动选项无效")
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_nanlong_enabled=_normalize_bool(enabled),
        auto_nanlong_choice=normalized_choice,
    )
    return normalized_choice


def set_auto_rift(db, chat_id, enabled, *, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_rift_enabled=_normalize_bool(enabled),
        rift_state="" if enabled else "已关闭",
    )
    if enabled:
        update_session(
            db,
            chat_id,
            profile_id=profile_id,
            rift_next_check_time=0,
            rift_retry_count=0,
            rift_state="等待首次执行",
            rift_last_asc_time="",
        )


def set_auto_yuanying(db, chat_id, enabled, *, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        auto_yuanying_enabled=_normalize_bool(enabled),
        yuanying_state="" if enabled else "已关闭",
    )
    if enabled:
        update_session(
            db,
            chat_id,
            profile_id=profile_id,
            yuanying_next_check_time=0,
            yuanying_state="等待首次检查",
        )


def parse_rift_reply(text):
    """解析探寻裂缝回包，返回 (is_success, cooldown_seconds)"""
    text = (text or "").strip()
    if not text:
        return False, None
    cooldown = parse_cooldown_seconds(text)
    if any(kw in text for kw in ["探寻裂缝", "裂缝", "裂隙"]):
        return True, cooldown or RIFT_EXPLORE_COOLDOWN_SECONDS
    if "失败" in text or "无法" in text or "不可" in text:
        return False, cooldown
    return True, cooldown or RIFT_EXPLORE_COOLDOWN_SECONDS


def parse_yuanying_reply(text):
    """解析元婴出窍回包，返回 (is_success, cooldown_seconds)"""
    text = (text or "").strip()
    if not text:
        return False, None
    cooldown = parse_cooldown_seconds(text)
    for kw in YUANYING_SUCCESS_KEYWORDS:
        if kw in text:
            return True, cooldown or YUANYING_OUTING_COOLDOWN_SECONDS
    if "失败" in text or "无法" in text or "不可" in text:
        return False, cooldown
    return False, cooldown


def parse_yuanying_status_reply(text):
    """解析 .元婴状态 回包，返回 (status, countdown_seconds)

    status 取值:
      "ready"   - 元婴在家(窍中温养)，可直接出窍
      "settled" - 元婴已归来结算完成
      "out"     - 元婴正在外云游，倒计时仍在
      "unknown" - 无法识别
    """
    text = (text or "").strip()
    if not text:
        return "unknown", None

    # 已归来结算完成
    for kw in YUANYING_RETURNED_KEYWORDS:
        if kw in text:
            return "settled", 0

    # 元婴在家，可直接出窍
    for kw in YUANYING_READY_KEYWORDS:
        if kw in text:
            return "ready", 0

    # 元婴仍在外
    for kw in YUANYING_STILL_OUT_KEYWORDS:
        if kw in text:
            return "out", None

    # 兜底：尝试解析倒计时
    cooldown = parse_cooldown_seconds(text)
    if cooldown:
        return "out", cooldown

    return "unknown", None


def get_rift_failure_lock_reason(payload: dict) -> str:
    status = str((payload or {}).get("status") or "").strip().upper()
    if status == "ESCAPED_SOUL":
        return "元婴遁逃·虚弱（残魂状态），已停止全部自动任务"
    return ""


def stop_all_automation_for_rift_failure(
    db, chat_id, payload: dict, *, profile_id=None
):
    reason = get_rift_failure_lock_reason(payload)
    if not reason:
        return False
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        enabled=0,
        next_check_time=0,
        next_check_source=reason,
        stopped_reason=reason,
        auto_jiyin_enabled=0,
        auto_nanlong_enabled=0,
        auto_rift_enabled=0,
        auto_yuanying_enabled=0,
        rift_state=reason,
        yuanying_state=reason,
        last_event="rift_escaped_soul",
        last_summary=reason,
    )
    return True


async def _check_asc_rift_time_changed(storage, profile_id, chat_id, db):
    """通过天机阁 API 检查 last_rift_explore_time 是否已刷新"""
    try:
        from tg_game.services.external_sync import (
            get_effective_external_cookie,
            sync_external_account,
        )

        cookie_text = get_effective_external_cookie(storage)
        if not cookie_text:
            return False, "天机阁未登录"
        cultivator = sync_external_account(storage, profile_id, cookie_text=cookie_text)
        new_rift_time = (cultivator.get("last_rift_explore_time") or "").strip()
        if not new_rift_time:
            return False, "天机阁未返回探寻裂缝时间"
        session = get_session(db, chat_id, profile_id=profile_id)
        old_rift_time = (session.get("rift_last_asc_time") or "").strip()
        update_session(
            db,
            chat_id,
            profile_id=profile_id,
            rift_last_asc_time=new_rift_time,
        )
        changed = new_rift_time != old_rift_time
        # 截短时间戳: "2026-04-30T05:08:58..." → "04-30 05:08"
        try:
            clean = new_rift_time.replace("T", " ")[:16]
            if "+" in clean:
                clean = clean[: clean.index("+")].strip()
            short_time = clean
        except Exception:
            short_time = new_rift_time[:16]
        return (
            changed,
            f"新: {short_time}" if changed else f"未变: {short_time}",
            cultivator if isinstance(cultivator, dict) else {},
        )
    except Exception as exc:
        logger.warning("ASC rift time check failed: %s", exc)
        return False, f"天机阁查询失败: {exc}", {}


async def _maybe_send_rift_explore(
    client, db, chat_id, *, storage=None, profile_id=None
):
    """处理自动探寻裂缝发送"""
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session or not session.get("auto_rift_enabled"):
        return False, "disabled"

    now = time.time()
    next_check = session.get("rift_next_check_time") or 0
    if next_check and now < next_check:
        return False, "not_due"

    resolved_storage = storage or getattr(client, "_tg_game_storage", None)
    if _pause_if_external_session_expired(
        db, chat_id, storage=resolved_storage, profile_id=profile_id, now=now
    ):
        return False, "external_expired"

    retry_count = int(session.get("rift_retry_count") or 0)
    update_session(
        db,
        chat_id,
        profile_id=session.get("profile_id"),
        rift_state=f"准备发送{'(重试)' if retry_count > 0 else ''}",
    )

    await send_message_in_session(
        client,
        session,
        chat_id,
        RIFT_EXPLORE_COMMAND,
        storage=storage,
        profile_id=profile_id,
    )
    update_session(
        db,
        chat_id,
        profile_id=session.get("profile_id"),
        last_action=RIFT_EXPLORE_COMMAND,
        last_action_time=now,
        rift_next_check_time=now + RIFT_EXPLORE_COOLDOWN_SECONDS,
        rift_state=f"已发送{'(第' + str(retry_count) + '次重试)' if retry_count > 0 else ''}，等待回包验证",
    )
    logger.info("Rift explore command sent to chat %s retry=%s", chat_id, retry_count)
    return True, "sent"


async def _maybe_send_yuanying_outing(
    client, db, chat_id, *, storage=None, profile_id=None
):
    """处理自动元婴出窍状态机

    状态流转:
      等待首次检查 → 发送 .元婴状态 → 解析倒计时 → 等待归来: X小时
      等待归来 (倒计时到期) → 发送 .元婴状态 (结算) → 结算完成 → 发送 .元婴出窍
      出窍成功 → 解析CD → 等待归来: X小时 → 循环
    """
    session = get_session(db, chat_id, profile_id=profile_id)
    if not session or not session.get("auto_yuanying_enabled"):
        return False, "disabled"

    now = time.time()
    state = (session.get("yuanying_state") or "").strip()
    next_check = session.get("yuanying_next_check_time") or 0

    resolved_storage = storage or getattr(client, "_tg_game_storage", None)
    if _pause_if_external_session_expired(
        db, chat_id, storage=resolved_storage, profile_id=profile_id, now=now
    ):
        return False, "external_expired"

    pid = session.get("profile_id")

    # Phase 1: 首次启动，先发 .元婴状态 确认倒计时
    if state == "等待首次检查":
        update_session(db, chat_id, profile_id=pid, yuanying_state="状态检查中")
        await send_message_in_session(
            client,
            session,
            chat_id,
            YUANYING_STATUS_COMMAND,
            storage=storage,
            profile_id=profile_id,
        )
        update_session(
            db,
            chat_id,
            profile_id=pid,
            last_action=YUANYING_STATUS_COMMAND,
            last_action_time=now,
            yuanying_state="状态检查中",
        )
        logger.info("Yuanying status check sent to chat %s (initial)", chat_id)
        return True, "checking"

    # Phase 2: 倒计时到期，发 .元婴状态 结算
    if (
        (state.startswith("等待归来") or state == "状态检查中")
        and next_check
        and now >= next_check
    ):
        update_session(db, chat_id, profile_id=pid, yuanying_state="结算指令已发送")
        await send_message_in_session(
            client,
            session,
            chat_id,
            YUANYING_STATUS_COMMAND,
            storage=storage,
            profile_id=profile_id,
        )
        update_session(
            db,
            chat_id,
            profile_id=pid,
            last_action=YUANYING_STATUS_COMMAND,
            last_action_time=now,
            yuanying_state="结算指令已发送",
        )
        logger.info("Yuanying settlement check sent to chat %s", chat_id)
        return True, "settling"

    # Phase 3: 结算完成，发 .元婴出窍
    if state == "结算完成":
        update_session(db, chat_id, profile_id=pid, yuanying_state="出窍指令已发送")
        await send_message_in_session(
            client,
            session,
            chat_id,
            YUANYING_OUTING_COMMAND,
            storage=storage,
            profile_id=profile_id,
        )
        update_session(
            db,
            chat_id,
            profile_id=pid,
            last_action=YUANYING_OUTING_COMMAND,
            last_action_time=now,
            yuanying_next_check_time=now + YUANYING_OUTING_COOLDOWN_SECONDS,
            yuanying_state="出窍指令已发送",
        )
        logger.info("Yuanying outing command sent to chat %s", chat_id)
        return True, "outing"

    # 等待状态且未到期：不做任何事
    if state.startswith("等待归来") and next_check and now < next_check:
        return False, "not_due"

    # 异常/未知状态：回退到状态检查
    if state in ("已关闭", "", "回包异常，将重试", "结算指令已发送", "出窍指令已发送"):
        update_session(
            db,
            chat_id,
            profile_id=pid,
            yuanying_state="等待首次检查",
            yuanying_next_check_time=0,
        )
        return False, "reset_to_initial"

    return False, "unknown_state"


async def maybe_handle_special_auto_event(
    event, db, session, client, *, storage=None, profile_id=None
):
    raw_text = (getattr(event, "raw_text", "") or "").strip()
    if not raw_text or client is None:
        return False
    auto_command = ""
    auto_label = ""
    is_jiyin_event = any(keyword in raw_text for keyword in FANREN_AUTO_JIYIN_KEYWORDS)
    if is_jiyin_event and session.get("auto_jiyin_enabled"):
        choice = _normalize_special_choice(session.get("auto_jiyin_choice") or "")
        auto_command = FANREN_AUTO_JIYIN_CHOICES.get(choice, "")
        auto_label = f"极阴祖师 → {choice}" if auto_command else ""
    elif FANREN_AUTO_NANLONG_KEYWORD in raw_text and session.get(
        "auto_nanlong_enabled"
    ):
        choice = _normalize_special_choice(session.get("auto_nanlong_choice") or "")
        auto_command = FANREN_AUTO_NANLONG_CHOICES.get(choice, "")
        auto_label = f"南陇侯 → {choice}" if auto_command else ""
    if not auto_command:
        return False
    try:
        await event.reply(f"自动应对：{auto_label}")
    except Exception:
        logger.warning("Special auto event reply notice failed", exc_info=True)
    await send_message_in_session(
        client,
        session,
        event.chat_id,
        auto_command,
        storage=storage or getattr(client, "_tg_game_storage", None),
        profile_id=profile_id,
    )
    update_session(
        db,
        event.chat_id,
        profile_id=session.get("profile_id"),
        last_action=auto_command,
        last_action_time=time.time(),
        last_summary=f"已自动应对 {auto_label}",
    )
    return True


async def maybe_delete_normal_command_message(
    event, session, client, reply_text, reply_message_id=None
):
    if client is None or not session:
        return False
    if (session.get("retreat_mode") or FANREN_DEFAULT_MODE).lower() != "normal":
        return False
    if not bool(session.get("delete_normal_command_message")):
        return False
    normalized_reply_text = (reply_text or "").strip()
    if normalized_reply_text and normalized_reply_text != FANREN_NORMAL_COMMAND:
        return False
    if (
        not normalized_reply_text
        and (session.get("last_action") or "").strip() != FANREN_NORMAL_COMMAND
    ):
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


def reset_failures(db, chat_id, profile_id=None):
    update_session(
        db, chat_id, profile_id=profile_id, failure_count=0, stopped_reason=None
    )


def trip_circuit_breaker(db, chat_id, reason, profile_id=None):
    update_session(
        db,
        chat_id,
        profile_id=profile_id,
        enabled=0,
        stopped_reason=reason,
        next_check_time=0,
    )
    logger.warning("Fanren circuit breaker tripped in chat %s: %s", chat_id, reason)


def record_failure(db, chat_id, reason, profile_id=None):
    session = get_session(db, chat_id, profile_id=profile_id)
    failure_count = int(session.get("failure_count") or 0) + 1
    update_session(
        db,
        chat_id,
        profile_id=session.get("profile_id"),
        failure_count=failure_count,
        last_summary=reason,
    )
    if failure_count >= FANREN_MAX_FAILURES:
        trip_circuit_breaker(
            db,
            chat_id,
            f"连续失败达到 {failure_count} 次: {reason}",
            profile_id=session.get("profile_id"),
        )
    return failure_count


def _resolve_runtime_profile_id(storage=None, profile_id=None):
    if profile_id:
        return int(profile_id)
    return None


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
        db,
        chat_id,
        profile_id=resolved_profile_id,
        **_build_external_expired_pause_fields(now or time.time()),
    )
    return True


async def maybe_send_check(
    client, db, chat_id, *, force=False, storage=None, profile_id=None
):
    session = get_session(db, chat_id, profile_id=profile_id)
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
            profile_id=session.get("profile_id"),
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
        profile_id=session.get("profile_id"),
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
    base_interval = int(session.get("interval_seconds") or FANREN_DEFAULT_INTERVAL)
    # 所有路径都强制最小间隔，避免深度/状态检查路径因 interval 过小导致连续发送
    min_interval = max(base_interval, FANREN_COMMAND_COOLDOWN, FANREN_MIN_INTERVAL)
    return now + min_interval


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
    session = get_session(db, chat_id, profile_id=profile_id)
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
            profile_id=session.get("profile_id"),
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
        profile_id=session.get("profile_id"),
        last_command_time=now,
        last_action=command_text,
        last_action_time=now,
        last_summary=f"已发送闭关指令: {command_text}",
        next_check_time=compute_cycle_next_check(now, session),
        next_check_source=f"已发送 {command_text}，等待机器人回复",
    )
    logger.info("Fanren retreat command sent to chat %s: %s", chat_id, command_text)
    return True, "sent"


async def handle_bot_message(event, db, client=None, profile_id=None):
    sender = await event.get_sender()
    username = (getattr(sender, "username", "") or "").lower()
    if username != FANREN_BOT_USERNAME:
        return None

    session = get_session(db, event.chat_id, profile_id=profile_id)
    if not session or not session["enabled"]:
        return None
    raw_text = (event.raw_text or "").strip()
    last_bot_text = (session.get("last_bot_text") or "").strip()
    if session["last_bot_msg_id"] == event.id and last_bot_text == raw_text[:1000]:
        return None

    # 检测编辑：同一条 bot 消息被编辑，只更新文本不重跑状态机
    is_edit = (
        session.get("last_bot_msg_id") == event.id and raw_text[:1000] != last_bot_text
    )
    if is_edit:
        last_event = session.get("last_event") or ""
        update_session(
            db,
            event.chat_id,
            profile_id=session.get("profile_id"),
            last_bot_text=raw_text[:1000],
            last_bot_msg_id=event.id,
        )
        # 仅当是探寻裂缝或元婴相关事件时返回结果以更新闭关记录
        if any(last_event.startswith(p) for p in ("rift_", "yuanying_")):
            return FanrenParseResult(
                f"{last_event}_edited", f"[已编辑] {raw_text[:120]}"
            )
        return None

    # 处理自动探寻裂缝回包
    last_action = (session.get("last_action") or "").strip()
    if last_action == RIFT_EXPLORE_COMMAND and session.get("auto_rift_enabled"):
        rift_success, rift_cd = parse_rift_reply(raw_text)
        now = time.time()
        if rift_success:
            # 通过天机阁 API 验证 last_rift_explore_time 是否刷新
            storage = (
                getattr(client, "_tg_game_storage", None)
                if client is not None
                else None
            )
            if storage and profile_id:
                (
                    changed,
                    check_msg,
                    cultivator_payload,
                ) = await _check_asc_rift_time_changed(
                    storage, profile_id, event.chat_id, db
                )
                if stop_all_automation_for_rift_failure(
                    db,
                    event.chat_id,
                    cultivator_payload,
                    profile_id=session.get("profile_id"),
                ):
                    try:
                        import sect_game

                        sect_game.stop_all_automation(
                            db,
                            event.chat_id,
                            get_rift_failure_lock_reason(cultivator_payload),
                            profile_id=session.get("profile_id"),
                        )
                    except Exception:
                        logger.warning(
                            "Stopping sect automation for rift failure failed",
                            exc_info=True,
                        )
                    return FanrenParseResult(
                        "rift_escaped_soul",
                        get_rift_failure_lock_reason(cultivator_payload),
                    )
                if changed:
                    update_session(
                        db,
                        event.chat_id,
                        profile_id=session.get("profile_id"),
                        rift_next_check_time=now
                        + (rift_cd or RIFT_EXPLORE_COOLDOWN_SECONDS),
                        rift_retry_count=0,
                        rift_state=f"探寻成功 - {check_msg}",
                        last_summary=f"自动探寻裂缝成功，下次在 {format_duration(rift_cd or RIFT_EXPLORE_COOLDOWN_SECONDS)} 后",
                        last_event="rift_explore_success",
                        last_bot_text=raw_text[:1000],
                        last_bot_msg_id=event.id,
                    )
                    logger.info(
                        "Rift explore succeeded in chat %s: %s",
                        event.chat_id,
                        check_msg,
                    )
                    return FanrenParseResult(
                        "rift_explore_success", f"探寻裂缝成功", rift_cd
                    )
                else:
                    retry_count = int(session.get("rift_retry_count") or 0) + 1
                    if retry_count <= RIFT_RETRY_MAX:
                        update_session(
                            db,
                            event.chat_id,
                            profile_id=session.get("profile_id"),
                            rift_next_check_time=now
                            + (rift_cd or RIFT_RETRY_INTERVAL_SECONDS),
                            rift_retry_count=retry_count,
                            rift_state=f"将第{retry_count}次重试 - {check_msg}",
                            last_summary=f"探寻裂缝时间未刷新，将在 {format_duration(RIFT_RETRY_INTERVAL_SECONDS)} 后重试(第{retry_count}次)",
                            last_event="rift_explore_retry",
                            last_bot_text=raw_text[:1000],
                            last_bot_msg_id=event.id,
                        )
                        logger.info(
                            "Rift explore will retry in chat %s (attempt %s)",
                            event.chat_id,
                            retry_count,
                        )
                    else:
                        update_session(
                            db,
                            event.chat_id,
                            profile_id=session.get("profile_id"),
                            auto_rift_enabled=0,
                            rift_state=f"失败已停止 - {check_msg}",
                            last_summary=f"探寻裂缝失败: {check_msg}，超过最大重试次数已停止自动探寻",
                            last_event="rift_explore_failed",
                            last_bot_text=raw_text[:1000],
                            last_bot_msg_id=event.id,
                        )
                        logger.warning(
                            "Rift explore stopped in chat %s: %s",
                            event.chat_id,
                            check_msg,
                        )
                    return FanrenParseResult(
                        "rift_explore_retry", f"探寻裂缝: {check_msg}"
                    )
            else:
                # 无法调用 ASC API 时，默认视为成功
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    rift_next_check_time=now
                    + (rift_cd or RIFT_EXPLORE_COOLDOWN_SECONDS),
                    rift_retry_count=0,
                    rift_state="探寻完成(无法验证)",
                    last_summary=f"自动探寻裂缝完成(无法验证天机阁)，下次在 {format_duration(rift_cd or RIFT_EXPLORE_COOLDOWN_SECONDS)} 后",
                    last_event="rift_explore_success",
                    last_bot_text=raw_text[:1000],
                    last_bot_msg_id=event.id,
                )
                return FanrenParseResult("rift_explore_success", "探寻裂缝完成")
        else:
            retry_count = int(session.get("rift_retry_count") or 0) + 1
            if retry_count <= RIFT_RETRY_MAX:
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    rift_next_check_time=now + RIFT_RETRY_INTERVAL_SECONDS,
                    rift_retry_count=retry_count,
                    rift_state=f"回包失败，将重试(第{retry_count}次)",
                    last_summary=f"探寻裂缝回包异常，将在 {format_duration(RIFT_RETRY_INTERVAL_SECONDS)} 后重试",
                    last_event="rift_explore_retry",
                )
            else:
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    auto_rift_enabled=0,
                    rift_state="失败已停止",
                    last_summary=f"探寻裂缝连续失败，已停止自动探寻",
                    last_event="rift_explore_failed",
                )
            return FanrenParseResult("rift_explore_retry", "探寻裂缝回包异常")

    # 处理自动元婴出窍 / 元婴状态 回包
    if session.get("auto_yuanying_enabled") and last_action in {
        YUANYING_STATUS_COMMAND,
        YUANYING_OUTING_COMMAND,
    }:
        yy_state = (session.get("yuanying_state") or "").strip()
        now_yy = time.time()

        # ---- .元婴状态 回包处理 ----
        if last_action == YUANYING_STATUS_COMMAND:
            yy_status, yy_cd = parse_yuanying_status_reply(raw_text)
            update_session(
                db,
                event.chat_id,
                profile_id=session.get("profile_id"),
                last_bot_text=raw_text[:1000],
                last_bot_msg_id=event.id,
            )
            if yy_status in ("ready", "settled"):
                # 元婴在家(窍中温养)或已归来结算 → 立即出窍
                label = "可出窍" if yy_status == "ready" else "已归来结算"
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    yuanying_state="结算完成",
                    yuanying_next_check_time=0,
                    last_summary=f"元婴{label}，即将发送元婴出窍",
                    last_event="yuanying_settled",
                )
                logger.info(
                    "Yuanying %s in chat %s, will send outing",
                    yy_status,
                    event.chat_id,
                )
                return FanrenParseResult("yuanying_settled", f"元婴{label}，即将出窍")
            elif yy_status == "out":
                # 元婴仍在外，沿用已有倒计时
                if yy_cd:
                    update_session(
                        db,
                        event.chat_id,
                        profile_id=session.get("profile_id"),
                        yuanying_next_check_time=now_yy + yy_cd,
                    )
                cd_remain = max(
                    (session.get("yuanying_next_check_time") or 0) - now_yy, 0
                )
                cd_text = format_duration(cd_remain)
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    yuanying_state=f"等待归来: {cd_text}",
                    last_summary=f"元婴仍在云游中，预计 {cd_text} 后归来",
                    last_event="yuanying_waiting",
                )
                return FanrenParseResult(
                    "yuanying_waiting", f"元婴云游中，{cd_text}后归来"
                )
            else:
                # 未知状态，设置兜底倒计时
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    yuanying_next_check_time=now_yy + YUANYING_OUTING_COOLDOWN_SECONDS,
                    yuanying_state=f"等待归来: {format_duration(YUANYING_OUTING_COOLDOWN_SECONDS)}",
                    last_summary=f"元婴状态未知，默认 {format_duration(YUANYING_OUTING_COOLDOWN_SECONDS)} 后重试",
                    last_event="yuanying_waiting",
                )
                return FanrenParseResult(
                    "yuanying_waiting", "元婴状态未知，使用默认倒计时"
                )

        # ---- .元婴出窍 回包处理 ----
        if last_action == YUANYING_OUTING_COMMAND:
            yy_success, yy_cd = parse_yuanying_reply(raw_text)
            update_session(
                db,
                event.chat_id,
                profile_id=session.get("profile_id"),
                last_bot_text=raw_text[:1000],
                last_bot_msg_id=event.id,
            )
            if yy_success:
                cd = yy_cd or YUANYING_OUTING_COOLDOWN_SECONDS
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    yuanying_next_check_time=now_yy + cd,
                    yuanying_state=f"等待归来: {format_duration(cd)}",
                    last_summary=f"元婴已出窍，{format_duration(cd)}后发送元婴状态结算",
                    last_event="yuanying_outing_success",
                )
                logger.info(
                    "Yuanying outing succeeded in chat %s, CD=%s", event.chat_id, cd
                )
                return FanrenParseResult(
                    "yuanying_outing_success",
                    f"元婴出窍成功，{format_duration(cd)}后结算",
                    cd,
                )
            else:
                # 出窍失败，等待后重试
                update_session(
                    db,
                    event.chat_id,
                    profile_id=session.get("profile_id"),
                    yuanying_next_check_time=now_yy + YUANYING_OUTING_COOLDOWN_SECONDS,
                    yuanying_state=f"等待归来: {format_duration(YUANYING_OUTING_COOLDOWN_SECONDS)}",
                    last_summary=f"元婴出窍回包异常，将在 {format_duration(YUANYING_OUTING_COOLDOWN_SECONDS)} 后重试",
                    last_event="yuanying_outing_retry",
                )
                return FanrenParseResult(
                    "yuanying_outing_retry", "元婴出窍回包异常，将重试"
                )

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
        profile_id=session.get("profile_id"),
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
    current_session = get_session(
        db, event.chat_id, profile_id=session.get("profile_id")
    )
    await maybe_handle_special_auto_event(
        event,
        db,
        current_session or session,
        client,
        storage=getattr(client, "_tg_game_storage", None)
        if client is not None
        else None,
        profile_id=profile_id,
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
            profile_id=session.get("profile_id"),
        )
    if retreat_mode == "normal":
        if parsed.event == "retreat_complete" and parsed.cooldown_seconds:
            wait_seconds = normal_retry_seconds(
                parsed.cooldown_seconds, session["interval_seconds"]
            )
            update_session(
                db,
                event.chat_id,
                profile_id=session.get("profile_id"),
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
                profile_id=session.get("profile_id"),
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
                profile_id=session.get("profile_id"),
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
                profile_id=session.get("profile_id"),
                next_check_time=now + wait_seconds,
                next_check_source=f"深度闭关占用中，等待 {format_duration(wait_seconds)}",
                last_summary=f"当前处于深度闭关中，普通闭关将在 {format_duration(wait_seconds)} 后重试",
            )
    if failure_count >= FANREN_MAX_FAILURES:
        trip_circuit_breaker(
            db,
            event.chat_id,
            f"收到机器人失败事件 {parsed.event}，连续 {failure_count} 次",
            profile_id=session.get("profile_id"),
        )
    logger.info(
        "Fanren event in chat %s: %s (%s)", event.chat_id, parsed.event, parsed.summary
    )
    return parsed


async def runner(client, storage, profile_id=None):
    while True:
        try:
            db = RuntimeDb(storage)
            now = time.time()
            for session in list_sessions(db, profile_id=profile_id):
                if not session["enabled"]:
                    # 即使闭关主任务未启用，也检查独立子任务
                    pass
                if session.get("stopped_reason"):
                    continue

                # 主线：闭关调度
                cultivation_due = session["enabled"] and (
                    not session["next_check_time"] or now >= session["next_check_time"]
                )
                if cultivation_due:
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
                            profile_id=session.get("profile_id"),
                        )
                    except Exception as exc:
                        record_failure(
                            db,
                            session["chat_id"],
                            f"check failed: {exc}",
                            profile_id=session.get("profile_id"),
                        )
                        update_session(
                            db,
                            session["chat_id"],
                            profile_id=session.get("profile_id"),
                            next_check_time=now + max(session["interval_seconds"], 60),
                            next_check_source="runner 异常后退避等待",
                        )
                        logger.warning(
                            "Fanren runner failed in chat %s: %s",
                            session["chat_id"],
                            exc,
                        )

                # 自动探寻裂缝
                if session.get("auto_rift_enabled"):
                    rift_next = session.get("rift_next_check_time") or 0
                    if not rift_next or now >= rift_next:
                        try:
                            await _maybe_send_rift_explore(
                                client,
                                db,
                                session["chat_id"],
                                storage=storage,
                                profile_id=session.get("profile_id"),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Rift explore runner failed in chat %s: %s",
                                session["chat_id"],
                                exc,
                            )

                # 自动元婴出窍
                if session.get("auto_yuanying_enabled"):
                    yy_next = session.get("yuanying_next_check_time") or 0
                    if not yy_next or now >= yy_next:
                        try:
                            await _maybe_send_yuanying_outing(
                                client,
                                db,
                                session["chat_id"],
                                storage=storage,
                                profile_id=session.get("profile_id"),
                            )
                        except Exception as exc:
                            logger.warning(
                                "Yuanying outing runner failed in chat %s: %s",
                                session["chat_id"],
                                exc,
                            )
            db.close()
            await asyncio.sleep(FANREN_RUNNER_POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Fanren runner error: %s", exc)
            await asyncio.sleep(10)
