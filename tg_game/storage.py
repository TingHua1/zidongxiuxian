import hashlib
import json
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Iterable, Optional

from tg_game.config import ALLOWED_GAME_BOT_IDS
from tg_game.models import ChatBinding, FeatureModule, ModuleSetting, PlayerProfile


BOUND_MESSAGE_RETENTION_SECONDS = 48 * 3600
BOUND_MESSAGE_CLEANUP_INTERVAL_SECONDS = 3600


def _bool_from_row(value: object) -> bool:
    return bool(int(value or 0))


def _normalize_bot_username(value: object) -> str:
    return str(value or "").strip().lstrip("@")


def _normalize_optional_int(value: object) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _json_dumps_compact(value) -> str:
    return json.dumps(value or [], ensure_ascii=False, separators=(",", ":"))


DEFAULT_CHAT_BINDING_BOT_IDS = tuple(sorted(int(value) for value in ALLOWED_GAME_BOT_IDS))
CHAT_BINDING_BOT_IDS_DEFAULT_MIGRATION_KEY = "chat_binding_bot_ids_default_migration_v2"


def _normalize_bot_id_list(value: object) -> list[int]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = [part.strip() for part in text.split(",")]
        else:
            if isinstance(parsed, list):
                return [int(x) for x in parsed if _normalize_optional_int(x) is not None]
            parsed = [parsed]
        result = []
        for item in parsed:
            normalized = _normalize_optional_int(item)
            if normalized is not None:
                result.append(normalized)
        return sorted(list(dict.fromkeys(result)))
    if isinstance(value, (list, tuple, set)):
        result = []
        for item in value:
            normalized = _normalize_optional_int(item)
            if normalized is not None:
                result.append(normalized)
        return sorted(list(dict.fromkeys(result)))
    normalized = _normalize_optional_int(value)
    return [normalized] if normalized is not None else []


def _merge_bot_id_lists(*values: object) -> list[int]:
    merged: list[int] = []
    for value in values:
        for bot_id in _normalize_bot_id_list(value):
            if bot_id not in merged:
                merged.append(bot_id)
    return merged


def _normalize_bot_username_map(value: object) -> dict[int, str]:
    if value is None:
        return {}
    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
    if not isinstance(parsed, dict):
        return {}
    result: dict[int, str] = {}
    for raw_bot_id, raw_username in parsed.items():
        bot_id = _normalize_optional_int(raw_bot_id)
        username = _normalize_bot_username(raw_username)
        if bot_id is not None and username:
            result[int(bot_id)] = username
    return result


def _json_dumps_bot_username_map(value: dict[int, str]) -> str:
    normalized = {}
    for bot_id, username in sorted((value or {}).items()):
        normalized_bot_id = _normalize_optional_int(bot_id)
        normalized_username = _normalize_bot_username(username)
        if normalized_bot_id is not None and normalized_username:
            normalized[str(int(normalized_bot_id))] = normalized_username
    return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))


def _merge_bot_username_maps(*values: object) -> dict[int, str]:
    merged: dict[int, str] = {}
    for value in values:
        for bot_id, username in _normalize_bot_username_map(value).items():
            if username:
                merged[int(bot_id)] = username
    return merged


def _guess_bot_ids_from_username(bot_username: str) -> list[int]:
    normalized = str(bot_username or "").strip().lower().lstrip("@")
    mapping = {
        "fanrenxiuxian_bot": list(DEFAULT_CHAT_BINDING_BOT_IDS),
        "fanren_xiuxian_bot": list(DEFAULT_CHAT_BINDING_BOT_IDS),
        "luoxueyao_bot": list(DEFAULT_CHAT_BINDING_BOT_IDS),
    }
    return list(mapping.get(normalized, []))


class CompatDb:
    def __init__(self, storage: "Storage"):
        self.conn = storage.connect()
        self.cur = self.conn.cursor()

    def close(self) -> None:
        self.conn.close()


class Storage:
    def __init__(self, path: Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _ensure_columns(
        self, conn: sqlite3.Connection, table: str, columns: dict
    ) -> None:
        existing = {
            row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for name, column_type in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}")

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    account_name TEXT NOT NULL DEFAULT '',
                    game_name TEXT NOT NULL DEFAULT '',
                    telegram_user_id TEXT NOT NULL DEFAULT '',
                    telegram_phone TEXT NOT NULL DEFAULT '',
                    telegram_username TEXT NOT NULL DEFAULT '',
                    telegram_verified_at REAL NOT NULL DEFAULT 0,
                    telegram_session_name TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    display_name TEXT NOT NULL DEFAULT '',
                    artifact_text TEXT NOT NULL DEFAULT '',
                    sect_name TEXT NOT NULL DEFAULT '',
                    sect_leader TEXT NOT NULL DEFAULT '',
                    sect_position TEXT NOT NULL DEFAULT '',
                    sect_description TEXT NOT NULL DEFAULT '',
                    sect_bonus_text TEXT NOT NULL DEFAULT '',
                    sect_contribution_text TEXT NOT NULL DEFAULT '',
                    spirit_root TEXT NOT NULL DEFAULT '',
                    stage_name TEXT NOT NULL DEFAULT '',
                    cultivation_text TEXT NOT NULL DEFAULT '',
                    poison_text TEXT NOT NULL DEFAULT '',
                    kill_count_text TEXT NOT NULL DEFAULT '',
                    info_updated_at REAL NOT NULL DEFAULT 0,
                    sect_info_updated_at REAL NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS chat_bindings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    bot_username TEXT NOT NULL DEFAULT '',
                    bot_id INTEGER,
                    bot_ids TEXT NOT NULL DEFAULT '[]',
                    bot_usernames TEXT NOT NULL DEFAULT '{}',
                    telegram_user_id TEXT NOT NULL DEFAULT '',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS module_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    module_key TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    cooldown_seconds INTEGER NOT NULL DEFAULT 30,
                    check_interval_seconds INTEGER NOT NULL DEFAULT 300,
                    command_template TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, module_key),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS cultivation_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER,
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    mode TEXT NOT NULL DEFAULT 'normal',
                    event TEXT NOT NULL DEFAULT '',
                    gain_value INTEGER,
                    stage_name TEXT NOT NULL DEFAULT '',
                    progress_text TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    raw_text TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bound_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    message_id INTEGER NOT NULL,
                    reply_to_msg_id INTEGER,
                    sender_id INTEGER,
                    sender_username TEXT,
                    direction TEXT NOT NULL DEFAULT '',
                    is_bot INTEGER NOT NULL DEFAULT 0,
                    text TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, chat_id, message_id)
                );

                CREATE TABLE IF NOT EXISTS external_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    provider TEXT NOT NULL,
                    telegram_user_id TEXT NOT NULL DEFAULT '',
                    telegram_username TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'connected',
                    cookie_text TEXT NOT NULL DEFAULT '',
                    api_token TEXT NOT NULL DEFAULT '',
                    me_json TEXT NOT NULL DEFAULT '{}',
                    last_verified_at REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, provider),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS app_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    session_token_hash TEXT NOT NULL UNIQUE,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    revoked_at REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS browser_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_token_hash TEXT NOT NULL UNIQUE,
                    current_profile_id INTEGER,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    revoked_at REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY (current_profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS browser_session_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    browser_session_id INTEGER NOT NULL,
                    profile_id INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(browser_session_id, profile_id),
                    FOREIGN KEY (browser_session_id) REFERENCES browser_sessions(id),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS app_runtime_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS game_items (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    type TEXT NOT NULL DEFAULT '',
                    rarity INTEGER NOT NULL DEFAULT 0,
                    value INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shop_items (
                    item_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    type TEXT NOT NULL DEFAULT '',
                    shop_price INTEGER NOT NULL DEFAULT 0,
                    sect_exclusive TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS level_thresholds (
                    stage_name TEXT PRIMARY KEY,
                    threshold INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS marketplace_listings (
                    id INTEGER PRIMARY KEY,
                    item_id TEXT NOT NULL DEFAULT '',
                    item_type TEXT NOT NULL DEFAULT '',
                    item_name TEXT NOT NULL DEFAULT '',
                    listing_time TEXT NOT NULL DEFAULT '',
                    quantity INTEGER NOT NULL DEFAULT 0,
                    price_json TEXT NOT NULL DEFAULT '{}',
                    seller_username TEXT NOT NULL DEFAULT '',
                    is_bundle INTEGER NOT NULL DEFAULT 0,
                    is_material INTEGER NOT NULL DEFAULT 0,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS telegram_login_challenges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL,
                    phone_code_hash TEXT NOT NULL DEFAULT '',
                    session_name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'code_sent',
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS outgoing_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    reply_to_msg_id INTEGER,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    bot_username TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    error_text TEXT NOT NULL DEFAULT '',
                    scheduled_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS divination_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    bot_username TEXT NOT NULL DEFAULT '',
                    initial_count INTEGER NOT NULL DEFAULT 0,
                    target_count INTEGER NOT NULL DEFAULT 0,
                    sent_count INTEGER NOT NULL DEFAULT 0,
                    completed_count INTEGER NOT NULL DEFAULT 0,
                    pending_command_msg_id INTEGER NOT NULL DEFAULT 0,
                    last_dispatch_at REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS companion_auto_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    bot_username TEXT NOT NULL DEFAULT '',
                    feature_key TEXT NOT NULL DEFAULT '',
                    strategy TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    workflow_state TEXT NOT NULL DEFAULT '',
                    next_run_at REAL NOT NULL DEFAULT 0,
                    last_run_at REAL NOT NULL DEFAULT 0,
                    anchor_command_msg_id INTEGER NOT NULL DEFAULT 0,
                    anchor_bot_msg_id INTEGER NOT NULL DEFAULT 0,
                    tribulation_msg_id INTEGER NOT NULL DEFAULT 0,
                    last_progress_fingerprint TEXT NOT NULL DEFAULT '',
                    last_stable_sent_at REAL NOT NULL DEFAULT 0,
                    last_settlement_text TEXT NOT NULL DEFAULT '',
                    last_settlement_at REAL NOT NULL DEFAULT 0,
                    previous_settlement_text TEXT NOT NULL DEFAULT '',
                    previous_settlement_at REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, chat_id, bot_username, feature_key),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS companion_heart_tribulation_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    chat_type TEXT NOT NULL DEFAULT 'group',
                    bot_username TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    run_id TEXT NOT NULL DEFAULT '',
                    workflow_state TEXT NOT NULL DEFAULT '',
                    next_run_at REAL NOT NULL DEFAULT 0,
                    step_deadline_at REAL NOT NULL DEFAULT 0,
                    last_run_at REAL NOT NULL DEFAULT 0,
                    matched_bot_id INTEGER NOT NULL DEFAULT 0,
                    anchor_command_msg_id INTEGER NOT NULL DEFAULT 0,
                    anchor_bot_msg_id INTEGER NOT NULL DEFAULT 0,
                    tribulation_command_msg_id INTEGER NOT NULL DEFAULT 0,
                    tribulation_msg_id INTEGER NOT NULL DEFAULT 0,
                    panel_reply_msg_id INTEGER NOT NULL DEFAULT 0,
                    round1_reply TEXT NOT NULL DEFAULT '稳',
                    round2_reply TEXT NOT NULL DEFAULT '稳',
                    round3_reply TEXT NOT NULL DEFAULT '稳',
                    last_action_round_sent INTEGER NOT NULL DEFAULT 0,
                    last_tribulation_command_at REAL NOT NULL DEFAULT 0,
                    last_progress_at REAL NOT NULL DEFAULT 0,
                    last_progress_fingerprint TEXT NOT NULL DEFAULT '',
                    last_stable_sent_at REAL NOT NULL DEFAULT 0,
                    last_settlement_text TEXT NOT NULL DEFAULT '',
                    last_settlement_at REAL NOT NULL DEFAULT 0,
                    previous_settlement_text TEXT NOT NULL DEFAULT '',
                    previous_settlement_at REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, chat_id, bot_username),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS companion_heart_tribulation_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    task_id INTEGER NOT NULL DEFAULT 0,
                    run_id TEXT NOT NULL DEFAULT '',
                    step TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT '',
                    message_id INTEGER NOT NULL DEFAULT 0,
                    reply_to_msg_id INTEGER NOT NULL DEFAULT 0,
                    sender_id INTEGER NOT NULL DEFAULT 0,
                    sender_username TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL DEFAULT '',
                    detail_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rift_execution_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER,
                    step TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL DEFAULT '',
                    rift_state TEXT NOT NULL DEFAULT '',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    message_id INTEGER NOT NULL DEFAULT 0,
                    reply_to_msg_id INTEGER NOT NULL DEFAULT 0,
                    sender_id INTEGER NOT NULL DEFAULT 0,
                    sender_username TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL DEFAULT '',
                    detail_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS stock_market_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL DEFAULT '',
                    current_price REAL NOT NULL DEFAULT 0,
                    change_amount REAL NOT NULL DEFAULT 0,
                    change_percent REAL NOT NULL DEFAULT 0,
                    sector TEXT NOT NULL DEFAULT '',
                    trend TEXT NOT NULL DEFAULT '',
                    heat TEXT NOT NULL DEFAULT '',
                    crowding TEXT NOT NULL DEFAULT '',
                    volatility TEXT NOT NULL DEFAULT '',
                    liquidity TEXT NOT NULL DEFAULT '',
                    open_price REAL NOT NULL DEFAULT 0,
                    prev_close REAL NOT NULL DEFAULT 0,
                    high_price REAL NOT NULL DEFAULT 0,
                    low_price REAL NOT NULL DEFAULT 0,
                    volume REAL NOT NULL DEFAULT 0,
                    turnover REAL NOT NULL DEFAULT 0,
                    pattern TEXT NOT NULL DEFAULT '',
                    volume_trend TEXT NOT NULL DEFAULT '',
                    position_text TEXT NOT NULL DEFAULT '',
                    score INTEGER NOT NULL DEFAULT 0,
                    strategy TEXT NOT NULL DEFAULT '',
                    direction_emoji TEXT NOT NULL DEFAULT '',
                    source_message_id INTEGER NOT NULL DEFAULT 0,
                    raw_text TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, stock_code),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE TABLE IF NOT EXISTS stock_market_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT NOT NULL DEFAULT '',
                    current_price REAL NOT NULL DEFAULT 0,
                    change_amount REAL NOT NULL DEFAULT 0,
                    change_percent REAL NOT NULL DEFAULT 0,
                    sector TEXT NOT NULL DEFAULT '',
                    trend TEXT NOT NULL DEFAULT '',
                    heat TEXT NOT NULL DEFAULT '',
                    crowding TEXT NOT NULL DEFAULT '',
                    volatility TEXT NOT NULL DEFAULT '',
                    liquidity TEXT NOT NULL DEFAULT '',
                    open_price REAL NOT NULL DEFAULT 0,
                    prev_close REAL NOT NULL DEFAULT 0,
                    high_price REAL NOT NULL DEFAULT 0,
                    low_price REAL NOT NULL DEFAULT 0,
                    volume REAL NOT NULL DEFAULT 0,
                    turnover REAL NOT NULL DEFAULT 0,
                    pattern TEXT NOT NULL DEFAULT '',
                    volume_trend TEXT NOT NULL DEFAULT '',
                    position_text TEXT NOT NULL DEFAULT '',
                    score INTEGER NOT NULL DEFAULT 0,
                    strategy TEXT NOT NULL DEFAULT '',
                    direction_emoji TEXT NOT NULL DEFAULT '',
                    raw_text TEXT NOT NULL DEFAULT '',
                    observed_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(chat_id, message_id, stock_code)
                );

                CREATE TABLE IF NOT EXISTS stock_player_replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL DEFAULT 0,
                    thread_id INTEGER,
                    command_text TEXT NOT NULL DEFAULT '',
                    reply_text TEXT NOT NULL DEFAULT '',
                    source_message_id INTEGER NOT NULL DEFAULT 0,
                    reply_to_msg_id INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(profile_id, command_text),
                    FOREIGN KEY (profile_id) REFERENCES profiles(id)
                );

                CREATE INDEX IF NOT EXISTS idx_profiles_active ON profiles(is_active);
                CREATE INDEX IF NOT EXISTS idx_chat_bindings_profile ON chat_bindings(profile_id, is_active);
                CREATE INDEX IF NOT EXISTS idx_cultivation_results_profile_created ON cultivation_results(profile_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_bound_messages_profile_created ON bound_messages(profile_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_app_sessions_hash ON app_sessions(session_token_hash);
                CREATE INDEX IF NOT EXISTS idx_browser_sessions_hash ON browser_sessions(session_token_hash);
                CREATE INDEX IF NOT EXISTS idx_browser_session_profiles_session ON browser_session_profiles(browser_session_id, profile_id);
                CREATE INDEX IF NOT EXISTS idx_outgoing_commands_status_created ON outgoing_commands(status, created_at ASC);
                CREATE INDEX IF NOT EXISTS idx_divination_batches_profile_status ON divination_batches(profile_id, status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_divination_batches_chat_status ON divination_batches(chat_id, status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_companion_auto_tasks_profile_enabled ON companion_auto_tasks(profile_id, enabled, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_companion_auto_tasks_chat_feature ON companion_auto_tasks(chat_id, feature_key, enabled);
                CREATE INDEX IF NOT EXISTS idx_companion_heart_trib_tasks_profile_enabled ON companion_heart_tribulation_tasks(profile_id, enabled, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_companion_heart_trib_tasks_chat ON companion_heart_tribulation_tasks(chat_id, enabled, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_companion_heart_trib_logs_task_created ON companion_heart_tribulation_logs(task_id, created_at ASC, id ASC);
                CREATE INDEX IF NOT EXISTS idx_companion_heart_trib_logs_run_created ON companion_heart_tribulation_logs(profile_id, run_id, created_at ASC, id ASC);
                CREATE INDEX IF NOT EXISTS idx_stock_market_info_profile_updated ON stock_market_info(profile_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_stock_market_history_code_observed ON stock_market_history(stock_code, observed_at DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_stock_player_replies_profile_updated ON stock_player_replies(profile_id, updated_at DESC);
                """
            )

            self._ensure_columns(
                conn,
                "external_accounts",
                {"api_token": "TEXT NOT NULL DEFAULT ''"},
            )
            self._ensure_columns(
                conn,
                "profiles",
                {
                    "account_name": "TEXT NOT NULL DEFAULT ''",
                    "game_name": "TEXT NOT NULL DEFAULT ''",
                    "telegram_user_id": "TEXT NOT NULL DEFAULT ''",
                    "telegram_phone": "TEXT NOT NULL DEFAULT ''",
                    "telegram_username": "TEXT NOT NULL DEFAULT ''",
                    "telegram_verified_at": "REAL NOT NULL DEFAULT 0",
                    "telegram_session_name": "TEXT NOT NULL DEFAULT ''",
                    "notes": "TEXT NOT NULL DEFAULT ''",
                    "display_name": "TEXT NOT NULL DEFAULT ''",
                    "artifact_text": "TEXT NOT NULL DEFAULT ''",
                    "sect_name": "TEXT NOT NULL DEFAULT ''",
                    "sect_leader": "TEXT NOT NULL DEFAULT ''",
                    "sect_position": "TEXT NOT NULL DEFAULT ''",
                    "sect_description": "TEXT NOT NULL DEFAULT ''",
                    "sect_bonus_text": "TEXT NOT NULL DEFAULT ''",
                    "sect_contribution_text": "TEXT NOT NULL DEFAULT ''",
                    "spirit_root": "TEXT NOT NULL DEFAULT ''",
                    "stage_name": "TEXT NOT NULL DEFAULT ''",
                    "cultivation_text": "TEXT NOT NULL DEFAULT ''",
                    "poison_text": "TEXT NOT NULL DEFAULT ''",
                    "kill_count_text": "TEXT NOT NULL DEFAULT ''",
                    "info_updated_at": "REAL NOT NULL DEFAULT 0",
                    "sect_info_updated_at": "REAL NOT NULL DEFAULT 0",
                    "is_active": "INTEGER NOT NULL DEFAULT 0",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "chat_bindings",
                {
                    "thread_id": "INTEGER",
                    "chat_type": "TEXT NOT NULL DEFAULT 'group'",
                    "bot_username": "TEXT NOT NULL DEFAULT ''",
                    "bot_id": "INTEGER",
                    "bot_ids": "TEXT NOT NULL DEFAULT '[]'",
                    "bot_usernames": "TEXT NOT NULL DEFAULT '{}'",
                    "telegram_user_id": "TEXT NOT NULL DEFAULT ''",
                    "is_active": "INTEGER NOT NULL DEFAULT 1",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            existing_chat_rows = conn.execute(
                "SELECT id, bot_id, bot_ids FROM chat_bindings"
            ).fetchall()
            for row in existing_chat_rows:
                existing_ids = _normalize_bot_id_list(row["bot_ids"])
                normalized_ids = existing_ids or _merge_bot_id_lists(row["bot_id"])
                if normalized_ids:
                    conn.execute(
                        "UPDATE chat_bindings SET bot_ids=? WHERE id=?",
                        (_json_dumps_compact(normalized_ids), row["id"]),
                    )
            migration_done = conn.execute(
                "SELECT value FROM app_runtime_state WHERE key=?",
                (CHAT_BINDING_BOT_IDS_DEFAULT_MIGRATION_KEY,),
            ).fetchone()
            if migration_done is None:
                chat_groups = conn.execute(
                    """
                    SELECT chat_id, thread_id
                    FROM chat_bindings
                    GROUP BY chat_id, COALESCE(thread_id, 0)
                    """
                ).fetchall()
                now = time.time()
                for group in chat_groups:
                    group_rows = conn.execute(
                        """
                        SELECT id, bot_id, bot_ids
                        FROM chat_bindings
                        WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                        """,
                        (group["chat_id"], group["thread_id"]),
                    ).fetchall()
                    migrated_ids = _merge_bot_id_lists(
                        DEFAULT_CHAT_BINDING_BOT_IDS,
                        *[row["bot_ids"] for row in group_rows],
                        *[row["bot_id"] for row in group_rows],
                    )
                    if not migrated_ids:
                        continue
                    for row in group_rows:
                        conn.execute(
                            "UPDATE chat_bindings SET bot_ids=?, updated_at=? WHERE id=?",
                            (_json_dumps_compact(migrated_ids), now, row["id"]),
                        )
                conn.execute(
                    """
                    INSERT INTO app_runtime_state(key, value, updated_at)
                    VALUES (?, '1', ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                    """,
                    (CHAT_BINDING_BOT_IDS_DEFAULT_MIGRATION_KEY, now),
                )
            self._ensure_columns(
                conn,
                "outgoing_commands",
                {
                    "profile_id": "INTEGER",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "thread_id": "INTEGER",
                    "reply_to_msg_id": "INTEGER",
                    "chat_type": "TEXT NOT NULL DEFAULT 'group'",
                    "bot_username": "TEXT NOT NULL DEFAULT ''",
                    "text": "TEXT NOT NULL DEFAULT ''",
                    "status": "TEXT NOT NULL DEFAULT 'pending'",
                    "error_text": "TEXT NOT NULL DEFAULT ''",
                    "scheduled_at": "REAL NOT NULL DEFAULT 0",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "divination_batches",
                {
                    "profile_id": "INTEGER NOT NULL DEFAULT 0",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "thread_id": "INTEGER",
                    "chat_type": "TEXT NOT NULL DEFAULT 'group'",
                    "bot_username": "TEXT NOT NULL DEFAULT ''",
                    "initial_count": "INTEGER NOT NULL DEFAULT 0",
                    "target_count": "INTEGER NOT NULL DEFAULT 0",
                    "sent_count": "INTEGER NOT NULL DEFAULT 0",
                    "completed_count": "INTEGER NOT NULL DEFAULT 0",
                    "pending_command_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "last_dispatch_at": "REAL NOT NULL DEFAULT 0",
                    "status": "TEXT NOT NULL DEFAULT 'active'",
                    "last_error": "TEXT NOT NULL DEFAULT ''",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "companion_auto_tasks",
                {
                    "profile_id": "INTEGER NOT NULL DEFAULT 0",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "thread_id": "INTEGER",
                    "chat_type": "TEXT NOT NULL DEFAULT 'group'",
                    "bot_username": "TEXT NOT NULL DEFAULT ''",
                    "feature_key": "TEXT NOT NULL DEFAULT ''",
                    "strategy": "TEXT NOT NULL DEFAULT ''",
                    "enabled": "INTEGER NOT NULL DEFAULT 1",
                    "workflow_state": "TEXT NOT NULL DEFAULT ''",
                    "next_run_at": "REAL NOT NULL DEFAULT 0",
                    "last_run_at": "REAL NOT NULL DEFAULT 0",
                    "anchor_command_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "anchor_bot_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "tribulation_command_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "tribulation_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "panel_reply_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "round1_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "round2_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "round3_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "last_action_round_sent": "INTEGER NOT NULL DEFAULT 0",
                    "last_tribulation_command_at": "REAL NOT NULL DEFAULT 0",
                    "last_progress_at": "REAL NOT NULL DEFAULT 0",
                    "last_progress_fingerprint": "TEXT NOT NULL DEFAULT ''",
                    "last_stable_sent_at": "REAL NOT NULL DEFAULT 0",
                    "last_settlement_text": "TEXT NOT NULL DEFAULT ''",
                    "last_settlement_at": "REAL NOT NULL DEFAULT 0",
                    "previous_settlement_text": "TEXT NOT NULL DEFAULT ''",
                    "previous_settlement_at": "REAL NOT NULL DEFAULT 0",
                    "last_error": "TEXT NOT NULL DEFAULT ''",
                    "retry_count": "INTEGER NOT NULL DEFAULT 0",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "companion_heart_tribulation_tasks",
                {
                    "run_id": "TEXT NOT NULL DEFAULT ''",
                    "step_deadline_at": "REAL NOT NULL DEFAULT 0",
                    "matched_bot_id": "INTEGER NOT NULL DEFAULT 0",
                    "profile_id": "INTEGER NOT NULL DEFAULT 0",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "thread_id": "INTEGER",
                    "chat_type": "TEXT NOT NULL DEFAULT 'group'",
                    "bot_username": "TEXT NOT NULL DEFAULT ''",
                    "enabled": "INTEGER NOT NULL DEFAULT 1",
                    "workflow_state": "TEXT NOT NULL DEFAULT ''",
                    "next_run_at": "REAL NOT NULL DEFAULT 0",
                    "last_run_at": "REAL NOT NULL DEFAULT 0",
                    "anchor_command_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "anchor_bot_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "tribulation_command_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "tribulation_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "panel_reply_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "round1_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "round2_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "round3_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "last_action_round_sent": "INTEGER NOT NULL DEFAULT 0",
                    "last_tribulation_command_at": "REAL NOT NULL DEFAULT 0",
                    "last_progress_at": "REAL NOT NULL DEFAULT 0",
                    "last_progress_fingerprint": "TEXT NOT NULL DEFAULT ''",
                    "last_stable_sent_at": "REAL NOT NULL DEFAULT 0",
                    "last_settlement_text": "TEXT NOT NULL DEFAULT ''",
                    "last_settlement_at": "REAL NOT NULL DEFAULT 0",
                    "previous_settlement_text": "TEXT NOT NULL DEFAULT ''",
                    "previous_settlement_at": "REAL NOT NULL DEFAULT 0",
                    "last_error": "TEXT NOT NULL DEFAULT ''",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                    "round_retry_count": "INTEGER NOT NULL DEFAULT 0",
                    "round_retry_deadline_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "companion_heart_tribulation_logs",
                {
                    "profile_id": "INTEGER NOT NULL DEFAULT 0",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "thread_id": "INTEGER",
                    "task_id": "INTEGER NOT NULL DEFAULT 0",
                    "run_id": "TEXT NOT NULL DEFAULT ''",
                    "step": "TEXT NOT NULL DEFAULT ''",
                    "event_type": "TEXT NOT NULL DEFAULT ''",
                    "message_id": "INTEGER NOT NULL DEFAULT 0",
                    "reply_to_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "sender_id": "INTEGER NOT NULL DEFAULT 0",
                    "sender_username": "TEXT NOT NULL DEFAULT ''",
                    "text": "TEXT NOT NULL DEFAULT ''",
                    "detail_json": "TEXT NOT NULL DEFAULT '{}'",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "stock_market_info",
                {
                    "profile_id": "INTEGER NOT NULL DEFAULT 0",
                    "stock_code": "TEXT NOT NULL DEFAULT ''",
                    "stock_name": "TEXT NOT NULL DEFAULT ''",
                    "current_price": "REAL NOT NULL DEFAULT 0",
                    "change_amount": "REAL NOT NULL DEFAULT 0",
                    "change_percent": "REAL NOT NULL DEFAULT 0",
                    "sector": "TEXT NOT NULL DEFAULT ''",
                    "trend": "TEXT NOT NULL DEFAULT ''",
                    "heat": "TEXT NOT NULL DEFAULT ''",
                    "crowding": "TEXT NOT NULL DEFAULT ''",
                    "volatility": "TEXT NOT NULL DEFAULT ''",
                    "liquidity": "TEXT NOT NULL DEFAULT ''",
                    "open_price": "REAL NOT NULL DEFAULT 0",
                    "prev_close": "REAL NOT NULL DEFAULT 0",
                    "high_price": "REAL NOT NULL DEFAULT 0",
                    "low_price": "REAL NOT NULL DEFAULT 0",
                    "volume": "REAL NOT NULL DEFAULT 0",
                    "turnover": "REAL NOT NULL DEFAULT 0",
                    "pattern": "TEXT NOT NULL DEFAULT ''",
                    "volume_trend": "TEXT NOT NULL DEFAULT ''",
                    "position_text": "TEXT NOT NULL DEFAULT ''",
                    "score": "INTEGER NOT NULL DEFAULT 0",
                    "strategy": "TEXT NOT NULL DEFAULT ''",
                    "direction_emoji": "TEXT NOT NULL DEFAULT ''",
                    "source_message_id": "INTEGER NOT NULL DEFAULT 0",
                    "raw_text": "TEXT NOT NULL DEFAULT ''",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "stock_market_history",
                {
                    "profile_id": "INTEGER",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "message_id": "INTEGER NOT NULL DEFAULT 0",
                    "stock_code": "TEXT NOT NULL DEFAULT ''",
                    "stock_name": "TEXT NOT NULL DEFAULT ''",
                    "current_price": "REAL NOT NULL DEFAULT 0",
                    "change_amount": "REAL NOT NULL DEFAULT 0",
                    "change_percent": "REAL NOT NULL DEFAULT 0",
                    "sector": "TEXT NOT NULL DEFAULT ''",
                    "trend": "TEXT NOT NULL DEFAULT ''",
                    "heat": "TEXT NOT NULL DEFAULT ''",
                    "crowding": "TEXT NOT NULL DEFAULT ''",
                    "volatility": "TEXT NOT NULL DEFAULT ''",
                    "liquidity": "TEXT NOT NULL DEFAULT ''",
                    "open_price": "REAL NOT NULL DEFAULT 0",
                    "prev_close": "REAL NOT NULL DEFAULT 0",
                    "high_price": "REAL NOT NULL DEFAULT 0",
                    "low_price": "REAL NOT NULL DEFAULT 0",
                    "volume": "REAL NOT NULL DEFAULT 0",
                    "turnover": "REAL NOT NULL DEFAULT 0",
                    "pattern": "TEXT NOT NULL DEFAULT ''",
                    "volume_trend": "TEXT NOT NULL DEFAULT ''",
                    "position_text": "TEXT NOT NULL DEFAULT ''",
                    "score": "INTEGER NOT NULL DEFAULT 0",
                    "strategy": "TEXT NOT NULL DEFAULT ''",
                    "direction_emoji": "TEXT NOT NULL DEFAULT ''",
                    "raw_text": "TEXT NOT NULL DEFAULT ''",
                    "observed_at": "REAL NOT NULL DEFAULT 0",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "stock_player_replies",
                {
                    "profile_id": "INTEGER NOT NULL DEFAULT 0",
                    "chat_id": "INTEGER NOT NULL DEFAULT 0",
                    "thread_id": "INTEGER",
                    "command_text": "TEXT NOT NULL DEFAULT ''",
                    "reply_text": "TEXT NOT NULL DEFAULT ''",
                    "source_message_id": "INTEGER NOT NULL DEFAULT 0",
                    "reply_to_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "created_at": "REAL NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='fanren_sessions'"
            ).fetchone():
                self._ensure_columns(
                    conn,
                    "fanren_sessions",
                    {"profile_id": "INTEGER NOT NULL DEFAULT 0"},
                )
            if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sect_sessions'"
            ).fetchone():
                self._ensure_columns(
                    conn,
                    "sect_sessions",
                    {"profile_id": "INTEGER NOT NULL DEFAULT 0"},
                )
            self._ensure_columns(
                conn,
                "shop_items",
                {
                    "item_id": "TEXT PRIMARY KEY",
                    "name": "TEXT NOT NULL DEFAULT ''",
                    "description": "TEXT NOT NULL DEFAULT ''",
                    "type": "TEXT NOT NULL DEFAULT ''",
                    "shop_price": "INTEGER NOT NULL DEFAULT 0",
                    "sect_exclusive": "TEXT NOT NULL DEFAULT ''",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._ensure_columns(
                conn,
                "marketplace_listings",
                {
                    "id": "INTEGER PRIMARY KEY",
                    "item_id": "TEXT NOT NULL DEFAULT ''",
                    "item_type": "TEXT NOT NULL DEFAULT ''",
                    "item_name": "TEXT NOT NULL DEFAULT ''",
                    "listing_time": "TEXT NOT NULL DEFAULT ''",
                    "quantity": "INTEGER NOT NULL DEFAULT 0",
                    "price_json": "TEXT NOT NULL DEFAULT '{}'",
                    "seller_username": "TEXT NOT NULL DEFAULT ''",
                    "is_bundle": "INTEGER NOT NULL DEFAULT 0",
                    "is_material": "INTEGER NOT NULL DEFAULT 0",
                    "updated_at": "REAL NOT NULL DEFAULT 0",
                },
            )
            self._migrate_bound_messages_schema(conn)

    def _migrate_bound_messages_schema(self, conn: sqlite3.Connection) -> None:
        index_rows = conn.execute("PRAGMA index_list(bound_messages)").fetchall()
        unique_index_name = ""
        for row in index_rows:
            if int(row[2] or 0):
                unique_index_name = str(row[1] or "")
                break
        if not unique_index_name:
            return
        index_columns = [
            str(row[2] or "")
            for row in conn.execute(f"PRAGMA index_info({unique_index_name})").fetchall()
        ]
        if index_columns == ["profile_id", "chat_id", "message_id"]:
            return
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bound_messages_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                message_id INTEGER NOT NULL,
                reply_to_msg_id INTEGER,
                sender_id INTEGER,
                sender_username TEXT,
                direction TEXT NOT NULL DEFAULT '',
                is_bot INTEGER NOT NULL DEFAULT 0,
                text TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(profile_id, chat_id, message_id)
            );
            INSERT INTO bound_messages_v2 (
                id, profile_id, chat_id, thread_id, message_id, reply_to_msg_id,
                sender_id, sender_username, direction, is_bot, text, created_at, updated_at
            )
            SELECT id, profile_id, chat_id, thread_id, message_id, reply_to_msg_id,
                   sender_id, sender_username, direction, is_bot, text, created_at, updated_at
            FROM bound_messages;
            DROP TABLE bound_messages;
            ALTER TABLE bound_messages_v2 RENAME TO bound_messages;
            CREATE INDEX IF NOT EXISTS idx_bound_messages_profile_created ON bound_messages(profile_id, created_at DESC);
            """
        )

    def _row_to_profile(self, row: sqlite3.Row) -> PlayerProfile:
        return PlayerProfile(
            id=row["id"],
            name=row["name"],
            account_name=row["account_name"],
            game_name=row["game_name"],
            telegram_user_id=row["telegram_user_id"],
            telegram_phone=row["telegram_phone"],
            telegram_username=row["telegram_username"],
            telegram_verified_at=row["telegram_verified_at"],
            telegram_session_name=row["telegram_session_name"],
            notes=row["notes"],
            display_name=row["display_name"],
            artifact_text=row["artifact_text"],
            sect_name=row["sect_name"],
            sect_leader=row["sect_leader"],
            sect_position=row["sect_position"],
            sect_description=row["sect_description"],
            sect_bonus_text=row["sect_bonus_text"],
            sect_contribution_text=row["sect_contribution_text"],
            spirit_root=row["spirit_root"],
            stage_name=row["stage_name"],
            cultivation_text=row["cultivation_text"],
            poison_text=row["poison_text"],
            kill_count_text=row["kill_count_text"],
            info_updated_at=row["info_updated_at"],
            sect_info_updated_at=row["sect_info_updated_at"],
            is_active=_bool_from_row(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_chat(self, row: sqlite3.Row) -> ChatBinding:
        bot_id = _normalize_optional_int(row["bot_id"])
        bot_ids = _normalize_bot_id_list(
            row["bot_ids"] if "bot_ids" in row.keys() else []
        )
        if not bot_ids:
            bot_ids = _merge_bot_id_lists(bot_id)
        bot_usernames = _normalize_bot_username_map(
            row["bot_usernames"] if "bot_usernames" in row.keys() else {}
        )
        return ChatBinding(
            id=row["id"],
            profile_id=row["profile_id"],
            chat_id=row["chat_id"],
            thread_id=row["thread_id"],
            chat_type=row["chat_type"],
            bot_username=row["bot_username"],
            bot_id=bot_id,
            bot_ids=bot_ids,
            bot_usernames=bot_usernames,
            telegram_user_id=row["telegram_user_id"],
            is_active=_bool_from_row(row["is_active"]),
            created_at=row["created_at"],
        )

    def _row_to_setting(self, row: sqlite3.Row) -> ModuleSetting:
        return ModuleSetting(
            id=row["id"],
            profile_id=row["profile_id"],
            module_key=row["module_key"],
            enabled=_bool_from_row(row["enabled"]),
            cooldown_seconds=row["cooldown_seconds"],
            check_interval_seconds=row["check_interval_seconds"],
            command_template=row["command_template"],
            notes=row["notes"],
            updated_at=row["updated_at"],
        )

    def list_profiles(self) -> list[PlayerProfile]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM profiles ORDER BY is_active DESC, updated_at DESC, id DESC"
            ).fetchall()
        return [self._row_to_profile(row) for row in rows]

    def get_profile(self, profile_id: int) -> Optional[PlayerProfile]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM profiles WHERE id=?", (profile_id,)
            ).fetchone()
        return self._row_to_profile(row) if row else None

    def get_profile_by_telegram_user_id(
        self, telegram_user_id: str
    ) -> Optional[PlayerProfile]:
        telegram_user_id = str(telegram_user_id or "").strip()
        if not telegram_user_id:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM profiles WHERE telegram_user_id=? ORDER BY updated_at DESC LIMIT 1",
                (telegram_user_id,),
            ).fetchone()
        return self._row_to_profile(row) if row else None

    def get_active_profile(self) -> Optional[PlayerProfile]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM profiles WHERE is_active=1 ORDER BY updated_at DESC, id DESC LIMIT 1"
            ).fetchone()
        return self._row_to_profile(row) if row else None

    def create_profile(self, name: str, activate: bool = False) -> PlayerProfile:
        now = time.time()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO profiles (
                    name, account_name, game_name, telegram_user_id, telegram_phone,
                    telegram_username, telegram_verified_at, telegram_session_name,
                    notes, display_name, artifact_text, sect_name, sect_leader,
                    sect_position, sect_description, sect_bonus_text,
                    sect_contribution_text, spirit_root, stage_name,
                    cultivation_text, poison_text, kill_count_text,
                    info_updated_at, sect_info_updated_at,
                    is_active, created_at, updated_at
                ) VALUES (?, ?, ?, '', '', '', 0, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 0, 0, ?, ?, ?)
                """,
                (name, "", name, 1 if activate else 0, now, now),
            )
            profile_id = cursor.lastrowid
        if activate:
            self.activate_profile(profile_id)
        return self.get_profile(profile_id)

    def activate_profile(self, profile_id: int) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute("UPDATE profiles SET is_active=0, updated_at=?", (now,))
            conn.execute(
                "UPDATE profiles SET is_active=1, updated_at=? WHERE id=?",
                (now, profile_id),
            )

    def bind_profile_telegram_account(
        self,
        profile_id: int,
        telegram_user_id: str = "",
        telegram_username: str = "",
        telegram_phone: str = "",
        telegram_session_name: str = "",
    ) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE profiles
                SET telegram_user_id=?, telegram_username=?, telegram_phone=?,
                    telegram_session_name=?, telegram_verified_at=?, updated_at=?
                WHERE id=?
                """,
                (
                    str(telegram_user_id or "").strip(),
                    str(telegram_username or "").strip(),
                    str(telegram_phone or "").strip(),
                    str(telegram_session_name or "").strip(),
                    now if telegram_user_id or telegram_session_name else 0,
                    now,
                    profile_id,
                ),
            )

    def clear_profile_telegram_account(self, profile_id: int) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE profiles
                SET telegram_user_id='', telegram_username='', telegram_phone='',
                    telegram_verified_at=0, telegram_session_name='', updated_at=?
                WHERE id=?
                """,
                (now, profile_id),
            )

    def update_profile_game_info(self, profile_id: int, **fields) -> None:
        allowed = {
            "display_name",
            "artifact_text",
            "spirit_root",
            "stage_name",
            "cultivation_text",
            "poison_text",
            "kill_count_text",
            "game_name",
            "account_name",
        }
        updates = {key: value for key, value in fields.items() if key in allowed}
        if not updates:
            return
        now = time.time()
        updates["info_updated_at"] = now
        updates["updated_at"] = now
        assignments = ", ".join(f"{key}=?" for key in updates)
        values = list(updates.values()) + [profile_id]
        with self.connect() as conn:
            conn.execute(f"UPDATE profiles SET {assignments} WHERE id=?", values)

    def update_profile_sect_info(self, profile_id: int, **fields) -> None:
        allowed = {
            "sect_name",
            "sect_leader",
            "sect_position",
            "sect_description",
            "sect_bonus_text",
            "sect_contribution_text",
        }
        updates = {key: value for key, value in fields.items() if key in allowed}
        if not updates:
            return
        now = time.time()
        updates["sect_info_updated_at"] = now
        updates["updated_at"] = now
        assignments = ", ".join(f"{key}=?" for key in updates)
        values = list(updates.values()) + [profile_id]
        with self.connect() as conn:
            conn.execute(f"UPDATE profiles SET {assignments} WHERE id=?", values)

    def list_chat_bindings(self, profile_id: int) -> list[ChatBinding]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM chat_bindings WHERE profile_id=? ORDER BY is_active DESC, created_at ASC, id ASC",
                (profile_id,),
            ).fetchall()
        return [self._row_to_chat(row) for row in rows]

    def get_chat_binding(
        self, profile_id: int, chat_id: int, thread_id: Optional[int] = None
    ) -> Optional[ChatBinding]:
        query = "SELECT * FROM chat_bindings WHERE profile_id=? AND chat_id=?"
        params = [profile_id, chat_id]
        if thread_id is None:
            query += " ORDER BY CASE WHEN thread_id IS NULL THEN 0 ELSE 1 END, is_active DESC, id ASC LIMIT 1"
        else:
            query += " AND thread_id=? ORDER BY is_active DESC, id ASC LIMIT 1"
            params.append(thread_id)
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_chat(row) if row else None

    def create_chat_binding(
        self,
        profile_id: int,
        chat_id: int,
        thread_id: Optional[int] = None,
        chat_type: str = "group",
        bot_username: str = "",
        bot_id: Optional[int] = None,
        telegram_user_id: str = "",
        is_active: bool = True,
    ) -> ChatBinding:
        normalized_bot_username = _normalize_bot_username(bot_username)
        normalized_bot_id = _normalize_optional_int(bot_id)
        now = time.time()
        with self.connect() as conn:
            existing_rows = conn.execute(
                """
                SELECT * FROM chat_bindings
                WHERE profile_id=? AND chat_id=?
                  AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                ORDER BY is_active DESC, id ASC
                """,
                (profile_id, chat_id, thread_id),
            ).fetchall()
            existing = None
            if normalized_bot_id is not None:
                existing = next(
                    (
                        row
                        for row in existing_rows
                        if _normalize_optional_int(row["bot_id"]) == normalized_bot_id
                    ),
                    None,
                )
            if existing is None and normalized_bot_username:
                existing = next(
                    (
                        row
                        for row in existing_rows
                        if _normalize_bot_username(row["bot_username"])
                        == normalized_bot_username
                    ),
                    None,
                )
            if existing is None and len(existing_rows) == 1:
                existing = existing_rows[0]
            if existing:
                shared_rows = conn.execute(
                    """
                    SELECT * FROM chat_bindings
                    WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                    """,
                    (chat_id, thread_id),
                ).fetchall()
                existing_bot_ids = _merge_bot_id_lists(
                    *[
                        row["bot_ids"]
                        if "bot_ids" in row.keys() and _normalize_bot_id_list(row["bot_ids"])
                        else row["bot_id"]
                        for row in shared_rows
                    ],
                    _guess_bot_ids_from_username(existing["bot_username"] if "bot_username" in existing.keys() else ""),
                )
                new_bot_ids = _merge_bot_id_lists(
                    existing_bot_ids,
                    _guess_bot_ids_from_username(normalized_bot_username),
                )
                if not new_bot_ids:
                    new_bot_ids = _merge_bot_id_lists(normalized_bot_id)
                new_bot_usernames = _merge_bot_username_maps(
                    *[row["bot_usernames"] if "bot_usernames" in row.keys() else {} for row in shared_rows],
                    {normalized_bot_id: normalized_bot_username}
                    if normalized_bot_id is not None and normalized_bot_username
                    else {},
                )
                conn.execute(
                    """
                    UPDATE chat_bindings
                    SET chat_type=?, bot_username=?, bot_id=?, bot_ids=?, bot_usernames=?, telegram_user_id=?, is_active=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        chat_type,
                        normalized_bot_username,
                        normalized_bot_id,
                        _json_dumps_compact(new_bot_ids),
                        _json_dumps_bot_username_map(new_bot_usernames),
                        telegram_user_id,
                        1 if is_active else 0,
                        now,
                        existing["id"],
                    ),
                )
                for row in shared_rows:
                    if row["id"] == existing["id"]:
                        continue
                    conn.execute(
                        "UPDATE chat_bindings SET bot_ids=?, bot_usernames=?, updated_at=? WHERE id=?",
                        (_json_dumps_compact(new_bot_ids), _json_dumps_bot_username_map(new_bot_usernames), now, row["id"]),
                    )
                binding_id = existing["id"]
            else:
                shared_rows = conn.execute(
                    """
                    SELECT * FROM chat_bindings
                    WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                    """,
                    (chat_id, thread_id),
                ).fetchall()
                initial_bot_ids = _merge_bot_id_lists(
                    DEFAULT_CHAT_BINDING_BOT_IDS,
                    *[
                        row["bot_ids"]
                        if "bot_ids" in row.keys() and _normalize_bot_id_list(row["bot_ids"])
                        else row["bot_id"]
                        for row in shared_rows
                    ],
                    normalized_bot_id,
                    _guess_bot_ids_from_username(normalized_bot_username),
                )
                initial_bot_usernames = _merge_bot_username_maps(
                    *[row["bot_usernames"] if "bot_usernames" in row.keys() else {} for row in shared_rows],
                    {normalized_bot_id: normalized_bot_username}
                    if normalized_bot_id is not None and normalized_bot_username
                    else {},
                )
                cursor = conn.execute(
                    """
                    INSERT INTO chat_bindings (
                        profile_id, chat_id, thread_id, chat_type, bot_username,
                        bot_id, bot_ids, bot_usernames,
                        telegram_user_id, is_active, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        profile_id,
                        chat_id,
                        thread_id,
                        chat_type,
                        normalized_bot_username,
                        normalized_bot_id,
                        _json_dumps_compact(initial_bot_ids),
                        _json_dumps_bot_username_map(initial_bot_usernames),
                        telegram_user_id,
                        1 if is_active else 0,
                        now,
                    ),
                )
                binding_id = cursor.lastrowid
                if initial_bot_ids:
                    for row in shared_rows:
                        conn.execute(
                            "UPDATE chat_bindings SET bot_ids=?, bot_usernames=?, updated_at=? WHERE id=?",
                            (_json_dumps_compact(initial_bot_ids), _json_dumps_bot_username_map(initial_bot_usernames), now, row["id"]),
                        )
        return self.get_binding_by_id(binding_id)

    def get_binding_by_id(self, binding_id: int) -> Optional[ChatBinding]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM chat_bindings WHERE id=?", (binding_id,)
            ).fetchone()
        return self._row_to_chat(row) if row else None

    def set_chat_binding_thread_id(
        self, profile_id: int, chat_id: int, thread_id: Optional[int]
    ) -> None:
        if thread_id is None:
            return
        now = time.time()
        with self.connect() as conn:
            existing = conn.execute(
                """
                SELECT id FROM chat_bindings
                WHERE profile_id=? AND chat_id=? AND thread_id=?
                ORDER BY is_active DESC, id ASC
                LIMIT 1
                """,
                (profile_id, chat_id, int(thread_id)),
            ).fetchone()
            if existing:
                return
            conn.execute(
                """
                UPDATE chat_bindings
                SET thread_id=?, updated_at=?
                WHERE id=(
                    SELECT id FROM chat_bindings
                    WHERE profile_id=? AND chat_id=? AND thread_id IS NULL
                    ORDER BY is_active DESC, id ASC
                    LIMIT 1
                )
                """,
                (int(thread_id), now, profile_id, chat_id),
            )

    def sync_env_chat_binding(
        self,
        profile_id: int,
        chat_id: Optional[int],
        thread_id: Optional[int] = None,
        chat_type: str = "group",
        bot_username: str = "",
        bot_id: Optional[int] = None,
        telegram_user_id: str = "",
    ) -> Optional[ChatBinding]:
        if chat_id is None:
            return None
        return self.create_chat_binding(
            profile_id=profile_id,
            chat_id=chat_id,
            thread_id=thread_id,
            chat_type=chat_type,
            bot_username=bot_username,
            bot_id=bot_id,
            telegram_user_id=telegram_user_id,
            is_active=True,
        )

    def get_chat_binding_bot_ids(
        self, profile_id: int, chat_id: int, thread_id: Optional[int] = None
    ) -> list[int]:
        binding = self.get_chat_binding(profile_id, chat_id, thread_id=thread_id)
        if not binding:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM chat_bindings WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0) ORDER BY is_active DESC, created_at ASC, id ASC LIMIT 1",
                    (int(chat_id), thread_id),
                ).fetchone()
            binding = self._row_to_chat(row) if row else None
        if not binding:
            return list(DEFAULT_CHAT_BINDING_BOT_IDS)
        bot_ids = _merge_bot_id_lists(binding.bot_ids)
        return bot_ids or list(DEFAULT_CHAT_BINDING_BOT_IDS)

    def set_chat_binding_bot_ids(
        self,
        profile_id: int,
        chat_id: int,
        bot_ids: list[int],
        thread_id: Optional[int] = None,
    ) -> Optional[ChatBinding]:
        normalized_ids = _merge_bot_id_lists(bot_ids)
        binding = self.get_chat_binding(profile_id, chat_id, thread_id=thread_id)
        if not binding:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM chat_bindings WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0) ORDER BY is_active DESC, created_at ASC, id ASC LIMIT 1",
                    (int(chat_id), thread_id),
                ).fetchone()
            binding = self._row_to_chat(row) if row else None
        if not binding:
            return None
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE chat_bindings
                SET bot_ids=?, updated_at=?
                WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                """,
                (_json_dumps_compact(normalized_ids), time.time(), int(chat_id), thread_id),
            )
        return self.get_binding_by_id(binding.id)

    def get_chat_binding_bot_usernames(
        self, profile_id: int, chat_id: int, thread_id: Optional[int] = None
    ) -> dict[int, str]:
        binding = self.get_chat_binding(profile_id, chat_id, thread_id=thread_id)
        if not binding:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM chat_bindings WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0) ORDER BY is_active DESC, created_at ASC, id ASC LIMIT 1",
                    (int(chat_id), thread_id),
                ).fetchone()
            binding = self._row_to_chat(row) if row else None
        if not binding:
            return {}
        bot_ids = self.get_chat_binding_bot_ids(profile_id, chat_id, thread_id=thread_id)
        username_map = dict(getattr(binding, "bot_usernames", {}) or {})
        with self.connect() as conn:
            for bot_id in bot_ids:
                if username_map.get(int(bot_id)):
                    continue
                row = conn.execute(
                    """
                    SELECT sender_username
                    FROM bound_messages
                    WHERE chat_id=? AND sender_id=? AND sender_username IS NOT NULL AND sender_username<>''
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """,
                    (int(chat_id), int(bot_id)),
                ).fetchone()
                username = _normalize_bot_username(row["sender_username"] if row else "")
                if username:
                    username_map[int(bot_id)] = username
            conn.execute(
                """
                UPDATE chat_bindings
                SET bot_usernames=?, updated_at=?
                WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                """,
                (
                    _json_dumps_bot_username_map(username_map),
                    time.time(),
                    int(chat_id),
                    thread_id,
                ),
            )
        return username_map

    def add_chat_binding_bot_id(
        self,
        profile_id: int,
        chat_id: int,
        bot_id: object,
        bot_username: str = "",
        thread_id: Optional[int] = None,
    ) -> Optional[ChatBinding]:
        normalized = _normalize_optional_int(bot_id)
        if normalized is None:
            return self.get_chat_binding(profile_id, chat_id, thread_id=thread_id)
        binding = self.get_chat_binding(profile_id, chat_id, thread_id=thread_id)
        if not binding:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM chat_bindings WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0) ORDER BY is_active DESC, created_at ASC, id ASC LIMIT 1",
                    (int(chat_id), thread_id),
                ).fetchone()
            binding = self._row_to_chat(row) if row else None
        if not binding:
            return None
        bot_ids = _merge_bot_id_lists(binding.bot_ids, normalized)
        username_map = dict(getattr(binding, "bot_usernames", {}) or {})
        normalized_username = _normalize_bot_username(bot_username)
        if normalized_username:
            username_map[int(normalized)] = normalized_username
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE chat_bindings
                SET bot_ids=?, bot_usernames=?, bot_id=COALESCE(bot_id, ?), updated_at=?
                WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                """,
                (_json_dumps_compact(bot_ids), _json_dumps_bot_username_map(username_map), normalized, time.time(), int(chat_id), thread_id),
            )
        return self.get_binding_by_id(binding.id)

    def remove_chat_binding_bot_id(
        self,
        profile_id: int,
        chat_id: int,
        bot_id: object,
        thread_id: Optional[int] = None,
    ) -> Optional[ChatBinding]:
        normalized = _normalize_optional_int(bot_id)
        binding = self.get_chat_binding(profile_id, chat_id, thread_id=thread_id)
        if not binding:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM chat_bindings WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0) ORDER BY is_active DESC, created_at ASC, id ASC LIMIT 1",
                    (int(chat_id), thread_id),
                ).fetchone()
            binding = self._row_to_chat(row) if row else None
        if not binding:
            return None
        if normalized is None:
            return binding
        bot_ids = [value for value in _merge_bot_id_lists(binding.bot_ids) if value != normalized]
        username_map = dict(getattr(binding, "bot_usernames", {}) or {})
        username_map.pop(int(normalized), None)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE chat_bindings
                SET bot_ids=?, bot_usernames=?, bot_id=CASE WHEN bot_id=? THEN NULL ELSE bot_id END, updated_at=?
                WHERE chat_id=? AND COALESCE(thread_id, 0)=COALESCE(?, 0)
                """,
                (_json_dumps_compact(bot_ids), _json_dumps_bot_username_map(username_map), normalized, time.time(), int(chat_id), thread_id),
            )
        return self.get_binding_by_id(binding.id)

    def resolve_bot_id_from_username(self, bot_username: str) -> Optional[int]:
        bot_ids = _guess_bot_ids_from_username(bot_username)
        return bot_ids[0] if bot_ids else None

    def get_primary_chat_binding(
        self, profile_id: int, bot_username: str = ""
    ) -> Optional[ChatBinding]:
        query = "SELECT * FROM chat_bindings WHERE profile_id=? AND is_active=1"
        params = [profile_id]
        if bot_username:
            query += " AND LOWER(bot_username)=LOWER(?)"
            params.append(bot_username.lstrip("@"))
        query += " ORDER BY CASE WHEN thread_id IS NULL THEN 0 ELSE 1 END, created_at ASC, id ASC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_chat(row) if row else None

    def resolve_chat_binding_for_event(
        self,
        profile_id: int,
        chat_id: int,
        thread_id: Optional[int],
        reply_to_msg_id: Optional[int],
    ) -> Optional[ChatBinding]:
        bindings = [
            binding
            for binding in self.list_chat_bindings(profile_id)
            if binding.chat_id == chat_id and binding.is_active
        ]
        if not bindings:
            return None
        if thread_id is not None:
            for binding in bindings:
                if binding.thread_id == thread_id:
                    return binding
        if reply_to_msg_id is not None:
            for binding in bindings:
                if binding.thread_id == reply_to_msg_id:
                    return binding
            parent_message = self.get_bound_message(
                chat_id,
                int(reply_to_msg_id),
                profile_id=int(profile_id),
            )
            if parent_message:
                parent_thread_id = parent_message.get("thread_id")
                if parent_thread_id is not None:
                    for binding in bindings:
                        if binding.thread_id == int(parent_thread_id):
                            return binding
        for binding in bindings:
            if binding.thread_id is None:
                return binding
        if len(bindings) == 1 and bindings[0].thread_id is None:
            return bindings[0]
        return None

    def ensure_module_settings(
        self, profile_id: int, modules: Iterable[FeatureModule]
    ) -> None:
        now = time.time()
        with self.connect() as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    "SELECT module_key FROM module_settings WHERE profile_id=?",
                    (profile_id,),
                ).fetchall()
            }
            for module in modules:
                module_key = getattr(module, "key", "")
                if not module_key or module_key in existing:
                    continue
                conn.execute(
                    """
                    INSERT INTO module_settings (
                        profile_id, module_key, enabled, cooldown_seconds,
                        check_interval_seconds, command_template, notes, updated_at
                    ) VALUES (?, ?, 0, 30, 300, '', '', ?)
                    """,
                    (profile_id, module_key, now),
                )

    def list_module_settings(self, profile_id: int) -> list[ModuleSetting]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM module_settings WHERE profile_id=? ORDER BY module_key ASC",
                (profile_id,),
            ).fetchall()
        return [self._row_to_setting(row) for row in rows]

    def get_module_setting(
        self, profile_id: int, module_key: str
    ) -> Optional[ModuleSetting]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM module_settings WHERE profile_id=? AND module_key=?",
                (profile_id, module_key),
            ).fetchone()
        return self._row_to_setting(row) if row else None

    def save_module_setting(
        self,
        profile_id: int,
        module_key: str,
        enabled: bool,
        cooldown_seconds: int,
        check_interval_seconds: int,
        command_template: str,
        notes: str,
    ) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO module_settings (
                    profile_id, module_key, enabled, cooldown_seconds,
                    check_interval_seconds, command_template, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id, module_key) DO UPDATE SET
                    enabled=excluded.enabled,
                    cooldown_seconds=excluded.cooldown_seconds,
                    check_interval_seconds=excluded.check_interval_seconds,
                    command_template=excluded.command_template,
                    notes=excluded.notes,
                    updated_at=excluded.updated_at
                """,
                (
                    profile_id,
                    module_key,
                    1 if enabled else 0,
                    int(cooldown_seconds or 0),
                    int(check_interval_seconds or 0),
                    command_template or "",
                    notes or "",
                    now,
                ),
            )

    def set_module_enabled(
        self, profile_id: int, module_key: str, enabled: bool
    ) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                "UPDATE module_settings SET enabled=?, updated_at=? WHERE profile_id=? AND module_key=?",
                (1 if enabled else 0, now, profile_id, module_key),
            )

    def create_app_session(
        self,
        profile_id: int,
        expires_seconds: int = 86400 * 7,
        session_token: str = "",
    ) -> str:
        now = time.time()
        current_token = str(session_token or "").strip()
        current_token_hash = (
            hashlib.sha256(current_token.encode("utf-8")).hexdigest()
            if current_token
            else ""
        )
        new_token = current_token or secrets.token_urlsafe(32)
        new_token_hash = hashlib.sha256(new_token.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            existing_session = None
            if current_token_hash:
                existing_session = conn.execute(
                    """
                    SELECT * FROM browser_sessions
                    WHERE session_token_hash=? AND revoked_at=0 AND expires_at>?
                    ORDER BY id DESC LIMIT 1
                    """,
                    (current_token_hash, now),
                ).fetchone()
            if existing_session:
                browser_session_id = int(existing_session["id"])
                conn.execute(
                    """
                    UPDATE browser_sessions
                    SET current_profile_id=?, expires_at=?, updated_at=?
                    WHERE id=?
                    """,
                    (int(profile_id), now + expires_seconds, now, browser_session_id),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO browser_sessions (
                        session_token_hash, current_profile_id, expires_at, created_at, updated_at, revoked_at
                    ) VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (new_token_hash, int(profile_id), now + expires_seconds, now, now),
                )
                browser_session_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO browser_session_profiles (
                    browser_session_id, profile_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(browser_session_id, profile_id) DO UPDATE SET
                    updated_at=excluded.updated_at
                """,
                (browser_session_id, int(profile_id), now, now),
            )
        return new_token

    def get_game_items(self) -> dict:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM game_items").fetchall()
        return {row["id"]: dict(row) for row in rows}

    def upsert_game_items(self, items: list[dict]) -> None:
        now = time.time()
        values = []
        for item in items:
            item_id = item.get("id") or item.get("item_id") or ""
            values.append(
                (
                    item_id,
                    item.get("name", ""),
                    item.get("description", ""),
                    item.get("type", ""),
                    int(item.get("rarity") or 0),
                    int(item.get("value") or 0),
                    now,
                )
            )
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO game_items (id, name, description, type, rarity, value, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    description=excluded.description,
                    type=excluded.type,
                    rarity=excluded.rarity,
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                values,
            )

    def upsert_game_items_partial(self, items: list[dict]) -> None:
        now = time.time()
        values = []
        for item in items:
            item_id = str(item.get("id") or item.get("item_id") or "").strip()
            if not item_id:
                continue
            values.append(
                (
                    item_id,
                    str(item.get("name") or ""),
                    str(item.get("description") or ""),
                    str(item.get("type") or ""),
                    int(item.get("rarity") or 0),
                    int(item.get("value") or 0),
                    now,
                )
            )
        if not values:
            return
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO game_items (id, name, description, type, rarity, value, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=CASE WHEN excluded.name != '' THEN excluded.name ELSE game_items.name END,
                    description=CASE WHEN excluded.description != '' THEN excluded.description ELSE game_items.description END,
                    type=CASE WHEN excluded.type != '' THEN excluded.type ELSE game_items.type END,
                    rarity=CASE WHEN excluded.rarity != 0 THEN excluded.rarity ELSE game_items.rarity END,
                    value=CASE WHEN excluded.value != 0 THEN excluded.value ELSE game_items.value END,
                    updated_at=excluded.updated_at
                """,
                values,
            )

    def get_shop_items(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM shop_items ORDER BY shop_price ASC, item_id ASC"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_level_thresholds(self) -> dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT stage_name, threshold FROM level_thresholds"
            ).fetchall()
        return {str(row["stage_name"]): int(row["threshold"] or 0) for row in rows}

    def replace_level_thresholds(self, mappings: dict[str, int]) -> None:
        now = time.time()
        values = []
        for stage_name, threshold in (mappings or {}).items():
            name = str(stage_name or "").strip()
            if not name:
                continue
            values.append((name, int(threshold or 0), now))
        with self.connect() as conn:
            conn.execute("DELETE FROM level_thresholds")
            if values:
                conn.executemany(
                    """
                    INSERT INTO level_thresholds (stage_name, threshold, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    values,
                )

    def replace_shop_items(self, items: list[dict]) -> None:
        now = time.time()
        values = []
        for item in items:
            item_id = str(item.get("item_id") or item.get("id") or "").strip()
            if not item_id:
                continue
            values.append(
                (
                    item_id,
                    str(item.get("name") or ""),
                    str(item.get("description") or ""),
                    str(item.get("type") or ""),
                    int(item.get("shop_price") or 0),
                    str(item.get("sect_exclusive") or ""),
                    now,
                )
            )
        with self.connect() as conn:
            conn.execute("DELETE FROM shop_items")
            if values:
                conn.executemany(
                    """
                    INSERT INTO shop_items (
                        item_id, name, description, type, shop_price,
                        sect_exclusive, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )

    def get_marketplace_listings(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM marketplace_listings ORDER BY listing_time DESC, id DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    def replace_marketplace_listings(self, items: list[dict]) -> None:
        now = time.time()
        values = []
        for item in items:
            listing_id = int(item.get("id") or 0)
            if not listing_id:
                continue
            values.append(
                (
                    listing_id,
                    str(item.get("item_id") or "").strip(),
                    str(item.get("item_type") or "").strip(),
                    str(item.get("item_name") or item.get("name") or "").strip(),
                    str(item.get("listing_time") or "").strip(),
                    int(item.get("quantity") or 0),
                    json.dumps(item.get("price_json") or {}, ensure_ascii=False),
                    str(item.get("seller_username") or "").strip(),
                    1 if item.get("is_bundle") else 0,
                    1 if item.get("is_material") else 0,
                    now,
                )
            )
        with self.connect() as conn:
            conn.execute("DELETE FROM marketplace_listings")
            if values:
                conn.executemany(
                    """
                    INSERT INTO marketplace_listings (
                        id, item_id, item_type, item_name, listing_time, quantity,
                        price_json, seller_username, is_bundle, is_material, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )

    def get_profile_by_session_token(
        self, session_token: str
    ) -> Optional[PlayerProfile]:
        token = str(session_token or "").strip()
        if not token:
            return None
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        now = time.time()
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT p.*
                FROM browser_sessions s
                JOIN profiles p ON p.id = s.current_profile_id
                WHERE s.session_token_hash=? AND s.revoked_at=0 AND s.expires_at>?
                ORDER BY s.id DESC
                LIMIT 1
                """,
                (token_hash, now),
            ).fetchone()
        return self._row_to_profile(row) if row else None

    def list_profiles_by_session_token(self, session_token: str) -> list[PlayerProfile]:
        token = str(session_token or "").strip()
        if not token:
            return []
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        now = time.time()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT p.*
                FROM browser_sessions s
                JOIN browser_session_profiles sp ON sp.browser_session_id = s.id
                JOIN profiles p ON p.id = sp.profile_id
                WHERE s.session_token_hash=? AND s.revoked_at=0 AND s.expires_at>?
                ORDER BY CASE WHEN p.id = s.current_profile_id THEN 0 ELSE 1 END,
                         p.updated_at DESC,
                         p.id DESC
                """,
                (token_hash, now),
            ).fetchall()
        return [self._row_to_profile(row) for row in rows]

    def set_current_profile_by_session_token(
        self, session_token: str, profile_id: int
    ) -> Optional[PlayerProfile]:
        token = str(session_token or "").strip()
        if not token:
            return None
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        now = time.time()
        with self.connect() as conn:
            browser_session = conn.execute(
                """
                SELECT * FROM browser_sessions
                WHERE session_token_hash=? AND revoked_at=0 AND expires_at>?
                ORDER BY id DESC LIMIT 1
                """,
                (token_hash, now),
            ).fetchone()
            if not browser_session:
                return None
            allowed = conn.execute(
                """
                SELECT 1 FROM browser_session_profiles
                WHERE browser_session_id=? AND profile_id=?
                LIMIT 1
                """,
                (int(browser_session["id"]), int(profile_id)),
            ).fetchone()
            if not allowed:
                return None
            conn.execute(
                "UPDATE browser_sessions SET current_profile_id=?, updated_at=? WHERE id=?",
                (int(profile_id), now, int(browser_session["id"])),
            )
        return self.get_profile(int(profile_id))

    def attach_profile_to_session_token(self, session_token: str, profile_id: int) -> bool:
        token = str(session_token or "").strip()
        if not token:
            return False
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        now = time.time()
        with self.connect() as conn:
            browser_session = conn.execute(
                """
                SELECT * FROM browser_sessions
                WHERE session_token_hash=? AND revoked_at=0 AND expires_at>?
                ORDER BY id DESC LIMIT 1
                """,
                (token_hash, now),
            ).fetchone()
            if not browser_session:
                return False
            profile = conn.execute(
                "SELECT 1 FROM profiles WHERE id=? LIMIT 1",
                (int(profile_id),),
            ).fetchone()
            if not profile:
                return False
            conn.execute(
                """
                INSERT INTO browser_session_profiles (
                    browser_session_id, profile_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(browser_session_id, profile_id) DO UPDATE SET
                    updated_at=excluded.updated_at
                """,
                (int(browser_session["id"]), int(profile_id), now, now),
            )
        return True

    def remove_profile_from_session_token(
        self, session_token: str, profile_id: int
    ) -> bool:
        token = str(session_token or "").strip()
        if not token:
            return False
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        now = time.time()
        with self.connect() as conn:
            browser_session = conn.execute(
                """
                SELECT * FROM browser_sessions
                WHERE session_token_hash=? AND revoked_at=0 AND expires_at>?
                ORDER BY id DESC LIMIT 1
                """,
                (token_hash, now),
            ).fetchone()
            if not browser_session:
                return False
            browser_session_id = int(browser_session["id"])
            conn.execute(
                "DELETE FROM browser_session_profiles WHERE browser_session_id=? AND profile_id=?",
                (browser_session_id, int(profile_id)),
            )
            remaining = conn.execute(
                "SELECT profile_id FROM browser_session_profiles WHERE browser_session_id=? ORDER BY updated_at DESC, id DESC",
                (browser_session_id,),
            ).fetchall()
            if not remaining:
                conn.execute(
                    "UPDATE browser_sessions SET current_profile_id=NULL, revoked_at=?, updated_at=? WHERE id=?",
                    (now, now, browser_session_id),
                )
                return False
            current_profile_id = int(browser_session["current_profile_id"] or 0)
            if current_profile_id == int(profile_id):
                conn.execute(
                    "UPDATE browser_sessions SET current_profile_id=?, updated_at=? WHERE id=?",
                    (int(remaining[0]["profile_id"]), now, browser_session_id),
                )
        return True

    def revoke_app_session(self, session_token: str) -> None:
        token = str(session_token or "").strip()
        if not token:
            return
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self.connect() as conn:
            conn.execute(
                "UPDATE browser_sessions SET revoked_at=?, updated_at=? WHERE session_token_hash=?",
                (time.time(), time.time(), token_hash),
            )

    def get_runtime_state(self, key: str) -> Optional[str]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM app_runtime_state WHERE key=?",
                (key or "",),
            ).fetchone()
        return row["value"] if row else None

    def set_runtime_state(self, key: str, value: str) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO app_runtime_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key or "", value or "", now),
            )

    def delete_bound_messages_older_than(
        self,
        max_age_seconds: int = BOUND_MESSAGE_RETENTION_SECONDS,
        *,
        now: Optional[float] = None,
    ) -> int:
        safe_age_seconds = max(int(max_age_seconds or 0), 0)
        if safe_age_seconds <= 0:
            return 0
        cutoff = float(now if now is not None else time.time()) - safe_age_seconds
        with self.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM bound_messages WHERE created_at < ?",
                (cutoff,),
            )
        return int(cursor.rowcount or 0)

    def maybe_cleanup_bound_messages(
        self,
        *,
        max_age_seconds: int = BOUND_MESSAGE_RETENTION_SECONDS,
        min_interval_seconds: int = BOUND_MESSAGE_CLEANUP_INTERVAL_SECONDS,
        now: Optional[float] = None,
    ) -> int:
        current_time = float(now if now is not None else time.time())
        state_key = "bound_messages:last_cleanup_at"
        last_cleanup_text = self.get_runtime_state(state_key) or ""
        try:
            last_cleanup_at = float(last_cleanup_text)
        except (TypeError, ValueError):
            last_cleanup_at = 0.0
        if (
            min_interval_seconds > 0
            and last_cleanup_at
            and current_time - last_cleanup_at < int(min_interval_seconds)
        ):
            return 0
        deleted_count = self.delete_bound_messages_older_than(
            max_age_seconds=max_age_seconds,
            now=current_time,
        )
        self.set_runtime_state(state_key, str(current_time))
        return deleted_count

    def get_external_cookie_override(self) -> Optional[str]:
        return self.get_runtime_state("asc_default_cookie_override")

    def set_external_cookie_override(self, cookie_text: str) -> None:
        self.set_runtime_state("asc_default_cookie_override", cookie_text or "")

    def clear_external_cookie_override(self) -> None:
        self.set_external_cookie_override("")

    def upsert_external_account(
        self,
        profile_id: int,
        provider: str,
        telegram_user_id: str,
        telegram_username: str,
        status: str,
        cookie_text: str,
        me_payload: dict,
        api_token: str,
    ) -> dict:
        now = time.time()
        me_json = json.dumps(me_payload or {}, ensure_ascii=False)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO external_accounts (
                    profile_id, provider, telegram_user_id, telegram_username,
                    status, cookie_text, api_token, me_json,
                    last_verified_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?)
                ON CONFLICT(profile_id, provider) DO UPDATE SET
                    telegram_user_id=excluded.telegram_user_id,
                    telegram_username=excluded.telegram_username,
                    status=excluded.status,
                    cookie_text=excluded.cookie_text,
                    api_token=excluded.api_token,
                    me_json=excluded.me_json,
                    last_verified_at=excluded.last_verified_at,
                    last_error='',
                    updated_at=excluded.updated_at
                """,
                (
                    profile_id,
                    provider,
                    telegram_user_id or "",
                    telegram_username or "",
                    status or "connected",
                    cookie_text or "",
                    api_token or "",
                    me_json,
                    now,
                    now,
                    now,
                ),
            )
        return self.get_external_account(profile_id, provider) or {}

    def get_external_account(self, profile_id: int, provider: str) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM external_accounts WHERE profile_id=? AND provider=?",
                (profile_id, provider),
            ).fetchone()
        return dict(row) if row else None

    def mark_external_account_error(
        self, profile_id: int, provider: str, error: str, *, status: str = "error"
    ) -> None:
        now = time.time()
        clear_api_token = status == "expired"
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO external_accounts (
                    profile_id, provider, telegram_user_id, telegram_username,
                    status, cookie_text, me_json,
                    last_verified_at, last_error, created_at, updated_at
                ) VALUES (?, ?, '', '', ?, '', '{}', 0, ?, ?, ?)
                ON CONFLICT(profile_id, provider) DO UPDATE SET
                    status=excluded.status,
                    api_token=CASE WHEN ? THEN '' ELSE external_accounts.api_token END,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at
                """,
                (
                    profile_id,
                    provider,
                    status or "error",
                    error or "",
                    now,
                    now,
                    1 if clear_api_token else 0,
                ),
            )

    def clear_external_account(
        self, profile_id: int, provider: str, *, status: str = "logged_out"
    ) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO external_accounts (
                    profile_id, provider, telegram_user_id, telegram_username,
                    status, cookie_text, me_json,
                    last_verified_at, last_error, created_at, updated_at
                ) VALUES (?, ?, '', '', ?, '', '{}', 0, '', ?, ?)
                ON CONFLICT(profile_id, provider) DO UPDATE SET
                    status=excluded.status,
                    cookie_text='',
                    me_json='{}',
                    last_verified_at=0,
                    last_error='',
                    updated_at=excluded.updated_at
                """,
                (profile_id, provider, status or "logged_out", now, now),
            )

    def create_telegram_login_challenge(
        self,
        phone: str,
        phone_code_hash: str,
        session_name: str,
        status: str = "code_sent",
        expires_seconds: int = 600,
    ) -> int:
        now = time.time()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO telegram_login_challenges (
                    phone, phone_code_hash, session_name, status,
                    created_at, expires_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    phone or "",
                    phone_code_hash or "",
                    session_name or "",
                    status or "code_sent",
                    now,
                    now + expires_seconds,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def get_telegram_login_challenge(self, challenge_id: int) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM telegram_login_challenges WHERE id=?",
                (challenge_id,),
            ).fetchone()
        return dict(row) if row else None

    def update_telegram_login_challenge_status(
        self,
        challenge_id: int,
        status: str,
        phone_code_hash: Optional[str] = None,
        session_name: Optional[str] = None,
        expires_at: Optional[float] = None,
    ) -> None:
        updates = {"status": status or "code_sent", "updated_at": time.time()}
        if phone_code_hash is not None:
            updates["phone_code_hash"] = phone_code_hash
        if session_name is not None:
            updates["session_name"] = session_name
        if expires_at is not None:
            updates["expires_at"] = expires_at
        assignments = ", ".join(f"{key}=?" for key in updates)
        values = list(updates.values()) + [challenge_id]
        with self.connect() as conn:
            conn.execute(
                f"UPDATE telegram_login_challenges SET {assignments} WHERE id=?",
                values,
            )

    def delete_telegram_login_challenge(self, challenge_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM telegram_login_challenges WHERE id=?",
                (challenge_id,),
            )

    def record_cultivation_result(
        self,
        profile_id: Optional[int],
        chat_id: int,
        mode: str,
        event: str,
        gain_value: Optional[int],
        stage_name: str,
        progress_text: str,
        summary: str,
        raw_text: str,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO cultivation_results (
                    profile_id, chat_id, mode, event, gain_value,
                    stage_name, progress_text, summary, raw_text, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    profile_id,
                    chat_id,
                    mode or "normal",
                    event or "",
                    gain_value,
                    stage_name or "",
                    progress_text or "",
                    summary or "",
                    raw_text or "",
                    time.time(),
                ),
            )

    def list_cultivation_results(
        self,
        profile_id: int,
        limit: int = 50,
        offset: int = 0,
        since_seconds: Optional[int] = None,
    ) -> list[dict]:
        query = "SELECT * FROM cultivation_results WHERE profile_id=?"
        params = [profile_id]
        if since_seconds:
            query += " AND created_at>=?"
            params.append(time.time() - since_seconds)
        query += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def count_cultivation_results(
        self, profile_id: int, since_seconds: Optional[int] = None
    ) -> int:
        query = "SELECT COUNT(*) FROM cultivation_results WHERE profile_id=?"
        params = [profile_id]
        if since_seconds:
            query += " AND created_at>=?"
            params.append(time.time() - since_seconds)
        with self.connect() as conn:
            return int(conn.execute(query, params).fetchone()[0])

    def request_sect_refresh(self, profile_id: int, cooldown_seconds: int = 0) -> None:
        next_check_time = time.time() + max(int(cooldown_seconds or 0), 0)
        with self.connect() as conn:
            table_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sect_sessions'"
            ).fetchone()
            if not table_exists:
                return
            chat_ids = [
                binding.chat_id for binding in self.list_chat_bindings(profile_id)
            ]
            for chat_id in chat_ids:
                try:
                    conn.execute(
                        "UPDATE sect_sessions SET next_check_time=? WHERE chat_id=? AND (profile_id=? OR profile_id IS NULL OR profile_id=0)",
                        (next_check_time, chat_id, int(profile_id)),
                    )
                except sqlite3.OperationalError:
                    conn.execute(
                        "UPDATE sect_sessions SET next_check_time=? WHERE chat_id=?",
                        (next_check_time, chat_id),
                    )

    def request_cultivation_refresh(
        self, profile_id: int, cooldown_seconds: int = 0
    ) -> None:
        next_check_time = time.time() + max(int(cooldown_seconds or 0), 0)
        with self.connect() as conn:
            table_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='fanren_sessions'"
            ).fetchone()
            if not table_exists:
                return
            chat_ids = [
                binding.chat_id for binding in self.list_chat_bindings(profile_id)
            ]
            for chat_id in chat_ids:
                try:
                    conn.execute(
                        "UPDATE fanren_sessions SET next_check_time=? WHERE chat_id=? AND (profile_id=? OR profile_id IS NULL OR profile_id=0)",
                        (next_check_time, chat_id, int(profile_id)),
                    )
                except sqlite3.OperationalError:
                    conn.execute(
                        "UPDATE fanren_sessions SET next_check_time=? WHERE chat_id=?",
                        (next_check_time, chat_id),
                    )

    def get_cultivation_session(
        self, chat_id: int, profile_id: Optional[int] = None
    ) -> Optional[dict]:
        with self.connect() as conn:
            if profile_id is not None:
                try:
                    row = conn.execute(
                        "SELECT * FROM fanren_sessions WHERE chat_id=? AND (profile_id=? OR profile_id IS NULL OR profile_id=0) ORDER BY profile_id DESC LIMIT 1",
                        (chat_id, int(profile_id)),
                    ).fetchone()
                except sqlite3.OperationalError:
                    row = conn.execute(
                        "SELECT * FROM fanren_sessions WHERE chat_id=? LIMIT 1",
                        (chat_id,),
                    ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM fanren_sessions WHERE chat_id=? LIMIT 1",
                    (chat_id,),
                ).fetchone()
        return dict(row) if row else None

    def get_sect_session(
        self, chat_id: int, profile_id: Optional[int] = None
    ) -> Optional[dict]:
        with self.connect() as conn:
            if profile_id is not None:
                try:
                    row = conn.execute(
                        "SELECT * FROM sect_sessions WHERE chat_id=? AND (profile_id=? OR profile_id IS NULL OR profile_id=0) ORDER BY profile_id DESC, bot_username ASC LIMIT 1",
                        (chat_id, int(profile_id)),
                    ).fetchone()
                except sqlite3.OperationalError:
                    row = conn.execute(
                        "SELECT * FROM sect_sessions WHERE chat_id=? ORDER BY bot_username ASC LIMIT 1",
                        (chat_id,),
                    ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM sect_sessions WHERE chat_id=? ORDER BY bot_username ASC LIMIT 1",
                    (chat_id,),
                ).fetchone()
        return dict(row) if row else None

    def get_active_divination_batch(
        self, profile_id: int, chat_id: Optional[int] = None
    ) -> Optional[dict]:
        query = (
            "SELECT * FROM divination_batches WHERE profile_id=? AND status='active'"
        )
        params = [int(profile_id)]
        if chat_id is not None:
            query += " AND chat_id=?"
            params.append(int(chat_id))
        query += " ORDER BY created_at DESC, id DESC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def get_latest_divination_batch(
        self, profile_id: int, chat_id: Optional[int] = None
    ) -> Optional[dict]:
        query = "SELECT * FROM divination_batches WHERE profile_id=?"
        params = [int(profile_id)]
        if chat_id is not None:
            query += " AND chat_id=?"
            params.append(int(chat_id))
        query += " ORDER BY created_at DESC, id DESC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def start_divination_batch(
        self,
        profile_id: int,
        chat_id: int,
        target_count: int,
        initial_count: int,
        thread_id: Optional[int] = None,
        chat_type: str = "group",
        bot_username: str = "",
    ) -> int:
        now = time.time()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO divination_batches (
                    profile_id, chat_id, thread_id, chat_type, bot_username,
                    initial_count, target_count, sent_count, completed_count,
                    pending_command_msg_id, status, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 'active', '', ?, ?)
                """,
                (
                    int(profile_id),
                    int(chat_id),
                    thread_id,
                    chat_type or "group",
                    bot_username or "",
                    max(int(initial_count or 0), 0),
                    max(int(target_count or 0), 0),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_divination_batch(self, batch_id: int, **fields) -> Optional[dict]:
        if not fields:
            return self.get_divination_batch(batch_id)
        updates = {**fields, "updated_at": time.time()}
        assignments = ", ".join(f"{key}=?" for key in updates)
        values = list(updates.values()) + [int(batch_id)]
        with self.connect() as conn:
            conn.execute(
                f"UPDATE divination_batches SET {assignments} WHERE id=?",
                values,
            )
            row = conn.execute(
                "SELECT * FROM divination_batches WHERE id=?",
                (int(batch_id),),
            ).fetchone()
        return dict(row) if row else None

    def get_divination_batch(self, batch_id: int) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM divination_batches WHERE id=?",
                (int(batch_id),),
            ).fetchone()
        return dict(row) if row else None

    def finish_divination_batch(
        self, batch_id: int, status: str = "completed", last_error: str = ""
    ) -> Optional[dict]:
        return self.update_divination_batch(
            batch_id,
            status=(status or "completed").strip() or "completed",
            pending_command_msg_id=0,
            last_error=(last_error or "")[:1000],
        )

    def get_companion_auto_task(
        self, profile_id: int, chat_id: int, feature_key: str
    ) -> Optional[dict]:
        normalized_feature_key = str(feature_key or "").strip()
        if not normalized_feature_key:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM companion_auto_tasks
                WHERE profile_id=? AND chat_id=? AND feature_key=?
                ORDER BY updated_at DESC, id DESC LIMIT 1
                """,
                (int(profile_id), int(chat_id), normalized_feature_key),
            ).fetchone()
        return dict(row) if row else None

    def list_active_companion_auto_tasks(self, profile_id: int) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM companion_auto_tasks
                WHERE profile_id=? AND enabled=1
                ORDER BY updated_at DESC, id DESC
                """,
                (int(profile_id),),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_companion_auto_task(
        self,
        *,
        profile_id: int,
        chat_id: int,
        feature_key: str,
        enabled: bool,
        strategy: str = "",
        thread_id: Optional[int] = None,
        chat_type: str = "group",
        bot_username: str = "",
        next_run_at: float = 0,
        last_run_at: float = 0,
        last_error: str = "",
    ) -> dict:
        now = time.time()
        normalized_feature_key = str(feature_key or "").strip()
        if not normalized_feature_key:
            raise ValueError("Feature key is required")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO companion_auto_tasks (
                    profile_id, chat_id, thread_id, chat_type, bot_username,
                    feature_key, strategy, enabled, next_run_at, last_run_at, last_error,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id, chat_id, bot_username, feature_key) DO UPDATE SET
                    thread_id=excluded.thread_id,
                    chat_type=excluded.chat_type,
                    strategy=excluded.strategy,
                    enabled=excluded.enabled,
                    next_run_at=excluded.next_run_at,
                    last_run_at=excluded.last_run_at,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at
                """,
                (
                    int(profile_id),
                    int(chat_id),
                    thread_id,
                    chat_type or "group",
                    bot_username or "",
                    normalized_feature_key,
                    str(strategy or "")[:100],
                    1 if enabled else 0,
                    float(next_run_at or 0),
                    float(last_run_at or 0),
                    str(last_error or "")[:1000],
                    now,
                    now,
                ),
            )
        return (
            self.get_companion_auto_task(
                int(profile_id), int(chat_id), normalized_feature_key
            )
            or {}
        )

    def update_companion_auto_task(self, task_id: int, **fields) -> Optional[dict]:
        if not fields:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM companion_auto_tasks WHERE id=?",
                    (int(task_id),),
                ).fetchone()
            return dict(row) if row else None
        updates = {**fields, "updated_at": time.time()}
        assignments = ", ".join(f"{key}=?" for key in updates)
        values = list(updates.values()) + [int(task_id)]
        with self.connect() as conn:
            conn.execute(
                f"UPDATE companion_auto_tasks SET {assignments} WHERE id=?",
                values,
            )
            row = conn.execute(
                "SELECT * FROM companion_auto_tasks WHERE id=?",
                (int(task_id),),
            ).fetchone()
        return dict(row) if row else None

    def disable_companion_auto_task(
        self, profile_id: int, chat_id: int, feature_key: str, *, last_error: str = ""
    ) -> Optional[dict]:
        task = self.get_companion_auto_task(profile_id, chat_id, feature_key)
        if not task:
            return None
        return self.update_companion_auto_task(
            int(task["id"]),
            enabled=0,
            next_run_at=0,
            last_error=(last_error or "")[:1000],
        )

    def get_companion_heart_tribulation_task(
        self,
        profile_id: int,
        chat_id: int,
        *,
        thread_id: Optional[int] = None,
        bot_username: str = "",
    ) -> Optional[dict]:
        with self.connect() as conn:
            query = """
                SELECT * FROM companion_heart_tribulation_tasks
                WHERE profile_id=? AND chat_id=?
            """
            params: list[object] = [int(profile_id), int(chat_id)]
            normalized_bot_username = str(bot_username or "").strip().lower().lstrip("@")
            if normalized_bot_username:
                query += " AND LOWER(bot_username)=?"
                params.append(normalized_bot_username)
            if thread_id is not None:
                query += " ORDER BY CASE WHEN thread_id=? THEN 0 ELSE 1 END, updated_at DESC, id DESC LIMIT 1"
                params.append(int(thread_id))
            else:
                query += " ORDER BY updated_at DESC, id DESC LIMIT 1"
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def list_active_companion_heart_tribulation_tasks(
        self, profile_id: int
    ) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM companion_heart_tribulation_tasks
                WHERE profile_id=? AND enabled=1
                ORDER BY updated_at DESC, id DESC
                """,
                (int(profile_id),),
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_companion_heart_tribulation_task(
        self,
        *,
        profile_id: int,
        chat_id: int,
        enabled: bool,
        thread_id: Optional[int] = None,
        chat_type: str = "group",
        bot_username: str = "",
        run_id: str = "",
        workflow_state: str = "",
        next_run_at: float = 0,
        step_deadline_at: float = 0,
        last_run_at: float = 0,
        matched_bot_id: int = 0,
        anchor_command_msg_id: int = 0,
        anchor_bot_msg_id: int = 0,
        tribulation_command_msg_id: int = 0,
        tribulation_msg_id: int = 0,
        panel_reply_msg_id: int = 0,
        round1_reply: str = "稳",
        round2_reply: str = "稳",
        round3_reply: str = "稳",
        last_action_round_sent: int = 0,
        last_tribulation_command_at: float = 0,
        last_progress_at: float = 0,
        last_progress_fingerprint: str = "",
        last_stable_sent_at: float = 0,
        last_settlement_text: str = "",
        last_settlement_at: float = 0,
        previous_settlement_text: str = "",
        previous_settlement_at: float = 0,
        last_error: str = "",
        retry_count: int = 0,
    ) -> dict:
        now = time.time()
        payload = {
            "profile_id": int(profile_id),
            "chat_id": int(chat_id),
            "thread_id": thread_id,
            "chat_type": chat_type or "group",
            "bot_username": bot_username or "",
            "enabled": 1 if enabled else 0,
            "run_id": str(run_id or "")[:64],
            "workflow_state": str(workflow_state or ""),
            "next_run_at": float(next_run_at or 0),
            "step_deadline_at": float(step_deadline_at or 0),
            "last_run_at": float(last_run_at or 0),
            "matched_bot_id": int(matched_bot_id or 0),
            "anchor_command_msg_id": int(anchor_command_msg_id or 0),
            "anchor_bot_msg_id": int(anchor_bot_msg_id or 0),
            "tribulation_command_msg_id": int(tribulation_command_msg_id or 0),
            "tribulation_msg_id": int(tribulation_msg_id or 0),
            "panel_reply_msg_id": int(panel_reply_msg_id or 0),
            "round1_reply": str(round1_reply or "稳")[:10],
            "round2_reply": str(round2_reply or "稳")[:10],
            "round3_reply": str(round3_reply or "稳")[:10],
            "last_action_round_sent": max(int(last_action_round_sent or 0), 0),
            "last_tribulation_command_at": float(last_tribulation_command_at or 0),
            "last_progress_at": float(last_progress_at or 0),
            "last_progress_fingerprint": str(last_progress_fingerprint or "")[:1000],
            "last_stable_sent_at": float(last_stable_sent_at or 0),
            "last_settlement_text": str(last_settlement_text or "")[:4000],
            "last_settlement_at": float(last_settlement_at or 0),
            "previous_settlement_text": str(previous_settlement_text or "")[:4000],
            "previous_settlement_at": float(previous_settlement_at or 0),
            "last_error": str(last_error or "")[:1000],
            "retry_count": max(int(retry_count or 0), 0),
            "created_at": now,
            "updated_at": now,
        }
        with self.connect() as conn:
            self._ensure_columns(
                conn,
                "companion_heart_tribulation_tasks",
                {
                    "tribulation_command_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "panel_reply_msg_id": "INTEGER NOT NULL DEFAULT 0",
                    "round1_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "round2_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "round3_reply": "TEXT NOT NULL DEFAULT '稳'",
                    "last_action_round_sent": "INTEGER NOT NULL DEFAULT 0",
                    "last_tribulation_command_at": "REAL NOT NULL DEFAULT 0",
                    "last_progress_at": "REAL NOT NULL DEFAULT 0",
                    "retry_count": "INTEGER NOT NULL DEFAULT 0",
                },
            )
            existing_row = conn.execute(
                """
                SELECT * FROM companion_heart_tribulation_tasks
                WHERE profile_id=? AND chat_id=?
                  AND ((thread_id IS NULL AND ? IS NULL) OR thread_id=?)
                ORDER BY updated_at DESC, id DESC LIMIT 1
                """,
                (
                    int(profile_id),
                    int(chat_id),
                    int(thread_id) if thread_id is not None else None,
                    int(thread_id) if thread_id is not None else 0,
                ),
            ).fetchone()
            if existing_row:
                existing_payload = dict(existing_row)
                payload["bot_username"] = str(existing_payload.get("bot_username") or payload["bot_username"] or "")
                preserved_fields = {
                    "last_settlement_text",
                    "last_settlement_at",
                    "previous_settlement_text",
                    "previous_settlement_at",
                }
                for field_name in preserved_fields:
                    if field_name not in payload or payload[field_name] in {"", 0, 0.0}:
                        payload[field_name] = existing_payload.get(field_name) or payload[field_name]
                updates = {key: value for key, value in payload.items() if key not in {"profile_id", "chat_id", "created_at"}}
                assignments = ", ".join(f"{key}=?" for key in updates)
                conn.execute(
                    f"UPDATE companion_heart_tribulation_tasks SET {assignments} WHERE id=?",
                    [*updates.values(), int(existing_payload.get("id") or 0)],
                )
            else:
                columns = list(payload.keys())
                placeholders = ", ".join("?" for _ in columns)
                values = [payload[column] for column in columns]
                conn.execute(
                    f"""
                    INSERT INTO companion_heart_tribulation_tasks (
                        {", ".join(columns)}
                    ) VALUES ({placeholders})
                    """,
                    values,
                )
        return self.get_companion_heart_tribulation_task(
            int(profile_id), int(chat_id), thread_id=thread_id, bot_username=bot_username
        ) or {}

    def update_companion_heart_tribulation_task(
        self, task_id: int, **fields
    ) -> Optional[dict]:
        if not fields:
            with self.connect() as conn:
                row = conn.execute(
                    "SELECT * FROM companion_heart_tribulation_tasks WHERE id=?",
                    (int(task_id),),
                ).fetchone()
            return dict(row) if row else None
        updates = {**fields, "updated_at": time.time()}
        assignments = ", ".join(f"{key}=?" for key in updates)
        values = list(updates.values()) + [int(task_id)]
        with self.connect() as conn:
            conn.execute(
                f"UPDATE companion_heart_tribulation_tasks SET {assignments} WHERE id=?",
                values,
            )
            row = conn.execute(
                "SELECT * FROM companion_heart_tribulation_tasks WHERE id=?",
                (int(task_id),),
            ).fetchone()
        return dict(row) if row else None

    def disable_companion_heart_tribulation_task(
        self,
        profile_id: int,
        chat_id: int,
        *,
        thread_id: Optional[int] = None,
        bot_username: str = "",
        last_error: str = "",
    ) -> Optional[dict]:
        task = self.get_companion_heart_tribulation_task(
            profile_id,
            chat_id,
            thread_id=thread_id,
            bot_username=bot_username,
        )
        if not task:
            return None
        return self.update_companion_heart_tribulation_task(
            int(task["id"]),
            enabled=0,
            workflow_state="",
            run_id="",
            next_run_at=0,
            step_deadline_at=0,
            matched_bot_id=0,
            tribulation_command_msg_id=0,
            tribulation_msg_id=0,
            panel_reply_msg_id=0,
            last_action_round_sent=0,
            last_tribulation_command_at=0,
            last_progress_at=0,
            last_progress_fingerprint="",
            retry_count=0,
            last_error=(last_error or "")[:1000],
        )

    def append_companion_heart_tribulation_log(
        self,
        *,
        profile_id: int,
        chat_id: int,
        thread_id: Optional[int] = None,
        task_id: int = 0,
        run_id: str = "",
        step: str = "",
        event_type: str = "",
        message_id: int = 0,
        reply_to_msg_id: int = 0,
        sender_id: int = 0,
        sender_username: str = "",
        text: str = "",
        detail: Optional[dict] = None,
    ) -> int:
        now = time.time()
        detail_json = "{}"
        try:
            detail_json = json.dumps(detail or {}, ensure_ascii=False)[:4000]
        except Exception:
            detail_json = "{}"
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO companion_heart_tribulation_logs (
                    profile_id, chat_id, thread_id, task_id, run_id, step,
                    event_type, message_id, reply_to_msg_id, sender_id, sender_username,
                    text, detail_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(profile_id),
                    int(chat_id),
                    int(thread_id) if thread_id is not None else None,
                    int(task_id or 0),
                    str(run_id or "")[:64],
                    str(step or "")[:80],
                    str(event_type or "")[:80],
                    int(message_id or 0),
                    int(reply_to_msg_id or 0),
                    int(sender_id or 0),
                    str(sender_username or "")[:255],
                    str(text or "")[:4000],
                    detail_json,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def list_companion_heart_tribulation_logs(
        self,
        *,
        task_id: int = 0,
        profile_id: Optional[int] = None,
        run_id: str = "",
        limit: int = 200,
    ) -> list[dict]:
        safe_limit = max(1, min(int(limit or 200), 1000))
        query = "SELECT * FROM companion_heart_tribulation_logs WHERE 1=1"
        params: list[object] = []
        if int(task_id or 0) > 0:
            query += " AND task_id=?"
            params.append(int(task_id))
        if profile_id is not None:
            query += " AND profile_id=?"
            params.append(int(profile_id))
        normalized_run_id = str(run_id or "").strip()
        if normalized_run_id:
            query += " AND run_id=?"
            params.append(normalized_run_id)
        query += " ORDER BY created_at ASC, id ASC LIMIT ?"
        params.append(safe_limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def append_rift_execution_log(
        self,
        *,
        profile_id: int,
        chat_id: int,
        thread_id: Optional[int] = None,
        step: str = "",
        event_type: str = "",
        rift_state: str = "",
        retry_count: int = 0,
        message_id: int = 0,
        reply_to_msg_id: int = 0,
        sender_id: int = 0,
        sender_username: str = "",
        text: str = "",
        detail: Optional[dict] = None,
    ) -> int:
        now = time.time()
        detail_json = "{}"
        try:
            detail_json = json.dumps(detail or {}, ensure_ascii=False)[:4000]
        except Exception:
            detail_json = "{}"
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO rift_execution_logs (
                    profile_id, chat_id, thread_id, step, event_type, rift_state,
                    retry_count, message_id, reply_to_msg_id, sender_id, sender_username,
                    text, detail_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(profile_id),
                    int(chat_id),
                    int(thread_id) if thread_id is not None else None,
                    str(step or "")[:100],
                    str(event_type or "")[:100],
                    str(rift_state or "")[:1000],
                    max(int(retry_count or 0), 0),
                    int(message_id or 0),
                    int(reply_to_msg_id or 0),
                    int(sender_id or 0),
                    str(sender_username or "")[:255],
                    str(text or "")[:4000],
                    detail_json,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def list_rift_execution_logs(
        self,
        *,
        profile_id: int,
        chat_id: Optional[int] = None,
        limit: int = 100,
    ) -> list[dict]:
        safe_limit = max(1, min(int(limit or 100), 500))
        query = "SELECT * FROM rift_execution_logs WHERE profile_id=?"
        params: list[object] = [int(profile_id)]
        if chat_id is not None:
            query += " AND chat_id=?"
            params.append(int(chat_id))
        query += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(safe_limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def upsert_bound_message(
        self,
        profile_id: Optional[int],
        chat_id: int,
        thread_id: Optional[int],
        message_id: int,
        reply_to_msg_id: Optional[int],
        sender_id: Optional[int],
        sender_username: str,
        direction: str,
        is_bot: bool,
        text: str,
    ) -> None:
        self.maybe_cleanup_bound_messages()
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO bound_messages (
                    profile_id, chat_id, thread_id, message_id, reply_to_msg_id,
                    sender_id, sender_username, direction, is_bot, text,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id, chat_id, message_id) DO UPDATE SET
                    thread_id=COALESCE(excluded.thread_id, bound_messages.thread_id),
                    reply_to_msg_id=COALESCE(excluded.reply_to_msg_id, bound_messages.reply_to_msg_id),
                    sender_id=COALESCE(excluded.sender_id, bound_messages.sender_id),
                    sender_username=COALESCE(NULLIF(excluded.sender_username, ''), bound_messages.sender_username),
                    direction=excluded.direction,
                    is_bot=excluded.is_bot,
                    text=excluded.text,
                    updated_at=excluded.updated_at
                """,
                (
                    profile_id,
                    chat_id,
                    thread_id,
                    message_id,
                    reply_to_msg_id,
                    sender_id,
                    sender_username or "",
                    direction or "",
                    1 if is_bot else 0,
                    text or "",
                    now,
                    now,
                ),
            )

    def enqueue_outgoing_command(
        self,
        profile_id: Optional[int],
        chat_id: int,
        text: str,
        thread_id: Optional[int] = None,
        reply_to_msg_id: Optional[int] = None,
        chat_type: str = "group",
        bot_username: str = "",
        delay_seconds: int = 0,
    ) -> int:
        now = time.time()
        scheduled_at = now + max(int(delay_seconds or 0), 0)
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO outgoing_commands (
                    profile_id, chat_id, thread_id, reply_to_msg_id, chat_type, bot_username,
                    text, status, error_text, scheduled_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', '', ?, ?, ?)
                """,
                (
                    profile_id,
                    int(chat_id),
                    thread_id,
                    int(reply_to_msg_id) if reply_to_msg_id is not None else None,
                    chat_type or "group",
                    bot_username or "",
                    text or "",
                    scheduled_at,
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def claim_next_outgoing_command(self, profile_id: Optional[int]) -> Optional[dict]:
        now = time.time()
        resolved_profile_id = int(profile_id) if profile_id is not None else None
        with self.connect() as conn:
            query = """
                SELECT * FROM outgoing_commands
                WHERE status='pending' AND (scheduled_at IS NULL OR scheduled_at<=?)
            """
            params = [now]
            if resolved_profile_id is None:
                query += " AND profile_id IS NULL"
            else:
                query += " AND profile_id=?"
                params.append(resolved_profile_id)
            query += " ORDER BY scheduled_at ASC, created_at ASC, id ASC LIMIT 1"
            row = conn.execute(query, params).fetchone()
            if not row:
                return None
            updated = conn.execute(
                """
                UPDATE outgoing_commands
                SET status='sending', updated_at=?, error_text=''
                WHERE id=? AND status='pending'
                """,
                (now, row["id"]),
            )
            if updated.rowcount != 1:
                return None
            claimed = conn.execute(
                "SELECT * FROM outgoing_commands WHERE id=?",
                (row["id"],),
            ).fetchone()
        return dict(claimed) if claimed else None

    def mark_outgoing_command_sent(self, command_id: int) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE outgoing_commands
                SET status='sent', error_text='', updated_at=?
                WHERE id=?
                """,
                (now, int(command_id)),
            )

    def mark_outgoing_command_failed(self, command_id: int, error_text: str) -> None:
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE outgoing_commands
                SET status='failed', error_text=?, updated_at=?
                WHERE id=?
                """,
                ((error_text or "")[:1000], now, int(command_id)),
            )

    def get_outgoing_command(self, command_id: int) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM outgoing_commands WHERE id=?",
                (int(command_id),),
            ).fetchone()
        return dict(row) if row else None

    def get_latest_outgoing_command(
        self,
        chat_id: int,
        profile_id: Optional[int] = None,
        text: str = "",
        thread_id: Optional[int] = None,
    ) -> Optional[dict]:
        query = "SELECT * FROM outgoing_commands WHERE chat_id=?"
        params = [int(chat_id)]
        if profile_id is not None:
            query += " AND profile_id=?"
            params.append(int(profile_id))
        normalized_text = str(text or "").strip()
        if normalized_text:
            query += " AND text=?"
            params.append(normalized_text)
        if thread_id is None:
            query += " AND thread_id IS NULL"
        else:
            query += " AND thread_id=?"
            params.append(int(thread_id))
        query += " ORDER BY updated_at DESC, created_at DESC, id DESC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def cancel_pending_outgoing_commands(
        self,
        profile_id: Optional[int],
        chat_id: int,
        text: str = "",
        *,
        thread_id: Optional[int] = None,
        require_exact_thread: bool = False,
    ) -> int:
        now = time.time()
        query = "UPDATE outgoing_commands SET status='failed', error_text=?, updated_at=? WHERE chat_id=? AND status IN ('pending', 'sending')"
        params = ["Cancelled by user", now, int(chat_id)]
        if profile_id is not None:
            query += " AND profile_id=?"
            params.append(int(profile_id))
        normalized_text = str(text or "").strip()
        if normalized_text:
            query += " AND text=?"
            params.append(normalized_text)
        if require_exact_thread:
            if thread_id is None:
                query += " AND thread_id IS NULL"
            else:
                query += " AND thread_id=?"
                params.append(int(thread_id))
        with self.connect() as conn:
            cursor = conn.execute(query, params)
        return int(cursor.rowcount or 0)

    def get_bound_message(
        self, chat_id: int, message_id: int, profile_id: Optional[int] = None
    ) -> Optional[dict]:
        with self.connect() as conn:
            if profile_id is not None:
                row = conn.execute(
                    "SELECT * FROM bound_messages WHERE chat_id=? AND message_id=? AND profile_id=? ORDER BY id DESC LIMIT 1",
                    (chat_id, message_id, int(profile_id)),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM bound_messages WHERE chat_id=? AND message_id=? ORDER BY id DESC LIMIT 1",
                    (chat_id, message_id),
                ).fetchone()
        return dict(row) if row else None

    def is_known_bot_sender(
        self,
        chat_id: int,
        sender_id: Optional[int],
        bot_username: str = "",
        profile_id: Optional[int] = None,
    ) -> bool:
        normalized_sender_id = int(sender_id or 0)
        if not normalized_sender_id:
            return False
        if profile_id is not None:
            bot_ids = self.get_chat_binding_bot_ids(profile_id, chat_id)
            if normalized_sender_id in bot_ids:
                return True
        normalized_bot = str(bot_username or "").strip().lower().lstrip("@")
        if normalized_bot:
            query = (
                "SELECT 1 FROM bound_messages WHERE chat_id=? AND sender_id=? AND (is_bot=1 OR lower(sender_username)=?) ORDER BY updated_at DESC, id DESC LIMIT 1"
            )
            with self.connect() as conn:
                row = conn.execute(query, (int(chat_id), normalized_sender_id, normalized_bot)).fetchone()
            return row is not None
        return False

    def delete_bound_messages(
        self,
        chat_id: int,
        message_ids: list[int],
        profile_id: Optional[int] = None,
    ) -> int:
        normalized_ids = sorted(
            {int(message_id) for message_id in message_ids if message_id}
        )
        if not normalized_ids:
            return 0
        placeholders = ", ".join("?" for _ in normalized_ids)
        params = [int(chat_id), *normalized_ids]
        profile_clause = ""
        if profile_id is not None:
            profile_clause = " AND profile_id=?"
            params.append(int(profile_id))
        with self.connect() as conn:
            cursor = conn.execute(
                f"DELETE FROM bound_messages WHERE chat_id=? AND message_id IN ({placeholders}){profile_clause}",
                params,
            )
        return int(cursor.rowcount or 0)

    def list_bound_messages(
        self,
        profile_id: Optional[int] = None,
        chat_id: Optional[int] = None,
        search_query: str = "",
        limit: int = 200,
    ) -> list[dict]:
        query = "SELECT * FROM bound_messages WHERE 1=1"
        params = []
        if profile_id is not None:
            query += " AND profile_id=?"
            params.append(profile_id)
        if chat_id is not None:
            query += " AND chat_id=?"
            params.append(chat_id)
        normalized_query = str(search_query or "").strip()
        if normalized_query:
            query += " AND (text LIKE ? OR sender_username LIKE ?)"
            like_value = f"%{normalized_query}%"
            params.extend([like_value, like_value])
        query += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(int(limit))
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_bound_message_context(
        self,
        chat_id: int,
        message_id: int,
        profile_id: Optional[int] = None,
        radius: int = 12,
    ) -> list[dict]:
        chat_id = int(chat_id)
        message_id = int(message_id)
        safe_radius = max(1, min(int(radius or 12), 50))
        profile_clause = ""
        profile_params = []
        if profile_id is not None:
            profile_clause = " AND profile_id=?"
            profile_params.append(int(profile_id))
        with self.connect() as conn:
            before_rows = conn.execute(
                f"""
                SELECT * FROM bound_messages
                WHERE chat_id=? AND message_id < ?{profile_clause}
                ORDER BY message_id DESC, id DESC
                LIMIT ?
                """,
                [chat_id, message_id, *profile_params, safe_radius],
            ).fetchall()
            focus_row = conn.execute(
                f"""
                SELECT * FROM bound_messages
                WHERE chat_id=? AND message_id=?{profile_clause}
                ORDER BY id DESC
                LIMIT 1
                """,
                [chat_id, message_id, *profile_params],
            ).fetchone()
            after_rows = conn.execute(
                f"""
                SELECT * FROM bound_messages
                WHERE chat_id=? AND message_id > ?{profile_clause}
                ORDER BY message_id ASC, id ASC
                LIMIT ?
                """,
                [chat_id, message_id, *profile_params, safe_radius],
            ).fetchall()
        rows = list(reversed(before_rows))
        if focus_row:
            rows.append(focus_row)
        rows.extend(after_rows)
        return [dict(row) for row in rows]

    def get_latest_outgoing_command_message(
        self,
        profile_id: Optional[int],
        chat_id: int,
        thread_id: Optional[int] = None,
    ) -> Optional[dict]:
        query = """
            SELECT * FROM bound_messages
            WHERE chat_id=? AND direction='outgoing' AND text LIKE '.%'
        """
        params = [int(chat_id)]
        if profile_id is not None:
            query += " AND profile_id=?"
            params.append(int(profile_id))
        if thread_id:
            query += " AND (thread_id=? OR reply_to_msg_id=?)"
            params.extend([int(thread_id), int(thread_id)])
        query += " ORDER BY created_at DESC, id DESC LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def get_latest_bot_reply_for_command(
        self,
        chat_id: int,
        command_text: str,
        profile_id: Optional[int] = None,
        thread_id: Optional[int] = None,
        sender_id: Optional[int] = None,
        sender_username: str = "",
    ) -> Optional[dict]:
        normalized_command = str(command_text or "").strip()
        if not normalized_command:
            return None

        normalized_sender_username = (
            str(sender_username or "").strip().lower().lstrip("@")
        )

        with self.connect() as conn:
            command_query = """
                SELECT * FROM bound_messages
                WHERE chat_id=? AND is_bot=0 AND text=?
            """
            command_params = [int(chat_id), normalized_command]
            if profile_id is not None:
                command_query += " AND profile_id=?"
                command_params.append(int(profile_id))
            if thread_id:
                command_query += " AND (thread_id=? OR reply_to_msg_id=?)"
                command_params.extend([int(thread_id), int(thread_id)])
            if sender_id is not None:
                command_query += " AND sender_id=?"
                command_params.append(int(sender_id))
            elif normalized_sender_username:
                command_query += " AND LOWER(COALESCE(sender_username, ''))=?"
                command_params.append(normalized_sender_username)
            command_query += " ORDER BY created_at DESC, id DESC LIMIT 20"
            command_rows = conn.execute(command_query, command_params).fetchall()
            for command_row in command_rows:
                reply_query = """
                    SELECT * FROM bound_messages
                    WHERE chat_id=? AND is_bot=1 AND reply_to_msg_id=?
                """
                reply_params = [int(chat_id), int(command_row["message_id"])]
                if profile_id is not None:
                    reply_query += " AND profile_id=?"
                    reply_params.append(int(profile_id))
                reply_query += " ORDER BY updated_at DESC, created_at DESC, id DESC LIMIT 1"
                reply_row = conn.execute(reply_query, reply_params).fetchone()
                if reply_row:
                    return dict(reply_row)
        return None

    def get_latest_bot_reply_message(
        self, chat_id: int, reply_to_msg_id: int, profile_id: Optional[int] = None
    ) -> Optional[dict]:
        with self.connect() as conn:
            if profile_id is not None:
                row = conn.execute(
                    """
                    SELECT * FROM bound_messages
                    WHERE chat_id=? AND is_bot=1 AND reply_to_msg_id=? AND profile_id=?
                    ORDER BY updated_at DESC, created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (int(chat_id), int(reply_to_msg_id), int(profile_id)),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM bound_messages
                    WHERE chat_id=? AND is_bot=1 AND reply_to_msg_id=?
                    ORDER BY updated_at DESC, created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (int(chat_id), int(reply_to_msg_id)),
                ).fetchone()
        return dict(row) if row else None

    def get_recent_companion_heart_tribulation_message(
        self,
        chat_id: int,
        *,
        profile_id: Optional[int] = None,
        thread_id: Optional[int] = None,
        bot_username: str = "",
        anchor_bot_msg_id: int = 0,
        since_ts: float = 0,
        limit: int = 20,
    ) -> Optional[dict]:
        normalized_bot_username = str(bot_username or "").strip().lower().lstrip("@")
        safe_limit = max(1, min(int(limit or 20), 100))
        query = """
            SELECT * FROM bound_messages
            WHERE chat_id=? AND is_bot=1 AND text LIKE ?
        """
        params = [int(chat_id), "%坠魔心劫%"]
        if profile_id is not None:
            query += " AND profile_id=?"
            params.append(int(profile_id))
        if thread_id is not None:
            query += " AND (thread_id=? OR reply_to_msg_id=? OR message_id=?)"
            params.extend([int(thread_id), int(thread_id), int(thread_id)])
        if normalized_bot_username:
            query += " AND LOWER(COALESCE(sender_username, ''))=?"
            params.append(normalized_bot_username)
        if anchor_bot_msg_id > 0:
            query += " AND message_id>=?"
            params.append(int(anchor_bot_msg_id))
        if since_ts > 0:
            query += " AND created_at>=?"
            params.append(float(since_ts))
        query += " ORDER BY updated_at DESC, created_at DESC, id DESC LIMIT ?"
        params.append(safe_limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        for row in rows:
            payload = dict(row)
            text = str(payload.get("text") or "").strip()
            if "【坠魔心劫·" in text:
                return payload
        return None

    def get_latest_stock_market_history_observed_at(self) -> float:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT MAX(observed_at) AS latest_observed_at FROM stock_market_history"
            ).fetchone()
        return float((row["latest_observed_at"] if row else 0) or 0)

    def upsert_stock_player_reply(
        self,
        profile_id: int,
        chat_id: int,
        command_text: str,
        reply_text: str,
        *,
        thread_id: Optional[int] = None,
        source_message_id: int = 0,
        reply_to_msg_id: int = 0,
    ) -> None:
        normalized_command = str(command_text or "").strip()
        normalized_reply = str(reply_text or "").strip()
        if not profile_id or not normalized_command or not normalized_reply:
            return
        now = time.time()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO stock_player_replies (
                    profile_id, chat_id, thread_id, command_text, reply_text,
                    source_message_id, reply_to_msg_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id, command_text) DO UPDATE SET
                    chat_id=excluded.chat_id,
                    thread_id=excluded.thread_id,
                    reply_text=excluded.reply_text,
                    source_message_id=excluded.source_message_id,
                    reply_to_msg_id=excluded.reply_to_msg_id,
                    updated_at=excluded.updated_at
                """,
                (
                    int(profile_id),
                    int(chat_id or 0),
                    int(thread_id) if thread_id is not None else None,
                    normalized_command,
                    normalized_reply,
                    int(source_message_id or 0),
                    int(reply_to_msg_id or 0),
                    now,
                    now,
                ),
            )

    def get_stock_player_reply(
        self, profile_id: int, command_text: str
    ) -> Optional[dict]:
        normalized_command = str(command_text or "").strip()
        if not profile_id or not normalized_command:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM stock_player_replies
                WHERE profile_id=? AND command_text=?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (int(profile_id), normalized_command),
            ).fetchone()
        return dict(row) if row else None

    def list_stock_source_messages(
        self, limit: int = 5000, since_created_at: Optional[float] = None
    ) -> list[dict]:
        safe_limit = max(int(limit or 0), 1)
        query = """
            SELECT * FROM bound_messages
            WHERE is_bot=1
              AND (
                text LIKE '%IDX_%'
                OR text LIKE '%股市%'
                OR text LIKE '%大盘%'
                OR text LIKE '%个股%'
                OR text LIKE '%天道股市%'
                OR text LIKE '%虚实交汇%'
              )
        """
        params = []
        if since_created_at is not None:
            query += " AND created_at>=?"
            params.append(float(since_created_at))
        query += " ORDER BY created_at ASC, id ASC LIMIT ?"
        params.append(safe_limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def upsert_stock_market_history(
        self,
        profile_id: Optional[int],
        chat_id: int,
        message_id: int,
        stock_code: str,
        **fields,
    ) -> None:
        normalized_code = str(stock_code or "").strip().upper()
        if not normalized_code:
            return
        now = time.time()
        observed_at = float(fields.get("observed_at") or 0) or now
        payload = {
            "stock_name": str(fields.get("stock_name") or "").strip(),
            "current_price": float(fields.get("current_price") or 0),
            "change_amount": float(fields.get("change_amount") or 0),
            "change_percent": float(fields.get("change_percent") or 0),
            "sector": str(fields.get("sector") or "").strip(),
            "trend": str(fields.get("trend") or "").strip(),
            "heat": str(fields.get("heat") or "").strip(),
            "crowding": str(fields.get("crowding") or "").strip(),
            "volatility": str(fields.get("volatility") or "").strip(),
            "liquidity": str(fields.get("liquidity") or "").strip(),
            "open_price": float(fields.get("open_price") or 0),
            "prev_close": float(fields.get("prev_close") or 0),
            "high_price": float(fields.get("high_price") or 0),
            "low_price": float(fields.get("low_price") or 0),
            "volume": float(fields.get("volume") or 0),
            "turnover": float(fields.get("turnover") or 0),
            "pattern": str(fields.get("pattern") or "").strip(),
            "volume_trend": str(fields.get("volume_trend") or "").strip(),
            "position_text": str(fields.get("position_text") or "").strip(),
            "score": int(fields.get("score") or 0),
            "strategy": str(fields.get("strategy") or "").strip(),
            "direction_emoji": str(fields.get("direction_emoji") or "").strip(),
            "raw_text": str(fields.get("raw_text") or "").strip(),
            "observed_at": observed_at,
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO stock_market_history (
                    profile_id, chat_id, message_id, stock_code, stock_name,
                    current_price, change_amount, change_percent, sector, trend,
                    heat, crowding, volatility, liquidity, open_price, prev_close,
                    high_price, low_price, volume, turnover, pattern, volume_trend,
                    position_text, score, strategy, direction_emoji, raw_text,
                    observed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, message_id, stock_code) DO UPDATE SET
                    profile_id=excluded.profile_id,
                    stock_name=excluded.stock_name,
                    current_price=excluded.current_price,
                    change_amount=excluded.change_amount,
                    change_percent=excluded.change_percent,
                    sector=excluded.sector,
                    trend=excluded.trend,
                    heat=excluded.heat,
                    crowding=excluded.crowding,
                    volatility=excluded.volatility,
                    liquidity=excluded.liquidity,
                    open_price=excluded.open_price,
                    prev_close=excluded.prev_close,
                    high_price=excluded.high_price,
                    low_price=excluded.low_price,
                    volume=excluded.volume,
                    turnover=excluded.turnover,
                    pattern=excluded.pattern,
                    volume_trend=excluded.volume_trend,
                    position_text=excluded.position_text,
                    score=excluded.score,
                    strategy=excluded.strategy,
                    direction_emoji=excluded.direction_emoji,
                    raw_text=excluded.raw_text,
                    observed_at=excluded.observed_at,
                    updated_at=excluded.updated_at
                """,
                (
                    int(profile_id) if profile_id else None,
                    int(chat_id),
                    int(message_id),
                    normalized_code,
                    payload["stock_name"],
                    payload["current_price"],
                    payload["change_amount"],
                    payload["change_percent"],
                    payload["sector"],
                    payload["trend"],
                    payload["heat"],
                    payload["crowding"],
                    payload["volatility"],
                    payload["liquidity"],
                    payload["open_price"],
                    payload["prev_close"],
                    payload["high_price"],
                    payload["low_price"],
                    payload["volume"],
                    payload["turnover"],
                    payload["pattern"],
                    payload["volume_trend"],
                    payload["position_text"],
                    payload["score"],
                    payload["strategy"],
                    payload["direction_emoji"],
                    payload["raw_text"],
                    payload["observed_at"],
                    now,
                    now,
                ),
            )

    def list_stock_market_history(
        self,
        stock_code: str,
        limit: int = 60,
        since_observed_at: Optional[float] = None,
    ) -> list[dict]:
        normalized_code = str(stock_code or "").strip().upper()
        if not normalized_code:
            return []
        safe_limit = max(int(limit or 0), 1)
        query = """
            SELECT * FROM stock_market_history
            WHERE stock_code=?
        """
        params = [normalized_code]
        if since_observed_at is not None:
            query += " AND observed_at>=?"
            params.append(float(since_observed_at))
        query += " ORDER BY observed_at DESC, id DESC LIMIT ?"
        params.append(safe_limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in reversed(rows)]

    def summarize_stock_market_history(self, stock_codes: list[str]) -> dict[str, dict]:
        normalized_codes = [
            str(stock_code or "").strip().upper()
            for stock_code in stock_codes
            if stock_code
        ]
        if not normalized_codes:
            return {}
        placeholders = ", ".join("?" for _ in normalized_codes)
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    stock_code,
                    COUNT(*) AS history_count,
                    MAX(observed_at) AS latest_observed_at
                FROM stock_market_history
                WHERE stock_code IN ({placeholders})
                GROUP BY stock_code
                """,
                normalized_codes,
            ).fetchall()
        return {
            str(row["stock_code"] or ""): {
                "history_count": int(row["history_count"] or 0),
                "latest_observed_at": float(row["latest_observed_at"] or 0),
            }
            for row in rows
        }

    def upsert_stock_market_info(
        self, profile_id: int, stock_code: str, **fields
    ) -> None:
        normalized_code = str(stock_code or "").strip().upper()
        if not normalized_code:
            return
        now = time.time()
        payload = {
            "stock_name": str(fields.get("stock_name") or "").strip(),
            "current_price": float(fields.get("current_price") or 0),
            "change_amount": float(fields.get("change_amount") or 0),
            "change_percent": float(fields.get("change_percent") or 0),
            "sector": str(fields.get("sector") or "").strip(),
            "trend": str(fields.get("trend") or "").strip(),
            "heat": str(fields.get("heat") or "").strip(),
            "crowding": str(fields.get("crowding") or "").strip(),
            "volatility": str(fields.get("volatility") or "").strip(),
            "liquidity": str(fields.get("liquidity") or "").strip(),
            "open_price": float(fields.get("open_price") or 0),
            "prev_close": float(fields.get("prev_close") or 0),
            "high_price": float(fields.get("high_price") or 0),
            "low_price": float(fields.get("low_price") or 0),
            "volume": float(fields.get("volume") or 0),
            "turnover": float(fields.get("turnover") or 0),
            "pattern": str(fields.get("pattern") or "").strip(),
            "volume_trend": str(fields.get("volume_trend") or "").strip(),
            "position_text": str(fields.get("position_text") or "").strip(),
            "score": int(fields.get("score") or 0),
            "strategy": str(fields.get("strategy") or "").strip(),
            "direction_emoji": str(fields.get("direction_emoji") or "").strip(),
            "source_message_id": int(fields.get("source_message_id") or 0),
            "raw_text": str(fields.get("raw_text") or "").strip(),
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO stock_market_info (
                    profile_id, stock_code, stock_name, current_price, change_amount,
                    change_percent, sector, trend, heat, crowding, volatility,
                    liquidity, open_price, prev_close, high_price, low_price,
                    volume, turnover, pattern, volume_trend, position_text, score,
                    strategy, direction_emoji, source_message_id, raw_text,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id, stock_code) DO UPDATE SET
                    stock_name=excluded.stock_name,
                    current_price=excluded.current_price,
                    change_amount=excluded.change_amount,
                    change_percent=excluded.change_percent,
                    sector=excluded.sector,
                    trend=excluded.trend,
                    heat=excluded.heat,
                    crowding=excluded.crowding,
                    volatility=excluded.volatility,
                    liquidity=excluded.liquidity,
                    open_price=excluded.open_price,
                    prev_close=excluded.prev_close,
                    high_price=excluded.high_price,
                    low_price=excluded.low_price,
                    volume=excluded.volume,
                    turnover=excluded.turnover,
                    pattern=excluded.pattern,
                    volume_trend=excluded.volume_trend,
                    position_text=excluded.position_text,
                    score=excluded.score,
                    strategy=excluded.strategy,
                    direction_emoji=excluded.direction_emoji,
                    source_message_id=excluded.source_message_id,
                    raw_text=excluded.raw_text,
                    updated_at=excluded.updated_at
                """,
                (
                    int(profile_id),
                    normalized_code,
                    payload["stock_name"],
                    payload["current_price"],
                    payload["change_amount"],
                    payload["change_percent"],
                    payload["sector"],
                    payload["trend"],
                    payload["heat"],
                    payload["crowding"],
                    payload["volatility"],
                    payload["liquidity"],
                    payload["open_price"],
                    payload["prev_close"],
                    payload["high_price"],
                    payload["low_price"],
                    payload["volume"],
                    payload["turnover"],
                    payload["pattern"],
                    payload["volume_trend"],
                    payload["position_text"],
                    payload["score"],
                    payload["strategy"],
                    payload["direction_emoji"],
                    payload["source_message_id"],
                    payload["raw_text"],
                    now,
                    now,
                ),
            )

    def list_stock_market_info(self, profile_id: int) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM stock_market_info WHERE profile_id=? ORDER BY change_percent DESC, stock_code ASC",
                (int(profile_id),),
            ).fetchall()
        return [dict(row) for row in rows]
