import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


class Settings(BaseModel):
    app_name: str = "自动修仙"
    app_version: str = "0.1.0"
    debug: bool = os.getenv("TG_GAME_DEBUG", "0") in {
        "1",
        "true",
        "True",
        "yes",
        "on",
    }
    host: str = os.getenv("TG_GAME_HOST", "127.0.0.1")
    port: int = int(os.getenv("TG_GAME_PORT", "8000"))
    database_path: Path = BASE_DIR / "tg_game.db"
    telegram_api_id: str = os.getenv("TELEGRAM_API_ID", "")
    telegram_api_hash: str = os.getenv("TELEGRAM_API_HASH", "")
    telegram_session_name: str = os.getenv("TG_GAME_SESSION_NAME", "tg_game")
    telegram_login_session_name: str = os.getenv(
        "TG_GAME_LOGIN_SESSION_NAME", "tg_game_login"
    )
    bound_chat_id: Optional[int] = (
        int(os.getenv("TG_GAME_BOUND_CHAT_ID", ""))
        if os.getenv("TG_GAME_BOUND_CHAT_ID", "").strip()
        else None
    )
    bound_thread_id: Optional[int] = (
        int(os.getenv("TG_GAME_BOUND_THREAD_ID", ""))
        if os.getenv("TG_GAME_BOUND_THREAD_ID", "").strip()
        else None
    )
    bound_chat_type: str = os.getenv("TG_GAME_BOUND_CHAT_TYPE", "group")
    bound_bot_username: str = os.getenv(
        "TG_GAME_BOUND_BOT_USERNAME", "fanrenxiuxian_bot"
    )
    bound_bot_id: Optional[int] = (
        int(os.getenv("TG_GAME_BOUND_BOT_ID", ""))
        if os.getenv("TG_GAME_BOUND_BOT_ID", "").strip()
        else None
    )
    external_keepalive_seconds: int = int(
        os.getenv("TG_GAME_EXTERNAL_KEEPALIVE_SECONDS", "900")
    )
    external_keepalive_poll_seconds: int = int(
        os.getenv("TG_GAME_EXTERNAL_KEEPALIVE_POLL_SECONDS", "30")
    )
    telegram_log_messages: bool = os.getenv("TG_GAME_LOG_MESSAGES", "0") in {
        "1",
        "true",
        "True",
        "yes",
        "on",
    }
    authorized_user_id: str = os.getenv("AUTHORIZED_USER_ID", "").strip()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
