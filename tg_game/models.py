from dataclasses import asdict, dataclass, field
from typing import Optional


@dataclass(frozen=True)
class FeatureModule:
    key: str
    name: str
    summary: str
    status: str
    capabilities: list[str] = field(default_factory=list)
    next_steps: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class PlayerProfile:
    id: int
    name: str
    account_name: str
    game_name: str
    telegram_user_id: str
    telegram_phone: str
    telegram_username: str
    telegram_verified_at: float
    telegram_session_name: str
    notes: str
    display_name: str
    artifact_text: str
    sect_name: str
    sect_leader: str
    sect_position: str
    sect_description: str
    sect_bonus_text: str
    sect_contribution_text: str
    spirit_root: str
    stage_name: str
    cultivation_text: str
    poison_text: str
    kill_count_text: str
    info_updated_at: float
    sect_info_updated_at: float
    is_active: bool
    created_at: float
    updated_at: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ChatBinding:
    id: int
    profile_id: int
    chat_id: int
    thread_id: Optional[int]
    chat_type: str
    bot_username: str
    bot_id: Optional[int]
    telegram_user_id: str
    is_active: bool
    created_at: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ModuleSetting:
    id: int
    profile_id: int
    module_key: str
    enabled: bool
    cooldown_seconds: int
    check_interval_seconds: int
    command_template: str
    notes: str
    updated_at: float

    def to_dict(self) -> dict:
        return asdict(self)
