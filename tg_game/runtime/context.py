from dataclasses import dataclass, field
from typing import Dict, Optional

from tg_game.models import ChatBinding, ModuleSetting, PlayerProfile


@dataclass
class EventContext:
    client: object
    event: object
    profile: Optional[PlayerProfile]
    chat_binding: Optional[ChatBinding]
    module_settings: Dict[str, ModuleSetting] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return (getattr(self.event, "raw_text", "") or "").strip()

    @property
    def chat_id(self) -> Optional[int]:
        return getattr(self.event, "chat_id", None)

    @property
    def sender_id(self) -> Optional[int]:
        return getattr(self.event, "sender_id", None)

    @property
    def is_private(self) -> bool:
        return bool(getattr(self.event, "is_private", False))

    @property
    def is_bot_sender(self) -> bool:
        sender = getattr(self.event, "sender", None)
        if sender and getattr(sender, "bot", False):
            return True
        binding_bot_id = self.chat_binding.bot_id if self.chat_binding else None
        if binding_bot_id is not None and str(self.sender_id or "") == str(
            binding_bot_id
        ):
            return True
        binding_bot = (
            (self.chat_binding.bot_username or "").strip().lower().lstrip("@")
            if self.chat_binding
            else ""
        )
        sender_username = (getattr(sender, "username", "") or "").strip().lower()
        return bool(binding_bot and sender_username.lstrip("@") == binding_bot)

    @property
    def thread_id(self) -> Optional[int]:
        message = getattr(self.event, "message", None)
        reply_to = getattr(message, "reply_to", None) if message else None
        for candidate in [
            getattr(reply_to, "reply_to_top_id", None),
            getattr(message, "reply_to_top_id", None) if message else None,
            getattr(reply_to, "top_msg_id", None),
            getattr(message, "top_msg_id", None) if message else None,
        ]:
            if candidate:
                return candidate
        return None

    @property
    def bot_username(self) -> str:
        sender = getattr(self.event, "sender", None)
        username = getattr(sender, "username", "") if sender else ""
        return (username or "").lower()

    @property
    def message_id(self) -> Optional[int]:
        return getattr(self.event, "id", None)

    @property
    def reply_to_msg_id(self) -> Optional[int]:
        message = getattr(self.event, "message", None)
        reply_to = getattr(message, "reply_to", None) if message else None
        return getattr(reply_to, "reply_to_msg_id", None) or getattr(
            reply_to, "reply_to_msg_id", None
        )

    @property
    def is_outgoing(self) -> bool:
        return bool(getattr(self.event, "out", False))

    async def get_reply_message_text(self) -> str:
        if not getattr(self.event, "is_reply", False):
            return ""
        try:
            reply_message = await self.event.get_reply_message()
        except Exception:
            return ""
        return (getattr(reply_message, "raw_text", "") or "").strip()

    def is_profile_owner(self) -> bool:
        binding_user_id = (
            self.chat_binding.telegram_user_id if self.chat_binding else ""
        )
        expected_user_id = binding_user_id or (
            self.profile.telegram_user_id if self.profile else ""
        )
        if not expected_user_id:
            return True
        return str(self.sender_id or "") == str(expected_user_id)

    def get_setting(self, module_key: str) -> Optional[ModuleSetting]:
        return self.module_settings.get(module_key)

    async def reply(self, text: str) -> None:
        await self.event.reply(text)

    async def bot_message_targets_profile(self) -> bool:
        if not self.is_bot_sender:
            return False

        binding_user_id = (
            self.chat_binding.telegram_user_id if self.chat_binding else ""
        )
        expected_user_id = binding_user_id or (
            self.profile.telegram_user_id if self.profile else ""
        )
        if expected_user_id and getattr(self.event, "is_reply", False):
            try:
                reply_message = await self.event.get_reply_message()
            except Exception:
                reply_message = None
            if reply_message and str(getattr(reply_message, "sender_id", "")) == str(
                expected_user_id
            ):
                return True

        try:
            me = await self.client.get_me()
        except Exception:
            me = None
        my_username = (getattr(me, "username", "") or "").lower()
        if my_username and f"@{my_username}" in self.text.lower():
            return True

        return False
