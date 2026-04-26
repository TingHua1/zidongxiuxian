import logging
from typing import Dict, List

from tg_game.config import get_settings
from tg_game.runtime.context import EventContext
from tg_game.runtime.executors import (
    BaseExecutor,
    FanrenExecutor,
    GeneralGameExecutor,
    SectExecutor,
)
from tg_game.services import module_registry
from tg_game.services.external_sync import is_authorized_profile
from tg_game.services.stock_sync import sync_stock_market_message
from tg_game.storage import Storage


logger = logging.getLogger(__name__)


class Router:
    def __init__(self, storage: Storage, executors: List[BaseExecutor]):
        self.storage = storage
        self.executors = executors

    async def startup(self, client: object) -> None:
        self.storage.init_schema()
        settings = get_settings()
        active_profile = self.storage.get_active_profile()
        if active_profile:
            self.storage.sync_env_chat_binding(
                profile_id=active_profile.id,
                chat_id=settings.bound_chat_id,
                thread_id=settings.bound_thread_id,
                chat_type=settings.bound_chat_type,
                bot_username=settings.bound_bot_username,
                bot_id=settings.bound_bot_id,
                telegram_user_id=active_profile.telegram_user_id,
            )
        for executor in self.executors:
            await executor.startup(client, self.storage)

    async def dispatch(self, client: object, event: object) -> bool:
        context = self._build_context(client, event)
        should_persist_message = is_authorized_profile(self.storage, context.profile)
        if context.chat_binding and context.message_id and should_persist_message:
            existing_message = self.storage.get_bound_message(
                context.chat_id or 0, context.message_id
            )
            sender = getattr(context.event, "sender", None)
            sender_username = (getattr(sender, "username", "") or "") or str(
                (existing_message or {}).get("sender_username") or ""
            )
            is_bot_sender = context.is_bot_sender or bool(
                (existing_message or {}).get("is_bot")
            )
            binding_bot = (
                (context.chat_binding.bot_username or "").strip().lower().lstrip("@")
            )
            if not is_bot_sender:
                is_bot_sender = self.storage.is_known_bot_sender(
                    context.chat_id or 0,
                    context.sender_id,
                    context.chat_binding.bot_username if context.chat_binding else "",
                )
            if (
                not is_bot_sender
                and binding_bot
                and sender_username.strip().lower().lstrip("@") == binding_bot
            ):
                is_bot_sender = True
            self.storage.upsert_bound_message(
                profile_id=context.profile.id if context.profile else None,
                chat_id=context.chat_id or 0,
                thread_id=context.thread_id,
                message_id=context.message_id,
                reply_to_msg_id=context.reply_to_msg_id,
                sender_id=context.sender_id
                or (existing_message or {}).get("sender_id"),
                sender_username=sender_username,
                direction="outgoing" if context.is_outgoing else "incoming",
                is_bot=is_bot_sender,
                text=context.text,
            )
            stored_message = self.storage.get_bound_message(
                context.chat_id or 0, context.message_id
            ) or {
                "profile_id": context.profile.id if context.profile else None,
                "chat_id": context.chat_id or 0,
                "message_id": context.message_id,
                "is_bot": is_bot_sender,
                "text": context.text,
            }
            sync_stock_market_message(
                self.storage,
                stored_message,
            )
        for executor in self.executors:
            try:
                handled = await executor.handle(context, self.storage)
            except Exception as exc:
                logger.exception("Executor %s failed", executor.key)
                continue
            if handled:
                return True
        return False

    def _build_context(self, client: object, event: object) -> EventContext:
        profile = self.storage.get_active_profile()
        chat_binding = None
        module_settings: Dict[str, object] = {}
        if profile:
            self.storage.ensure_module_settings(
                profile.id, module_registry.list_modules()
            )
            module_settings = {
                setting.module_key: setting
                for setting in self.storage.list_module_settings(profile.id)
            }
            chat_id = getattr(event, "chat_id", None)
            if chat_id is not None:
                message = getattr(event, "message", None)
                reply_to = getattr(message, "reply_to", None) if message else None
                reply_to_msg_id = getattr(reply_to, "reply_to_msg_id", None)
                thread_id = None
                for candidate in [
                    getattr(reply_to, "reply_to_top_id", None),
                    getattr(message, "reply_to_top_id", None) if message else None,
                    getattr(reply_to, "top_msg_id", None),
                    getattr(message, "top_msg_id", None) if message else None,
                ]:
                    if candidate:
                        thread_id = candidate
                        break
                chat_binding = self.storage.resolve_chat_binding_for_event(
                    profile.id, chat_id, thread_id, reply_to_msg_id
                )
        return EventContext(
            client=client,
            event=event,
            profile=profile,
            chat_binding=chat_binding,
            module_settings=module_settings,
        )


def build_router(storage: Storage) -> Router:
    return Router(
        storage=storage,
        executors=[FanrenExecutor(), SectExecutor(), GeneralGameExecutor()],
    )
