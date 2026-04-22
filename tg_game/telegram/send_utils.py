import logging
from typing import Optional

from tg_game.services.external_sync import ASC_PROVIDER, is_external_account_expired
from tg_game.storage import Storage


logger = logging.getLogger(__name__)


def _normalize_bot_username(bot_username: str) -> str:
    return str(bot_username or "").strip().lower().lstrip("@")


def _resolve_storage(storage: Optional[Storage], client) -> Optional[Storage]:
    return storage or getattr(client, "_tg_game_storage", None)


def _resolve_profile_id(
    storage: Optional[Storage], profile_id: Optional[int]
) -> Optional[int]:
    if profile_id:
        return int(profile_id)
    if not storage:
        return None
    active_profile = storage.get_active_profile()
    return int(getattr(active_profile, "id", 0) or 0) or None


def _ensure_external_session_available(
    storage: Optional[Storage], profile_id: Optional[int]
) -> None:
    if not storage:
        return
    resolved_profile_id = _resolve_profile_id(storage, profile_id)
    if not resolved_profile_id:
        return
    external_account = storage.get_external_account(resolved_profile_id, ASC_PROVIDER)
    if is_external_account_expired(external_account):
        raise RuntimeError("天机阁会话已失效，请先前往 /login 重新导入 Cookie")


def _resolve_binding_thread_id(
    storage: Optional[Storage],
    profile_id: Optional[int],
    chat_id: int,
    bot_username: str = "",
    *,
    exclude_thread_id: Optional[int] = None,
) -> Optional[int]:
    if not storage or not chat_id:
        return None
    resolved_profile_id = int(profile_id) if profile_id else 0
    if not resolved_profile_id:
        active_profile = storage.get_active_profile()
        resolved_profile_id = int(getattr(active_profile, "id", 0) or 0)
    if not resolved_profile_id:
        return None
    normalized_bot = _normalize_bot_username(bot_username)
    for binding in storage.list_chat_bindings(resolved_profile_id):
        binding_chat_id = int(getattr(binding, "chat_id", 0) or 0)
        binding_thread_id = getattr(binding, "thread_id", None)
        binding_bot = _normalize_bot_username(getattr(binding, "bot_username", ""))
        if binding_chat_id != int(chat_id) or not binding_thread_id:
            continue
        if normalized_bot and binding_bot and binding_bot != normalized_bot:
            continue
        if exclude_thread_id and int(binding_thread_id) == int(exclude_thread_id):
            continue
        return int(binding_thread_id)
    return None


async def send_message_with_thread_fallback(
    client,
    chat_id: int,
    text: str,
    *,
    thread_id: Optional[int] = None,
    storage: Optional[Storage] = None,
    profile_id: Optional[int] = None,
    bot_username: str = "",
    log_prefix: str = "Telegram",
):
    resolved_storage = _resolve_storage(storage, client)
    _ensure_external_session_available(resolved_storage, profile_id)
    attempted_thread_id = int(thread_id) if thread_id else None
    if attempted_thread_id is None:
        attempted_thread_id = _resolve_binding_thread_id(
            resolved_storage, profile_id, chat_id, bot_username
        )
    alternate_thread_id = None
    topic_closed_error = None

    if attempted_thread_id:
        try:
            return await client.send_message(
                chat_id, text, reply_to=attempted_thread_id
            )
        except Exception as exc:
            if "TOPIC_CLOSED" not in str(exc):
                raise
            topic_closed_error = exc
            logger.warning(
                "%s send hit TOPIC_CLOSED chat=%s thread=%s command=%s",
                log_prefix,
                chat_id,
                attempted_thread_id,
                text,
            )
            alternate_thread_id = _resolve_binding_thread_id(
                resolved_storage,
                profile_id,
                chat_id,
                bot_username,
                exclude_thread_id=attempted_thread_id,
            )
            if alternate_thread_id:
                try:
                    logger.info(
                        "%s retrying with alternate thread chat=%s thread=%s command=%s",
                        log_prefix,
                        chat_id,
                        alternate_thread_id,
                        text,
                    )
                    return await client.send_message(
                        chat_id, text, reply_to=alternate_thread_id
                    )
                except Exception as retry_exc:
                    if "TOPIC_CLOSED" not in str(retry_exc):
                        raise
                    topic_closed_error = retry_exc
                    logger.warning(
                        "%s alternate thread also TOPIC_CLOSED chat=%s thread=%s command=%s",
                        log_prefix,
                        chat_id,
                        alternate_thread_id,
                        text,
                    )

    if topic_closed_error is not None:
        logger.warning(
            "%s falling back to main chat after TOPIC_CLOSED chat=%s command=%s",
            log_prefix,
            chat_id,
            text,
        )
    return await client.send_message(chat_id, text)
