import asyncio
import logging

from telethon import TelegramClient, events

from tg_game.config import get_settings
from tg_game.runtime import build_router
from tg_game.services.external_sync import (
    ASC_PROVIDER,
    get_effective_external_cookie,
    get_external_keepalive_poll_seconds,
    is_external_account_expired,
    mark_external_account_failure,
    should_keep_external_session_fresh,
    sync_external_account,
)
from tg_game.storage import Storage
from tg_game.telegram.account import resolve_authorized_session_name
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)
DIVINATION_COMMAND = ".卜筮问天"


def _has_expired_external_session(storage: Storage) -> bool:
    active_profile = storage.get_active_profile()
    if not active_profile:
        return False
    external_account = storage.get_external_account(active_profile.id, ASC_PROVIDER)
    return is_external_account_expired(external_account)


async def _refresh_external_sessions(storage: Storage) -> None:
    while True:
        try:
            for profile in storage.list_profiles():
                if not profile.telegram_verified_at:
                    continue
                external_account = storage.get_external_account(
                    profile.id, ASC_PROVIDER
                )
                if not should_keep_external_session_fresh(profile, external_account):
                    continue
                cookie_text = (
                    (external_account or {}).get("cookie_text")
                    or get_effective_external_cookie(storage)
                ).strip()
                if not cookie_text:
                    continue
                try:
                    await asyncio.to_thread(
                        sync_external_account,
                        storage,
                        profile.id,
                        cookie_text=cookie_text,
                    )
                except Exception as exc:
                    mark_external_account_failure(
                        storage, profile.id, exc, cookie_text=cookie_text
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Telegram external keepalive failed")
        await asyncio.sleep(get_external_keepalive_poll_seconds())


async def _dispatch_outgoing_commands(client: TelegramClient, storage: Storage) -> None:
    while True:
        try:
            if _has_expired_external_session(storage):
                await asyncio.sleep(1)
                continue
            command = storage.claim_next_outgoing_command()
            if not command:
                await asyncio.sleep(0.5)
                continue

            chat_id = int(command.get("chat_id") or 0)
            thread_id = command.get("thread_id")
            profile_id = command.get("profile_id")
            bot_username = command.get("bot_username") or ""
            text = (command.get("text") or "").strip()
            if not chat_id or not text:
                storage.mark_outgoing_command_failed(
                    command["id"], "Missing chat_id or text"
                )
                continue

            latest_command = storage.get_outgoing_command(int(command["id"]))
            if (
                not latest_command
                or str(latest_command.get("status") or "") != "sending"
            ):
                continue

            message = await send_message_with_thread_fallback(
                client,
                chat_id,
                text,
                thread_id=int(thread_id) if thread_id else None,
                storage=storage,
                profile_id=int(profile_id) if profile_id else None,
                bot_username=bot_username,
                log_prefix="Outgoing queue",
            )
            if (
                text == DIVINATION_COMMAND
                and message is not None
                and profile_id
                and chat_id
            ):
                batch = storage.get_active_divination_batch(int(profile_id), chat_id)
                if batch:
                    planned_rounds = max(
                        int(batch.get("target_count") or 0)
                        - int(batch.get("initial_count") or 0),
                        0,
                    )
                    current_sent = max(int(batch.get("sent_count") or 0), 0)
                    current_completed = max(int(batch.get("completed_count") or 0), 0)
                    if (
                        planned_rounds > 0
                        and int(batch.get("pending_command_msg_id") or 0)
                        != int(message.id)
                        and current_sent <= current_completed
                    ):
                        storage.update_divination_batch(
                            int(batch["id"]),
                            thread_id=int(thread_id)
                            if thread_id
                            else batch.get("thread_id"),
                            sent_count=min(current_sent + 1, planned_rounds),
                            pending_command_msg_id=int(message.id),
                        )
            storage.mark_outgoing_command_sent(command["id"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if "command" in locals() and command and command.get("id"):
                storage.mark_outgoing_command_failed(command["id"], str(exc))
            logger.exception("Failed to dispatch queued outgoing command")
            await asyncio.sleep(1)


def _build_client(session_name: str) -> TelegramClient:
    settings = get_settings()
    if not settings.telegram_api_id or not settings.telegram_api_hash:
        raise RuntimeError(
            "Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in environment"
        )
    return TelegramClient(
        session_name,
        int(settings.telegram_api_id),
        settings.telegram_api_hash,
    )


async def _register_handlers(client: TelegramClient) -> None:
    settings = get_settings()
    storage = Storage(settings.database_path)
    client._tg_game_storage = storage
    router = build_router(storage)
    await router.startup(client)

    def _should_log_chat(event):
        active_profile = storage.get_active_profile()
        chat_id = getattr(event, "chat_id", None)
        if not active_profile or chat_id is None:
            return False
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
        return (
            storage.resolve_chat_binding_for_event(
                active_profile.id, chat_id, thread_id, reply_to_msg_id
            )
            is not None
        )

    @client.on(events.NewMessage(incoming=True, outgoing=True))
    async def _incoming_handler(event):
        if settings.telegram_log_messages and _should_log_chat(event):
            logger.info(
                "Message received chat=%s sender=%s text=%r",
                event.chat_id,
                event.sender_id,
                event.raw_text or "",
            )
        await router.dispatch(client, event)

    @client.on(events.MessageEdited(incoming=True))
    async def _edited_handler(event):
        if settings.telegram_log_messages and _should_log_chat(event):
            logger.info(
                "Message edited chat=%s sender=%s text=%r",
                event.chat_id,
                event.sender_id,
                event.raw_text or "",
            )
        await router.dispatch(client, event)

    client._tg_game_outgoing_task = asyncio.create_task(
        _dispatch_outgoing_commands(client, storage)
    )
    client._tg_game_external_keepalive_task = asyncio.create_task(
        _refresh_external_sessions(storage)
    )


async def _bootstrap() -> TelegramClient:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )
    settings = get_settings()
    storage = Storage(settings.database_path)
    storage.init_schema()
    active_profile = storage.get_active_profile()
    preferred_session_name = (
        active_profile.telegram_session_name if active_profile else ""
    ) or ""
    if (
        active_profile
        and active_profile.telegram_verified_at
        and not preferred_session_name
        and getattr(settings, "telegram_login_session_name", "")
    ):
        preferred_session_name = settings.telegram_login_session_name
    resolved_session_name = await resolve_authorized_session_name(
        preferred_session_name
    )
    client = _build_client(resolved_session_name)
    await client.connect()
    if not await client.is_user_authorized():
        logger.warning(
            "Telegram session is not authorized yet; runtime will wait for web login"
        )
        return client
    me = await client.get_me()
    logger.info(
        "Telegram authorized as id=%s username=%s phone=%s",
        getattr(me, "id", None),
        getattr(me, "username", None),
        getattr(me, "phone", None),
    )
    if active_profile and resolved_session_name != (
        active_profile.telegram_session_name or ""
    ):
        storage.bind_profile_telegram_account(
            active_profile.id,
            telegram_user_id=str(
                getattr(me, "id", "") or active_profile.telegram_user_id
            ),
            telegram_username=(
                getattr(me, "username", "") or active_profile.telegram_username
            ),
            telegram_phone=(getattr(me, "phone", "") or active_profile.telegram_phone),
            telegram_session_name=resolved_session_name,
        )
    await _register_handlers(client)
    return client


async def _main() -> None:
    logger.info("Telegram runtime started")
    while True:
        client = await _bootstrap()
        if not await client.is_user_authorized():
            await client.disconnect()
            await asyncio.sleep(5)
            continue
        try:
            await client.run_until_disconnected()
        finally:
            outgoing_task = getattr(client, "_tg_game_outgoing_task", None)
            if outgoing_task:
                outgoing_task.cancel()
                try:
                    await outgoing_task
                except asyncio.CancelledError:
                    pass
            keepalive_task = getattr(client, "_tg_game_external_keepalive_task", None)
            if keepalive_task:
                keepalive_task.cancel()
                try:
                    await keepalive_task
                except asyncio.CancelledError:
                    pass
            if client.is_connected():
                await client.disconnect()
        await asyncio.sleep(2)


def run_telegram_runtime() -> None:
    asyncio.run(_main())
