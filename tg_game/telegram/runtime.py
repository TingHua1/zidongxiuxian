import asyncio
import logging
from contextlib import suppress

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
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)
DIVINATION_COMMAND = ".卜筮问天"
WORKER_RECONCILE_SECONDS = 5


def _has_expired_external_session(storage: Storage, profile_id: int) -> bool:
    external_account = storage.get_external_account(int(profile_id), ASC_PROVIDER)
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


async def _dispatch_outgoing_commands(
    client: TelegramClient,
    storage: Storage,
    profile_id: int,
) -> None:
    while True:
        try:
            if _has_expired_external_session(storage, profile_id):
                await asyncio.sleep(1)
                continue
            command = storage.claim_next_outgoing_command(profile_id)
            if not command:
                await asyncio.sleep(0.5)
                continue

            chat_id = int(command.get("chat_id") or 0)
            thread_id = command.get("thread_id")
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
                profile_id=profile_id,
                bot_username=bot_username,
                log_prefix=f"Outgoing queue profile={profile_id}",
            )
            if text == DIVINATION_COMMAND and message is not None and chat_id:
                batch = storage.get_active_divination_batch(profile_id, chat_id)
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
            logger.exception(
                "Failed to dispatch queued outgoing command for profile=%s", profile_id
            )
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


async def _register_handlers(
    client: TelegramClient,
    *,
    profile_id: int,
    session_name: str,
) -> None:
    settings = get_settings()
    storage = Storage(settings.database_path)
    client._tg_game_storage = storage
    client._tg_game_profile_id = int(profile_id)
    client._tg_game_session_name = session_name
    router = build_router(storage, runtime_profile_id=int(profile_id))
    await router.startup(client)

    def _should_log_chat(event):
        chat_id = getattr(event, "chat_id", None)
        if chat_id is None:
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
                int(profile_id), chat_id, thread_id, reply_to_msg_id
            )
            is not None
        )

    @client.on(events.NewMessage(incoming=True, outgoing=True))
    async def _incoming_handler(event):
        if settings.telegram_log_messages and _should_log_chat(event):
            logger.info(
                "Message received profile=%s chat=%s sender=%s text=%r",
                profile_id,
                event.chat_id,
                event.sender_id,
                event.raw_text or "",
            )
        await router.dispatch(client, event)

    @client.on(events.MessageEdited(incoming=True))
    async def _edited_handler(event):
        if settings.telegram_log_messages and _should_log_chat(event):
            logger.info(
                "Message edited profile=%s chat=%s sender=%s text=%r",
                profile_id,
                event.chat_id,
                event.sender_id,
                event.raw_text or "",
            )
        await router.dispatch(client, event)

    client._tg_game_outgoing_task = asyncio.create_task(
        _dispatch_outgoing_commands(client, storage, int(profile_id))
    )


async def _cancel_client_background_tasks(client: TelegramClient) -> None:
    background_tasks = list(
        getattr(client, "_tg_game_background_tasks", set()) or set()
    )
    alive_tasks = [task for task in background_tasks if task and not task.done()]
    for task in alive_tasks:
        task.cancel()
    if alive_tasks:
        results = await asyncio.gather(*alive_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception) and not isinstance(
                result, asyncio.CancelledError
            ):
                logger.warning(
                    "Background task exited with error during shutdown: %r", result
                )
    setattr(client, "_tg_game_background_tasks", set())


async def _shutdown_client(client: TelegramClient) -> None:
    await _cancel_client_background_tasks(client)
    outgoing_task = getattr(client, "_tg_game_outgoing_task", None)
    if outgoing_task:
        outgoing_task.cancel()
        with suppress(asyncio.CancelledError):
            await outgoing_task
    if client.is_connected():
        try:
            await asyncio.shield(client.disconnect())
        except Exception:
            logger.exception("Telegram client disconnect failed")
    try:
        await asyncio.shield(asyncio.wait_for(client.disconnected, timeout=8))
    except asyncio.TimeoutError:
        logger.warning("Timed out waiting for Telegram client.disconnected")
    except Exception:
        logger.exception("Waiting for Telegram client.disconnected failed")
    await asyncio.sleep(0.1)


async def _run_profile_worker(profile_id: int) -> None:
    settings = get_settings()
    storage = Storage(settings.database_path)
    while True:
        client = None
        try:
            profile = storage.get_profile(int(profile_id))
            if not profile or not profile.telegram_verified_at:
                await asyncio.sleep(WORKER_RECONCILE_SECONDS)
                continue
            preferred_session_name = (profile.telegram_session_name or "").strip()
            if not preferred_session_name:
                await asyncio.sleep(WORKER_RECONCILE_SECONDS)
                continue
            resolved_session_name = preferred_session_name
            client = _build_client(resolved_session_name)
            await client.connect()
            if not await client.is_user_authorized():
                logger.warning(
                    "Telegram session for profile=%s is not authorized yet; worker waiting",
                    profile_id,
                )
                await client.disconnect()
                await asyncio.sleep(WORKER_RECONCILE_SECONDS)
                continue
            me = await client.get_me()
            logger.info(
                "Telegram worker connected profile=%s telegram_id=%s username=%s phone=%s",
                profile_id,
                getattr(me, "id", None),
                getattr(me, "username", None),
                getattr(me, "phone", None),
            )
            if resolved_session_name != (profile.telegram_session_name or ""):
                storage.bind_profile_telegram_account(
                    profile.id,
                    telegram_user_id=str(
                        getattr(me, "id", "") or profile.telegram_user_id
                    ),
                    telegram_username=(
                        getattr(me, "username", "") or profile.telegram_username
                    ),
                    telegram_phone=(getattr(me, "phone", "") or profile.telegram_phone),
                    telegram_session_name=resolved_session_name,
                )
            await _register_handlers(
                client,
                profile_id=int(profile.id),
                session_name=resolved_session_name,
            )
            await client.run_until_disconnected()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Telegram worker failed for profile=%s", profile_id)
            await asyncio.sleep(2)
        finally:
            if client is not None:
                try:
                    await asyncio.shield(_shutdown_client(client))
                except Exception:
                    logger.exception(
                        "Telegram worker shutdown failed for profile=%s", profile_id
                    )


async def _resolve_worker_targets(storage: Storage) -> dict[int, str]:
    targets = {}
    for profile in storage.list_profiles():
        if not profile.telegram_verified_at:
            continue
        preferred_session_name = (profile.telegram_session_name or "").strip()
        if not preferred_session_name:
            continue
        targets[int(profile.id)] = preferred_session_name
    return targets


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )
    logger.info("Telegram runtime started")
    settings = get_settings()
    storage = Storage(settings.database_path)
    storage.init_schema()
    storage.maybe_cleanup_bound_messages(min_interval_seconds=0)

    keepalive_task = asyncio.create_task(_refresh_external_sessions(storage))
    worker_tasks: dict[int, asyncio.Task] = {}
    worker_sessions: dict[int, str] = {}
    try:
        while True:
            targets = await _resolve_worker_targets(storage)

            for profile_id, session_name in list(worker_sessions.items()):
                if targets.get(profile_id) == session_name:
                    continue
                task = worker_tasks.pop(profile_id, None)
                worker_sessions.pop(profile_id, None)
                if task:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                    logger.info(
                        "Stopped Telegram worker for profile=%s due to runtime target change",
                        profile_id,
                    )

            for profile_id, session_name in targets.items():
                existing_task = worker_tasks.get(profile_id)
                if existing_task and not existing_task.done():
                    continue
                worker_sessions[profile_id] = session_name
                worker_tasks[profile_id] = asyncio.create_task(
                    _run_profile_worker(profile_id)
                )
                logger.info(
                    "Started Telegram worker for profile=%s session=%s",
                    profile_id,
                    session_name,
                )

            completed_profile_ids = [
                profile_id
                for profile_id, task in worker_tasks.items()
                if task.done() and profile_id not in targets
            ]
            for profile_id in completed_profile_ids:
                worker_tasks.pop(profile_id, None)
                worker_sessions.pop(profile_id, None)

            await asyncio.sleep(WORKER_RECONCILE_SECONDS)
    finally:
        keepalive_task.cancel()
        with suppress(asyncio.CancelledError):
            await keepalive_task
        worker_task_list = list(worker_tasks.values())
        for task in worker_task_list:
            task.cancel()
        if worker_task_list:
            await asyncio.gather(*worker_task_list, return_exceptions=True)
        worker_tasks.clear()
        worker_sessions.clear()
        await asyncio.sleep(0.3)


def run_telegram_runtime() -> None:
    asyncio.run(_main())
