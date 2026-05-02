import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

import artifact_game
import basic_game
import battle_feature_game
import breakthrough_game
import companion_game
import diplomacy_game
import dungeon_feature_game
import estate_game
import fanren_game
import market_trade_game
import sect_game
import shop_game
import stock_trade_game
import inventory_feature_game

from tg_game.runtime.context import EventContext
from tg_game.services.cultivation_sync import sync_cultivation_session
from tg_game.storage import CompatDb as SQLiteCompatDb, Storage
from tg_game.telegram.send_utils import send_message_with_thread_fallback


logger = logging.getLogger(__name__)

DIVINATION_COMMAND = ".卜筮问天"


SECT_FEATURE_REPLY_WHITELISTS = {
    "huangfeng": {
        ".小药园",
        ".播种",
        ".采药",
        ".除草",
        ".除虫",
        ".浇水",
        ".扩建药园",
    },
    "xingong": {
        ".启阵",
        ".助阵",
        ".观星台",
        ".牵引星辰",
        ".收集精华",
        ".安抚星辰",
        ".观星",
        ".改换星移",
        ".我的侍妾",
        ".每日问安",
    },
    "lingxiao": {
        ".凌霄宫",
        ".天阶状态",
        ".问心台",
        ".登天阶",
        ".引九天罡风",
        ".借天门势",
    },
    "taiyi": {".引道", ".神识冲击"},
    "wanling": {
        ".寻觅灵兽",
        ".我的灵兽",
        ".喂养",
        ".灵兽出战",
        ".灵兽休息",
        ".一键放养",
        ".灵兽偷菜",
        ".探渊",
    },
    "luoyun": {".灵树状态", ".灵树灌溉", ".协同守山", ".采摘灵果"},
    "yinluo": {
        ".我的阴罗幡",
        ".升级阴罗幡",
        ".每日献祭",
        ".化功为煞",
        ".血洗山林",
        ".召唤魔影",
        ".囚禁魂魄",
        ".安抚幡灵",
        ".收取精华",
        ".下咒",
        ".收割",
    },
    "yuanying": {
        ".元婴状态",
        ".元婴出窍",
        ".元婴闭关",
        ".元婴归窍",
        ".问道",
        ".参悟功法",
    },
    "hehuan": {
        ".闭关双修",
        ".缔结同参",
        ".双修 温养",
        ".种下心印",
        ".双修 采补",
        ".挣脱心印",
        ".结印",
    },
}


class BaseExecutor(ABC):
    key = "base"

    async def startup(self, client: object, storage: Storage) -> None:
        return None

    def _expected_profile_user_id(self, context: EventContext) -> str:
        binding_user_id = (
            context.chat_binding.telegram_user_id if context.chat_binding else ""
        )
        return binding_user_id or (
            context.profile.telegram_user_id if context.profile else ""
        )

    async def _bot_message_targets_profile(
        self, context: EventContext, storage: Storage
    ) -> bool:
        if await context.bot_message_targets_profile():
            return True
        return False

    def _get_stored_reply_message(
        self, context: EventContext, storage: Storage
    ) -> Optional[dict]:
        return None

    async def _get_reply_message_text(
        self, context: EventContext, storage: Storage
    ) -> str:
        reply_text = await context.get_reply_message_text()
        if reply_text:
            return reply_text.strip()
        return ""

    @abstractmethod
    async def handle(self, context: EventContext, storage: Storage) -> bool:
        raise NotImplementedError


class FanrenExecutor(BaseExecutor):
    key = "fanren"

    def __init__(self) -> None:
        self._runner_started = False

    async def startup(self, client: object, storage: Storage) -> None:
        if self._runner_started:
            return
        self._runner_started = True
        db = SQLiteCompatDb(storage)
        fanren_game.ensure_tables(db)
        db.close()
        asyncio.create_task(
            fanren_game.runner(
                client,
                storage,
                profile_id=getattr(client, "_tg_game_profile_id", None),
            )
        )
        logger.info("Fanren executor runner started")

    async def handle(self, context: EventContext, storage: Storage) -> bool:
        if not context.chat_binding:
            return False

        db = SQLiteCompatDb(storage)
        try:
            if context.text.startswith(".fanren") and context.is_profile_owner():
                if context.profile:
                    storage.set_chat_binding_thread_id(
                        context.profile.id, context.chat_id, context.thread_id
                    )
                    fanren_game.update_session(
                        db,
                        context.chat_id,
                        profile_id=context.profile.id if context.profile else None,
                        thread_id=context.thread_id,
                    )
                return await self._handle_command(context, db)

            binding_bot = (context.chat_binding.bot_username or "").lower().lstrip("@")
            if (
                context.is_bot_sender
                and binding_bot
                and context.bot_username == binding_bot
                and await self._bot_message_targets_profile(context, storage)
            ):
                reply_text = await self._get_reply_message_text(context, storage)
                if reply_text and reply_text not in {
                    fanren_game.FANREN_CHECK_COMMAND,
                    fanren_game.FANREN_NORMAL_COMMAND,
                    fanren_game.FANREN_DEEP_COMMAND,
                    ".强行出关",
                    fanren_game.RIFT_EXPLORE_COMMAND,
                    fanren_game.YUANYING_OUTING_COMMAND,
                    fanren_game.YUANYING_STATUS_COMMAND,
                }:
                    return False
                stored_reply_message = self._get_stored_reply_message(context, storage)
                reply_message_id = context.reply_to_msg_id or int(
                    (stored_reply_message or {}).get("message_id") or 0
                )
                parsed = await fanren_game.handle_bot_message(
                    context.event,
                    db,
                    client=context.client,
                    profile_id=context.profile.id if context.profile else None,
                )
                if parsed is not None:
                    session = fanren_game.get_session(
                        db,
                        context.chat_id,
                        profile_id=context.profile.id if context.profile else None,
                    )
                    await fanren_game.maybe_delete_normal_command_message(
                        context.event,
                        session,
                        context.client,
                        reply_text,
                        reply_message_id=reply_message_id or None,
                    )
                    if context.profile and context.chat_id is not None:
                        try:
                            sync_cultivation_session(
                                storage, context.profile.id, context.chat_id, db
                            )
                        except Exception as exc:
                            logger.warning(
                                "Cultivation API sync failed in chat %s: %s",
                                context.chat_id,
                                exc,
                            )
                    self._record_result(context, storage, parsed.event)
                return parsed is not None
            if (
                context.is_bot_sender
                and binding_bot
                and context.bot_username == binding_bot
                and await context.bot_message_targets_profile()
            ):
                parsed = await fanren_game.handle_bot_message(
                    context.event,
                    db,
                    client=context.client,
                    profile_id=context.profile.id if context.profile else None,
                )
                if parsed is not None:
                    return True
            return False
        finally:
            db.close()

    def _record_result(
        self, context: EventContext, storage: Storage, event_name: str
    ) -> None:
        if not event_name:
            return
        # 记录所有闭关/元婴/裂缝相关事件，不只是里程碑
        if event_name in {"empty", "ignored", "blocked", "resource_blocked", "unknown"}:
            return
        if event_name.endswith("_edited"):
            pass  # 编辑事件总是记录
        elif not any(
            event_name.startswith(prefix)
            for prefix in (
                "retreat_",
                "deep_",
                "cultivat",
                "cooldown",
                "rift_",
                "yuanying_",
                "soul_",
                "meditation",
            )
        ) and event_name not in {
            "cultivation_full",
            "soul_returning",
            "jie_dan",
            "jie_dan_complete",
        }:
            return
        session_setting = context.get_setting("cultivation") or context.get_setting(
            "basic"
        )
        gain_value = fanren_game.parse_gain_value(context.text)
        stage_name, progress_text = fanren_game.extract_stage_progress(context.text)
        mode = "normal"
        if context.chat_id is not None:
            db = SQLiteCompatDb(storage)
            try:
                session = fanren_game.get_session(
                    db,
                    context.chat_id,
                    profile_id=context.profile.id if context.profile else None,
                )
                mode = (
                    (session.get("retreat_mode") or "normal") if session else "normal"
                )
            finally:
                db.close()
        elif (
            session_setting
            and session_setting.command_template == fanren_game.FANREN_CHECK_COMMAND
        ):
            mode = "deep"
        storage.record_cultivation_result(
            profile_id=context.profile.id if context.profile else None,
            chat_id=context.chat_id or 0,
            mode=mode,
            event=event_name,
            gain_value=gain_value,
            stage_name=stage_name,
            progress_text=progress_text,
            summary=fanren_game.parse_message(context.text).summary,
            raw_text=context.text,
        )

    async def _handle_command(self, context: EventContext, db: SQLiteCompatDb) -> bool:
        parts = context.text.split(maxsplit=2)
        action = parts[1].lower() if len(parts) > 1 else "status"
        payload = parts[2].strip() if len(parts) > 2 else ""
        chat_id = context.chat_id
        if chat_id is None:
            return False

        setting = context.get_setting("cultivation") or context.get_setting("basic")
        if setting:
            fanren_game.set_interval(
                db,
                chat_id,
                setting.check_interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            if setting.command_template:
                fanren_game.set_check_command(
                    db,
                    chat_id,
                    setting.command_template,
                    profile_id=context.profile.id if context.profile else None,
                )

        if action == "on":
            if payload in {"normal", "deep"}:
                fanren_game.set_mode(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            if context.profile:
                sync_cultivation_session(storage, context.profile.id, chat_id, db)
            fanren_game.set_enabled(
                db,
                chat_id,
                True,
                reset_failure=True,
                profile_id=context.profile.id if context.profile else None,
            )
            session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(
                f"凡人修仙自动化已开启，当前模式为 {'深度闭关' if session.get('retreat_mode') == 'deep' else '普通闭关'}，将按接口冷却时间自动调度。"
            )
            return True
        if action == "off":
            fanren_game.set_enabled(
                db,
                chat_id,
                False,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("凡人修仙自动化已关闭。")
            return True
        if action == "status":
            session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(fanren_game.build_status_text(session))
            return True
        if action == "dry-run":
            enabled = payload.lower() == "on"
            fanren_game.set_dry_run(
                db,
                chat_id,
                enabled,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"凡人修仙 dry-run 已{'开启' if enabled else '关闭'}。")
            return True
        if action == "interval":
            try:
                interval_seconds = fanren_game.parse_interval_input(payload)
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            fanren_game.set_interval(
                db,
                chat_id,
                interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(
                f"凡人修仙检查间隔已设置为 {fanren_game.format_duration(interval_seconds)}。"
            )
            return True
        if action == "check":
            try:
                check_command = fanren_game.set_check_command(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            await context.reply(f"凡人修仙检查指令已设置为: {check_command}")
            return True
        if action == "mode":
            try:
                retreat_mode = fanren_game.set_mode(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            if context.profile:
                sync_cultivation_session(storage, context.profile.id, chat_id, db)
            await context.reply(
                f"凡人修仙模式已设置为 {'深度闭关' if retreat_mode == 'deep' else '普通闭关'}，将按接口冷却时间自动调度。"
            )
            return True
        if action == "run":
            if context.profile:
                sync_cultivation_session(storage, context.profile.id, chat_id, db)
            _ok, status = await fanren_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=False,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "reset":
            fanren_game.reset_failures(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("凡人修仙失败计数已重置。")
            return True
        if action == "rift":
            rift_action = payload.lower() if payload else "status"
            rift_session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            if rift_action == "on":
                fanren_game.set_auto_rift(
                    db,
                    chat_id,
                    True,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动探寻裂缝已开启，CD 12 小时。")
                return True
            if rift_action == "off":
                fanren_game.set_auto_rift(
                    db,
                    chat_id,
                    False,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动探寻裂缝已关闭。")
                return True
            if rift_action == "status":
                await context.reply(
                    "\n".join(
                        [
                            "自动探寻裂缝状态",
                            f"开关: {'开启' if rift_session.get('auto_rift_enabled') else '关闭'}",
                            f"状态: {rift_session.get('rift_state') or '-'}",
                            f"下次: {fanren_game.format_timestamp(rift_session.get('rift_next_check_time') or 0)}",
                            f"重试: {rift_session.get('rift_retry_count') or 0}/{fanren_game.RIFT_RETRY_MAX}",
                        ]
                    )
                )
                return True
        if action == "yuanying":
            yy_action = payload.lower() if payload else "status"
            yy_session = fanren_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            if yy_action == "on":
                fanren_game.set_auto_yuanying(
                    db,
                    chat_id,
                    True,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动元婴出窍已开启，CD 8 小时。")
                return True
            if yy_action == "off":
                fanren_game.set_auto_yuanying(
                    db,
                    chat_id,
                    False,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("自动元婴出窍已关闭。")
                return True
            if yy_action == "status":
                await context.reply(
                    "\n".join(
                        [
                            "自动元婴出窍状态",
                            f"开关: {'开启' if yy_session.get('auto_yuanying_enabled') else '关闭'}",
                            f"状态: {yy_session.get('yuanying_state') or '-'}",
                            f"下次: {fanren_game.format_timestamp(yy_session.get('yuanying_next_check_time') or 0)}",
                        ]
                    )
                )
                return True

        await context.reply(
            "用法: .fanren status|on [normal|deep]|off|mode normal|deep|dry-run on|off|interval 5m|check 指令|run|reset|rift on|off|status|yuanying on|off|status"
        )
        return True


class SectExecutor(BaseExecutor):
    key = "sect"

    def __init__(self) -> None:
        self._runner_started = False

    def _reply_matches_whitelist(self, reply_text: str, feature_key: str) -> bool:
        reply_text = (reply_text or "").strip()
        if not reply_text:
            return False
        for command in SECT_FEATURE_REPLY_WHITELISTS.get(feature_key, set()):
            if reply_text == command or reply_text.startswith(f"{command} "):
                return True
        return False

    async def startup(self, client: object, storage: Storage) -> None:
        if self._runner_started:
            return
        self._runner_started = True
        db = SQLiteCompatDb(storage)
        sect_game.ensure_tables(db)
        db.close()
        asyncio.create_task(
            sect_game.runner(
                client,
                storage,
                profile_id=getattr(client, "_tg_game_profile_id", None),
            )
        )
        logger.info("Sect executor runner started")

    async def handle(self, context: EventContext, storage: Storage) -> bool:
        if not context.chat_binding:
            return False

        db = SQLiteCompatDb(storage)
        try:
            if context.text.startswith(".sect") and context.is_profile_owner():
                if context.profile:
                    storage.set_chat_binding_thread_id(
                        context.profile.id, context.chat_id, context.thread_id
                    )
                    sect_game.update_session(
                        db,
                        context.chat_id,
                        profile_id=context.profile.id if context.profile else None,
                        thread_id=context.thread_id,
                    )
                return await self._handle_command(context, db)

            binding_bot = (context.chat_binding.bot_username or "").lower().lstrip("@")
            if (
                context.is_bot_sender
                and binding_bot
                and context.bot_username == binding_bot
                and await self._bot_message_targets_profile(context, storage)
            ):
                preview_parsed = sect_game.parse_message(context.text)
                reply_text = await self._get_reply_message_text(context, storage)
                if reply_text:
                    if (
                        preview_parsed.get("event")
                        in {
                            "sect_panel",
                            "sect_panel_pending",
                            "sect_info",
                        }
                        and reply_text != ".我的宗门"
                    ):
                        return False
                    if (
                        preview_parsed.get("event") == "lingxiao_step"
                        and reply_text != ".登天阶"
                    ):
                        return False
                parsed = await sect_game.handle_bot_message(
                    context.event,
                    db,
                    client=context.client,
                    profile_id=context.profile.id if context.profile else None,
                )
                if parsed is not None:
                    return True
                return parsed is not None
            return False
        finally:
            db.close()

    async def _handle_command(self, context: EventContext, db: SQLiteCompatDb) -> bool:
        parts = context.text.split(maxsplit=2)
        action = parts[1].lower() if len(parts) > 1 else "status"
        payload = parts[2].strip() if len(parts) > 2 else ""
        chat_id = context.chat_id
        if chat_id is None:
            return False

        setting = context.get_setting("sect") or context.get_setting("basic")
        if setting:
            sect_game.set_interval(
                db,
                chat_id,
                setting.check_interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            if setting.command_template:
                sect_game.set_check_command(
                    db,
                    chat_id,
                    setting.command_template,
                    profile_id=context.profile.id if context.profile else None,
                )

        if action == "on":
            sect_game.set_enabled(
                db,
                chat_id,
                True,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("宗门模块已开启。")
            return True
        if action == "off":
            sect_game.set_enabled(
                db,
                chat_id,
                False,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply("宗门模块已关闭。")
            return True
        if action == "status":
            session = sect_game.get_session(
                db,
                chat_id,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(sect_game.build_status_text(session))
            return True
        if action == "dry-run":
            enabled = payload.lower() == "on"
            sect_game.set_dry_run(
                db,
                chat_id,
                enabled,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"宗门 dry-run 已{'开启' if enabled else '关闭'}。")
            return True
        if action == "interval":
            try:
                interval_seconds = fanren_game.parse_interval_input(payload)
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            sect_game.set_interval(
                db,
                chat_id,
                interval_seconds,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(
                f"宗门检查间隔已设置为 {fanren_game.format_duration(interval_seconds)}。"
            )
            return True
        if action == "check":
            try:
                check_command = sect_game.set_check_command(
                    db,
                    chat_id,
                    payload,
                    profile_id=context.profile.id if context.profile else None,
                )
            except ValueError as exc:
                await context.reply(f"设置失败: {exc}")
                return True
            await context.reply(f"宗门查询指令已设置为: {check_command}")
            return True
        if action == "panel":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".我的宗门",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "sign":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".宗门点卯",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "teach":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".宗门传功",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "bounty":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=".宗门悬赏",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "submit":
            if not payload:
                await context.reply("用法: .sect submit 问候")
                return True
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                command_text=f".提交任务 {payload}",
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True
        if action == "hf":
            return await self._handle_huangfeng_command(context, db, payload)
        if action == "xg":
            return await self._handle_xingong_command(context, db, payload)
        if action == "lx":
            return await self._handle_lingxiao_command(context, db, payload)
        if action == "ty":
            return await self._handle_taiyi_command(context, db, payload)
        if action == "wl":
            return await self._handle_wanling_command(context, db, payload)
        if action == "ly":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "status": ".灵树状态",
                    "water": ".灵树灌溉",
                    "guard": ".协同守山",
                    "harvest": ".采摘灵果",
                },
                "用法: .sect ly status|water|guard|harvest",
            )
        if action == "yl":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "banner": ".我的阴罗幡",
                    "upgrade": ".升级阴罗幡",
                    "daily": ".每日献祭",
                    "convert": ".化功为煞",
                    "hunt": ".血洗山林",
                    "summon": ".召唤魔影",
                    "prison": ".囚禁魂魄",
                    "soothe": ".安抚幡灵",
                    "collect": ".收取精华",
                    "curse": ".下咒",
                    "reap": ".收割",
                },
                "用法: .sect yl banner|upgrade|daily|convert|hunt|summon|prison|soothe|collect|curse|reap",
            )
        if action == "yy":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "status": ".元婴状态",
                    "trip": ".元婴出窍",
                    "retreat": ".元婴闭关",
                    "return": ".元婴归窍",
                    "seek": ".问道",
                    "skill": ".参悟功法",
                },
                "用法: .sect yy status|trip|retreat|return|seek|skill",
            )
        if action == "hh":
            return await self._handle_simple_feature_command(
                context,
                db,
                payload,
                {
                    "dual": ".闭关双修",
                    "contract": ".缔结同参",
                    "warm": ".双修 温养",
                    "mark": ".种下心印",
                    "harvest": ".双修 采补",
                    "break": ".挣脱心印",
                    "seal": ".结印",
                },
                "用法: .sect hh dual|contract|warm|mark|harvest|break|seal",
            )
        if action == "run":
            _ok, status, _msg_id = await sect_game.maybe_send_check(
                context.client,
                db,
                chat_id,
                force=True,
                profile_id=context.profile.id if context.profile else None,
            )
            await context.reply(f"执行结果: {status}")
            return True

        await context.reply(
            "用法: .sect status|on|off|dry-run on|off|interval 30m|check 指令|panel|sign|teach|bounty|submit 内容|hf/xg/lx/ty/wl/ly/yl/yy/hh 子命令|run"
        )
        return True

    async def _handle_simple_feature_command(
        self,
        context: EventContext,
        db: SQLiteCompatDb,
        payload: str,
        action_map: dict,
        usage: str,
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        action = (payload or "").strip().lower()
        command_text = action_map.get(action)
        if not command_text:
            await context.reply(usage)
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True


class GeneralGameExecutor(BaseExecutor):
    key = "game"

    def __init__(self) -> None:
        self._runner_started = False
        self._parsers = [
            ("basic", basic_game.parse_message),
            ("breakthrough", breakthrough_game.parse_message),
            ("battle", battle_feature_game.parse_message),
            ("inventory", inventory_feature_game.parse_message),
            ("artifact", artifact_game.parse_message),
            ("estate", estate_game.parse_message),
            ("companion", companion_game.parse_message),
            ("dungeon", dungeon_feature_game.parse_message),
            ("market", market_trade_game.parse_message),
            ("stock", stock_trade_game.parse_message),
            ("diplomacy", diplomacy_game.parse_message),
            ("shop", shop_game.parse_message),
        ]

    async def startup(self, client: object, storage: Storage) -> None:
        if self._runner_started:
            return
        self._runner_started = True
        return

    async def handle(self, context: EventContext, storage: Storage) -> bool:
        if context.text.strip() == ".chatid" and context.is_profile_owner():
            binding_ref = (
                f"{context.chat_id}_{context.thread_id}"
                if context.thread_id
                else f"{context.chat_id}"
            )
            await context.reply(
                "\n".join(
                    [
                        "当前聊天信息",
                        f"绑定 ID: {binding_ref}",
                        f"Chat ID: {context.chat_id}",
                        f"Thread ID: {context.thread_id or '无'}",
                        f"类型: {'私聊' if context.is_private else '群组/频道'}",
                        f"发送者 ID: {context.sender_id}",
                        f"线程状态: {'话题线程' if context.thread_id else '主会话'}",
                    ]
                )
            )
            return True
        if not context.chat_binding:
            return False

        await self._maybe_advance_divination_batch(context, storage)

        if context.is_bot_sender and await self._bot_message_targets_profile(
            context, storage
        ):
            reply_text = await self._get_reply_message_text(context, storage)
            if (
                context.profile
                and context.chat_id is not None
                and reply_text
                in {
                    ".我的持仓",
                    ".股市任务",
                }
                and context.text
            ):
                # 校验回包内容确实是股票相关，过滤误匹配
                stock_keywords = (
                    "持仓",
                    "股票",
                    "浮盈",
                    "市值",
                    "仓位",
                    "股息",
                    "融资",
                )
                is_stock_reply = any(
                    kw in (context.text or "") for kw in stock_keywords
                )
                if reply_text == ".我的持仓":
                    is_stock_reply = is_stock_reply or "我的股票账户" in (
                        context.text or ""
                    )
                elif reply_text == ".股市任务":
                    is_stock_reply = is_stock_reply or "股市任务" in (
                        context.text or ""
                    )
                if is_stock_reply:
                    storage.upsert_stock_player_reply(
                        context.profile.id,
                        context.chat_id,
                        reply_text,
                        context.text,
                        thread_id=context.thread_id,
                        source_message_id=int(context.message_id or 0),
                        reply_to_msg_id=int(context.reply_to_msg_id or 0),
                    )
            for module_key, parser in self._parsers:
                parsed = parser(context.text)
                if parsed is not None:
                    if module_key == "basic" and parsed.get("event") in {
                        "basic_profile",
                        "basic_profile_pending",
                    }:
                        continue
                    if (
                        module_key == "battle"
                        and parsed.get("event") == "battle_profile"
                    ):
                        continue
                    if (
                        module_key == "artifact"
                        and parsed.get("event") == "artifact_status_profile"
                    ):
                        continue
                    return True
        return False

    async def _maybe_advance_divination_batch(
        self, context: EventContext, storage: Storage
    ) -> None:
        if not context.profile or context.chat_id is None:
            return
        batch = storage.get_active_divination_batch(context.profile.id, context.chat_id)
        if not batch:
            return

        pending_command_msg_id = int(batch.get("pending_command_msg_id") or 0)
        planned_rounds = max(
            int(batch.get("target_count") or 0) - int(batch.get("initial_count") or 0),
            0,
        )
        if await self._maybe_resume_idle_divination_batch(
            context, storage, batch, planned_rounds
        ):
            return

        if context.is_outgoing:
            if context.text.strip() != DIVINATION_COMMAND or not context.message_id:
                return
            if pending_command_msg_id:
                return
            sent_count = min(int(batch.get("sent_count") or 0) + 1, planned_rounds)
            storage.update_divination_batch(
                int(batch["id"]),
                thread_id=context.thread_id or batch.get("thread_id"),
                sent_count=sent_count,
                pending_command_msg_id=int(context.message_id),
            )
            return

        if not context.is_bot_sender:
            return
        binding_bot = (context.chat_binding.bot_username or "").lower().lstrip("@")
        effective_bot_username = context.bot_username
        if (
            binding_bot
            and effective_bot_username
            and effective_bot_username != binding_bot
        ):
            return
        reply_to_msg_id = int(context.reply_to_msg_id or 0)
        if not pending_command_msg_id or reply_to_msg_id != pending_command_msg_id:
            return

        completed_count = min(
            int(batch.get("completed_count") or 0) + 1, planned_rounds
        )
        if completed_count >= planned_rounds:
            storage.update_divination_batch(
                int(batch["id"]),
                completed_count=completed_count,
                pending_command_msg_id=0,
            )
            storage.finish_divination_batch(int(batch["id"]), status="completed")
            return

        storage.update_divination_batch(
            int(batch["id"]),
            completed_count=completed_count,
            pending_command_msg_id=0,
        )
        storage.enqueue_outgoing_command(
            profile_id=context.profile.id,
            chat_id=int(batch.get("chat_id") or context.chat_id),
            text=DIVINATION_COMMAND,
            thread_id=int(batch.get("thread_id")) if batch.get("thread_id") else None,
            chat_type=str(batch.get("chat_type") or "group"),
            bot_username=str(batch.get("bot_username") or ""),
            delay_seconds=15,
        )

    async def _maybe_resume_idle_divination_batch(
        self,
        context: EventContext,
        storage: Storage,
        batch: dict,
        planned_rounds: int,
    ) -> bool:
        if planned_rounds <= 0:
            storage.finish_divination_batch(int(batch["id"]), status="completed")
            return True

        pending_command_msg_id = int(batch.get("pending_command_msg_id") or 0)
        if pending_command_msg_id:
            return False

        completed_count = max(int(batch.get("completed_count") or 0), 0)
        if completed_count >= planned_rounds:
            storage.finish_divination_batch(int(batch["id"]), status="completed")
            return True

        thread_id = int(batch.get("thread_id")) if batch.get("thread_id") else None
        latest_command = storage.get_latest_outgoing_command(
            int(batch.get("chat_id") or context.chat_id),
            profile_id=context.profile.id,
            text=DIVINATION_COMMAND,
            thread_id=thread_id,
        )
        if latest_command:
            latest_status = str(latest_command.get("status") or "").strip()
            latest_updated_at = float(latest_command.get("updated_at") or 0)
            batch_updated_at = float(batch.get("updated_at") or 0)
            if latest_status in {"pending", "sending"}:
                return True
            if latest_status == "sent" and latest_updated_at >= batch_updated_at:
                return True

        storage.enqueue_outgoing_command(
            profile_id=context.profile.id,
            chat_id=int(batch.get("chat_id") or context.chat_id),
            text=DIVINATION_COMMAND,
            thread_id=thread_id,
            chat_type=str(batch.get("chat_type") or "group"),
            bot_username=str(batch.get("bot_username") or ""),
        )
        return True

    async def _send(self, context: EventContext, command_text: str) -> bool:
        await send_message_with_thread_fallback(
            context.client,
            context.chat_id,
            command_text,
            thread_id=context.thread_id,
            storage=None,
            profile_id=context.profile.id if context.profile else None,
            bot_username=(
                context.chat_binding.bot_username if context.chat_binding else ""
            ),
            log_prefix="Runtime executor",
        )
        await context.reply(f"执行结果: sent `{command_text}`")
        return True

    async def _handle_huangfeng_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        session = sect_game.get_session(
            db,
            chat_id,
            profile_id=context.profile.id if context.profile else None,
        )
        parts = (payload or "").split(maxsplit=2)
        action = parts[0].lower() if parts else ""
        command_texts = []
        if action == "garden":
            command_texts = [".小药园"]
        elif action == "sow":
            if len(parts) >= 3:
                command_texts = [f".播种 {parts[1]} {parts[2]}"]
            elif len(parts) >= 2:
                plots = sect_game._get_huangfeng_known_plots(session)
                command_texts = [f".播种 {plot} {parts[1]}" for plot in plots]
        elif action == "harvest":
            if len(parts) >= 2:
                command_texts = [f".采药 {parts[1]}"]
            else:
                command_texts = [
                    f".采药 {plot}"
                    for plot in sect_game._get_huangfeng_known_plots(session)
                ]
        elif action == "weed":
            if len(parts) >= 2:
                command_texts = [f".除草 {parts[1]}"]
            else:
                command_texts = [
                    f".除草 {plot}"
                    for plot in sect_game._get_huangfeng_known_plots(session)
                ]
        elif action == "bug":
            if len(parts) >= 2:
                command_texts = [f".除虫 {parts[1]}"]
            else:
                command_texts = [
                    f".除虫 {plot}"
                    for plot in sect_game._get_huangfeng_known_plots(session)
                ]
        elif action == "water":
            if len(parts) >= 2:
                command_texts = [f".浇水 {parts[1]}"]
            else:
                command_texts = [
                    f".浇水 {plot}"
                    for plot in sect_game._get_huangfeng_known_plots(session)
                ]
        elif action == "expand":
            command_texts = [".扩建药园"]
        elif action == "auto":
            auto_body = parts[1] if len(parts) >= 2 else ""
            if len(parts) >= 3:
                auto_body = f"{parts[1]} {parts[2]}".strip()
            auto_parts = auto_body.split(maxsplit=1)
            auto_action = auto_parts[0].lower() if auto_parts else "status"
            auto_payload = auto_parts[1].strip() if len(auto_parts) > 1 else ""
            if auto_action == "on":
                seed_name = (
                    auto_payload
                    or str((session or {}).get("huangfeng_seed_name") or "").strip()
                )
                if not seed_name:
                    await context.reply("用法: .sect hf auto on 种子名")
                    return True
                sect_game.configure_huangfeng_auto(
                    db,
                    chat_id,
                    True,
                    seed_name=seed_name,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(f"黄枫谷自动化已开启，播种种子为 {seed_name}。")
                return True
            if auto_action == "off":
                sect_game.configure_huangfeng_auto(
                    db,
                    chat_id,
                    False,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply("黄枫谷自动化已关闭。")
                return True
            if auto_action == "seed":
                if not auto_payload:
                    await context.reply("用法: .sect hf auto seed 种子名")
                    return True
                sect_game.set_huangfeng_seed(
                    db,
                    chat_id,
                    auto_payload,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(f"黄枫谷自动播种种子已设置为 {auto_payload}。")
                return True
            if auto_action == "exchange":
                enabled = auto_payload.lower() == "on"
                sect_game.set_huangfeng_exchange_auto(
                    db,
                    chat_id,
                    enabled,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(
                    f"黄枫谷自动兑换种子已{'开启' if enabled else '关闭'}。"
                )
                return True
            if auto_action == "status":
                refreshed_session = sect_game.get_session(
                    db,
                    chat_id,
                    profile_id=context.profile.id if context.profile else None,
                )
                await context.reply(
                    "\n".join(
                        [
                            "黄枫谷自动化状态",
                            f"开关: {'开启' if refreshed_session.get('auto_huangfeng_enabled') else '关闭'}",
                            f"播种种子: {refreshed_session.get('huangfeng_seed_name') or '-'}",
                            f"自动兑换: {'开启' if refreshed_session.get('auto_huangfeng_exchange_enabled') else '关闭'}",
                            f"下次检查: {sect_game.format_timestamp(refreshed_session.get('huangfeng_next_check_time') or 0)}",
                            f"状态来源: {refreshed_session.get('huangfeng_next_check_source') or '-'}",
                        ]
                    )
                )
                return True
        if not command_texts:
            await context.reply(
                "用法: .sect hf garden|sow [地块] 种子|harvest [地块]|weed [地块]|bug [地块]|water [地块]|expand|auto on 种子|off|seed 种子|exchange on|off|status"
            )
            return True
        if len(command_texts) > 1 and not session:
            await context.reply(
                "黄枫谷会话未初始化，请先执行 .sect on 或 .sect hf garden。"
            )
            return True
        if len(command_texts) > 1 and not sect_game._get_huangfeng_known_plots(session):
            await context.reply(
                "缺少最近药园状态，请先执行 .sect hf garden 后再省略地块。"
            )
            return True
        command_text = command_texts[0]
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        if status == "sent" and len(command_texts) > 1:
            storage = getattr(context.client, "_tg_game_storage", None)
            if storage and context.profile:
                for index, extra_command in enumerate(command_texts[1:], start=1):
                    storage.enqueue_outgoing_command(
                        profile_id=context.profile.id,
                        chat_id=chat_id,
                        text=extra_command,
                        thread_id=session.get("thread_id")
                        if session
                        else context.thread_id,
                        chat_type="group",
                        bot_username=(
                            context.chat_binding.bot_username
                            if context.chat_binding
                            else ""
                        ),
                        delay_seconds=index * 3,
                    )
                await context.reply(
                    f"执行结果: {status}，已按最近药园状态为全部地块排队 {len(command_texts)} 条命令。"
                )
                return True
        await context.reply(f"执行结果: {status}")
        return True

    async def _handle_taiyi_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        parts = (payload or "").split(maxsplit=1)
        action = parts[0].lower() if parts else ""
        argument = parts[1] if len(parts) > 1 else ""
        command_text = None
        if action == "guide":
            if argument not in {"金", "木", "水", "火", "土"}:
                await context.reply("用法: .sect ty guide 金|木|水|火|土")
                return True
            command_text = f".引道 {argument}"
        elif action == "shock":
            command_text = ".神识冲击"
        if not command_text:
            await context.reply("用法: .sect ty guide 金|木|水|火|土|shock")
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True

    async def _handle_wanling_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        parts = (payload or "").split(maxsplit=2)
        action = parts[0].lower() if parts else ""
        command_text = None
        if action == "search":
            command_text = ".寻觅灵兽"
        elif action == "status":
            command_text = ".我的灵兽"
        elif action == "feed" and len(parts) >= 3:
            command_text = f".喂养 {parts[1]} {parts[2]}"
        elif action == "battle" and len(parts) >= 2:
            command_text = f".灵兽出战 {parts[1]}"
        elif action == "rest":
            command_text = ".灵兽休息"
        elif action == "farm":
            command_text = ".一键放养"
        elif action == "steal":
            command_text = ".灵兽偷菜"
        elif action == "abyss":
            command_text = ".探渊"
        if not command_text:
            await context.reply(
                "用法: .sect wl search|status|feed 灵兽 物品*数量|battle 灵兽|rest|farm|steal|abyss"
            )
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True

    async def _handle_lingxiao_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        action = (payload or "").strip().lower()
        command_text = None
        if action == "status":
            command_text = ".天阶状态"
        elif action == "mind":
            command_text = ".问心台"
        elif action == "step":
            command_text = ".登天阶"
        elif action == "wind":
            command_text = ".引九天罡风"
        elif action == "gate":
            command_text = ".借天门势"
        elif action == "overview":
            command_text = ".凌霄宫"
        if not command_text:
            await context.reply("用法: .sect lx overview|status|mind|step|wind|gate")
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True

    async def _handle_xingong_command(
        self, context: EventContext, db: SQLiteCompatDb, payload: str
    ) -> bool:
        chat_id = context.chat_id
        if chat_id is None:
            return False
        parts = payload.split(maxsplit=2)
        if not parts:
            await context.reply(
                "用法: .sect xg matrix|assist|starboard|pull 编号 星辰|collect 编号|soothe 编号|divine|shift @目标|companion"
            )
            return True
        action = parts[0].lower()
        command_text = None
        if action == "matrix":
            command_text = ".启阵"
        elif action == "assist":
            command_text = ".助阵"
        elif action == "starboard":
            command_text = ".观星台"
        elif action == "pull" and len(parts) >= 3:
            command_text = f".牵引星辰 {parts[1]} {parts[2]}"
        elif action == "collect" and len(parts) >= 2:
            command_text = f".收集精华 {parts[1]}"
        elif action == "soothe" and len(parts) >= 2:
            command_text = f".安抚星辰 {parts[1]}"
        elif action == "divine":
            command_text = ".观星"
        elif action == "shift" and len(parts) >= 2:
            command_text = f".改换星移 {parts[1]}"
        elif action == "companion":
            command_text = ".我的侍妾"
        if not command_text:
            await context.reply(
                "用法: .sect xg matrix|assist|starboard|pull 编号 星辰|collect 编号|soothe 编号|divine|shift @目标|companion"
            )
            return True
        _ok, status, _msg_id = await sect_game.maybe_send_check(
            context.client,
            db,
            chat_id,
            force=True,
            command_text=command_text,
            profile_id=context.profile.id if context.profile else None,
        )
        await context.reply(f"执行结果: {status}")
        return True
