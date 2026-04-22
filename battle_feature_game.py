import re


ACTION_KEYWORDS = {
    "battle_pvp": ["斗法", "决斗", "夺宝", "仇敌"],
    "battle_rank": ["修为榜", "恶人榜"],
    "battle_test": ["切磋木人", "战力"],
    "battle_tower": ["闯塔", "琉璃塔榜", "重置古塔", "退出古塔"],
}

SUCCESS_KEYWORDS = ["成功", "失败", "获得", "完成", "已"]

GAME_NAME_PATTERN = re.compile(
    r"修士[:：]\s*(?P<game_name>[^(@\n]+?)\s*\((?P<account_name>@[^)\n]+)\)"
)
SECT_PATTERN = re.compile(r"境界[:：]\s*[^\n()]+\((?P<sect_name>[^)\n]+)\)")
STAGE_PATTERN = re.compile(r"境界[:：]\s*(?P<stage_name>[^\n()]+?)\s*\([^)\n]+\)")
CULTIVATION_PATTERN = re.compile(r"基础修为[:：]\s*(?P<value>[0-9.万千亿wW+-]+)")


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    if "战力评估" in text or "综合战力" in text:
        name_match = GAME_NAME_PATTERN.search(text)
        sect_match = SECT_PATTERN.search(text)
        stage_match = STAGE_PATTERN.search(text)
        cultivation_match = CULTIVATION_PATTERN.search(text)
        game_name = name_match.group("game_name").strip() if name_match else ""
        account_name = name_match.group("account_name").strip() if name_match else ""
        sect_name = sect_match.group("sect_name").strip() if sect_match else ""
        stage_name = stage_match.group("stage_name").strip() if stage_match else ""
        cultivation_text = (
            cultivation_match.group("value").strip() if cultivation_match else ""
        )
        summary_parts = ["收到战力面板"]
        if game_name:
            summary_parts.append(f"角色 {game_name}")
        if account_name:
            summary_parts.append(f"账号 {account_name}")
        return {
            "event": "battle_profile",
            "summary": "，".join(summary_parts),
            "feature_name": "test",
            "game_name": game_name,
            "account_name": account_name,
            "sect_name": sect_name,
            "stage_name": stage_name,
            "cultivation_text": cultivation_text,
            "display_name": (
                f"{game_name} ({account_name})"
                if game_name and account_name
                else game_name or account_name
            ),
        }
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到战斗消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"战斗结果记录: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("battle_", ""),
            }
    return None
