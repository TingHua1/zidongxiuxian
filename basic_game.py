import re


ACTION_KEYWORDS = {
    "basic_root_check": ["检测灵根", "生成灵根", "道号"],
    "basic_profile": ["我的灵根", "角色面板", "境界"],
    "basic_sect_list": ["宗门列表", "可加入宗门"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "创建", "已", "显示"]

DISPLAY_NAME_PATTERN = re.compile(r"@?[^\n]+的大命玉璞")
ARTIFACT_PATTERN = re.compile(
    r"御使法宝[:：]\s*(?P<value>(?:[^\n]+(?:\n\s*[-*]\s*[^\n]+)*))"
)
SECT_PATTERN = re.compile(r"宗门[:：]\s*(?P<value>[^\n]+)")
ROOT_PATTERN = re.compile(r"灵根[:：]\s*(?P<value>[^\n]+)")
STAGE_PATTERN = re.compile(r"(?:当前境界|境界)[:：]\s*(?P<value>[^\n]+)")
CULTIVATION_PATTERN = re.compile(r"修为[:：]\s*(?P<value>[^\n]+)")
POISON_PATTERN = re.compile(r"丹毒[:：]\s*(?P<value>[^\n]+)")
KILL_PATTERN = re.compile(r"杀戮[:：]\s*(?P<value>[^\n]+)")
GAME_NAME_PATTERN = re.compile(r"@?(?P<value>[^(@\n]+)的大命玉璞")
ACCOUNT_PATTERN = re.compile(r"道友\s*(?P<value>@[^\s\n]+)")


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    if "正在推演天机" in text or "锁定道友神魂" in text:
        return {
            "event": "basic_profile_pending",
            "summary": "人物信息推演中，等待机器人完成编辑",
            "feature_name": "profile",
        }
    display_name_match = DISPLAY_NAME_PATTERN.search(text)
    if display_name_match or "御使法宝" in text or "灵根:" in text or "修为:" in text:
        return {
            "event": "basic_profile",
            "summary": "收到人物信息面板",
            "feature_name": "profile",
            "display_name": display_name_match.group(0).strip()
            if display_name_match
            else "",
            "game_name": (
                GAME_NAME_PATTERN.search(text).group("value").strip()
                if GAME_NAME_PATTERN.search(text)
                else ""
            ),
            "account_name": (
                ACCOUNT_PATTERN.search(text).group("value").strip()
                if ACCOUNT_PATTERN.search(text)
                else ""
            ),
            "artifact_text": (
                ARTIFACT_PATTERN.search(text).group("value").strip()
                if ARTIFACT_PATTERN.search(text)
                else ""
            ),
            "sect_name": (
                SECT_PATTERN.search(text).group("value").strip()
                if SECT_PATTERN.search(text)
                else ""
            ),
            "spirit_root": (
                ROOT_PATTERN.search(text).group("value").strip()
                if ROOT_PATTERN.search(text)
                else ""
            ),
            "stage_name": (
                STAGE_PATTERN.search(text).group("value").strip()
                if STAGE_PATTERN.search(text)
                else ""
            ),
            "cultivation_text": (
                CULTIVATION_PATTERN.search(text).group("value").strip()
                if CULTIVATION_PATTERN.search(text)
                else ""
            ),
            "poison_text": (
                POISON_PATTERN.search(text).group("value").strip()
                if POISON_PATTERN.search(text)
                else ""
            ),
            "kill_count_text": (
                KILL_PATTERN.search(text).group("value").strip()
                if KILL_PATTERN.search(text)
                else ""
            ),
        }
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到基础角色消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"基础角色动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("basic_", ""),
            }
    return None
