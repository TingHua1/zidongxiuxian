import re


ACTION_KEYWORDS = {
    "artifact_status": ["状态", "法宝耐久", "耐久"],
    "artifact_repair": ["修理", "一键修理"],
    "artifact_awaken": ["唤醒器灵", "器灵"],
    "artifact_touch": ["抚摸法宝", "器灵经验"],
    "artifact_spirit": ["我的器灵", "器灵信息"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "提升"]

ARTIFACT_PATTERN = re.compile(r"御使法宝[:：]\s*(?P<value>[^\n]+)")
STAGE_PATTERN = re.compile(r"当前境界[:：]\s*(?P<value>[^\n]+)")
PROGRESS_PATTERN = re.compile(r"当前修为[:：]\s*(?P<value>[^\n]+)")
STATUS_HEADER_PATTERN = re.compile(r"【修士状态\s*·\s*@(?P<value>[^】\n]+)】")
STATUS_ARTIFACT_PATTERN = re.compile(
    r"本命法宝耐久[:：]\s*(?P<value>(?:\n-\s*[^\n]+)+)", re.MULTILINE
)


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    if "御使法宝" in text or "当前境界" in text or "当前修为" in text:
        artifact_text = (
            ARTIFACT_PATTERN.search(text).group("value").strip()
            if ARTIFACT_PATTERN.search(text)
            else ""
        )
        stage_text = (
            STAGE_PATTERN.search(text).group("value").strip()
            if STAGE_PATTERN.search(text)
            else ""
        )
        progress_text = (
            PROGRESS_PATTERN.search(text).group("value").strip()
            if PROGRESS_PATTERN.search(text)
            else ""
        )
        return {
            "event": "artifact_status_profile",
            "summary": "收到状态面板",
            "feature_name": "status",
            "artifact_text": artifact_text,
            "stage_name": stage_text,
            "cultivation_text": progress_text,
        }
    if "【修士状态" in text and "本命法宝耐久" in text:
        header_match = STATUS_HEADER_PATTERN.search(text)
        artifact_block_match = STATUS_ARTIFACT_PATTERN.search(text)
        artifact_lines = []
        if artifact_block_match:
            artifact_lines = [
                line.strip()
                for line in artifact_block_match.group("value").splitlines()
                if line.strip()
            ]
        stage_text = (
            re.search(r"境界[:：]\s*(?P<value>[^\n]+)", text).group("value").strip()
            if re.search(r"境界[:：]\s*(?P<value>[^\n]+)", text)
            else ""
        )
        return {
            "event": "artifact_status_profile",
            "summary": "收到状态面板",
            "feature_name": "status",
            "artifact_text": "\n".join(artifact_lines),
            "stage_name": stage_text,
            "telegram_username": (
                header_match.group("value").strip() if header_match else ""
            ),
        }
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到法宝消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"法宝动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("artifact_", ""),
            }
    return None
