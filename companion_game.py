ACTION_KEYWORDS = {
    "companion_place": ["安置侍妾", "召回侍妾"],
    "companion_search": ["红尘寻缘", "寻缘"],
    "companion_dream": ["入梦"],
    "companion_marry": ["宗门赐婚"],
    "companion_visit": ["拜见仙子", "敕令仙子", "携手同游"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "提升"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到侍妾消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"侍妾动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("companion_", ""),
            }
    return None
