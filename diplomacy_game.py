ACTION_KEYWORDS = {
    "diplomacy_map": ["宗门外交", "天下大势"],
    "diplomacy_favor": ["示好"],
    "diplomacy_enemy": ["敌对"],
    "diplomacy_ally": ["结盟"],
    "diplomacy_remove": ["解除"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "建立"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到外交消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"外交动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("diplomacy_", ""),
            }
    return None
