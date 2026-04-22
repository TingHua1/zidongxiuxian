ACTION_KEYWORDS = {
    "inventory_bag": ["储物袋", "全部物品"],
    "inventory_learn": ["学习", "丹方", "图纸"],
    "inventory_make": ["炼制", "炼丹", "炼器"],
    "inventory_divine": ["卜筮问天", "赌运气"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "学会"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到储物消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"储物动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("inventory_", ""),
            }
    return None
