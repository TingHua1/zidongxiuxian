ACTION_KEYWORDS = {
    "estate_open": ["开辟洞府", "创建洞府"],
    "estate_status": ["洞府", "灵脉", "静室"],
    "estate_spirit": ["升级灵脉"],
    "estate_room": ["升级静室"],
    "estate_paint": ["洞天绘卷", "布置景观"],
    "estate_guest": ["查看访客", "接待访客", "驱逐访客"],
    "estate_visit": ["拜访洞府", "洞府留言", "查看留言"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "提升"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到洞府消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"洞府动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("estate_", ""),
            }
    return None
