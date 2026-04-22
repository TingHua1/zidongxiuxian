ACTION_KEYWORDS = {
    "dungeon_open": ["开启虚天殿", "开启昆吾山", "创建副本房间"],
    "dungeon_join": ["加入副本", "加入昆吾山"],
    "dungeon_enter": ["进入虚天殿", "进入昆吾山"],
    "dungeon_route": ["选择道路", "冰", "火"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "进入"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到副本消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"副本动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("dungeon_", ""),
            }
    return None
