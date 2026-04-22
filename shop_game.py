ACTION_KEYWORDS = {
    "shop_open": ["氪金", "小卖部", "充值", "商城"],
}


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            return {
                "event": event_name,
                "summary": f"收到商城消息: {event_name}",
                "feature_name": event_name.replace("shop_", ""),
            }
    return None
