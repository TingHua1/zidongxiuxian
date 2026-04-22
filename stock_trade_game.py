ACTION_KEYWORDS = {
    "stock_board": ["股市", "大盘", "个股"],
    "stock_buy": ["买入", "融资买入"],
    "stock_sell": ["卖出", "融资平仓"],
    "stock_position": ["我的持仓", "持仓盈亏"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "成交"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到股市消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"股市动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("stock_", ""),
            }
    return None
