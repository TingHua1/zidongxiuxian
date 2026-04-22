ACTION_KEYWORDS = {
    "market_gift": ["赠送"],
    "market_stall": ["上架", "万宝楼", "购买", "我的货摊", "下架"],
    "market_auction": ["拍卖", "拍卖行", "竞拍", "撤销拍卖"],
    "market_bounty": ["发布悬赏", "悬赏榜", "接单", "放弃任务", "撤销悬赏"],
    "market_bet": ["对赌", "神识对决", "应战", "凝神", "固元"],
    "market_gamble": ["六道轮回盘", "卜卦", "赌石", "押"],
}

SUCCESS_KEYWORDS = ["成功", "获得", "完成", "已", "成交"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到市集消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"市集动作成功: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("market_", ""),
            }
    return None
