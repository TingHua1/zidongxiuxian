ACTION_KEYWORDS = {
    "breakthrough_jiedan": ["冲击结丹", "结丹"],
    "breakthrough_yuanying": ["冲击元婴", "元婴"],
    "breakthrough_body": ["五行淬体", "淬体"],
    "breakthrough_huashen": ["冲击化神", "化神"],
    "breakthrough_trace": ["天机回溯", "回溯"],
}

SUCCESS_KEYWORDS = ["成功", "失败", "获得", "完成", "已"]


def parse_message(text):
    text = (text or "").strip()
    if not text:
        return None
    for event_name, keywords in ACTION_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            summary = f"收到突破消息: {event_name}"
            if any(keyword in text for keyword in SUCCESS_KEYWORDS):
                summary = f"突破结果记录: {event_name}"
            return {
                "event": event_name,
                "summary": summary,
                "feature_name": event_name.replace("breakthrough_", ""),
            }
    return None
