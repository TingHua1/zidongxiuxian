import re
import time
from typing import Optional

from tg_game.storage import Storage


def _parse_float_text(value) -> float:
    text = str(value or "").replace(",", "").replace("灵石", "").replace("股", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else 0.0


def _normalize_command_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _is_stock_snapshot_command(value: object) -> bool:
    normalized = _normalize_command_text(value)
    if normalized in {".股市", ".大盘"}:
        return True
    return normalized.startswith(".个股 ")


def should_sync_stock_market_message(storage: Storage, message: dict) -> bool:
    if not bool(message.get("is_bot")):
        return False
    if not str(message.get("text") or "").strip():
        return False
    if not extract_stock_snapshot_entries(message.get("text") or ""):
        return False

    chat_id = int(message.get("chat_id") or 0)
    reply_to_msg_id = int(message.get("reply_to_msg_id") or 0)
    if not chat_id or not reply_to_msg_id:
        return False

    reply_message = storage.get_bound_message(chat_id, reply_to_msg_id) or {}
    return _is_stock_snapshot_command(reply_message.get("text") or "")


def _extract_stock_board_entries(text: str) -> list[dict]:
    lines = [line.strip() for line in str(text or "").splitlines()]
    price_pattern = re.compile(
        r"^(?P<price>-?\d+(?:\.\d+)?)\s*\|\s*(?P<change>[+-]?\d+(?:\.\d+)?)%\s*(?P<tail>.*)$"
    )
    entries = []
    index = 0
    while index < len(lines):
        line = lines[index]
        header_match = re.match(r"^(?P<code>IDX_[A-Z0-9_]+)\s+(?P<name>.+)$", line)
        if not header_match:
            index += 1
            continue

        price_line = lines[index + 1] if index + 1 < len(lines) else ""
        price_match = price_pattern.match(price_line)
        if not price_match:
            index += 1
            continue

        raw_name = header_match.group("name").strip()
        tokens = raw_name.split()
        trailing_tokens = []
        while tokens and not re.search(r"[0-9A-Za-z\u4e00-\u9fff]", tokens[-1]):
            trailing_tokens.insert(0, tokens.pop())
        direction_emoji = " ".join(trailing_tokens).strip()
        if not direction_emoji:
            direction_emoji = str(price_match.group("tail") or "").strip()
            if direction_emoji.startswith("("):
                direction_emoji = ""

        entry = {
            "stock_code": header_match.group("code").strip(),
            "stock_name": (" ".join(tokens).strip() or raw_name),
            "current_price": float(price_match.group("price") or 0),
            "change_percent": float(price_match.group("change") or 0),
            "direction_emoji": direction_emoji,
        }
        detail_line = lines[index + 2] if index + 2 < len(lines) else ""
        detail_parts = [part.strip() for part in detail_line.split("/") if part.strip()]
        if len(detail_parts) >= 4:
            entry["sector"] = detail_parts[0]
            entry["trend"] = detail_parts[1]
            entry["heat"] = detail_parts[2]
            entry["liquidity"] = detail_parts[3]
            index += 3
        else:
            index += 2
        entries.append(entry)
    return entries


def _parse_stock_quote_message(text: str) -> Optional[dict]:
    raw_text = str(text or "").strip()
    title_match = re.search(
        r"📊\s*(?P<name>.+?)\s*\((?P<code>IDX_[A-Z0-9_]+)\)", raw_text
    )
    if not title_match:
        return None
    entry = {
        "stock_name": title_match.group("name").strip(),
        "stock_code": title_match.group("code").strip(),
    }
    patterns = {
        "sector": r"赛道:\s*([^\s]+)",
        "trend": r"风向:\s*([^\s]+)",
        "heat": r"热度:\s*([^\s]+)",
        "crowding": r"拥挤度:\s*([^\s]+)",
        "volatility": r"波动:\s*([^\s]+)",
        "liquidity": r"流动性:\s*([^\s]+)",
        "pattern": r"形态:\s*([^\s]+)",
        "volume_trend": r"量能:\s*([^\s]+)",
        "position_text": r"位置:\s*([^\s]+)",
        "strategy": r"策略:\s*(.+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_text)
        if match:
            entry[key] = match.group(1).strip()
    score_match = re.search(r"盘感分:\s*(\d+)\/100", raw_text)
    if score_match:
        entry["score"] = int(score_match.group(1))
    price_match = re.search(
        r"现价:\s*(-?\d+(?:\.\d+)?)\s*\(([-+]?\d+(?:\.\d+)?)\s*/\s*([-+]?\d+(?:\.\d+)?)%\)\s*(\S+)?",
        raw_text,
    )
    if price_match:
        entry["current_price"] = float(price_match.group(1) or 0)
        entry["change_amount"] = float(price_match.group(2) or 0)
        entry["change_percent"] = float(price_match.group(3) or 0)
        entry["direction_emoji"] = str(price_match.group(4) or "").strip()
    for key, pattern in {
        "open_price": r"今开:\s*(-?\d+(?:\.\d+)?)",
        "prev_close": r"昨收:\s*(-?\d+(?:\.\d+)?)",
        "high_price": r"最高:\s*(-?\d+(?:\.\d+)?)",
        "low_price": r"最低:\s*(-?\d+(?:\.\d+)?)",
        "volume": r"成交量:\s*(-?\d+(?:\.\d+)?)",
        "turnover": r"成交额:\s*(-?\d+(?:\.\d+)?)",
    }.items():
        match = re.search(pattern, raw_text)
        if match:
            entry[key] = _parse_float_text(match.group(1))
    return entry


def extract_stock_snapshot_entries(text: str) -> list[dict]:
    raw_text = str(text or "").strip()
    if not raw_text:
        return []
    entries = []
    if "IDX_" in raw_text and (
        "实时行情" in raw_text or "虚实交汇" in raw_text or "天道股市" in raw_text
    ):
        entries.extend(_extract_stock_board_entries(raw_text))
    quote_entry = _parse_stock_quote_message(raw_text)
    if quote_entry:
        entries.append(quote_entry)
    return [entry for entry in entries if entry.get("stock_code")]


def sync_stock_market_message(
    storage: Storage,
    message: dict,
    *,
    update_history: bool = True,
    update_info: bool = True,
) -> int:
    if not should_sync_stock_market_message(storage, message):
        return 0
    text = str(message.get("text") or "").strip()
    entries = extract_stock_snapshot_entries(text)
    if not entries:
        return 0

    profile_id = message.get("profile_id")
    chat_id = int(message.get("chat_id") or 0)
    message_id = int(message.get("message_id") or 0)
    observed_at = float(message.get("created_at") or 0) or time.time()

    for entry in entries:
        stock_code = entry.get("stock_code") or ""
        payload = {key: value for key, value in entry.items() if key != "stock_code"}
        if update_history:
            storage.upsert_stock_market_history(
                profile_id,
                chat_id,
                message_id,
                stock_code,
                **payload,
                raw_text=text,
                observed_at=observed_at,
            )
        if update_info and profile_id:
            storage.upsert_stock_market_info(
                int(profile_id),
                stock_code,
                **payload,
                source_message_id=message_id,
                raw_text=text,
            )
    return len(entries)
