import json
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from typing import Tuple


ASC_BASE_URL = "https://asc.aiopenai.app"
_DASHBOARD_API_TOKEN_MARKER = "window.DASHBOARD_API_TOKEN = "


class AscAuthError(RuntimeError):
    pass


def _extract_session_cookie(headers) -> str:
    if not headers:
        return ""
    cookie_headers = []
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        cookie_headers.extend(get_all("Set-Cookie") or [])
    single_header = headers.get("Set-Cookie") if hasattr(headers, "get") else None
    if single_header:
        cookie_headers.append(single_header)
    for header in cookie_headers:
        text = str(header or "").strip()
        if not text:
            continue
        for part in text.split(";"):
            candidate = part.strip()
            if candidate.lower().startswith("session="):
                return candidate
    return ""


def _build_headers(cookie_text: str) -> dict:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    if (cookie_text or "").strip():
        headers["Cookie"] = cookie_text.strip()
    return headers


def _build_html_headers(cookie_text: str) -> dict:
    headers = {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": "Mozilla/5.0",
    }
    if (cookie_text or "").strip():
        headers["Cookie"] = cookie_text.strip()
    return headers


def _extract_dashboard_api_token(html_text: str) -> str:
    text = str(html_text or "")
    marker_index = text.find(_DASHBOARD_API_TOKEN_MARKER)
    if marker_index < 0:
        return ""
    value_start = marker_index + len(_DASHBOARD_API_TOKEN_MARKER)
    if value_start >= len(text):
        return ""
    quote = text[value_start]
    if quote not in {'"', "'"}:
        return ""
    value_end = text.find(quote, value_start + 1)
    if value_end < 0:
        return ""
    return text[value_start + 1 : value_end].strip()


def _resolve_session_cookie(primary_cookie: str, fallback_cookie: str = "") -> str:
    primary = str(primary_cookie or "").strip()
    fallback = str(fallback_cookie or "").strip()
    if primary.startswith("session="):
        return primary
    return fallback


def _bootstrap_dashboard_auth(cookie_text: str) -> Tuple[str, str]:
    request = urllib.request.Request(
        ASC_BASE_URL,
        headers=_build_html_headers(cookie_text),
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            html_text = response.read().decode("utf-8", errors="ignore")
            refreshed_cookie = _extract_session_cookie(
                getattr(response, "headers", None)
            )
            return (
                _resolve_session_cookie(refreshed_cookie, cookie_text),
                _extract_dashboard_api_token(html_text),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body or f"HTTP {exc.code}"}
        if exc.code in {401, 403}:
            raise AscAuthError(payload.get("error") or f"HTTP {exc.code}") from exc
        raise RuntimeError(payload.get("error") or f"HTTP {exc.code}") from exc


def _api_headers(cookie_text: str, api_token: str = "") -> dict:
    headers = _build_headers(cookie_text)
    headers["Accept"] = "application/json,text/plain,*/*"
    headers["Referer"] = ASC_BASE_URL + "/"
    if api_token:
        headers["X-API-Token"] = api_token
    return headers


def _perform_json_get(path: str, cookie_text: str) -> Tuple[dict, int, str]:
    boot_cookie, api_token = _bootstrap_dashboard_auth(cookie_text)
    request = urllib.request.Request(
        f"{ASC_BASE_URL}{path}",
        headers=_api_headers(boot_cookie, api_token),
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            status = getattr(response, "status", HTTPStatus.OK)
            payload = json.loads(response.read().decode("utf-8"))
            refreshed_cookie = _extract_session_cookie(
                getattr(response, "headers", None)
            )
            return (
                payload,
                int(status),
                _resolve_session_cookie(refreshed_cookie, boot_cookie),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body or f"HTTP {exc.code}"}
        if exc.code in {401, 403}:
            raise AscAuthError(payload.get("error") or f"HTTP {exc.code}") from exc
        raise RuntimeError(payload.get("error") or f"HTTP {exc.code}") from exc


def get_cultivator(username: str, cookie_text: str) -> Tuple[dict, int, str]:
    return _perform_json_get(
        f"/api/cultivator/{(username or '').strip()}",
        cookie_text,
    )


def get_all_items(cookie_text: str) -> Tuple[dict, int]:
    payload, status, _cookie = _perform_json_get("/api/all_items", cookie_text)
    return payload, status


def get_shop_items(cookie_text: str) -> Tuple[dict, int]:
    payload, status, _cookie = _perform_json_get("/api/shop_items", cookie_text)
    return payload, status


def get_marketplace_listings_page(
    cookie_text: str, page: int = 1, search: str = ""
) -> Tuple[dict, int]:
    query = urllib.parse.urlencode(
        {
            "page": max(int(page or 1), 1),
            "search": str(search or ""),
        }
    )
    payload, status, _cookie = _perform_json_get(
        f"/api/marketplace_listings?{query}", cookie_text
    )
    return payload, status


def get_all_marketplace_listings(cookie_text: str, search: str = "") -> list[dict]:
    listings = []
    page = 1
    total_count = None
    while True:
        payload, _status = get_marketplace_listings_page(
            cookie_text, page=page, search=search
        )
        page_items = payload.get("listings") or []
        if not isinstance(page_items, list):
            break
        listings.extend(item for item in page_items if isinstance(item, dict))
        total_count = int(payload.get("total_count") or len(listings))
        page_size = max(int(payload.get("page_size") or len(page_items) or 1), 1)
        if (
            not page_items
            or len(listings) >= total_count
            or len(page_items) < page_size
        ):
            break
        page += 1
    return listings
