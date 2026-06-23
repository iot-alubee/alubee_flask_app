"""WhatsApp notifications for logistics portal (Interakt API — same as Interakt bot)."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

INTERAKT_MESSAGE_URL = "https://api.interakt.ai/v1/public/message/"
INTERAKT_TRACK_USERS_URL = "https://api.interakt.ai/v1/public/track/users/"

_ROMAN_NUMERAL_CHARS = frozenset("IVXLCDM")


def _phone_to_10(phone: str) -> str:
    return "".join(c for c in (phone or "") if c.isdigit())[-10:]


def wa_id_to_phone(wa_id: str) -> str:
    return _phone_to_10((wa_id or "").replace("whatsapp:", ""))


def _api_key() -> str:
    return (os.getenv("INTERAKT_API_KEY") or "").strip()


def _headers() -> dict[str, str]:
    key = _api_key()
    if not key:
        raise ValueError("INTERAKT_API_KEY is not set")
    return {
        "Authorization": f"Basic {key}",
        "Content-Type": "application/json",
    }


_session: requests.Session | None = None


def _http_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session
    session = requests.Session()
    session.verify = certifi.where()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    _session = session
    return session


def _http_post(url: str, payload: dict[str, Any], *, timeout: int = 30) -> requests.Response:
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            return _http_session().post(
                url, json=payload, headers=_headers(), timeout=timeout
            )
        except Exception as e:
            last_err = e
            logger.warning("Interakt POST attempt %s failed: %s", attempt, e)
            if attempt < 3:
                time.sleep(0.6 * attempt)
    assert last_err is not None
    raise last_err


def _post_message(payload: dict[str, Any]) -> None:
    resp = _http_post(INTERAKT_MESSAGE_URL, payload, timeout=30)
    if resp.status_code >= 400:
        try:
            data = resp.json()
        except Exception:
            data = resp.text[:500]
        raise RuntimeError(f"Interakt API {resp.status_code}: {data}")


def ensure_customer(phone: str, *, name: str = "") -> bool:
    phone10 = _phone_to_10(phone)
    if not phone10:
        return False
    payload = {
        "phoneNumber": phone10,
        "countryCode": "+91",
        "traits": {
            "name": (name or "Contact")[:256],
            "whatsapp_opted_in": True,
        },
        "tags": ["logistics_portal"],
    }
    try:
        resp = _http_post(INTERAKT_TRACK_USERS_URL, payload, timeout=15)
        return resp.status_code < 400
    except Exception:
        logger.exception("Interakt track user failed phone=%s", phone10)
        return False


def send_text(wa_id: str, text: str, *, callback_data: str = "") -> None:
    if not _api_key():
        logger.warning("INTERAKT_API_KEY not set — skip WhatsApp text to %s", wa_id)
        return
    phone = wa_id_to_phone(wa_id)
    payload: dict[str, Any] = {
        "countryCode": "+91",
        "phoneNumber": phone,
        "type": "Text",
        "data": {"message": text},
    }
    if callback_data:
        payload["callbackData"] = callback_data[:512]
    _post_message(payload)


def send_template(
    phone: str,
    template_name: str,
    *,
    language_code: str = "en",
    body_values: list[str] | None = None,
    callback_data: str = "",
    contact_name: str = "",
) -> None:
    if not _api_key():
        logger.warning("INTERAKT_API_KEY not set — skip template to %s", phone)
        return
    ensure_customer(phone, name=contact_name or "Contact")
    template: dict[str, Any] = {
        "name": template_name.strip(),
        "languageCode": (language_code or "en").strip(),
    }
    if body_values:
        template["bodyValues"] = [str(v)[:1024] for v in body_values]
    payload: dict[str, Any] = {
        "countryCode": "+91",
        "phoneNumber": _phone_to_10(phone),
        "type": "Template",
        "template": template,
    }
    if callback_data:
        payload["callbackData"] = callback_data[:512]
    _post_message(payload)


def send_reply_buttons(
    phone: str,
    body_text: str,
    buttons: list[tuple[str, str]],
    *,
    callback_data: str = "",
    contact_name: str = "",
) -> None:
    if not _api_key():
        logger.warning("INTERAKT_API_KEY not set — skip buttons to %s", phone)
        return
    ensure_customer(phone, name=contact_name or "Contact")
    wa_buttons = []
    for btn_id, label in buttons[:3]:
        wa_buttons.append({
            "type": "reply",
            "reply": {"id": str(btn_id)[:256], "title": str(label)[:20]},
        })
    payload: dict[str, Any] = {
        "countryCode": "+91",
        "phoneNumber": _phone_to_10(phone),
        "type": "InteractiveButton",
        "data": {
            "message": {
                "type": "button",
                "body": {"text": body_text},
                "action": {"buttons": wa_buttons},
            },
        },
    }
    if callback_data:
        payload["callbackData"] = callback_data[:512]
    _post_message(payload)


def _sentence_case_word(word: str) -> str:
    if not word:
        return word
    up = word.upper()
    if len(up) <= 8 and all(c in _ROMAN_NUMERAL_CHARS for c in up):
        return up
    return word[:1].upper() + word[1:].lower()


def sentence_case_name(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return raw
    return " ".join(_sentence_case_word(p) for p in raw.split())


def _normalize_vehicle_type(raw: str) -> str:
    key = (raw or "").strip().lower().replace(" ", "_").replace("-", "_")
    if key in ("internal", "in_house", "company_vehicle"):
        return "in_house"
    if key in ("external", "external_hire", "hire", "external_vehicle"):
        return "external_hire"
    return key


def _assignee_template_name() -> str:
    return (
        os.getenv("VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_NAME")
        or os.getenv("VEHICLE_INTERNAL_ASSIGNEE_TEMPLATE_NAME")
        or ""
    ).strip()


def _assignee_template_language() -> str:
    return (
        os.getenv("VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_LANGUAGE_CODE")
        or os.getenv("VEHICLE_INTERNAL_ASSIGNEE_TEMPLATE_LANGUAGE_CODE")
        or "en"
    ).strip()


def _assignee_template_body_fields() -> list[str]:
    raw = (
        os.getenv("VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_BODY_FIELDS")
        or os.getenv("VEHICLE_INTERNAL_ASSIGNEE_TEMPLATE_BODY_FIELDS")
        or "assignee_name,requester,from,request_type,category,destination,time"
    ).strip()
    return [k.strip().lower() for k in raw.split(",") if k.strip()]


def _assignee_template_values(rd: dict, assignee_name: str) -> dict[str, str]:
    return {
        "assignee_name": sentence_case_name(assignee_name or "—"),
        "requester": sentence_case_name(rd.get("employee_name") or "—"),
        "from": rd.get("from_unit_label") or "—",
        "request_type": rd.get("request_type_label") or "—",
        "category": rd.get("destination_category_label") or "—",
        "destination": rd.get("destination_label") or "—",
        "time": rd.get("required_at") or "—",
    }


def _assignee_notify_body(rd: dict, assignee_name: str) -> str:
    v = _assignee_template_values(rd, assignee_name)
    first = (assignee_name or "there").strip().split()[0] if assignee_name else "there"
    return (
        f"Hi {first}, new request has been assigned to you. Please refer below.\n\n"
        f"Requester: {v['requester']}\n"
        f"From: {v['from']}\n"
        f"Request Type: {v['request_type']}\n"
        f"Category: {v['category']}\n"
        f"Destination: {v['destination']}\n"
        f"Time: {v['time']}\n\n"
        "Click 'Start' once you are ready!"
    )


def _assignee_template_body_values(rd: dict, assignee_name: str) -> list[str]:
    values = _assignee_template_values(rd, assignee_name)
    fields = _assignee_template_body_fields()
    return [values.get(key, "—")[:1024] for key in fields]


def _whatsapp_session_hours() -> float:
    try:
        return float(os.getenv("WHATSAPP_SESSION_HOURS") or "24")
    except ValueError:
        return 24.0


def has_active_whatsapp_session(db, wa_id: str) -> bool:
    wa = (wa_id or "").strip()
    if not wa or db is None:
        return False
    try:
        snap = db.collection("whatsapp_activity").document(wa).get()
    except Exception:
        logger.exception("whatsapp_activity lookup failed wa=%s", wa)
        return False
    if not snap.exists:
        return False
    last = (snap.to_dict() or {}).get("last_inbound_at")
    if not last:
        return False
    if hasattr(last, "timestamp"):
        last_dt = datetime.fromtimestamp(last.timestamp(), tz=timezone.utc)
    elif isinstance(last, datetime):
        last_dt = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
    else:
        return False
    age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
    return age_hours < _whatsapp_session_hours()


def notify_vehicle_assignee(
    db,
    rd: dict,
    *,
    request_id: str,
    assignee_code: str,
    assignee_label: str,
) -> None:
    """Notify internal assignee with Start button / template (same as WhatsApp bot)."""
    if _normalize_vehicle_type(rd.get("vehicle_type") or "") != "in_house":
        return

    assignee_wa = (rd.get("assigned_to_wa") or "").strip()
    if not assignee_wa:
        logger.warning(
            "vehicle assignee notify skipped — no wa for code=%s", assignee_code
        )
        return

    display_name = sentence_case_name(assignee_label or "Assignee")
    phone = wa_id_to_phone(assignee_wa)
    rid = (request_id or "").strip()
    start_id = f"VEHICLE_START_{rid}"[:256]
    can_start = rd.get("assignee_can_start") is not False
    body = _assignee_notify_body(rd, display_name)
    template_name = _assignee_template_name()

    if has_active_whatsapp_session(db, assignee_wa):
        try:
            if can_start:
                send_reply_buttons(
                    phone,
                    body,
                    [(start_id, "Start")],
                    callback_data=rid,
                    contact_name=display_name,
                )
            else:
                send_text(assignee_wa, body)
            logger.info(
                "vehicle assignee session message sent wa=%s request_id=%s",
                assignee_wa,
                request_id,
            )
            return
        except Exception:
            logger.exception(
                "vehicle assignee session notify failed wa=%s request_id=%s",
                assignee_wa,
                request_id,
            )

    if template_name:
        try:
            send_template(
                phone,
                template_name,
                language_code=_assignee_template_language(),
                body_values=_assignee_template_body_values(rd, display_name),
                callback_data=rid,
                contact_name=display_name,
            )
            logger.info(
                "vehicle assignee template sent wa=%s request_id=%s",
                assignee_wa,
                request_id,
            )
            return
        except Exception:
            logger.exception(
                "vehicle assignee template failed wa=%s request_id=%s",
                assignee_wa,
                request_id,
            )

    logger.warning(
        "skip vehicle assignee notify wa=%s (set VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_NAME)",
        assignee_wa,
    )
