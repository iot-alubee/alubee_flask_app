"""WhatsApp notifications for logistics portal (Interakt API — same as Interakt bot)."""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_PORTAL_CONFIG_LOADED = False


def _bootstrap_portal_config() -> None:
    """Load portal_config.env (non-secrets) — Flask does not use Interakt/bot_config.env."""
    global _PORTAL_CONFIG_LOADED
    if _PORTAL_CONFIG_LOADED:
        return
    _PORTAL_CONFIG_LOADED = True
    path = Path(__file__).resolve().parent / "portal_config.env"
    if not path.is_file():
        return
    applied = 0
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key == "INTERAKT_API_KEY":
            continue
        if not os.environ.get(key):
            os.environ[key] = value.strip()
            applied += 1
    if applied:
        logger.info("Loaded portal_config.env (%s keys)", applied)


_bootstrap_portal_config()

INTERAKT_MESSAGE_URL = "https://api.interakt.ai/v1/public/message/"
INTERAKT_TRACK_USERS_URL = "https://api.interakt.ai/v1/public/track/users/"

_VEHICLE_ASSIGNEE_BODY_FIELDS_DEFAULT = (
    "assignee_name,requester,from,request_type,category,destination,vehicle,time"
)
_VEHICLE_ASSIGNEE_BODY_FIELD_COUNT = 8

_ROMAN_NUMERAL_CHARS = frozenset("IVXLCDM")


def _phone_to_10(phone: str) -> str:
    return "".join(c for c in (phone or "") if c.isdigit())[-10:]


def wa_id_to_phone(wa_id: str) -> str:
    return _phone_to_10((wa_id or "").replace("whatsapp:", ""))


def _api_key() -> str:
    return (os.getenv("INTERAKT_API_KEY") or "").strip()


def _require_api_key() -> str:
    key = _api_key()
    if not key:
        raise ValueError("INTERAKT_API_KEY is not set")
    return key


def _env_enabled(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or ("true" if default else "false")).strip().lower()
    return raw in ("1", "true", "yes", "on")


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
    _require_api_key()
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


def _template_body_value_text(value: object) -> str:
    text = str(value).strip() if value is not None else ""
    return (text[:1024] if text else "-")


def send_template(
    phone: str,
    template_name: str,
    *,
    language_code: str = "en",
    body_values: list[str] | None = None,
    callback_data: str = "",
    contact_name: str = "",
) -> None:
    _require_api_key()
    ensure_customer(phone, name=contact_name or "Contact")
    template: dict[str, Any] = {
        "name": template_name.strip(),
        "languageCode": (language_code or "en").strip(),
    }
    if body_values is not None:
        template["bodyValues"] = [_template_body_value_text(v) for v in body_values]
    payload: dict[str, Any] = {
        "countryCode": "+91",
        "phoneNumber": _phone_to_10(phone),
        "type": "Template",
        "template": template,
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


def send_guest_visit_otp(
    phone: str,
    *,
    guest_name: str,
    otp: str,
    organization: str = "",
) -> bool:
    """WhatsApp Authentication template with OTP for visitor guests."""
    template_name = (os.getenv("VISITOR_OTP_TEMPLATE_NAME") or "visitor_pass_code").strip()
    if not template_name:
        logger.warning("VISITOR_OTP_TEMPLATE_NAME not set — skip guest OTP")
        return False
    phone10 = _phone_to_10(phone)
    if not phone10:
        return False
    if not ensure_customer(phone10, name=(guest_name or "Guest")[:50]):
        return False
    otp_code = str(otp or "").strip()[:15]
    if not otp_code:
        return False
    lang = (os.getenv("VISITOR_OTP_TEMPLATE_LANGUAGE_CODE") or "en").strip()
    try:
        send_template(
            phone10,
            template_name,
            language_code=lang,
            body_values=[otp_code],
            contact_name=(guest_name or "Guest")[:50],
        )
        return True
    except Exception:
        logger.exception("guest visit OTP template failed phone=%s", phone10)
        return False


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


def _normalize_assignee_code(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def _logistics_department_name() -> str:
    return (
        os.getenv("VEHICLE_INTERNAL_ASSIGN_DEPARTMENT")
        or os.getenv("LOGISTICS_DEPARTMENT_NAME")
        or "LOGISTICS"
    ).strip().upper()


def staff_wa_for_assignee_code(db, assignee_code: str) -> str:
    """Resolve staff WhatsApp id from employee_id (Firestore users doc id)."""
    code = _normalize_assignee_code(assignee_code)
    if not code or db is None:
        return ""
    raw = (assignee_code or "").strip()
    candidates: list[str] = []
    for value in (raw, raw.upper(), code):
        if value and value not in candidates:
            candidates.append(value)
    try:
        for emp_id in candidates:
            for snap in (
                db.collection("users")
                .where("employee_id", "==", emp_id)
                .limit(1)
                .stream()
            ):
                wa = (snap.id or "").strip()
                if wa:
                    return wa
        for snap in db.collection("users").stream():
            ud = snap.to_dict() or {}
            emp_id = _normalize_assignee_code(ud.get("employee_id") or "")
            if emp_id == code:
                return (snap.id or "").strip()
    except Exception:
        logger.exception(
            "logistics staff wa lookup failed code=%s", assignee_code
        )
    return ""


def _assignee_template_name() -> str:
    return (
        os.getenv("VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_NAME")
        or os.getenv("VEHICLE_INTERNAL_ASSIGNEE_TEMPLATE_NAME")
        or "vehicle_assignee_v02"
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
        or _VEHICLE_ASSIGNEE_BODY_FIELDS_DEFAULT
    ).strip()
    fields = [k.strip().lower() for k in raw.split(",") if k.strip()]
    default_fields = [
        k.strip().lower()
        for k in _VEHICLE_ASSIGNEE_BODY_FIELDS_DEFAULT.split(",")
        if k.strip()
    ]
    if len(fields) != _VEHICLE_ASSIGNEE_BODY_FIELD_COUNT:
        logger.warning(
            "VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_BODY_FIELDS invalid (got %s fields); "
            "using default %s fields",
            len(fields),
            _VEHICLE_ASSIGNEE_BODY_FIELD_COUNT,
        )
        return default_fields
    return fields


def _assignee_time_label(rd: dict) -> str:
    raw = rd.get("required_at")
    if raw is None or raw == "":
        return "-"
    if isinstance(raw, str):
        return raw.strip() or "-"
    return str(raw).strip() or "-"


def _assignee_template_values(rd: dict, assignee_name: str) -> dict[str, str]:
    vehicle = (
        (rd.get("fleet_vehicle_label") or "").strip()
        or (rd.get("external_vehicle_number") or "").strip()
        or "-"
    )
    return {
        "assignee_name": sentence_case_name(assignee_name or "-"),
        "requester": sentence_case_name(rd.get("employee_name") or "-"),
        "from": (rd.get("from_unit_label") or "").strip() or "-",
        "request_type": (rd.get("request_type_label") or "").strip() or "-",
        "category": (rd.get("destination_category_label") or "").strip() or "-",
        "destination": (rd.get("destination_label") or "").strip() or "-",
        "vehicle": vehicle,
        "time": _assignee_time_label(rd),
    }


def _assignee_template_body_values(rd: dict, assignee_name: str) -> list[str]:
    values = _assignee_template_values(rd, assignee_name)
    fields = _assignee_template_body_fields()
    body = [_template_body_value_text(values.get(key, "-")) for key in fields]
    if len(body) != _VEHICLE_ASSIGNEE_BODY_FIELD_COUNT:
        raise ValueError(
            f"vehicle_assignee_v02 requires {_VEHICLE_ASSIGNEE_BODY_FIELD_COUNT} body values, got {len(body)}"
        )
    return body


def notify_vehicle_assignee(
    db,
    rd: dict,
    *,
    request_id: str,
    assignee_code: str,
    assignee_label: str,
    assignee_wa: str = "",
) -> None:
    """Notify internal assignee via WhatsApp template only (vehicle_assignee_v02)."""
    if _normalize_vehicle_type(rd.get("vehicle_type") or "") != "in_house":
        logger.info(
            "vehicle assignee notify skipped — not internal request_id=%s type=%s",
            request_id,
            rd.get("vehicle_type"),
        )
        return

    wa = (assignee_wa or rd.get("assigned_to_wa") or "").strip()
    if not wa and assignee_code:
        wa = staff_wa_for_assignee_code(db, assignee_code)
    if not wa:
        raise ValueError(
            f"No WhatsApp id for assignee {assignee_label or assignee_code or 'unknown'}"
        )

    if not _api_key():
        raise ValueError("INTERAKT_API_KEY is not set on the portal")

    display_name = sentence_case_name(assignee_label or "Assignee")
    phone = wa_id_to_phone(wa)
    if not phone:
        raise ValueError(f"Invalid WhatsApp id for assignee {display_name}")

    rid = (request_id or "").strip()
    template_name = _assignee_template_name()
    if not template_name:
        raise ValueError("VEHICLE_ASSIGNEE_NOTIFY_TEMPLATE_NAME is not configured")

    body_values = _assignee_template_body_values(rd, display_name)
    logger.info(
        "vehicle assignee template prepare request_id=%s template=%s body_count=%s",
        request_id,
        template_name,
        len(body_values),
    )
    if not ensure_customer(phone, name=display_name):
        logger.warning(
            "vehicle assignee track user failed phone=%s request_id=%s",
            phone,
            request_id,
        )

    send_template(
        phone,
        template_name,
        language_code=_assignee_template_language(),
        body_values=body_values,
        callback_data=rid,
        contact_name=display_name,
    )
    logger.info(
        "vehicle assignee template sent wa=%s request_id=%s template=%s fields=%s",
        wa,
        request_id,
        template_name,
        len(body_values),
    )


def _vehicle_purpose_line(rd: dict) -> str:
    req_type = (rd.get("request_type_label") or "—").strip()
    destination = (rd.get("destination_label") or "—").strip()
    return f"{req_type} - {destination}"


def _vehicle_security_gate_body(
    rd: dict,
    *,
    event: str,
    vehicle_number: str = "",
) -> str:
    """Plain-text body for JMD/MD when security records vehicle OUT (internal) or IN (external)."""
    header = "Vehicle OUT:" if event == "out" else "Vehicle IN:"
    requester = sentence_case_name(rd.get("employee_name") or "—")
    purpose = _vehicle_purpose_line(rd)
    assignee = sentence_case_name(rd.get("assigned_to") or "—")
    if event == "out":
        vehicle_no = (
            (rd.get("fleet_vehicle_label") or "").strip()
            or (vehicle_number or "").strip()
            or "—"
        )
    else:
        vehicle_no = (
            (vehicle_number or "").strip()
            or (rd.get("external_vehicle_number") or "").strip()
            or "—"
        )
    return (
        f"{header}\n\n"
        f"Requester: {requester}\n"
        f"Purpose: {purpose}\n"
        f"Assignee: {assignee}\n"
        f"Vehicle No: {vehicle_no}"
    )


def notify_vehicle_security_gate(
    rd: dict,
    *,
    event: str,
    jmd_wa: str,
    md_wa: str,
    vehicle_number: str = "",
) -> None:
    """Notify unit JMD + MD on internal OUT or external IN (security gate)."""
    if not _env_enabled("ENABLE_JMD_MD_VEHICLE_NOTIFY"):
        logger.info(
            "vehicle security gate JMD/MD notify disabled "
            "(ENABLE_JMD_MD_VEHICLE_NOTIFY) request_id=%s",
            rd.get("request_id") or "—",
        )
        return
    ev = (event or "").strip().lower()
    if ev not in ("out", "in"):
        return
    body = _vehicle_security_gate_body(rd, event=ev, vehicle_number=vehicle_number)
    recipients: list[str] = []
    for wa in (jmd_wa, md_wa):
        wa = (wa or "").strip()
        if not wa:
            continue
        key = wa.lower()
        if key in {r.lower() for r in recipients}:
            continue
        recipients.append(wa)
    if not recipients:
        logger.warning("vehicle security gate notify skipped — no JMD/MD configured")
        return
    for wa in recipients:
        try:
            send_text(wa, body, callback_data=(rd.get("request_id") or "")[:512])
            logger.info(
                "vehicle security gate %s notify sent wa=%s request_id=%s",
                ev,
                wa,
                rd.get("request_id") or "—",
            )
        except Exception:
            logger.exception(
                "vehicle security gate %s notify failed wa=%s request_id=%s",
                ev,
                wa,
                rd.get("request_id") or "—",
            )
