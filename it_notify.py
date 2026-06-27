"""WhatsApp notifications for IT portal assign (Interakt API)."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore[misc, assignment]

from vehicle_notify import (
    _api_key,
    _phone_to_10,
    _post_message,
    ensure_customer,
    send_template,
    sentence_case_name,
    wa_id_to_phone,
)

logger = logging.getLogger(__name__)


def _send_template_with_image_header(
    phone: str,
    template_name: str,
    image_url: str,
    *,
    language_code: str = "en",
    body_values: list[str] | None = None,
    callback_data: str = "",
    contact_name: str = "",
) -> None:
    if not _api_key():
        logger.warning("INTERAKT_API_KEY not set — skip image template to %s", phone)
        return
    ensure_customer(phone, name=contact_name or "Contact")
    link = (image_url or "").strip()
    if not link.lower().startswith("https://"):
        raise ValueError("image_url must be an https URL")
    template: dict[str, Any] = {
        "name": template_name.strip(),
        "languageCode": (language_code or "en").strip(),
        "headerValues": [link[:2048]],
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


def _format_ist(val) -> str:
    if val is None:
        return ""
    try:
        if hasattr(val, "timestamp"):
            dt = val
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if ZoneInfo is not None:
                local = dt.astimezone(ZoneInfo("Asia/Kolkata"))
            else:
                local = dt
            return local.strftime("%d-%b-%Y %I:%M %p")
    except Exception:
        pass
    return ""


def _engineer_template_name(rd: dict) -> str:
    photo = (rd.get("issue_photo_url") or "").strip()
    if photo.lower().startswith("https://"):
        return (
            os.getenv("IT_ENGINEER_WITH_PHOTO_TEMPLATE_NAME")
            or os.getenv("IT_ENGINEER_ASSIGN_TEMPLATE_NAME")
            or "it_ticket_with_photo_v01"
        ).strip()
    return (
        os.getenv("IT_ENGINEER_NO_PHOTO_TEMPLATE_NAME")
        or os.getenv("IT_ENGINEER_ASSIGN_BODY_TEMPLATE_NAME")
        or "it_ticket_no_photo_v01"
    ).strip()


def _engineer_template_language() -> str:
    return (os.getenv("IT_ENGINEER_ASSIGN_TEMPLATE_LANGUAGE_CODE") or "en").strip()


def _engineer_template_body_fields() -> list[str]:
    raw = (
        os.getenv("IT_ENGINEER_ASSIGN_TEMPLATE_BODY_FIELDS")
        or "employee,department,category,machine,issue,description,priority,requested_at"
    ).strip()
    return [k.strip().lower() for k in raw.split(",") if k.strip()]


def _engineer_template_body_values(rd: dict) -> list[str]:
    values = {
        "employee": sentence_case_name(rd.get("employee_name") or "Employee"),
        "department": (rd.get("department") or "—").strip(),
        "category": (rd.get("it_category_label") or "—").strip(),
        "machine": (rd.get("machine_no_label") or "—").strip() or "—",
        "issue": (rd.get("issue_type_label") or "—").strip(),
        "description": (rd.get("description") or "—").strip() or "—",
        "priority": (rd.get("priority_label") or "—").strip(),
        "requested_at": _format_ist(rd.get("requested_datetime")) or "—",
    }
    fields = _engineer_template_body_fields()
    return [values.get(key, "—")[:1024] for key in fields]


def notify_it_engineer_assigned(
    rd: dict,
    *,
    request_id: str,
    engineer_wa: str,
) -> None:
    """Notify engineer via it_ticket_with_photo_v01 / it_ticket_no_photo_v01."""
    wa = (engineer_wa or "").strip()
    if not wa:
        logger.warning("IT engineer notify skipped — no wa request_id=%s", request_id)
        return

    template_name = _engineer_template_name(rd)
    if not template_name:
        logger.warning("IT engineer template not configured request_id=%s", request_id)
        return

    phone = wa_id_to_phone(wa)
    display_name = sentence_case_name(rd.get("assigned_engineer_name") or "IT Engineer")
    body_values = _engineer_template_body_values(rd)
    photo_url = (rd.get("issue_photo_url") or "").strip()
    rid = (request_id or "").strip()

    try:
        if photo_url.lower().startswith("https://"):
            _send_template_with_image_header(
                phone,
                template_name,
                photo_url,
                language_code=_engineer_template_language(),
                body_values=body_values,
                callback_data=rid[:512],
                contact_name=display_name,
            )
        else:
            send_template(
                phone,
                template_name,
                language_code=_engineer_template_language(),
                body_values=body_values,
                callback_data=rid[:512],
                contact_name=display_name,
            )
        logger.info(
            "IT engineer template sent wa=%s request_id=%s template=%s photo=%s",
            wa,
            request_id,
            template_name,
            bool(photo_url),
        )
    except Exception:
        logger.exception(
            "IT engineer template failed wa=%s request_id=%s template=%s",
            wa,
            request_id,
            template_name,
        )
