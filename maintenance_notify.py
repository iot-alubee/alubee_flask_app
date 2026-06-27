"""WhatsApp notifications for maintenance portal assign (Interakt API)."""

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
    """Maintenance-only: approved template with issue photo in header."""
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


def _team_notify_template_name() -> str:
    return (
        os.getenv("MAINTENANCE_TEAM_NOTIFY_TEMPLATE_NAME")
        or "maintenance_team_notification_v01"
    ).strip()


def _team_notify_template_language() -> str:
    return (os.getenv("MAINTENANCE_TEAM_NOTIFY_TEMPLATE_LANGUAGE_CODE") or "en").strip()


def _team_notify_template_body_fields() -> list[str]:
    raw = (
        os.getenv("MAINTENANCE_TEAM_NOTIFY_TEMPLATE_BODY_FIELDS")
        or "employee,unit,department,machine,issue,requested_at"
    ).strip()
    return [k.strip().lower() for k in raw.split(",") if k.strip()]


def _unit_label(route: str) -> str:
    r = (route or "").strip().upper()
    if r in ("JMD2", "UNIT_II", "UNIT2", "UNIT-2", "UNIT 2"):
        return "Unit II"
    return "Unit I"


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


def _team_notify_template_body_values(rd: dict) -> list[str]:
    values = {
        "employee": sentence_case_name(rd.get("employee_name") or "Employee"),
        "unit": _unit_label(rd.get("jmd_route") or ""),
        "department": (rd.get("department") or "—").strip(),
        "machine": (rd.get("machine_no_label") or "—").strip(),
        "issue": (rd.get("issue_category_label") or "—").strip(),
        "requested_at": _format_ist(rd.get("requested_datetime")) or "—",
    }
    fields = _team_notify_template_body_fields()
    return [values.get(key, "—")[:1024] for key in fields]


def notify_maintenance_assignee(
    rd: dict,
    *,
    request_id: str,
    assignee_wa: str,
) -> None:
    """Notify technician via maintenance_team_notification_v01 (issue photo in header)."""
    wa = (assignee_wa or "").strip()
    if not wa:
        logger.warning("maintenance assignee notify skipped — no wa request_id=%s", request_id)
        return

    photo_url = (rd.get("issue_photo_url") or "").strip()
    if not photo_url.lower().startswith("https://"):
        logger.error(
            "maintenance assignee notify skipped — no photo request_id=%s", request_id
        )
        return

    template_name = _team_notify_template_name()
    if not template_name:
        logger.warning("MAINTENANCE_TEAM_NOTIFY_TEMPLATE_NAME not set")
        return

    phone = wa_id_to_phone(wa)
    display_name = sentence_case_name(rd.get("assigned_to") or "Technician")
    body_values = _team_notify_template_body_values(rd)
    rid = (request_id or "").strip()

    try:
        _send_template_with_image_header(
            phone,
            template_name,
            photo_url,
            language_code=_team_notify_template_language(),
            body_values=body_values,
            callback_data=rid[:512],
            contact_name=display_name,
        )
        logger.info(
            "maintenance team image template sent wa=%s request_id=%s photo=%s",
            wa,
            request_id,
            photo_url[:80],
        )
    except Exception:
        logger.exception(
            "maintenance team template failed wa=%s request_id=%s photo=%s",
            wa,
            request_id,
            photo_url[:80],
        )
