"""Portal admin JMD/MD approval — self-contained for Cloud Run (no Interakt imports)."""

from __future__ import annotations

import logging
import os
import secrets
from datetime import datetime, timezone

from vehicle_notify import (
    send_guest_visit_otp,
    send_template,
    send_text,
    sentence_case_name,
    wa_id_to_phone,
)

logger = logging.getLogger(__name__)

STATUS_AUTO_APPROVE = "AUTO_APPROVE"
_APPROVAL_DONE = frozenset({"APPROVED", STATUS_AUTO_APPROVE})
APPROVER_STATUS_COLLECTION = "approver_status"

_VISITING_TO_LABELS = {
    "UNIT_I": "Unit I",
    "UNIT_II": "Unit II",
    "BOTH": "Both",
}


def _utcnow():
    return datetime.now(timezone.utc)


def _approval_step_done(status: str) -> bool:
    return (status or "").strip().upper() in _APPROVAL_DONE


def _wa_from_env(*keys: str) -> str:
    for key in keys:
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return ""


def _wa_from_mobile(mobile: str) -> str:
    digits = "".join(c for c in (mobile or "") if c.isdigit())
    if len(digits) == 10:
        return f"whatsapp:+91{digits}"
    if len(digits) >= 12 and digits.startswith("91"):
        return f"whatsapp:+{digits}"
    return ""


def _jmd_i_wa() -> str:
    return _wa_from_env("JMD_I_WHATSAPP_NUMBER", "JMD_WHATSAPP_NUMBER") or _wa_from_mobile(
        "7339221730"
    )


def _jmd_ii_wa() -> str:
    return _wa_from_env("JMD_II_WHATSAPP_NUMBER") or _wa_from_mobile("7339221731")


def _md_wa() -> str:
    return _wa_from_env("MD_WHATSAPP_NUMBER") or _wa_from_mobile("7538866308")


def _approver_is_offline(db, wa_id: str) -> bool:
    if not db or not wa_id:
        return False
    snap = (
        db.collection(APPROVER_STATUS_COLLECTION)
        .document(wa_id.strip().lower())
        .get()
    )
    if not snap.exists:
        return False
    return (snap.to_dict() or {}).get("availability", "").strip().lower() == "offline"


def _send_to(wa_id: str, text: str) -> None:
    wa = (wa_id or "").strip()
    if not wa:
        return
    try:
        send_text(wa_id_to_phone(wa), text)
    except Exception:
        logger.exception("portal approval send_text failed to=%s", wa)


def _request_type_label(rd: dict) -> str:
    req_type = (rd.get("type") or "").strip().upper()
    if req_type == "VISITOR":
        return "visitor"
    if req_type == "LEAVE":
        return "leave"
    if req_type == "PERMISSION":
        return "permission"
    return "OD"


def _request_type_key(rd: dict) -> str:
    return (rd.get("type") or "OD").strip().upper()


def _jmd_wa_for_route(route: str) -> str:
    if (route or "").strip().upper() == "JMD2":
        return _jmd_ii_wa()
    return _jmd_i_wa()


def _request_jmd_whatsapp(rd: dict) -> str:
    stored = (rd.get("jmd") or "").strip()
    if stored:
        return stored
    route = (rd.get("jmd_route") or "JMD1").strip().upper()
    return _jmd_wa_for_route(route)


def _request_md_whatsapp(rd: dict) -> str:
    stored = (rd.get("md") or "").strip()
    return stored or _md_wa()


def _is_leave_request(rd: dict) -> bool:
    return _request_type_key(rd) == "LEAVE"


def _md_offline_applies_bypass(db, rd: dict, md_wa: str) -> bool:
    if _is_leave_request(rd):
        return False
    return _approver_is_offline(db, md_wa)


def _md_offline_closed(rd: dict) -> bool:
    if rd.get("md_offline_bypass"):
        return True
    md_st = (rd.get("md_status") or "").strip().upper()
    return md_st in (STATUS_AUTO_APPROVE, "OFFLINE")


def _md_status_after_jmd(md_offline: bool) -> str:
    return STATUS_AUTO_APPROVE if md_offline else "PENDING"


def _md_offline_bypass_fields(md_offline: bool) -> dict:
    if not md_offline:
        return {}
    return {"md_offline_bypass": True, "approved_datetime": _utcnow()}


def _dual_jmd_both_approved(rd: dict) -> bool:
    i = (rd.get("jmd_i_status") or "").strip().upper()
    ii = (rd.get("jmd_ii_status") or "").strip().upper()
    return _approval_step_done(i) and _approval_step_done(ii)


def _visitor_jmd_fully_approved(rd: dict) -> bool:
    if rd.get("visitor_dual_jmd"):
        return _dual_jmd_both_approved(rd)
    return _approval_step_done(rd.get("jmd_status"))


def _employee_final_approval_message(req_label: str) -> str:
    if req_label == "leave":
        return "Your leave request has been approved."
    if req_label == "permission":
        return "Your permission request has been approved."
    if req_label == "visitor":
        return "Your visitor request has been approved."
    return "Your OD has been Approved."


def _digits(raw: str) -> str:
    return "".join(c for c in str(raw or "") if c.isdigit())


def _coming_on_label(rd: dict) -> str:
    return (rd.get("coming_on_date") or rd.get("visit_date") or "").strip() or "—"


def _coming_from_label(rd: dict) -> str:
    return (
        (rd.get("coming_from") or rd.get("coming_from_label") or rd.get("organization") or "")
        .strip()
        or "—"
    )


def _coming_for_label(rd: dict) -> str:
    return (
        (rd.get("coming_for_label") or rd.get("visit_for_label") or rd.get("purpose_label") or "")
        .strip()
        or "—"
    )


def _resolve_guest_phone10(rd: dict) -> str:
    for key in ("guest_phone", "guest_whatsapp", "guest_wa", "guest_mobile", "visitor_mobile"):
        digits = _digits(rd.get(key) or "")
        if len(digits) >= 10:
            return digits[-10:]
    return ""


def _send_visitor_otps_after_md_approve(ref, rd: dict) -> str:
    snap = ref.get()
    if snap.exists:
        rd = snap.to_dict() or rd

    otp = f"{secrets.randbelow(1_000_000):06d}"
    ref.update({"visitor_otp": otp, "guest_otp_sent": False})

    names = rd.get("visitor_names") or []
    if isinstance(names, str):
        names = [n.strip() for n in names.split(",") if n.strip()]
    names_text = ", ".join(names) if names else "—"
    employee = (rd.get("employee") or "").strip()
    guest_phone10 = _resolve_guest_phone10(rd)
    request_id = (rd.get("request_id") or "").strip()

    if employee:
        _send_to(
            employee,
            (
                "Your visitor request is approved.\n\n"
                f"Coming on: {_coming_on_label(rd)}\n"
                f"Visitors: {names_text}\n"
                f"Coming from: {_coming_from_label(rd)}\n"
                f"Coming for: {_coming_for_label(rd)}\n"
                f"Entry OTP: {otp}\n\n"
                "Share this OTP with your visitors and security at the gate."
            ),
        )

    if not guest_phone10:
        if employee:
            _send_to(
                employee,
                "Visitor WhatsApp number was not saved on this request. "
                "Share the OTP with the guest manually.",
            )
        return otp

    guest_name = (names[0] if names else "Guest") or "Guest"
    if send_guest_visit_otp(
        guest_phone10,
        guest_name=str(guest_name)[:50],
        otp=otp,
        organization=_coming_from_label(rd),
    ):
        ref.update({"guest_otp_sent": True, "guest_phone": guest_phone10})
        if employee:
            _send_to(
                employee,
                f"Entry OTP was also sent on WhatsApp to the visitor ({guest_phone10}).",
            )
    elif employee:
        _send_to(
            employee,
            (
                f"Visitor OTP {otp} could not be sent on WhatsApp to {guest_phone10}. "
                "Share the OTP manually."
            ),
        )
    logger.info("visitor OTP generated request_id=%s guest=%s", request_id, guest_phone10)
    return otp


def _after_jmd_when_md_offline(
    db,
    ref,
    rd: dict,
    *,
    employee: str,
    req_label: str,
) -> None:
    snap = ref.get()
    rd_fresh = snap.to_dict() if snap.exists else rd
    if req_label == "visitor" and _visitor_jmd_fully_approved(rd_fresh):
        _send_visitor_otps_after_md_approve(ref, rd_fresh)
    elif employee:
        _send_to(employee, _employee_final_approval_message(req_label))


def _approval_template_name(rd: dict) -> str:
    req_type = _request_type_key(rd)
    return (
        os.getenv(f"{req_type}_APPROVAL_TEMPLATE_NAME")
        or os.getenv("OD_APPROVAL_TEMPLATE_NAME")
        or ""
    ).strip()


def _approval_template_body_fields(rd: dict) -> list[str]:
    req_type = _request_type_key(rd)
    raw = (
        os.getenv(f"{req_type}_APPROVAL_TEMPLATE_BODY_FIELDS")
        or os.getenv("APPROVAL_TEMPLATE_BODY_FIELDS")
        or "employee,department,reason"
    ).strip()
    return [k.strip().lower() for k in raw.split(",") if k.strip()]


def _approval_template_values(rd: dict) -> dict[str, str]:
    emp = sentence_case_name(rd.get("employee_name") or "Employee")
    dept = (rd.get("department") or "—").strip()
    req_type = _request_type_key(rd)

    if req_type == "VISITOR":
        raw_names = rd.get("visitor_names") or []
        if isinstance(raw_names, str):
            names = raw_names.strip() or "—"
        else:
            names = ", ".join(raw_names) or "—"
        visiting = (
            (rd.get("visiting_to_label") or "").strip()
            or _VISITING_TO_LABELS.get((rd.get("visiting_to") or "").strip().upper(), "")
            or "—"
        )
        return {
            "employee": emp,
            "department": dept,
            "visitor_name": names,
            "visitor_type": (rd.get("visitor_type_label") or "—").strip(),
            "visiting_to": visiting,
            "purpose": (rd.get("purpose_detail") or rd.get("purpose_label") or "—").strip(),
            "coming_on": _coming_on_label(rd),
            "coming_from": _coming_from_label(rd),
            "people_count": str(rd.get("people_count") or "—"),
            "mobile": str(rd.get("guest_phone") or "—"),
        }

    if req_type == "LEAVE":
        return {
            "employee": emp,
            "department": dept,
            "days": str(rd.get("leave_days") or "—"),
            "from_date": (rd.get("leave_from_date") or "—").strip(),
            "to_date": (rd.get("leave_to_date") or "—").strip(),
            "reason": (rd.get("reason") or "—").strip(),
            "leaves_current_month": str(rd.get("leaves_current_month") or 0),
            "leaves_last_month": str(rd.get("leaves_last_month") or 0),
        }

    return {
        "employee": emp,
        "department": dept,
        "date": _coming_on_label(rd),
        "visiting_to": (rd.get("reason") or "—").strip(),
        "purpose": (rd.get("od_purpose") or "—").strip(),
        "time_required": "—",
        "reason": (rd.get("reason") or "—").strip(),
    }


def _notify_md_for_approval(md_wa: str, rd: dict, request_id: str) -> None:
    template_name = _approval_template_name(rd)
    if not template_name:
        _send_to(
            md_wa,
            (
                f"Approval needed for {sentence_case_name(rd.get('employee_name') or 'employee')} "
                f"({rd.get('department') or '—'}). Request id: {request_id}"
            ),
        )
        return
    fields = _approval_template_body_fields(rd)
    values = _approval_template_values(rd)
    body_values = [values.get(key, "—")[:1024] for key in fields]
    lang = (os.getenv("APPROVAL_TEMPLATE_LANGUAGE_CODE") or "en").strip()
    try:
        send_template(
            wa_id_to_phone(md_wa),
            template_name,
            language_code=lang,
            body_values=body_values,
            callback_data=(request_id or "")[:512],
            contact_name="Approver",
        )
    except Exception:
        logger.exception("portal MD approval template failed request_id=%s", request_id)


def portal_admin_approve(db, request_id: str, step: str) -> tuple[bool, str | None]:
    """Admin portal override for stuck approvals. step: 'jmd' or 'md'."""
    rid = (request_id or "").strip()
    step_key = (step or "").strip().lower()
    if not rid:
        return False, "Missing request id."
    if step_key not in ("jmd", "md"):
        return False, "Invalid approval step."

    ref = db.collection("requests").document(rid)
    snap = ref.get()
    if not snap.exists:
        return False, "Request not found."
    rd = snap.to_dict() or {}
    employee = (rd.get("employee") or "").strip()
    req_label = _request_type_label(rd)
    req_type = _request_type_key(rd)

    if step_key == "jmd":
        if rd.get("visitor_dual_jmd"):
            jmd_i_st = (rd.get("jmd_i_status") or "").strip().upper()
            jmd_ii_st = (rd.get("jmd_ii_status") or "").strip().upper()
            if not _approval_step_done(jmd_i_st):
                ref.update({"jmd_i_status": "APPROVED"})
            elif not _approval_step_done(jmd_ii_st):
                ref.update({"jmd_ii_status": "APPROVED"})
            else:
                return False, "JMD approval is already complete."
            rd = ref.get().to_dict() or rd
            if _dual_jmd_both_approved(rd):
                md_wa = _request_md_whatsapp(rd)
                md_off = _md_offline_applies_bypass(db, rd, md_wa)
                ref.update({
                    "jmd_status": "APPROVED",
                    "md_status": _md_status_after_jmd(md_off),
                    "md": md_wa,
                    "manager_status": "N/A",
                    **_md_offline_bypass_fields(md_off),
                })
                rd = ref.get().to_dict() or rd
                if md_off:
                    _after_jmd_when_md_offline(
                        db, ref, rd, employee=employee, req_label=req_label
                    )
                else:
                    _notify_md_for_approval(md_wa, rd, rid)
            return True, None

        jmd_st = (rd.get("jmd_status") or "").strip().upper()
        if jmd_st in ("APPROVED", STATUS_AUTO_APPROVE):
            return False, "JMD has already approved this request."
        if jmd_st in ("DENIED", "N/A"):
            return False, "JMD approval is not pending."

        md_wa = _request_md_whatsapp(rd)
        md_off = _md_offline_applies_bypass(db, rd, md_wa)
        ref.update({
            "jmd": _request_jmd_whatsapp(rd),
            "jmd_route": (rd.get("jmd_route") or "JMD1").strip().upper(),
            "md": md_wa,
            "manager_status": "N/A",
            "jmd_status": "APPROVED",
            "md_status": _md_status_after_jmd(md_off),
            **_md_offline_bypass_fields(md_off),
        })
        rd = ref.get().to_dict() or rd
        if md_off:
            _after_jmd_when_md_offline(
                db, ref, rd, employee=employee, req_label=req_label
            )
        else:
            _notify_md_for_approval(md_wa, rd, rid)
        return True, None

    md_st = (rd.get("md_status") or "").strip().upper()
    if md_st in ("APPROVED", STATUS_AUTO_APPROVE):
        return False, "MD has already approved this request."
    if _md_offline_closed(rd):
        return False, "MD approval was already bypassed (offline)."

    if rd.get("visitor_dual_jmd"):
        if not _visitor_jmd_fully_approved(rd):
            return False, "Both JMD approvals must be completed first."
    else:
        jmd_st = (rd.get("jmd_status") or "").strip().upper()
        if jmd_st not in ("APPROVED", STATUS_AUTO_APPROVE) and not rd.get(
            "jmd_offline_bypass"
        ):
            return False, "JMD approval must be completed first."

    patch = {"md_status": "APPROVED", "approved_datetime": _utcnow()}
    if (rd.get("jmd_status") or "").strip().upper() in ("PENDING", "AWAITING_MANAGER"):
        patch["jmd_status"] = "APPROVED"
    ref.update(patch)
    fresh = ref.get()
    rd_fresh = fresh.to_dict() if fresh.exists else rd
    if req_type == "VISITOR":
        _send_visitor_otps_after_md_approve(ref, rd_fresh)
    elif employee:
        _send_to(employee, _employee_final_approval_message(req_label))
    return True, None


def admin_approve(db, request_id: str, step: str) -> tuple[bool, str | None]:
    return portal_admin_approve(db, request_id, step)
