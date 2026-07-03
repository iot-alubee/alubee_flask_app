"""Portal admin JMD/MD approval — reuses Interakt Production approval chain."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_INTERAKT_PROD = Path(__file__).resolve().parent.parent / "Interakt" / "Production"
if _INTERAKT_PROD.is_dir() and str(_INTERAKT_PROD) not in sys.path:
    sys.path.insert(0, str(_INTERAKT_PROD))

_configured = False


def _utcnow():
    return datetime.now(timezone.utc)


def _send_to(wa_id: str, text: str) -> None:
    from vehicle_notify import send_text, wa_id_to_phone

    wa = (wa_id or "").strip()
    if not wa:
        return
    try:
        send_text(wa_id_to_phone(wa), text)
    except Exception:
        logger.exception("portal approval send_text failed to=%s", wa)


def _configure(db) -> None:
    global _configured
    if _configured:
        return

    from bot_config import bootstrap_env

    bootstrap_env(_INTERAKT_PROD)

    import approval
    import bot_shared
    import visitor_request
    from bot_shared import wa_from_env

    def _session_ref(sender: str):
        return db.collection("sessions").document(sender)

    def _session_merge(sender: str, **fields) -> None:
        _session_ref(sender).set(fields, merge=True)

    def _chat_name(name) -> str:
        raw = str(name or "").strip()
        return raw.title() if raw else "Employee"

    def _same_whatsapp(a: str, b: str) -> bool:
        return bool(a and b and a.strip().lower() == b.strip().lower())

    def _has_active_whatsapp_session(sender: str) -> bool:
        return True

    def _on_visitor_md_approved(ref, rd: dict) -> None:
        visitor_request.send_otps_after_md_approve(ref, rd, _send_to)

    jmd_i = wa_from_env("JMD_I_WHATSAPP_NUMBER", "JMD_WHATSAPP_NUMBER")
    jmd_ii = wa_from_env("JMD_II_WHATSAPP_NUMBER")
    md = wa_from_env("MD_WHATSAPP_NUMBER")
    test_md = wa_from_env("TEST_MD_WHATSAPP_NUMBER")
    ppc = wa_from_env("PPC_WHATSAPP_NUMBER")
    hr = wa_from_env("HR_WHATSAPP_NUMBER")
    session_hours = int((os.getenv("WHATSAPP_SESSION_HOURS") or "24").strip() or "24")

    bot_shared.configure(
        db=db,
        send_to=_send_to,
        session_ref=_session_ref,
        session_merge=_session_merge,
        utcnow=_utcnow,
        has_active_whatsapp_session=_has_active_whatsapp_session,
        chat_name=_chat_name,
        same_whatsapp=_same_whatsapp,
    )
    approval.configure(
        approval.ApprovalDeps(
            db=db,
            send_to=_send_to,
            session_merge=_session_merge,
            session_ref=_session_ref,
            utcnow=_utcnow,
            chat_name=_chat_name,
            same_whatsapp=_same_whatsapp,
            has_active_whatsapp_session=_has_active_whatsapp_session,
            jmd_i=jmd_i,
            jmd_ii=jmd_ii,
            md=md,
            test_md=test_md,
            whatsapp_session_hours=session_hours,
            menu_idle_state="MENU_IDLE",
            on_visitor_md_approved=_on_visitor_md_approved,
            visitor_jmd_i=jmd_i,
            visitor_jmd_ii=jmd_ii,
            visitor_md=md,
            visitor_route_by_unit=(
                (os.getenv("VISITOR_ROUTE_BY_UNIT") or "").strip().lower()
                in ("1", "true", "yes")
            ),
            visitor_test_jmd_i=wa_from_env("VISITOR_TEST_JMD_I_WHATSAPP_NUMBER"),
            visitor_test_jmd_ii=wa_from_env("VISITOR_TEST_JMD_II_WHATSAPP_NUMBER"),
            visitor_test_md=wa_from_env("VISITOR_TEST_MD_WHATSAPP_NUMBER"),
            visitor_test_employee_wa_ids=frozenset(),
            ppc=ppc,
            hr=hr,
        )
    )
    _configured = True
    logger.info("Portal approval module configured")


def admin_approve(db, request_id: str, step: str) -> tuple[bool, str | None]:
    """Run JMD or MD approval from the admin portal."""
    _configure(db)
    import approval

    return approval.portal_admin_approve(request_id, step)
