# app/routers/reconcile.py
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any, List, Tuple, Union
import os
import httpx
import re
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from app.routers.razorpay_export import (
    _assert_keys,
    amount_to_display,
    ts_to_ddmmyyyy_hhmmss,
)
import hmac, hashlib
from datetime import timedelta
from dateutil import parser as dtparser, tz as dttz
from zoneinfo import ZoneInfo
import asyncio
from datetime import datetime, timezone
import json
import html
import logging
from email.message import EmailMessage
import smtplib

IST_TZ = ZoneInfo("Asia/Kolkata")
router = APIRouter(prefix="/reconcile", tags=["reconcile"])
REPORT_TZ = ZoneInfo(os.getenv("RECONCILE_TZ", "Asia/Kolkata"))  # default IST
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def norm(s: str | None, *, case_insensitive: bool) -> str:
    t = (s or "").replace("\u00A0", " ").strip()
    return t.lower() if case_insensitive else t

# ---- Razorpay fetcher (reuse your existing code) ----------------------------
from app.routers.razorpay_export import fetch_payments, _assert_keys
# ----------------------------------------------------------------------------

# ---- Mongo connection via ENV ----------------------------------------------
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    # Fail fast with a clear message instead of silently defaulting to localhost.
    raise RuntimeError("MONGO_URI not set")

client = MongoClient(MONGO_URI, tz_aware=True)
db = client["candyman"]
orders_collection = db["user_details"]

_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)

def _to_unix_start(s: str | None) -> int | None:
    if not s:
        return None
    dt = dtparser.parse(s)
    # If no tz provided, interpret in REPORT_TZ, not server tz
    if dt.tzinfo is None:
        # date-only → midnight in REPORT_TZ
        if len(s.strip()) <= 10:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=REPORT_TZ)
        else:
            dt = dt.replace(tzinfo=REPORT_TZ)
    return int(dt.timestamp())

def _to_unix_end(s: str | None) -> int | None:
    if not s:
        return None
    dt = dtparser.parse(s)
    if dt.tzinfo is None:
        if len(s.strip()) <= 10:
            # end of day in REPORT_TZ
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=REPORT_TZ)
        else:
            dt = dt.replace(tzinfo=REPORT_TZ)
    return int(dt.timestamp())

def _send_email_html(
    to_email: Union[str, List[str], None],
    subject: str,
    html_body: str,
) -> None:
    """
    Sends an HTML email using Gmail SMTP.
    Falls back to EMAIL_TO from environment if no recipient provided.
    """
    email_user = (os.getenv("EMAIL_ADDRESS") or "").strip()
    email_pass = (os.getenv("EMAIL_PASSWORD") or "").strip()
    if not email_user or not email_pass:
        raise RuntimeError("EMAIL_ADDRESS or EMAIL_PASSWORD not configured")
    EMAIL_TO = os.getenv("EMAIL_TO", "").split(",")
    # Normalize recipients
    if to_email is None:
        recipients = EMAIL_TO[:]
    elif isinstance(to_email, list):
        recipients = [e.strip() for e in to_email if e and e.strip()]
    else:  # string
        recipients = [e.strip() for e in to_email.split(",") if e.strip()]

    if not recipients:
        raise RuntimeError(
            "No recipients found. Configure EMAIL_TO in .env or pass to_email."
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Diffrun <{email_user}>"
    msg["To"] = ", ".join(recipients)
    msg.set_content("This message contains HTML content.")
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_user, email_pass)
            smtp.sendmail(email_user, recipients, msg.as_string())
        logger.info(f"[EMAIL] Sent '{subject}' to {', '.join(recipients)}")
    except Exception as e:
        logger.exception(f"[EMAIL] Failed to send '{subject}' — {e}")

def _render_na_table(title: str, wnd_from: str, wnd_to: str, rows: list[dict]) -> str:
    """Render an HTML table with: Payment ID, Email, Payment Date, Amount, Paid, Preview, Job ID."""
    def safe(v):  # basic escape
        return html.escape(str(v)) if v is not None else "—"

    header = f"""
    <h2 style="margin:0 0 8px 0;font-family:Arial,sans-serif">{safe(title)}</h2>
    <div style="font-family:Arial,sans-serif;font-size:13px;margin:0 0 12px 0">
      <strong>Window (IST):</strong> {safe(wnd_from)} → {safe(wnd_to)}
    </div>
    """

    if not rows:
        return header + '<p style="font-family:Arial,sans-serif">No NA payment details.</p>'

    # Build table rows
    tr_html = []
    for r in rows:
        pid = r.get("id") or r.get("payment_id") or "—"
        email = r.get("email") or "—"
        dt = r.get("created_at") or "—"
        amt = r.get("amount_display") or "—"
        paid = r.get("paid")
        paid_str = "true" if paid is True else (
            "false" if paid is False else "—")
        prev = r.get("preview_url") or ""
        job = r.get("job_id") or "—"

        prev_link = f'<a href="{html.escape(prev)}" target="_blank">preview</a>' if prev else "—"

        tr_html.append(f"""
          <tr>
            <td style="padding:6px 8px;border:1px solid #e5e7eb;font-family:Consolas,Menlo,monospace">{safe(pid)}</td>
            <td style="padding:6px 8px;border:1px solid #e5e7eb">{safe(email)}</td>
            <td style="padding:6px 8px;border:1px solid #e5e7eb">{safe(dt)}</td>
            <td style="padding:6px 8px;border:1px solid #e5e7eb">{safe(amt)}</td>
            <td style="padding:6px 8px;border:1px solid #e5e7eb">{safe(paid_str)}</td>
            <td style="padding:6px 8px;border:1px solid #e5e7eb">{prev_link}</td>
            <td style="padding:6px 8px;border:1px solid #e5e7eb;font-family:Consolas,Menlo,monospace">{safe(job)}</td>
          </tr>
        """)

    table = f"""
    <table cellspacing="0" cellpadding="0" style="border-collapse:collapse;border:1px solid #e5e7eb;font-family:Arial,sans-serif;font-size:13px">
      <thead>
        <tr style="background:#f9fafb">
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Payment ID</th>
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Email</th>
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Payment Date</th>
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Amount</th>
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Paid</th>
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Preview</th>
          <th style="text-align:left;padding:6px 8px;border:1px solid #e5e7eb">Job ID</th>
        </tr>
      </thead>
      <tbody>
        {''.join(tr_html)}
      </tbody>
    </table>
    """

    return header + table


# ------------------------------ KEEP: /orders --------------------------------
@router.get("/orders")
def get_orders(
    sort_by: Optional[str] = Query(None, description="Field to sort by"),
    sort_dir: Optional[str] = Query("asc", description="asc or desc"),
    filter_status: Optional[str] = Query(None),
    filter_book_style: Optional[str] = Query(None),
    filter_print_approval: Optional[str] = Query(None),
    filter_discount_code: Optional[str] = Query(None),
    exclude_discount_code: Optional[str] = None,
):
    # Base query: only show paid orders
    query = {"paid": True}

    # Add additional filters
    if filter_status == "approved":
        query["approved"] = True
    elif filter_status == "uploaded":
        query["approved"] = False

    if filter_book_style:
        query["book_style"] = filter_book_style

    if filter_print_approval == "yes":
        query["print_approval"] = True
    elif filter_print_approval == "no":
        query["print_approval"] = False
    elif filter_print_approval == "not_found":
        query["print_approval"] = {"$exists": False}

    if filter_discount_code:
        if filter_discount_code.lower() == "none":
            query["discount_amount"] = 0
            query["paid"] = True
        else:
            query["discount_code"] = filter_discount_code.upper()

    if exclude_discount_code:
        if "discount_code" in query and isinstance(query["discount_code"], str):
            query["$and"] = [
                {"discount_code": query["discount_code"]},
                {"discount_code": {"$ne": exclude_discount_code.upper()}},
            ]
            del query["discount_code"]
        elif "discount_code" not in query:
            query["discount_code"] = {"$ne": exclude_discount_code.upper()}

    # Fetch and sort records
    sort_field = sort_by if sort_by else "created_at"
    sort_order = 1 if sort_dir == "asc" else -1

    projection = {
        "order_id": 1, "job_id": 1, "cover_url": 1, "book_url": 1, "preview_url": 1,
        "name": 1, "shipping_address": 1, "created_at": 1, "processed_at": 1,
        "approved_at": 1, "approved": 1, "book_id": 1, "book_style": 1,
        "print_status": 1, "price": 1, "total_price": 1, "amount": 1, "total_amount": 1,
        "feedback_email": 1, "print_approval": 1, "discount_code": 1,
        "currency": 1, "locale": 1, "_id": 0,
    }

    records = list(orders_collection.find(query, projection).sort(sort_field, sort_order))
    result = []
    for doc in records:
        result.append({
            "order_id": doc.get("order_id", ""),
            "job_id": doc.get("job_id", ""),
            "coverPdf": doc.get("cover_url", ""),
            "interiorPdf": doc.get("book_url", ""),
            "previewUrl": doc.get("preview_url", ""),
            "name": doc.get("name", ""),
            "city": doc.get("shipping_address", {}).get("city", ""),
            "price": doc.get("price", doc.get("total_price", doc.get("amount", doc.get("total_amount", 0)))),
            "paymentDate": doc.get("processed_at", ""),
            "approvalDate": doc.get("approved_at", ""),
            "status": "Approved" if doc.get("approved") else "Uploaded",
            "bookId": doc.get("book_id", ""),
            "bookStyle": doc.get("book_style", ""),
            "printStatus": doc.get("print_status", ""),
            "feedback_email": doc.get("feedback_email", False),
            "print_approval": doc.get("print_approval", None),
            "discount_code": doc.get("discount_code", ""),
            "currency": doc.get("currency", ""),
            "locale": doc.get("locale", ""),
        })
    return result
# ----------------------------------------------------------------------------

async def _vlookup_core(
    *,
    status,
    max_fetch,
    from_date,
    to_date,
    case_insensitive_ids,
    orders_batch_size,
    na_status,
):
    resp = await vlookup_payment_to_orders_auto(
        status=status,
        max_fetch=max_fetch,
        from_date=from_date,
        to_date=to_date,
        case_insensitive_ids=case_insensitive_ids,
        orders_batch_size=orders_batch_size,
        na_status=na_status,
    )
    logger.info(f"Response data: {resp.body}")
    if isinstance(resp, JSONResponse):
        # JSONResponse.body is bytes → decode to dict
        return json.loads(resp.body.decode("utf-8"))
    return resp  # already a dict


@router.get("/vlookup-payment-to-orders/auto")
async def vlookup_payment_to_orders_auto(
    # Payments: ALL STATUSES by default (None)
    status: Optional[str] = Query(None, description="Filter payments fetched from Razorpay by status; omit for ALL"),
    max_fetch: int = Query(200_000, ge=1, le=1_000_000, description="Upper bound for Razorpay pulls"),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD / ISO; omit for ALL time"),
    to_date:   Optional[str] = Query(None, description="YYYY-MM-DD / ISO; omit for ALL time"),
    case_insensitive_ids: bool = Query(False, description="Lowercase both sides before matching"),

    # Orders paging (scan *all* orders)
    orders_batch_size: int = Query(50_000, ge=1_000, le=200_000, description="Mongo batch size"),

    # IMPORTANT: default to only NA with status=captured
    na_status: Optional[str] = Query("captured", description="Only include NA payments with this Razorpay status"),
):
    _assert_keys()

    def _to_unix(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        from dateutil import parser as dtparser
        return int(dtparser.parse(s).timestamp())
    
    from_unix = _to_unix_start(from_date)
    to_unix   = _to_unix_end(to_date)
    # 1) Razorpay: fetch ALL (status=None => all statuses)
    try:
        async with httpx.AsyncClient(
            auth=(os.getenv("RAZORPAY_KEY_ID"), os.getenv("RAZORPAY_KEY_SECRET")),
            timeout=60.0
        ) as client:
            payments: List[Dict[str, Any]] = await fetch_payments(
                client=client,
                status_filter=status,   # None => all
                from_unix=from_unix,
                to_unix=to_unix,
                max_fetch=max_fetch,
            )
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Network error calling Razorpay: {e}")

    # Index: normalized id -> (raw id, status)
    pay_index: Dict[str, Dict[str, str]] = {}
    for p in payments:
        raw_id = str(p.get("id", "") or "")
        if not raw_id:
            continue
        key = norm(raw_id, case_insensitive=case_insensitive_ids)
        st = (p.get("status") or "").strip().lower()
        pay_index[key] = {"id": raw_id, "status": st}

    payment_keys = set(pay_index.keys())
    matched_keys: set[str] = set()

    # 2) Scan ALL orders by _id (Atlas-safe)
    total_orders_docs = 0
    orders_with_tx = 0
    last_id = None

    try:
        while True:
            q: Dict[str, Any] = {}
            if last_id is not None:
                q["_id"] = {"$gt": last_id}

            batch = list(
                orders_collection.find(q, projection={"transaction_id": 1, "order_id": 1})
                                 .sort([("_id", 1)])
                                 .limit(orders_batch_size)
            )
            if not batch:
                break

            total_orders_docs += len(batch)

            for doc in batch:
                raw_tx = doc.get("transaction_id")
                if not raw_tx:
                    continue
                tx_key = norm(str(raw_tx), case_insensitive=case_insensitive_ids)
                if not tx_key:
                    continue
                orders_with_tx += 1
                if tx_key in payment_keys:
                    matched_keys.add(tx_key)

            last_id = batch[-1]["_id"]
    except PyMongoError as e:
        raise HTTPException(status_code=502, detail=f"Mongo query failed: {e}")

    # 3) NA keys (in payments but not matched to any order)
    na_keys = payment_keys - matched_keys

    # Build NA items and FILTER by status (default captured)
    target_status = (na_status or "captured").strip().lower()
    na_items: List[Dict[str, str]] = []
    for k in na_keys:
        rec = pay_index.get(k)
        if not rec:
            continue
        if (rec.get("status") or "") == target_status:
            na_items.append({"id": rec["id"], "status": target_status})

    # Sort by id (status is uniform now)
    na_items.sort(key=lambda x: x["id"])

    # Group (will only contain the target status)
    na_by_status: Dict[str, List[str]] = {}
    for item in na_items:
        na_by_status.setdefault(item["status"], []).append(item["id"])

    matched_distinct = len(matched_keys)
    logger.info(f"na items {na_items}")
    return JSONResponse({
        "summary": {
            "total_orders_docs_scanned": total_orders_docs,
            "orders_with_transaction_id": orders_with_tx,
            "total_payments_rows": len(payments),
            "payment_status_filter": status or "(ALL)",
            "case_insensitive_ids": case_insensitive_ids,
            "matched_distinct_payment_ids": matched_distinct,
            # IMPORTANT: now counts ONLY the chosen status (default captured)
            "na_count": len(na_items),
            "max_fetch": max_fetch,
            "date_window": {
                "from_date": from_date or "(all-time)",
                "to_date": to_date or "(all-time)",
            },
            "orders_batch_size": orders_batch_size,
            "na_status_filter": target_status,
        },
        # Only the chosen status (default captured)
        "na_payment_ids": [x["id"] for x in na_items],
        "na_by_status": na_by_status,  # contains only the chosen status
    })

def _extract_uuid(s: str | None) -> str | None:
    if not isinstance(s, str) or not s:
        return None
    m = _UUID_RE.search(s)
    return m.group(0) if m else None

def _extract_job_id_from_payment(p: Dict[str, Any]) -> Optional[str]:
    notes = p.get("notes") or {}
    if isinstance(notes, dict):
        # common keys first
        for k in ("job_id", "JobId", "JOB_ID"):
            v = notes.get(k)
            got = _extract_uuid(v) if isinstance(v, str) else None
            if got:
                return got
        # scan all string values
        for v in notes.values():
            if isinstance(v, str):
                got = _extract_uuid(v)
                if got:
                    return got
    desc = p.get("description") or ""
    return _extract_uuid(desc) if isinstance(desc, str) else None

def _fmt_inr_number(n: float) -> str:
    # ₹1,234.56 (trim trailing .00)
    s = f"₹{n:,.2f}"
    return s[:-3] if s.endswith(".00") else s

def _epoch_to_ist_str(epoch: int | float | None) -> str:
    if not epoch:
        return "—"
    try:
        dt = datetime.fromtimestamp(int(epoch), IST_TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return "—"

def _extract_preview_url_from_notes(notes: dict) -> str:
    if not isinstance(notes, dict):
        return ""
    for k in ("preview_url", "preview", "previewUrl", "PREVIEW_URL"):
        v = notes.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""



async def _auto_reconcile_and_sign_once() -> None:
    """
    Auto-verify ALL eligible payments found in the last window, every run.

    Window (IST): [now-10m, now-2m]
    For each NA 'captured' payment within the window:
      - Fetch Razorpay payment details
      - Extract job_id, book_id, book_style, discount_code from Razorpay (notes/description)
      - Generate signature
      - Compute pricing using BOOK_PRICING + DISCOUNT_PCT (numbers, not strings)
      - POST /verify-razorpay
      - On success: mark in user_details and persist numeric pricing fields
    Never breaks on failure; continues to next payment_id.
    """
    import os, json, re, httpx
    from decimal import Decimal, ROUND_HALF_UP
    from datetime import datetime, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
    except Exception:
        ZoneInfo = None

    # ---------- static pricing/discount tables (Python mirror of TS) ----------
    BOOK_PRICING = {
        "astro":  {"paperback": {"price": "₹1,775", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,275", "shipping": "0", "taxes": "0"}},
        "hero":   {"paperback": {"price": "₹1,665", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,200", "shipping": "0", "taxes": "0"}},
        "bloom":  {"paperback": {"price": "₹1,665", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,220", "shipping": "0", "taxes": "0"}},
        "wigu":   {"paperback": {"price": "₹1,775", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,220", "shipping": "0", "taxes": "0"}},
        "twin":   {"paperback": {"price": "₹1,665", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,220", "shipping": "0", "taxes": "0"}},
        "dream":  {"paperback": {"price": "₹1,665", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,220", "shipping": "0", "taxes": "0"}},
        "sports": {"paperback": {"price": "₹1,665", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,220", "shipping": "0", "taxes": "0"}},
        "abcd":   {"paperback": {"price": "₹1,665", "shipping": "0", "taxes": "0"}, "hardcover": {"price": "₹2,220", "shipping": "0", "taxes": "0"}},
    }
    DISCOUNT_PCT = {
        "LHMM": 99.93, "LHMM50": 50, "SPECIAL10": 10, "LEMON20": 20, "TEST": 99.93,
        "COLLAB": 99.93, "SUKHKARMAN5": 5, "WELCOME5": 5, "SAM5": 5, "SUBSCRIBER10": 10,
        "MRSNAMBIAR15": 15, "AKMEMON15": 15, "TANVI15": 15, "PERKY15": 15, "SPECIAL15": 15,
        "JISHU15": 15, "JESSICA15": 15,
    }

    # ---------- helpers (ensure numeric types) ----------
    def _round2_d(x: Decimal) -> Decimal:
        return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _to_d(n) -> Decimal:
        try:
            return Decimal(str(n))
        except Exception:
            return Decimal("0")

    def _parse_inr_string_to_number(s: str | None) -> float:
        """'₹1,750' → 1750.0 ; '0' → 0.0; None → 0.0"""
        if not s:
            return 0.0
        cleaned = str(s).replace("₹", "").replace(",", "").strip()
        try:
            return float(_round2_d(Decimal(cleaned)))
        except Exception:
            return 0.0

    def _inr_from_paise_to_number(paise) -> float:
        """Razorpay paise → INR number, rounded to 2 decimals"""
        d = _to_d(paise) / Decimal(100)
        return float(_round2_d(d))

    UUID_RX = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b")

    def _extract_job_id_from_payment(pay: dict) -> str | None:
        """Pull job_id from Razorpay payment payload: notes first, then description, else None."""
        notes = pay.get("notes") or {}
        for k in ("job_id", "JobId", "JOB_ID", "job", "Job", "JOB"):
            v = notes.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in (notes.values() if isinstance(notes, dict) else []):
            if isinstance(v, str):
                m = UUID_RX.search(v)
                if m:
                    return m.group(0)
        desc = pay.get("description")
        if isinstance(desc, str) and desc.strip():
            m = UUID_RX.search(desc)
            if m:
                return m.group(0)
            return desc.strip()
        return None

   
    def _note_str(notes: dict, *keys: str) -> str:
            if not isinstance(notes, dict):
                return ""
            for k in keys:
                v = notes.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""    

    def _resolve_book_pricing_numbers(book_id: str | None, book_style: str | None) -> tuple[float, float, float] | None:
        """
        Lookup by lowercased keys and return numbers (not strings):
        (actual_price, shipping, taxes) as floats, or None if not found.
        """
        if not book_id or not book_style:
            return None
        bid = book_id.strip().lower()
        style = book_style.strip().lower()
        conf = BOOK_PRICING.get(bid, {})
        variant = conf.get(style)
        if not variant:
            return None
        return (
            _parse_inr_string_to_number(variant.get("price")),
            _parse_inr_string_to_number(variant.get("shipping")),
            _parse_inr_string_to_number(variant.get("taxes")),
        )

    # ---------- time window (IST) ----------
    ist = ZoneInfo("Asia/Kolkata") if ZoneInfo else None
    now_ist = datetime.now(ist) if ist else datetime.utcnow()
    window_start = now_ist - timedelta(minutes=10)
    window_end = now_ist - timedelta(minutes=2)
    from_iso = window_start.isoformat(timespec="seconds")
    to_iso = window_end.isoformat(timespec="seconds")

    # ---------- discover NA captured payments ----------
    payload = await _vlookup_core(
        status=None,
        max_fetch=200_000,
        from_date=from_iso,
        to_date=to_iso,
        case_insensitive_ids=False,
        orders_batch_size=50_000,
        na_status="captured",
    )
    if isinstance(payload, (bytes, bytearray)):
        payload = json.loads(payload.decode("utf-8"))

    na_ids = payload.get("na_payment_ids", []) or []
    if not na_ids:
        logger.info("[AUTO] No NA payments in window; nothing to do.")
        return

    candidate_ids = sorted({str(x).strip() for x in na_ids if x})
    logger.info(f"[AUTO] Candidate payments in window: {candidate_ids}")

    key_id = os.getenv("RAZORPAY_KEY_ID")
    key_secret = os.getenv("RAZORPAY_KEY_SECRET")
    if not key_id or not key_secret:
        raise RuntimeError("RAZORPAY_KEY_ID / RAZORPAY_KEY_SECRET must be set")

    # Use your internal base; change default if needed
    API_BASE = os.getenv("BACKEND_INTERNAL_BASE", "https://admin.diffrun.com").rstrip("/")

    rows_for_email: list[dict] = []

    # ---------- process ALL candidates (no break on failures) ----------
    async with httpx.AsyncClient(timeout=30.0) as client, \
               httpx.AsyncClient(auth=(key_id, key_secret), timeout=20.0) as rz:

        for payment_id in candidate_ids:
            try:
                # 1) Fetch Razorpay payment
                r = await rz.get(f"https://api.razorpay.com/v1/payments/{payment_id}")
                if r.status_code == 404:
                    logger.warning(f"[AUTO] Payment {payment_id} not found at Razorpay; skipping.")
                    continue
                r.raise_for_status()
                pay = r.json()

                order_id = (pay.get("order_id") or "").strip()
                if not order_id:
                    logger.warning(f"[AUTO] Payment {payment_id} missing order_id; skipping.")
                    continue

                # Build a base row for email (we’ll set paid=True only on success)
                notes = pay.get("notes") or {}
                base_row = {
                    "id": payment_id,
                    "email": pay.get("email") or notes.get("email") or "—",
                    "created_at": _epoch_to_ist_str(pay.get("created_at")),
                    "amount_display": _fmt_inr_number(_inr_from_paise_to_number(pay.get("amount") or 0)),
                    "paid": False,  # default; flip to True on success
                    "preview_url": _extract_preview_url_from_notes(notes),
                    "job_id": "",   # fill after extraction
                }


                # 2) Signature (same as /sign-razorpay)
                signature = _make_razorpay_signature(order_id, payment_id)
                logger.info(f"[AUTO] Signature generated for {payment_id}")

                # 3) Pull meta from Razorpay
                
                job_id = _extract_job_id_from_payment(pay)
                base_row["job_id"] = job_id or "—"
                if not job_id:
                    logger.info(f"[AUTO] No job_id in Razorpay payload for {payment_id}; skipping.")
                    rows_for_email.append(base_row)
                    continue

                doc = orders_collection.find_one(
                    {"job_id": job_id},
                    {"book_id": 1, "book_style": 1},
                )
                book_id = (doc or {}).get("book_id") or ""
                book_style = (doc or {}).get("book_style") or ""


                # 4) Pricing (numbers only)
                # actual_price from BOOK_PRICING, else fallback to paid amount from Razorpay
                resolved = _resolve_book_pricing_numbers(book_id, book_style)
                print(f"Resolved pricing for book_id={book_id}, book_style={book_style}: {resolved}")
                paid_amount = _inr_from_paise_to_number(pay.get("amount"))  # number
                if resolved is None:
                    actual_price, shipping, taxes = paid_amount, 0.0, 0.0
                else:
                    actual_price, shipping, taxes = resolved

                discount_code = (_note_str(notes, "discount_code", "DiscountCode", "DISCOUNT_CODE") or "").upper()   
                discount_percentage = float(DISCOUNT_PCT.get(discount_code, 0.0))  # number
                # discount_amount = round2((discountPct / 100) * actualPrice)
                discount_amount = float(_round2_d(Decimal(discount_percentage) / Decimal(100) * Decimal(str(actual_price))))
                final_amount = float(_round2_d(Decimal(str(actual_price)) - Decimal(str(discount_amount)) + Decimal(str(shipping)) + Decimal(str(taxes))))
                
                # 5) /verify-razorpay (send numeric types)
                verify_payload = {
                    "razorpay_order_id": order_id,
                    "razorpay_payment_id": payment_id,
                    "razorpay_signature": signature,
                    "job_id": job_id,
                    "actual_price": actual_price,
                    "discount_code": discount_code,
                    "discount_percentage": discount_percentage,
                    "discount_amount": discount_amount,
                    "final_amount": final_amount,
                    "shipping_price": shipping,
                    "taxes": taxes,
                    "book_id": book_id or None,
                    "book_style": book_style or None,
                }
                vr = await client.post(f"https://test-backend.diffrun.com/verify-razorpay", json=verify_payload)

                vjson = None
                try:
                    vjson = vr.json()
                except Exception:
                    pass

                if not (vr.is_success and isinstance(vjson, dict) and vjson.get("success")):
                    logger.warning(f"[AUTO] Verify failed for {payment_id}; status={vr.status_code}, body={vjson}")
                    rows_for_email.append(base_row)   # paid stays False
                    continue

                # 6) Reconcile flag + pricing fields in DB (only when verify succeeded)
                now_utc = datetime.now(timezone.utc)

                # NOTE: if your real handle is different, replace `user_details` below.
                orders_collection.update_many(
                    {"transaction_id": payment_id},
                    {"$set": {
                        "reconcile": True,
                        "reconciled_at": now_utc,
                        # Persist the same numeric fields for backoffice/reporting
                        "actual_price": actual_price,
                        "discount_code": discount_code,
                        "discount_percentage": discount_percentage,
                        "discount_amount": discount_amount,
                        "final_amount": final_amount,
                        "shipping_price": shipping,
                        "taxes": taxes,
                    }},
                )
                logger.info(f"[AUTO] Reconciled {payment_id} and updated pricing fields in user_details.")

                # mark this row as success
                success_row = dict(base_row)
                success_row["paid"] = True
                rows_for_email.append(success_row)


                # 7) Optional /reconcile/mark (best-effort)
                try:
                    mark_payload = {"job_id": job_id, "razorpay_payment_id": payment_id}
                    await client.post(f"{API_BASE}/reconcile/mark", json=mark_payload)
                except Exception:
                    pass

            except httpx.HTTPStatusError as e:
                logger.warning(f"[AUTO] HTTPStatusError for {payment_id}: {e}")
                rows_for_email.append({
                    "id": payment_id, "email": "—", "created_at": "—",
                    "amount_display": "—", "paid": False, "preview_url": "—", "job_id": "—",
                })
                continue
            except httpx.RequestError as e:
                logger.warning(f"[AUTO] RequestError (network) for {payment_id}: {e}")
                # network is unhealthy; stop the run to avoid partial storms
                break
            except Exception as e:
                logger.exception(f"[AUTO] Unexpected error for {payment_id}: {e}")
                rows_for_email.append({
                    "id": payment_id, "email": "—", "created_at": "—",
                    "amount_display": "—", "paid": False, "preview_url": "—", "job_id": "—",
                })
                # keep going to the next payment_id
                continue
    try:
        if rows_for_email:
            verified = sum(1 for r in rows_for_email if r.get("paid") is True)
            subject = f"[Auto-Reconcile] NA payments: {verified} verified / {len(rows_for_email)} found"
            body_html = _render_na_table(subject, from_iso, to_iso, rows_for_email)
            _send_email_html(None, subject, body_html)
            logger.info(f"[AUTO] Email sent: {subject}")
        else:
            logger.info("[AUTO] No candidate rows gathered; no email.")
    except Exception:
        logger.exception("[AUTO] Failed to render/send auto-reconcile email")


def _lookup_paid_preview_by_job(job_id: Optional[str]) -> Tuple[Optional[bool], Optional[str]]:
    if not job_id:
        return (None, None)
    try:
        doc = orders_collection.find_one({"job_id": job_id}, {"paid": 1, "preview_url": 1})
        if not doc:
            return (None, None)
        paid = bool(doc.get("paid")) if "paid" in doc else None
        preview_url = doc.get("preview_url") if isinstance(doc.get("preview_url"), str) else None
        return (paid, preview_url)
    except Exception:
        return (None, None)

def _project_row(payment: Dict[str, Any]) -> Dict[str, Any]:
    """Combine Razorpay fields + DB (job_id, paid, preview_url)."""
    upi = payment.get("upi") or {}
    acq = payment.get("acquirer_data") or {}
    vpa = payment.get("vpa") or upi.get("vpa") or ""
    flow = upi.get("flow", "")

    pid = payment.get("id", "")

    # Primary: transaction_id == payment_id mapping in your user_details
    job_id_db = None
    paid, preview_url = (None, None)
    try:
        doc_tx = orders_collection.find_one({"transaction_id": pid}, {"job_id": 1, "paid": 1, "preview_url": 1})
        if doc_tx:
            job_id_db = doc_tx.get("job_id")
            paid = bool(doc_tx.get("paid")) if "paid" in doc_tx else None
            preview_url = doc_tx.get("preview_url") if isinstance(doc_tx.get("preview_url"), str) else None
    except Exception:
        pass

    # Fallback: extract UUID job_id from Razorpay payload then lookup by job_id
    if not job_id_db:
        job_id_guess = _extract_job_id_from_payment(payment)
        if job_id_guess:
            paid, preview_url = _lookup_paid_preview_by_job(job_id_guess)
            job_id_db = job_id_guess

    return {
        "id": pid,
        "email": payment.get("email") or None,
        "contact": payment.get("contact") or None,
        "status": payment.get("status") or None,
        "method": payment.get("method") or None,
        "currency": payment.get("currency") or None,
        "amount_display": amount_to_display(payment.get("amount")),
        "created_at": ts_to_ddmmyyyy_hhmmss(payment.get("created_at")),
        "order_id": payment.get("order_id") or None,
        "description": payment.get("description") or None,
        "vpa": vpa or None,
        "flow": flow or None,
        "rrn": acq.get("rrn") or None,
        "arn": acq.get("authentication_reference_number") or None,
        "auth_code": acq.get("auth_code") or None,
        # DB-enriched:
        "job_id": job_id_db,
        "paid": paid,
        "preview_url": preview_url,
    }

@router.post("/na-payment-details")
async def na_payment_details(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Body: {"ids": ["pay_ABC...", ...]}
    Returns Razorpay details + DB-enriched (job_id, paid, preview_url) for each ID.
    """
    _assert_keys()

    ids = body.get("ids")
    if not isinstance(ids, list) or not ids:
        raise HTTPException(400, detail="Body must contain 'ids' as a non-empty list of strings")

    uniq_ids = list(dict.fromkeys([str(x).strip() for x in ids if str(x).strip()]))
    if len(uniq_ids) > 2000:
        raise HTTPException(413, detail="Too many IDs; max 2000 per request")

    items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    try:
        async with httpx.AsyncClient(
            auth=(os.getenv("RAZORPAY_KEY_ID"), os.getenv("RAZORPAY_KEY_SECRET")),
            timeout=20.0
        ) as client:
            for pid in uniq_ids:
                try:
                    r = await client.get(f"https://api.razorpay.com/v1/payments/{pid}")
                    if r.status_code == 404:
                        errors.append({"id": pid, "error": "not_found"})
                        continue
                    r.raise_for_status()
                    p = r.json()
                    items.append(_project_row(p))
                except httpx.HTTPStatusError as e:
                    errors.append({"id": pid, "error": f"http_{e.response.status_code}", "detail": (e.response.text or "")[:200]})
                except httpx.RequestError as e:
                    errors.append({"id": pid, "error": "network", "detail": str(e)})
    except httpx.RequestError as e:
        raise HTTPException(502, detail=f"Network error calling Razorpay: {e}")

    return {"count": len(items), "items": items, "errors": errors}

def _make_razorpay_signature(order_id: str, payment_id: str) -> str:
    secret = os.getenv("RAZORPAY_KEY_SECRET")
    if not secret:
        raise RuntimeError("RAZORPAY_KEY_SECRET not set")
    message = f"{order_id}|{payment_id}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


@router.post("/sign-razorpay")
def sign_razorpay(body: Dict[str, Any]) -> Dict[str, str]:
    order_id = (body or {}).get("razorpay_order_id")
    payment_id = (body or {}).get("razorpay_payment_id")
    if not order_id or not payment_id:
        raise HTTPException(400, detail="razorpay_order_id and razorpay_payment_id are required")
    return {"razorpay_signature": _make_razorpay_signature(order_id, payment_id)}


@router.post("/email-report")
async def email_report(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str]   = Query(None),
    status: Optional[str]    = Query(None),           # keep parity with the UI (None → ALL)
    na_status: Optional[str] = Query("captured"),     # same default as UI
    max_fetch: int = Query(200_000, ge=1, le=1_000_000),
):
    # 1) Call the same core logic via the existing route (or refactor into a shared function)
    data = await vlookup_payment_to_orders_auto(
        status=status,
        max_fetch=max_fetch,
        from_date=from_date,
        to_date=to_date,
        case_insensitive_ids=False,
        orders_batch_size=50_000,
        na_status=na_status,
    )
    logger.info(f"data: {data}")
    # 2) Build your email from the returned 'summary' and 'na_payment_ids'
    payload = data.body if isinstance(data, JSONResponse) else data  # FastAPI returns JSONResponse
    summary = payload["summary"]
    na_ids  = payload["na_payment_ids"]  # <- EXACTLY the same list UI uses

    # TODO: render HTML and send email here (omitted)
    # Include summary['date_window'], summary['na_count'], and a table of na_ids

    return {"ok": True, "emailed": len(na_ids)}
