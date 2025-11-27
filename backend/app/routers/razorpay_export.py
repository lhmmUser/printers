# app/routers/razorpay_export.py
import os, io, csv, re
from datetime import datetime
from typing import Any, Dict, List, Optional
import httpx
from fastapi import APIRouter, Query, HTTPException, Body
from fastapi.responses import StreamingResponse
from dateutil import parser as dtparser
from dotenv import load_dotenv

router = APIRouter(prefix="/razorpay", tags=["razorpay"])

load_dotenv()

RZP_BASE = "https://api.razorpay.com/v1"
KEY_ID = os.getenv("RAZORPAY_KEY_ID")
KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")

_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)

def _assert_keys():
    if not KEY_ID or not KEY_SECRET:
        raise HTTPException(500, detail="RAZORPAY_KEY_ID/RAZORPAY_KEY_SECRET not set on backend")

def amount_to_display(v: Any) -> str:
    # Razorpay amounts are subunits (paise). 148500 -> 1485.00
    try:
        return f"{int(v)/100:.2f}"
    except Exception:
        return ""

def ts_to_ddmmyyyy_hhmmss(ts: Any) -> str:
    try:
        if ts is None or ts == "":
            return ""
        if isinstance(ts, (int, float)):
            # local server time; change to utcfromtimestamp(...) if you need UTC
            return datetime.fromtimestamp(int(ts)).strftime("%d/%m/%Y %H:%M:%S")
        return dtparser.parse(str(ts)).strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return ""

async def fetch_payments(
    client: httpx.AsyncClient,
    *,
    status_filter: Optional[str],
    from_unix: Optional[int],
    to_unix: Optional[int],
    max_fetch: int = 10000,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    skip = 0
    COUNT = 100  # Razorpay max per call

    while True:
        params: Dict[str, Any] = {"count": COUNT, "skip": skip}
        if from_unix is not None: params["from"] = from_unix
        if to_unix is not None:   params["to"]   = to_unix

        # include UPI/card context where available
        params["expand[]"] = "card"

        r = await client.get(f"{RZP_BASE}/payments", params=params)
        r.raise_for_status()
        data = r.json()
        batch = data.get("items", []) or []
        if status_filter:
            sf = status_filter.lower()
            batch = [p for p in batch if (p.get("status") or "").lower() == sf]
        items.extend(batch)

        if len(batch) < COUNT or len(items) >= max_fetch:
            break
        skip += COUNT

    return items[:max_fetch]

@router.get("/payments-csv")
async def payments_csv(
    status: Optional[str] = Query("captured", description="Filter by status (e.g. captured)"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD or ISO datetime)"),
    to_date: Optional[str]   = Query(None, description="End date (YYYY-MM-DD or ISO datetime)"),
    max_fetch: int = Query(2000, ge=1, le=50000, description="Upper bound to avoid runaway downloads"),
) -> StreamingResponse:
    """
    Fetch Razorpay payments and stream as CSV.
    Keys must be set in backend env: RAZORPAY_KEY_ID / RAZORPAY_KEY_SECRET
    """
    _assert_keys()

    def to_unix(s: Optional[str]) -> Optional[int]:
        return int(dtparser.parse(s).timestamp()) if s else None

    from_unix = to_unix(from_date)
    to_unix   = to_unix(to_date)

    try:
        async with httpx.AsyncClient(auth=(KEY_ID, KEY_SECRET), timeout=30.0) as client:
            payments = await fetch_payments(
                client,
                status_filter=status,
                from_unix=from_unix,
                to_unix=to_unix,
                max_fetch=max_fetch,
            )
    except httpx.HTTPStatusError as e:
        # bubble up Razorpay error content
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Network error calling Razorpay: {e}")

    # Prepare CSV
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)

    # Columns tailored to the JSON you pasted
    header = [
        "id","amount","currency","status","order_id","invoice_id","international","method",
        "amount_refunded","refund_status","captured","description","card_id","bank","wallet",
        "vpa","email","contact","notes","fee","tax","error_code","error_description","created_at",
        "Payments_RRN","Payments_ARN","Auth_code","flow"
    ]
    w.writerow(header)

    for p in payments:
        upi = p.get("upi") or {}
        acq = p.get("acquirer_data") or {}
        # VPA may appear in root.vpa or upi.vpa
        vpa = p.get("vpa") or upi.get("vpa") or ""
        flow = upi.get("flow", "")

        # Notes can be an object; keep compact JSON-ish string
        notes = p.get("notes") or ""
        notes_str = "" if notes == "" else str(notes)

        row = [
            p.get("id",""),
            amount_to_display(p.get("amount")),
            p.get("currency",""),
            p.get("status",""),
            p.get("order_id",""),
            p.get("invoice_id",""),
            str(p.get("international","") if p.get("international") is not None else ""),
            p.get("method",""),
            amount_to_display(p.get("amount_refunded")),
            p.get("refund_status","") if p.get("refund_status") is not None else "",
            str(p.get("captured","") if p.get("captured") is not None else ""),
            p.get("description",""),
            p.get("card_id","") if p.get("card_id") is not None else "",
            p.get("bank","") if p.get("bank") is not None else "",
            p.get("wallet","") if p.get("wallet") is not None else "",
            vpa,
            p.get("email",""),
            p.get("contact",""),
            notes_str,
            amount_to_display(p.get("fee")),
            amount_to_display(p.get("tax")),
            p.get("error_code","") if p.get("error_code") is not None else "",
            p.get("error_description","") if p.get("error_description") is not None else "",
            ts_to_ddmmyyyy_hhmmss(p.get("created_at")),
            acq.get("rrn",""),
            acq.get("authentication_reference_number",""),
            acq.get("auth_code",""),
            flow,
        ]
        w.writerow(row)

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="razorpay_payments.csv"'}
    )

def _extract_job_id(p: Dict[str, Any]) -> Optional[str]:
    # 1) Prefer notes.job_id if present
    notes = p.get("notes") or {}
    if isinstance(notes, dict):
        jid = notes.get("job_id") or notes.get("JobId") or notes.get("JOB_ID")
        if isinstance(jid, str) and _UUID_RE.search(jid):
            return _UUID_RE.search(jid).group(0)
        # scan all values for a UUID
        for v in notes.values():
            if isinstance(v, str):
                m = _UUID_RE.search(v)
                if m:
                    return m.group(0)
    # 2) Fallback: scan description for UUID
    desc = p.get("description") or ""
    if isinstance(desc, str):
        m = _UUID_RE.search(desc)
        if m:
            return m.group(0)
    return None

def _payment_to_detail(p: Dict[str, Any]) -> Dict[str, Any]:
    upi = p.get("upi") or {}
    acq = p.get("acquirer_data") or {}
    vpa = p.get("vpa") or upi.get("vpa") or ""
    flow = upi.get("flow", "")

    return {
        "id": p.get("id", ""),
        "email": p.get("email", "") or None,
        "contact": p.get("contact", "") or None,
        "status": p.get("status", "") or None,
        "method": p.get("method", "") or None,
        "currency": p.get("currency", "") or None,
        "amount_display": amount_to_display(p.get("amount")),
        "created_at": ts_to_ddmmyyyy_hhmmss(p.get("created_at")),
        "order_id": p.get("order_id", "") or None,
        "description": p.get("description", "") or None,
        "notes": p.get("notes") or {},
        "vpa": vpa or None,
        "flow": flow or None,
        "rrn": acq.get("rrn", "") or None,
        "arn": acq.get("authentication_reference_number", "") or None,
        "auth_code": acq.get("auth_code", "") or None,
        "job_id": _extract_job_id(p),
    }

@router.post("/payments/by-ids")
async def payments_by_ids(
    payload: Dict[str, Any] = Body(..., example={"ids": ["pay_123", "pay_456"]}),
) -> Dict[str, Any]:
    """
    Given a list of Razorpay payment IDs, return payment details for each.
    Requires RAZORPAY_KEY_ID / RAZORPAY_KEY_SECRET.
    """
    _assert_keys()
    ids = payload.get("ids")
    if not isinstance(ids, list) or not all(isinstance(x, str) and x.strip() for x in ids):
        raise HTTPException(400, detail="Body must contain 'ids' as a non-empty list of strings")

    # Deduplicate and cap to a sane upper bound
    uniq_ids = list(dict.fromkeys(x.strip() for x in ids if x.strip()))
    if len(uniq_ids) > 1000:
        raise HTTPException(413, detail="Too many IDs; max 1000 per request")

    items: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    try:
        async with httpx.AsyncClient(auth=(KEY_ID, KEY_SECRET), timeout=20.0) as client:
            for pid in uniq_ids:
                try:
                    r = await client.get(f"{RZP_BASE}/payments/{pid}")
                    if r.status_code == 404:
                        errors.append({"id": pid, "error": "Not found"})
                        continue
                    r.raise_for_status()
                    p = r.json()
                    items.append(_payment_to_detail(p))
                except httpx.HTTPStatusError as e:
                    errors.append({"id": pid, "error": f"http {e.response.status_code}", "detail": e.response.text[:200]})
                except httpx.RequestError as e:
                    errors.append({"id": pid, "error": "network", "detail": str(e)})
    except httpx.RequestError as e:
        raise HTTPException(502, detail=f"Network error calling Razorpay: {e}")

    return {"count": len(items), "items": items, "errors": errors}