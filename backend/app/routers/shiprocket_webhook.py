# app/routers/shiprocket_webhook.py
import os
import logging
from datetime import datetime
from typing import List, Optional, Union
from .cloudprinter_webhook import _send_tracking_email
import requests

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=False)

from fastapi import APIRouter, Request, Response, BackgroundTasks
from pydantic import BaseModel, Field, ConfigDict
from pymongo import MongoClient

router = APIRouter()

EXPECTED_TOKEN = (os.getenv("SHIPROCKET_WEBHOOK_TOKEN") or "").strip()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI, tz_aware=True)
db = client["candyman"]
orders_collection = db["shipping_details"]

class Scan(BaseModel):
    model_config = ConfigDict(extra="allow")
    date: Optional[str] = None
    status: Optional[str] = None
    activity: Optional[str] = None
    location: Optional[str] = None
    sr_status: Optional[Union[str, int]] = Field(default=None, alias="sr-status")
    sr_status_label: Optional[str] = Field(default=None, alias="sr-status-label")

class ShiprocketEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    awb: Optional[str] = None
    courier_name: Optional[str] = None
    current_status: Optional[str] = None
    current_status_id: Optional[int] = None
    shipment_status: Optional[str] = None
    shipment_status_id: Optional[int] = None
    current_timestamp: Optional[str] = None
    order_id: Optional[str] = None
    sr_order_id: Optional[int] = None
    awb_assigned_date: Optional[str] = None
    pickup_scheduled_date: Optional[str] = None
    etd: Optional[str] = None
    scans: Optional[List[Scan]] = None
    is_return: Optional[int] = None
    channel_id: Optional[int] = None
    pod_status: Optional[str] = None
    pod: Optional[str] = None

SHIPROCKET_TRACKING_URL_TEMPLATE = "https://shiprocket.co/tracking/{tracking}"

def _parse_ts(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    try:
        from datetime import datetime
        return datetime.strptime(ts, "%d %m %Y %H:%M:%S").isoformat()
    except Exception:
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).isoformat()
        except Exception:
            return None

def _dedupe_key(ev: ShiprocketEvent) -> str:
    base = f"{ev.awb or ''}|{ev.current_status_id or ''}|{ev.current_timestamp or ''}"
    import hashlib
    return hashlib.sha256(base.encode()).hexdigest()

_seen: set[str] = set()

from datetime import datetime, timezone
def _upsert_tracking(e: ShiprocketEvent, raw: dict) -> None:
    q = {"order_id": e.order_id} if e.order_id else {"awb_code": e.awb}
    update = {
        "$set": {
            "shiprocket_data": {
                "awb": e.awb,
                "courier_name": e.courier_name,
                "current_status": e.current_status,
                "current_status_id": e.current_status_id,
                "shipment_status": e.shipment_status,
                "shipment_status_id": e.shipment_status_id,
                "current_timestamp_iso": _parse_ts(e.current_timestamp),
                "current_timestamp_raw": e.current_timestamp,
                "sr_order_id": e.sr_order_id,
                "pod_status": e.pod_status,
                "pod": e.pod,
                "last_update_utc": datetime.now(timezone.utc),
                "scans": [s.model_dump(by_alias=True) for s in (e.scans or [])],
                "raw": raw,
            },
            "tracking_number": e.awb or raw.get("tracking") or "",
            "courier_partner": e.courier_name or "",
            "delivery_status": "shipped" if (e.current_status or "").upper() in {"DELIVERED", "RTO DELIVERED"} else None,
        }
    }
    if update["$set"]["delivery_status"] is None:
        update["$set"].pop("delivery_status", None)
    orders_collection.update_one(q, update, upsert=False)



@router.post("/api/webhook/Genesis")
@router.post("/api/webhook/Genesis/")
async def shiprocket_tracking(request: Request, background: BackgroundTasks) -> Response:
    try:
        if EXPECTED_TOKEN:
            token = request.headers.get("x-api-key")
            if not token or token.strip() != EXPECTED_TOKEN:
                logging.warning("[SR WH] token mismatch; ignoring payload")
                return Response(status_code=200)

        ct = (request.headers.get("content-type") or "").lower()
        if not ct.startswith("application/json"):
            return Response(status_code=200)

        raw = await request.json()
        logging.info(f"[SR WH] payload: {raw}")
        event = ShiprocketEvent.model_validate(raw)

        key = _dedupe_key(event)
        if key in _seen:
            return Response(status_code=200)
        _seen.add(key)

        _upsert_tracking(event, raw)

        try:
            internal_id = event.order_id  # same order_id you stored in DB
            if internal_id:
                base_url = os.getenv("NEXT_PUBLIC_API_BASE_URL")
                requests.get(
                    f"{base_url}/shiprocket/order/show",
                    params={"internal_order_id": internal_id},
                    timeout=10
                )
                logging.info(f"[SR WH] Triggered /shiprocket/order/show for {internal_id}")
        except Exception as exc:
            logging.exception(f"[SR WH] Failed to trigger order/show for {event.order_id}: {exc}")
        
        query_base = {"order_id": event.order_id} if event.order_id else {"awb_code": event.awb}
        should_attempt = bool(event.awb or (raw.get("tracking")))
        if should_attempt:
            filter_once = {
                **query_base,
                "$or": [
                    {"shiprocket_shipped_email_sent": {"$exists": False}},
                    {"shiprocket_shipped_email_sent": False},
                ],
            }
            set_once = {"$set": {"shiprocket_shipped_email_sent": True}}
            once = orders_collection.update_one(filter_once, set_once, upsert=False)
            if once.modified_count == 1:
                doc = orders_collection.find_one(
                    query_base,
                    {"email": 1, "user_name": 1, "child_name": 1, "order_id": 1, "tracking_number": 1, "_id": 0},
                ) or {}
                to_email = (doc.get("email") or "").strip()
                if to_email:
                    order_ref = (doc.get("order_id") or event.order_id or "").strip()
                    shipping_option = "shiprocket"
                    tracking = (doc.get("tracking_number") or event.awb or "").strip()
                    user_name = doc.get("user_name")
                    name = doc.get("child_name")
                    background.add_task(
                        _send_tracking_email,
                        to_email,
                        order_ref,
                        shipping_option,
                        tracking,
                        user_name,
                        name,
                        SHIPROCKET_TRACKING_URL_TEMPLATE,
                        None
                    )
                    logging.info(f"[SR WH] queued shipped-email to {to_email} for {order_ref}")

        return Response(status_code=200)
    except Exception as exc:
        logging.exception(f"[SR WH] error: {exc}")
        return Response(status_code=200)
