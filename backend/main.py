# main.py
from typing import Dict, Any
from datetime import datetime
import time
from fastapi import FastAPI, HTTPException, Query, Body
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone
from dateutil import parser
import pytz
import os
import re
import requests
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Basic config & DB
# -----------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI not set")

client = MongoClient(MONGO_URI, tz_aware=True)
db = client["candyman"]
orders_collection = db["user_details"]
shipping_collection = db["shipping_details"]

IST_TZ = pytz.timezone("Asia/Kolkata")


def split_full_name(full_name: str) -> tuple[str, str]:
    """Split a full name into first name and last name."""
    if not full_name:
        return ("", "")

    parts = full_name.strip().split()
    if len(parts) == 1:
        return (parts[0], "")
    return (" ".join(parts[:-1]), parts[-1])


def generate_book_title(book_id: str, child_name: Optional[str]) -> str:
    if not child_name:
        child_name = "Your child"
    else:
        child_name = child_name.strip().capitalize()

    book_id = (book_id or "").lower()

    if book_id == "wigu":
        return f"When {child_name} grows up"
    elif book_id == "astro":
        return f"{child_name}'s Space Adventure"
    elif book_id == "abcd":
        return f"{child_name} meets ABC"
    elif book_id == "dream":
        return f"Many Dreams of {child_name}"
    elif book_id == "sports":
        return f"Game On, {child_name}!"
    elif book_id == "hero":
        return f"{child_name}, the Little Hero"
    elif book_id == "bloom":
        return f"{child_name}' is Growing Up Fast"
    else:
        return f"{child_name}'s Storybook"


PRINTER_TOKENS = {
    "genesis": os.getenv("GENESIS_TOKEN", "GENESIS123"),
    "yara": os.getenv("YARA_TOKEN", "YARA123"),
    "admin": os.getenv("ADMIN_TOKEN", "ADMIN999"),
}


@app.get("/orders")
def get_orders(
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    printer: Optional[str] = Query(
        None, description="Filter by printer, e.g. 'genesis'"
    ),
    search: Optional[str] = Query(
        None, description="Search by order_id substring"
    ),
    token: Optional[str] = Query(
        None, description="Access token tied to printer/user"
    ),
):
    """
    Paginated /orders endpoint for Genesis/Yara dashboards.

    - Returns only fields your frontend actually uses.
    - Supports:
      - page / page_size pagination
      - optional printer filter (case-insensitive exact match)
      - optional search on order_id (case-insensitive substring)
    - Access is controlled by `token`:
      - printer tokens can only view their own printer
      - admin token can view any printer
    """

    # -------------------------------------------------------------------------
    # Very simple token-based auth (NOT production-grade)
    # -------------------------------------------------------------------------
    if not token:
        raise HTTPException(status_code=401, detail="token is required")

    role: Optional[str] = None
    printer_from_token: Optional[str] = None

    # Find which token this is (genesis / yara / admin)
    for key, tok in PRINTER_TOKENS.items():
        if tok == token:
            if key == "admin":
                role = "admin"
            else:
                role = "printer"
                printer_from_token = key
            break

    if role is None:
        # token not recognized
        raise HTTPException(status_code=403, detail="invalid token")

    # Non-admins: force printer based on token, ignore whatever was passed
    if role != "admin":
        printer = printer_from_token

    # -------------------------------------------------------------------------
    # Query building
    # -------------------------------------------------------------------------

    # base query: paid orders only (earlier behavior)
    query: Dict[str, Any] = {"paid": True}

    # optional printer filter (e.g. "genesis" / "yara")
    if printer:
        query["printer"] = {
            "$regex": f"^{re.escape(printer)}$",
            "$options": "i",
        }

    # optional search filter on order_id
    if search:
        query["order_id"] = {
            "$regex": re.escape(search),
            "$options": "i",
        }

    projection = {
        "order_id": 1,
        "name": 1,
        "shipping_address": 1,
        "book_id": 1,
        "book_style": 1,
        "cover_url": 1,
        "book_url": 1,
        "printer": 1,
        "label_url": 1,
        "phone_number": 1,
        "created_at": 1,
        "_id": 0,
        "print_sent_at": 1,
        "zip": 1,  # in case you have it stored separately
    }

    # total count for this filter (for UI page count)
    total = orders_collection.count_documents(query)

    # paginated query
    cursor = (
        orders_collection.find(query, projection)
        .sort("created_at", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )

    records = list(cursor)

    result: List[Dict[str, Any]] = []
    for doc in records:
        ship = doc.get("shipping_address", {}) or {}
        result.append(
            {
                # IDs and basic info
                "order_id": doc.get("order_id", ""),
                "name": doc.get("name", ""),
                "city": ship.get("city", ""),
                "zip": ship.get("zip", "") or doc.get("zip", ""),

                # book info – matches your frontend RawOrder type
                "bookId": doc.get("book_id", ""),
                "bookStyle": doc.get("book_style", ""),

                # PDFs
                "coverPdf": doc.get("cover_url", ""),
                "interiorPdf": doc.get("book_url", ""),

                # shipping integration fields
                "printer": doc.get("printer", ""),
                "label_url": doc.get("label_url", ""),

                # phone used in the table
                "phone_number": doc.get("phone_number", ""),
                "print_sent_at": doc.get("print_sent_at", ""),
            }
        )

    return {
        "items": result,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# -----------------------------------------------------------------------------
# Shiprocket integration – /shiprocket/create-from-orders
# -----------------------------------------------------------------------------
SHIPROCKET_BASE = os.getenv(
    "SHIPROCKET_BASE", "https://apiv2.shiprocket.in"
).rstrip("/")
SHIPROCKET_EMAIL = os.getenv("SHIPROCKET_EMAIL")
SHIPROCKET_PASSWORD = os.getenv("SHIPROCKET_PASSWORD")



def _sr_login_token() -> str:
    if not SHIPROCKET_EMAIL or not SHIPROCKET_PASSWORD:
        raise HTTPException(
            status_code=500, detail="Shiprocket API creds missing")
    r = requests.post(
        f"{SHIPROCKET_BASE}/v1/external/auth/login",
        json={"email": SHIPROCKET_EMAIL, "password": SHIPROCKET_PASSWORD},
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(
            status_code=502, detail=f"Shiprocket auth failed: {r.text}")
    token = (r.json() or {}).get("token")
    if not token:
        raise HTTPException(
            status_code=502, detail="Shiprocket auth returned no token")
    return token


def _sr_headers(tok: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}


def _sr_order_payload_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    ship = doc.get("shipping_address") or {}

    # name split
    first, last = split_full_name(
        ship.get("name", "") or (doc.get("user_name") or doc.get("name") or "")
    )

    # qty and price
    qty = int(doc.get("quantity", 1) or 1)
    subtotal = float(
        doc.get("total_amount")
        or doc.get("total_price")
        or doc.get("amount")
        or doc.get("price")
        or 0.0
    )

    # package dimensions by book_id (simplified)
    book_id_raw = (doc.get("book_id") or "").lower().strip()
    if book_id_raw == "wigu":
        length, breadth, height = 32.0, 23.0, 3.0
    else:
        length, breadth, height = 23.0, 23.0, 3.0

    weight = float(doc.get("weight_kg", 0.5))

    # book identity for item line
    order_id = doc.get("order_id")
    book_id = (doc.get("book_id") or "BOOK").upper()
    book_style = (doc.get("book_style") or "HARDCOVER").upper()
    order_id_long = doc.get("order_id_long") or order_id
    name = doc.get("name")
    product_name = generate_book_title(book_id, name)

    # order date "YYYY-MM-DD HH:MM" (IST)
    dt = doc.get("processed_at") or doc.get("created_at")
    try:
        if isinstance(dt, str):
            dt = parser.isoparse(dt)
        if isinstance(dt, datetime) and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        order_date = (
            (dt or datetime.now(timezone.utc))
            .astimezone(IST_TZ)
            .strftime("%Y-%m-%d %H:%M")
        )
    except Exception:
        order_date = datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M")

    # ---- PICKUP LOCATION BASED ON PRINTER ----
    printer = (doc.get("printer") or "").strip().lower()

    if printer == "yara":
        # must match the pickup name configured in Shiprocket
        pickup_name = "Diffrun"
    elif printer == "genesis":
        # must match the pickup name configured in Shiprocket
        pickup_name = "warehouse-1"

    if not pickup_name:
        raise HTTPException(
            status_code=400, detail="Shiprocket pickup_location not configured"
        )

    # payment
    cod = bool(doc.get("payment_method") == "COD")

    return {
        "order_id": str(doc.get("order_id") or ""),
        "order_date": order_date,
        "pickup_location": pickup_name,
        "comment": doc.get("comment", ""),

        "billing_customer_name": first or ship.get("name", "") or "Customer",
        "billing_last_name": last,
        "billing_address": ship.get("address1", ""),
        "billing_address_2": ship.get("address2", ""),
        "billing_city": ship.get("city", ""),
        "billing_pincode": str(ship.get("zip", ""))[:6],
        "billing_state": ship.get("province", ""),
        "billing_country": ship.get("country", "India"),
        "billing_email": (doc.get("email") or doc.get("customer_email") or ""),
        "billing_phone": ship.get("phone") or doc.get("phone_number") or "",

        "shipping_is_billing": True,
        "shipping_customer_name": "",
        "shipping_last_name": "",
        "shipping_address": "",
        "shipping_address_2": "",
        "shipping_city": "",
        "shipping_pincode": "",
        "shipping_country": "",
        "shipping_state": "",
        "shipping_email": "",
        "shipping_phone": "",

        "order_items": [
            {
                "name": f"{product_name}",
                "sku": f"{order_id_long}",
                "units": qty,
                "selling_price": float(round(subtotal / max(qty, 1), 2)),
                "discount": 0,
                "tax": 0,
                "hsn": "",
            }
        ],
        "payment_method": "COD" if cod else "Prepaid",
        "shipping_charges": 0,
        "giftwrap_charges": 0,
        "transaction_charges": 0,
        "total_discount": 0,
        "sub_total": float(subtotal),

        "length": length,
        "breadth": breadth,
        "height": height,
        "weight": weight,
    }



@app.post("/shiprocket/create-from-orders", tags=["shiprocket"])
def shiprocket_create_from_orders(
    order_ids: List[str] = Body(..., embed=True,
                                description="Diffrun order_ids like ['#123', '#124']"),
    assign_awb: bool = Body(
        True, embed=True, description="If true, assign AWB after creating order"),
    request_pickup: bool = Body(
        True, embed=True, description="If true, generate pickup after AWB assignment"),
    generate_label: bool = Body(
        True, embed=True, description="If true, generate label after AWB assignment"),
):
    """
    Creates Shiprocket orders for the provided order_ids (reads delivery details from Mongo),
    assigns AWB, generates labels and requests pickup by default.
    """
    if not order_ids:
        raise HTTPException(status_code=400, detail="order_ids required")

    # dedupe, preserve order
    seen, unique_ids = set(), []
    for oid in order_ids:
        if oid not in seen:
            seen.add(oid)
            unique_ids.append(oid)

    token = _sr_login_token()
    headers = _sr_headers(token)

    created_refs: List[Dict[str, Any]] = []
    shipment_ids: List[int] = []
    errors: List[str] = []

    # 1) Create orders
    for oid in unique_ids:
        doc = orders_collection.find_one({"order_id": oid})
        if not doc:
            errors.append(f"{oid}: not found")
            continue

        try:
            # avoid duplicate create if already created
            existing_sid = doc.get("sr_shipment_id")
            existing_soid = doc.get("sr_order_id")
            if existing_sid:
                created_refs.append({"order_id": oid, "sr_order_id": existing_soid,
                                    "shipment_id": existing_sid, "skipped_create": True})
                shipment_ids.append(int(existing_sid))
                continue

            payload = _sr_order_payload_from_doc(doc)
            r = requests.post(
                f"{SHIPROCKET_BASE}/v1/external/orders/create/adhoc",
                headers=headers, json=payload, timeout=40
            )
            if r.status_code != 200:
                errors.append(f"{oid}: create failed {r.status_code} {r.text}")
                continue

            j = r.json() or {}
            sr_order_id = j.get("order_id")
            shipment_id = j.get("shipment_id")

            orders_collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "sr_order_id": sr_order_id,
                    "sr_shipment_id": shipment_id,
                    "shiprocket_created_at": datetime.utcnow().isoformat(),
                    "shiprocket_pickup_location": payload.get("pickup_location")
                }}
            )

            created_refs.append(
                {"order_id": oid, "sr_order_id": sr_order_id, "shipment_id": shipment_id})
            if shipment_id:
                shipment_ids.append(int(shipment_id))
        except Exception as e:
            errors.append(f"{oid}: exception {e}")

    # If caller didn't request AWB, stop here (keeps orders unassigned)

        # 2) Assign AWB
    awb_results: List[Dict[str, Any]] = []
    for sid in shipment_ids:
        try:
            existing = orders_collection.find_one({"sr_shipment_id": sid})
            if existing and existing.get("awb_code"):
                awb_results.append({
                    "shipment_id": sid,
                    "awb_code": existing.get("awb_code"),
                    "courier_company_id": existing.get("courier_company_id"),
                    "skipped_assign": True
                })
                continue

            rr = requests.post(
                f"{SHIPROCKET_BASE}/v1/external/courier/assign/awb",
                headers=headers,
                json={"shipment_id": sid},
                timeout=30
            )

            if rr.status_code != 200:
                # AWB assign failed
                errors.append(f"awb({sid}) failed {rr.status_code}: {rr.text}")
                continue

            # Shiprocket docs say this may return no body.
            # Be defensive: if body is empty, json() will raise.
            try:
                j = rr.json() or {}
            except ValueError:
                j = {}

            awb_code = j.get("awb_code")          # may be None
            courier_id = j.get("courier_company_id")

            awb_results.append({
                "shipment_id": sid,
                "awb_code": awb_code,
                "courier_company_id": courier_id
            })

            # Even if awb_code is None, AWB is most likely assigned internally.
            update_fields: Dict[str, Any] = {}
            if awb_code is not None:
                update_fields["awb_code"] = awb_code
            if courier_id is not None:
                update_fields["courier_company_id"] = courier_id

            # Only update if we actually have something to set
            if update_fields:
                orders_collection.update_one(
                    {"sr_shipment_id": sid},
                    {"$set": update_fields}
                )

        except Exception as e:
            errors.append(f"awb({sid}): exception {e}")

        # 3) Generate label (use shipment_id; don't require awb_code in response)
    label_res = {}
    if generate_label and awb_results:
        label_shipments = [int(x["shipment_id"]) for x in awb_results if x.get("shipment_id")]

        for sid in label_shipments:
            # skip if this shipment already has a label_url
            doc = orders_collection.find_one({"sr_shipment_id": sid})
            if doc and doc.get("label_url"):
                continue

            try:
                payload = {"shipment_id": [sid]}
                lr = requests.post(
                    f"{SHIPROCKET_BASE}/v1/external/courier/generate/label",
                    headers=headers,
                    json=payload,
                    timeout=60,
                )

                if lr.status_code != 200:
                    errors.append(
                        f"label generation failed for {sid} {lr.status_code}: {lr.text}"
                    )
                    continue

                lj = lr.json() or {}
                label_res[str(sid)] = lj

                label_url = lj.get("label_url")
                not_created = lj.get("not_created") or []

                failed_ids = {int(x) for x in not_created if str(x).isdigit()}

                # if Shiprocket created label for this shipment_id
                if label_url and sid not in failed_ids:
                    orders_collection.update_one(
                        {"sr_shipment_id": sid},
                        {
                            "$set": {
                                "label_url": label_url,
                                "label_created_at": datetime.utcnow().isoformat(),
                            }
                        },
                    )
                else:
                    errors.append(
                        f"label_not_created_for: shipment_id={sid}, response={lj}"
                    )

            except Exception as e:
                errors.append(f"label generation exception for {sid}: {e}")

    # 4) Generate pickup
    pickup_res = None
    if request_pickup and awb_results:
        try:
            rr = requests.post(
                f"{SHIPROCKET_BASE}/v1/external/courier/generate/pickup",
                headers=headers,
                json={"shipment_id": [x["shipment_id"] for x in awb_results]},
                timeout=30
            )
            if rr.status_code == 200:
                pickup_res = rr.json()
                orders_collection.update_many(
                    {"sr_shipment_id": {
                        "$in": [x["shipment_id"] for x in awb_results]}},
                    {"$set": {"pickup_requested": True,
                              "pickup_requested_at": datetime.utcnow().isoformat()}}
                )
            else:
                errors.append(f"pickup failed {rr.status_code}: {rr.text}")
        except Exception as e:
            errors.append(f"pickup: exception {e}")

    return {"created": created_refs, "awbs": awb_results, "pickup": pickup_res, "labels": label_res, "errors": errors}


def _sr_get_shipment_tracking_with_retries(shipment_id: int, headers: Dict[str, str], tries: int = 3) -> Dict[str, Any]:
    url = f"{SHIPROCKET_BASE}/v1/external/courier/track/shipment/{shipment_id}"
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=20)
        except Exception as e:
            return {"ok": False, "exception": str(e)}

        if r.status_code == 200:
            try:
                return {"ok": True, "json": r.json()}
            except:
                return {"ok": False, "status_code": r.status_code, "text": "invalid json"}

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else min(6, 2 ** attempt)
            time.sleep(wait)
            continue

        return {"ok": False, "status_code": r.status_code, "text": r.text}

    return {"ok": False, "status_code": 429, "text": "Too Many Attempts"}


# ============================================================
#   FINAL VERSION — PER SHIPMENT LABEL GENERATION
# ============================================================
@app.post("/shiprocket/sync-missing-labels", tags=["shiprocket"])
def shiprocket_sync_missing_labels(
    batch_size: int = 40,
    printer: str = Body("genesis")
):
    printer = (printer or "genesis").strip().lower()
    if printer not in ("genesis", "yara"):
        raise HTTPException(status_code=400, detail="Invalid printer")

    token = _sr_login_token()
    headers = _sr_headers(token)

    # ------- 1. Find orders missing label_url -------
    query = {
        "paid": True,
        "printer": {"$regex": f"^{re.escape(printer)}$", "$options": "i"},
        "sr_shipment_id": {"$exists": True, "$ne": None},
        "$or": [
            {"label_url": {"$exists": False}},
            {"label_url": ""},
            {"label_url": None},
        ],
    }

    docs = list(
        orders_collection.find(
            query,
            {"order_id": 1, "sr_shipment_id": 1}
        )
    )

    candidates = {}
    for d in docs:
        sid = d.get("sr_shipment_id")
        if sid is None:
            continue
        try:
            candidates[int(sid)] = d.get("order_id")
        except:
            continue

    if not candidates:
        return {"message": "No candidate orders", "matched_docs": len(docs)}

    eligible_shipments = []
    skipped = {}

    all_sids = list(candidates.keys())

    # ------- 2. Determine which shipments have status == 19 -------
    for i in range(0, len(all_sids), batch_size):
        batch = all_sids[i: i + batch_size]

        for sid in batch:
            tr = _sr_get_shipment_tracking_with_retries(sid, headers, tries=3)

            if not tr.get("ok"):
                skipped[sid] = {"reason": "tracking_failed", "resp": tr}
                continue

            tracking_json = tr["json"]
            td = tracking_json.get("tracking_data")

            if not td:
                skipped[sid] = {"reason": "no_tracking_data",
                                "resp": tracking_json}
                continue

            status_code = td.get("shipment_status")

            try:
                if int(status_code) == 19 or int(status_code) == 3 :     # STRICT RULE
                    eligible_shipments.append(sid)
                else:
                    skipped[sid] = {
                        "reason": "shipment_status_not_19",
                        "shipment_status": status_code
                    }
            except:
                skipped[sid] = {
                    "reason": "invalid_status",
                    "shipment_status": status_code
                }

        time.sleep(1)

    if not eligible_shipments:
        return {
            "message": "No eligible shipments",
            "eligible_count": 0,
            "skipped": skipped
        }

    # ============================================================
    # 3. GENERATE LABEL FOR EACH SHIPMENT (one-by-one, not batch)
    # ============================================================
    succeeded = []
    failed = []
    per_label_results = {}

    def generate_label_single(sid):
        url = f"{SHIPROCKET_BASE}/v1/external/courier/generate/label"
        payload = {"shipment_id": [sid]}

        try:
            r = requests.post(url, headers=headers, json=payload, timeout=60)
            if r.status_code != 200:
                return {"ok": False, "status_code": r.status_code, "text": r.text}
            return {"ok": True, "json": r.json()}
        except Exception as e:
            return {"ok": False, "exception": str(e)}

    # ------- Loop each shipment individually -------
    for sid in eligible_shipments:
        time.sleep(0.5)   # IMPORTANT: avoid 429 rate limit

        res = generate_label_single(sid)
        per_label_results[sid] = res

        if not res.get("ok"):
            failed.append(sid)
            skipped[sid] = {"reason": "label_api_failed", "resp": res}
            continue

        lj = res["json"]
        label_url = lj.get("label_url")
        not_created = lj.get("not_created") or []

        if label_url and sid not in [int(x) for x in not_created]:
            orders_collection.update_one(
                {"sr_shipment_id": sid},
                {"$set": {
                    "label_url": label_url,
                    "label_created_at": datetime.utcnow().isoformat()
                }}
            )
            succeeded.append(sid)
        else:
            failed.append(sid)
            skipped[sid] = {"reason": "label_not_created", "response": lj}

    return {
        "message": "Labels generated individually",
        "eligible_shipments": eligible_shipments,
        "succeeded_shipments": succeeded,
        "failed_shipments": failed,
        "results": per_label_results,
        "skipped": skipped,
    }


@app.get("/shiprocket/test-tracking/{shipment_id}", tags=["shiprocket"])
def shiprocket_test_tracking(shipment_id: int):
    token = _sr_login_token()
    headers = _sr_headers(token)
    url = f"{SHIPROCKET_BASE}/v1/external/courier/track/shipment/{shipment_id}"

    try:
        r = requests.get(url, headers=headers, timeout=30)
        return {"json": r.json() if r.status_code == 200 else r.text}
    except Exception as e:
        return {"ok": False, "error": str(e)}
