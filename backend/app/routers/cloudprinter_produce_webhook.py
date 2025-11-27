import json, time, hmac, os, smtplib
from email.message import EmailMessage
from fastapi import APIRouter, Request, HTTPException, status, Depends, Response, BackgroundTasks
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

router = APIRouter()
security = HTTPBasic(auto_error=False)

WEBHOOK_KEY = (os.getenv("CLOUDPRINTER_WEBHOOK_KEY") or "").strip()
BASIC_USER  = (os.getenv("CP_WEBHOOK_USER") or "").strip()
BASIC_PASS  = (os.getenv("CP_WEBHOOK_PASS") or "").strip()
EMAIL_USER  = (os.getenv("EMAIL_ADDRESS") or "").strip()
EMAIL_PASS  = (os.getenv("EMAIL_PASSWORD") or "").strip()

def _eq(a: str, b: str) -> bool:
    return hmac.compare_digest(str(a or ""), str(b or ""))

class ItemProducePayload(BaseModel):
    apikey: str
    type: str  # "ItemProduce"
    order: str
    item: str
    order_reference: str
    item_reference: str
    datetime: str  # ISO 8601

def _send_production_email(
    to_email: str,
    display_name: str,
    child_name: str,
    job_id: str | None,
):
    if not to_email:
        print("[MAIL] skipped: empty recipient for production email")
        return

    display = (display_name or "there").strip().title() or "there"
    child   = (child_name or "Your").strip().title() or "Your"
    track_href = f"https://diffrun.com/track-your-order?job_id={job_id}" if job_id else "https://diffrun.com/track-your-order"

    subject = f"{child}'s storybook is now in production 🎉"

    html = f"""\
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="color-scheme" content="light">
            <meta name="supported-color-schemes" content="light">
            <style>
                @media only screen and (max-width: 480px) {{
                    .container {{
                        width: 100% !important;
                        max-width: 100% !important;
                        padding: 16px !important;
                    }}
                    .col, .img-col {{
                        display: block !important;
                        width: 100% !important;
                    }}
                    .img-col img {{
                        width: 100% !important;
                        height: auto !important;
                    }}
                    .browse-now-btn {{
                        font-size: 14px !important;
                        padding: 12px 16px !important;
                    }}
                    p, li, a {{
                        font-size: 15px !important;
                        line-height: 1.5 !important;
                    }}
                }}
            </style>
        </head>
        <body style="font-family: Arial, sans-serif; background:#f7f7f7; margin:0; padding:20px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">
                <tr>
                    <td align="center">
                        <table role="presentation" class="container" width="100%" cellpadding="0" cellspacing="0" border="0"
                            style="max-width: 48rem; margin: 0 auto; background:#ffffff; border-radius:8px; box-shadow:0 0 10px rgba(0,0,0,0.08); overflow:hidden;">
                            <tr>
                                <td style="padding:24px;">
                                    <p>Hey {display},</p>
                                    <p><strong>{child}'s storybook</strong> has been moved to production at our print factory. 🎉</p>
                                    <p>It will be shipped within the next 3–4 business days. We will notify you with the tracking ID once your order is shipped.</p>
                                    <a href="{track_href}"
                                        style="display: inline-block; background:#5784ba; color: white; text-decoration: none; border-radius: 18px; padding: 12px 24px; font-weight: bold;">
                                        Track your order
                                    </a>
                                    <p>Thanks,<br />Team Diffrun</p>
                                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                                        style="margin-top: 30px; background-color: #f7f6cf; border-radius: 8px;">
                                        <tr>
                                            <td class="col" style="padding: 20px; vertical-align: middle;">
                                                <p style="font-size: 15px; margin: 0;">
                                                    Explore more magical books in our growing collection &nbsp;
                                                    <button class="browse-now-btn"
                                                        style="background-color:#5784ba; margin-top: 20px; border-radius: 30px; border: none; padding:10px 15px;">
                                                        <a href="https://diffrun.com"
                                                            style="color:white; font-weight: bold; text-decoration: none; display:inline-block;">
                                                            Browse Now
                                                        </a>
                                                    </button>
                                                </p>
                                            </td>
                                            <td class="img-col" width="300"
                                                style="padding: 0 20px 0 0; margin: 0; vertical-align: middle;">
                                                <img src="https://diffrungenerations.s3.ap-south-1.amazonaws.com/email_image+(2).jpg"
                                                    alt="Storybook Preview" width="300"
                                                    style="display: block; border-radius: 0; margin: 0; padding: 0;">
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Diffrun <{EMAIL_USER}>"
    msg["To"] = to_email
    msg.set_content("Your book has moved to production. View this email in HTML to see the formatted message.")
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)

@router.post("/api/webhook/cloudprinter/produce")
@router.post("/api/webhook/cloudprinter/produce/")
async def cloudprinter_itemproduce_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    credentials: HTTPBasicCredentials | None = Depends(security),
):
    t0 = time.perf_counter()

    if BASIC_USER and BASIC_PASS:
        if not credentials or not (_eq(credentials.username, BASIC_USER) and _eq(credentials.password, BASIC_PASS)):
            print(f"[CP PRODUCE] 401 basic-auth failed (user={getattr(credentials,'username',None)!r})")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    raw = await request.body()
    remote = request.client.host if request.client else "?"
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        print(f"[CP PRODUCE] <-- {remote} invalid JSON (size={len(raw)}B)")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not _eq(payload.get("apikey"), WEBHOOK_KEY):
        print(f"[CP PRODUCE] 401 bad webhook key for order_ref={payload.get('order_reference')}")
        raise HTTPException(status_code=401, detail="Bad webhook apikey")

    evt = payload.get("type")
    order_ref = payload.get("order_reference")
    print(f"[CP PRODUCE] <-- {remote} type={evt} order_ref={order_ref}")

    if evt != "ItemProduce":
        return Response(status_code=204)

    data = ItemProducePayload(**payload)

    try:
        from main import orders_collection
    except Exception as e:
        print(f"[CP PRODUCE] DB import error: {e}")
        raise HTTPException(status_code=500, detail="Server misconfiguration")

    update_fields = {
        "print_status": "in_production",
        "production_started_at": data.datetime,
        "cp_order_id": data.order,
        "cp_item_id": data.item,
        "cp_item_reference": data.item_reference,
    }
    res = orders_collection.update_one({"order_id": data.order_reference}, {"$set": update_fields})

    if res.matched_count == 0:
        print(f"[CP PRODUCE] order not found for order_ref={data.order_reference} -> 204")
        return Response(status_code=204)

    # Idempotent email gate
    once = orders_collection.update_one(
        {"order_id": data.order_reference, "$or": [{"production_email_sent": {"$exists": False}}, {"production_email_sent": False}]},
        {"$set": {"production_email_sent": True}}
    )

    if once.modified_count == 1:
        order = orders_collection.find_one(
            {"order_id": data.order_reference},
            {"customer_email": 1, "email": 1, "user_name": 1, "name": 1, "job_id": 1, "_id": 0},
        )
        to_email = (order.get("customer_email") or order.get("email") or "").strip() if order else ""
        user_name = order.get("user_name") if order else None
        name = order.get("name") if order else None
        job_id = order.get("job_id") if order else None

        if to_email and EMAIL_USER and EMAIL_PASS:
            background_tasks.add_task(
                _send_production_email,
                to_email,
                user_name or "there",
                name or "Your",
                job_id,
            )
            print(f"[CP PRODUCE] queued production email to {to_email} for {data.order_reference}")
        else:
            print(f"[CP PRODUCE] email skipped (to={to_email!r}) for {data.order_reference}")

    dt_ms = (time.perf_counter() - t0) * 1000
    print(f"[CP PRODUCE] --> 200 ok ({dt_ms:.1f} ms) ItemProduce {order_ref}")
    return {"ok": True}
