# app/routers/cloudprinter_webhook.py
import json
import time
import hmac
import os
import smtplib
import urllib.parse
from email.message import EmailMessage
from fastapi import APIRouter, Request, HTTPException, status, Depends, BackgroundTasks
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

router = APIRouter()
security = HTTPBasic(auto_error=False)

WEBHOOK_KEY = (os.getenv("CLOUDPRINTER_WEBHOOK_KEY") or "").strip()
BASIC_USER = (os.getenv("CP_WEBHOOK_USER") or "").strip()
BASIC_PASS = (os.getenv("CP_WEBHOOK_PASS") or "").strip()
EMAIL_USER = (os.getenv("EMAIL_ADDRESS") or "").strip()
EMAIL_PASS = (os.getenv("EMAIL_PASSWORD") or "").strip()

# Provider-specific tracking URL templates (keep as constants; don't hardcode inline)
CLOUDPRINTER_TRACKING_URL_TEMPLATE = "https://parcelsapp.com/en/tracking/{tracking}"


def _eq(a: str, b: str) -> bool:
    return hmac.compare_digest(str(a or ""), str(b or ""))


class ItemShippedPayload(BaseModel):
    apikey: str
    type: str  # must be "ItemShipped"
    order_reference: str
    order: str | None = None
    item: str | None = None
    item_reference: str | None = None
    tracking: str
    shipping_option: str
    datetime: str  # ISO 8601 from Cloudprinter


def _tracking_link(shipping_option: str, tracking: str) -> str:
    """
    Backwards-compatible fallback. Keeps existing behavior if no template/override is provided.
    """
    if shipping_option and tracking:
        return f"https://parcelsapp.com/en/tracking/{tracking}"
    return ""


def _send_tracking_email(to_email: str,
                         order_ref: str,
                         shipping_option: str,
                         tracking: str,
                         user_name: str | None = None,
                         name: str | None = None,
                         tracking_url_template: str | None = None,
                         tracking_url_override: str | None = None
                         ):
    """
    Send shipped email.

    Priority for deciding tracking URL:
      1) tracking_url_override (full URL, used as-is)
      2) tracking_url_template (format string containing "{tracking}")
      3) fallback _tracking_link(shipping_option, tracking)

    The function will URL-encode the tracking token when using a template.
    """
    if not to_email:
        print(f"[MAIL] skipped: empty recipient for order {order_ref}")
        return

    display_name = (user_name or "there").strip().title() or "there"
    child_name = (name or "Your").strip().title() or "Your"

    # Build tracking URL (priority order)
    if tracking_url_override:
        track_url = tracking_url_override
    elif tracking_url_template and tracking:
        # URL-encode the tracking code to be safe for embedding in URLs
        safe_tracking = urllib.parse.quote_plus(tracking)
        try:
            track_url = tracking_url_template.format(tracking=safe_tracking)
        except Exception:
            # If template formatting fails, fallback to raw tracking insertion
            track_url = tracking_url_template.replace("{tracking}", safe_tracking)
    else:
        track_url = _tracking_link(shipping_option, tracking)

    track_button_html = (
        f"""
        <p style="margin: 20px 0;">
          <a href="{track_url}"
             style="background-color:#5784ba; color:#ffffff; text-decoration:none; font-weight:bold;
                    padding:12px 18px; border-radius:30px; display:inline-block;">
            Track your order
          </a>
        </p>
        """ if track_url else ""
    )

    subject = f"Your order from Diffrun {order_ref} has been shipped!"
    html = f"""
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
                  
                  <!-- Your original content (unchanged) -->
                  <p>Hey {display_name},</p>
                  Order Update! <strong>{child_name}'s storybook</strong> has been printed and is ready to be shipped. 🚚✨

                  <ul>
                    <li><strong>Order:</strong> {order_ref}</li>
                    <li><strong>Tracking:</strong> {tracking}</li>
                  </ul>

                  {track_button_html}

                  <p>Thanks,<br />Team Diffrun</p>

                  <!-- Explore More Row -->
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
                      <td class="img-col" width="300" style="padding: 0 20px 0 0; margin: 0; vertical-align: middle;">
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
    msg.set_content(
        "Your order has been shipped. View this email in HTML to see the formatted message.")
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)
    print(f"[MAIL] sent shipped-email to {to_email} for order {order_ref}")


@router.post("/api/webhook/cloudprinter")
@router.post("/api/webhook/cloudprinter/")
async def cloudprinter_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    credentials: HTTPBasicCredentials | None = Depends(security),
):
    t0 = time.perf_counter()

    # ---- Basic Auth (only if BOTH are configured)
    if BASIC_USER and BASIC_PASS:
        if not credentials or not (_eq(credentials.username, BASIC_USER) and _eq(credentials.password, BASIC_PASS)):
            print(
                f"[CP WEBHOOK] 401 basic-auth failed (user={getattr(credentials, 'username', None)!r})")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    # ---- parse JSON
    raw = await request.body()
    remote = request.client.host if request.client else "?"
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        print(f"[CP WEBHOOK] <-- {remote} invalid JSON (size={len(raw)}B)")
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # ---- apikey check
    if not _eq(payload.get("apikey"), WEBHOOK_KEY):
        print(
            f"[CP WEBHOOK] 401 bad webhook key for order_ref={payload.get('order_reference')}")
        raise HTTPException(status_code=401, detail="Bad webhook apikey")

    evt = payload.get("type")
    order_ref = payload.get("order_reference")
    print(f"[CP WEBHOOK] <-- {remote} type={evt} order_ref={order_ref}")

    # ---- only act on ItemShipped; ack others silently
    if evt != "ItemShipped":
        # 204: we intentionally do nothing for other events
        return {"status": "ignored"}

    # Validate payload shape
    data = ItemShippedPayload(**payload)

    # ---- DB work + idempotent email
    try:
        # Lazy import to avoid circular import with main.py
        from main import orders_collection
    except Exception as e:
        print(f"[CP WEBHOOK] DB import error: {e}")
        raise HTTPException(status_code=500, detail="Server misconfiguration")

    # 1) Update tracking fields (always) and set print_status to 'shipped'
    update_fields = {
        "tracking_code": data.tracking,
        "shipping_option": data.shipping_option,
        "shipped_at": data.datetime,
        "print_status": "shipped",
    }
    orders_collection.update_one({"order_id": data.order_reference}, {
                                 "$set": update_fields})

    # 2) Idempotent email: set shipped_email_sent=True only once; send email iff we flipped it now
    filter_once = {
        "order_id": data.order_reference,
        "$or": [{"shipped_email_sent": {"$exists": False}}, {"shipped_email_sent": False}],
    }
    set_once = {"$set": {"shipped_email_sent": True}}
    once = orders_collection.update_one(filter_once, set_once)

    if once.modified_count == 1:
        # We "won" the race to send the email → fetch recipient + name
        order = orders_collection.find_one(
            {"order_id": data.order_reference},
            {"customer_email": 1, "email": 1, "user_name": 1, "name": 1, "_id": 0},
        )
        to_email = (order.get("customer_email") or order.get("email")
                    or "").strip() if order else "support@diffrun.com"
        # to_email = "support@diffrun.com"
        user_name = (order.get("user_name") if order else None)
        name = order.get("name") if order else None

        if to_email:
            # queue email in background
            # pass the provider-specific template constant (clean, maintainable)
            background_tasks.add_task(
                _send_tracking_email,
                to_email,
                data.order_reference,
                data.shipping_option,
                data.tracking,
                user_name,
                name,
                CLOUDPRINTER_TRACKING_URL_TEMPLATE,
                None
            )
            print(
                f"[CP WEBHOOK] queued shipped-email to {to_email} for {data.order_reference}")
        else:
            print(
                f"[CP WEBHOOK] no customer_email/email in DB for {data.order_reference}; email skipped")
    else:
        print(
            f"[CP WEBHOOK] shipped-email already sent for {data.order_reference}; skipping")

    dt_ms = (time.perf_counter() - t0) * 1000
    print(f"[CP WEBHOOK] --> 200 ok ({dt_ms:.1f} ms) ItemShipped {order_ref}")
    return {"ok": True}
