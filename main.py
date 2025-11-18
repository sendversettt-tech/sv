from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import csv
import io
import threading
import time
import smtplib
from email.mime.text import MIMEText
from typing import Dict, Any, List

app = FastAPI()
security = HTTPBasic()

# ========= MANUAL USERS =========
# You can manually add/remove users here
USERS = {
    "user1": "pass1",
    "user2": "pass2",
    # "anotheruser": "anotherpass",
}

# ========= IN-MEMORY CAMPAIGNS =========
# campaign_id -> campaign_data
CAMPAIGNS: Dict[str, Dict[str, Any]] = {}
_campaign_counter = 0
_campaign_lock = threading.Lock()


def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password
    real_pass = USERS.get(username)
    if real_pass is None or real_pass != password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return username


def parse_contacts_file(file_bytes: bytes, filename: str) -> List[Dict[str, str]]:
    """
    Very simple CSV parser: expects columns like: name,email
    For now we only support CSV to keep things minimal.
    """
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV is supported in this minimal version")

    text = file_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    contacts = []
    for row in reader:
        email = (row.get("email") or row.get("Email") or "").strip()
        if not email:
            continue
        name = (row.get("name") or row.get("Name") or "").strip()
        contacts.append({"name": name, "email": email})
    if not contacts:
        raise HTTPException(status_code=400, detail="No valid contacts found in CSV")
    return contacts


def send_email_smtp(
    host: str,
    port: int,
    username: str,
    password: str,
    use_tls: bool,
    from_email: str,
    to_email: str,
    subject: str,
    html: str,
):
    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    with smtplib.SMTP(host, port, timeout=30) as server:
        if use_tls:
            server.starttls()
        if username and password:
            server.login(username, password)
        server.sendmail(from_email, [to_email], msg.as_string())


def render_template(html_body: str, contact: Dict[str, str]) -> str:
    # Very basic placeholder replacement
    result = html_body.replace("{{name}}", contact.get("name", ""))
    result = result.replace("{{email}}", contact.get("email", ""))
    return result


def run_campaign(campaign_id: str):
    camp = CAMPAIGNS.get(campaign_id)
    if not camp:
        return

    contacts = camp["contacts"]
    speed = camp["speed_per_minute"]
    delay = 60.0 / speed if speed > 0 else 0.0

    camp["status"] = "running"

    for idx, contact in enumerate(contacts):
        # Check if user stopped the campaign
        if camp["status"] == "stopped":
            break

        try:
            personalized_html = render_template(camp["html_body"], contact)
            send_email_smtp(
                host=camp["smtp_host"],
                port=camp["smtp_port"],
                username=camp["smtp_username"],
                password=camp["smtp_password"],
                use_tls=camp["smtp_use_tls"],
                from_email=camp["from_email"],
                to_email=contact["email"],
                subject=camp["subject"],
                html=personalized_html,
            )
            camp["sent"] += 1
        except Exception as e:
            camp["failed"] += 1
            # For simplicity, keep only last error
            camp["last_error"] = str(e)

        camp["processed"] += 1

        if delay > 0 and idx < len(contacts) - 1:
            time.sleep(delay)

    if camp["status"] != "stopped":
        camp["status"] = "finished"


@app.post("/start_campaign")
async def start_campaign(
    subject: str = Form(...),
    html_body: str = Form(...),
    smtp_host: str = Form(...),
    smtp_port: int = Form(...),
    smtp_username: str = Form(""),
    smtp_password: str = Form(""),
    smtp_use_tls: bool = Form(True),
    from_email: str = Form(...),
    speed_per_minute: int = Form(60),
    contacts_file: UploadFile = File(...),
    current_user: str = Depends(get_current_user),
):
    file_bytes = await contacts_file.read()
    contacts = parse_contacts_file(file_bytes, contacts_file.filename)

    global _campaign_counter
    with _campaign_lock:
        _campaign_counter += 1
        campaign_id = f"{current_user}-{_campaign_counter}"

    CAMPAIGNS[campaign_id] = {
        "user": current_user,
        "subject": subject,
        "html_body": html_body,
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "smtp_username": smtp_username,
        "smtp_password": smtp_password,
        "smtp_use_tls": smtp_use_tls,
        "from_email": from_email,
        "speed_per_minute": max(1, speed_per_minute),
        "contacts": contacts,
        "status": "queued",
        "processed": 0,
        "sent": 0,
        "failed": 0,
        "last_error": None,
    }

    # Start background thread
    t = threading.Thread(target=run_campaign, args=(campaign_id,), daemon=True)
    t.start()

    return {
        "campaign_id": campaign_id,
        "total_contacts": len(contacts),
        "message": "Campaign started",
    }


@app.get("/campaign_status/{campaign_id}")
def campaign_status(campaign_id: str, current_user: str = Depends(get_current_user)):
    camp = CAMPAIGNS.get(campaign_id)
    if not camp or camp["user"] != current_user:
        raise HTTPException(status_code=404, detail="Campaign not found")

    return {
        "campaign_id": campaign_id,
        "status": camp["status"],
        "processed": camp["processed"],
        "sent": camp["sent"],
        "failed": camp["failed"],
        "last_error": camp["last_error"],
        "total": len(camp["contacts"]),
    }


@app.post("/stop_campaign/{campaign_id}")
def stop_campaign(campaign_id: str, current_user: str = Depends(get_current_user)):
    camp = CAMPAIGNS.get(campaign_id)
    if not camp or camp["user"] != current_user:
        raise HTTPException(status_code=404, detail="Campaign not found")

    camp["status"] = "stopped"
    return {"message": "Campaign stop requested", "campaign_id": campaign_id}


@app.get("/", response_class=HTMLResponse)
def root():
    # Serve index.html from the same folder
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse("<h1>Backend is running.</h1>", status_code=200)

