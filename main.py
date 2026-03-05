import os
import csv
import io
import json
import threading
import time
from typing import Dict, Any, List
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from email.mime.text import MIMEText
import smtplib

app = FastAPI()
security = HTTPBasic()

# ========= FILE PATHS =========
USERS_FILE = "users.json"
SMTP_FILE = "smtp_profiles.json"
CAMPAIGNS_FILE = "campaigns.json"

file_lock = threading.Lock()

# ========= JSON HELPERS =========
def load_json(file):
    if not Path(file).exists():
        return {}
    with open(file, "r") as f:
        try:
            return json.load(f)
        except:
            return {}

def save_json(file, data):
    with file_lock:
        with open(file, "w") as f:
            json.dump(data, f, indent=2)

# ========= USERS =========
def get_users():
    return load_json(USERS_FILE)

# ========= IN-MEMORY CAMPAIGNS =========
CAMPAIGNS: Dict[str, Dict[str, Any]] = {}
_campaign_counter = 0
_campaign_lock = threading.Lock()

# ========= AUTH =========
def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    users = get_users()
    username = credentials.username
    password = credentials.password
    real_pass = users.get(username)

    if real_pass is None or real_pass != password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return username


@app.get("/me")
def me(current_user: str = Depends(get_current_user)):
    return {"user": current_user}

# ========= CSV PARSE =========
def parse_contacts_file(file_bytes: bytes, filename: str) -> List[Dict[str, str]]:
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV is supported")

    text = file_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    contacts = []

    for row in reader:
        email = (row.get("email") or row.get("Email") or "").strip()
        if not email:
            continue

        name = (row.get("name") or row.get("Name") or "").strip()

        contacts.append({
            "name": name,
            "email": email
        })

    if not contacts:
        raise HTTPException(status_code=400, detail="No valid contacts found")

    return contacts

# ========= SMTP SEND =========
def send_email_smtp(host, port, username, password, use_tls, from_email, to_email, subject, html):

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


def render_template(html_body, contact):

    result = html_body.replace("{{name}}", contact.get("name", ""))
    result = result.replace("{{email}}", contact.get("email", ""))

    return result

# ========= SMTP PROFILES =========
@app.post("/smtp_profiles")
def create_smtp_profile(
    name: str = Form(""),
    host: str = Form(...),
    port: int = Form(...),
    use_tls: bool = Form(True),
    smtp_username: str = Form(""),
    smtp_password: str = Form(""),
    from_email: str = Form(...),
    current_user: str = Depends(get_current_user),
):

    data = load_json(SMTP_FILE)

    user_profiles = data.get(current_user, [])

    new_id = len(user_profiles) + 1

    profile = {
        "id": new_id,
        "name": name,
        "host": host,
        "port": port,
        "use_tls": use_tls,
        "smtp_username": smtp_username,
        "smtp_password": smtp_password,
        "from_email": from_email,
        "created_at": time.time()
    }

    user_profiles.append(profile)
    data[current_user] = user_profiles

    save_json(SMTP_FILE, data)

    return profile


@app.get("/smtp_profiles")
def list_smtp_profiles(current_user: str = Depends(get_current_user)):

    data = load_json(SMTP_FILE)

    return data.get(current_user, [])

# ========= CAMPAIGN JSON STORAGE =========
def create_campaign_record(campaign_id, username, subject, total):

    data = load_json(CAMPAIGNS_FILE)

    data[campaign_id] = {
        "campaign_id": campaign_id,
        "username": username,
        "subject": subject,
        "status": "queued",
        "total": total,
        "processed": 0,
        "sent": 0,
        "failed": 0,
        "delivered": 0,
        "bounced": 0,
        "last_error": None,
        "created_at": time.time()
    }

    save_json(CAMPAIGNS_FILE, data)


def update_campaign_record(campaign_id, camp):

    data = load_json(CAMPAIGNS_FILE)

    if campaign_id not in data:
        return

    data[campaign_id]["status"] = camp["status"]
    data[campaign_id]["processed"] = camp["processed"]
    data[campaign_id]["sent"] = camp["sent"]
    data[campaign_id]["failed"] = camp["failed"]
    data[campaign_id]["delivered"] = camp["delivered"]
    data[campaign_id]["bounced"] = camp["bounced"]
    data[campaign_id]["last_error"] = camp["last_error"]

    save_json(CAMPAIGNS_FILE, data)

# ========= CAMPAIGN WORKER =========
def run_campaign(campaign_id):

    camp = CAMPAIGNS.get(campaign_id)

    if not camp:
        return

    contacts = camp["contacts"]
    speed = camp["speed_per_minute"]

    delay = 60.0 / speed if speed > 0 else 0

    camp["status"] = "running"

    update_campaign_record(campaign_id, camp)

    for idx, contact in enumerate(contacts):

        if camp["status"] == "stopped":
            break

        try:

            html = render_template(camp["html_body"], contact)

            send_email_smtp(
                camp["smtp_host"],
                camp["smtp_port"],
                camp["smtp_username"],
                camp["smtp_password"],
                camp["smtp_use_tls"],
                camp["from_email"],
                contact["email"],
                camp["subject"],
                html
            )

            camp["sent"] += 1
            camp["delivered"] += 1

        except Exception as e:

            camp["failed"] += 1
            camp["bounced"] += 1
            camp["last_error"] = str(e)

        camp["processed"] += 1

        update_campaign_record(campaign_id, camp)

        if delay > 0 and idx < len(contacts) - 1:
            time.sleep(delay)

    if camp["status"] != "stopped":
        camp["status"] = "finished"

    update_campaign_record(campaign_id, camp)

# ========= START CAMPAIGN =========
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
        "delivered": 0,
        "bounced": 0,
        "last_error": None,
        "created_at": time.time(),
        "total": len(contacts)
    }

    create_campaign_record(campaign_id, current_user, subject, len(contacts))

    t = threading.Thread(target=run_campaign, args=(campaign_id,), daemon=True)
    t.start()

    return {
        "campaign_id": campaign_id,
        "total_contacts": len(contacts),
        "message": "Campaign started"
    }

# ========= STATUS =========
@app.get("/campaign_status/{campaign_id}")
def campaign_status(campaign_id: str, current_user: str = Depends(get_current_user)):

    data = load_json(CAMPAIGNS_FILE)

    camp = data.get(campaign_id)

    if not camp or camp["username"] != current_user:
        raise HTTPException(status_code=404, detail="Campaign not found")

    return camp

# ========= STOP =========
@app.post("/stop_campaign/{campaign_id}")
def stop_campaign(campaign_id: str, current_user: str = Depends(get_current_user)):

    camp = CAMPAIGNS.get(campaign_id)

    if camp and camp["user"] == current_user:

        camp["status"] = "stopped"

        update_campaign_record(campaign_id, camp)

    else:

        data = load_json(CAMPAIGNS_FILE)

        if campaign_id in data and data[campaign_id]["username"] == current_user:

            data[campaign_id]["status"] = "stopped"

            save_json(CAMPAIGNS_FILE, data)

    return {"message": "Campaign stop requested", "campaign_id": campaign_id}

# ========= LIST CAMPAIGNS =========
@app.get("/campaigns")
def list_campaigns(current_user: str = Depends(get_current_user)):

    data = load_json(CAMPAIGNS_FILE)

    result = []

    for c in data.values():

        if c["username"] == current_user:

            result.append(c)

    result.sort(key=lambda x: x.get("created_at", 0), reverse=True)

    return result

# ========= ROOT =========
@app.get("/", response_class=HTMLResponse)
def root():

    try:

        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()

    except FileNotFoundError:

        return HTMLResponse("<h1>SendVerse backend running</h1>", status_code=200)
