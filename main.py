import os
import csv
import io
import threading
import time
from typing import Dict, Any, List

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from email.mime.text import MIMEText
import smtplib

app = FastAPI()
security = HTTPBasic()

# ========= MANUAL USERS =========
# Edit this dict to add/remove logins
USERS = {
    "user1": "pass1",
    "user2": "pass2",
    # "anotheruser": "anotherpass",
}

# ========= IN-MEMORY CAMPAIGNS (live sending) =========
CAMPAIGNS: Dict[str, Dict[str, Any]] = {}
_campaign_counter = 0
_campaign_lock = threading.Lock()

# ========= DATABASE (PostgreSQL) =========
DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="Database not configured (DATABASE_URL missing).")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL not set; SMTP profiles won't persist.")
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS smtp_profiles (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                name TEXT,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                use_tls BOOLEAN NOT NULL DEFAULT TRUE,
                smtp_username TEXT,
                smtp_password TEXT,
                from_email TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_smtp_profiles_username ON smtp_profiles(username);
            """
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


@app.on_event("startup")
def on_startup():
    init_db()


# ========= AUTH =========
def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password
    real_pass = USERS.get(username)
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
        contacts.append({"name": name, "email": email})
    if not contacts:
        raise HTTPException(status_code=400, detail="No valid contacts found in CSV")
    return contacts


# ========= SMTP SENDING =========
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
    result = html_body.replace("{{name}}", contact.get("name", ""))
    result = result.replace("{{email}}", contact.get("email", ""))
    return result


# ========= SMTP PROFILES (DB) =========
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
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            INSERT INTO smtp_profiles (username, name, host, port, use_tls, smtp_username, smtp_password, from_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, created_at
            """,
            (current_user, name, host, port, use_tls, smtp_username, smtp_password, from_email),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return {"id": row["id"], "created_at": row["created_at"]}
    finally:
        conn.close()


@app.get("/smtp_profiles")
def list_smtp_profiles(current_user: str = Depends(get_current_user)):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id, name, host, port, use_tls, smtp_username, smtp_password, from_email, created_at
            FROM smtp_profiles
            WHERE username = %s
            ORDER BY created_at DESC
            """,
            (current_user,),
        )
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


# ========= CAMPAIGN LOGIC (in-memory live, list via API) =========
def run_campaign(campaign_id: str):
    camp = CAMPAIGNS.get(campaign_id)
    if not camp:
        return

    contacts = camp["contacts"]
    speed = camp["speed_per_minute"]
    delay = 60.0 / speed if speed > 0 else 0.0

    camp["status"] = "running"

    for idx, contact in enumerate(contacts):
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
            camp["delivered"] += 1
        except Exception as e:
            camp["failed"] += 1
            camp["bounced"] += 1
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
        "delivered": 0,
        "bounced": 0,
        "last_error": None,
        "created_at": time.time(),
        "total": len(contacts),
    }

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
        "subject": camp["subject"],
        "status": camp["status"],
        "processed": camp["processed"],
        "sent": camp["sent"],
        "failed": camp["failed"],
        "delivered": camp["delivered"],
        "bounced": camp["bounced"],
        "last_error": camp["last_error"],
        "total": camp["total"],
        "created_at": camp["created_at"],
    }


@app.post("/stop_campaign/{campaign_id}")
def stop_campaign(campaign_id: str, current_user: str = Depends(get_current_user)):
    camp = CAMPAIGNS.get(campaign_id)
    if not camp or camp["user"] != current_user:
        raise HTTPException(status_code=404, detail="Campaign not found")

    camp["status"] = "stopped"
    return {"message": "Campaign stop requested", "campaign_id": campaign_id}


@app.get("/campaigns")
def list_campaigns(current_user: str = Depends(get_current_user)):
    """Return all campaigns for this user (current server session)."""
    result = []
    for cid, camp in CAMPAIGNS.items():
        if camp["user"] != current_user:
            continue
        result.append({
            "campaign_id": cid,
            "subject": camp["subject"],
            "status": camp["status"],
            "total": camp["total"],
            "delivered": camp["delivered"],
            "bounced": camp["bounced"],
            "processed": camp["processed"],
            "created_at": camp["created_at"],
        })
    result.sort(key=lambda c: c["created_at"], reverse=True)
    return result


@app.get("/", response_class=HTMLResponse)
def root():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse("<h1>SendVerse backend is running.</h1>", status_code=200)
