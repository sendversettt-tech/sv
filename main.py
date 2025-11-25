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
    # "samual": "samual123",
    # "anotheruser": "anotherpass",
}

# ========= IN-MEMORY CAMPAIGNS (for live sending only) =========
# We still use this for the sending threads,
# but the canonical data is stored in PostgreSQL.
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
    """
    Create required tables if they don't exist:
      - smtp_profiles
      - campaigns
    """
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL not set; DB features will not work.")
        return
    conn = get_conn()
    try:
        cur = conn.cursor()
        # SMTP profiles table
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

        # Campaigns table (persistent history)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
                id SERIAL PRIMARY KEY,
                campaign_id TEXT UNIQUE NOT NULL,
                username TEXT NOT NULL,
                subject TEXT,
                status TEXT NOT NULL,
                total INTEGER NOT NULL DEFAULT 0,
                processed INTEGER NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0,
                bounced INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_campaigns_username ON campaigns(username);
            CREATE INDEX IF NOT EXISTS idx_campaigns_campaign_id ON campaigns(campaign_id);
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


# ========= CAMPAIGN DB HELPERS =========
def create_campaign_db(
    campaign_id: str,
    username: str,
    subject: str,
    total: int,
):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO campaigns (campaign_id, username, subject, status, total, processed,
                                   sent, failed, delivered, bounced, last_error)
            VALUES (%s, %s, %s, %s, %s, 0, 0, 0, 0, 0, NULL)
            """,
            (campaign_id, username, subject, "queued", total),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def update_campaign_db_stats(campaign_id: str, camp: Dict[str, Any]):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE campaigns
            SET status = %s,
                processed = %s,
                sent = %s,
                failed = %s,
                delivered = %s,
                bounced = %s,
                last_error = %s,
                updated_at = NOW()
            WHERE campaign_id = %s
            """,
            (
                camp["status"],
                camp["processed"],
                camp["sent"],
                camp["failed"],
                camp["delivered"],
                camp["bounced"],
                camp["last_error"],
                campaign_id,
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def set_campaign_db_status(campaign_id: str, username: str, new_status: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE campaigns
            SET status = %s,
                updated_at = NOW()
            WHERE campaign_id = %s AND username = %s
            """,
            (new_status, campaign_id, username),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


# ========= CAMPAIGN SENDING LOGIC =========
def run_campaign(campaign_id: str):
    """
    Worker thread: sends emails and updates the in-memory structure
    plus the PostgreSQL campaigns table.
    """
    camp = CAMPAIGNS.get(campaign_id)
    if not camp:
        return

    contacts = camp["contacts"]
    speed = camp["speed_per_minute"]
    delay = 60.0 / speed if speed > 0 else 0.0

    camp["status"] = "running"
    update_campaign_db_stats(campaign_id, camp)

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

        # Persist stats after each email (simple; you can later batch this)
        update_campaign_db_stats(campaign_id, camp)

        if delay > 0 and idx < len(contacts) - 1:
            time.sleep(delay)

    if camp["status"] != "stopped":
        camp["status"] = "finished"
    update_campaign_db_stats(campaign_id, camp)


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

    # In-memory structure for the worker
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

    # Create row in DB for persistent history
    create_campaign_db(campaign_id, current_user, subject, len(contacts))

    # Start worker thread
    t = threading.Thread(target=run_campaign, args=(campaign_id,), daemon=True)
    t.start()

    return {
        "campaign_id": campaign_id,
        "total_contacts": len(contacts),
        "message": "Campaign started",
    }


@app.get("/campaign_status/{campaign_id}")
def campaign_status(campaign_id: str, current_user: str = Depends(get_current_user)):
    """
    Always read from DB for persistent stats.
    """
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT campaign_id, subject, status, total, processed, sent, failed,
                   delivered, bounced, last_error, EXTRACT(EPOCH FROM created_at) AS created_at
            FROM campaigns
            WHERE campaign_id = %s AND username = %s
            """,
            (campaign_id, current_user),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            raise HTTPException(status_code=404, detail="Campaign not found")

        return {
            "campaign_id": row["campaign_id"],
            "subject": row["subject"],
            "status": row["status"],
            "processed": row["processed"],
            "sent": row["sent"],
            "failed": row["failed"],
            "delivered": row["delivered"],
            "bounced": row["bounced"],
            "last_error": row["last_error"],
            "total": row["total"],
            "created_at": row["created_at"],
        }
    finally:
        conn.close()


@app.post("/stop_campaign/{campaign_id}")
def stop_campaign(campaign_id: str, current_user: str = Depends(get_current_user)):
    # Stop in-memory worker if present
    camp = CAMPAIGNS.get(campaign_id)
    if camp and camp["user"] == current_user:
        camp["status"] = "stopped"
        update_campaign_db_stats(campaign_id, camp)
    else:
        # No in-memory worker (maybe after restart); still mark as stopped in DB
        set_campaign_db_status(campaign_id, current_user, "stopped")

    return {"message": "Campaign stop requested", "campaign_id": campaign_id}


@app.get("/campaigns")
def list_campaigns(current_user: str = Depends(get_current_user)):
    """
    Return all campaigns for this user from DB (persistent history).
    """
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT campaign_id, subject, status, total, delivered, bounced, processed,
                   EXTRACT(EPOCH FROM created_at) AS created_at
            FROM campaigns
            WHERE username = %s
            ORDER BY created_at DESC
            """,
            (current_user,),
        )
        rows = cur.fetchall()
        cur.close()

        result = []
        for row in rows:
            result.append({
                "campaign_id": row["campaign_id"],
                "subject": row["subject"],
                "status": row["status"],
                "total": row["total"],
                "delivered": row["delivered"],
                "bounced": row["bounced"],
                "processed": row["processed"],
                "created_at": row["created_at"],
            })
        return result
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def root():
  try:
      with open("index.html", "r", encoding="utf-8") as f:
          return f.read()
  except FileNotFoundError:
      return HTMLResponse("<h1>SendVerse backend is running.</h1>", status_code=200)
