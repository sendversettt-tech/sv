import os
import csv
import io
import threading
import time
from typing import Dict, Any, List
from fastapi.responses import Response

from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from email.mime.text import MIMEText
import smtplib
import ssl
import psycopg2
import psycopg2.extras
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_conn():
    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("ERROR: DATABASE_URL environment variable missing")
        raise Exception("DATABASE_URL not configured")

    return psycopg2.connect(database_url, sslmode="require")
app = FastAPI()
security = HTTPBasic()



# ========= IN-MEMORY CAMPAIGNS =========
CAMPAIGNS: Dict[str, Dict[str, Any]] = {}
_campaign_counter = 0
_campaign_lock = threading.Lock()

@app.on_event("startup")
def init_db():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS smtp_profiles (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        name TEXT,
        host TEXT,
        port INTEGER,
        use_tls BOOLEAN,
        smtp_username TEXT,
        smtp_password TEXT,
        from_email TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS campaigns (
        campaign_id TEXT PRIMARY KEY,
        username TEXT,
        subject TEXT,
        status TEXT,
        total INTEGER,
        processed INTEGER,
        sent INTEGER,
        failed INTEGER,
        delivered INTEGER,
        bounced INTEGER,
        last_error TEXT,
        opens INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """)

    conn.commit()
    conn.close()

# ========= AUTH =========
# ========= MANUAL USERS =========
USERS = {
    "user1": "pass1",
    "user2": "pass2"
}

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

        contacts.append({
            "name": name,
            "email": email
        })

    if not contacts:
        raise HTTPException(status_code=400, detail="No valid contacts found")

    return contacts

# ========= SMTP SEND =========
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def send_email_smtp(
    host,
    port,
    username,
    password,
    use_tls,
    from_email,
    to_email,
    subject,
    html,
    attachment_bytes=None,
    attachment_name=None
):

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    msg.attach(MIMEText(html, "html"))

    if attachment_bytes:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{attachment_name}"'
        )
        msg.attach(part)

    # Create unverified SSL context (FIX FOR IP-BASED SMTP)
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    with smtplib.SMTP(host, port, timeout=60) as server:
        if use_tls:
            server.starttls(context=context)
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

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    INSERT INTO smtp_profiles
    (username,name,host,port,use_tls,smtp_username,smtp_password,from_email)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    RETURNING *
    """,(
        current_user,
        name,
        host,
        port,
        use_tls,
        smtp_username,
        smtp_password,
        from_email
    ))

    row = cur.fetchone()

    conn.commit()
    conn.close()

    return row


@app.get("/smtp_profiles")
def list_smtp_profiles(current_user: str = Depends(get_current_user)):

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    SELECT * FROM smtp_profiles
    WHERE username=%s
    ORDER BY created_at DESC
    """,(current_user,))

    rows = cur.fetchall()

    conn.close()

    return rows


def create_campaign_record(campaign_id, username, subject, total):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO campaigns
    (campaign_id,username,subject,status,total,processed,sent,failed,delivered,bounced,last_error)
    VALUES (%s,%s,%s,'queued',%s,0,0,0,0,0,NULL)
    """,(campaign_id,username,subject,total))

    conn.commit()
    conn.close()

def update_campaign_record(campaign_id, camp):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    UPDATE campaigns
    SET status=%s,
        processed=%s,
        sent=%s,
        failed=%s,
        delivered=%s,
        bounced=%s,
        last_error=%s
    WHERE campaign_id=%s
    """,(
        camp["status"],
        camp["processed"],
        camp["sent"],
        camp["failed"],
        camp["delivered"],
        camp["bounced"],
        camp["last_error"],
        campaign_id
    ))

    conn.commit()
    conn.close()

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

    try:

        # Create unverified SSL context (FIX FOR IP-BASED SMTP)
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with smtplib.SMTP(camp["smtp_host"], camp["smtp_port"], timeout=60) as server:

            if camp["smtp_use_tls"]:
                server.starttls(context=context)

            if camp["smtp_username"] and camp["smtp_password"]:
                server.login(camp["smtp_username"], camp["smtp_password"])

            for idx, contact in enumerate(contacts):

                if camp["status"] == "stopped":
                    break

                try:

                    tracking_pixel = f'<img src="https://www.sendverse.world/open/{campaign_id}/{contact["email"]}" width="1" height="1"/>'

                    html = render_template(camp["html_body"], contact) + tracking_pixel

                    msg = MIMEMultipart()
                    msg["Subject"] = camp["subject"]
                    msg["From"] = camp["from_email"]
                    msg["To"] = contact["email"]

                    msg.attach(MIMEText(html, "html"))

                    if camp.get("attachment_bytes"):
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(camp["attachment_bytes"])
                        encoders.encode_base64(part)
                        part.add_header(
                            "Content-Disposition",
                            f'attachment; filename="{camp.get("attachment_name")}"'
                        )
                        msg.attach(part)

                    server.sendmail(
                        camp["from_email"],
                        [contact["email"]],
                        msg.as_string()
                    )

                    camp["sent"] += 1
                    camp["delivered"] += 1

                except Exception as e:

                    camp["failed"] += 1
                    camp["bounced"] += 1
                    camp["last_error"] = str(e)
                    print(f"Error sending to {contact['email']}: {e}")

                camp["processed"] += 1
                update_campaign_record(campaign_id, camp)

                if delay > 0 and idx < len(contacts) - 1:
                    time.sleep(delay)

    except Exception as e:
        camp["last_error"] = str(e)
        print(f"Campaign error: {e}")

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
    attachment_file: UploadFile = File(None),
    current_user: str = Depends(get_current_user),
):

    file_bytes = await contacts_file.read()
    attachment_bytes = None
    attachment_name = None

    if attachment_file:
        attachment_bytes = await attachment_file.read()
        attachment_name = attachment_file.filename

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
        "attachment_bytes": attachment_bytes,
        "attachment_name": attachment_name,
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

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    SELECT * FROM campaigns
    WHERE campaign_id=%s AND username=%s
    """,(campaign_id,current_user))

    row = cur.fetchone()

    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Campaign not found")

    return row
# ========= STOP =========
@app.post("/stop_campaign/{campaign_id}")
def stop_campaign(campaign_id: str, current_user: str = Depends(get_current_user)):

    camp = CAMPAIGNS.get(campaign_id)

    if camp and camp["user"] == current_user:

        camp["status"] = "stopped"
        update_campaign_record(campaign_id, camp)

    else:

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        UPDATE campaigns
        SET status='stopped'
        WHERE campaign_id=%s AND username=%s
        """,(campaign_id,current_user))

        conn.commit()
        conn.close()

    return {"message": "Campaign stop requested", "campaign_id": campaign_id}
# ========= LIST CAMPAIGNS =========
@app.get("/campaigns")
def list_campaigns(current_user: str = Depends(get_current_user)):

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
    SELECT * FROM campaigns
    WHERE username=%s
    ORDER BY created_at DESC
    """,(current_user,))

    rows = cur.fetchall()

    conn.close()

    return rows
@app.delete("/smtp_profiles/{profile_id}")
def delete_smtp_profile(profile_id: int, current_user: str = Depends(get_current_user)):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    DELETE FROM smtp_profiles
    WHERE id=%s AND username=%s
    """,(profile_id,current_user))

    conn.commit()
    conn.close()

    return {"message": "SMTP profile deleted"}

@app.delete("/campaigns/{campaign_id}")
def delete_campaign(campaign_id: str, current_user: str = Depends(get_current_user)):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    DELETE FROM campaigns
    WHERE campaign_id=%s AND username=%s
    """,(campaign_id,current_user))

    conn.commit()
    conn.close()

    return {"message": "Campaign deleted"}

@app.get("/open/{campaign_id}/{email}")
def track_open(campaign_id: str, email: str):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    UPDATE campaigns
    SET opens = opens + 1
    WHERE campaign_id=%s
    """,(campaign_id,))

    conn.commit()
    conn.close()

    pixel = b'\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x00\x00\x00\x21\xf9\x04\x01\x00\x00\x00\x00\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b'

    return Response(content=pixel, media_type="image/gif")

# ========= ROOT =========
@app.get("/", response_class=HTMLResponse)
def root():

    try:

        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()

    except FileNotFoundError:

        return HTMLResponse("<h1>SendVerse backend running</h1>", status_code=200)
        
