#!/usr/bin/env python3
from __future__ import annotations
"""
2WEEKコンタクト＆社用ガソリンカード期限管理アプリ バックエンドサーバー
- クラウド稼働対応: Supabase (PostgreSQL + Realtime) ＆ Vercel Serverless Functions / Vercel Cron 対応
- ローカル稼働対応: SQLite + Server-Sent Events (SSE) + 内蔵Cronスケジューラ
- 毎朝 08:00 自動実行 Cron ジョブ（コンタクト交換期限3日前〜超過 / ガソリン精算期限3日前〜超過）
- メール通知 (SMTP / Gmailアプリパスワード対応 + プレビューログ保存)
- Web Push 通知 (RFC 8292 VAPID + Service Worker 連携)
"""

import datetime
import email.mime.multipart
import email.mime.text
import email.utils
import http.server
import json
import mimetypes
import os
import queue
import socket
import sqlite3
import ssl
import smtplib
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import vapid_push
import supabase_client

# Use /tmp on serverless platforms (Vercel / AWS Lambda) where project root is read-only
IS_SERVERLESS = bool(os.environ.get("VERCEL") or os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))
DB_PATH = Path("/tmp/app_data.db") if IS_SERVERLESS else (BASE_DIR / "app_data.db")
SEED_PATH = BASE_DIR / "seed_data.json"
PUBLIC_DIR = BASE_DIR / "public"

# Japan Standard Time (UTC+9) helper so cloud servers running in UTC evaluate dates in JST
JST = datetime.timezone(datetime.timedelta(hours=9))


def now_jst() -> datetime.datetime:
    return datetime.datetime.now(JST).replace(tzinfo=None)


def today_jst() -> datetime.date:
    return now_jst().date()


db_lock = threading.Lock()
sse_clients_lock = threading.Lock()
sse_clients: list[queue.Queue] = []

latest_push_payload_lock = threading.Lock()
latest_push_payload = {
    "title": "🔔 期限管理アラート（コンタクト＆ガソリン精算）",
    "body": "交換・精算期限が近づいている項目があります。タップして確認してください。",
    "icon": "/icon.svg",
    "badge": "/icon.svg",
    "tag": "deadline-alert",
    "url": "/",
    "timestamp": int(time.time() * 1000),
}


def get_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(force_reseed: bool = False) -> None:
    with db_lock:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                efficiency REAL NOT NULL DEFAULT 15,
                fuelPrice REAL NOT NULL DEFAULT 175,
                updatedAt TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS members (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL DEFAULT '',
                themeColor TEXT NOT NULL DEFAULT 'blue',
                startDate TEXT NOT NULL,
                deadlineDate TEXT NOT NULL,
                memo TEXT NOT NULL DEFAULT '',
                sortOrder INTEGER NOT NULL DEFAULT 0,
                updatedAt TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS presets (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                km REAL NOT NULL DEFAULT 10,
                count INTEGER NOT NULL DEFAULT 0,
                sortOrder INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS fuel_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                useDate TEXT NOT NULL,
                deadline TEXT NOT NULL,
                mode TEXT NOT NULL,
                memo TEXT NOT NULL DEFAULT '',
                bizKm REAL NOT NULL DEFAULT 0,
                bizLiters REAL NOT NULL DEFAULT 0,
                bizYen INTEGER NOT NULL DEFAULT 0,
                privLiters REAL NOT NULL DEFAULT 0,
                privYen INTEGER NOT NULL DEFAULT 0,
                settled INTEGER NOT NULL DEFAULT 0,
                createdAt TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notification_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cronTime TEXT NOT NULL DEFAULT '08:00',
                lastCronRunDate TEXT NOT NULL DEFAULT '',
                emailEnabled INTEGER NOT NULL DEFAULT 1,
                smtpHost TEXT NOT NULL DEFAULT 'smtp.gmail.com',
                smtpPort INTEGER NOT NULL DEFAULT 587,
                smtpUser TEXT NOT NULL DEFAULT 'kuuasdfghjk@gmail.com',
                smtpPass TEXT NOT NULL DEFAULT '',
                smtpFromName TEXT NOT NULL DEFAULT 'コンタクト＆ガソリン期限管理Bot',
                webPushEnabled INTEGER NOT NULL DEFAULT 1,
                vapidPublicKey TEXT NOT NULL DEFAULT '',
                vapidPrivateKeyPem TEXT NOT NULL DEFAULT '',
                lineEnabled INTEGER NOT NULL DEFAULT 0,
                lineChannelToken TEXT NOT NULL DEFAULT '',
                lineTargetUserId TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint TEXT NOT NULL UNIQUE,
                p256dh TEXT NOT NULL DEFAULT '',
                auth TEXT NOT NULL DEFAULT '',
                memberName TEXT NOT NULL DEFAULT '全員',
                userAgent TEXT NOT NULL DEFAULT '',
                createdAt TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notification_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                executedAt TEXT NOT NULL,
                triggerType TEXT NOT NULL,
                channel TEXT NOT NULL,
                recipient TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT ''
            );
            """
        )

        # Ensure notification_config row exists with VAPID keys
        cur.execute("SELECT * FROM notification_config WHERE id = 1")
        notif_row = cur.fetchone()
        if not notif_row:
            keys = vapid_push.generate_vapid_keys()
            smtp_pass_env = os.environ.get("SMTP_PASS", "")
            smtp_user_env = os.environ.get("SMTP_USER", "kuuasdfghjk@gmail.com")
            cur.execute(
                """
                INSERT INTO notification_config (
                    id, cronTime, lastCronRunDate, emailEnabled,
                    smtpHost, smtpPort, smtpUser, smtpPass, smtpFromName,
                    webPushEnabled, vapidPublicKey, vapidPrivateKeyPem,
                    lineEnabled, lineChannelToken, lineTargetUserId
                ) VALUES (1, '08:00', '', 1, 'smtp.gmail.com', 587, ?, ?, 'コンタクト＆ガソリン期限管理Bot', 1, ?, ?, 0, '', '')
                """,
                (smtp_user_env, smtp_pass_env, keys["publicKey"], keys["privateKeyPem"]),
            )

        cur.execute("SELECT COUNT(*) as cnt FROM settings")
        settings_cnt = cur.fetchone()["cnt"]

        if force_reseed or settings_cnt == 0:
            seed_data = json.loads(SEED_PATH.read_text(encoding="utf-8"))
            now_iso = now_jst().isoformat(timespec="seconds")

            if force_reseed:
                cur.execute("DELETE FROM settings")
                cur.execute("DELETE FROM members")
                cur.execute("DELETE FROM presets")
                cur.execute("DELETE FROM fuel_records")

            s = seed_data.get("settings", {})
            cur.execute(
                "INSERT OR REPLACE INTO settings (id, efficiency, fuelPrice, updatedAt) VALUES (1, ?, ?, ?)",
                (float(s.get("efficiency", 15)), float(s.get("fuelPrice", 175)), now_iso),
            )

            for idx, m in enumerate(seed_data.get("members", [])):
                cur.execute(
                    """
                    INSERT OR REPLACE INTO members (id, name, email, themeColor, startDate, deadlineDate, memo, sortOrder, updatedAt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        m["id"],
                        m["name"],
                        m.get("email", ""),
                        m.get("themeColor", "blue"),
                        m["startDate"],
                        m["deadlineDate"],
                        m.get("memo", ""),
                        idx,
                        now_iso,
                    ),
                )

            for idx, p in enumerate(seed_data.get("presets", [])):
                cur.execute(
                    """
                    INSERT OR REPLACE INTO presets (id, name, km, count, sortOrder)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        p["id"],
                        p["name"],
                        float(p["km"]),
                        int(p.get("count", 0)),
                        idx,
                    ),
                )

            for r in seed_data.get("fuelRecords", []):
                cur.execute(
                    """
                    INSERT OR REPLACE INTO fuel_records (
                        id, useDate, deadline, mode, memo,
                        bizKm, bizLiters, bizYen, privLiters, privYen, settled, createdAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (
                        int(r["id"]),
                        r["useDate"],
                        r["deadline"],
                        r["mode"],
                        r.get("memo", ""),
                        float(r.get("bizKm", 0)),
                        float(r.get("bizLiters", 0)),
                        int(r.get("bizYen", 0)),
                        float(r.get("privLiters", 0)),
                        int(r.get("privYen", 0)),
                        now_iso,
                    ),
                )

        conn.commit()
        conn.close()


def sync_from_supabase() -> tuple[bool, str]:
    """
    If Supabase credentials are configured, pull authoritative cloud rows into local SQLite cache.
    If Supabase tables are empty, push initial seed data to Supabase automatically.
    """
    if not supabase_client.is_supabase_enabled():
        return False, "Supabase未設定（ローカルSQLiteモード）"

    try:
        sb_settings = supabase_client.sb_request("GET", "settings", {"select": "*"})
        sb_members = supabase_client.sb_request("GET", "members", {"select": "*", "order": "sortOrder.asc,id.asc"})
        sb_presets = supabase_client.sb_request("GET", "presets", {"select": "*", "order": "sortOrder.asc,id.asc"})
        sb_fuels = supabase_client.sb_request("GET", "fuel_records", {"select": "*", "order": "deadline.asc,id.desc"})
        sb_notif = supabase_client.sb_request("GET", "notification_config", {"select": "*"})
        sb_subs = supabase_client.sb_request("GET", "push_subscriptions", {"select": "*", "order": "id.desc"})
        sb_logs = supabase_client.sb_request("GET", "notification_logs", {"select": "*", "order": "id.desc", "limit": "30"})

        # If cloud settings/members are empty, push local seed data up to Supabase
        if not sb_settings and not sb_members:
            push_local_to_supabase()
            return True, "Supabaseへ初期シードデータを自動投入しました"

        with db_lock:
            conn = get_db_connection()
            cur = conn.cursor()

            if sb_settings and isinstance(sb_settings, list):
                s = sb_settings[0]
                cur.execute(
                    "INSERT OR REPLACE INTO settings (id, efficiency, fuelPrice, updatedAt) VALUES (1, ?, ?, ?)",
                    (float(s.get("efficiency", 15)), float(s.get("fuelPrice", 175)), s.get("updatedAt", "")),
                )

            if isinstance(sb_members, list):
                cur.execute("DELETE FROM members")
                for m in sb_members:
                    cur.execute(
                        """
                        INSERT INTO members (id, name, email, themeColor, startDate, deadlineDate, memo, sortOrder, updatedAt)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            m["id"],
                            m["name"],
                            m.get("email", ""),
                            m.get("themeColor", "blue"),
                            m["startDate"],
                            m["deadlineDate"],
                            m.get("memo", ""),
                            int(m.get("sortOrder", 0)),
                            m.get("updatedAt", ""),
                        ),
                    )

            if isinstance(sb_presets, list):
                cur.execute("DELETE FROM presets")
                for p in sb_presets:
                    cur.execute(
                        "INSERT INTO presets (id, name, km, count, sortOrder) VALUES (?, ?, ?, ?, ?)",
                        (p["id"], p["name"], float(p["km"]), int(p.get("count", 0)), int(p.get("sortOrder", 0))),
                    )

            if isinstance(sb_fuels, list):
                cur.execute("DELETE FROM fuel_records")
                for f in sb_fuels:
                    cur.execute(
                        """
                        INSERT INTO fuel_records (id, useDate, deadline, mode, memo, bizKm, bizLiters, bizYen, privLiters, privYen, settled, createdAt)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(f["id"]),
                            f["useDate"],
                            f["deadline"],
                            f["mode"],
                            f.get("memo", ""),
                            float(f.get("bizKm", 0)),
                            float(f.get("bizLiters", 0)),
                            int(f.get("bizYen", 0)),
                            float(f.get("privLiters", 0)),
                            int(f.get("privYen", 0)),
                            int(f.get("settled", 0)),
                            f.get("createdAt", ""),
                        ),
                    )

            if sb_notif and isinstance(sb_notif, list):
                n = sb_notif[0]
                if n.get("vapidPublicKey") and n.get("vapidPrivateKeyPem"):
                    cur.execute(
                        """
                        UPDATE notification_config
                        SET cronTime = ?, lastCronRunDate = ?, emailEnabled = ?,
                            smtpHost = ?, smtpPort = ?, smtpUser = ?, smtpPass = ?, smtpFromName = ?,
                            webPushEnabled = ?, vapidPublicKey = ?, vapidPrivateKeyPem = ?,
                            lineEnabled = ?, lineChannelToken = ?, lineTargetUserId = ?
                        WHERE id = 1
                        """,
                        (
                            n.get("cronTime", "08:00"),
                            n.get("lastCronRunDate", ""),
                            int(n.get("emailEnabled", 1)),
                            n.get("smtpHost", "smtp.gmail.com"),
                            int(n.get("smtpPort", 587)),
                            n.get("smtpUser", ""),
                            n.get("smtpPass", "") or os.environ.get("SMTP_PASS", ""),
                            n.get("smtpFromName", "コンタクト＆ガソリン期限管理Bot"),
                            int(n.get("webPushEnabled", 1)),
                            n["vapidPublicKey"],
                            n["vapidPrivateKeyPem"],
                            int(n.get("lineEnabled", 0)),
                            n.get("lineChannelToken", ""),
                            n.get("lineTargetUserId", ""),
                        ),
                    )
            else:
                # Push generated VAPID config to Supabase
                cur.execute("SELECT * FROM notification_config WHERE id = 1")
                local_n = dict(cur.fetchone())
                supabase_client.sb_request(
                    "POST",
                    "notification_config",
                    body=local_n,
                    prefer="resolution=merge-duplicates,return=representation",
                )

            if isinstance(sb_subs, list):
                cur.execute("DELETE FROM push_subscriptions")
                for s in sb_subs:
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO push_subscriptions (id, endpoint, p256dh, auth, memberName, userAgent, createdAt)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(s["id"]),
                            s["endpoint"],
                            s.get("p256dh", ""),
                            s.get("auth", ""),
                            s.get("memberName", "全員"),
                            s.get("userAgent", ""),
                            s.get("createdAt", ""),
                        ),
                    )

            if isinstance(sb_logs, list):
                cur.execute("DELETE FROM notification_logs")
                for l in reversed(sb_logs):
                    cur.execute(
                        """
                        INSERT INTO notification_logs (id, executedAt, triggerType, channel, recipient, subject, body, status, detail)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(l["id"]),
                            l["executedAt"],
                            l["triggerType"],
                            l["channel"],
                            l["recipient"],
                            l["subject"],
                            l["body"],
                            l["status"],
                            l.get("detail", ""),
                        ),
                    )

            conn.commit()
            conn.close()
        return True, "SupabaseクラウドDBと同期完了"
    except Exception as e:
        return False, f"Supabase同期エラー: {e}"


def push_local_to_supabase() -> None:
    if not supabase_client.is_supabase_enabled():
        return
    with db_lock:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM settings WHERE id = 1")
        s = dict(cur.fetchone())
        cur.execute("SELECT * FROM members")
        members = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM presets")
        presets = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM fuel_records")
        fuels = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM notification_config WHERE id = 1")
        notif = dict(cur.fetchone())
        conn.close()

    try:
        supabase_client.sb_request("POST", "settings", body=s, prefer="resolution=merge-duplicates")
        if members:
            supabase_client.sb_request("POST", "members", body=members, prefer="resolution=merge-duplicates")
        if presets:
            supabase_client.sb_request("POST", "presets", body=presets, prefer="resolution=merge-duplicates")
        if fuels:
            supabase_client.sb_request("POST", "fuel_records", body=fuels, prefer="resolution=merge-duplicates")
        supabase_client.sb_request("POST", "notification_config", body=notif, prefer="resolution=merge-duplicates")
    except Exception as e:
        print(f"[Supabase Push Warning] {e}")


def get_full_state(pull_cloud: bool = True) -> dict:
    if not DB_PATH.exists():
        init_db(force_reseed=False)

    cloud_ok = False
    cloud_msg = "ローカルSQLiteモード"
    if pull_cloud and supabase_client.is_supabase_enabled():
        cloud_ok, cloud_msg = sync_from_supabase()

    sb_url, sb_key = supabase_client.get_supabase_credentials()

    with db_lock:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT efficiency, fuelPrice, updatedAt FROM settings WHERE id = 1")
        s_row = cur.fetchone()
        settings = dict(s_row) if s_row else {"efficiency": 15, "fuelPrice": 175, "updatedAt": ""}

        cur.execute("SELECT id, name, email, themeColor, startDate, deadlineDate, memo, sortOrder, updatedAt FROM members ORDER BY sortOrder ASC, id ASC")
        members = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT id, name, km, count, sortOrder FROM presets ORDER BY sortOrder ASC, id ASC")
        presets = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id, useDate, deadline, mode, memo, bizKm, bizLiters, bizYen, privLiters, privYen, settled, createdAt
            FROM fuel_records
            ORDER BY deadline ASC, id DESC
            """
        )
        fuel_records = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT cronTime, lastCronRunDate, emailEnabled, smtpHost, smtpPort, smtpUser,
                   CASE WHEN smtpPass != '' THEN '********' ELSE '' END AS smtpPassMasked,
                   CASE WHEN smtpPass != '' THEN 1 ELSE 0 END AS hasSmtpPass,
                   smtpFromName, webPushEnabled, vapidPublicKey,
                   lineEnabled,
                   CASE WHEN lineChannelToken != '' THEN 1 ELSE 0 END AS hasLineToken,
                   lineTargetUserId
            FROM notification_config WHERE id = 1
            """
        )
        notif_config = dict(cur.fetchone())

        cur.execute("SELECT id, endpoint, memberName, userAgent, createdAt FROM push_subscriptions ORDER BY id DESC")
        subs = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id, executedAt, triggerType, channel, recipient, subject, body, status, detail
            FROM notification_logs
            ORDER BY id DESC LIMIT 30
            """
        )
        logs = [dict(r) for r in cur.fetchall()]

        conn.close()

    return {
        "settings": settings,
        "members": members,
        "presets": presets,
        "fuelRecords": fuel_records,
        "notificationConfig": notif_config,
        "pushSubscriptions": subs,
        "notificationLogs": logs,
        "cloudStatus": {
            "enabled": supabase_client.is_supabase_enabled(),
            "connected": cloud_ok,
            "message": cloud_msg,
            "supabaseUrl": sb_url,
            "supabaseAnonKey": sb_key if sb_url else "",
        },
        "serverDate": today_jst().isoformat(),
        "serverTime": now_jst().isoformat(timespec="seconds"),
        "lanIp": get_lan_ip(),
    }


def broadcast_sse(event_type: str = "sync", extra_payload: dict | None = None) -> None:
    state = get_full_state(pull_cloud=False)
    message_obj = {
        "type": event_type,
        "state": state,
        "extra": extra_payload or {},
        "timestamp": int(time.time() * 1000),
    }
    data_str = f"data: {json.dumps(message_obj, ensure_ascii=False)}\n\n"

    with sse_clients_lock:
        dead_clients = []
        for q in sse_clients:
            try:
                q.put_nowait(data_str)
            except queue.Full:
                dead_clients.append(q)
        for dq in dead_clients:
            if dq in sse_clients:
                sse_clients.remove(dq)


def days_until(target_date_str: str, today: datetime.date | None = None) -> int:
    if today is None:
        today = today_jst()
    target = datetime.date.fromisoformat(target_date_str)
    return (target - today).days


def log_notification(
    trigger_type: str,
    channel: str,
    recipient: str,
    subject: str,
    body: str,
    status: str,
    detail: str = "",
) -> None:
    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    with db_lock:
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO notification_logs (executedAt, triggerType, channel, recipient, subject, body, status, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now_str, trigger_type, channel, recipient, subject, body, status, detail),
        )
        conn.commit()
        conn.close()

    if supabase_client.is_supabase_enabled():
        try:
            supabase_client.sb_request(
                "POST",
                "notification_logs",
                body={
                    "executedAt": now_str,
                    "triggerType": trigger_type,
                    "channel": channel,
                    "recipient": recipient,
                    "subject": subject,
                    "body": body,
                    "status": status,
                    "detail": detail,
                },
            )
        except Exception:
            pass


def send_smtp_email(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    from_name: str,
    to_email: str,
    subject: str,
    plain_body: str,
    html_body: str,
) -> tuple[bool, str]:
    effective_pass = smtp_pass.strip() or os.environ.get("SMTP_PASS", "").strip()
    if not effective_pass:
        return (
            False,
            "SMTPパスワード未設定のためプレビュー記録のみ完了（設定画面または環境変数SMTP_PASSにGmailアプリパスワードを登録すると実メール送信されます）",
        )

    msg = email.mime.multipart.MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email.utils.formataddr((from_name, smtp_user))
    msg["To"] = to_email
    msg["Date"] = email.utils.formatdate(localtime=True)

    msg.attach(email.mime.text.MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(email.mime.text.MIMEText(html_body, "html", "utf-8"))

    try:
        context = ssl.create_default_context()
        if int(smtp_port) == 465:
            with smtplib.SMTP_SSL(smtp_host, int(smtp_port), timeout=12, context=context) as server:
                server.login(smtp_user, effective_pass)
                server.sendmail(smtp_user, [to_email], msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, int(smtp_port), timeout=12) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(smtp_user, effective_pass)
                server.sendmail(smtp_user, [to_email], msg.as_string())
        return True, f"SMTP送信成功 ({smtp_host}:{smtp_port} -> {to_email})"
    except Exception as e:
        return False, f"SMTPエラー: {e}"


def send_line_push(channel_token: str, target_user_id: str, text_message: str) -> tuple[bool, str]:
    if not channel_token.strip():
        return False, "LINEチャネルアクセストークン未設定"

    if target_user_id.strip():
        url = "https://api.line.me/v2/bot/message/push"
        payload = {
            "to": target_user_id.strip(),
            "messages": [{"type": "text", "text": text_message}],
        }
    else:
        url = "https://api.line.me/v2/bot/message/broadcast"
        payload = {
            "messages": [{"type": "text", "text": text_message}],
        }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {channel_token.strip()}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return True, f"LINE API 送信成功 (HTTP {resp.getcode()})"
    except Exception as e:
        return False, f"LINE API エラー: {e}"


def execute_daily_notifications(trigger_type: str = "manual_test", force_all: bool = False) -> dict:
    if not DB_PATH.exists():
        init_db(force_reseed=False)
    if supabase_client.is_supabase_enabled():
        sync_from_supabase()

    today = today_jst()

    with db_lock:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT * FROM notification_config WHERE id = 1")
        config = dict(cur.fetchone())

        cur.execute("SELECT * FROM members ORDER BY sortOrder ASC")
        members = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT * FROM fuel_records WHERE settled = 0 ORDER BY deadline ASC")
        unsettled_fuels = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT * FROM push_subscriptions")
        subs = [dict(r) for r in cur.fetchall()]

        conn.close()

    due_members = []
    for m in members:
        rem = days_until(m["deadlineDate"], today)
        m_copy = dict(m)
        m_copy["remainingDays"] = rem
        if rem <= 3 or force_all:
            due_members.append(m_copy)

    due_fuels = []
    for f in unsettled_fuels:
        rem = days_until(f["deadline"], today)
        f_copy = dict(f)
        f_copy["remainingDays"] = rem
        if rem <= 3 or force_all:
            due_fuels.append(f_copy)

    summary_results = {
        "evaluatedDate": today.isoformat(),
        "triggerType": trigger_type,
        "dueMembersCount": len(due_members),
        "dueFuelsCount": len(due_fuels),
        "dueMembers": due_members,
        "dueFuels": due_fuels,
        "deliveries": [],
    }

    if not due_members and not due_fuels:
        log_notification(
            trigger_type=trigger_type,
            channel="SYSTEM",
            recipient="全員",
            subject="期限3日前以内の対象項目なし",
            body="本日時点で交換期限3日前以内のコンタクトおよび精算期限3日前以内のガソリン記録はありませんでした。",
            status="SKIPPED",
            detail="対象0件",
        )
        broadcast_sse("sync")
        return summary_results

    push_lines = []
    for m in due_members:
        rem = m["remainingDays"]
        status_str = f"残り{rem}日" if rem > 0 else ("本日交換日！" if rem == 0 else f"{abs(rem)}日超過！")
        push_lines.append(f"👁️ {m['name']}のコンタクト({m['memo']}): 期限 {m['deadlineDate']} 【{status_str}】")

    for f in due_fuels:
        rem = f["remainingDays"]
        status_str = f"残り{rem}日" if rem > 0 else ("本日精算期限！" if rem == 0 else f"{abs(rem)}日超過！")
        push_lines.append(
            f"⛽ ガソリン精算({f['useDate']}給油 / {f['mode']}): 期限 {f['deadline']} 【{status_str}】 私用控除 ¥{f['privYen']:,} / 業務経費 ¥{f['bizYen']:,}"
        )

    combined_title = f"🔔 期限アラート ({len(due_members) + len(due_fuels)}件)"
    combined_body = "\n".join(push_lines)

    # 1. Web Push Notifications
    if config.get("webPushEnabled", 1):
        global latest_push_payload
        with latest_push_payload_lock:
            latest_push_payload = {
                "title": combined_title,
                "body": combined_body,
                "icon": "/icon.svg",
                "badge": "/icon.svg",
                "tag": f"deadline-alert-{today.isoformat()}",
                "url": "/",
                "items": push_lines,
                "timestamp": int(time.time() * 1000),
            }

        vapid_sent_count = 0
        vapid_details = []
        for sub in subs:
            ok, http_code, msg = vapid_push.send_web_push(
                endpoint=sub["endpoint"],
                vapid_public_key=config["vapidPublicKey"],
                vapid_private_key_pem=config["vapidPrivateKeyPem"],
                subject=f"mailto:{config['smtpUser'] or 'kuuasdfghjk@gmail.com'}",
            )
            if ok:
                vapid_sent_count += 1
            vapid_details.append(f"{sub['memberName']}({msg})")

        broadcast_sse(
            "push_alert",
            {
                "title": combined_title,
                "body": combined_body,
                "tag": f"deadline-alert-{int(time.time())}",
            },
        )

        push_status = "SENT"
        detail_msg = (
            f"Web Push配信 + VAPID購読端末 {vapid_sent_count}/{len(subs)}台 送信完了"
            + (f" ({', '.join(vapid_details)})" if vapid_details else "（ブラウザの『Web Push通知を有効化』を押すとバックグラウンド購読も追加されます）")
        )
        log_notification(
            trigger_type=trigger_type,
            channel="WEB_PUSH",
            recipient=f"購読端末 ({len(subs)}台) & アクティブ画面",
            subject=combined_title,
            body=combined_body,
            status=push_status,
            detail=detail_msg,
        )
        summary_results["deliveries"].append(
            {"channel": "WEB_PUSH", "status": push_status, "detail": detail_msg}
        )

    # 2. Email Notifications
    if config.get("emailEnabled", 1):
        recipients_map: dict[str, dict] = {}
        for m in due_members:
            addr = m.get("email", "").strip()
            if addr:
                recipients_map[addr] = {
                    "member": m,
                    "includeContact": True,
                    "includeFuels": True,
                }

        if due_fuels and not recipients_map:
            for m in members:
                addr = m.get("email", "").strip()
                if addr:
                    recipients_map[addr] = {
                        "member": m,
                        "includeContact": False,
                        "includeFuels": True,
                    }
                    break

        for to_addr, info in recipients_map.items():
            m = info["member"]
            subject_parts = []
            plain_sections = [f"{m['name']} さん、おはようございます。\n毎朝8:00の期限管理自動チェック結果をお知らせします。\n"]
            html_cards = []

            if info["includeContact"]:
                rem = m["remainingDays"]
                badge = f"残り {rem} 日" if rem > 0 else ("本日が交換日です！" if rem == 0 else f"期限を {abs(rem)} 日超過しています！")
                subject_parts.append(f"コンタクト交換期限（{badge}）")
                plain_sections.append(
                    f"■ 2WEEKコンタクト交換アラート\n"
                    f"・対象メンバー: {m['name']}\n"
                    f"・レンズ銘柄/メモ: {m['memo']}\n"
                    f"・開封日: {m['startDate']}\n"
                    f"・交換期限日: {m['deadlineDate']} （{badge}）\n"
                )
                html_cards.append(
                    f"""
                    <div style="border-left:4px solid #3b82f6;background:#eff6ff;padding:14px 16px;border-radius:8px;margin-bottom:16px;">
                      <h3 style="margin:0 0 8px 0;color:#1e40af;font-size:16px;">👁️ 2WEEKコンタクト交換アラート（{badge}）</h3>
                      <p style="margin:4px 0;font-size:14px;"><strong>メンバー:</strong> {m['name']}</p>
                      <p style="margin:4px 0;font-size:14px;"><strong>使用レンズ:</strong> {m['memo']}</p>
                      <p style="margin:4px 0;font-size:14px;"><strong>開封日:</strong> {m['startDate']} → <strong>交換期限: <span style="color:#dc2626;">{m['deadlineDate']}</span></strong></p>
                    </div>
                    """
                )

            if info["includeFuels"] and due_fuels:
                subject_parts.append(f"社用ガソリン精算期限（{len(due_fuels)}件）")
                plain_sections.append("■ 社用ガソリンカード 精算期限アラート")
                for f in due_fuels:
                    rem = f["remainingDays"]
                    badge = f"残り {rem} 日" if rem > 0 else ("本日が精算期限！" if rem == 0 else f"{abs(rem)} 日超過！")
                    plain_sections.append(
                        f"・給油日: {f['useDate']} / 精算期限: {f['deadline']} （{badge}）\n"
                        f"  計算モード: {f['mode']}\n"
                        f"  メモ: {f['memo']}\n"
                        f"  業務計上分: {f['bizKm']} km / {f['bizLiters']} L / ¥{f['bizYen']:,}\n"
                        f"  私用除外（自己負担控除額）: {f['privLiters']} L / ¥{f['privYen']:,}\n"
                    )
                    html_cards.append(
                        f"""
                        <div style="border-left:4px solid #f59e0b;background:#fffbeb;padding:14px 16px;border-radius:8px;margin-bottom:16px;">
                          <h3 style="margin:0 0 8px 0;color:#92400e;font-size:16px;">⛽ 社用ガソリンカード精算期限（{badge}）</h3>
                          <p style="margin:4px 0;font-size:14px;"><strong>給油日:</strong> {f['useDate']} ／ <strong>精算期限: <span style="color:#dc2626;">{f['deadline']}</span></strong></p>
                          <p style="margin:4px 0;font-size:14px;"><strong>算出モード:</strong> {f['mode']}（{f['memo']}）</p>
                          <p style="margin:4px 0;font-size:14px;"><strong>🏢 業務経費計上:</strong> {f['bizKm']} km（{f['bizLiters']} L / <strong>¥{f['bizYen']:,}</strong>）</p>
                          <p style="margin:4px 0;font-size:14px;"><strong>🏠 私用分除外（自己負担精算額）:</strong> {f['privLiters']} L / <strong style="color:#dc2626;">¥{f['privYen']:,}</strong></p>
                        </div>
                        """
                    )

            subject = "【期限アラート】" + " ＆ ".join(subject_parts)
            plain_body = "\n".join(plain_sections)
            html_body = f"""
            <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:20px;border:1px solid #e2e8f0;border-radius:12px;">
              <h2 style="color:#0f172a;margin-top:0;">🔔 コンタクト＆社用ガソリン期限管理通知</h2>
              <p style="color:#475569;font-size:14px;">{m['name']} さん、期限3日前以内の項目をお知らせします。</p>
              {''.join(html_cards)}
              <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;" />
              <p style="color:#64748b;font-size:12px;">※交換・精算完了後は管理アプリからワンタップで更新できます。</p>
            </div>
            """

            ok, detail_msg = send_smtp_email(
                smtp_host=config["smtpHost"],
                smtp_port=config["smtpPort"],
                smtp_user=config["smtpUser"],
                smtp_pass=config["smtpPass"],
                from_name=config["smtpFromName"],
                to_email=to_addr,
                subject=subject,
                plain_body=plain_body,
                html_body=html_body,
            )
            status_code = "SENT" if ok else "PREVIEW_SAVED"
            log_notification(
                trigger_type=trigger_type,
                channel="EMAIL",
                recipient=f"{m['name']} <{to_addr}>",
                subject=subject,
                body=plain_body,
                status=status_code,
                detail=detail_msg,
            )
            summary_results["deliveries"].append(
                {
                    "channel": "EMAIL",
                    "recipient": f"{m['name']} <{to_addr}>",
                    "status": status_code,
                    "detail": detail_msg,
                }
            )

    # 3. Optional LINE Messaging API Notification
    if config.get("lineEnabled", 0):
        ok, detail_msg = send_line_push(
            channel_token=config.get("lineChannelToken", ""),
            target_user_id=config.get("lineTargetUserId", ""),
            text_message=f"{combined_title}\n\n{combined_body}",
        )
        status_code = "SENT" if ok else "ERROR"
        log_notification(
            trigger_type=trigger_type,
            channel="LINE",
            recipient=config.get("lineTargetUserId") or "Broadcast",
            subject=combined_title,
            body=combined_body,
            status=status_code,
            detail=detail_msg,
        )
        summary_results["deliveries"].append(
            {"channel": "LINE", "status": status_code, "detail": detail_msg}
        )

    broadcast_sse("sync")
    return summary_results


def cron_scheduler_loop() -> None:
    while True:
        try:
            now = now_jst()
            today_str = now.strftime("%Y-%m-%d")
            current_hhmm = now.strftime("%H:%M")

            should_run = False
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT cronTime, lastCronRunDate FROM notification_config WHERE id = 1")
                row = cur.fetchone()
                if row:
                    cron_time = row["cronTime"] or "08:00"
                    last_run = row["lastCronRunDate"] or ""
                    if current_hhmm == cron_time and last_run != today_str:
                        cur.execute(
                            "UPDATE notification_config SET lastCronRunDate = ? WHERE id = 1",
                            (today_str,),
                        )
                        conn.commit()
                        should_run = True
                conn.close()

            if should_run:
                if supabase_client.is_supabase_enabled():
                    try:
                        supabase_client.sb_request(
                            "PATCH",
                            "notification_config",
                            {"id": "eq.1"},
                            {"lastCronRunDate": today_str},
                        )
                    except Exception:
                        pass
                print(f"[CRON {now.isoformat()}] Running scheduled 08:00 notification job...")
                execute_daily_notifications(trigger_type=f"cron_{current_hhmm}")
        except Exception as e:
            print(f"[CRON ERROR] {e}")

        time.sleep(20)


class AppHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args) -> None:
        pass

    def _send_json(self, data: dict | list, status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        if not DB_PATH.exists():
            init_db(force_reseed=False)

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/state":
            self._send_json(get_full_state(pull_cloud=True))
            return

        if path == "/api/cron":
            # Triggered automatically by Vercel Cron (vercel.json) at 08:00 AM JST (23:00 UTC)
            res = execute_daily_notifications(trigger_type="cloud_cron_0800")
            self._send_json({"ok": True, "result": res})
            return

        if path == "/api/vapid-public-key":
            state = get_full_state(pull_cloud=False)
            self._send_json({"publicKey": state["notificationConfig"]["vapidPublicKey"]})
            return

        if path == "/api/notifications/latest-payload":
            with latest_push_payload_lock:
                self._send_json(latest_push_payload)
            return

        if path == "/api/events":
            if IS_SERVERLESS:
                # On Vercel Serverless, return a single immediate SSE message and close cleanly so function doesn't timeout
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                init_msg = json.dumps(
                    {"type": "connected", "serverless": True, "serverTime": now_jst().isoformat(timespec="seconds")},
                    ensure_ascii=False,
                )
                self.wfile.write(f"data: {init_msg}\n\n".encode("utf-8"))
                self.wfile.flush()
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            client_q: queue.Queue = queue.Queue(maxsize=30)
            with sse_clients_lock:
                sse_clients.append(client_q)

            try:
                init_msg = json.dumps(
                    {"type": "connected", "serverTime": now_jst().isoformat(timespec="seconds")},
                    ensure_ascii=False,
                )
                self.wfile.write(f"data: {init_msg}\n\n".encode("utf-8"))
                self.wfile.flush()

                while True:
                    try:
                        msg = client_q.get(timeout=15)
                        self.wfile.write(msg.encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": heartbeat\n\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, Exception):
                pass
            finally:
                with sse_clients_lock:
                    if client_q in sse_clients:
                        sse_clients.remove(client_q)
            return

        # Serve static files from PUBLIC_DIR or BASE_DIR
        if path == "/" or path == "":
            file_path = (PUBLIC_DIR / "index.html") if (PUBLIC_DIR / "index.html").exists() else (BASE_DIR / "index.html")
        else:
            clean_rel = path.lstrip("/")
            cand_pub = (PUBLIC_DIR / clean_rel).resolve()
            cand_root = (BASE_DIR / clean_rel).resolve()
            if PUBLIC_DIR.exists() and str(cand_pub).startswith(str(PUBLIC_DIR.resolve())) and cand_pub.exists() and cand_pub.is_file():
                file_path = cand_pub
            elif str(cand_root).startswith(str(BASE_DIR.resolve())) and cand_root.exists() and cand_root.is_file() and cand_root.suffix in (".html", ".js", ".svg", ".json", ".css", ".ico", ".png"):
                file_path = cand_root
            else:
                self._send_json({"error": "Not found"}, 404)
                return

        if file_path.exists() and file_path.is_file():
            ctype, _ = mimetypes.guess_type(str(file_path))
            if str(file_path).endswith(".js"):
                ctype = "application/javascript; charset=utf-8"
            elif str(file_path).endswith(".html"):
                ctype = "text/html; charset=utf-8"
            elif str(file_path).endswith(".svg"):
                ctype = "image/svg+xml"
            elif str(file_path).endswith(".json"):
                ctype = "application/manifest+json; charset=utf-8"

            data = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache")
            if path == "/sw.js":
                self.send_header("Service-Worker-Allowed", "/")
            self.end_headers()
            self.wfile.write(data)
            return

        self._send_json({"error": "Not found"}, 404)

    def do_POST(self) -> None:
        if not DB_PATH.exists():
            init_db(force_reseed=False)

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_json_body()
        now_iso = now_jst().isoformat(timespec="seconds")

        if path == "/api/cloud-config":
            sb_url = (body.get("supabaseUrl") or "").strip()
            sb_key = (body.get("supabaseKey") or "").strip()
            supabase_client.save_local_supabase_credentials(sb_url, sb_key)
            if sb_url and sb_key:
                push_local_to_supabase()
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=True)})
            return

        if path == "/api/members":
            member_id = body.get("id") or f"m{int(time.time() * 1000)}"
            name = (body.get("name") or "").strip()
            if not name:
                self._send_json({"error": "名前を入力してください"}, 400)
                return
            email_addr = (body.get("email") or "").strip()
            theme_color = body.get("themeColor") or "blue"
            start_date = body.get("startDate") or today_jst().isoformat()
            deadline_date = body.get("deadlineDate")
            if not deadline_date:
                d0 = datetime.date.fromisoformat(start_date)
                deadline_date = (d0 + datetime.timedelta(days=14)).isoformat()
            memo = body.get("memo") or "2WEEKコンタクト"

            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT COALESCE(MAX(sortOrder), -1) + 1 AS nextOrder FROM members")
                next_order = cur.fetchone()["nextOrder"]
                cur.execute(
                    """
                    INSERT INTO members (id, name, email, themeColor, startDate, deadlineDate, memo, sortOrder, updatedAt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (member_id, name, email_addr, theme_color, start_date, deadline_date, memo, next_order, now_iso),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "POST",
                        "members",
                        body={
                            "id": member_id,
                            "name": name,
                            "email": email_addr,
                            "themeColor": theme_color,
                            "startDate": start_date,
                            "deadlineDate": deadline_date,
                            "memo": memo,
                            "sortOrder": next_order,
                            "updatedAt": now_iso,
                        },
                        prefer="resolution=merge-duplicates",
                    )
                except Exception as e:
                    print(f"[Supabase POST members] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/presets":
            preset_id = body.get("id") or f"p{int(time.time() * 1000)}"
            name = (body.get("name") or "").strip()
            km = float(body.get("km", 10))
            count = int(body.get("count", 1))
            if not name:
                self._send_json({"error": "ルート名を入力してください"}, 400)
                return

            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT COALESCE(MAX(sortOrder), -1) + 1 AS nextOrder FROM presets")
                next_order = cur.fetchone()["nextOrder"]
                cur.execute(
                    "INSERT INTO presets (id, name, km, count, sortOrder) VALUES (?, ?, ?, ?, ?)",
                    (preset_id, name, km, count, next_order),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "POST",
                        "presets",
                        body={"id": preset_id, "name": name, "km": km, "count": count, "sortOrder": next_order},
                        prefer="resolution=merge-duplicates",
                    )
                except Exception as e:
                    print(f"[Supabase POST presets] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/fuel-records":
            use_date = body.get("useDate") or today_jst().isoformat()
            deadline = body.get("deadline")
            if not deadline:
                d0 = datetime.date.fromisoformat(use_date)
                deadline = (d0 + datetime.timedelta(days=6)).isoformat()
            mode = body.get("mode") or "① 固定ルート合算"
            memo = body.get("memo") or ""
            biz_km = round(float(body.get("bizKm", 0)), 1)
            biz_liters = round(float(body.get("bizLiters", 0)), 2)
            biz_yen = int(round(float(body.get("bizYen", 0))))
            priv_liters = round(float(body.get("privLiters", 0)), 2)
            priv_yen = int(round(float(body.get("privYen", 0))))

            if supabase_client.is_supabase_enabled():
                try:
                    inserted = supabase_client.sb_request(
                        "POST",
                        "fuel_records",
                        body={
                            "useDate": use_date,
                            "deadline": deadline,
                            "mode": mode,
                            "memo": memo,
                            "bizKm": biz_km,
                            "bizLiters": biz_liters,
                            "bizYen": biz_yen,
                            "privLiters": priv_liters,
                            "privYen": priv_yen,
                            "settled": 0,
                            "createdAt": now_iso,
                        },
                    )
                    sync_from_supabase()
                    broadcast_sse("sync")
                    self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
                    return
                except Exception as e:
                    print(f"[Supabase POST fuel_records] {e}")

            with db_lock:
                conn = get_db_connection()
                conn.execute(
                    """
                    INSERT INTO fuel_records (
                        useDate, deadline, mode, memo, bizKm, bizLiters, bizYen, privLiters, privYen, settled, createdAt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (use_date, deadline, mode, memo, biz_km, biz_liters, biz_yen, priv_liters, priv_yen, now_iso),
                )
                conn.commit()
                conn.close()

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/push/subscribe":
            sub = body.get("subscription") or {}
            endpoint = sub.get("endpoint") or ""
            keys = sub.get("keys") or {}
            p256dh = keys.get("p256dh") or ""
            auth = keys.get("auth") or ""
            member_name = body.get("memberName") or "全員"
            user_agent = self.headers.get("User-Agent", "")[:160]

            if not endpoint:
                self._send_json({"error": "Invalid subscription endpoint"}, 400)
                return

            with db_lock:
                conn = get_db_connection()
                conn.execute(
                    """
                    INSERT INTO push_subscriptions (endpoint, p256dh, auth, memberName, userAgent, createdAt)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(endpoint) DO UPDATE SET
                        p256dh = excluded.p256dh,
                        auth = excluded.auth,
                        memberName = excluded.memberName,
                        userAgent = excluded.userAgent
                    """,
                    (endpoint, p256dh, auth, member_name, user_agent, now_iso),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "POST",
                        "push_subscriptions?on_conflict=endpoint",
                        body={
                            "endpoint": endpoint,
                            "p256dh": p256dh,
                            "auth": auth,
                            "memberName": member_name,
                            "userAgent": user_agent,
                            "createdAt": now_iso,
                        },
                        prefer="resolution=merge-duplicates",
                    )
                except Exception as e:
                    print(f"[Supabase POST push_subscriptions] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/push/unsubscribe":
            endpoint = body.get("endpoint") or ""
            with db_lock:
                conn = get_db_connection()
                if endpoint:
                    conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
                conn.commit()
                conn.close()
            if endpoint and supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request("DELETE", "push_subscriptions", {"endpoint": f"eq.{endpoint}"})
                except Exception:
                    pass
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/notifications/run-now":
            force_all = bool(body.get("forceAll", False))
            res = execute_daily_notifications(
                trigger_type="manual_test" if not force_all else "manual_force_all",
                force_all=force_all,
            )
            self._send_json({"ok": True, "result": res, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/reset-seed":
            init_db(force_reseed=True)
            if supabase_client.is_supabase_enabled():
                push_local_to_supabase()
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        self._send_json({"error": "Unknown POST endpoint"}, 404)

    def do_PUT(self) -> None:
        if not DB_PATH.exists():
            init_db(force_reseed=False)

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_json_body()
        now_iso = now_jst().isoformat(timespec="seconds")

        if path == "/api/settings":
            eff = float(body.get("efficiency", 15))
            price = float(body.get("fuelPrice", 175))
            with db_lock:
                conn = get_db_connection()
                conn.execute(
                    "UPDATE settings SET efficiency = ?, fuelPrice = ?, updatedAt = ? WHERE id = 1",
                    (eff, price, now_iso),
                )
                conn.commit()
                conn.close()
            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "PATCH",
                        "settings",
                        {"id": "eq.1"},
                        {"efficiency": eff, "fuelPrice": price, "updatedAt": now_iso},
                    )
                except Exception as e:
                    print(f"[Supabase PATCH settings] {e}")
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path.startswith("/api/members/"):
            member_id = urllib.parse.unquote(path.split("/api/members/")[1])
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM members WHERE id = ?", (member_id,))
                existing = cur.fetchone()
                if not existing:
                    conn.close()
                    self._send_json({"error": "Member not found"}, 404)
                    return

                name = body.get("name", existing["name"])
                email_addr = body.get("email", existing["email"])
                theme_color = body.get("themeColor", existing["themeColor"])
                start_date = body.get("startDate", existing["startDate"])
                deadline_date = body.get("deadlineDate")
                if not deadline_date:
                    d0 = datetime.date.fromisoformat(start_date)
                    deadline_date = (d0 + datetime.timedelta(days=14)).isoformat()
                memo = body.get("memo", existing["memo"])

                cur.execute(
                    """
                    UPDATE members
                    SET name = ?, email = ?, themeColor = ?, startDate = ?, deadlineDate = ?, memo = ?, updatedAt = ?
                    WHERE id = ?
                    """,
                    (name, email_addr, theme_color, start_date, deadline_date, memo, now_iso, member_id),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "PATCH",
                        "members",
                        {"id": f"eq.{member_id}"},
                        {
                            "name": name,
                            "email": email_addr,
                            "themeColor": theme_color,
                            "startDate": start_date,
                            "deadlineDate": deadline_date,
                            "memo": memo,
                            "updatedAt": now_iso,
                        },
                    )
                except Exception as e:
                    print(f"[Supabase PATCH members] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path.startswith("/api/presets/"):
            preset_id = urllib.parse.unquote(path.split("/api/presets/")[1])
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM presets WHERE id = ?", (preset_id,))
                existing = cur.fetchone()
                if not existing:
                    conn.close()
                    self._send_json({"error": "Preset not found"}, 404)
                    return

                name = body.get("name", existing["name"])
                km = float(body.get("km", existing["km"]))
                count = max(0, int(body.get("count", existing["count"])))

                cur.execute(
                    "UPDATE presets SET name = ?, km = ?, count = ? WHERE id = ?",
                    (name, km, count, preset_id),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "PATCH",
                        "presets",
                        {"id": f"eq.{preset_id}"},
                        {"name": name, "km": km, "count": count},
                    )
                except Exception as e:
                    print(f"[Supabase PATCH presets] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path.startswith("/api/fuel-records/"):
            rec_id = int(path.split("/api/fuel-records/")[1])
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM fuel_records WHERE id = ?", (rec_id,))
                existing = cur.fetchone()
                if not existing:
                    conn.close()
                    self._send_json({"error": "Record not found"}, 404)
                    return

                use_date = body.get("useDate", existing["useDate"])
                deadline = body.get("deadline", existing["deadline"])
                mode = body.get("mode", existing["mode"])
                memo = body.get("memo", existing["memo"])
                biz_km = float(body.get("bizKm", existing["bizKm"]))
                biz_liters = float(body.get("bizLiters", existing["bizLiters"]))
                biz_yen = int(body.get("bizYen", existing["bizYen"]))
                priv_liters = float(body.get("privLiters", existing["privLiters"]))
                priv_yen = int(body.get("privYen", existing["privYen"]))
                settled = int(body.get("settled", existing["settled"]))

                cur.execute(
                    """
                    UPDATE fuel_records
                    SET useDate = ?, deadline = ?, mode = ?, memo = ?,
                        bizKm = ?, bizLiters = ?, bizYen = ?, privLiters = ?, privYen = ?, settled = ?
                    WHERE id = ?
                    """,
                    (use_date, deadline, mode, memo, biz_km, biz_liters, biz_yen, priv_liters, priv_yen, settled, rec_id),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "PATCH",
                        "fuel_records",
                        {"id": f"eq.{rec_id}"},
                        {
                            "useDate": use_date,
                            "deadline": deadline,
                            "mode": mode,
                            "memo": memo,
                            "bizKm": biz_km,
                            "bizLiters": biz_liters,
                            "bizYen": biz_yen,
                            "privLiters": priv_liters,
                            "privYen": priv_yen,
                            "settled": settled,
                        },
                    )
                except Exception as e:
                    print(f"[Supabase PATCH fuel_records] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/notification-config":
            with db_lock:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM notification_config WHERE id = 1")
                existing = dict(cur.fetchone())

                cron_time = body.get("cronTime", existing["cronTime"])
                email_enabled = 1 if body.get("emailEnabled", existing["emailEnabled"]) else 0
                smtp_host = body.get("smtpHost", existing["smtpHost"]).strip()
                smtp_port = int(body.get("smtpPort", existing["smtpPort"]))
                smtp_user = body.get("smtpUser", existing["smtpUser"]).strip()
                smtp_from_name = body.get("smtpFromName", existing["smtpFromName"]).strip()

                smtp_pass = existing["smtpPass"]
                if "smtpPass" in body and body["smtpPass"] != "********":
                    smtp_pass = body["smtpPass"]

                web_push_enabled = 1 if body.get("webPushEnabled", existing["webPushEnabled"]) else 0
                line_enabled = 1 if body.get("lineEnabled", existing["lineEnabled"]) else 0
                line_target = body.get("lineTargetUserId", existing["lineTargetUserId"]).strip()

                line_token = existing["lineChannelToken"]
                if "lineChannelToken" in body and body["lineChannelToken"] != "********":
                    line_token = body["lineChannelToken"].strip()

                cur.execute(
                    """
                    UPDATE notification_config
                    SET cronTime = ?, emailEnabled = ?, smtpHost = ?, smtpPort = ?, smtpUser = ?,
                        smtpPass = ?, smtpFromName = ?, webPushEnabled = ?,
                        lineEnabled = ?, lineChannelToken = ?, lineTargetUserId = ?
                    WHERE id = 1
                    """,
                    (
                        cron_time,
                        email_enabled,
                        smtp_host,
                        smtp_port,
                        smtp_user,
                        smtp_pass,
                        smtp_from_name,
                        web_push_enabled,
                        line_enabled,
                        line_token,
                        line_target,
                    ),
                )
                conn.commit()
                conn.close()

            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request(
                        "PATCH",
                        "notification_config",
                        {"id": "eq.1"},
                        {
                            "cronTime": cron_time,
                            "emailEnabled": email_enabled,
                            "smtpHost": smtp_host,
                            "smtpPort": smtp_port,
                            "smtpUser": smtp_user,
                            "smtpPass": smtp_pass,
                            "smtpFromName": smtp_from_name,
                            "webPushEnabled": web_push_enabled,
                            "lineEnabled": line_enabled,
                            "lineChannelToken": line_token,
                            "lineTargetUserId": line_target,
                        },
                    )
                except Exception as e:
                    print(f"[Supabase PATCH notification_config] {e}")

            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        self._send_json({"error": "Unknown PUT endpoint"}, 404)

    def do_DELETE(self) -> None:
        if not DB_PATH.exists():
            init_db(force_reseed=False)

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/members/"):
            member_id = urllib.parse.unquote(path.split("/api/members/")[1])
            with db_lock:
                conn = get_db_connection()
                conn.execute("DELETE FROM members WHERE id = ?", (member_id,))
                conn.commit()
                conn.close()
            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request("DELETE", "members", {"id": f"eq.{member_id}"})
                except Exception:
                    pass
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path.startswith("/api/presets/"):
            preset_id = urllib.parse.unquote(path.split("/api/presets/")[1])
            with db_lock:
                conn = get_db_connection()
                conn.execute("DELETE FROM presets WHERE id = ?", (preset_id,))
                conn.commit()
                conn.close()
            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request("DELETE", "presets", {"id": f"eq.{preset_id}"})
                except Exception:
                    pass
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path.startswith("/api/fuel-records/"):
            rec_id = int(path.split("/api/fuel-records/")[1])
            with db_lock:
                conn = get_db_connection()
                conn.execute("DELETE FROM fuel_records WHERE id = ?", (rec_id,))
                conn.commit()
                conn.close()
            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request("DELETE", "fuel_records", {"id": f"eq.{rec_id}"})
                except Exception:
                    pass
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        if path == "/api/notification-logs":
            with db_lock:
                conn = get_db_connection()
                conn.execute("DELETE FROM notification_logs")
                conn.commit()
                conn.close()
            if supabase_client.is_supabase_enabled():
                try:
                    supabase_client.sb_request("DELETE", "notification_logs", {"id": "gt.0"})
                except Exception:
                    pass
            broadcast_sse("sync")
            self._send_json({"ok": True, "state": get_full_state(pull_cloud=False)})
            return

        self._send_json({"error": "Unknown DELETE endpoint"}, 404)


class handler(AppHTTPRequestHandler):
    """Vercel Serverless Function entrypoint."""
    pass


def main() -> None:
    init_db(force_reseed=False)
    if supabase_client.is_supabase_enabled():
        sync_from_supabase()

    if "--run-cron" in sys.argv:
        res = execute_daily_notifications(trigger_type="cron_0800")
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return

    port = int(os.environ.get("PORT", "8080"))
    cron_thread = threading.Thread(target=cron_scheduler_loop, daemon=True)
    cron_thread.start()

    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), AppHTTPRequestHandler)
    lan_ip = get_lan_ip()
    print("=" * 68)
    print("🚀 2WEEKコンタクト＆社用ガソリンカード期限管理アプリ 起動完了")
    print(f"💻 このMacからアクセス   : http://localhost:{port}")
    print(f"📱 スマホ・別端末から同期 : http://{lan_ip}:{port}")
    print("⏰ 自動通知Cronジョブ    : 毎朝 08:00 自動実行中 (メール & Web Push)")
    print("=" * 68)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nサーバーを停止しました。")


if __name__ == "__main__":
    main()
