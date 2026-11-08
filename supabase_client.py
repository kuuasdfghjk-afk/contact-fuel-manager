#!/usr/bin/env python3
from __future__ import annotations
"""
Zero-dependency Supabase PostgREST client using Python standard library (urllib.request).
Allows seamless reading/writing to Supabase PostgreSQL from both Vercel Serverless Functions
and local execution without requiring external C-extension packages.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CLOUD_CONFIG_PATH = BASE_DIR / "cloud_config.json"

DEFAULT_SUPABASE_URL = "https://dljrpotzoewusqqihsky.supabase.co"
DEFAULT_SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRsanJwb3R6b2V3dXNxcWloc2t5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTQwNzQ0NzUsImV4cCI6MjEwOTY1MDQ3NX0.1WgBZekCRljF4fopG5w_zhU7PkYHxq76uVOmFEERNhU"


def get_supabase_credentials() -> tuple[str, str]:
    """
    Retrieve SUPABASE_URL and SUPABASE_KEY from environment variables (Vercel / Render),
    local cloud_config.json file, or built-in project defaults.
    """
    url = (
        os.environ.get("SUPABASE_URL")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
        or ""
    ).strip().rstrip("/")
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
        or ""
    ).strip()

    if (not url or not key) and CLOUD_CONFIG_PATH.exists():
        try:
            cfg = json.loads(CLOUD_CONFIG_PATH.read_text(encoding="utf-8"))
            url = url or (cfg.get("supabaseUrl") or "").strip().rstrip("/")
            key = key or (cfg.get("supabaseKey") or "").strip()
        except Exception:
            pass

    url = url or DEFAULT_SUPABASE_URL
    key = key or DEFAULT_SUPABASE_ANON_KEY
    return url, key


def save_local_supabase_credentials(url: str, key: str) -> None:
    try:
        CLOUD_CONFIG_PATH.write_text(
            json.dumps({"supabaseUrl": url.strip().rstrip("/"), "supabaseKey": key.strip()}, indent=2),
            encoding="utf-8",
        )
    except OSError:
        # Read-only filesystem on Vercel Serverless (env vars are used there instead)
        pass


def is_supabase_enabled() -> bool:
    url, key = get_supabase_credentials()
    return bool(url and key)


def sb_request(
    method: str,
    table: str,
    query_params: dict[str, str] | None = None,
    body: dict | list | None = None,
    prefer: str = "return=representation",
) -> list | dict:
    url, key = get_supabase_credentials()
    if not url or not key:
        raise RuntimeError("Supabase URL / Key not configured")

    qs = f"?{urllib.parse.urlencode(query_params)}" if query_params else ""
    endpoint = f"{url}/rest/v1/{table}{qs}"

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }

    data_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(endpoint, data=data_bytes, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            if not raw.strip():
                return []
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"Supabase HTTP {e.code} on {method} {table}: {err_body}")
