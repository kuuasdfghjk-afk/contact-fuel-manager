#!/usr/bin/env python3
from __future__ import annotations
"""
Zero-dependency VAPID (RFC 8292) Web Push helper using system OpenSSL/LibreSSL.
Generates P-256 keys, signs ES256 JWT tokens, and triggers Web Push endpoints.
Once woken up by the push event, the Service Worker fetches the rich notification
payload from /api/notifications/latest-payload and displays the native notification.
"""

import base64
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import urllib.error

OPENSSL_BIN = shutil.which("openssl") or "/usr/bin/openssl"


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s + pad)


def generate_vapid_keys() -> dict:
    """
    Generate a new prime256v1 (P-256) EC keypair using openssl.
    Returns:
      {
        "privateKeyPem": "<PEM string>",
        "publicKey": "<65-byte uncompressed point in base64url>"
      }
    """
    pem_proc = subprocess.run(
        [OPENSSL_BIN, "ecparam", "-name", "prime256v1", "-genkey", "-noout"],
        capture_output=True,
        check=True,
    )
    private_pem = pem_proc.stdout.decode("utf-8")

    text_proc = subprocess.run(
        [OPENSSL_BIN, "ec", "-text", "-noout"],
        input=pem_proc.stdout,
        capture_output=True,
        check=True,
    )
    text_out = text_proc.stdout.decode("utf-8")

    # Extract hex bytes under 'pub:' section
    m = re.search(r"pub:\s*\n((?:\s*[0-9a-fA-F:]+\n)+)", text_out)
    if not m:
        raise RuntimeError("Failed to parse public key from openssl output")

    hex_str = re.sub(r"[^0-9a-fA-F]", "", m.group(1))
    pub_bytes = bytes.fromhex(hex_str)
    if len(pub_bytes) != 65 or pub_bytes[0] != 0x04:
        raise RuntimeError(f"Unexpected uncompressed P-256 public key length: {len(pub_bytes)}")

    return {
        "privateKeyPem": private_pem,
        "publicKey": b64url_encode(pub_bytes),
    }


def _der_to_raw_ecdsa(der_sig: bytes) -> bytes:
    """
    Convert ASN.1 DER encoded ECDSA signature (SEQUENCE { INTEGER r, INTEGER s })
    into 64-byte raw IEEE P1363 format (32-byte big-endian r || 32-byte big-endian s)
    required for JWS ES256.
    """
    if len(der_sig) < 8 or der_sig[0] != 0x30:
        raise ValueError("Invalid DER ECDSA signature header")

    idx = 2
    if der_sig[1] & 0x80:
        idx += (der_sig[1] & 0x7F)

    if der_sig[idx] != 0x02:
        raise ValueError("Expected INTEGER tag for r")
    r_len = der_sig[idx + 1]
    r_bytes = der_sig[idx + 2 : idx + 2 + r_len]
    idx += 2 + r_len

    if der_sig[idx] != 0x02:
        raise ValueError("Expected INTEGER tag for s")
    s_len = der_sig[idx + 1]
    s_bytes = der_sig[idx + 2 : idx + 2 + s_len]

    r_int = int.from_bytes(r_bytes, "big")
    s_int = int.from_bytes(s_bytes, "big")

    return r_int.to_bytes(32, "big") + s_int.to_bytes(32, "big")


def sign_vapid_jwt(private_key_pem: str, audience: str, subject: str = "mailto:kuuasdfghjk@gmail.com") -> str:
    """
    Create and sign an ES256 JWT token for RFC 8292 VAPID Authorization header.
    """
    header = {"typ": "JWT", "alg": "ES256"}
    payload = {
        "aud": audience,
        "exp": int(time.time()) + 12 * 3600,
        "sub": subject,
    }

    signing_input = (
        b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        + "."
        + b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    )

    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as f:
        f.write(private_key_pem)
        pem_path = f.name

    try:
        proc = subprocess.run(
            [OPENSSL_BIN, "dgst", "-sha256", "-sign", pem_path],
            input=signing_input.encode("ascii"),
            capture_output=True,
            check=True,
        )
        raw_sig = _der_to_raw_ecdsa(proc.stdout)
        return signing_input + "." + b64url_encode(raw_sig)
    finally:
        if os.path.exists(pem_path):
            os.remove(pem_path)


def send_web_push(
    endpoint: str,
    vapid_public_key: str,
    vapid_private_key_pem: str,
    subject: str = "mailto:kuuasdfghjk@gmail.com",
    ttl: int = 86400,
) -> tuple[bool, int, str]:
    """
    Send a VAPID-authenticated Web Push request to the subscriber endpoint.
    Returns (success, http_status, message).
    """
    parsed = urllib.parse.urlparse(endpoint)
    audience = f"{parsed.scheme}://{parsed.netloc}"
    jwt_token = sign_vapid_jwt(vapid_private_key_pem, audience=audience, subject=subject)

    headers = {
        "TTL": str(ttl),
        "Authorization": f"vapid t={jwt_token}, k={vapid_public_key}",
        "Crypto-Key": f"p256ecdsa={vapid_public_key}",
        "Content-Length": "0",
        "Urgency": "high",
    }

    req = urllib.request.Request(endpoint, data=b"", headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.getcode()
            return (200 <= status < 300), status, f"HTTP {status}"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")[:200]
        except Exception:
            pass
        return False, e.code, f"HTTP {e.code}: {body}"
    except Exception as e:
        return False, 0, str(e)
