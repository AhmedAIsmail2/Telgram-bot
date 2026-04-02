import atexit
import signal
import logging
import json
import sys as _sys
from flask import Flask, request, jsonify
import sys
import threading
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from threading import Thread
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pytz
import gc
import random
import traceback
import requests
import time
import os

# ---------- Timezone configuration for Egypt ----------
import os
import time
import pytz
from datetime import datetime

os.environ["TZ"] = "Africa/Cairo"
try:
    time.tzset()
except Exception:
    pass


def now_egypt():
    tz = pytz.timezone("Africa/Cairo")
    return datetime.now(tz)
# ---------- End timezone configuration ----------


# Setup file logging
logging.basicConfig(
    filename='bot.log',
    level=logging.WARNING,  # لا يظهر DEBUG أو INFO
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %I:%M %p'   # صيغة التاريخ والوقت
)


class ArabicFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        # تحويل AM/PM إلى ص/م
        msg = msg.replace("AM", "ص").replace("PM", "م")
        return msg


# تطبيق الـ Formatter على كل Handlers
for handler in logging.getLogger().handlers:
    handler.setFormatter(ArabicFormatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %I:%M %p'
    ))

# Heartbeat/watchdog
_last_heartbeat = time.time()
_heartbeat_lock = threading.Lock()


def update_heartbeat():
    """Mark the bot as alive. Call this in key handlers and cron endpoints."""
    global _last_heartbeat
    with _heartbeat_lock:
        _last_heartbeat = time.time()
    # also log to file
    logging.info("[heartbeat] updated")


def start_watchdog_thread(check_interval=30, timeout_seconds=180):
    def _watchdog():
        while True:
            try:
                time.sleep(check_interval)
                now = time.time()
                with _heartbeat_lock:
                    last = _last_heartbeat
                if now - last > timeout_seconds:
                    logging.error(
                        f"[watchdog] no heartbeat for {int(now-last)}s -> alerting admin")
                    try:
                        notify_admin_instant(
                            'watchdog_timeout', location='watchdog', error=f'لم يصل heartbeat منذ {int(now-last)} ثواني')
                    except Exception as e:
                        logging.error(
                            f"[watchdog] failed to notify admin: {e}")
                    # update last_heartbeat to avoid spamming
                    with _heartbeat_lock:
                        _last_heartbeat = time.time()
            except Exception as e:
                logging.error(f"[watchdog] exception: {e}")
    t = threading.Thread(target=_watchdog, daemon=True)
    t.start()


print("DEBUG: reached top of bot.py ✅")

try:
    from sheets_integration import GoogleSheetsIntegration
except ImportError:
    # fallback للكود المحلي لو الملف مش موجود
    class GoogleSheetsIntegration:
        def __init__(self, credentials_file):
            self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file, scopes=self.SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            self.sheets = self.service.spreadsheets()

            # المعرفات الخاصة بيك
            self.SPREADSHEET_ID = '1XDqAhMa_N9iThRotfyI0zkgKhkNoq1EdliM2qC2zOgo'
            self.USER_DATA_SHEET = 'user_data'
            self.QURAN_TRACKING_SHEET = 'quran_tracking'

            # كاش بسيط لتسريع الجلب من Google API
            self._cache = {
                "all_users": None,
                "last_fetch": 0
            }

            self._ensure_sheets_exist()

        # باقي دوال الكلاس لو تحب تسيبها fallback (زي add_or_update_user, get_user_data ...)

    def _safe_execute(self, func, *args, retries=4, delay=1, **kwargs):
        """تنفيذ آمن مع إعادة المحاولة عند حدوث خطأ مؤقت في الشبكة أو Google API"""
        for attempt in range(retries):
            try:
                return func(*args, **kwargs).execute() if hasattr(func, '__call__') else func(*args, **kwargs)
            except HttpError as e:
                print(f"[Google API HttpError] محاولة {attempt+1}: {e}")
            except Exception as e:
                print(
                    f"[Google API Error] محاولة {attempt+1}: {type(e).__name__}: {e}")
                print(traceback.format_exc())
                try:
                    notify_admin(
                        'safe_execute_error', location='_safe_execute', error=traceback.format_exc())
                except Exception:
                    pass
            time.sleep(delay)
        print(
            f"[ERROR] فشل تنفيذ الدالة {getattr(func, '__name__', str(func))} بعد {retries} محاولات.")
        return None

    def _ensure_sheets_exist(self):
        try:
            sheet_metadata = self._safe_execute(
                self.sheets.get, spreadsheetId=self.SPREADSHEET_ID)
            if not sheet_metadata:
                return
            existing_sheets = [s['properties']['title']
                               for s in sheet_metadata.get('sheets', [])]
            if self.USER_DATA_SHEET not in existing_sheets:
                self._create_user_data_sheet()
            if self.QURAN_TRACKING_SHEET not in existing_sheets:
                self._create_quran_tracking_sheet()
        except Exception as e:
            print(f"[DEBUG] Error ensuring sheets exist: {e}")


def _create_user_data_sheet(self):
    header = [
        "user_id", "username", "first_name", "last_name", "join_date",
        "quran_service", "prayer_service", "dhikr_service", "qiyam_service",
        "last_quran_page", "pending_quran_pages", "read_confirmation", "last_update"
    ]

    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {"title": self.USER_DATA_SHEET}
                }
            }
        ]
    }

    # إنشاء الورقة الجديدة
    self._safe_execute(
        self.sheets.batchUpdate,
        spreadsheetId=self.SPREADSHEET_ID,
        body=body
    )

    # كتابة العناوين في الصف الأول
    self._safe_execute(
        self.sheets.values().update,
        spreadsheetId=self.SPREADSHEET_ID,
        range=f"{self.USER_DATA_SHEET}!A1:M1",
        valueInputOption="RAW",
        body={"values": [header]}
    )

    print("[DEBUG] Created sheet:", self.USER_DATA_SHEET)

    def _create_quran_tracking_sheet(self):
        header = ["user_id", "last_quran_page", "last_update"]
        body = {'requests': [
            {'addSheet': {'properties': {'title': self.QURAN_TRACKING_SHEET}}}]}
        self._safe_execute(self.sheets.batchUpdate,
                           spreadsheetId=self.SPREADSHEET_ID, body=body)
        self._safe_execute(
            self.sheets.values().update,
            spreadsheetId=self.SPREADSHEET_ID,
            range=f"{self.QURAN_TRACKING_SHEET}!A1:C1",
            valueInputOption="RAW",
            body={"values": [header]}
        )
        print("[DEBUG] Created sheet:", self.QURAN_TRACKING_SHEET)

    def get_user_data(self, user_id):
        try:
            result = self._safe_execute(
                self.sheets.values().get,
                spreadsheetId=self.SPREADSHEET_ID,
                range=f"{self.USER_DATA_SHEET}!A2:M"
            )
            if not result:
                return None
            rows = result.get('values', [])
            for row in rows:
                if str(row[0]) == str(user_id):
                    return {
                        "user_id": row[0],
                        "username": row[1] if len(row) > 1 else "",
                        "first_name": row[2] if len(row) > 2 else "",
                        "last_name": row[3] if len(row) > 3 else "",
                        "join_date": row[4] if len(row) > 4 else "",
                        "quran_service": row[5] if len(row) > 5 else "False",
                        "prayer_service": row[6] if len(row) > 6 else "False",
                        "dhikr_service": row[7] if len(row) > 7 else "False",
                        "qiyam_service": row[8] if len(row) > 8 else "False",
                        "last_quran_page": row[9] if len(row) > 9 else "0",
                        "pending_quran_pages": row[10] if len(row) > 10 else "",
                        "read_confirmation": row[11] if len(row) > 11 else "",
                        "last_update": row[12] if len(row) > 12 else ""
                    }
        except Exception as e:
            print(f"[get_user_data] Error: {e}")
            print(traceback.format_exc())
        return None

    def add_or_update_user(self, user_data):
        try:
            result = self._safe_execute(
                self.sheets.values().get,
                spreadsheetId=self.SPREADSHEET_ID,
                range=f"{self.USER_DATA_SHEET}!A2:M"
            )
            if not result:
                return
            rows = result.get('values', [])
            updated = False

            for i, row in enumerate(rows, start=2):
                if str(row[0]) == str(user_data["user_id"]):
                    existing_data = self.get_user_data(
                        user_data["user_id"]) or {}
                    existing_data.update(user_data)

                    values = [
                        existing_data.get("user_id", ""),
                        existing_data.get("username", ""),
                        existing_data.get("first_name", ""),
                        existing_data.get("last_name", ""),
                        existing_data.get("join_date", ""),
                        existing_data.get("quran_service", "False"),
                        existing_data.get("prayer_service", "False"),
                        existing_data.get("dhikr_service", "False"),
                        existing_data.get("qiyam_service", "False"),
                        existing_data.get("last_quran_page", "0"),
                        existing_data.get("pending_quran_pages", ""),
                        existing_data.get("read_confirmation", ""),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ]
                    self._safe_execute(
                        self.sheets.values().update,
                        spreadsheetId=self.SPREADSHEET_ID,
                        range=f"{self.USER_DATA_SHEET}!A{i}:M{i}",
                        valueInputOption="RAW",
                        body={"values": [values]}
                    )
                    updated = True
                    break

            if not updated:
                values = [
                    user_data.get("user_id", ""),
                    user_data.get("username", ""),
                    user_data.get("first_name", ""),
                    user_data.get("last_name", ""),
                    user_data.get("join_date", datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S")),
                    user_data.get("quran_service", "False"),
                    user_data.get("prayer_service", "False"),
                    user_data.get("dhikr_service", "False"),
                    user_data.get("qiyam_service", "False"),
                    user_data.get("last_quran_page", "0"),
                    user_data.get("pending_quran_pages", ""),
                    user_data.get("read_confirmation", ""),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
                self._safe_execute(
                    self.sheets.values().append,
                    spreadsheetId=self.SPREADSHEET_ID,
                    range=f"{self.USER_DATA_SHEET}!A2:M",
                    valueInputOption="RAW",
                    body={"values": [values]}
                )
                print(f"[DEBUG] Added new user: {values}")

            # مسح الكاش بعد أي تحديث
            self._cache["all_users"] = None

        except Exception as e:
            print(f"[add_or_update_user] Error: {e}")
            print(traceback.format_exc())

    def get_all_users(self, force_refresh=False):
        # استرجاع من الكاش إن لم يمر أكثر من 30 ثانية
        if (not force_refresh and self._cache["all_users"] and time.time() - self._cache["last_fetch"] < 30):
            return self._cache["all_users"]

        try:
            result = self._safe_execute(
                self.sheets.values().get,
                spreadsheetId=self.SPREADSHEET_ID,
                range=f"{self.USER_DATA_SHEET}!A2:M"
            )
            if not result:
                return []
            rows = result.get('values', [])
            users = [
                {
                    "user_id": row[0],
                    "username": row[1] if len(row) > 1 else "",
                    "first_name": row[2] if len(row) > 2 else "",
                    "last_name": row[3] if len(row) > 3 else "",
                    "join_date": row[4] if len(row) > 4 else "",
                    "quran_service": row[5] if len(row) > 5 else "False",
                    "prayer_service": row[6] if len(row) > 6 else "False",
                    "dhikr_service": row[7] if len(row) > 7 else "False",
                    "qiyam_service": row[8] if len(row) > 8 else "False",
                    "last_quran_page": row[9] if len(row) > 9 else "0",
                    "pending_quran_pages": row[10] if len(row) > 10 else "",
                    "read_confirmation": row[11] if len(row) > 11 else "",
                    "last_update": row[12] if len(row) > 12 else ""
                }
                for row in rows
            ]

            # تخزين في الكاش
            self._cache["all_users"] = users
            self._cache["last_fetch"] = time.time()
            return users
        except Exception as e:
            print(f"[get_all_users] Error: {e}")
            print(traceback.format_exc())
            return []

    # ✅ دالة جديدة: جلب المستخدمين على دفعات صغيرة لتقليل استهلاك الذاكرة
    def get_users_batch(self, start=0, end=10):
        try:
            all_users = self.get_all_users()
            return all_users[start:end]
        except Exception as e:
            print(f"[get_users_batch] Error: {e}")
            print(traceback.format_exc())
            return []
# ---------- END: GoogleSheetsIntegration (محسّن) ----------


# ---------- بقية البوت (محسّن) ----------
# ربط أسماء الأزرار بأسماء الأعمدة في الشيت
SERVICE_KEYS = {
    "quran": "quran_service",
    "prayer": "prayer_service",
    "dhikr": "dhikr_service",
    "qiyam": "qiyam_service"
}

# --------- إعدادات عامة ----------
BOT_TOKEN = os.environ.get("BOT_TOKEN") or "YOUR_BOT_TOKEN"
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_CREDS_FILE") or "telegram-bot-credentials.json"
QURAN_JSON = os.environ.get("QURAN_JSON") or "quran_images_links.json"

# معرف الأدمن
ADMIN_ID = 853742750

# تهيئة Google Sheets integration (النسخة المدموجة أعلاه)
sheets = GoogleSheetsIntegration(SERVICE_ACCOUNT_FILE)

# ---------- تحسين requests Session مع retries لتيليجرام ----------
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=0.4,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "POST",
                     "PUT", "DELETE", "OPTIONS", "TRACE"]
)
adapter = HTTPAdapter(max_retries=retry_strategy,
                      pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ---------- Lazy loading للصور القرآن بدل تحميل كامل الملف في الذاكرة ----------


def get_quran_images_count():
    """Return number of quran image entries in the JSON file without keeping them in memory."""
    try:
        with open(QURAN_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            count = len(data) if isinstance(data, list) else 0
        # مسح المتغير لتفريغ الذاكرة بسرعة
        try:
            del data
        except Exception:
            pass
        gc.collect()
        return count
    except Exception as e:
        print(f"[get_quran_images_count] Error: {e}")
        return 0


def get_quran_page_link(page_index):
    """Return the URL for the given page index (0-based) by reading the JSON file on demand."""
    try:
        with open(QURAN_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list) and 0 <= page_index < len(data):
                url = data[page_index].get("url")
            else:
                url = None
        # مسح المتغير لتقليل بصمة الذاكرة
        try:
            del data
        except Exception:
            pass
        gc.collect()
        return url
    except Exception as e:
        print(f"[get_quran_page_link] Error reading {QURAN_JSON}: {e}")
        return None


MAX_QURAN_PAGES = None  # لنستخدم الدالة get_quran_images_count() عند الحاجة

# timezone
TZ = pytz.timezone("Africa/Cairo")

# ---------- Logging helper ----------


def log_error(prefix, e):
    try:
        print(f"[{prefix}] {type(e).__name__}: {e}")
        tb = traceback.format_exc()
        print(tb)
    except Exception:
        print(f"[{prefix}] error logging failed: {e}")

# ---------- Utilities ----------


def bool_from_str(s):
    if s is None:
        return False
    s = str(s).lower()
    return s in ("1", "true", "yes", "y", "نعم", "on", "true")


def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return None


# ---------- Telegram API (improved) ----------
DEFAULT_TIMEOUT = (5, 20)  # connect, read timeout (seconds)  # seconds


def telegram_request(method, payload=None, files=None, timeout=DEFAULT_TIMEOUT):
    url = f"{TELEGRAM_API_BASE}/{method}"
    for attempt in range(3):
        try:
            if files:
                resp = session.post(url, data=payload,
                                    files=files, timeout=timeout)
            else:
                resp = session.post(url, json=payload, timeout=timeout)
            data = safe_json(resp)
            if not resp.ok:
                print(
                    f"[telegram_request] Warning: Bad status code {resp.status_code} for {method}")
            if data is None:
                print(
                    f"[telegram_request] non-json response for {method} (attempt {attempt+1}). Status: {getattr(resp, 'status_code', None)}")
            else:
                return data
        except requests.exceptions.RequestException as e:
            print(
                f"[telegram_request] network error for {method} (attempt {attempt+1}): {e}")
            time.sleep(0.6 + random.uniform(0, 0.6))
        except Exception as e:
            log_error("telegram_request", e)
            time.sleep(0.6)
    return None


def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = telegram_request("sendMessage", payload)
        return resp
    except Exception as e:
        log_error("send_message", e)
        return None


# ---------- Notifications to Admin (non-spam, 60s delay) ----------

ADMIN_ID = 853742750
_notify_recent = {}
_notify_lock = threading.Lock()


def _send_pending_alert(alert_key: str, message_text: str):
    """Send the alert after 30 seconds (runs in background Thread)."""
    try:
        time.sleep(30)
        try:
            # Prefer existing send_message() if available
            try:
                send_message(ADMIN_ID, message_text)
            except NameError:
                # fallback to telegram.Bot if send_message not available
                try:
                    from telegram import Bot
                    TOKEN = os.environ.get("BOT_TOKEN") or "YOUR_BOT_TOKEN"
                    Bot(TOKEN).send_message(ADMIN_ID, message_text)
                except Exception as e_bot:
                    print(f"[notify_admin] فشل ارسال عبر Bot: {e_bot}")
            print("[notify_admin] تم إرسال تنبيه للإدمن")
        except Exception as e_send:
            print(f"[notify_admin] فشل إرسال التنبيه: {e_send}")
    except Exception as e:
        print(f"[notify_admin] خطأ في _send_pending_alert: {e}")


def notify_admin_instant(alert_key: str, *, location: str = "", error: str = "", extra: str = ""):
    """Send an immediate admin notification using the exact same formatting as notify_admin.
    This bypasses the deferred 30s send and is used for critical alerts (signals, watchdog).
    """
    try:
        timestamp = now_egypt().strftime(
            "%I:%M %p – %d/%m/%Y").replace("AM", "ص").replace("PM", "م")
        message = (
            f"⚠️ تنبيه إدارة: ({alert_key})\n\n"
            f"📍 المكان: {location}\n"
            f"🕒 الوقت: {timestamp}\n"
            f"💥 الخطأ: {error}"
        )
        if extra:
            message += f"\n\n🧩 التفاصيل:\n{extra}"

        if len(message) > 4000:
            message = message[:4000] + \
                "\n\n⚠️ تم اختصار الرسالة لأنها طويلة جدًا"

        try:
            send_message(ADMIN_ID, message)
        except Exception:
            try:
                from telegram import Bot
                TOKEN = os.environ.get("BOT_TOKEN") or "YOUR_BOT_TOKEN"
                Bot(TOKEN).send_message(ADMIN_ID, message)
            except Exception as e:
                logging.error(f"[notify_admin_instant] failed to send: {e}")
        logging.info(
            f"[notify_admin_instant] sent immediate alert ({alert_key})")
    except Exception as e:
        logging.error(
            f"[notify_admin_instant] exception while building message: {e}")


def notify_admin(alert_key: str, *, location: str = "", error: str = "", extra: str = ""):
    """Schedule an admin alert:
    - Do not send the same alert within 180 seconds.
    - Use Arabic 12-hour time with صباحًا/مساءً.
    """
    now = time.time()
    with _notify_lock:
        last = _notify_recent.get(alert_key)
        if last and (now - last) < 180:
            return
        _notify_recent[alert_key] = now

    timestamp = now_egypt().strftime(
        "%I:%M %p – %d/%m/%Y").replace("AM", "ص").replace("PM", "م")

    message = (
        f"⚠️ تنبيه إدارة: ({alert_key})\n\n"
        f"📍 المكان: {location}\n"
        f"🕒 الوقت: {timestamp}\n"
        f"💥 الخطأ: {error}"
    )
    if extra:
        message += f"\n\n🧩 التفاصيل:\n{extra}"

    print(f"[notify_admin] تم جدولة تنبيه ({alert_key}) بعد 30 ثانية")
    t = threading.Thread(target=_send_pending_alert,
                         args=(alert_key, message), daemon=True)
    t.start()


def _thread_wrapper(fn, *a, **kw):
    """Wrapper for thread targets to capture exceptions and notify admin."""
    try:
        return fn(*a, **kw)
    except Exception:
        try:
            notify_admin("thread_exception", location=getattr(
                fn, "__name__", "thread"), error=traceback.format_exc())
        except Exception:
            pass
        raise

# ---------- end notifications ----------


def send_photo(chat_id, photo_url, caption=None):
    payload = {"chat_id": chat_id, "photo": photo_url}
    if caption:
        payload["caption"] = caption
        payload["parse_mode"] = "HTML"
    try:
        resp = telegram_request("sendPhoto", payload)
        return resp
    except Exception as e:
        log_error("send_photo", e)
        return None


def answer_callback(callback_id, text=None, show_alert=False):
    payload = {"callback_query_id": callback_id, "show_alert": show_alert}
    if text:
        payload["text"] = text
    try:
        return telegram_request("answerCallbackQuery", payload)
    except Exception as e:
        log_error("answer_callback", e)
        return None


def edit_message_reply_markup(chat_id, message_id, reply_markup):
    payload = {"chat_id": chat_id, "message_id": message_id,
               "reply_markup": reply_markup}
    try:
        return telegram_request("editMessageReplyMarkup", payload)
    except Exception as e:
        log_error("edit_message_reply_markup", e)
        return None


def edit_message_text(chat_id, message_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id,
               "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        return telegram_request("editMessageText", payload)
    except Exception as e:
        log_error("edit_message_text", e)
        return None


def delete_message(chat_id, message_id):
    payload = {"chat_id": chat_id, "message_id": message_id}
    try:
        return telegram_request("deleteMessage", payload)
    except Exception as e:
        log_error("delete_message", e)
        return None

# ---------- الخدمات ----------


def build_services_keyboard(user_data):
    quran = bool_from_str(user_data.get("quran_service")
                          if user_data else False)
    prayer = bool_from_str(user_data.get(
        "prayer_service") if user_data else False)
    dhikr = bool_from_str(user_data.get("dhikr_service")
                          if user_data else False)
    qiyam = bool_from_str(user_data.get("qiyam_service")
                          if user_data else False)

    def btn(label, key, state):
        text = f"{label} {'✅' if state else '❌'}"
        return {"text": text, "callback_data": f"toggle:{key}"}

    keyboard = [
        [btn("- القرآن الكريم 📖", "quran", quran)],
        [btn("- الصلاة على النبي ﷺ", "prayer", prayer)],
        [btn("- الأدعية وذكر اللّٰه 🤲", "dhikr", dhikr)],
        [btn("- قيام الليل 🌙", "qiyam", qiyam)],
        [{"text": "🔵 تأكيد الاختيارات 🔵", "callback_data": "confirm"}]
    ]
    return {"inline_keyboard": keyboard}

# ---------- Google Sheets helpers (batching) ----------


def get_users_in_batches(batch_size=7):
    """
    Generator that yields lists of users.
    Uses sheets.get_users_batch(start,end) if available to avoid loading all users.
    """
    try:
        if hasattr(sheets, "get_users_batch"):
            start = 0
            while True:
                try:
                    batch = sheets.get_users_batch(start, start + batch_size)
                except Exception as e:
                    try:
                        batch = sheets.get_users_batch(
                            start + 1, start + batch_size)
                    except Exception as e2:
                        log_error(
                            "get_users_in_batches -> get_users_batch", e2)
                        break
                if not batch:
                    break
                yield batch
                start += batch_size
        else:
            all_users = []
            try:
                all_users = sheets.get_all_users() or []
            except Exception as e:
                log_error("get_users_in_batches -> get_all_users", e)
                all_users = []

            if not all_users:
                return

            for i in range(0, len(all_users), batch_size):
                yield all_users[i:i + batch_size]
    except Exception as e:
        log_error("get_users_in_batches", e)
        return


def ensure_user_row(telegram_user):
    user_id = telegram_user["id"]
    try:
        existing = sheets.get_user_data(user_id)
    except Exception as e:
        log_error("ensure_user_row -> get_user_data", e)
        existing = None

    now = now_egypt().strftime("%Y-%m-%d %H:%M:%S")

    if existing:
        return existing

    new_user = {
        "user_id": user_id,
        "username": telegram_user.get("username", ""),
        "first_name": telegram_user.get("first_name", ""),
        "last_name": telegram_user.get("last_name", ""),
        "join_date": now,
        "quran_service": "",
        "prayer_service": "",
        "dhikr_service": "",
        "qiyam_service": "",
        "last_quran_page": "0",
        "pending_quran_pages": "",
        "pending_quran_message_id": "",
        "read_confirmation": "",
        "last_update": now
    }
    try:
        sheets.add_or_update_user(new_user)
    except Exception as e:
        log_error("ensure_user_row -> add_or_update_user", e)
    return new_user


def toggle_service_for_user(user_id, key):
    sheet_key = SERVICE_KEYS.get(key)
    if not sheet_key:
        return None

    try:
        user = sheets.get_user_data(user_id)
    except Exception as e:
        log_error("toggle_service_for_user -> get_user_data", e)
        user = None

    if not user:
        return None

    current = bool_from_str(user.get(sheet_key))
    user[sheet_key] = "False" if current else "True"
    user["last_update"] = now_egypt().strftime("%Y-%m-%d %H:%M:%S")

    try:
        sheets.add_or_update_user(user)
    except Exception as e:
        log_error("toggle_service_for_user -> add_or_update_user", e)
    return user

# ---------- وظائف التعامل مع ورد القرآن ----------


def parse_pending_range(s):
    if not s:
        return (None, None)
    try:
        parts = str(s).split("-")
        if len(parts) == 2:
            return (int(parts[0]), int(parts[1]))
        elif len(parts) == 1:
            v = int(parts[0])
            return (v, v)
    except Exception:
        pass
    return (None, None)


def send_quran_batch_for_user(user):
    chat_id = int(user["user_id"])
    last_page = int(user.get("last_quran_page") or 0)
    start = last_page + 1
    max_pages = get_quran_images_count()
    if start > max_pages:
        send_message(chat_id, "انتهت صور القرآن المتاحة. ✅")
        return {"sent": 0, "finished": True}

    end = min(start + 4, max_pages)  # إرسال 5 صفحات: start..end
    intro_text = f"🔵 إليك ورد اليوم من القرآن الكريم (من {start} إلى {end}) :"
    try:
        send_message(chat_id, intro_text)
    except Exception as e:
        log_error("send_quran_batch_for_user -> send_message intro", e)

    sent = 0
    for idx in range(start, end + 1):
        try:
            img_url = get_quran_page_link(idx - 1)
            if img_url:
                send_photo(chat_id, img_url)
                sent += 1
            else:
                log_error("send_quran_batch_for_user", Exception(
                    f"Missing url for index {idx-1}"))
            time.sleep(0.6)
        except IndexError:
            log_error("send_quran_batch_for_user", IndexError(
                f"index {idx-1} missing in QURAN_IMAGES"))
        except Exception as e:
            log_error("send_quran_batch_for_user -> send_photo", e)

    pending_range = f"{start}-{end}"
    user["pending_quran_pages"] = pending_range
    user["read_confirmation"] = "pending"
    user["last_update"] = now_egypt().strftime("%Y-%m-%d %H:%M:%S")
    keyboard = {"inline_keyboard": [
        [{"text": "نعم ✅", "callback_data": "read_yes"},
            {"text": "لا ❌", "callback_data": "read_no"}]
    ]}
    resp = send_message(chat_id, "هل قرأت الوِرد؟", reply_markup=keyboard)
    msg_id = None
    try:
        if resp and resp.get("ok"):
            msg_id = resp["result"]["message_id"]
            user["pending_quran_message_id"] = str(msg_id)
    except Exception as e:
        log_error("send_quran_batch_for_user -> save msg id", e)

    try:
        sheets.add_or_update_user(user)
    except Exception as e:
        log_error("send_quran_batch_for_user -> add_or_update_user", e)

    # تنظيف الذاكرة بعد المهمة
    try:
        gc.collect()
    except Exception:
        pass

    return {"sent": sent, "finished": (end >= max_pages), "pending": pending_range, "message_id": msg_id}


def resend_pending_quran_for_user(user):
    chat_id = int(user["user_id"])
    start, end = parse_pending_range(user.get("pending_quran_pages", ""))
    if not start:
        return send_quran_batch_for_user(user)

    intro_text = f"🔵 إليك ورد اليوم من القرآن الكريم (من {start} إلى {end}) :"
    try:
        send_message(chat_id, intro_text)
    except Exception as e:
        log_error("resend_pending_quran_for_user -> send_message intro", e)

    sent = 0
    for idx in range(start, end + 1):
        try:
            img_url = get_quran_page_link(idx - 1)
            if img_url:
                send_photo(chat_id, img_url)
                sent += 1
            else:
                log_error("resend_pending_quran_for_user",
                          Exception(f"Missing url for index {idx-1}"))
            time.sleep(0.6)
        except Exception as e:
            log_error("resend_pending_quran_for_user -> send_photo", e)

    user["read_confirmation"] = "pending"
    user["last_update"] = now_egypt().strftime("%Y-%m-%d %H:%M:%S")

    keyboard = {"inline_keyboard": [
        [{"text": "نعم ✅", "callback_data": "read_yes"},
            {"text": "لا ❌", "callback_data": "read_no"}]
    ]}
    resp = send_message(chat_id, "هل قرأت الوِرد؟", reply_markup=keyboard)
    try:
        if resp and resp.get("ok"):
            user["pending_quran_message_id"] = str(
                resp["result"]["message_id"])
    except Exception as e:
        log_error("resend_pending_quran_for_user -> save msg id", e)

    try:
        sheets.add_or_update_user(user)
    except Exception as e:
        log_error("resend_pending_quran_for_user -> add_or_update_user", e)

    # تنظيف الذاكرة بعد المهمة
    try:
        gc.collect()
    except Exception:
        pass

    return {"resent": sent, "pending": f"{start}-{end}"}


# ---------- Flask webhook ----------
app = Flask(__name__)


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update_heartbeat()
    except Exception:
        pass

    try:
        data = {}
        try:
            data = request.get_json(force=True)
        except Exception as e:
            try:
                notify_admin('webhook_error', location='webhook()',
                             error=traceback.format_exc())
            except Exception:
                pass
            log_error("webhook -> get_json", e)
            return jsonify(ok=True)

        # التعامل مع الرسائل العادية
        if "message" in data:
            msg = data["message"]
            chat_id = msg["chat"]["id"]
            user_info = {
                "id": msg["from"]["id"],
                "username": msg["from"].get("username", ""),
                "first_name": msg["from"].get("first_name", ""),
                "last_name": msg["from"].get("last_name", "")
            }
            text = msg.get("text", "")

            if text and text.startswith("/start"):
                try:
                    existing_user = sheets.get_user_data(user_info["id"])
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("webhook /start -> get_user_data", e)
                    existing_user = None

                if existing_user:
                    active_services = []
                    if bool_from_str(existing_user.get("quran_service")):
                        active_services.append("- القرآن الكريم 📖")
                    if bool_from_str(existing_user.get("prayer_service")):
                        active_services.append("- الصلاة على النبي ﷺ")
                    if bool_from_str(existing_user.get("dhikr_service")):
                        active_services.append("- الأدعية وذكر اللّٰه 🤲")
                    if bool_from_str(existing_user.get("qiyam_service")):
                        active_services.append("- قيام الليل 🌙")

                    services_text = "\n".join(
                        active_services) if active_services else "لا توجد اشتراكات حالياً."
                    welcome_text = (
                        f"مرحباً مجدداً {existing_user.get('first_name', '')} ! 👋\n\n"
                        f"أنت مشترك حالياً في الخدمات التالية :\n{services_text}\n\n"
                        "يمكنك تعديل اختياراتك باستخدام الأمر /edit\n"
                        "أو إيقاف البوت مؤقتاً باستخدام الأمر /stop"
                    )

                    keyboard = {
                        "inline_keyboard": [
                            [{"text": "تعديل الخدمات ✏️ ",
                                "callback_data": "edit_services"}],
                            [{"text": "إيقاف البوت ⏹ ", "callback_data": "stop_bot"}]
                        ]
                    }
                    send_message(chat_id, welcome_text, reply_markup=keyboard)
                    gc.collect()
                    return jsonify(ok=True)
                else:
                    user = ensure_user_row(user_info)
                    kb = build_services_keyboard(user)
                    send_message(
                        chat_id, "أهلاً بك في بوت « اذكر اللّٰه ».\nاختر الخدمات التي تريد الاشتراك فيها :", reply_markup=kb)
                    gc.collect()
                    return jsonify(ok=True)

            elif text and text.startswith("/edit"):
                try:
                    user = sheets.get_user_data(user_info["id"])
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("webhook /edit -> get_user_data", e)
                    user = None

                if user:
                    kb = build_services_keyboard(user)
                    send_message(
                        chat_id, "اختر الخدمات التي تريدها ( اختيار متعدد ) : ", reply_markup=kb)
                else:
                    send_message(
                        chat_id, "يجب عليك استخدام /start أولاً للتسجيل في البوت.")
                gc.collect()
                return jsonify(ok=True)

            elif text and text.startswith("/stop"):
                try:
                    user = sheets.get_user_data(user_info["id"])
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("webhook /stop -> get_user_data", e)
                    user = None

                if user:
                    user["quran_service"] = "False"
                    user["prayer_service"] = "False"
                    user["dhikr_service"] = "False"
                    user["qiyam_service"] = "False"
                    try:
                        sheets.add_or_update_user(user)
                    except Exception as e:
                        try:
                            notify_admin(
                                'webhook_error', location='webhook()', error=traceback.format_exc())
                        except Exception:
                            pass
                        log_error("webhook /stop -> add_or_update_user", e)
                    send_message(
                        chat_id, "✅ تم إيقاف جميع الاشتراكات. يمكنك إعادة تشغيلها باستخدام /start أو /edit.")
                else:
                    send_message(chat_id, "لا توجد اشتراكات لإيقافها.")
                gc.collect()
                return jsonify(ok=True)

            elif text and text.startswith("/users_count"):
                if user_info["id"] != ADMIN_ID:
                    send_message(chat_id, "❌ هذا الأمر مخصص للأدمن فقط.")
                    return jsonify(ok=True)

                try:
                    all_users = sheets.get_all_users()
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("webhook /users_count -> get_all_users", e)
                    all_users = []

                total_users = len(all_users) if all_users else 0
                quran_count = sum(
                    1 for u in all_users if bool_from_str(u.get("quran_service")))
                prayer_count = sum(
                    1 for u in all_users if bool_from_str(u.get("prayer_service")))
                dhikr_count = sum(
                    1 for u in all_users if bool_from_str(u.get("dhikr_service")))
                qiyam_count = sum(
                    1 for u in all_users if bool_from_str(u.get("qiyam_service")))

                stats_text = (
                    f"إحصائيات البوت 📊 :\n\n"
                    f"إجمالي المستخدمين 👥 : {total_users}\n\n"
                    f"عدد المشتركين في القرآن الكريم 📖 : {quran_count}\n"
                    f"عدد المشتركين في الصلاة على النبي ﷺ : {prayer_count}\n"
                    f"مشتركين في الأذكار والأدعية 🤲 : {dhikr_count}\n"
                    f"عدد المشتركين في قيام الليل 🌙 : {qiyam_count}"
                )
                send_message(chat_id, stats_text)
                gc.collect()
                return jsonify(ok=True)

            elif text and text.startswith("/test_admin_alert"):
                if user_info["id"] != ADMIN_ID:
                    send_message(chat_id, "❌ هذا الأمر مخصص للأدمن فقط.")
                    return jsonify(ok=True)

                # تجربة إرسال تنبيه يدوي
                send_message(
                    chat_id, "🔄 جاري اختبار نظام التنبيهات... سيتم الإرسال بعد 30 ثانيه ⏳")
                notify_admin("test_manual_alert", location="manual_test",
                             error="اختبار يدوي من الأدمن ✅")
                return jsonify(ok=True)

            elif text and text.startswith("/users_info"):
                if user_info["id"] != ADMIN_ID:
                    send_message(chat_id, "❌ هذا الأمر مخصص للأدمن فقط.")
                    return jsonify(ok=True)

                try:
                    all_users = sheets.get_all_users()
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("webhook /users_info -> get_all_users", e)
                    all_users = []

                if not all_users:
                    send_message(chat_id, "لا يوجد مستخدمين مسجلين.")
                    return jsonify(ok=True)

                users_info = []
                for user in all_users:
                    name = user.get("first_name", "")
                    if user.get("last_name"):
                        name += f" {user.get('last_name')}"
                    if not name:
                        name = user.get(
                            "username", f"User {user.get('user_id')}")

                    join_date = user.get("join_date", "")
                    if join_date and " " in join_date:
                        join_date = join_date.split(" ")[0]

                    services = []
                    if bool_from_str(user.get("quran_service")):
                        services.append("القرآن")
                    if bool_from_str(user.get("prayer_service")):
                        services.append("الصلاة")
                    if bool_from_str(user.get("dhikr_service")):
                        services.append("الأذكار")
                    if bool_from_str(user.get("qiyam_service")):
                        services.append("قيام الليل")

                    services_text = ", ".join(
                        services) if services else "لا توجد اشتراكات"
                    user_info_text = f"الاسم : {name}\nالتاريخ : {join_date}\nالخدمات : {services_text}\n"
                    users_info.append(user_info_text)

                message_text = "معلومات المستخدمين 👥 :\n\n"
                for info in users_info:
                    if len(message_text + info) > 4000:
                        send_message(chat_id, message_text)
                        message_text = info + "\n"
                    else:
                        message_text += info + "\n"

                if message_text.strip():
                    send_message(chat_id, message_text)

                gc.collect()
                return jsonify(ok=True)

        # التعامل مع الكول باك من الأزرار
        if "callback_query" in data:
            cb = data["callback_query"]
            cid = cb["id"]
            from_user = cb["from"]
            chat = cb.get("message", {}).get("chat", {})
            message = cb.get("message", {})
            message_id = message.get("message_id")
            data_cd = cb["data"]

            if data_cd.startswith("toggle:"):
                key = data_cd.split(":", 1)[1]
                user = toggle_service_for_user(from_user["id"], key)
                kb = build_services_keyboard(user)
                try:
                    if message_id and chat.get("id"):
                        edit_message_reply_markup(chat["id"], message_id, kb)
                    answer_callback(cid)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback toggle", e)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "edit_services":
                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback edit_services -> delete_message", e)

                try:
                    user = sheets.get_user_data(from_user["id"])
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback edit_services -> get_user_data", e)
                    user = None

                kb = build_services_keyboard(user)
                send_message(chat.get(
                    "id"), "اختر الخدمات التي تريدها ( اختيار متعدد ) : ", reply_markup=kb)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "stop_bot":
                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback stop_bot -> delete_message", e)

                try:
                    user = sheets.get_user_data(from_user["id"])
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback stop_bot -> get_user_data", e)
                    user = None

                if user:
                    user["quran_service"] = "False"
                    user["prayer_service"] = "False"
                    user["dhikr_service"] = "False"
                    user["qiyam_service"] = "False"
                    try:
                        sheets.add_or_update_user(user)
                    except Exception as e:
                        try:
                            notify_admin(
                                'webhook_error', location='webhook()', error=traceback.format_exc())
                        except Exception:
                            pass
                        log_error("callback stop_bot -> add_or_update_user", e)
                send_message(
                    chat.get("id"), "✅ تم إيقاف البوت مؤقتاً. يمكنك إعادة تشغيله باستخدام /start.")
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "confirm":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback confirm -> get_user_data", e)
                    user = {}

                parts = []
                if bool_from_str(user.get("quran_service")):
                    parts.append(
                        "1- خدمة القرآن الكريم 📖 : يومياً الساعة 12:10 ظهراً.\n")
                if bool_from_str(user.get("prayer_service")):
                    parts.append(
                        "2- خدمة الصلاة على النبي ﷺ : كل ثُلث ساعه.\n")
                if bool_from_str(user.get("dhikr_service")):
                    parts.append(
                        "3- خدمة الأدعية وذكر اللّٰه 🤲 : في مواعيد متفرقه\n")
                if bool_from_str(user.get("qiyam_service")):
                    parts.append(
                        "4- خدمة قيام الليل 🌙 : يومياً في الثُلث الأخير من الليل.")
                if not parts:
                    send_message(chat.get("id"), "لم تختر أي خدمة.")
                    answer_callback(cid)
                    gc.collect()
                    return jsonify(ok=True)
                text = "مواعيد التذكيرات :\n\n" + "\n".join(parts)

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback confirm -> delete_message", e)

                send_message(chat.get("id"), "تم تأكيد اختيار الخدمات")
                send_message(chat.get(
                    "id"), "يمكنك تعديل اختياراتك باستخدام الأمر /edit\nأو إيقاف البوت مؤقتاً باستخدام الأمر /stop")
                send_message(chat.get("id"), text)
                answer_callback(cid, text="تم تأكيد الاختيارات ✅")
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "more_quran":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback more_quran -> get_user_data", e)
                    user = {}
                send_quran_batch_for_user(user)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "no_quran":
                try:
                    if message_id and chat.get("id"):
                        edit_message_text(
                            chat["id"], message_id, "تم إيقاف إرسال المزيد مؤقتًا.")
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback no_quran -> edit_message_text", e)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "read_yes":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_yes -> get_user_data", e)
                    user = {}

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_yes -> delete_message", e)

                start, end = parse_pending_range(
                    user.get("pending_quran_pages", ""))
                if start and end:
                    user["last_quran_page"] = str(end)
                    user["pending_quran_pages"] = ""
                    user["pending_quran_message_id"] = ""
                    user["read_confirmation"] = "yes"
                    user["last_update"] = now_egypt().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    try:
                        sheets.add_or_update_user(user)
                    except Exception as e:
                        try:
                            notify_admin(
                                'webhook_error', location='webhook()', error=traceback.format_exc())
                        except Exception:
                            pass
                        log_error("callback read_yes -> add_or_update_user", e)

                keyboard = {"inline_keyboard": [
                    [{"text": "نعم ✅", "callback_data": "more_yes"},
                        {"text": "لا ❌", "callback_data": "more_no"}]
                ]}
                send_message(chat.get("id"), "هل تريد المزيد؟",
                             reply_markup=keyboard)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "read_no":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_no -> get_user_data", e)
                    user = {}

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_no -> delete_message", e)

                user["read_confirmation"] = "no"
                user["pending_quran_message_id"] = ""
                user["last_update"] = now_egypt().strftime("%Y-%m-%d %H:%M:%S")
                try:
                    sheets.add_or_update_user(user)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_no -> add_or_update_user", e)

                send_message(chat.get("id"),
                             "حسناً ، سيتم تذكيرك بالقرائه لاحقا او سنرسل لك غداً إن شاء اللّٰه.")
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "more_yes":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback more_yes -> get_user_data", e)
                    user = {}

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback more_yes -> delete_message", e)

                send_quran_batch_for_user(user)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "more_no":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback more_no -> get_user_data", e)
                    user = {}

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback more_no -> delete_message", e)

                send_message(chat.get("id"),
                             "حسناً ، سنرسل لك المزيد غداً إن شاء الله.")
                total_pages = int(user.get("last_quran_page") or 0)
                send_message(chat.get("id"),
                             f"أنت خلصت {total_pages} صفحات من القرآن الكريم.")
                user["pending_quran_pages"] = ""
                user["pending_quran_message_id"] = ""
                user["read_confirmation"] = ""
                user["last_update"] = now_egypt().strftime("%Y-%m-%d %H:%M:%S")
                try:
                    sheets.add_or_update_user(user)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback more_no -> add_or_update_user", e)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "reminder_resend":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback reminder_resend -> get_user_data", e)
                    user = {}

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback reminder_resend -> delete_message", e)

                resend_pending_quran_for_user(user)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "reminder_no":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback reminder_no -> get_user_data", e)
                    user = {}

                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback reminder_no -> delete_message", e)

                user["read_confirmation"] = "no"
                user["last_update"] = now_egypt().strftime("%Y-%m-%d %H:%M:%S")
                try:
                    sheets.add_or_update_user(user)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback reminder_no -> add_or_update_user", e)

                send_message(chat.get("id"),
                             "حسناً ، سنرسل لك غداً إن شاء اللّٰه.")
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)

            if data_cd == "read_yes_mid":
                try:
                    user = sheets.get_user_data(from_user["id"]) or {}
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_yes_mid -> get_user_data", e)
                    user = {}

                # delete the reminder message if exists
                try:
                    if message_id and chat.get("id"):
                        delete_message(chat["id"], message_id)
                except Exception as e:
                    try:
                        notify_admin(
                            'webhook_error', location='webhook()', error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("callback read_yes_mid -> delete_message", e)

                start, end = parse_pending_range(
                    user.get("pending_quran_pages", ""))
                if start and end:
                    # mark pages as read
                    user["last_quran_page"] = str(end)
                    user["pending_quran_pages"] = ""
                    user["pending_quran_message_id"] = ""
                    user["read_confirmation"] = "yes"
                    user["last_update"] = now_egypt().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    try:
                        sheets.add_or_update_user(user)
                    except Exception as e:
                        try:
                            notify_admin(
                                'webhook_error', location='webhook()', error=traceback.format_exc())
                        except Exception:
                            pass
                        log_error(
                            "callback read_yes_mid -> add_or_update_user", e)

                # ask if want more (same as morning flow)
                keyboard = {"inline_keyboard": [
                    [{"text": "نعم ✅", "callback_data": "more_yes"},
                        {"text": "لا ❌", "callback_data": "more_no"}]
                ]}
                send_message(chat.get("id"), "هل تريد المزيد؟",
                             reply_markup=keyboard)
                answer_callback(cid)
                gc.collect()
                return jsonify(ok=True)
        gc.collect()
        return jsonify(ok=True)
    except Exception as e:
        log_error('webhook -> main', e)
        try:
            notify_admin('webhook_error', location='webhook()',
                         error=traceback.format_exc())
        except Exception:
            pass
        return jsonify(ok=True)

# ---------- Endpoints خاصة بالـ Cron (استدعاء من CronJob.org أو أي مُجدول) ----------


@app.route("/cron/quran", methods=["GET", "POST"])
def cron_quran():
    try:
        update_heartbeat()
    except Exception:
        pass
    # Start the long-running Quran sending task in a background thread to avoid worker timeout.
    thread = Thread(target=lambda: _thread_wrapper(run_quran_task))
    thread.daemon = True
    thread.start()
    return "Started Quran task ✅", 200


def run_quran_task():
    try:
        sent_total = 0
        batch_size = 5

        for users_batch in get_users_in_batches(batch_size=batch_size):
            for u in users_batch:
                try:
                    if not bool_from_str(u.get("quran_service")):
                        continue

                    pending = u.get("pending_quran_pages", "")

                    # لو في وِرد متأخر ولسه الرد pending أو no → ابعت الوِرد المتأخر
                    if pending and u.get("read_confirmation") in ("pending", "no"):
                        res = resend_pending_quran_for_user(u)
                        sent_total += res.get("resent",
                                              0) if isinstance(res, dict) else 0

                    else:
                        # إرسال الوِرد الجديد
                        res = send_quran_batch_for_user(u)
                        sent_total += res.get("sent",
                                              0) if isinstance(res, dict) else 0

                    time.sleep(0.6)

                except Exception as e:
                    try:
                        notify_admin(
                            "cron_quran_error", location="run_quran_task", error=traceback.format_exc())
                    except Exception:
                        pass
                    log_error("cron_quran -> sending", e)

            time.sleep(1)
            gc.collect()

    except Exception as e:
        log_error("run_quran_task -> main", e)
        try:
            notify_admin("cron_quran_error", location="run_quran_task",
                         error=traceback.format_exc())
        except Exception:
            pass

# مفيش داعي ترجع حاجة من هنا لأن الـ thread داخلي


def run_bismillah_task():
    try:
        text = " 🟡 بِسمِ اللّٰهِ الّذي لا يَضُرُّ مع اسمِه شيءٌ في الأرضِ ولا في السماءِ ، وهو السميعُ العليمُ '' ثلاث مرات '' "
        sent = 0
        batch_size = 7
        for users_batch in get_users_in_batches(batch_size=batch_size):
            for u in users_batch:
                try:
                    send_message(int(u["user_id"]), text)
                    sent += 1
                    time.sleep(0.5)
                except Exception as e:
                    log_error("run_bismillah_task -> sending", e)
            time.sleep(1)
            gc.collect()
    except Exception as e:
        log_error("run_bismillah_task -> main", e)
        try:
            notify_admin('cron_bismillah_error',
                         location='run_bismillah_task', error=traceback.format_exc())
        except Exception:
            pass


def run_thursday_task():
    try:
        text = "🟣 مِن مغرب الخَميس إلى مغرب الجُمعة كُلّ ثانية فيها خزائن من الحسناتِ والرّحمات وتفريج الكُربات ، فليُكثر المرء من الصَّلاة على النَّبي ﷺ"
        sent = 0
        batch_size = 7
        for users_batch in get_users_in_batches(batch_size=batch_size):
            for u in users_batch:
                try:
                    send_message(int(u["user_id"]), text)
                    sent += 1
                    time.sleep(0.5)
                except Exception as e:
                    log_error("run_thursday_task -> sending", e)
            time.sleep(1)
            gc.collect()
    except Exception as e:
        log_error("run_thursday_task -> main", e)
        try:
            notify_admin('cron_thursday_error',
                         location='run_thursday_task', error=traceback.format_exc())
        except Exception:
            pass


def run_saturday_task():
    try:
        text1 = "🟣 بدايه اسبوع جديد وحاول تبعد عن الذنوب وخصوصا الكبائر عشان بتسبب مشاكل و تعب نفسي و نقص الرزق و عدم استجابه الدعاء و عدم التوفيق و غيره الكثير"
        text2 = "بعض من الكبائر : ترك الصلاة , العقوق , الكذب , الغيبة , النميمة , الربا ( من ضمنها القروض ) , شرب الخمر والمخدرات , شتم الاهل ( اهل اي حد ) , الزنا , أكل المال الحرام , الرياء ( التظاهر بالصلاح ) , شهادة الزور , قطع صله الرحم"
        sent = 0
        batch_size = 5
        for users_batch in get_users_in_batches(batch_size=batch_size):
            for u in users_batch:
                try:
                    send_message(int(u["user_id"]), text1)
                    send_message(int(u["user_id"]), text2)
                    sent += 1
                    time.sleep(0.6)
                except Exception as e:
                    log_error("run_saturday_task -> sending", e)
            time.sleep(1)
            gc.collect()
    except Exception as e:
        log_error("run_saturday_task -> main", e)
        try:
            notify_admin('cron_saturday_error',
                         location='run_saturday_task', error=traceback.format_exc())
        except Exception:
            pass


def run_heal_task():
    try:
        text1 = "🟡 إلهي أذهب البأس ربّ النّاس ، اشف وأنت الشّافي ، لا شفاء إلا شفاؤك ، شفاءً لا يغادر سقماً ، أذهب البأس ربّ النّاس ، بيدك الشفاء ، لا كاشف له إلّا أنت يارب العالمين"
        text2 = "﴿ ۞ وَأَيُّوبَ إِذۡ نَادَىٰ رَبَّهُۥٓ أَنِّي مَسَّنِيَ ٱلضُّرُّ وَأنتَ أَرۡحَمُ ٱلرَّٰحِمِينَ ﴾  [ الأنبياء : ٨٣ ]"
        sent = 0
        batch_size = 5
        for users_batch in get_users_in_batches(batch_size=batch_size):
            for u in users_batch:
                try:
                    if not bool_from_str(u.get("prayer_service")):
                        continue
                    send_message(int(u["user_id"]), text1)
                    time.sleep(0.5)
                    send_message(int(u["user_id"]), text2)
                    sent += 1
                    time.sleep(0.5)
                except Exception as e:
                    log_error("run_heal_task -> sending", e)
            time.sleep(1)
            gc.collect()
    except Exception as e:
        log_error("run_heal_task -> main", e)
        try:
            notify_admin('cron_heal_error', location='run_heal_task',
                         error=traceback.format_exc())
        except Exception:
            pass


def run_remind_task():
    try:
        sent = 0
        batch_size = 5
        for users_batch in get_users_in_batches(batch_size=batch_size):
            for u in users_batch:
                try:
                    if not bool_from_str(u.get("quran_service")):
                        continue
                    pending = u.get("pending_quran_pages", "")
                    # only remind if user has pending pages and did NOT respond yet (pending)
                    if pending and u.get("read_confirmation") in ("pending", "no"):
                        keyboard = {"inline_keyboard": [
                            [{"text": "- أنا قرأت الوِرد ✅",
                                "callback_data": "read_yes_mid"}],
                            [{"text": "- إعادة إرسال وِرد النهارده 🔁",
                                "callback_data": "reminder_resend"}],
                            [{"text": "- مش هقرأ الوِرد النهارده 🚫",
                                "callback_data": "reminder_no"}]
                        ]}
                        send_message(
                            int(u["user_id"]), "🔴 متنساش تقرأ الوِرد 🔔", reply_markup=keyboard)
                        sent += 1
                        time.sleep(0.6)
                except Exception as e:
                    log_error("run_remind_task -> sending", e)
            time.sleep(1)
            gc.collect()
    except Exception as e:
        log_error("run_remind_task -> main", e)
        try:
            notify_admin('cron_remind_error', location='run_remind_task',
                         error=traceback.format_exc())
        except Exception:
            pass

    except Exception as e:
        log_error("run_quran_task -> main", e)
        try:
            notify_admin('cron_quran_error', location='run_quran_task',
                         error=traceback.format_exc())
        except Exception:
            pass


@app.route("/cron/bismillah", methods=["GET", "POST"])
def cron_bismillah():

    try:
        update_heartbeat()
    except Exception:
        pass
    Thread(target=lambda: _thread_wrapper(
        run_bismillah_task), daemon=True).start()
    return "Started bismillah task ✅", 200


@app.route("/cron/thursday", methods=["GET", "POST"])
def cron_thursday():

    try:
        update_heartbeat()
    except Exception:
        pass
    Thread(target=lambda: _thread_wrapper(
        run_thursday_task), daemon=True).start()
    return "Started thursday task ✅", 200


@app.route("/cron/saturday", methods=["GET", "POST"])
def cron_saturday():

    try:
        update_heartbeat()
    except Exception:
        pass
    Thread(target=lambda: _thread_wrapper(
        run_saturday_task), daemon=True).start()
    return "Started saturday task ✅", 200


@app.route("/cron/heal", methods=["GET", "POST"])
def cron_heal():

    try:
        update_heartbeat()
    except Exception:
        pass
    Thread(target=lambda: _thread_wrapper(run_heal_task), daemon=True).start()
    return "Started heal task ✅", 200


@app.route("/cron/remind", methods=["GET", "POST"])
def cron_remind():

    try:
        update_heartbeat()
    except Exception:
        pass
    Thread(target=lambda: _thread_wrapper(
        run_remind_task), daemon=True).start()
    return "Started remind task ✅", 200


@app.route("/")
def home():
    return "I'm alive"

# ---------- Ping endpoint for uptime monitors ----------


@app.route("/ping")
def ping():
    return "OK", 200

# ---------- After-request cleanup ----------


@app.after_request
def after_request_cleanup(response):
    try:
        gc.collect()
    except Exception:
        pass
    return response


# ---------- Main ----------

# Signal handlers to notify admin on graceful shutdowns

def _on_terminate(signum, frame):
    try:
        logging.info(f"[signal_handler] received signal {signum}")
        notify_admin_instant(
            'signal_received', location='signal_handler', error=f'Signal {signum} received')
    except Exception as e:
        logging.error(f"_on_terminate failed: {e}")


signal.signal(signal.SIGTERM, _on_terminate)
signal.signal(signal.SIGHUP, _on_terminate)

atexit.register(lambda: logging.info('[atexit] process exiting'))

if __name__ == "__main__":
    try:
        # debug True for local testing only; in production set debug=False
        from flask.cli import load_dotenv
        load_dotenv()
        app.run(host="0.0.0.0", port=int(
            os.environ.get("PORT", 8080)), debug=True)
    except Exception:
        try:
            notify_admin('fatal_crash', location='__main__',
                         error=traceback.format_exc())
        except Exception:
            pass
        raise


# ---------- Global exception handler (captures uncaught exceptions) ----------


def global_exception_handler(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        _sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    tb = ''.join(traceback.format_exception(
        exc_type, exc_value, exc_traceback))
    filename = os.path.basename(
        exc_traceback.tb_frame.f_code.co_filename) if exc_traceback else ""
    lineno = exc_traceback.tb_lineno if exc_traceback else 0
    error_name = f"{exc_type.__name__}: {exc_value}"

    timestamp = now_egypt().strftime(
        "%I:%M %p – %d/%m/%Y").replace("AM", "ص").replace("PM", "م")

    message = (
        f"⚠️ تنبيه إدارة: (uncaught_exception)\\n\\n"
        f"📄 الملف: {filename}\\n"
        f"📍 السطر: {lineno}\\n"
        f"🕒 الوقت: {timestamp}\\n"
        f"💥 الخطأ: {error_name}\\n\\n"
        f"🧩 التفاصيل:\\n{tb}"
    )

    try:
        notify_admin("uncaught_exception", location=filename,
                     error=error_name, extra=tb)
    except Exception as _e:
        print(f"[global_error_handler] فشل إرسال التنبيه: {_e}")


_sys.excepthook = global_exception_handler

# ---------- end global handler ----------


# ---------- Appended clean background task definitions (overrides earlier duplicates) ----------
# These use existing helpers: _thread_wrapper, _send_with_retries_service, get_users_in_batches, bool_from_str, send_message


def _send_with_retries_service(service_name, users, send_func, max_retries=2, wait_seconds=20):
    """
    إرسال آمن للخدمة مع إعادة المحاولة في حال فشل الإرسال لبعض المستخدمين.
    البوت يحاول الإرسال، لو فشل لبعض المستخدمين ينتظر ثم يحاول مرة ثانية وثالثة.
    إذا فشل نهائياً يتم تنبيه الأدمن.
    """
    failed_users = []
    for attempt in range(1, max_retries + 2):
        failed_users.clear()
        try:
            for user in users:
                try:
                    success = send_func(user)
                    if not success:
                        failed_users.append(user)
                except Exception as e:
                    failed_users.append(user)
                    log_error(f"{service_name}_attempt_{attempt}", e)
            if not failed_users:
                if attempt > 1:
                    try:
                        notify_admin(
                            f"{service_name}_retry_success",
                            location=service_name,
                            error=f"✅ تمت إعادة الإرسال بنجاح بعد المحاولة {attempt-1}",
                        )
                    except Exception as e:
                        print(f"[notify_admin] فشل إرسال إشعار النجاح: {e}")
                return True
            else:
                if attempt <= max_retries:
                    print(
                        f"[INFO] فشل الإرسال لبعض المستخدمين ({len(failed_users)}). إعادة المحاولة بعد {wait_seconds}s...")
                    time.sleep(wait_seconds)
                else:
                    try:
                        notify_admin(
                            f"{service_name}_failed",
                            location=service_name,
                            error=f"فشل الإرسال بعد {max_retries+1} محاولات. عدد المستخدمين الفاشلين: {len(failed_users)}"
                        )
                    except Exception as e:
                        print(f"[notify_admin] فشل إرسال إشعار الفشل: {e}")
                    return False
        except Exception as e:
            log_error(f"{service_name}_fatal", e)
            if attempt <= max_retries:
                time.sleep(wait_seconds)
            else:
                try:
                    notify_admin(
                        f"{service_name}_fatal_error",
                        location=service_name,
                        error=f"استثناء أثناء تنفيذ الخدمة {service_name}: {e}"
                    )
                except Exception as e2:
                    print(f"[notify_admin] فشل إرسال إشعار الخطأ الفادح: {e2}")
                return False


def run_prayer_task():
    """Background prayer sender: 1 second pause between batches (per-request sleep inside _send_one is not needed)."""
    try:
        text = "🟢 اللهم صلِ وسلم و زِد و بارك علي سيدنا و مولانا محمد وعلي آله و صحبه اچمعين."
        batch_size = 7
        for users_batch in get_users_in_batches(batch_size=batch_size):
            eligible = [u for u in users_batch if bool_from_str(
                u.get("prayer_service"))]
            if not eligible:
                continue

            def _send_one(u):
                try:
                    resp = send_message(int(u["user_id"]), text)
                    return isinstance(resp, dict) and resp.get("ok", True)
                except Exception as e:
                    log_error("run_prayer_task -> send", e)
                    return False

            _send_with_retries_service(
                "prayer", eligible, _send_one, max_retries=2, wait_seconds=20)
            # pause between batches (keeps load low)
            time.sleep(1)
            gc.collect()
    except Exception:
        # allow _thread_wrapper to catch and notify admin
        raise


def run_dhikr_task():
    """Background dhikr sender: send intro then each line with 3s pause (as requested)."""
    try:
        intro = "🟡 ادعيه و ذكر اللًٰه :"
        lines = [
            "لا حول ولا قوة إلا باللًٰه العليّ العظيم",
            "سبحان اللّٰه عدد خلقه و رضا نفسه و زنه عرشه و مداد كلماته",
            "استغفر اللّٰه العظيم الذي لا اله إلا هو الحي القيوم واتوب إليه",
            "لا اله الا اللّٰه وحده لا شريك له ، له الملك وله الحمد وهو علي كل شئ قدير",
            "اللهم اغفر للمؤمنين و المؤمنات ، المسلمين و المسلمات الاحياء منهم والاموات",
            "استودعتُكَ اللّٰه الذي لا تَضيعُ ودائِعُهُ شيءٌ في الأرضِ ولا في السماءِ ، وهو السميعُ العليم",
            "اللهم أنت ربي لا إله إلا أنت ، خلقتني وأنا عبدك وأنا على عهدك و وعدك ما استطعت ، أعوذ بك من شر ما صنعت ، أبوء لك بنعمتك عليّْ ، وأبوء بذنبي فاغفر لي فإنه لا يغفر الذنوب إلا أنت",
            "اللهم إني أسألك من الخير كله : عاجله وآجله ، ما علمت منه وما لم أعلم ، وأعوذ بك من الشر كله عاجله وآجله ، ما علمت منه وما لم أعلم. اللهم إني أسألك من خير ما سألك عبدك ونبيك ، وأعوذ بك من شر ما استعاذ بك عبدك ونبيك. اللهم إني أسألك الجنة ، وما قرب إليها من قول أو عمل ، وأعوذ بك من النار ، وما قرب إليها من قول أو عمل ، وأسألك أن تجعل كل قضاء قضيته لي خيرا.",
            "آيه الكرسي : ﴿ ٱللَّهُ لَاۤ إِلَـٰهَ إِلَّا هُوَ ٱلۡحَیُّ ٱلۡقَيُّومُۚ لَا تَأۡخُذُهُۥ سِنَةࣱ وَلَا نَوۡمࣱۚ لَّهُۥ مَا فِی ٱلسَّمَـٰوَٰتِ وَمَا فِی ٱلۡأَرۡضِۗ مَن ذَا ٱلَّذِی يَشۡفَعُ عِندَهُۦۤ إِلَّا بِإِذۡنِهِۦۚ يَعۡلَمُ مَا بَيۡنَ أَيۡدِيهِمۡ وَمَا خَلۡفَهُمۡۖ وَلَا يُحِيطُونَ بِشَیۡءࣲ مِّنۡ عِلۡمِهِۦۤ إِلَّا بِمَا شَاۤءَۚ وَسِعَ كُرۡسِيُّهُ ٱلسَّمَـٰوَٰتِ وَٱلۡأَرۡضَۖ وَلَا يَـُٔودُهُۥ حِفۡظُهُمَاۚ وَهُوَ ٱلۡعلیُّ ٱلۡعظیمُ ﴾ [ البقره : ٢٥٥ ]"
        ]
        batch_size = 5
        for users_batch in get_users_in_batches(batch_size=batch_size):
            eligible = [u for u in users_batch if bool_from_str(
                u.get("dhikr_service"))]
            if not eligible:
                continue

            def _send_one(u):
                try:
                    uid = int(u["user_id"])
                    resp_intro = send_message(uid, intro)
                    if not (isinstance(resp_intro, dict) and resp_intro.get("ok", True)):
                        return False
                    # pause after intro
                    time.sleep(0.6)
                    for line in lines:
                        r = send_message(uid, line)
                        if not (isinstance(r, dict) and r.get("ok", True)):
                            return False
                        # pause between lines (3s as requested)
                        time.sleep(0.6)
                    return True
                except Exception as e:
                    log_error("run_dhikr_task -> send", e)
                    return False

            _send_with_retries_service(
                "dhikr", eligible, _send_one, max_retries=2, wait_seconds=20)
            time.sleep(1)
            gc.collect()
    except Exception:
        raise


def run_qiyam_task():
    """Background qiyam sender (keeps existing behavior)."""
    try:
        text1 = "🟤 تذكير قيام الليل :"
        text2 = "وإن لم تستطع فا قرائه اخر آيتان من سوره البقره كفتاه :"
        text3 = (
            "بسم الله الرحمن الرحيم ﴿ آمَنَ الرَّسُولُ بِمَا أُنْزِلَ إِلَيْهِ مِنْ رَبِّهِ "
            "وَالْمُؤْمِنُونَ ۚ كُلٌّ آمَنَ بِاللَّهِ وَمَلَائِكَتِهِ وَكُتُبِهِ وَرُسُلِهِ "
            "لَا نُفَرِّقُ بَيْنَ أَحَدٍ مِنْ رُسُلِهِ ۚ وَقَالُوا سَمِعْنَا وَأَطَعْنَا ۖ "
            "غُفْرَانَكَ رَبَّنَا وَإِلَيْكَ الْمَصِيرُ (٢٨٥) لَا يُكَلِّفُ اللَّهُ نَفْسًا "
            "إِلَّا وُسْعَهَا لَهَا مَا كَسَبَتْ وَعَلَيْهَا مَا اكْتَسَبَتْ رَبَّنَا لَا "
            "تُؤَاخِذْنَا إِنْ نَسِينَا أَوْ أَخْطَأْنَا رَبَّنَا وَلَا تَحْمِلْ عَلَيْنَا "
            "إِصْرًا كَمَا حَمَلْتَهُ عَلَى الَّذِينَ مِنْ قَبْلِنَا رَبَّنَا وَلَا تُحَمِّلْنَا "
            "مَا لَا طَاقَةَ لَنَا بِهِ وَاعْفُ عَنَّا وَاغْفِرْ لَنَا وَارْحَمْنَا أَنْتَ "
            "مَوْلَانَا فَانْصُرْنَا عَلَى الْقَوْمِ الْكَافِرِينَ (٢٨٦) ﴾"
        )
        batch_size = 5
        for users_batch in get_users_in_batches(batch_size=batch_size):
            eligible = [u for u in users_batch if bool_from_str(
                u.get("qiyam_service"))]
            if not eligible:
                continue

            def _send_one(u):
                try:
                    uid = int(u["user_id"])
                    r1 = send_message(uid, text1)
                    ok = isinstance(r1, dict) and r1.get("ok", True)
                    if not ok:
                        return False
                    time.sleep(0.5)
                    r2 = send_message(uid, text2)
                    ok = isinstance(r2, dict) and r2.get("ok", True)
                    if not ok:
                        return False
                    time.sleep(0.5)
                    r3 = send_message(uid, text3)
                    ok = isinstance(r3, dict) and r3.get("ok", True)
                    return ok
                except Exception as e:
                    log_error("run_qiyam_task -> send", e)
                    return False

            _send_with_retries_service(
                "qiyam", eligible, _send_one, max_retries=2, wait_seconds=20)
            time.sleep(1)
            gc.collect()
    except Exception:
        raise


# cron endpoints that spawn background threads using _thread_wrapper (override duplicates above)
@app.route("/cron/prayer", methods=["GET", "POST"])
def cron_prayer():
    thread = Thread(target=lambda: _thread_wrapper(run_prayer_task))
    thread.daemon = True
    thread.start()
    return "Started Prayer task ✅", 200


@app.route("/cron/dhikr", methods=["GET", "POST"])
def cron_dhikr():
    thread = Thread(target=lambda: _thread_wrapper(run_dhikr_task))
    thread.daemon = True
    thread.start()
    return "Started Dhikr task ✅", 200


@app.route("/cron/qiyam", methods=["GET", "POST"])
def cron_qiyam():
    thread = Thread(target=lambda: _thread_wrapper(run_qiyam_task))
    thread.daemon = True
    thread.start()
    return "Started Qiyam task ✅", 200

# ---------- End appended block ----------
