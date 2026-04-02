import os
import time
import traceback
import threading
from datetime import datetime
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpRequest
from httplib2 import Http

# Allow Google API client to consider BrokenPipeError retriable in lower-level HttpRequest handling
HttpRequest.RETRYABLE_ERRORS = (BrokenPipeError,)


def now_egypt():
    """ترجع الوقت الحالي بتوقيت القاهرة"""
    return datetime.now(pytz.timezone('Africa/Cairo'))


# Default admin ID (can be overridden by environment variable ADMIN_ID)
ADMIN_ID = int(os.environ.get("ADMIN_ID") or 853742750)
# keep a short in-memory rate limiter for alert keys
_notify_recent = {}
_NOTIFY_RATE_LIMIT_SECONDS = 180
# seconds before actually sending the alert (as in original code)
_ALERT_SEND_DELAY = 30
_MAX_MESSAGE_LENGTH = 4000

# centralized small helper used by notify_admin and internal callers


def _trim_message(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    if len(text) > _MAX_MESSAGE_LENGTH:
        return text[:_MAX_MESSAGE_LENGTH] + "\n\n⚠️ تم اختصار الرسالة لأنها طويلة جدًا"
    return text


def _send_alert_via_telegram(chat_id: int, message_text: str, token: str):
    """Send a message using python-telegram-bot's Bot. Separated to allow easy testing/mocking."""
    try:
        from telegram import Bot
        bot = Bot(token)
        bot.send_message(chat_id, message_text)
        # Only print essential success info to avoid spam in logs
        print(f"[notify_admin] تم إرسال تنبيه للإدمن ✅")
    except Exception as e:
        # log only error-level messages
        print(f"[notify_admin] ⚠️ فشل إرسال التنبيه: {e}")


def _delayed_send(chat_id: int, message_text: str, token: str, delay_seconds: int = _ALERT_SEND_DELAY):
    """Send message after a fixed delay in a background thread."""
    try:
        time.sleep(delay_seconds)
        _send_alert_via_telegram(chat_id, message_text, token)
    except Exception as e:
        print(f"[notify_admin] ⚠️ فشل في _delayed_send: {e}")


def notify_admin(alert_key: str, *, location: str = "", error: str = "", extra: str = ""):
    """
    Unified admin alert function.
    - Trims messages > 4000 chars
    - Rate-limits identical alerts for _NOTIFY_RATE_LIMIT_SECONDS seconds
    - Sends the alert in a background thread after _ALERT_SEND_DELAY seconds
    - Message format is unified and preserves the original fields (alert_key, location, time, error, extra)
    """
    try:
        now_ts = time.time()
        last = _notify_recent.get(alert_key)
        if last and (now_ts - last) < _NOTIFY_RATE_LIMIT_SECONDS:
            # Rate limit - skip sending duplicate alert
            return
        _notify_recent[alert_key] = now_ts

        timestamp = now_egypt().strftime(
            "%I:%M %p – %d/%m/%Y").replace("AM", " ص").replace("PM", " م")
        # unified message format (keeps original content markers)
        message = (
            f"⚠️ تنبيه إدارة: ({alert_key})\n\n"
            f"📍 المكان : {location}\n"
            f"🕒 الوقت : {timestamp}\n"
            f"💥 الخطأ : {error}"
        )

        if extra:
            message += f"\n\n📎 تفاصيل إضافية:\n{extra}"

        if len(message) > 4000:
            message = message[:4000] + \
                "\n\n⚠️ تم اختصار الرسالة لأنها طويلة جدًا"

        # Trim message to allowed length
        message = _trim_message(message)

        token = os.environ.get("BOT_TOKEN") or ""

        if not token:
            # If no token, we still record the attempt and log a concise message
            print("[notify_admin] ❌ BOT_TOKEN غير محدد، تخطى الإرسال.")
            return

        # Send after delay in a daemon thread
        t = threading.Thread(target=_delayed_send, args=(
            ADMIN_ID, message, token), daemon=True)
        t.start()
        print(
            f"[notify_admin] تم جدولة تنبيه ({alert_key}) بعد {_ALERT_SEND_DELAY} ثانية")

    except Exception as e:
        # Fatal failure to create notification; log minimal info
        print(f"[notify_admin] ❌ فشل إنشاء التنبيه: {e}")


class GoogleSheetsIntegration:
    def __init__(self, credentials_file):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self._credentials_file = credentials_file

        # store credentials / service handles
        self.credentials = None
        self.service = None
        self.sheets = None

        # prefer larger timeout (as requested)
        self._http_timeout = 180

        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_file, scopes=self.SCOPES)
            self.service = self.http = Http(timeout=self._http_timeout)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            # ensure underlying http timeout is set
            if hasattr(self.service, "_http") and getattr(self.service, "_http", None) is not None:
                try:
                    self.service._http.timeout = self._http_timeout
                except Exception:
                    pass
            self.sheets = self.service.spreadsheets()
            print("[GoogleSheetsIntegration] ✅ تم تحميل credentials و service بنجاح")
        except Exception as e:
            print(
                f"[GoogleSheetsIntegration.__init__] ⚠️ فشل تحميل credentials: {e}")
            notify_admin("credentials_load_error", location="GoogleSheetsIntegration.__init__",
                         error=str(e), extra=traceback.format_exc())

        self.SPREADSHEET_ID = '1XDAqhMa_N9iThRotfylOzkgKhkNoq1EdliM2qCz2Qgo'
        self.USER_DATA_SHEET = 'user_data'
        self.QURAN_TRACKING_SHEET = 'quran_tracking'

        self._cache = {"all_users": None, "last_fetch": 0}

        # run ensure in background to avoid blocking bot startup
        threading.Thread(
            target=self._ensure_sheets_exist_async, daemon=True).start()

    def _ensure_sheets_exist_async(self):
        try:
            time.sleep(2)
            self._ensure_sheets_exist()
            print("[_ensure_sheets_exist_async] ✅ تم التحقق من الـ sheets بنجاح")
        except Exception as e:
            print(
                f"[_ensure_sheets_exist_async] ⚠️ فشل التحقق من الـ sheets: {e}")
            notify_admin("ensure_sheets_async_error", location="_ensure_sheets_exist_async",
                         error=str(e), extra=traceback.format_exc())

    def _safe_execute(self, func, *args, retries=3, cooldown=15, delay=2, **kwargs):
        """
        Safe execution wrapper with:
         - retries (default 3)
         - exponential backoff for transient HTTP errors
         - rebuilt credentials attempt on auth errors
         - consolidated notification policy: no per-attempt notify; only notify once on final failure
         - notify safe_execute_recovered if there was an earlier failure and later success
        """
        import random
        had_failure = False
        real_failure = False
        last_exception = None

        for attempt in range(1, retries + 1):
            try:
                # lazy-init service if missing
                if (self.sheets is None or self.service is None) and self._credentials_file:
                    try:
                        self.credentials = service_account.Credentials.from_service_account_file(
                            self._credentials_file, scopes=self.SCOPES)
                        self.service = self.http = Http(
                            timeout=self._http_timeout)
                        self.service = build(
                            'sheets', 'v4', credentials=self.credentials)
                        if hasattr(self.service, "_http") and getattr(self.service, "_http", None) is not None:
                            try:
                                self.service._http.timeout = self._http_timeout
                            except Exception:
                                pass
                        self.sheets = self.service.spreadsheets()
                        print("[_safe_execute] ✅ تهيئة الخدمة نجحت")
                    except Exception as e_init:
                        had_failure = True
                        last_exception = e_init
                        # wait before retrying
                        wait_time = delay * (attempt) + random.uniform(0, 1)
                        time.sleep(wait_time)
                        continue

                result = func(*args, **kwargs)
                if hasattr(result, "execute"):
                    result = result.execute()
                # success after previous failures -> send recovered alert once
                return result

            except HttpError as e:
                had_failure = True
                real_failure = True
                last_exception = e
                status = getattr(getattr(e, "resp", None), "status", None)
                # transient errors -> backoff & retry
                if status in (429, 500, 502, 503):
                    wait_time = delay * \
                        (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_time)
                    continue

                # auth-related errors -> attempt to rebuild credentials and retry
                if status in (401, 403):
                    try:
                        if self._credentials_file:
                            self.credentials = service_account.Credentials.from_service_account_file(
                                self._credentials_file, scopes=self.SCOPES)
                            self.service = self.http = Http(
                                timeout=self._http_timeout)
                            self.service = build(
                                'sheets', 'v4', credentials=self.credentials)
                            if hasattr(self.service, "_http") and getattr(self.service, "_http", None) is not None:
                                try:
                                    self.service._http.timeout = self._http_timeout
                                except Exception:
                                    pass
                            self.sheets = self.service.spreadsheets()
                    except Exception as reinit_e:
                        last_exception = reinit_e
                    wait_time = delay * \
                        (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(wait_time)
                    continue

                # unexpected non-retriable HttpError -> record and break
                last_exception = e
                break

            except BrokenPipeError as e:
                # Handle BrokenPipe: attempt to rebuild service and retry
                had_failure = True
                real_failure = True
                last_exception = e
                try:
                    if self._credentials_file:
                        self.credentials = service_account.Credentials.from_service_account_file(
                            self._credentials_file, scopes=self.SCOPES)
                        self.service = self.http = Http(
                            timeout=self._http_timeout)
                        self.service = build(
                            'sheets', 'v4', credentials=self.credentials)
                        if hasattr(self.service, "_http") and getattr(self.service, "_http", None) is not None:
                            try:
                                self.service._http.timeout = self._http_timeout
                            except Exception:
                                pass
                        self.sheets = self.service.spreadsheets()
                        # give a short cooldown then retry
                        time.sleep(cooldown)
                        continue
                except Exception as rebuild_e:
                    last_exception = rebuild_e
                    # continue to next attempt after cooldown
                    time.sleep(cooldown)
                    continue

            except Exception as e:
                had_failure = True
                real_failure = True
                last_exception = e
                # unexpected non-http exception -> backoff and retry
                wait_time = delay * attempt + random.uniform(0, 1)
                time.sleep(wait_time)
                continue

        # All attempts exhausted -> send a single failure alert
        try:
            extra = ""
            if last_exception is not None:
                extra = traceback.format_exc()
            notify_admin("safe_execute_error", location="_safe_execute",
                         error=str(last_exception), extra=extra)
        except Exception:
            pass

        return None

    def _ensure_sheets_exist(self):
        try:
            sheet_metadata = self._safe_execute(
                self.sheets.get, spreadsheetId=self.SPREADSHEET_ID) if self.sheets else None
            if not sheet_metadata:
                print("[_ensure_sheets_exist] ⚠️ فشل جلب metadata للـ spreadsheet")
                return

            existing_sheets = [s['properties']['title']
                               for s in sheet_metadata.get('sheets', [])]

            if self.USER_DATA_SHEET not in existing_sheets:
                self._create_user_data_sheet()

            if self.QURAN_TRACKING_SHEET not in existing_sheets:
                self._create_quran_tracking_sheet()

        except Exception as e:
            print(f"[DEBUG] Error ensuring sheets exist: {e}")
            notify_admin("ensure_sheets_exist_error",
                         location="_ensure_sheets_exist", error=traceback.format_exc())

    def _create_user_data_sheet(self):
        header = [
            "user_id", "username", "first_name", "last_name", "join_date",
            "quran_service", "prayer_service", "dhikr_service", "qiyam_service",
            "last_quran_page", "pending_quran_pages", "read_confirmation", "last_update"
        ]
        body = {'requests': [
            {'addSheet': {'properties': {'title': self.USER_DATA_SHEET}}}]}
        self._safe_execute(self.sheets.batchUpdate,
                           spreadsheetId=self.SPREADSHEET_ID, body=body)
        self._safe_execute(self.sheets.values().update,
                           spreadsheetId=self.SPREADSHEET_ID,
                           range=f"{self.USER_DATA_SHEET}!A1:M1",
                           valueInputOption="RAW",
                           body={"values": [header]})
        time.sleep(0.5)
        print("[_create_user_data_sheet] Created sheet:", self.USER_DATA_SHEET)

    def _create_quran_tracking_sheet(self):
        header = ["user_id", "last_quran_page", "last_update"]
        body = {'requests': [
            {'addSheet': {'properties': {'title': self.QURAN_TRACKING_SHEET}}}]}
        self._safe_execute(self.sheets.batchUpdate,
                           spreadsheetId=self.SPREADSHEET_ID, body=body)
        self._safe_execute(self.sheets.values().update,
                           spreadsheetId=self.SPREADSHEET_ID,
                           range=f"{self.QURAN_TRACKING_SHEET}!A1:C1",
                           valueInputOption="RAW",
                           body={"values": [header]})
        time.sleep(0.5)
        print("[_create_quran_tracking_sheet] Created sheet:",
              self.QURAN_TRACKING_SHEET)

    def get_user_data(self, user_id):
        try:
            result = self._safe_execute(self.sheets.values().get,
                                        spreadsheetId=self.SPREADSHEET_ID,
                                        range=f"{self.USER_DATA_SHEET}!A2:M") if self.sheets else None
            time.sleep(0.5)
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
            notify_admin("get_user_data_error",
                         location="get_user_data", error=traceback.format_exc())
        return None

    def add_or_update_user(self, user_data):
        try:
            result = self._safe_execute(self.sheets.values().get,
                                        spreadsheetId=self.SPREADSHEET_ID,
                                        range=f"{self.USER_DATA_SHEET}!A2:M") if self.sheets else None
            time.sleep(0.5)
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
                    self._safe_execute(self.sheets.values().update,
                                       spreadsheetId=self.SPREADSHEET_ID,
                                       range=f"{self.USER_DATA_SHEET}!A{i}:M{i}",
                                       valueInputOption="RAW",
                                       body={"values": [values]})
                    time.sleep(0.5)
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
                self._safe_execute(self.sheets.values().append,
                                   spreadsheetId=self.SPREADSHEET_ID,
                                   range=f"{self.USER_DATA_SHEET}!A2:M",
                                   valueInputOption="RAW",
                                   body={"values": [values]})
                time.sleep(0.5)
                print(f"[add_or_update_user] Added new user")
            self._cache["all_users"] = None
        except Exception as e:
            print(f"[add_or_update_user] Error: {e}")
            notify_admin("add_or_update_user_error",
                         location="add_or_update_user", error=traceback.format_exc())

    def get_all_users(self, force_refresh=False):
        if (not force_refresh and self._cache["all_users"] and time.time() - self._cache["last_fetch"] < 30):
            return self._cache["all_users"]
        try:
            result = self._safe_execute(self.sheets.values().get,
                                        spreadsheetId=self.SPREADSHEET_ID,
                                        range=f"{self.USER_DATA_SHEET}!A2:M") if self.sheets else None
            time.sleep(0.5)
            if not result:
                return []
            rows = result.get('values', [])
            users = [{
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
            } for row in rows]
            self._cache["all_users"] = users
            self._cache["last_fetch"] = time.time()
            return users
        except Exception as e:
            print(f"[get_all_users] Error: {e}")
            notify_admin("get_all_users_error",
                         location="get_all_users", error=traceback.format_exc())
            return []

    def get_users_batch(self, start=0, end=10):
        try:
            all_users = self.get_all_users()
            time.sleep(0.5)
            return all_users[start:end]
        except Exception as e:
            print(f"[get_users_batch] Error: {e}")
            notify_admin("get_users_batch_error",
                         location="get_users_batch", error=traceback.format_exc())
            return []
