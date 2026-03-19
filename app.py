from __future__ import annotations

import os
import re
import sqlite3
from csv import DictWriter
from datetime import datetime
from functools import wraps
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4
from xml.sax.saxutils import escape

from flask import Flask, Response, abort, flash, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

try:
    import pytesseract
except ImportError:  # 可選依賴：未安裝時仍可啟動主系統。
    pytesseract = None

BASE_DIR = Path(__file__).resolve().parent
DATABASE = BASE_DIR / "crm.db"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

DNO_OPTIONS = ["CMHK", "CSL", "SmarTone", "3HK", "HKBN", "其他"]
PLAN_OPTIONS = ["5G Basic", "5G Premium", "4.5G Value", "Family Plan", "Data SIM"]
TIME_OPTIONS = ["AM", "PM"]
APPOINTMENT_TELECOM_OPTIONS = ["CSL", "SmarTone", "中國移動", "3HK", "中國聯通", "電訊數碼"]
APPOINTMENT_OTHER_COMPANY_OPTIONS = ["SmarTone", "CSL", "China Mobile", "中國聯通", "電訊數碼", "其他"]
ROUTER_MODEL_OPTIONS = ["Wi-Fi 7", "Wi-Fi 6"]
DELIVERY_TIME_SLOT_OPTIONS = ["星期二", "星期四", "星期六"]
DELIVERY_STATUS_OPTIONS = ["自行送貨", "速遞"]
DATE_FIELDS = ["contract_end_date", "transfer_out_date", "start_date", "replacement_date"]
MNP_REQUIRED_FIELDS = [
    "english_name",
    "chinese_name",
    "hkid",
    "port_in_number",
    "sim_number",
    "dno",
    "card_type",
    "plan",
    "cutover_date",
    "cutover_time",
    "real_name_registration",
    "a_card_number",
    "b_card_number",
    "contract_end_date",
    "transfer_out_date",
    "start_date",
    "replacement_date",
]
EDIT_REQUIRED_FIELDS = [
    "english_name",
    "chinese_name",
    "hkid",
    "port_in_number",
    "sim_number",
    "dno",
    "card_type",
    "plan",
    "cutover_date",
    "cutover_time",
    "real_name_registration",
]
VALID_REAL_NAME_VALUES = {"是", "否"}
UPLOAD_FOLDER = BASE_DIR / "static" / "uploads"
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "bmp", "heic", "heif"}

DEMO_USER = os.environ.get("CRM_USERNAME", "admin")
DEMO_PASSWORD = os.environ.get("CRM_PASSWORD", "password123")


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


def init_db() -> None:
    db = sqlite3.connect(DATABASE)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            english_name TEXT NOT NULL,
            chinese_name TEXT NOT NULL,
            hkid TEXT NOT NULL,
            port_in_number TEXT NOT NULL,
            sim_number TEXT NOT NULL,
            dno TEXT NOT NULL,
            card_type TEXT NOT NULL,
            plan TEXT NOT NULL,
            cutover_date TEXT NOT NULL,
            cutover_time TEXT NOT NULL,
            real_name_registration TEXT NOT NULL,
            remark TEXT,
            photo_path TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number TEXT NOT NULL,
            telecom_category TEXT NOT NULL DEFAULT '其他公司',
            current_telecom TEXT NOT NULL,
            contract_end_date TEXT,
            current_plan_usage TEXT,
            remark TEXT,
            photo_path TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS router_deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            contact_person TEXT,
            delivery_address TEXT NOT NULL,
            delivery_date TEXT,
            old_broadband_contract_period TEXT,
            preferred_time_slot TEXT,
            router_model TEXT NOT NULL,
            requires_installation INTEGER NOT NULL DEFAULT 0,
            delivery_method TEXT NOT NULL DEFAULT '自行送貨',
            order_reference TEXT,
            status TEXT NOT NULL DEFAULT '自行送貨',
            photo_path TEXT,
            remark TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    existing_columns = {
        row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()
    }
    required_columns = {
        "a_card_number": "TEXT",
        "b_card_number": "TEXT",
        "pps": "INTEGER NOT NULL DEFAULT 0",
        "ns": "INTEGER NOT NULL DEFAULT 0",
        "contract_end_date": "TEXT",
        "transfer_out_date": "TEXT",
        "start_date": "TEXT",
        "replacement_date": "TEXT",
        "remark": "TEXT",
        "photo_path": "TEXT",
    }
    for column, column_type in required_columns.items():
        if column not in existing_columns:
            db.execute(f"ALTER TABLE orders ADD COLUMN {column} {column_type}")

    existing_appointment_columns = {
        row[1] for row in db.execute("PRAGMA table_info(appointments)").fetchall()
    }
    required_appointment_columns = {
        "telecom_category": "TEXT NOT NULL DEFAULT '其他公司'",
        "photo_path": "TEXT",
    }
    for column, column_type in required_appointment_columns.items():
        if column not in existing_appointment_columns:
            db.execute(f"ALTER TABLE appointments ADD COLUMN {column} {column_type}")

    existing_router_columns = {
        row[1] for row in db.execute("PRAGMA table_info(router_deliveries)").fetchall()
    }
    required_router_columns = {
        "contact_person": "TEXT",
        "delivery_date": "TEXT",
        "old_broadband_contract_period": "TEXT",
        "preferred_time_slot": "TEXT",
        "requires_installation": "INTEGER NOT NULL DEFAULT 0",
        "delivery_method": "TEXT NOT NULL DEFAULT '自行送貨'",
        "order_reference": "TEXT",
        "status": "TEXT NOT NULL DEFAULT '自行送貨'",
        "photo_path": "TEXT",
        "remark": "TEXT",
    }
    for column, column_type in required_router_columns.items():
        if column not in existing_router_columns:
            db.execute(f"ALTER TABLE router_deliveries ADD COLUMN {column} {column_type}")

    db.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_orders_contract_end_date ON orders(contract_end_date);
        CREATE INDEX IF NOT EXISTS idx_orders_transfer_out_date ON orders(transfer_out_date);
        CREATE INDEX IF NOT EXISTS idx_orders_start_date ON orders(start_date);
        CREATE INDEX IF NOT EXISTS idx_orders_replacement_date ON orders(replacement_date);
        CREATE INDEX IF NOT EXISTS idx_orders_a_card_number ON orders(a_card_number);
        CREATE INDEX IF NOT EXISTS idx_orders_b_card_number ON orders(b_card_number);
        CREATE INDEX IF NOT EXISTS idx_orders_pps ON orders(pps);
        CREATE INDEX IF NOT EXISTS idx_orders_ns ON orders(ns);
        CREATE INDEX IF NOT EXISTS idx_appointments_created_at ON appointments(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_appointments_phone_number ON appointments(phone_number);
        CREATE INDEX IF NOT EXISTS idx_appointments_contract_end_date ON appointments(contract_end_date);
        CREATE INDEX IF NOT EXISTS idx_router_deliveries_created_at ON router_deliveries(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_router_deliveries_phone_number ON router_deliveries(phone_number);
        CREATE INDEX IF NOT EXISTS idx_router_deliveries_delivery_date ON router_deliveries(delivery_date);
        """
    )

    db.commit()
    db.close()


@app.teardown_appcontext
def close_db(_exception: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def login_required(view):
    @wraps(view)
    def wrapped_view(**kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return view(**kwargs)

    return wrapped_view


@app.before_request
def csrf_protect() -> None:
    if request.method != "POST":
        return

    token = request.form.get("_csrf_token") or request.headers.get("X-CSRF-Token")
    expected = session.get("csrf_token")
    if not expected or token != expected:
        if request.path.startswith("/api/"):
            abort(400, description="CSRF 驗證失敗。")
        flash("表單驗證逾時，請重新提交。", "danger")
        return redirect(request.url)


@app.context_processor
def inject_csrf_token() -> dict[str, str]:
    token = session.get("csrf_token")
    if not token:
        token = uuid4().hex
        session["csrf_token"] = token
    return {"csrf_token": token}


def insert_order(data: dict[str, Any]) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO orders (
            english_name, chinese_name, hkid, port_in_number, sim_number,
            dno, card_type, plan, cutover_date, cutover_time,
            real_name_registration, remark, photo_path, created_at,
            a_card_number, b_card_number, pps, ns,
            contract_end_date, transfer_out_date, start_date, replacement_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["english_name"],
            data["chinese_name"],
            data["hkid"],
            data["port_in_number"],
            data["sim_number"],
            data["dno"],
            data["card_type"],
            data["plan"],
            data["cutover_date"],
            data["cutover_time"],
            data["real_name_registration"],
            data.get("remark", ""),
            data.get("photo_path", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            data.get("a_card_number", ""),
            data.get("b_card_number", ""),
            data.get("pps", 0),
            data.get("ns", 0),
            data.get("contract_end_date", ""),
            data.get("transfer_out_date", ""),
            data.get("start_date", ""),
            data.get("replacement_date", ""),
        ),
    )
    db.commit()


def build_order_filters(args) -> tuple[str, list[Any]]:
    query = "SELECT * FROM orders WHERE 1=1"
    params: list[Any] = []

    port_in_number = args.get("port_in_number", "").strip()
    a_card_number = args.get("a_card_number", "").strip()
    b_card_number = args.get("b_card_number", "").strip()
    contract_end_date = args.get("contract_end_date", "").strip()
    transfer_out_date = args.get("transfer_out_date", "").strip()
    start_date = args.get("start_date", "").strip()
    replacement_date = args.get("replacement_date", "").strip()
    keyword = args.get("keyword", "").strip()
    sort_by = args.get("sort_by", "").strip()
    sort_order = args.get("sort_order", "asc").strip().lower()
    pps = args.get("pps") == "1"
    ns = args.get("ns") == "1"

    if port_in_number:
        query += " AND port_in_number LIKE ?"
        params.append(f"%{port_in_number}%")
    if a_card_number:
        query += " AND a_card_number LIKE ?"
        params.append(f"%{a_card_number}%")
    if b_card_number:
        query += " AND b_card_number LIKE ?"
        params.append(f"%{b_card_number}%")
    if pps:
        query += " AND pps = 1"
    if ns:
        query += " AND ns = 1"
    if contract_end_date:
        query += " AND contract_end_date = ?"
        params.append(contract_end_date)
    if transfer_out_date:
        query += " AND transfer_out_date = ?"
        params.append(transfer_out_date)
    if start_date:
        query += " AND start_date >= ?"
        params.append(start_date)
    if replacement_date:
        query += " AND replacement_date <= ?"
        params.append(replacement_date)

    for field in DATE_FIELDS:
        month = args.get(f"{field}_month", "").strip()
        year = args.get(f"{field}_year", "").strip()
        if month and year:
            query += f" AND strftime('%m', {field}) = ? AND strftime('%Y', {field}) = ?"
            params.extend([month.zfill(2), year])

    if keyword:
        query += " AND (a_card_number LIKE ? OR b_card_number LIKE ? OR remark LIKE ?)"
        keyword_like = f"%{keyword}%"
        params.extend([keyword_like, keyword_like, keyword_like])

    if sort_by in DATE_FIELDS and sort_order in {"asc", "desc"}:
        query += f" ORDER BY {sort_by} {sort_order}, created_at DESC"
    else:
        query += " ORDER BY created_at DESC"
    return query, params


def build_appointment_filters(args) -> tuple[str, list[Any]]:
    query = "SELECT * FROM appointments WHERE 1=1"
    params: list[Any] = []

    phone_number = args.get("phone_number", "").strip()
    telecom_category = args.get("telecom_category", "").strip()
    current_telecom = args.get("current_telecom", "").strip()
    contract_end_date = args.get("contract_end_date", "").strip()
    current_plan_usage = args.get("current_plan_usage", "").strip()
    remark = args.get("remark", "").strip()
    sort_by = args.get("sort_by", "").strip()
    sort_order = args.get("sort_order", "").strip().lower()

    if phone_number:
        query += " AND phone_number LIKE ?"
        params.append(f"%{phone_number}%")
    if telecom_category:
        query += " AND telecom_category = ?"
        params.append(telecom_category)
    if current_telecom:
        query += " AND current_telecom = ?"
        params.append(current_telecom)
    if contract_end_date:
        query += " AND contract_end_date = ?"
        params.append(contract_end_date)
    if current_plan_usage:
        query += " AND current_plan_usage LIKE ?"
        params.append(f"%{current_plan_usage}%")
    if remark:
        query += " AND remark LIKE ?"
        params.append(f"%{remark}%")

    if sort_by == "contract_end_date" and sort_order in {"asc", "desc"}:
        query += (
            " ORDER BY CASE WHEN contract_end_date IS NULL OR contract_end_date = '' THEN 1 ELSE 0 END, "
            f"contract_end_date {sort_order}, created_at DESC"
        )
    else:
        query += " ORDER BY created_at DESC"
    return query, params


def insert_appointment(data: dict[str, Any]) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO appointments (
            phone_number, telecom_category, current_telecom, contract_end_date, current_plan_usage, remark, photo_path, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["phone_number"],
            data.get("telecom_category", "其他公司"),
            data["current_telecom"],
            data.get("contract_end_date", ""),
            data.get("current_plan_usage", ""),
            data.get("remark", "")[:100],
            data.get("photo_path", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    db.commit()


def fetch_appointment(appointment_id: int) -> sqlite3.Row | None:
    db = get_db()
    return db.execute("SELECT * FROM appointments WHERE id = ?", (appointment_id,)).fetchone()


def build_router_delivery_filters(args) -> tuple[str, list[Any]]:
    query = "SELECT * FROM router_deliveries WHERE 1=1"
    params: list[Any] = []

    phone_number = args.get("phone_number", "").strip()
    customer_name = args.get("customer_name", "").strip()
    delivery_date = args.get("delivery_date", "").strip()
    status = args.get("status", "").strip()
    delivery_method = args.get("delivery_method", "").strip()
    router_model = args.get("router_model", "").strip()
    requires_installation = args.get("requires_installation", "").strip()
    old_broadband_contract_period = args.get("old_broadband_contract_period", "").strip()
    remark = args.get("remark", "").strip()
    sort_delivery_date = args.get("sort_delivery_date", "").strip()
    sort_contract_period = args.get("sort_contract_period", "").strip()

    if phone_number:
        query += " AND phone_number LIKE ?"
        params.append(f"%{phone_number}%")
    if customer_name:
        query += " AND customer_name LIKE ?"
        params.append(f"%{customer_name}%")
    if delivery_date:
        query += " AND delivery_date = ?"
        params.append(delivery_date)
    if status in DELIVERY_STATUS_OPTIONS:
        query += " AND status = ?"
        params.append(status)
    if delivery_method in DELIVERY_STATUS_OPTIONS:
        query += " AND delivery_method = ?"
        params.append(delivery_method)
    if router_model in ROUTER_MODEL_OPTIONS:
        query += " AND router_model = ?"
        params.append(router_model)
    if requires_installation in {"0", "1"}:
        query += " AND requires_installation = ?"
        params.append(int(requires_installation))
    if old_broadband_contract_period:
        query += " AND old_broadband_contract_period LIKE ?"
        params.append(f"%{old_broadband_contract_period}%")
    if remark:
        query += " AND remark LIKE ?"
        params.append(f"%{remark}%")

    if sort_delivery_date == "1":
        query += " ORDER BY CASE WHEN delivery_date IS NULL OR delivery_date = '' THEN 1 ELSE 0 END, delivery_date ASC, created_at DESC"
    elif sort_contract_period == "1":
        query += (
            " ORDER BY CASE WHEN old_broadband_contract_period IS NULL OR old_broadband_contract_period = '' THEN 1 ELSE 0 END, "
            "old_broadband_contract_period ASC, created_at DESC"
        )
    else:
        query += " ORDER BY created_at DESC"
    return query, params


def insert_router_delivery(data: dict[str, Any]) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO router_deliveries (
            phone_number, customer_name, contact_person, delivery_address, delivery_date,
            old_broadband_contract_period, preferred_time_slot, router_model,
            requires_installation, delivery_method, order_reference, status, photo_path, remark, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["phone_number"],
            data["customer_name"],
            data.get("contact_person", ""),
            data["delivery_address"],
            data.get("delivery_date", ""),
            data.get("old_broadband_contract_period", ""),
            data.get("preferred_time_slot", ""),
            data["router_model"],
            data.get("requires_installation", 0),
            data.get("delivery_method", "自行送貨"),
            data.get("order_reference", ""),
            data.get("status", "自行送貨"),
            data.get("photo_path", ""),
            data.get("remark", "")[:150],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    db.commit()


def fetch_router_delivery(delivery_id: int) -> sqlite3.Row | None:
    db = get_db()
    return db.execute("SELECT * FROM router_deliveries WHERE id = ?", (delivery_id,)).fetchone()


def collect_form_data(form, required_fields: list[str]) -> dict[str, Any]:
    form_data = {field: form.get(field, "").strip() for field in required_fields}
    form_data["remark"] = form.get("remark", "").strip()[:100]
    form_data["pps"] = 1 if form.get("pps") else 0
    form_data["ns"] = 1 if form.get("ns") else 0
    return form_data


def validate_form_data(form_data: dict[str, Any], required_fields: list[str]) -> list[str]:
    missing = [field for field in required_fields if not form_data.get(field)]

    if "dno" in required_fields and form_data.get("dno") not in DNO_OPTIONS:
        missing.append("dno")
    if "plan" in required_fields and form_data.get("plan") not in PLAN_OPTIONS:
        missing.append("plan")
    if "cutover_time" in required_fields and form_data.get("cutover_time") not in TIME_OPTIONS:
        missing.append("cutover_time")
    if "real_name_registration" in required_fields and form_data.get("real_name_registration") not in VALID_REAL_NAME_VALUES:
        missing.append("real_name_registration")

    return missing


def parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def build_dashboard_metrics(orders: list[sqlite3.Row]) -> dict[str, Any]:
    today = datetime.now().date()
    upcoming_contracts: list[dict[str, Any]] = []

    for order in orders:
        contract_date = parse_date(order["contract_end_date"])
        if contract_date is None:
            continue

        days_left = (contract_date.date() - today).days
        if 0 <= days_left <= 30:
            upcoming_contracts.append(
                {
                    "id": order["id"],
                    "a_card_number": order["a_card_number"],
                    "b_card_number": order["b_card_number"],
                    "contract_end_date": order["contract_end_date"],
                    "days_left": days_left,
                }
            )

    upcoming_contracts.sort(key=lambda item: item["days_left"])

    return {
        "total": len(orders),
        "pps_total": sum(1 for order in orders if order["pps"]),
        "ns_total": sum(1 for order in orders if order["ns"]),
        "photo_total": sum(1 for order in orders if order["photo_path"]),
        "upcoming_contracts": upcoming_contracts[:5],
    }


def save_uploaded_photo(uploaded_file) -> str:
    if not uploaded_file or not uploaded_file.filename:
        return ""

    filename = secure_filename(uploaded_file.filename)
    if not filename:
        return ""

    extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if extension not in ALLOWED_IMAGE_EXTENSIONS:
        return ""

    UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
    unique_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex}.{extension}"
    destination = UPLOAD_FOLDER / unique_name
    uploaded_file.save(destination)
    return str(Path("uploads") / unique_name)


def save_photo_bytes(image_bytes: bytes, extension: str) -> str:
    if extension not in ALLOWED_IMAGE_EXTENSIONS:
        return ""
    UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
    unique_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex}.{extension}"
    destination = UPLOAD_FOLDER / unique_name
    destination.write_bytes(image_bytes)
    return str(Path("uploads") / unique_name)


def extract_sim_card_number(ocr_text: str) -> str:
    """從 OCR 文字中嘗試找出 18-20 位的連續數字，作為 SIM 卡號碼。"""
    matches = re.findall(r"\b\d{18,20}\b", ocr_text)
    return matches[0] if matches else ""


def parse_hkid_ocr_fields(ocr_text: str) -> dict[str, str]:
    """嘗試從香港身份證 OCR 文字中擷取 HKID / 英文名 / 中文名。"""
    compact_text = re.sub(r"\s+", "", ocr_text.upper())

    hkid_patterns = [
        r"([A-Z]{1,2}\d{6}\([0-9A]\))",
        r"([A-Z]{1,2}\d{6}[0-9A])",
    ]
    hkid = ""
    for pattern in hkid_patterns:
        match = re.search(pattern, compact_text)
        if match:
            token = match.group(1)
            if "(" not in token and len(token) >= 8:
                token = f"{token[:-1]}({token[-1]})"
            hkid = token
            break

    lines = [line.strip() for line in ocr_text.splitlines() if line.strip()]

    english_name = ""
    for index, line in enumerate(lines):
        upper_line = line.upper()
        normalized = re.sub(r"[^A-Z,\-\s]", " ", upper_line)
        normalized = re.sub(r"\s+", " ", normalized).strip(" ,-")
        if not normalized:
            continue

        if "NAME" in upper_line or "SURNAME" in upper_line or "GIVEN" in upper_line:
            name_portion = re.sub(r".*?(NAME|SURNAME|GIVEN\s+NAMES?)[:：]?", "", normalized).strip(" ,-")
            if name_portion:
                normalized = name_portion
            elif index + 1 < len(lines):
                follow_line = re.sub(r"[^A-Z,\-\s]", " ", lines[index + 1].upper())
                normalized = re.sub(r"\s+", " ", follow_line).strip(" ,-")

        words = [word for word in re.split(r"[\s,]+", normalized) if word]
        if len(words) < 2:
            continue
        if all(re.fullmatch(r"[A-Z\-]+", word) for word in words):
            if any(flag in normalized for flag in ["HONG", "KONG", "IDENTITY", "CARD", "PERMANENT"]):
                continue
            english_name = " ".join(words)
            break

    chinese_name = ""
    chinese_candidates = re.findall(r"[\u4e00-\u9fff]{2,5}", ocr_text)
    blocked_tokens = {"香港", "身份證", "永久", "居民", "出生", "日期", "簽發", "持有人"}
    filtered_candidates = [token for token in chinese_candidates if token not in blocked_tokens]
    if filtered_candidates:
        chinese_name = max(filtered_candidates, key=len)

    return {
        "hkid": hkid,
        "english_name": english_name,
        "chinese_name": chinese_name,
    }


def run_ocr_on_image_bytes(image_bytes: bytes) -> str:
    if pytesseract is None:
        raise RuntimeError("OCR 模組未安裝，請先執行 pip install pytesseract。")

    try:
        from PIL import Image, ImageEnhance, ImageOps

        image = Image.open(BytesIO(image_bytes))
        image = ImageOps.exif_transpose(image).convert("RGB")

        # 手機拍攝身份證常見情況：反光、陰影、字體偏細。先灰階+提高對比有助 OCR。
        grayscale = ImageOps.grayscale(image)
        boosted = ImageEnhance.Contrast(grayscale).enhance(1.8)

        # 先用中英混合語言，若環境未安裝 chi_tra 再回退到英文。
        try:
            text = pytesseract.image_to_string(boosted, lang="eng+chi_tra", config="--psm 6")
        except pytesseract.TesseractError:
            text = pytesseract.image_to_string(boosted, lang="eng", config="--psm 6")

        # 若輸出太少，改用另一種 page segmentation 再試一次。
        if len(re.sub(r"\s+", "", text)) < 12:
            fallback_text = pytesseract.image_to_string(boosted, lang="eng", config="--psm 11")
            if len(re.sub(r"\s+", "", fallback_text)) > len(re.sub(r"\s+", "", text)):
                text = fallback_text

        return text
    except pytesseract.TesseractNotFoundError as error:
        raise RuntimeError("找不到 Tesseract-OCR 執行檔，請先安裝系統套件。") from error
    except Exception as error:
        raise RuntimeError("OCR 辨識失敗，請嘗試更清晰的圖片。") from error


def fetch_order(order_id: int) -> sqlite3.Row | None:
    db = get_db()
    return db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()


def build_dashboard_order_data(form) -> dict[str, Any]:
    a_card_number = form.get("a_card_number", "").strip()
    b_card_number = form.get("b_card_number", "").strip()

    placeholder_name = a_card_number or b_card_number or "Dashboard"

    return {
        "english_name": f"Dashboard-{placeholder_name}",
        "chinese_name": "Dashboard新增",
        "hkid": "N/A",
        "port_in_number": "N/A",
        "sim_number": "N/A",
        "dno": "其他",
        "card_type": "N/A",
        "plan": PLAN_OPTIONS[0],
        "cutover_date": datetime.now().strftime("%Y-%m-%d"),
        "cutover_time": TIME_OPTIONS[0],
        "real_name_registration": "否",
        "a_card_number": a_card_number,
        "b_card_number": b_card_number,
        "pps": 1 if form.get("pps") else 0,
        "ns": 1 if form.get("ns") else 0,
        "contract_end_date": form.get("contract_end_date", "").strip(),
        "transfer_out_date": form.get("transfer_out_date", "").strip(),
        "start_date": form.get("start_date", "").strip(),
        "replacement_date": form.get("replacement_date", "").strip(),
        "remark": form.get("remark", "").strip()[:100],
    }


@app.route("/")
def index():
    if session.get("user"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == DEMO_USER and password == DEMO_PASSWORD:
            session["user"] = username
            flash("登入成功。", "success")
            return redirect(url_for("dashboard"))
        flash("帳號或密碼錯誤。", "danger")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    session.clear()
    flash("已登出系統。", "info")
    return redirect(url_for("login"))


@app.route("/mnp", methods=["GET", "POST"])
@login_required
def mnp_form():
    if request.method == "POST":
        form_data = collect_form_data(request.form, MNP_REQUIRED_FIELDS)

        uploaded_photo = request.files.get("photo")
        form_data["photo_path"] = save_uploaded_photo(uploaded_photo)
        if uploaded_photo and uploaded_photo.filename and not form_data["photo_path"]:
            flash("相片格式不支援，請上傳圖片檔案。", "danger")
            return render_template(
                "mnp_form.html",
                dno_options=DNO_OPTIONS,
                plan_options=PLAN_OPTIONS,
                time_options=TIME_OPTIONS,
                form_data=form_data,
            )

        missing = validate_form_data(form_data, MNP_REQUIRED_FIELDS)
        if missing:
            flash("請填寫所有必填欄位。", "danger")
            return render_template(
                "mnp_form.html",
                dno_options=DNO_OPTIONS,
                plan_options=PLAN_OPTIONS,
                time_options=TIME_OPTIONS,
                form_data=form_data,
            )

        insert_order(form_data)
        flash("轉台申請已成功儲存。", "success")
        return redirect(url_for("mnp_form"))

    return render_template(
        "mnp_form.html",
        dno_options=DNO_OPTIONS,
        plan_options=PLAN_OPTIONS,
        time_options=TIME_OPTIONS,
        form_data={},
    )


@app.post("/api/ocr")
@login_required
def ocr_scan():
    uploaded_photo = request.files.get("photo")
    if not uploaded_photo or not uploaded_photo.filename:
        return jsonify({"error": "請先上傳圖片檔案。"}), 400

    filename = secure_filename(uploaded_photo.filename)
    extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if extension not in ALLOWED_IMAGE_EXTENSIONS:
        return jsonify({"error": "檔案格式不支援，請上傳圖片。"}), 400

    try:
        image_bytes = uploaded_photo.read()
        ocr_text = run_ocr_on_image_bytes(image_bytes)
    except RuntimeError as error:
        if "未安裝" in str(error) or "找不到 Tesseract" in str(error):
            return jsonify({"error": str(error)}), 503
        return jsonify({"error": "OCR 辨識失敗，請嘗試更清晰的圖片。"}), 500

    sim_card_number = extract_sim_card_number(ocr_text)
    hkid_fields = parse_hkid_ocr_fields(ocr_text)
    return jsonify(
        {
            "sim_card_number": sim_card_number,
            "hkid": hkid_fields["hkid"],
            "english_name": hkid_fields["english_name"],
            "chinese_name": hkid_fields["chinese_name"],
            "raw_text": ocr_text,
        }
    )


@app.post("/api/dashboard/ocr-add")
@login_required
def dashboard_ocr_add():
    target_field = request.form.get("target_field", "").strip()
    if target_field not in {"a_card_number", "b_card_number"}:
        return jsonify({"error": "目標欄位不正確。"}), 400

    uploaded_photo = request.files.get("photo")
    if not uploaded_photo or not uploaded_photo.filename:
        return jsonify({"error": "請先拍照或選擇相片。"}), 400

    filename = secure_filename(uploaded_photo.filename)
    extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if extension not in ALLOWED_IMAGE_EXTENSIONS:
        return jsonify({"error": "檔案格式不支援，請上傳圖片。"}), 400

    image_bytes = uploaded_photo.read()
    photo_path = save_photo_bytes(image_bytes, extension)
    if not photo_path:
        return jsonify({"error": "相片儲存失敗。"}), 500

    try:
        ocr_text = run_ocr_on_image_bytes(image_bytes)
    except RuntimeError as error:
        if "未安裝" in str(error) or "找不到 Tesseract" in str(error):
            return jsonify({"error": str(error)}), 503
        return jsonify({"error": "OCR 辨識失敗，請嘗試更清晰的圖片。"}), 500

    recognized_number = extract_sim_card_number(ocr_text)
    form_data = build_dashboard_order_data(request.form)
    form_data[target_field] = recognized_number or form_data.get(target_field, "")
    form_data["photo_path"] = photo_path

    if not form_data["a_card_number"] and not form_data["b_card_number"]:
        return jsonify({"error": "未辨識到號碼，請重試或手動輸入。"}), 400

    insert_order(form_data)
    return jsonify(
        {
            "ok": True,
            "target_field": target_field,
            "recognized_number": recognized_number,
            "message": "OCR 完成，已自動新增紀錄。",
        }
    )


@app.route("/dashboard", methods=["GET", "POST"])
@login_required
def dashboard():
    if request.method == "POST":
        action = request.form.get("action", "search")

        if action == "search":
            search_params = {
                "a_card_number": request.form.get("a_card_number", "").strip(),
                "contract_end_date": request.form.get("contract_end_date", "").strip(),
                "transfer_out_date": request.form.get("transfer_out_date", "").strip(),
                "b_card_number": request.form.get("b_card_number", "").strip(),
                "start_date": request.form.get("start_date", "").strip(),
                "replacement_date": request.form.get("replacement_date", "").strip(),
                "keyword": request.form.get("keyword", "").strip(),
                "sort_by": request.form.get("sort_by", "").strip(),
                "sort_order": request.form.get("sort_order", "").strip(),
            }
            for field in DATE_FIELDS:
                search_params[f"{field}_month"] = request.form.get(f"{field}_month", "").strip()
                search_params[f"{field}_year"] = request.form.get(f"{field}_year", "").strip()
            if request.form.get("pps"):
                search_params["pps"] = "1"
            if request.form.get("ns"):
                search_params["ns"] = "1"

            cleaned_params = {key: value for key, value in search_params.items() if value}
            return redirect(url_for("dashboard", **cleaned_params))

        if action == "add":
            form_data = build_dashboard_order_data(request.form)

            if not form_data["a_card_number"] and not form_data["b_card_number"]:
                flash("請至少輸入 A咭號碼 或 B咭號碼，才可新增紀錄。", "danger")
                return redirect(url_for("dashboard", **request.args.to_dict(flat=False)))

            form_photo = request.files.get("photo")
            camera_photo = request.files.get("camera_photo")
            album_photo = request.files.get("album_photo")
            selected_photo = (
                form_photo
                if form_photo and form_photo.filename
                else camera_photo
                if camera_photo and camera_photo.filename
                else album_photo
            )
            form_data["photo_path"] = save_uploaded_photo(selected_photo)

            if selected_photo and selected_photo.filename and not form_data["photo_path"]:
                flash("相片格式不支援，請上傳圖片檔案。", "danger")
                return redirect(url_for("dashboard", **request.args.to_dict(flat=False)))

            insert_order(form_data)
            flash("已從 Dashboard 新增紀錄。", "success")
            return redirect(url_for("dashboard", **request.args.to_dict(flat=False)))

        flash("不支援的操作。", "danger")
        return redirect(url_for("dashboard", **request.args.to_dict(flat=False)))

    query, params = build_order_filters(request.args)

    db = get_db()
    orders = db.execute(query, params).fetchall()
    metrics = build_dashboard_metrics(orders)

    return render_template(
        "dashboard.html",
        orders=orders,
        metrics=metrics,
    )


@app.route("/appointments", methods=["GET", "POST"])
@login_required
def appointments():
    if request.method == "POST":
        action = request.form.get("action", "search")

        if action == "search":
            search_params = {
                "phone_number": request.form.get("phone_number", "").strip(),
                "telecom_category": request.form.get("telecom_category", "").strip(),
                "current_telecom": request.form.get("current_telecom", "").strip(),
                "contract_end_date": request.form.get("contract_end_date", "").strip(),
                "current_plan_usage": request.form.get("current_plan_usage", "").strip(),
                "remark": request.form.get("remark", "").strip(),
                "sort_by": request.form.get("sort_by", "").strip(),
                "sort_order": request.form.get("sort_order", "").strip().lower(),
            }
            if search_params["sort_by"] != "contract_end_date":
                search_params["sort_by"] = ""
                search_params["sort_order"] = ""
            elif search_params["sort_order"] not in {"asc", "desc"}:
                search_params["sort_order"] = "asc"
            cleaned_params = {key: value for key, value in search_params.items() if value}
            return redirect(url_for("appointments", **cleaned_params))

        if action == "add":
            has_3hk = request.form.get("telecom_3hk") == "1"
            has_other = request.form.get("telecom_other") == "1"
            other_company_name = request.form.get("other_company_name", "").strip()

            if has_3hk and has_other:
                flash("請只可選擇一個電訊商分類。", "danger")
                return redirect(url_for("appointments", **request.args.to_dict(flat=False)))
            if not has_3hk and not has_other:
                flash("請先剔選電訊商分類（3HK 或 其他公司）。", "danger")
                return redirect(url_for("appointments", **request.args.to_dict(flat=False)))

            if has_3hk:
                telecom_category = "3HK"
                telecom_provider = "3HK"
            else:
                telecom_category = "其他公司"
                telecom_provider = other_company_name
                if telecom_provider not in APPOINTMENT_OTHER_COMPANY_OPTIONS:
                    flash("請選擇有效的其他公司名稱。", "danger")
                    return redirect(url_for("appointments", **request.args.to_dict(flat=False)))

            form_data = {
                "phone_number": request.form.get("phone_number", "").strip(),
                "telecom_category": telecom_category,
                "current_telecom": telecom_provider,
                "contract_end_date": request.form.get("contract_end_date", "").strip(),
                "current_plan_usage": request.form.get("current_plan_usage", "").strip(),
                "remark": request.form.get("remark", "").strip()[:100],
            }
            appointment_photo = request.files.get("appointment_photo")
            form_data["photo_path"] = save_uploaded_photo(appointment_photo)

            if appointment_photo and appointment_photo.filename and not form_data["photo_path"]:
                flash("相片格式不支援，請上傳圖片檔案。", "danger")
                return redirect(url_for("appointments", **request.args.to_dict(flat=False)))

            if not form_data["phone_number"]:
                flash("請填寫電話號碼。", "danger")
                return redirect(url_for("appointments", **request.args.to_dict(flat=False)))

            insert_appointment(form_data)
            flash("已新增未登記及預約簽單紀錄。", "success")
            return redirect(url_for("appointments", **request.args.to_dict(flat=False)))

        flash("不支援的操作。", "danger")
        return redirect(url_for("appointments", **request.args.to_dict(flat=False)))

    query, params = build_appointment_filters(request.args)
    db = get_db()
    appointments_data = db.execute(query, params).fetchall()

    return render_template(
        "appointments.html",
        appointments=appointments_data,
        telecom_options=APPOINTMENT_TELECOM_OPTIONS,
        other_company_options=APPOINTMENT_OTHER_COMPANY_OPTIONS,
    )


@app.get("/appointments/export")
@login_required
def export_appointments():
    export_format = request.args.get("format", "csv").lower()
    if export_format not in {"csv", "excel"}:
        flash("匯出格式不支援。", "danger")
        return redirect(url_for("appointments", **request.args))

    query, params = build_appointment_filters(request.args)
    db = get_db()
    appointments_data = db.execute(query, params).fetchall()

    columns = [
        ("phone_number", "電話號碼"),
        ("current_telecom", "公司名稱"),
        ("contract_end_date", "合約完結日"),
        ("current_plan_usage", "月費及用量"),
        ("remark", "備註"),
        ("photo_path", "相片路徑"),
        ("created_at", "建立時間"),
    ]
    filename_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    if export_format == "csv":
        output = StringIO()
        writer = DictWriter(output, fieldnames=[label for _field, label in columns])
        writer.writeheader()
        for appointment in appointments_data:
            writer.writerow({label: appointment[field] for field, label in columns})

        csv_text = output.getvalue()
        csv_bytes = csv_text.encode("utf-8-sig")
        return Response(
            csv_bytes,
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename=appointments_{filename_time}.csv",
            },
        )

    rows = [
        "<?xml version=\"1.0\"?>",
        '<?mso-application progid="Excel.Sheet"?>',
        '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"',
        ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">',
        '<Worksheet ss:Name="Appointments"><Table>',
        "<Row>",
    ]
    for _field, label in columns:
        rows.append(f"<Cell><Data ss:Type=\"String\">{escape(label)}</Data></Cell>")
    rows.append("</Row>")
    for appointment in appointments_data:
        rows.append("<Row>")
        for field, _label in columns:
            value = "" if appointment[field] is None else str(appointment[field])
            rows.append(f"<Cell><Data ss:Type=\"String\">{escape(value)}</Data></Cell>")
        rows.append("</Row>")
    rows.extend(["</Table></Worksheet>", "</Workbook>"])

    return Response(
        "\n".join(rows),
        mimetype="application/vnd.ms-excel",
        headers={
            "Content-Disposition": f"attachment; filename=appointments_{filename_time}.xls",
        },
    )


@app.route("/router-delivery", methods=["GET", "POST"])
@login_required
def router_delivery():
    if request.method == "POST":
        action = request.form.get("action", "search")

        if action == "search":
            search_params = {
                "phone_number": request.form.get("phone_number", "").strip(),
                "customer_name": request.form.get("customer_name", "").strip(),
                "delivery_date": request.form.get("delivery_date", "").strip(),
                "status": request.form.get("status", "").strip(),
                "delivery_method": request.form.get("delivery_method", "").strip(),
                "router_model": request.form.get("router_model", "").strip(),
                "requires_installation": "1" if request.form.get("requires_installation") else "",
                "old_broadband_contract_period": request.form.get("old_broadband_contract_period", "").strip(),
                "remark": request.form.get("remark", "").strip(),
                "sort_delivery_date": "1" if request.form.get("sort_delivery_date") else "",
                "sort_contract_period": "1" if request.form.get("sort_contract_period") else "",
            }
            cleaned_params = {key: value for key, value in search_params.items() if value}
            return redirect(url_for("router_delivery", **cleaned_params))

        if action == "add":
            form_data = {
                "phone_number": request.form.get("phone_number", "").strip(),
                "customer_name": request.form.get("customer_name", "").strip(),
                "contact_person": request.form.get("contact_person", "").strip(),
                "delivery_address": request.form.get("delivery_address", "").strip(),
                "delivery_date": request.form.get("delivery_date", "").strip(),
                "old_broadband_contract_period": request.form.get("old_broadband_contract_period", "").strip(),
                "preferred_time_slot": request.form.get("preferred_time_slot", "").strip(),
                "router_model": request.form.get("router_model", "").strip(),
                "requires_installation": 1 if request.form.get("requires_installation") else 0,
                "delivery_method": request.form.get("delivery_method", "").strip() or "自行送貨",
                "order_reference": request.form.get("order_reference", "").strip(),
                "status": request.form.get("status", "").strip() or "自行送貨",
                "photo_path": "",
                "remark": request.form.get("remark", "").strip()[:150],
            }
            photo_file = request.files.get("delivery_photo")
            form_data["photo_path"] = save_uploaded_photo(photo_file)
            if photo_file and photo_file.filename and not form_data["photo_path"]:
                flash("相片格式不支援，請上傳圖片檔案。", "danger")
                return redirect(url_for("router_delivery", **request.args.to_dict(flat=False)))

            if (
                not form_data["phone_number"]
                or not form_data["customer_name"]
                or not form_data["delivery_address"]
                or not form_data["delivery_date"]
                or not form_data["old_broadband_contract_period"]
                or form_data["router_model"] not in ROUTER_MODEL_OPTIONS
                or form_data["status"] not in DELIVERY_STATUS_OPTIONS
                or form_data["delivery_method"] not in DELIVERY_STATUS_OPTIONS
            ):
                flash("請填妥必要欄位（電話、客戶、地址、送貨日期、舊有寬頻合約期）。", "danger")
                return redirect(url_for("router_delivery", **request.args.to_dict(flat=False)))

            if form_data["preferred_time_slot"] and form_data["preferred_time_slot"] not in DELIVERY_TIME_SLOT_OPTIONS:
                flash("請選擇有效的送貨時段。", "danger")
                return redirect(url_for("router_delivery", **request.args.to_dict(flat=False)))

            insert_router_delivery(form_data)
            flash("已新增 5G Router 送貨記錄。", "success")
            return redirect(url_for("router_delivery", **request.args.to_dict(flat=False)))

        flash("不支援的操作。", "danger")
        return redirect(url_for("router_delivery", **request.args.to_dict(flat=False)))

    query, params = build_router_delivery_filters(request.args)
    db = get_db()
    deliveries = db.execute(query, params).fetchall()
    return render_template(
        "router_delivery.html",
        deliveries=deliveries,
        router_model_options=ROUTER_MODEL_OPTIONS,
        delivery_time_slots=DELIVERY_TIME_SLOT_OPTIONS,
        delivery_status_options=DELIVERY_STATUS_OPTIONS,
    )


@app.route("/router-delivery/<int:delivery_id>/edit", methods=["GET", "POST"])
@login_required
def edit_router_delivery(delivery_id: int):
    delivery = fetch_router_delivery(delivery_id)
    if delivery is None:
        flash("找不到該筆送貨記錄。", "danger")
        return redirect(url_for("router_delivery"))

    if request.method == "POST":
        form_data = {
            "phone_number": request.form.get("phone_number", "").strip(),
            "customer_name": request.form.get("customer_name", "").strip(),
            "contact_person": request.form.get("contact_person", "").strip(),
            "delivery_address": request.form.get("delivery_address", "").strip(),
            "delivery_date": request.form.get("delivery_date", "").strip(),
            "old_broadband_contract_period": request.form.get("old_broadband_contract_period", "").strip(),
            "preferred_time_slot": request.form.get("preferred_time_slot", "").strip(),
            "router_model": request.form.get("router_model", "").strip(),
            "requires_installation": 1 if request.form.get("requires_installation") else 0,
            "delivery_method": request.form.get("delivery_method", "").strip(),
            "order_reference": request.form.get("order_reference", "").strip(),
            "status": request.form.get("status", "").strip(),
            "photo_path": delivery["photo_path"] or "",
            "remark": request.form.get("remark", "").strip()[:150],
        }
        photo_file = request.files.get("delivery_photo")
        new_photo_path = save_uploaded_photo(photo_file)
        if photo_file and photo_file.filename and not new_photo_path:
            flash("相片格式不支援，請上傳圖片檔案。", "danger")
            return render_template(
                "edit_router_delivery.html",
                delivery=form_data,
                delivery_id=delivery_id,
                router_model_options=ROUTER_MODEL_OPTIONS,
                delivery_time_slots=DELIVERY_TIME_SLOT_OPTIONS,
                delivery_status_options=DELIVERY_STATUS_OPTIONS,
            )
        if new_photo_path:
            form_data["photo_path"] = new_photo_path
        if (
            not form_data["phone_number"]
            or not form_data["customer_name"]
            or not form_data["delivery_address"]
            or not form_data["delivery_date"]
            or not form_data["old_broadband_contract_period"]
            or form_data["router_model"] not in ROUTER_MODEL_OPTIONS
            or form_data["status"] not in DELIVERY_STATUS_OPTIONS
            or form_data["delivery_method"] not in DELIVERY_STATUS_OPTIONS
        ):
            flash("資料不完整或包含無效欄位，請檢查後重試。", "danger")
            return render_template(
                "edit_router_delivery.html",
                delivery=form_data,
                delivery_id=delivery_id,
                router_model_options=ROUTER_MODEL_OPTIONS,
                delivery_time_slots=DELIVERY_TIME_SLOT_OPTIONS,
                delivery_status_options=DELIVERY_STATUS_OPTIONS,
            )

        if form_data["preferred_time_slot"] and form_data["preferred_time_slot"] not in DELIVERY_TIME_SLOT_OPTIONS:
            flash("請選擇有效的送貨時段。", "danger")
            return render_template(
                "edit_router_delivery.html",
                delivery=form_data,
                delivery_id=delivery_id,
                router_model_options=ROUTER_MODEL_OPTIONS,
                delivery_time_slots=DELIVERY_TIME_SLOT_OPTIONS,
                delivery_status_options=DELIVERY_STATUS_OPTIONS,
            )

        db = get_db()
        db.execute(
            """
            UPDATE router_deliveries
            SET phone_number = ?, customer_name = ?, delivery_address = ?, delivery_date = ?,
                old_broadband_contract_period = ?, preferred_time_slot = ?, router_model = ?,
                requires_installation = ?, contact_person = ?, delivery_method = ?, order_reference = ?, status = ?, photo_path = ?, remark = ?
            WHERE id = ?
            """,
            (
                form_data["phone_number"],
                form_data["customer_name"],
                form_data["delivery_address"],
                form_data["delivery_date"],
                form_data["old_broadband_contract_period"],
                form_data["preferred_time_slot"],
                form_data["router_model"],
                form_data["requires_installation"],
                form_data["contact_person"],
                form_data["delivery_method"],
                form_data["order_reference"],
                form_data["status"],
                form_data["photo_path"],
                form_data["remark"],
                delivery_id,
            ),
        )
        db.commit()
        flash("送貨記錄已更新。", "success")
        return redirect(url_for("router_delivery"))

    return render_template(
        "edit_router_delivery.html",
        delivery=delivery,
        delivery_id=delivery_id,
        router_model_options=ROUTER_MODEL_OPTIONS,
        delivery_time_slots=DELIVERY_TIME_SLOT_OPTIONS,
        delivery_status_options=DELIVERY_STATUS_OPTIONS,
    )


@app.post("/router-delivery/<int:delivery_id>/delete")
@login_required
def delete_router_delivery(delivery_id: int):
    db = get_db()
    cursor = db.execute("DELETE FROM router_deliveries WHERE id = ?", (delivery_id,))
    db.commit()
    if cursor.rowcount:
        flash("送貨記錄已刪除。", "success")
    else:
        flash("找不到要刪除的送貨記錄。", "danger")
    return redirect(url_for("router_delivery", **request.args.to_dict(flat=False)))


@app.get("/dashboard/export")
@login_required
def export_orders():
    export_format = request.args.get("format", "csv").lower()
    if export_format not in {"csv", "excel"}:
        flash("匯出格式不支援。", "danger")
        return redirect(url_for("dashboard", **request.args))

    query, params = build_order_filters(request.args)
    db = get_db()
    orders = db.execute(query, params).fetchall()

    columns = [
        ("a_card_number", "A咭號碼"),
        ("contract_end_date", "3HK合約完結日"),
        ("transfer_out_date", "轉走日期"),
        ("b_card_number", "B咭號碼"),
        ("pps", "PPS (是/否)"),
        ("ns", "ns (是/否)"),
        ("start_date", "開始日期"),
        ("replacement_date", "取代日期"),
        ("remark", "備註"),
        ("created_at", "建立時間"),
    ]
    filename_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    if export_format == "csv":
        output = StringIO()
        writer = DictWriter(output, fieldnames=[label for _field, label in columns])
        writer.writeheader()
        for order in orders:
            writer.writerow(
                {
                    label: (
                        "是"
                        if field in {"pps", "ns"} and order[field]
                        else "否"
                        if field in {"pps", "ns"}
                        else order[field]
                    )
                    for field, label in columns
                }
            )

        csv_text = output.getvalue()
        csv_bytes = csv_text.encode("utf-8-sig")
        return Response(
            csv_bytes,
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename=orders_{filename_time}.csv",
            },
        )

    rows = [
        "<?xml version=\"1.0\"?>",
        '<?mso-application progid="Excel.Sheet"?>',
        '<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"',
        ' xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">',
        '<Worksheet ss:Name="Orders"><Table>',
        "<Row>",
    ]
    for _field, label in columns:
        rows.append(f"<Cell><Data ss:Type=\"String\">{escape(label)}</Data></Cell>")
    rows.append("</Row>")
    for order in orders:
        rows.append("<Row>")
        for field, _label in columns:
            if field in {"pps", "ns"}:
                value = "是" if order[field] else "否"
            else:
                value = "" if order[field] is None else str(order[field])
            rows.append(f"<Cell><Data ss:Type=\"String\">{escape(value)}</Data></Cell>")
        rows.append("</Row>")
    rows.extend(["</Table></Worksheet>", "</Workbook>"])

    return Response(
        "\n".join(rows),
        mimetype="application/vnd.ms-excel",
        headers={
            "Content-Disposition": f"attachment; filename=orders_{filename_time}.xls",
        },
    )


@app.route("/orders/<int:order_id>/edit", methods=["GET", "POST"])
@login_required
def edit_order(order_id: int):
    order = fetch_order(order_id)
    if order is None:
        flash("找不到該筆訂單。", "danger")
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        form_data = collect_form_data(request.form, EDIT_REQUIRED_FIELDS)
        missing = validate_form_data(form_data, EDIT_REQUIRED_FIELDS)

        if missing:
            flash("資料不完整或包含無效欄位，請檢查後重試。", "danger")
            return render_template(
                "edit_order.html",
                order=form_data,
                order_id=order_id,
                dno_options=DNO_OPTIONS,
                plan_options=PLAN_OPTIONS,
                time_options=TIME_OPTIONS,
            )

        db = get_db()
        db.execute(
            """
            UPDATE orders
            SET english_name = ?, chinese_name = ?, hkid = ?, port_in_number = ?, sim_number = ?,
                dno = ?, card_type = ?, plan = ?, cutover_date = ?, cutover_time = ?,
                real_name_registration = ?, remark = ?
            WHERE id = ?
            """,
            (
                form_data["english_name"],
                form_data["chinese_name"],
                form_data["hkid"],
                form_data["port_in_number"],
                form_data["sim_number"],
                form_data["dno"],
                form_data["card_type"],
                form_data["plan"],
                form_data["cutover_date"],
                form_data["cutover_time"],
                form_data["real_name_registration"],
                form_data["remark"],
                order_id,
            ),
        )
        db.commit()
        flash("訂單資料已更新。", "success")
        return redirect(url_for("dashboard"))

    return render_template(
        "edit_order.html",
        order=order,
        order_id=order_id,
        dno_options=DNO_OPTIONS,
        plan_options=PLAN_OPTIONS,
        time_options=TIME_OPTIONS,
    )


@app.post("/orders/<int:order_id>/delete")
@login_required
def delete_order(order_id: int):
    db = get_db()
    cursor = db.execute("DELETE FROM orders WHERE id = ?", (order_id,))
    db.commit()
    if cursor.rowcount:
        flash("訂單已刪除。", "success")
    else:
        flash("找不到要刪除的訂單。", "danger")
    return redirect(url_for("dashboard", **request.args.to_dict(flat=False)))


@app.route("/appointments/<int:appointment_id>/edit", methods=["GET", "POST"])
@login_required
def edit_appointment(appointment_id: int):
    appointment = fetch_appointment(appointment_id)
    if appointment is None:
        flash("找不到該筆紀錄。", "danger")
        return redirect(url_for("appointments"))

    if request.method == "POST":
        telecom_category = request.form.get("telecom_category", "").strip()
        selected_other_company = request.form.get("other_company_name", "").strip()
        if telecom_category == "3HK":
            provider = "3HK"
        elif telecom_category == "其他公司":
            provider = selected_other_company
        else:
            provider = ""

        form_data = {
            "phone_number": request.form.get("phone_number", "").strip(),
            "telecom_category": telecom_category,
            "current_telecom": provider,
            "contract_end_date": request.form.get("contract_end_date", "").strip(),
            "current_plan_usage": request.form.get("current_plan_usage", "").strip(),
            "remark": request.form.get("remark", "").strip()[:100],
        }

        appointment_photo = request.files.get("appointment_photo")
        new_photo_path = save_uploaded_photo(appointment_photo)
        if appointment_photo and appointment_photo.filename and not new_photo_path:
            flash("相片格式不支援，請上傳圖片檔案。", "danger")
            return render_template(
                "edit_appointment.html",
                appointment=form_data,
                appointment_id=appointment_id,
                other_company_options=APPOINTMENT_OTHER_COMPANY_OPTIONS,
            )
        form_data["photo_path"] = new_photo_path or appointment["photo_path"] or ""

        if telecom_category == "其他公司" and form_data["current_telecom"] not in APPOINTMENT_OTHER_COMPANY_OPTIONS:
            provider_valid = False
        elif telecom_category == "3HK" and form_data["current_telecom"] == "3HK":
            provider_valid = True
        else:
            provider_valid = False

        if not form_data["phone_number"] or not provider_valid:
            flash("資料不完整或包含無效欄位，請檢查後重試。", "danger")
            return render_template(
                "edit_appointment.html",
                appointment=form_data,
                appointment_id=appointment_id,
                other_company_options=APPOINTMENT_OTHER_COMPANY_OPTIONS,
            )

        db = get_db()
        db.execute(
            """
            UPDATE appointments
            SET phone_number = ?, telecom_category = ?, current_telecom = ?, contract_end_date = ?, current_plan_usage = ?, remark = ?, photo_path = ?
            WHERE id = ?
            """,
            (
                form_data["phone_number"],
                form_data["telecom_category"],
                form_data["current_telecom"],
                form_data["contract_end_date"],
                form_data["current_plan_usage"],
                form_data["remark"],
                form_data["photo_path"],
                appointment_id,
            ),
        )
        db.commit()
        flash("紀錄資料已更新。", "success")
        return redirect(url_for("appointments"))

    return render_template(
        "edit_appointment.html",
        appointment=appointment,
        appointment_id=appointment_id,
        other_company_options=APPOINTMENT_OTHER_COMPANY_OPTIONS,
    )


@app.post("/appointments/<int:appointment_id>/delete")
@login_required
def delete_appointment(appointment_id: int):
    db = get_db()
    cursor = db.execute("DELETE FROM appointments WHERE id = ?", (appointment_id,))
    db.commit()
    if cursor.rowcount:
        flash("紀錄已刪除。", "success")
    else:
        flash("找不到要刪除的紀錄。", "danger")
    return redirect(url_for("appointments", **request.args.to_dict(flat=False)))


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
