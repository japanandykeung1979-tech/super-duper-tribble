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

from flask import Flask, Response, flash, g, jsonify, redirect, render_template, request, session, url_for
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
    query += " ORDER BY created_at DESC"
    return query, params


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


def extract_sim_card_number(ocr_text: str) -> str:
    """從 OCR 文字中嘗試找出 18-20 位的連續數字，作為 SIM 卡號碼。"""
    matches = re.findall(r"\b\d{18,20}\b", ocr_text)
    return matches[0] if matches else ""


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
        required_fields = [
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
        form_data = {field: request.form.get(field, "").strip() for field in required_fields}
        form_data["remark"] = request.form.get("remark", "").strip()[:100]
        form_data["pps"] = 1 if request.form.get("pps") else 0
        form_data["ns"] = 1 if request.form.get("ns") else 0

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

        missing = [field for field, value in form_data.items() if field in required_fields and not value]
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

    if pytesseract is None:
        # 註解：若缺少 pytesseract 套件，回傳可讀錯誤，避免前端卡住。
        return jsonify({"error": "OCR 模組未安裝，請先執行 pip install pytesseract。"}), 503

    filename = secure_filename(uploaded_photo.filename)
    extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if extension not in ALLOWED_IMAGE_EXTENSIONS:
        return jsonify({"error": "檔案格式不支援，請上傳圖片。"}), 400

    try:
        from PIL import Image

        image_bytes = uploaded_photo.read()
        image = Image.open(BytesIO(image_bytes))
        # 註解：可依需要加入語言參數，例如 lang="chi_tra+eng"。
        ocr_text = pytesseract.image_to_string(image)
    except pytesseract.TesseractNotFoundError:
        return jsonify({"error": "找不到 Tesseract-OCR 執行檔，請先安裝系統套件。"}), 503
    except Exception:
        return jsonify({"error": "OCR 辨識失敗，請嘗試更清晰的圖片。"}), 500

    sim_card_number = extract_sim_card_number(ocr_text)
    return jsonify(
        {
            "sim_card_number": sim_card_number,
            "raw_text": ocr_text,
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
            }
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

            camera_photo = request.files.get("camera_photo")
            album_photo = request.files.get("album_photo")
            selected_photo = camera_photo if camera_photo and camera_photo.filename else album_photo
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

    return render_template(
        "dashboard.html",
        orders=orders,
    )


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
        ("id", "ID"),
        ("english_name", "English Name"),
        ("chinese_name", "Chinese Name"),
        ("hkid", "HKID"),
        ("port_in_number", "Port-in Number"),
        ("sim_number", "SIM Number"),
        ("a_card_number", "A Card Number"),
        ("b_card_number", "B Card Number"),
        ("pps", "PPS"),
        ("ns", "NS"),
        ("contract_end_date", "3HK Contract End Date"),
        ("transfer_out_date", "Transfer Out Date"),
        ("start_date", "Start Date"),
        ("replacement_date", "Replacement Date"),
        ("dno", "DNO"),
        ("card_type", "Card Type"),
        ("plan", "Plan"),
        ("cutover_date", "Cutover Date"),
        ("cutover_time", "Cutover Time"),
        ("real_name_registration", "Real Name Registration"),
        ("remark", "Remark"),
        ("photo_path", "Photo Path"),
        ("created_at", "Created At"),
    ]
    filename_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    if export_format == "csv":
        output = StringIO()
        writer = DictWriter(output, fieldnames=[label for _field, label in columns])
        writer.writeheader()
        for order in orders:
            writer.writerow({label: order[field] for field, label in columns})

        csv_text = output.getvalue()
        return Response(
            csv_text,
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
        required_fields = [
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
        form_data = {field: request.form.get(field, "").strip() for field in required_fields}
        form_data["remark"] = request.form.get("remark", "").strip()[:100]
        missing = [field for field in required_fields if not form_data[field]]

        if form_data["dno"] not in DNO_OPTIONS:
            missing.append("dno")
        if form_data["plan"] not in PLAN_OPTIONS:
            missing.append("plan")
        if form_data["cutover_time"] not in TIME_OPTIONS:
            missing.append("cutover_time")
        if form_data["real_name_registration"] not in {"是", "否"}:
            missing.append("real_name_registration")

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


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
