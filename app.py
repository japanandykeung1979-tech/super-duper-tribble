from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

from flask import Flask, flash, g, redirect, render_template, request, session, url_for

BASE_DIR = Path(__file__).resolve().parent
DATABASE = BASE_DIR / "crm.db"

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

DNO_OPTIONS = ["CMHK", "CSL", "SmarTone", "3HK", "HKBN", "其他"]
PLAN_OPTIONS = ["5G Basic", "5G Premium", "4.5G Value", "Family Plan", "Data SIM"]
STATUS_OPTIONS = ["New", "Processing", "Invalid", "Reject", "Success"]
TIME_OPTIONS = ["AM", "PM"]

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
            status TEXT NOT NULL DEFAULT 'New',
            created_at TEXT NOT NULL
        )
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


def insert_order(data: dict[str, Any]) -> None:
    db = get_db()
    db.execute(
        """
        INSERT INTO orders (
            english_name, chinese_name, hkid, port_in_number, sim_number,
            dno, card_type, plan, cutover_date, cutover_time,
            real_name_registration, remark, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            "New",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    db.commit()


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
        ]
        form_data = {field: request.form.get(field, "").strip() for field in required_fields}
        form_data["remark"] = request.form.get("remark", "").strip()

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


@app.route("/dashboard")
@login_required
def dashboard():
    port_in_number = request.args.get("port_in_number", "").strip()
    sim_number = request.args.get("sim_number", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    statuses = request.args.getlist("status")

    query = "SELECT * FROM orders WHERE 1=1"
    params: list[Any] = []

    if port_in_number:
        query += " AND port_in_number LIKE ?"
        params.append(f"%{port_in_number}%")
    if sim_number:
        query += " AND sim_number LIKE ?"
        params.append(f"%{sim_number}%")
    if start_date:
        query += " AND cutover_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND cutover_date <= ?"
        params.append(end_date)
    if statuses:
        placeholders = ",".join("?" * len(statuses))
        query += f" AND status IN ({placeholders})"
        params.extend(statuses)

    query += " ORDER BY created_at DESC"

    db = get_db()
    orders = db.execute(query, params).fetchall()

    return render_template(
        "dashboard.html",
        orders=orders,
        status_options=STATUS_OPTIONS,
        selected_statuses=statuses,
    )


@app.post("/dashboard/update_status/<int:order_id>")
@login_required
def update_status(order_id: int):
    new_status = request.form.get("status", "").strip()
    if new_status not in STATUS_OPTIONS:
        flash("無效的狀態。", "danger")
        return redirect(url_for("dashboard"))

    db = get_db()
    db.execute("UPDATE orders SET status = ? WHERE id = ?", (new_status, order_id))
    db.commit()
    flash("訂單狀態已更新。", "success")
    return redirect(url_for("dashboard", **request.args))


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
