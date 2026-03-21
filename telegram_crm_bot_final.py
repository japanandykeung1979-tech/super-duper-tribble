from __future__ import annotations

import csv
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import telebot
from telebot import apihelper


# =========================================================
# 0. CONFIG
# =========================================================

@dataclass(frozen=True)
class Config:
    token: str
    owner_chat_id: str
    db_name: Path
    export_dir: Path
    log_file: Path
    proxy_http: str | None = None
    proxy_https: str | None = None


def load_config() -> Config:
    token = os.getenv("CRM_BOT_TOKEN", "").strip()
    owner_chat_id = os.getenv("CRM_OWNER_CHAT_ID", "").strip()
    if not token or not owner_chat_id:
        raise RuntimeError("Missing CRM_BOT_TOKEN / CRM_OWNER_CHAT_ID environment variables.")

    base_dir = Path(__file__).resolve().parent
    export_dir = base_dir / os.getenv("CRM_EXPORT_DIR", "exports")
    export_dir.mkdir(parents=True, exist_ok=True)

    return Config(
        token=token,
        owner_chat_id=owner_chat_id,
        db_name=base_dir / os.getenv("CRM_DB_NAME", "crm_a_plus.db"),
        export_dir=export_dir,
        log_file=base_dir / os.getenv("CRM_LOG_FILE", "crm.log"),
        proxy_http=os.getenv("CRM_PROXY_HTTP"),
        proxy_https=os.getenv("CRM_PROXY_HTTPS"),
    )


CONFIG = load_config()

if CONFIG.proxy_http and CONFIG.proxy_https:
    apihelper.proxy = {"http": CONFIG.proxy_http, "https": CONFIG.proxy_https}

bot = telebot.TeleBot(CONFIG.token, parse_mode="Markdown")

logger = logging.getLogger("crm_bot")
logger.setLevel(logging.INFO)
_handler = RotatingFileHandler(CONFIG.log_file, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logger.addHandler(_handler)


# =========================================================
# 1. DATABASE
# =========================================================

@contextmanager
def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(CONFIG.db_name, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_code TEXT UNIQUE,
                case_type TEXT NOT NULL,
                main_number TEXT,
                effect_date TEXT,
                raw_text TEXT NOT NULL,
                formatted_text TEXT,
                status TEXT NOT NULL DEFAULT '已入單',
                search_text TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS case_fields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                field_key TEXT NOT NULL,
                field_value TEXT,
                FOREIGN KEY(case_id) REFERENCES cases(id) ON DELETE CASCADE
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_cases_effect_date ON cases(effect_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cases_record_code ON cases(record_code)")


def generate_record_code(case_type: str) -> str:
    prefix = {"CMN": "CMN", "PLAN": "PLN", "ROUTER": "RTR"}.get(case_type, "UNK")
    date_part = datetime.now().strftime("%Y%m%d")

    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) AS cnt FROM cases WHERE record_code LIKE ?", (f"{prefix}-{date_part}-%",))
        seq = (c.fetchone()["cnt"] or 0) + 1

    return f"{prefix}-{date_part}-{seq:03d}"


def insert_case(
    case_type: str,
    main_number: str,
    effect_date: str,
    raw_text: str,
    formatted_text: str,
    fields: dict[str, str],
) -> str:
    record_code = generate_record_code(case_type)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    search_parts = [case_type, main_number, effect_date, raw_text] + list(fields.values())
    search_text = " | ".join(p.strip() for p in search_parts if p and p.strip()).lower()

    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO cases (record_code, case_type, main_number, effect_date, raw_text, formatted_text, status, search_text, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (record_code, case_type, main_number, effect_date, raw_text, formatted_text, "已入單", search_text, now, now),
        )
        case_id = c.lastrowid
        for k, v in fields.items():
            if v:
                c.execute(
                    "INSERT INTO case_fields (case_id, field_key, field_value) VALUES (?, ?, ?)",
                    (case_id, k, v),
                )

    return record_code


# =========================================================
# 2. PARSERS
# =========================================================

NUMBER_PATTERN = re.compile(r"(?:號碼|登記|取消\s*3\s*號碼)[：:]?\s*([0-9\s-]+)", re.I)
EFFECT_PATTERN = re.compile(r"(?:生效日期|開啟日期)[：:]?\s*([0-9\-/年 月 日]+)")
TRANSFER_PATTERN = re.compile(r"(?:轉走日期|轉出日期|a卡轉出日期)[：:]?\s*([0-9\-/年 月 日]+)", re.I)
REMARK_PATTERN = re.compile(r"備註[：:]?\s*([^\n]+)")


def standardize_date(value: str) -> str:
    if not value:
        return ""

    raw = re.sub(r"[-/]+", "-", value.strip().replace("年", "-").replace("月", "-").replace("日", ""))
    raw = re.sub(r"\s+", "", raw)

    formats = ["%Y-%m-%d", "%d-%m-%Y", "%Y-%m", "%d-%m-%y"]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt == "%Y-%m":
                return dt.strftime("%Y-%m")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return raw


def extract_info(text: str) -> dict[str, str]:
    lower = text.lower()
    case_type = "CMN" if any(k in lower for k in ["cmn", "取消3", "換入"]) else ("ROUTER" if "router" in lower else "PLAN")

    number_match = NUMBER_PATTERN.search(text)
    effect_match = EFFECT_PATTERN.search(text)
    transfer_match = TRANSFER_PATTERN.search(text)
    remark_match = REMARK_PATTERN.search(text)

    return {
        "case_type": case_type,
        "main_num": number_match.group(1).strip() if number_match else "",
        "effect_date": standardize_date(effect_match.group(1)) if effect_match else "",
        "transfer_out": standardize_date(transfer_match.group(1)) if transfer_match else "",
        "remarks": remark_match.group(1).strip() if remark_match else "",
    }


def build_display_text(
    case_type: str,
    main_num: str,
    effect_date: str,
    status: str,
    record_code: str,
    transfer_out: str = "",
    remarks: str = "",
) -> str:
    icon = {"CMN": "📄", "PLAN": "📃", "ROUTER": "🛜"}.get(case_type, "❓")
    lines = [
        f"{icon} 【{case_type}】",
        "➖➖➖➖➖➖➖➖➖➖",
        f"🆔 編號：`{record_code}`",
        f"📞 號碼：`{main_num or '-'}`",
        f"📅 生效：{effect_date or '-'}",
    ]
    if transfer_out:
        lines.append(f"🏃 轉走：{transfer_out}")
    if remarks:
        lines.append(f"💡 備註：{remarks}")
    lines.extend([f"📊 狀態：*{status}*", "➖➖➖➖➖➖➖➖➖➖"])
    return "\n".join(lines)


# =========================================================
# 3. COMMANDS
# =========================================================

def is_owner(message: Any) -> bool:
    return str(message.chat.id) == CONFIG.owner_chat_id


def reply_owner_only(message: Any) -> bool:
    if not is_owner(message):
        logger.warning("Unauthorized access attempt, chat_id=%s", message.chat.id)
        return False
    return True


@bot.message_handler(commands=["start"])
def cmd_start(message: Any) -> None:
    if not reply_owner_only(message):
        return
    bot.reply_to(
        message,
        "老闆你好！CRM Bot 最終版已啟動。🚀\n\n"
        "直接貼上單據即可入單。\n"
        "指令：\n"
        "`/查 關鍵字` - 搜尋\n"
        "`/匯出` - 匯出 CSV\n"
        "`/備份DB` - 下載資料庫\n"
        "`/狀態 編號 新狀態` - 更新狀態",
    )


@bot.message_handler(commands=["查"])
def cmd_search(message: Any) -> None:
    if not reply_owner_only(message):
        return

    try:
        kw = message.text.split(" ", 1)[1].strip().lower()
        if not kw:
            raise ValueError("empty keyword")

        keywords = kw.split()
        query = "SELECT * FROM cases WHERE " + " AND ".join(["search_text LIKE ?"] * len(keywords)) + " ORDER BY created_at DESC"

        with db_conn() as conn:
            rows = conn.execute(query, [f"%{k}%" for k in keywords]).fetchall()

        if not rows:
            bot.reply_to(message, "搵唔到資料。")
            return

        result = [f"🔍 搵到 {len(rows)} 張單："]
        for row in rows[:15]:
            result.append(
                f"• `{row['record_code']}` | {row['case_type']} | `{row['main_number'] or '-'}` | 📅{row['effect_date'] or '-'}"
            )
        bot.send_message(message.chat.id, "\n".join(result))
    except Exception:
        bot.reply_to(message, "格式：`/查 CMN 2026-07`")


@bot.message_handler(commands=["匯出"])
def cmd_export(message: Any) -> None:
    if not reply_owner_only(message):
        return

    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT c.record_code, c.case_type, c.main_number, c.effect_date, c.status, c.created_at,
                   (SELECT field_value FROM case_fields WHERE case_id = c.id AND field_key = '轉走日期') as transfer_date,
                   (SELECT field_value FROM case_fields WHERE case_id = c.id AND field_key = '備註') as remarks
            FROM cases c
            ORDER BY c.effect_date ASC, c.created_at DESC
            """
        ).fetchall()

    path = CONFIG.export_dir / f"CRM_{datetime.now().strftime('%m%d_%H%M')}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(["編號", "類型", "號碼", "生效日期", "轉走日期", "狀態", "備註", "入單時間"])
        for row in rows:
            writer.writerow(
                [
                    row["record_code"],
                    row["case_type"],
                    row["main_number"],
                    row["effect_date"],
                    row["transfer_date"],
                    row["status"],
                    row["remarks"],
                    row["created_at"],
                ]
            )

    with path.open("rb") as fp:
        bot.send_document(message.chat.id, fp, caption="✅ CSV 報表已生成")


@bot.message_handler(commands=["備份DB"])
def cmd_backup(message: Any) -> None:
    if not reply_owner_only(message):
        return

    with CONFIG.db_name.open("rb") as fp:
        bot.send_document(message.chat.id, fp, caption="🛡️ 數據庫備份")


@bot.message_handler(commands=["狀態"])
def cmd_status(message: Any) -> None:
    if not reply_owner_only(message):
        return

    try:
        _, record_code, new_status = message.text.split(" ", 2)
        with db_conn() as conn:
            cur = conn.execute("UPDATE cases SET status = ?, updated_at = ? WHERE record_code = ?", (new_status, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), record_code))
            if cur.rowcount == 0:
                bot.reply_to(message, f"⚠️ 找不到編號：`{record_code}`")
                return
        bot.reply_to(message, f"✅ `{record_code}` 狀態更新為：{new_status}")
    except Exception:
        bot.reply_to(message, "格式：`/狀態 編號 已寄SIM`")


# =========================================================
# 4. INGESTION
# =========================================================

@bot.message_handler(content_types=["text"])
def handle_text(message: Any) -> None:
    if not reply_owner_only(message) or message.text.startswith("/"):
        return

    text = message.text.strip()
    if text.startswith("查 "):
        message.text = f"/{text}"
        cmd_search(message)
        return

    try:
        info = extract_info(text)
        if not info["main_num"] or not info["effect_date"]:
            bot.reply_to(message, "⚠️ 入單失敗，請確保內容包含「號碼」及「生效日期」。")
            return

        draft_display = build_display_text(
            info["case_type"],
            info["main_num"],
            info["effect_date"],
            "已入單",
            "TEMP",
            info["transfer_out"],
            info["remarks"],
        )

        record_code = insert_case(
            info["case_type"],
            info["main_num"],
            info["effect_date"],
            text,
            draft_display,
            {"轉走日期": info["transfer_out"], "備註": info["remarks"]},
        )

        final_display = build_display_text(
            info["case_type"],
            info["main_num"],
            info["effect_date"],
            "已入單",
            record_code,
            info["transfer_out"],
            info["remarks"],
        )

        with db_conn() as conn:
            conn.execute(
                "UPDATE cases SET formatted_text = ?, updated_at = ? WHERE record_code = ?",
                (final_display, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), record_code),
            )

        bot.reply_to(
            message,
            "✅ 已成功入庫！\n"
            f"編號：`{record_code}`\n"
            f"號碼：`{info['main_num'] or '-'}`\n"
            f"生效：{info['effect_date'] or '-'}",
        )
    except Exception:
        logger.exception("Failed to ingest message")
        bot.reply_to(message, "⚠️ 入單失敗，請檢查格式後再試。")


if __name__ == "__main__":
    init_db()
    logger.info("CRM Bot final version starting")
    print("CRM Bot Final 版本啟動中...")
    bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
