#!/usr/bin/env python3
"""客戶記事簿：按日期記錄客戶及電話號碼生效日，並輸出簡單報表。"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

DB_PATH = Path("customer_diary.db")


@dataclass
class PhoneEntry:
    phone_number: str
    effective_date: date


@dataclass
class CustomerEntry:
    register_date: date
    customer_name: str
    contact_number: str
    phones: list[PhoneEntry]


class DiaryDB:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS customer_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    register_date TEXT NOT NULL,
                    customer_name TEXT NOT NULL,
                    contact_number TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS phone_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_entry_id INTEGER NOT NULL,
                    phone_number TEXT NOT NULL,
                    effective_date TEXT NOT NULL,
                    FOREIGN KEY(customer_entry_id) REFERENCES customer_entries(id)
                );
                """
            )

    def add_entry(self, entry: CustomerEntry) -> None:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO customer_entries (register_date, customer_name, contact_number)
                VALUES (?, ?, ?)
                """,
                (
                    entry.register_date.isoformat(),
                    entry.customer_name,
                    entry.contact_number,
                ),
            )
            entry_id = cursor.lastrowid
            conn.executemany(
                """
                INSERT INTO phone_entries (customer_entry_id, phone_number, effective_date)
                VALUES (?, ?, ?)
                """,
                [
                    (
                        entry_id,
                        phone.phone_number,
                        phone.effective_date.isoformat(),
                    )
                    for phone in entry.phones
                ],
            )

    def iter_entries(self, start_date: date | None = None, end_date: date | None = None) -> Iterable[sqlite3.Row]:
        query = (
            """
            SELECT c.id, c.register_date, c.customer_name, c.contact_number,
                   p.phone_number, p.effective_date
            FROM customer_entries c
            LEFT JOIN phone_entries p ON p.customer_entry_id = c.id
            """
        )
        clauses: list[str] = []
        params: list[str] = []

        if start_date is not None:
            clauses.append("c.register_date >= ?")
            params.append(start_date.isoformat())

        if end_date is not None:
            clauses.append("c.register_date <= ?")
            params.append(end_date.isoformat())

        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        query += " ORDER BY c.register_date, c.id, p.effective_date"

        with self.connect() as conn:
            for row in conn.execute(query, params):
                yield row


def parse_date(text: str) -> date:
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"日期格式錯誤：{text}，請用 YYYY-MM-DD") from exc


def prompt_non_empty(label: str) -> str:
    while True:
        value = input(label).strip()
        if value:
            return value
        print("唔可以留空，請再輸入。")


def prompt_date(label: str, default: date | None = None) -> date:
    while True:
        suffix = f" [{default.isoformat()}]" if default else ""
        raw = input(f"{label}{suffix}: ").strip()
        if not raw and default:
            return default
        try:
            return parse_date(raw)
        except argparse.ArgumentTypeError as err:
            print(err)


def collect_entry_interactively() -> CustomerEntry:
    today = date.today()
    register_date = prompt_date("登記日期", default=today)
    customer_name = prompt_non_empty("客戶名稱: ")
    contact_number = prompt_non_empty("客戶聯絡號碼: ")

    phones: list[PhoneEntry] = []
    while True:
        phone_number = prompt_non_empty("電話號碼: ")
        effective_date = prompt_date("電話生效日期", default=register_date)
        phones.append(PhoneEntry(phone_number=phone_number, effective_date=effective_date))

        more = input("仲有其他電話號碼？(y/N): ").strip().lower()
        if more != "y":
            break

    return CustomerEntry(
        register_date=register_date,
        customer_name=customer_name,
        contact_number=contact_number,
        phones=phones,
    )


def format_report(rows: Iterable[sqlite3.Row]) -> str:
    grouped: dict[int, dict[str, object]] = {}

    for row in rows:
        item = grouped.setdefault(
            row["id"],
            {
                "register_date": row["register_date"],
                "customer_name": row["customer_name"],
                "contact_number": row["contact_number"],
                "phones": [],
            },
        )
        if row["phone_number"] is not None:
            item["phones"].append((row["phone_number"], row["effective_date"]))

    if not grouped:
        return "暫時未有資料。"

    lines: list[str] = ["=== 客戶登記報表 ==="]
    for idx, item in enumerate(grouped.values(), start=1):
        lines.append(f"{idx}. 登記日期: {item['register_date']}")
        lines.append(f"   客戶名稱: {item['customer_name']}")
        lines.append(f"   聯絡號碼: {item['contact_number']}")
        phones = item["phones"]
        lines.append(f"   已登記電話數量: {len(phones)}")
        for phone_no, eff_date in phones:
            lines.append(f"   - {phone_no} (生效日期: {eff_date})")

    return "\n".join(lines)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="客戶記事簿（日期式登記 + 報表）")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="初始化資料庫")
    sub.add_parser("add", help="互動式新增客戶記錄")

    report = sub.add_parser("report", help="顯示報表")
    report.add_argument("--start", type=parse_date, help="開始日期 YYYY-MM-DD")
    report.add_argument("--end", type=parse_date, help="結束日期 YYYY-MM-DD")

    return parser


def main() -> None:
    parser = make_parser()
    args = parser.parse_args()

    db = DiaryDB()

    if args.command == "init":
        db.init_db()
        print(f"已初始化資料庫：{db.db_path}")
        return

    db.init_db()

    if args.command == "add":
        entry = collect_entry_interactively()
        db.add_entry(entry)
        print("已成功新增記錄。")
        return

    if args.command == "report":
        rows = db.iter_entries(start_date=args.start, end_date=args.end)
        print(format_report(rows))


if __name__ == "__main__":
    main()
