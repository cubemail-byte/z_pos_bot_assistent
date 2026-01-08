from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


TID_RE = re.compile(r"\b(\d{8})\b")  # terminal_id = ровно 8 цифр


def norm_digits(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in str(s).strip() if ch.isdigit())


def pick_tid(val_raw: str) -> str:
    """
    Из Val может прилететь одно число или несколько.
    Берём ПЕРВОЕ 8-значное. (Если нужно — поменяем на "последнее".)
    """
    if not val_raw:
        return ""
    m = TID_RE.search(str(val_raw))
    return m.group(1) if m else ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/agent.db")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--encoding", default="utf-8-sig")  # BOM-friendly
    ap.add_argument("--delimiter", default=";", help="CSV delimiter: ';' or ',' or 'auto'")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    imported_at_utc = datetime.now(timezone.utc).isoformat()

    # delimiter detection
    delimiter = args.delimiter
    if delimiter == "auto":
        with open(csv_path, "r", encoding=args.encoding, newline="") as f:
            sample = f.read(4096)
        delimiter = csv.Sniffer().sniff(sample).delimiter

    rows = []
    with open(csv_path, "r", encoding=args.encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header / fieldnames")

        for r in reader:
            azs = norm_digits(r.get("AZS") or r.get("﻿AZS") or "")
            arm = (r.get("ARM") or "").strip()
            plnum = norm_digits(r.get("PlNum") or "")
            ip = (r.get("IP") or r.get("Ip") or "").strip()
            val_raw = (r.get("Val") or "").strip()
            src_ts = (r.get("Timestamp") or "").strip()

            tid = pick_tid(val_raw)

            # строгость форматов
            if not re.fullmatch(r"\d{2,4}", azs):
                continue
            if not re.fullmatch(r"\d{1,2}", plnum):
                continue
            if tid and not re.fullmatch(r"\d{8}", tid):
                continue

            if not arm:
                arm = "UNKNOWN"

            rows.append((azs, arm, plnum, ip, tid, val_raw, src_ts))

    print(f"Prepared rows: {len(rows)}")
    if args.dry_run:
        print("Sample:", rows[:5])
        return

    con = sqlite3.connect(args.db)
    try:
        con.execute("BEGIN")
        con.executemany(
            """
            INSERT INTO terminal_directory(
              azs, arm, plnum,
              ip, tid, serial_number,
              val_raw, src_timestamp, source_file, imported_at_utc
            )
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
            ON CONFLICT(azs, arm, plnum) DO UPDATE SET
              ip=excluded.ip,
              tid=excluded.tid,
              val_raw=excluded.val_raw,
              src_timestamp=excluded.src_timestamp,
              source_file=excluded.source_file,
              imported_at_utc=excluded.imported_at_utc
            """,
            [
                (azs, arm, plnum, ip, tid, val_raw, src_ts, str(csv_path), imported_at_utc)
                for (azs, arm, plnum, ip, tid, val_raw, src_ts) in rows
            ],
        )
        con.commit()
        print("Import done.")
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
