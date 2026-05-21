"""
ingest_vendor_calls.py
Incrementally loads vendor call data from an Excel file into PostgreSQL.

Usage:
    python ingest_vendor_calls.py --file "Master Vendor Call Data for Dashboard.xlsx"

Dependencies:
    pip install pandas openpyxl psycopg2-binary python-dotenv

Environment variables (set in .env or shell):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TABLE_NAME = "asnfdm.vendor_call_data"
SHEET_NAME = "Vendor Data"

COLUMN_MAP = {
    "Vendor":                "vendor",
    "Date":                  "call_date",
    "Outbound Calls #":      "outbound_calls",
    "Inbound Calls #":       "inbound_calls",
    "ASA Time":              "asa_time",
    "Abandonment Rate %":    "abandonment_rate_pct",
    "Avg Handle/Talk Time":  "avg_handle_talk_time",
    "Max Hold Time":         "max_hold_time",
}

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id                   SERIAL PRIMARY KEY,
    vendor               TEXT        NOT NULL,
    call_date            DATE        NOT NULL,
    outbound_calls       INTEGER,
    inbound_calls        INTEGER,
    asa_time             INTERVAL,
    abandonment_rate_pct NUMERIC(6,4),
    avg_handle_talk_time INTERVAL,
    max_hold_time        INTERVAL,
    load_time            TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_vendor_date UNIQUE (vendor, call_date)
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def parse_time_to_interval(value) -> str | None:
    """Convert HH:MM:SS string or timedelta from Excel to a Postgres-compatible interval string."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return value.strip()          # already "HH:MM:SS"
    # Excel may parse time-formatted cells as timedelta
    if hasattr(value, "total_seconds"):
        total = int(value.total_seconds())
        h, remainder = divmod(total, 3600)
        m, s = divmod(remainder, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    return str(value)


def parse_pct(value) -> float | None:
    """Normalise percent: '2.17%' -> 0.0217 or 2.17 -> 2.17 (stored as-is)."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        return float(value.strip().rstrip("%"))
    return float(value)


def load_excel(filepath: str) -> pd.DataFrame:
    df = pd.read_excel(filepath, sheet_name=SHEET_NAME, header=0)
    # Keep only expected columns (ignore any extra trailing columns)
    df = df[[c for c in COLUMN_MAP if c in df.columns]]
    df = df.rename(columns=COLUMN_MAP)
    df = df.dropna(subset=["vendor", "call_date"])

    df["call_date"]            = pd.to_datetime(df["call_date"]).dt.date
    df["outbound_calls"]       = pd.to_numeric(df["outbound_calls"], errors="coerce").astype("Int64")
    df["inbound_calls"]        = pd.to_numeric(df["inbound_calls"],  errors="coerce").astype("Int64")
    df["asa_time"]             = df["asa_time"].apply(parse_time_to_interval)
    df["abandonment_rate_pct"] = df["abandonment_rate_pct"].apply(parse_pct)
    df["avg_handle_talk_time"] = df["avg_handle_talk_time"].apply(parse_time_to_interval)
    df["max_hold_time"]        = df["max_hold_time"].apply(parse_time_to_interval)

    return df


def upsert_rows(conn, df: pd.DataFrame, load_time: datetime) -> tuple[int, int]:
    df["load_time"] = load_time

    cols = [
        "vendor", "call_date", "outbound_calls", "inbound_calls",
        "asa_time", "abandonment_rate_pct", "avg_handle_talk_time",
        "max_hold_time", "load_time",
    ]
    rows = [tuple(row[c] for c in cols) for _, row in df.iterrows()]

    # ON CONFLICT: update all metric columns but keep the original load_time
    # so we know when the row first appeared; bump load_time only if data changes.
    upsert_sql = f"""
        INSERT INTO {TABLE_NAME}
            (vendor, call_date, outbound_calls, inbound_calls,
             asa_time, abandonment_rate_pct, avg_handle_talk_time,
             max_hold_time, load_time)
        VALUES %s
        ON CONFLICT (vendor, call_date) DO UPDATE SET
            outbound_calls       = EXCLUDED.outbound_calls,
            inbound_calls        = EXCLUDED.inbound_calls,
            asa_time             = EXCLUDED.asa_time,
            abandonment_rate_pct = EXCLUDED.abandonment_rate_pct,
            avg_handle_talk_time = EXCLUDED.avg_handle_talk_time,
            max_hold_time        = EXCLUDED.max_hold_time,
            load_time            = EXCLUDED.load_time
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        results = execute_values(cur, upsert_sql, rows, fetch=True)
        conn.commit()

    inserted = sum(1 for (was_insert,) in results if was_insert)
    updated  = len(results) - inserted
    return inserted, updated


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest vendor call data Excel → PostgreSQL")
    parser.add_argument(
        "--file",
        default=r"C:\Users\hp\Downloads\Master Vendor Call Data for Dashboard.xlsx",
        help="Path to the Excel file",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse the file and show row counts without writing to the DB",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        sys.exit(f"ERROR: File not found: {args.file}")

    print(f"Reading: {args.file}")
    df = load_excel(args.file)
    print(f"  Parsed {len(df)} rows across vendors: {sorted(df['vendor'].unique())}")

    if args.dry_run:
        print("Dry-run mode — no DB writes.")
        print(df.to_string(index=False))
        return

    for var in ("DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        if not os.environ.get(var):
            sys.exit(f"ERROR: Environment variable {var} is not set.")

    load_time = datetime.now(tz=timezone.utc)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("Table ready.")

        inserted, updated = upsert_rows(conn, df, load_time)
        print(f"Done — inserted: {inserted}, updated: {updated}, load_time: {load_time.isoformat()}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
