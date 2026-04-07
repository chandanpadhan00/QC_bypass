"""
ASNF Product Shipments - Full ETL Pipeline
==========================================
Reads Excel shipment data + product mapping → PostgreSQL
For Tableau reporting

Usage:
    python ingest_shipments.py

Refresh Strategy: Truncate & Reload (safe for monthly updates)
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# ── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest_shipments.log"),
    ],
)
log = logging.getLogger(__name__)


# ── CONFIG  ───────────────────────────────────────────────────────────────────
# Update these values before running

SHIPMENTS_FILE = "ASNF Product Shipments in Single Units and Packs - to 02.28.26.xlsx"
MAPPING_FILE   = "Shipments_data_mapping_asnf.xlsx"

DB_URL = "postgresql+psycopg2://username:password@host:5432/your_database"
# Example: "postgresql+psycopg2://admin:secret@localhost:5432/asnf_db"

FACT_TABLE = "fact_product_shipments"
DIM_TABLE  = "dim_product"

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_MAP = {m: i + 1 for i, m in enumerate(MONTHS)}

# Add or remove sheets as new years are added
DETAIL_SHEETS = {
    "2023 Detail": 2023,
    "2024 Detail": 2024,
    "2025 Detail": 2025,
    "2026 Detail": 2026,
}


# ── HELPERS ───────────────────────────────────────────────────────────────────

def find_header_row(df_raw: pd.DataFrame) -> int:
    """Find the row index where 'NDC' appears — the true column header row."""
    for i, row in df_raw.iterrows():
        if any(str(v).strip().upper() == "NDC" for v in row.values):
            return i
    raise ValueError(
        "Could not find 'NDC' header row. "
        "Check that rows 1-2 are title rows and row 3 is blank."
    )


def build_column_names(df_raw: pd.DataFrame, header_row: int) -> list:
    """
    Build section-aware column names by combining:
      - section label row  (e.g. 'IN SINGLES', 'CONVERTED TO PACKS')
      - column header row  (e.g. 'NDC', 'Jan', 'Feb' ...)

    Returns list like:
      ['NDC', 'Product Name', 'Product Detail',
       'SINGLES_Jan', ..., 'SINGLES_Dec',
       'PACKS_Jan',   ..., 'PACKS_Dec']
    """
    section_row    = header_row - 1
    section_labels = df_raw.iloc[section_row].ffill().tolist()
    col_headers    = df_raw.iloc[header_row].tolist()

    named = []
    for sec, col in zip(section_labels, col_headers):
        sec = str(sec).strip().upper()
        col = str(col).strip()

        if col in ("NDC", "Product Name", "Product Detail"):
            named.append(col)
        elif col.upper() == "TOTAL" or col in ("nan", ""):
            named.append(f"_SKIP_{col}")
        elif "SINGLE" in sec and col in MONTHS:
            named.append(f"SINGLES_{col}")
        elif "PACK" in sec and col in MONTHS:
            named.append(f"PACKS_{col}")
        else:
            named.append(f"_SKIP_{col}")

    return named


# ── PARSE ─────────────────────────────────────────────────────────────────────

def parse_detail_sheet(df_raw: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Transform one Detail sheet from wide/pivoted format into
    a clean long-format DataFrame ready for database load.

    Input shape (after source cleanup):
        Row 0  : AMGEN SAFETY NET FOUNDATION
        Row 1  : 20XX Product Shipments ...
        Row 2  : (blank)
        Row 3  : IN SINGLES ...  CONVERTED TO PACKS ...
        Row 4  : NDC | Product Name | Product Detail | Jan...Dec | Jan...Dec
        Row 5+ : data

    Output columns:
        ndc, product_name, product_detail,
        year, month, month_name,
        units_singles, units_packs
    """
    header_row  = find_header_row(df_raw)
    named_cols  = build_column_names(df_raw, header_row)

    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = named_cols

    # Drop fully empty rows and rows missing NDC
    df = df.dropna(how="all")
    df = df[df["NDC"].notna() & (df["NDC"].astype(str).str.strip() != "nan")]
    df = df[df["NDC"].astype(str).str.strip() != ""]

    # Identify column groups
    id_cols     = [c for c in ["NDC", "Product Name", "Product Detail"] if c in df.columns]
    single_cols = [c for c in df.columns if c.startswith("SINGLES_")]
    pack_cols   = [c for c in df.columns if c.startswith("PACKS_")]

    # Unpivot months → rows
    df_singles = (
        df[id_cols + single_cols]
        .melt(id_vars=id_cols, var_name="month_key", value_name="units_singles")
        .assign(month_name=lambda x: x["month_key"].str.replace("SINGLES_", "", regex=False))
        .drop(columns="month_key")
    )

    df_packs = (
        df[id_cols + pack_cols]
        .melt(id_vars=id_cols, var_name="month_key", value_name="units_packs")
        .assign(month_name=lambda x: x["month_key"].str.replace("PACKS_", "", regex=False))
        .drop(columns="month_key")
    )

    # Merge singles + packs on NDC + month
    df_merged = pd.merge(
        df_singles,
        df_packs,
        on=id_cols + ["month_name"],
        how="outer",
    )

    # Drop future months where BOTH values are NULL (not yet received)
    df_merged = df_merged.dropna(subset=["units_singles", "units_packs"], how="all")

    # Coerce to numeric (handles dashes "-", blanks, formula remnants)
    df_merged["units_singles"] = pd.to_numeric(df_merged["units_singles"], errors="coerce")
    df_merged["units_packs"]   = pd.to_numeric(df_merged["units_packs"],   errors="coerce")

    # Add time dimensions
    df_merged["year"]  = year
    df_merged["month"] = df_merged["month_name"].map(MONTH_MAP)

    # Rename to DB-friendly names
    df_merged = df_merged.rename(columns={
        "NDC":            "ndc",
        "Product Name":   "product_name",
        "Product Detail": "product_detail",
    })

    # Ensure ndc is a clean string (no float formatting like 55513000204.0)
    df_merged["ndc"] = df_merged["ndc"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    final_cols = [
        "ndc", "product_name", "product_detail",
        "year", "month", "month_name",
        "units_singles", "units_packs",
    ]
    return df_merged[[c for c in final_cols if c in df_merged.columns]]


# ── LOADERS ───────────────────────────────────────────────────────────────────

def load_dim_product(engine):
    """Truncate & reload dim_product from the mapping Excel file."""
    log.info(f"Loading {DIM_TABLE} from: {MAPPING_FILE}")

    df = pd.read_excel(MAPPING_FILE, sheet_name="Sheet1", dtype=str)
    df.columns = df.columns.str.strip()

    df = df.rename(columns={
        "Product":                 "product_full_name",
        "NDC":                     "ndc",
        "NDC Name for Dashboard":  "ndc_name_dashboard",
        "Drug Name for Dashboard": "drug_name_dashboard",
    })

    df["ndc"] = df["ndc"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df = df[df["ndc"].notna() & (df["ndc"] != "nan") & (df["ndc"] != "")]

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {DIM_TABLE}"))

    df.to_sql(DIM_TABLE, engine, if_exists="append", index=False, chunksize=500)
    log.info(f"  ✔  {len(df):,} rows loaded into {DIM_TABLE}")


def load_fact_shipments(engine):
    """Truncate & reload fact_product_shipments from all Detail sheets."""
    xls        = pd.ExcelFile(SHIPMENTS_FILE)
    all_frames = []

    for sheet_name, year in DETAIL_SHEETS.items():
        if sheet_name not in xls.sheet_names:
            log.warning(f"  Sheet '{sheet_name}' not found — skipping")
            continue

        log.info(f"  Processing: {sheet_name} ({year}) ...")
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        df     = parse_detail_sheet(df_raw, year)
        log.info(f"    → {len(df):,} rows extracted")
        all_frames.append(df)

    if not all_frames:
        log.error("No data extracted. Verify SHIPMENTS_FILE path and sheet names.")
        sys.exit(1)

    df_final = pd.concat(all_frames, ignore_index=True)
    log.info(f"Total rows to load: {len(df_final):,}")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {FACT_TABLE}"))

    df_final.to_sql(FACT_TABLE, engine, if_exists="append", index=False, chunksize=500)
    log.info(f"  ✔  {len(df_final):,} rows loaded into {FACT_TABLE}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("ASNF Shipments ETL — Started")
    log.info(f"Run time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    engine = create_engine(DB_URL)

    # Step 1: Load product dimension / lookup table
    load_dim_product(engine)

    # Step 2: Load shipments fact table
    log.info(f"Loading {FACT_TABLE} from: {SHIPMENTS_FILE}")
    load_fact_shipments(engine)

    log.info("=" * 60)
    log.info("ETL Complete ✔")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
