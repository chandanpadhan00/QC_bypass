ASNF Shipments ETL — Quick Start Guide
=======================================

FILES IN THIS FOLDER
--------------------
  ingest_shipments.py   → Main ETL script (run this monthly)
  setup_db.sql          → Run ONCE to create DB tables & view
  requirements.txt      → Python package dependencies
  README.txt            → This file


FIRST-TIME SETUP
----------------
1. Install dependencies:
      pip install -r requirements.txt

2. Run the SQL setup in your PostgreSQL database:
      psql -U your_user -d your_database -f setup_db.sql

3. Open ingest_shipments.py and update the CONFIG section:
      SHIPMENTS_FILE  → path to the ASNF Excel file
      MAPPING_FILE    → path to the mapping Excel file
      DB_URL          → your PostgreSQL connection string
                        Format: postgresql+psycopg2://user:pass@host:5432/dbname


MONTHLY REFRESH (When New Month Data Arrives)
---------------------------------------------
1. Open the ASNF Excel file
2. Add the new month's data to the relevant year sheet (e.g. 2026 Detail)
3. Save the file
4. Run:
      python ingest_shipments.py
5. Done — Tableau will show updated data on next refresh


TABLEAU CONNECTION
------------------
Connect Tableau to PostgreSQL and use:
   - vw_shipments_tableau   → Main view (recommended — pre-joined)
   - shipment_date field     → For time-series/trend charts
   - drug_name               → For drug-level filtering
   - ndc_display_name        → For product-level detail
   - units_singles           → Measure: single unit shipments
   - units_packs             → Measure: pack shipments


LOG FILE
--------
Each run creates/appends to: ingest_shipments.log
Check this file if something goes wrong.
