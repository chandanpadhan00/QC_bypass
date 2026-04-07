-- ============================================================
-- ASNF Shipments Database Setup
-- Run this ONCE in your PostgreSQL database before the ETL
-- ============================================================


-- ------------------------------------------------------------
-- 1. DIMENSION TABLE: Product Lookup
--    Source: Shipments_data_mapping_asnf.xlsx
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    ndc                     VARCHAR(20)  PRIMARY KEY,
    product_full_name       VARCHAR(255),
    ndc_name_dashboard      VARCHAR(255),   -- Use this in Tableau labels
    drug_name_dashboard     VARCHAR(100),   -- Use this for drug-level grouping
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 2. FACT TABLE: Monthly Shipments
--    Source: ASNF Product Shipments Excel (2023–2026 Detail sheets)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_product_shipments (
    id              SERIAL PRIMARY KEY,
    ndc             VARCHAR(20),
    product_name    VARCHAR(100),
    product_detail  VARCHAR(255),
    year            INT,
    month           INT,            -- 1 = Jan, 2 = Feb, ... 12 = Dec
    month_name      VARCHAR(3),     -- 'Jan', 'Feb', ... 'Dec'
    units_singles   NUMERIC(18, 4), -- Shipments in single units
    units_packs     NUMERIC(18, 4), -- Shipments converted to packs
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ------------------------------------------------------------
-- 3. INDEXES for Tableau query performance
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_shipments_ndc
    ON fact_product_shipments(ndc);

CREATE INDEX IF NOT EXISTS idx_shipments_year_month
    ON fact_product_shipments(year, month);

CREATE INDEX IF NOT EXISTS idx_shipments_product
    ON fact_product_shipments(product_name);


-- ------------------------------------------------------------
-- 4. VIEW: Tableau-Ready Combined View
--    Joins fact + dim so Tableau only connects to one object
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW asnfdm.vw_shipments_tableau AS
WITH base AS (
    SELECT
        f.year,
        f.month,
        f.month_name,
        TO_DATE(
            f.year::TEXT || '-' || LPAD(f.month::TEXT, 2, '0') || '-01',
            'YYYY-MM-DD'
        ) AS shipment_date,
        f.ndc,
        COALESCE(d.ndc_name_dashboard,  f.product_name)   AS ndc_display_name,
        COALESCE(d.drug_name_dashboard, f.product_name)   AS drug_name,
        COALESCE(d.product_full_name,   f.product_detail) AS product_full_name,
        f.product_detail,
        f.units_singles,
        f.units_packs,
        f.loaded_at
    FROM asnfdm.fact_product_shipments f
    LEFT JOIN asnfdm.dim_product d ON f.ndc = d.ndc
)
SELECT
    b.*,
    v.vendor,
    v.benefit_type
FROM base b
LEFT JOIN asnfdm.product_benefit_vendor_map v ON b.drug_name = v.product;



-- ------------------------------------------------------------
-- 5. QUICK VERIFICATION QUERIES (run after ETL to validate)
-- ------------------------------------------------------------

-- Row counts per year
-- SELECT year, COUNT(*) AS rows, SUM(units_singles) AS total_singles
-- FROM fact_product_shipments
-- GROUP BY year ORDER BY year;

-- Check products matched to dim
-- SELECT COUNT(*) AS unmatched
-- FROM fact_product_shipments f
-- LEFT JOIN dim_product d ON f.ndc = d.ndc
-- WHERE d.ndc IS NULL;

-- Preview the Tableau view
-- SELECT * FROM vw_shipments_tableau LIMIT 20;
