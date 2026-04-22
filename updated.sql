CREATE OR REPLACE VIEW asnfdm.forecast_vs_actual_vw
AS
WITH forecast_norm AS (
    SELECT
        CASE
            WHEN f.month::text ~ '^\d{4}-\d{2}$'
                THEN to_date(f.month || '-01', 'YYYY-MM-DD')
            ELSE NULL::date
        END AS month_start,
        f.month AS month_label,
        COALESCE(NULLIF(btrim(f.drug::text), ''), 'unknown') AS drug,
        COALESCE(NULLIF(btrim(f.vendor::text), ''), 'unknown') AS vendor,
        f.published_quarter,
        f.forecasted_application,
        f.forecasted_enrollment
    FROM asnfdm.forecast_table f
),
actual_app AS (
    SELECT
        make_date(
            date_part('year', r.file_receipt_date_time)::int,
            date_part('month', r.file_receipt_date_time)::int,
            1
        ) AS month_start,
        COALESCE(NULLIF(btrim(r.drug::text), ''), 'unknown') AS drug,
        COALESCE(NULLIF(btrim(r.external_source::text), ''), 'unknown') AS vendor,
        COUNT(DISTINCT r.case_id) AS actual_application
    FROM asnfdm.f_pcd2_detailed_report_vw r
    WHERE r.file_receipt_date_time IS NOT NULL
      AND COALESCE(r.case_sub_status, '') <> 'Case Created in Error'
    GROUP BY 1, 2, 3
),
actual_enr AS (
    SELECT
        make_date(
            date_part('year', r.eligibility_start_date)::int,
            date_part('month', r.eligibility_start_date)::int,
            1
        ) AS month_start,
        COALESCE(NULLIF(btrim(r.drug::text), ''), 'unknown') AS drug,
        COALESCE(NULLIF(btrim(r.external_source::text), ''), 'unknown') AS vendor,
        COUNT(*) AS actual_enrollment
    FROM asnfdm.f_pcd2_detailed_report_vw r
    WHERE r.eligibility_start_date IS NOT NULL
      AND COALESCE(r.case_sub_status, '') <> 'Case Created in Error'
    GROUP BY 1, 2, 3
)
SELECT
    COALESCE(f.month_start, a.month_start, e.month_start) AS month_start,
    COALESCE(f.month_label, to_char(COALESCE(a.month_start, e.month_start), 'YYYY-MM')) AS month_label,
    COALESCE(f.drug, a.drug, e.drug)     AS drug,
    COALESCE(f.vendor, a.vendor, e.vendor) AS vendor,
    f.published_quarter,
    COALESCE(a.actual_application, 0)    AS actual_application,
    COALESCE(f.forecasted_application, 0) AS forecasted_application,
    COALESCE(e.actual_enrollment, 0)     AS actual_enrollment,
    COALESCE(f.forecasted_enrollment, 0) AS forecasted_enrollment
FROM forecast_norm f
FULL OUTER JOIN actual_app a
    ON a.month_start = f.month_start
   AND a.drug        = f.drug
   AND a.vendor      = f.vendor
FULL OUTER JOIN actual_enr e
    ON e.month_start = COALESCE(f.month_start, a.month_start)
   AND e.drug        = COALESCE(f.drug, a.drug)
   AND e.vendor      = COALESCE(f.vendor, a.vendor);
