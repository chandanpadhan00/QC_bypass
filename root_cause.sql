-- What's in actual_app (for 2026) vs what comes out of the view (for 2026)?
WITH actual_app_standalone AS (
    SELECT
        make_date(
            date_part('year', r.file_receipt_date_time)::int,
            date_part('month', r.file_receipt_date_time)::int,
            1
        ) AS month_start,
        r.drug,
        COALESCE(NULLIF(btrim(r.external_source::text), ''), 'unknown') AS vendor,
        COUNT(DISTINCT r.case_id) AS actual_application
    FROM asnfdm.f_pcd2_detailed_report_vw r
    WHERE r.file_receipt_date_time IS NOT NULL
      AND COALESCE(r.case_sub_status, '') <> 'Case Created in Error'
      AND date_part('year', r.file_receipt_date_time) = 2026
    GROUP BY 1, 2, 3
),
view_agg AS (
    SELECT 
        month_start, 
        drug, 
        vendor, 
        MAX(actual_application) AS actual_application  -- MAX to collapse quarter fan-out
    FROM asnfdm.forecast_vs_actual_vw
    WHERE date_part('year', month_start) = 2026
    GROUP BY 1, 2, 3
)
SELECT 
    COALESCE(s.month_start, v.month_start) AS month_start,
    COALESCE(s.drug, v.drug)               AS drug,
    COALESCE(s.vendor, v.vendor)           AS vendor,
    s.actual_application AS standalone_count,
    v.actual_application AS view_count,
    COALESCE(s.actual_application, 0) - COALESCE(v.actual_application, 0) AS diff
FROM actual_app_standalone s
FULL OUTER JOIN view_agg v
    ON s.month_start = v.month_start
   AND s.drug        = v.drug
   AND s.vendor      = v.vendor
WHERE COALESCE(s.actual_application, 0) <> COALESCE(v.actual_application, 0)
ORDER BY diff DESC;
