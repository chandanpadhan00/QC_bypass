--q1
SELECT COUNT(DISTINCT case_id) 
FROM asnfdm.f_pcd2_detailed_report_vw
WHERE date_part('year', file_receipt_date_time) = 2026
  AND COALESCE(case_sub_status, '') <> 'Case Created in Error'
  AND file_receipt_date_time IS NOT NULL;

-----------

--q2
SELECT SUM(actual_application) 
FROM asnfdm.forecast_vs_actual_vw
WHERE date_part('year', month_start) = 2026;

-----------------

--q3
SELECT SUM(actual_application) FROM (
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
) x;



--q4

SELECT COUNT(DISTINCT case_id)
FROM asnfdm.f_pcd2_detailed_report_vw
WHERE date_part('year', file_receipt_date_time) = 2026
  AND COALESCE(case_sub_status, '') <> 'Case Created in Error'
  AND file_receipt_date_time IS NOT NULL
  AND drug IS NULL;
