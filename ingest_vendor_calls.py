CREATE OR REPLACE VIEW asnfdm.vw_vendor_call_data AS
SELECT
    vendor,
    call_date,
    outbound_calls,
    inbound_calls,
    asa_time,
    abandonment_rate_pct,
    avg_handle_talk_time,
    max_hold_time,
    load_time,
    EXTRACT(YEAR    FROM call_date)::INTEGER                    AS year,
    TO_CHAR(call_date, 'Mon')                                   AS month,
    EXTRACT(QUARTER FROM call_date)::INTEGER                    AS quarter
FROM asnfdm.vendor_call_data;
