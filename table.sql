vendor_call_data (
  id, vendor, call_date,          -- natural unique key: (vendor, call_date)
  outbound_calls, inbound_calls,
  asa_time, abandonment_rate_pct,
  avg_handle_talk_time, max_hold_time,
  load_time                        -- ← your requested addition
)
