/* ================================
   RULE-BASED QC BYPASS – AUDIT
   ================================ */

INSERT INTO asnfdm.qc_rule_bypass_audit
(
  batch_id,
  source_name,
  case_id,
  qc_rule_no,
  deleted_by,
  reason
)
SELECT
  f.batch_id,
  '{SOURCE_NAME}'      AS source_name,
  f.case_id,
  f.qc_rule_no,
  'qc_script'          AS deleted_by,
  b.reason
FROM {QC_FAILED_TABLE} f
JOIN asnfdm.qc_rule_case_bypass b
  ON b.source_name = '{SOURCE_NAME}'
 AND b.is_active   = TRUE
 AND b.qc_rule_no  = f.qc_rule_no
 AND b.case_id     = f.case_id
WHERE f.batch_id = {BATCH_ID};


/* ================================
   RULE-BASED QC BYPASS – DELETE
   ================================ */

DELETE FROM {QC_FAILED_TABLE} f
USING asnfdm.qc_rule_case_bypass b
WHERE f.batch_id   = {BATCH_ID}
  AND b.source_name = '{SOURCE_NAME}'
  AND b.is_active   = TRUE
  AND b.qc_rule_no  = f.qc_rule_no
  AND b.case_id     = f.case_id;
