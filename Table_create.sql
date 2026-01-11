CREATE TABLE IF NOT EXISTS asnfdm.qc_rule_case_bypass (
  source_name   VARCHAR(20) NOT NULL,     -- 'SNX' / 'REGALORX'
  qc_rule_no    VARCHAR(50) NOT NULL,     -- must match qc_failed.qc_rule_no
  case_id       VARCHAR(50) NOT NULL,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  reason        VARCHAR(255) NULL,
  created_by    VARCHAR(100) NULL,
  created_ts    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (source_name, qc_rule_no, case_id)
);

CREATE INDEX IF NOT EXISTS ix_qc_rule_case_bypass_active
ON asnfdm.qc_rule_case_bypass (source_name, is_active, qc_rule_no, case_id);
