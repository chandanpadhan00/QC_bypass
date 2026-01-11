CREATE TABLE IF NOT EXISTS asnfdm.qc_rule_bypass_audit (
  batch_id      BIGINT NOT NULL,
  source_name   VARCHAR(20) NOT NULL,
  case_id       VARCHAR(50) NOT NULL,
  qc_rule_no    VARCHAR(50) NOT NULL,
  deleted_ts    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_by    VARCHAR(100) NULL DEFAULT 'lambda',
  reason        VARCHAR(255) NULL
);

CREATE INDEX IF NOT EXISTS ix_qc_rule_bypass_audit_batch
ON asnfdm.qc_rule_bypass_audit (batch_id, source_name);
