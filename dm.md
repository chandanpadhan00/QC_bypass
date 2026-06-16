# ASNF PAP DataMart – Audit Response
## Request IDs: 45-IS and 46-IS
**Prepared by:** ASNF DataMart Tech Team (Rollie Abunan / Chandan Padhan)
**Date:** June 16, 2026
**Audit:** Amgen PAP – ASNF – Sonexus Audit (CA Audit Team / PwC)

---

## 45-IS: Evidence of Data Load Completeness Validation (ConnectSource / Sonexus → DataMart)

### Overview

The ASNF DataMart team performs automated load validation as part of every Sonexus (SNX) data ingestion cycle. The process is orchestrated via an AWS Lambda function (`S3tofactLoad.py`) that triggers whenever Sonexus delivers a new Patient Case Detail Report file to the designated S3 inbound bucket. The validation encompasses source record count verification, staging table load confirmation, QC rule execution, and weekly exception reporting — all tracked via a centralized process log table (`anfsdm.pap_data_process_log`).

### Step-by-Step Load Validation Process

**Step 1 – Source File Receipt and Record Count Capture**

Upon S3 trigger, the Lambda function immediately reads the incoming Sonexus file and computes the source record count:

```python
src_count = len(filedata.split('\n')) - 1
print('Source CSV File Record count: {}'.format(src_count))
```

This establishes the expected number of records received from Sonexus for the given load cycle.

**Step 2 – Staging Table Load**

The file is standardized (encoding normalized to UTF-8, column names cleaned) and loaded into the PostgreSQL RDS staging table (`asnfdm.stg_pcd_snx`) using a `COPY` command with a pre-truncate to ensure a clean load:

```sql
TRUNCATE TABLE {stg_table} CASCADE;
COPY {stg_table} FROM stdin WITH CSV HEADER DELIMITER AS E'\t' NULL AS '' ENCODING 'utf-8'
```

The staging load status is logged to `anfsdm.pap_data_process_log` with `stg_status = 'SUCCESS'` or `'FAILED'`.

**Step 3 – Record Count Reconciliation (Source vs. Stage)**

After staging, the system queries the RDS staging table for the loaded row count:

```python
stg_count = execute_query('select count(*) from {}'.format(rds_table))
```

This is compared against the source file count via the `counts_validation()` function. If there is a mismatch, an automated alert email is sent to the PAP team and Sonexus leads with the exact source and staging counts:

> *"Counts are not matching between src and target file (stage) for: SNX — src_count: X, stg_count: Y"*

The staging count is also persisted in `anfsdm.pap_data_process_log` (`stage_count` column) for audit traceability.

**Step 4 – QC Rule Execution**

For Patient Case Detail Report files, the `qc_check()` function is invoked. This executes a comprehensive set of QC business rules against the staged SNX data and populates:

- `anfsdm.stg_pcd_snx_qc_failed` — records failing any QC rule
- `anfsdm.stg_pcd_snx_qc_passed` — records passing all QC rules
- `anfsdm.pap_qc_rule_history` — historical rule-level exception counts per batch

QC completion is logged as `quality_check = 'SUCCESS'` in `pap_data_process_log`.

**Step 5 – Weekly PCD QC Exceptions Report Generation**

The `load_report_to_s3()` function generates the Weekly PCD QC Exceptions Report as an Excel workbook with the following structure:

| Sheet | Content |
|---|---|
| **PCD 2.0 Exception SNX** | Full patient-level detail for all QC exception records (vendor_id, patient_id, case_id, demographics, financials) |
| **COUNT** | Rule-level exception summary: QC_RULE_NO, QC_RULE description, COUNT (current week), LAST_LOAD_COUNT (prior week), DIFFERENCE |
| **New This Wk** | Net-new exception cases identified in the current load cycle |

The COUNT sheet provides direct load-to-load reconciliation: the `DIFFERENCE` column flags any increase or decrease in exceptions per rule versus the prior week's load, enabling immediate identification of anomalies.

This report is uploaded to the S3 QC report bucket and copied to the S3 outbound folder, and an email notification with the report path is automatically sent to Sonexus leads and ASNF Ops for their Wednesday weekly review.

### Artifact Provided

The attached **PCD2_Exception_SNX_2026_06_15_06160001** is a representative example of this weekly report, generated for the June 15, 2026 load cycle (Batch ID: 06160001). It demonstrates the full QC/load validation process described above, including current-vs-prior-week count comparison across all active QC rules.

---

## 46-IS: Confirmation of Completeness – All Applicable Sonexus Records Included in PCD Monitoring

### Overview

The ASNF DataMart team ensures completeness of Sonexus records in the PCD monitoring process through a combination of automated ingestion controls, SCD-style fact table management, and QC-driven inclusion logic. Every record received from Sonexus that passes QC validation is included in the PCD monitoring dataset (`asnfdm_f_pcd_report_v2`).

### Completeness Assurance Mechanisms

**Mechanism 1 – Full File Ingestion (No Partial Loads)**

Each Sonexus delivery is a complete extract of Patient Case Detail records for the relevant period. The staging table (`asnfdm.stg_pcd_snx`) is fully truncated before each load to ensure no stale or duplicate records from prior cycles are retained. The source record count is captured and reconciled against the staging count (as described under 45-IS) to confirm that every record in the delivered file was successfully loaded into the DataMart.

**Mechanism 2 – QC-Based Inclusion Logic**

All records that pass QC validation (`qc_passed = 'Y'`) from `anfsdm.stg_pcd_snx_qc_passed` are merged into the fact table `asnfdm_f_pcd_report_v2` via `f_pcd_report.sql`. Records that fail QC (`asnfdm.stg_pcd_snx_qc_failed`) are loaded with `qc_passed = 'N'` and remain in the fact table for transparency and remediation tracking — they are not excluded from the monitoring dataset.

**Mechanism 3 – Historical Record Preservation (No Record Loss Across Loads)**

The fact table rebuild logic in `f_pcd_report.sql` uses a UNION approach that explicitly preserves records from prior load cycles that are no longer present in the current Sonexus file:

```sql
-- Retain prior-cycle records not in current staging file and not already in history
INSERT INTO asnfdm_f_pcd_report_v2
SELECT ... FROM asnfdm_f_pcd_report_v2 fpr2
WHERE NOT EXISTS (SELECT ... FROM stg_table WHERE case_id matches)
AND NOT EXISTS (SELECT ... FROM asnfdm_f_pcd_report_v2_history WHERE case_id matches)
AND external_source = 'SNX';
```

This ensures that Sonexus records present in prior loads but absent from the current delivery are not silently dropped — they are either retained in the active fact table or moved to the history table, maintaining full longitudinal completeness.

**Mechanism 4 – Exception History Tracking for Completeness Monitoring**

The `anfsdm.pap_qc_rule_history` table maintains a rule-level count history across all batches for the SNX vendor. The DIFFERENCE column in the COUNT sheet of the Weekly PCD QC Exceptions Report provides a direct week-over-week completeness signal: a negative difference (e.g., RULE #12A: COUNT=34, LAST_LOAD_COUNT=65, DIFFERENCE=-31) indicates that previously flagged cases were remediated and successfully re-entered the clean PCD dataset, confirming that the monitoring process captured and resolved those records.

**Mechanism 5 – Process Log Audit Trail**

Every load cycle is tracked end-to-end in `anfsdm.pap_data_process_log` with the following status fields:

| Field | Purpose |
|---|---|
| `stg_status` | Confirms staging load success/failure |
| `stage_count` | Records count loaded into staging |
| `quality_check` | Confirms QC execution status |
| `stageidim` | Confirms staging-to-dimension load |
| `stage_dimtofact` | Confirms dimension-to-fact load |
| `stg_start_dt` / `stg_end_dt` | Full timestamp traceability per batch |

This log provides an auditable chain of custody for every Sonexus record from file receipt through PCD fact table inclusion.

### Summary

The combination of source-to-stage record count reconciliation, QC-driven inclusion with full exception tracking, historical record preservation logic, and the Weekly PCD QC Exceptions Report collectively ensure that all applicable Sonexus records expected to be included in the PCD monitoring process are accounted for in each load cycle. The attached report for June 15, 2026 (Batch 06160001) serves as evidence of this completeness validation.

---

*Note: The ASNF DataMart team does not use the term "ConnectSource" internally. Sonexus is referenced directly as the source vendor for PCD data, regardless of the system(s) they use to generate and deliver the records.*
