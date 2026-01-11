def apply_rule_based_bypass(conn, cursor, config, source_name, batch_id, qc_failed_table):
    """
    Deletes rule-specific bypassed failures from qc_failed_table for this batch,
    and writes deleted rows to audit table.
    Global exclusion (config['conf_json']['qc_details']['exclusion']) remains as-is.
    """
    rb = config['conf_json']['qc_details'].get('rule_bypass', {})
    if not rb or not rb.get('enabled', False):
        print("Rule bypass disabled.")
        return

    bypass_table = rb.get('table', 'asnfdm.qc_rule_case_bypass')
    audit_table = rb.get('audit_table', 'asnfdm.qc_rule_bypass_audit')
    deleted_by = rb.get('deleted_by', 'lambda')

    print(f"Applying rule-based bypass using {bypass_table}; audit into {audit_table}")

    # 1) Insert rows that will be deleted into audit table (for traceability)
    ins_sql = f"""
        INSERT INTO {audit_table} (batch_id, source_name, case_id, qc_rule_no, deleted_by, reason)
        SELECT f.batch_id,
               %s AS source_name,
               f.case_id,
               f.qc_rule_no,
               %s AS deleted_by,
               b.reason
        FROM {qc_failed_table} f
        JOIN {bypass_table} b
          ON b.source_name = %s
         AND b.is_active = TRUE
         AND b.qc_rule_no = f.qc_rule_no
         AND b.case_id = f.case_id
        WHERE f.batch_id = %s
    """
    cursor.execute(ins_sql, (source_name, deleted_by, source_name, batch_id))
    ins_count = cursor.rowcount
    print(f"Audit inserted rows: {ins_count}")

    # 2) Delete bypassed rows from qc_failed for this batch
    del_sql = f"""
        DELETE FROM {qc_failed_table} f
        USING {bypass_table} b
        WHERE f.batch_id = %s
          AND b.source_name = %s
          AND b.is_active = TRUE
          AND b.qc_rule_no = f.qc_rule_no
          AND b.case_id = f.case_id
    """
    cursor.execute(del_sql, (batch_id, source_name))
    del_count = cursor.rowcount
    print(f"Deleted bypassed QC failures: {del_count}")

    conn.commit()
