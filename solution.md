# ASNF / PAP Fraud, Waste & Abuse Detection Framework

**Target view:** `asnfdm.pcd2_tuesday_full_extract_vw_v2`

**Engine:** PostgreSQL  

**Design principle:** modular detectors → a single scorecard. Every rule emits a uniform `(case_id, rule_id, points, evidence)` row. Detectors never talk to each other; the scoring layer aggregates them. This keeps rules independently testable, tunable, and auditable — and it's the pattern a real FWA pipeline uses.

---

## 0. Foundations (do these first)

**Filter to the current record.** The PHI side carries `is_latest` and `qc_passed`. Version history will otherwise masquerade as duplicate identities and poison every dedup rule. Build all detectors on top of a stable base:

```sql
CREATE OR REPLACE VIEW asnf_fraud_qc.base_active AS
SELECT *
FROM asnfdm.pcd2_tuesday_full_extract_vw_v2
WHERE COALESCE(is_latest, TRUE) = TRUE;
```

**Enable fuzzy matching once.** Identity and provider rules lean on it.

```sql
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;   -- levenshtein, soundex, dmetaphone
CREATE EXTENSION IF NOT EXISTS pg_trgm;         -- similarity() for names/addresses
```

**Normalization helpers.** Fraud hides in formatting noise. Normalize before you compare or count.

```sql
-- collapse case, punctuation, whitespace, common abbreviations
-- (inline as a scalar expression or wrap in a SQL function)
regexp_replace(upper(btrim(addr1)), '[^A-Z0-9]', '', 'g')      AS addr1_norm
regexp_replace(phone_preferred,     '[^0-9]',    '', 'g')      AS phone_norm
lower(btrim(email_addr))                                        AS email_norm
```

---

## 1. Advanced anomaly scenarios + detection logic

The three baseline rules (address >5, phone >5, correctional-facility string match) are pure **count** and **keyword** rules. The vectors below are where structured fraud actually lives: cross-field contradictions, timelines, geography, and identity graphs.

### R-100 · Identity stitching / duplicate enrollment
**Hypothesis:** one real person re-enrolls under variant identities to exceed program caps or re-qualify after termination.

**Logic:** self-join the active base on stable biometric-ish keys, tolerate name variation with `soundex`/`levenshtein`, and *exclude the same patient_id* so you catch splitting, not versioning. The strongest single anchor is `dob` + phonetic surname; escalate when phone or address also collide.

```sql
-- Same DOB + fuzzy surname across DIFFERENT patient_ids
WITH p AS (
  SELECT patient_id, case_id, dob,
         upper(btrim(lname)) AS lname, upper(btrim(fname)) AS fname,
         regexp_replace(phone_preferred,'[^0-9]','','g') AS phone_norm,
         lower(btrim(email_addr)) AS email_norm
  FROM asnf_fraud_qc.base_active
  WHERE dob IS NOT NULL
)
SELECT a.case_id, b.case_id AS matched_case_id,
       a.dob, a.lname, b.lname,
       levenshtein(a.lname,b.lname) AS lname_dist,
       (a.phone_norm = b.phone_norm) AS phone_match,
       (a.email_norm = b.email_norm) AS email_match
FROM p a
JOIN p b
  ON a.dob = b.dob
 AND a.patient_id < b.patient_id          -- distinct people, dedup the pair
 AND soundex(a.lname) = soundex(b.lname)
 AND levenshtein(a.lname,b.lname) <= 2;    -- typo-tolerant, not identical
```

Escalate the same pattern to `email_norm` (a single email across many patient_ids is a strong synthetic-identity signal) and to `phone_norm` across *different* surnames.

### R-110 · Shared identifier across different names
**Hypothesis:** enrollment ring — one address/phone/email fronting for many "patients."
**Logic:** count *distinct surnames* (not cases) per normalized identifier. This is the sharper cousin of the baseline count rule, which counts cases and misses the ring structure.

```sql
SELECT regexp_replace(upper(btrim(addr1)),'[^A-Z0-9]','','g') AS addr_norm,
       addr_zip,
       count(DISTINCT upper(btrim(lname))) AS distinct_surnames,
       count(DISTINCT patient_id)          AS distinct_patients,
       count(DISTINCT case_id)             AS cases
FROM asnf_fraud_qc.base_active
WHERE addr1 IS NOT NULL
GROUP BY 1,2
HAVING count(DISTINCT upper(btrim(lname))) >= 4
ORDER BY distinct_surnames DESC;
```

### R-120 · Income / FPL manipulation (reported vs validated)
**Hypothesis:** applicant understates income or inflates household size to drop under the FPL cutoff.
**Logic:** the view hands you the perfect contradiction pair — `*_reported` vs `*_validated`. Flag material downward divergence, and specifically the cases that *only qualify on the reported figure*.

```sql
SELECT case_id, patient_id,
       patient_fpl_reported, patient_fpl_validated,
       patient_income_reported, patient_income_validated,
       patient_household_size_reported, patient_household_size_validated
FROM asnf_fraud_qc.base_active
WHERE patient_fpl_validated IS NOT NULL
  AND patient_fpl_reported  IS NOT NULL
  AND (
        patient_fpl_validated - patient_fpl_reported > 50          -- >50 FPL-pt gap
     OR patient_household_size_reported > patient_household_size_validated  -- inflated HH
     OR (patient_income_validated > patient_income_reported * 1.25)         -- understated income
      );
```

### R-121 · FPL threshold bunching
**Hypothesis:** honest income is continuous; fraudulent applications *cluster just under* the qualification line (e.g. 250% / 400% / 500% FPL). Bunching below a bright line is a classic manipulation fingerprint.
**Logic:** count reported FPL in a narrow band below the cutoff vs a symmetric band above. A heavy below/above ratio is your alarm — and individual cases sitting in the just-below band get flagged.

```sql
-- :cutoff is your program's FPL qualification ceiling
SELECT
  count(*) FILTER (WHERE patient_fpl_reported BETWEEN :cutoff-10 AND :cutoff)     AS just_below,
  count(*) FILTER (WHERE patient_fpl_reported >  :cutoff AND patient_fpl_reported <= :cutoff+10) AS just_above
FROM asnf_fraud_qc.base_active;
-- ratio >> 1 signals bunching; then flag the individual just_below cases as R-121
```

### R-122 · Net-worth / LIS contradiction
**Hypothesis:** low reported income is inconsistent with declared assets.
**Logic:** cross the poverty claim against `overnetworth`, `patient_net_worth_reported`, and `lis_received`.

```sql
SELECT case_id, patient_id, patient_income_reported,
       patient_net_worth_reported, overnetworth, lis_received
FROM asnf_fraud_qc.base_active
WHERE (overnetworth = TRUE OR patient_net_worth_reported > 250000)
  AND patient_fpl_reported <= :cutoff;
```

### R-200 · Prescriber concentration (pill-mill velocity)
**Hypothesis:** a small number of NPIs / facilities drive a disproportionate share of enrollments.
**Logic:** rank prescribers by volume with a window function; flag the top percentile *and* absolute outliers. Percentile alone drifts, so pair it with a hard floor.

```sql
WITH md AS (
  SELECT md_npi,
         count(DISTINCT case_id)    AS cases,
         count(DISTINCT patient_id) AS patients
  FROM asnf_fraud_qc.base_active
  WHERE md_npi IS NOT NULL
  GROUP BY md_npi
)
SELECT md_npi, cases, patients,
       percent_rank() OVER (ORDER BY cases) AS pctl
FROM md
WHERE cases >= 50                              -- absolute floor
   OR percent_rank() OVER (ORDER BY cases) >= 0.99;
```

### R-210 · Office-contact reused across many prescribers
**Hypothesis:** one broker/"office" fronts many NPIs — a fake-practice or enrollment-mill signature.
**Logic:** count distinct `md_npi` per office contact. A legitimate office contact maps to one (or a few) prescribers.

```sql
SELECT upper(btrim(office_contact_first_name)) AS oc_first,
       upper(btrim(office_contact_last_name))  AS oc_last,
       count(DISTINCT md_npi)  AS distinct_prescribers,
       count(DISTINCT case_id) AS cases
FROM asnf_fraud_qc.base_active
WHERE office_contact_last_name IS NOT NULL
GROUP BY 1,2
HAVING count(DISTINCT md_npi) >= 5
ORDER BY distinct_prescribers DESC;
```

### R-220 · MD–patient address collision
**Hypothesis:** prescriber address equals patient home/ship address → self-referral or diversion.
**Logic:** normalize and compare `md_address_1`/`md_zip` against `addr1`/`addr_zip`.

```sql
SELECT case_id, md_npi, md_zip, addr_zip
FROM asnf_fraud_qc.base_active
WHERE regexp_replace(upper(btrim(md_address_1)),'[^A-Z0-9]','','g')
    = regexp_replace(upper(btrim(addr1)),      '[^A-Z0-9]','','g')
  AND addr1 IS NOT NULL;
```

### R-300 · Geographic implausibility
**Hypothesis:** prescriber, patient, and ship-to should be geographically coherent. Scatter suggests brokered enrollment or drug diversion.
**Logic:** two angles. (a) Per-case state mismatch between `md_state`, `addr_state`, and the ship destination. (b) Per-NPI *spread* — one prescriber serving patients across many states.

```sql
-- (a) case-level mismatch
SELECT case_id, md_state, addr_state, rx_facility_to_address
FROM asnf_fraud_qc.base_active
WHERE md_state IS NOT NULL AND addr_state IS NOT NULL
  AND md_state <> addr_state;

-- (b) NPI geographic spread
SELECT md_npi, count(DISTINCT addr_state) AS patient_states, count(DISTINCT case_id) AS cases
FROM asnf_fraud_qc.base_active
WHERE md_npi IS NOT NULL
GROUP BY md_npi
HAVING count(DISTINCT addr_state) >= 5;
```

Add a ZIP↔state consistency check if you can join a reference ZIP table; without one, R-300(b) captures most of the value.

### R-310 · Ship-to divergence (diversion)
**Hypothesis:** drug shipping somewhere other than the patient's home → diversion or resale.
**Logic:** compare normalized `rx_facility_to_address` against `addr1`; flag mismatches, and cluster the ship-to addresses (a ship-to used by many patients is a collection point).

```sql
SELECT rx_facility_to_address,
       count(DISTINCT patient_id) AS distinct_patients,
       count(DISTINCT case_id)    AS cases
FROM asnf_fraud_qc.base_active
WHERE rx_facility_to_address IS NOT NULL
GROUP BY rx_facility_to_address
HAVING count(DISTINCT patient_id) >= 5
ORDER BY distinct_patients DESC;
```

### R-400 · Temporal / velocity anomalies
**Hypothesis:** legitimate cases follow a verification lifecycle. Compressed or impossible timelines mean the controls were bypassed.
**Logic:** interval arithmetic on the date fields. Four distinct checks, each its own rule so they score independently:

```sql
SELECT case_id, patient_id,
  -- R-401 impossible sequencing: determined before intake
  (case_determination_date < file_receipt_date_time::date)            AS determ_before_receipt,
  -- R-402 rapid intake->ship (bypassed verification)
  (last_ship_date - file_receipt_date_time::date) < 2                 AS rapid_ship,
  -- R-403 shipped after eligibility ended
  (last_ship_date > eligibility_end_date)                            AS ship_after_elig,
  -- R-404 inverted eligibility window
  (eligibility_end_date < eligibility_start_date)                    AS inverted_window
FROM asnf_fraud_qc.base_active;
```

### R-410 · Overlapping eligibility windows (same patient)
**Hypothesis:** a patient shouldn't hold two active eligibility windows at once; overlaps indicate double-enrollment.
**Logic:** classic gaps-and-islands with `LAG` over each patient's windows ordered by start date.

```sql
WITH w AS (
  SELECT patient_id, case_id, eligibility_start_date, eligibility_end_date,
         LAG(eligibility_end_date) OVER (
           PARTITION BY patient_id ORDER BY eligibility_start_date
         ) AS prev_end
  FROM asnf_fraud_qc.base_active
  WHERE eligibility_start_date IS NOT NULL
)
SELECT patient_id, case_id, eligibility_start_date, prev_end
FROM w
WHERE eligibility_start_date <= prev_end;     -- overlap with prior window
```

### R-500 · Insurance inconsistency (eligibility contradiction)
**Hypothesis:** PAP is for the uninsured/underinsured. Active robust commercial coverage during the eligibility window suggests ineligibility or double-dipping.
**Logic:** date-range overlap between active `primary_medical_insurance_start/end` and the eligibility window; and contradiction between the `*_used_for_determination` flags and the presence of a payer.

```sql
SELECT case_id, patient_id,
       primary_medical_insurance_payer_or_insurer,
       medical_insurance_primary_status
FROM asnf_fraud_qc.base_active
WHERE medical_insurance_primary_status IN ('Active','ACTIVE')
  AND primary_medical_insurance_start_date <= eligibility_end_date
  AND COALESCE(primary_medical_insurance_end_date,'9999-12-31') >= eligibility_start_date;
```

### R-510 · Fake / degenerate contact data
**Hypothesis:** placeholder or fabricated contacts to slip past validation.
**Logic:** regex for repeating/sequential digits, and `phone_preferred = phone_alternate`.

```sql
SELECT case_id, phone_preferred, phone_alternate
FROM asnf_fraud_qc.base_active
WHERE regexp_replace(phone_preferred,'[^0-9]','','g') ~ '^(\d)\1{9}$'   -- all same digit
   OR regexp_replace(phone_preferred,'[^0-9]','','g') = regexp_replace(phone_alternate,'[^0-9]','','g');
```

---

## 2. Composite Fraud Risk Score

Don't treat every alert equally. Each detector emits weighted points into one table; the scorecard sums them per case and tiers the result.

### 2.1 Emit flags into a common structure

```sql
CREATE TABLE asnf_fraud_qc.case_flags (
  case_id     text,
  rule_id     text,
  points      int,
  evidence    jsonb,
  scored_at   timestamptz DEFAULT now()
);
```

Each detector above becomes an `INSERT ... SELECT` writing its `rule_id` and `points`. Example wrapper:

```sql
INSERT INTO asnf_fraud_qc.case_flags (case_id, rule_id, points, evidence)
SELECT case_id, 'R-120', 25,
       jsonb_build_object('fpl_reported',patient_fpl_reported,'fpl_validated',patient_fpl_validated)
FROM asnf_fraud_qc.base_active
WHERE patient_fpl_validated - patient_fpl_reported > 50;
```

### 2.2 Suggested weights

Weight by **fraud specificity** (how uniquely the signal implies intent) and **severity**, not by how easy the query was. Hard contradictions and impossibilities outweigh soft clustering.

| Rule | Signal | Points | Rationale |
|------|--------|-------:|-----------|
| R-401 | Determination before intake | 40 | Impossible sequence — data integrity or forced approval |
| R-120/121 | Income/FPL manipulation, bunching | 25 | Direct eligibility fraud, intent-bearing |
| R-500 | Active commercial insurance in window | 25 | Ineligibility / double-dipping |
| R-100/110 | Identity stitching / ring | 25 | Program-cap evasion |
| R-403 | Ship after eligibility end | 20 | Waste + control failure |
| R-210 | Office contact → many NPIs | 20 | Enrollment-mill signature |
| R-310 | Ship-to divergence / collection point | 20 | Diversion risk |
| R-200 | Prescriber velocity outlier | 15 | Concentration, needs corroboration |
| R-122 | Net-worth / LIS contradiction | 15 | Eligibility inconsistency |
| R-300 | Geographic implausibility | 10 | Weak alone, strong in combination |
| R-402 | Rapid intake→ship | 10 | Suggestive, not conclusive |
| R-510 | Fake contact data | 10 | Often data quality, not fraud |
| baseline | Address / phone >5 clustering | 10 | High false-positive rate solo |

### 2.3 Scorecard + tiers

```sql
CREATE OR REPLACE VIEW asnf_fraud_qc.case_scorecard AS
SELECT case_id,
       sum(points)                          AS risk_score,
       count(*)                             AS rules_fired,
       array_agg(DISTINCT rule_id ORDER BY rule_id) AS rules,
       CASE
         WHEN sum(points) >= 60 THEN 'CRITICAL'
         WHEN sum(points) >= 40 THEN 'HIGH'
         WHEN sum(points) >= 20 THEN 'MEDIUM'
         ELSE 'LOW'
       END                                  AS tier
FROM asnf_fraud_qc.case_flags
GROUP BY case_id;
```

**Multi-rule bonus.** Independent signals converging is far more damning than one loud rule. Apply a corroboration multiplier so a case firing 3+ distinct vectors escalates even if individual points are modest:

```sql
-- in the scorecard: multiply by 1.3 when >=3 distinct rule families fire
sum(points) * CASE WHEN count(DISTINCT left(rule_id,3)) >= 3 THEN 1.3 ELSE 1.0 END
```

### 2.4 Calibration path

Start with these heuristic weights, but treat them as a v1. Once you have a labelled set of investigator-confirmed outcomes, back-test: for each rule, compute precision (confirmed-fraud rate among cases it flagged) and re-weight toward the high-precision rules. The natural end state is a logistic regression / gradient-boosted model over the same flag features — the flag table you're already building *is* the feature matrix, so nothing is wasted. Route only CRITICAL/HIGH to manual audit initially and use MEDIUM as a sampling pool to keep measuring your false-positive rate.

---

## Implementation notes

- Run detectors as scheduled jobs writing to `case_flags` with a run timestamp; truncate-and-reload per extract cycle (the "Tuesday full extract" cadence).
- Keep each rule's threshold in a small config table, not hardcoded, so tuning doesn't require a code change.
- Log evidence as `jsonb` on every flag — investigators need the *why*, and it makes precision back-testing trivial.
- Watch NULL semantics: the baseline `NOT IN` NULL trap applies here too. Prefer `NOT EXISTS` / explicit `IS NOT NULL` guards in any anti-join detector.
