# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/chad.t.vanorsdale.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,Stage 1: Source references and full-batch profile
# Your investigation here.
#Schema: to6_challenge3_inputs
#  Description: Synthetic data that includes parsed source data, return data, and source lineage data
#  Tables:
#      returns_initial
#        Description: Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output.

#      returns_corrected
#        Description: Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness.

#      source_lineage
#        Description: Source data lineage

#      parsed_source
#        Description: Synthetic parsed source: one row is one return; return_id identifies the return, taxpayer_id identifies a filer who may recur across years. Nonnegative amounts; zero income is valid.

#Returns_Initial Sample Data Rows:
#return_id	taxpayer_id	tax_year	income	deduction
#R01	F0	2022	100	20
#R01	F0	2022	100	20
#R02	F1	2022	null	33
#R03	F2	2022	0	46
#R04	F3	2022	400	59
#R05	F0	2023	500	72
#R06	F1	2023	600	85
#R07	F2	2023	700	98
#R08	F3	2023	800	111
#R09	F0	2024	0	124
#R10	F1	2024	1000	137
#R11	F2	2024	1100	150
#R12	F3	2024	1200	163

#Returns_Corrected Sample Rows:
#    return_id	taxpayer_id	    tax_year	income	    deduction
#    R01	        F0	            2022	    100	        20
#    R02	        F1	            2022	    200	        33
#    R03	        F2	            2022	    0	          46

#Lineage:
#  Returns_initial comes from parsed_source and returns_initial from to6_inputs_20261115
#  Returns_corrected comes from to6_inputs_20261115 and to6_challenge3_inputs

#Returns_initial has more data than corrected so some cleanup happened possibly due to duplicates
#Returns_initial has duplicate return_id of 01 that was corrected in returns_corrected
#Returns_initial has income as nulls instead of 0 that need to be corrected in returns_corrected
#Returns_initial and returns_corrected has tax year distribution of 2022 to 2024

# ---------------------------------------------------------------------------
# Stage 1 — Full-batch profile (queries + results kept in the notebook and in
# outputs/input-profile.json). Observations only; the quality checks that
# decide which batch is usable are in stage 2.
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F
import json

PROFILE_REQUIRED = ["return_id", "tax_year", "income", "deduction"]

# Source references: supplied lineage view and Catalog table comments
lineage = spark.table(LINEAGE_VIEW)
display(lineage)
table_comments = {
    t: spark.sql(f"DESCRIBE TABLE EXTENDED {t}")
        .where(F.col("col_name") == "Comment").select("data_type").first()["data_type"]
    for t in [INITIAL_TABLE, CORRECTED_TABLE, f"{INPUT_SCHEMA}.parsed_source"]
}

# At least three sample rows of each batch
for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    print(f"sample rows: {t}")
    display(spark.table(t).orderBy("return_id").limit(3))


def profile_batch(table_name):
    """Summarize the FULL batch: returns by year, missing required values,
    duplicate return IDs and zero-income records."""
    df = spark.table(table_name)
    by_year = (df.groupBy("tax_year")
                 .agg(F.count("*").alias("rows"),
                      F.countDistinct("return_id").alias("distinct_return_ids"))
                 .orderBy("tax_year"))
    missing = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in PROFILE_REQUIRED])
    duplicates = (df.groupBy("return_id").count().where(F.col("count") > 1)
                    .withColumnRenamed("count", "occurrences").orderBy("return_id"))
    zero_income = df.where(F.col("income") == 0).orderBy("return_id")
    negative_amounts = df.where((F.col("income") < 0) | (F.col("deduction") < 0)).count()

    print(f"===== {table_name}: {df.count()} rows, "
          f"{df.select('return_id').distinct().count()} distinct return_id =====")
    print("returns by year"); display(by_year)
    print("missing required values (null count per column)"); display(missing)
    print("duplicate return IDs"); display(duplicates)
    print("zero-income records (valid data)"); display(zero_income)
    print(f"records with a negative income or deduction: {negative_amounts}")

    return {
        "table": table_name,
        "catalog_comment": table_comments.get(table_name),
        "total_rows": df.count(),
        "distinct_return_ids": df.select("return_id").distinct().count(),
        "returns_by_year": [r.asDict() for r in by_year.collect()],
        "missing_required_values": missing.first().asDict(),
        "records_with_missing_required_values": [
            r.asDict() for r in df.where(" OR ".join(f"{c} IS NULL" for c in PROFILE_REQUIRED))
                                  .orderBy("return_id").collect()],
        "duplicate_return_ids": [r.asDict() for r in duplicates.collect()],
        "zero_income_return_ids": [r["return_id"] for r in zero_income.collect()],
        "negative_amount_records": negative_amounts,
    }


profiles = [profile_batch(INITIAL_TABLE), profile_batch(CORRECTED_TABLE)]

# Side-by-side overview of both batches
display(spark.createDataFrame(
    [(p["table"], p["total_rows"], p["distinct_return_ids"],
      sum(p["missing_required_values"].values()), len(p["duplicate_return_ids"]),
      len(p["zero_income_return_ids"])) for p in profiles],
    "table STRING, total_rows INT, distinct_return_ids INT, missing_required_values INT, "
    "duplicated_return_ids INT, zero_income_records INT",
))

# Persist the profile so a reviewer can see the results outside the notebook
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs" / "input-profile.json").write_text(
    json.dumps({"lineage": [r.asDict() for r in lineage.collect()],
                "catalog_comments": table_comments, "profiles": profiles},
               indent=2, default=str) + "\n",
    encoding="utf-8",
)
print("wrote outputs/input-profile.json")

# COMMAND ----------

# DBTITLE 1,Stage 1: Findings that shape the next steps
# MAGIC %md
# MAGIC ### Stage 1 findings and how they affect the next steps
# MAGIC **Source reference:** both deliveries come from `training.to6_challenge3_inputs.parsed_source` (supplied `source_lineage` view). One row is one return; `return_id` is the uniqueness identifier; `taxpayer_id` is the filer and may recur across years.
# MAGIC
# MAGIC | Finding (full batch, see profile above and `outputs/input-profile.json`) | Effect on the next stage |
# MAGIC | --- | --- |
# MAGIC | `returns_initial`: 13 rows but 12 distinct `return_id`; R01 occurs twice | Stage 2 uniqueness check on `return_id` must reject this batch and keep both R01 rows as evidence |
# MAGIC | `returns_initial`: R02 has a null `income` (a required value) | Stage 2 completeness check must reject it; the null is **not** replaced with 0 |
# MAGIC | `returns_corrected`: 12 rows, 12 distinct IDs, no missing required values | Expected to pass the same Stage 2 rules and become the batch for the final run |
# MAGIC | Zero income in R03 (2022) and R09 (2024); zero income is valid | Stage 3 keeps these returns with a null `deduction_to_income` instead of dividing by zero |
# MAGIC | 4 returns per year for 2022, 2023, 2024 | Stage 4 split: train = 2022, validation = 2023, test = 2024; every year is covered |
# MAGIC | No negative income or deduction values | Consistent with the Catalog's nonnegative-amounts statement |
# MAGIC
# MAGIC These observations do not replace the Stage 2 quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,Stage 2: Completeness and uniqueness checks (batch decision)
# Stage 2 — Decide which batch is usable.
#
# Rules from the task's data contract (CHALLENGE.md, "Supplied data"):
#   - Completeness: required values are return_id, tax_year, income and deduction.
#     (taxpayer_id is the filer identifier — present in the batch, but not a
#     required value for this contract.)
#   - Uniqueness: each return must occur exactly once, keyed by return_id.
#
# Rejection handling: the affected records are preserved with the reason(s) for
# rejection. Unknown amounts are NOT replaced with zero and no record is
# silently discarded — rejecting the batch, not patching it, is the correction.

from pyspark.sql import functions as F
import json

REQUIRED_COLUMNS = ["return_id", "tax_year", "income", "deduction"]
UNIQUENESS_COLUMN = "return_id"


def assess_batch(table_name):
    """Apply the same completeness and uniqueness rules to one delivered batch.

    Returns (rows, evidence, decision): evidence holds every record that
    violates a rule together with the reason(s); decision is REJECTED when
    any record violates, otherwise ACCEPTED.
    """
    batch = spark.table(table_name)
    row_counts = batch.groupBy(UNIQUENESS_COLUMN).agg(F.count("*").alias("row_count"))
    evidence = (
        batch.join(row_counts, UNIQUENESS_COLUMN, "left")
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("row_count") > 1,
                    F.concat(
                        F.lit(f"duplicate {UNIQUENESS_COLUMN}: occurs "),
                        F.col("row_count").cast("string"),
                        F.lit(" times; each return must occur exactly once"),
                    ),
                ),
                *[
                    F.when(F.col(c).isNull(), F.lit(f"missing required value: {c}"))
                    for c in REQUIRED_COLUMNS
                ],
            ),
        )
        .where(F.col("rejection_reason") != "")
        .select(*batch.columns, "row_count", "rejection_reason")
    )
    rows, violations = batch.count(), evidence.count()
    return rows, evidence, ("REJECTED" if violations else "ACCEPTED")


# 1) Assess the initial delivery — it must be rejected before feature output.
initial_rows, initial_evidence, initial_decision = assess_batch(INITIAL_TABLE)
print(f"returns_initial: {initial_rows} rows -> {initial_decision}")
display(initial_evidence.orderBy(UNIQUENESS_COLUMN, "row_count"))

# 2) Assess the replacement with the same rules.
corrected_rows, corrected_evidence, corrected_decision = assess_batch(CORRECTED_TABLE)
print(f"returns_corrected: {corrected_rows} rows -> {corrected_decision} "
      f"({corrected_evidence.count()} records violate the rules)")
display(corrected_evidence)

# 3) Decision summary for both batches.
display(spark.createDataFrame(
    [
        (INITIAL_TABLE, initial_rows, initial_evidence.count(), initial_decision),
        (CORRECTED_TABLE, corrected_rows, corrected_evidence.count(), corrected_decision),
    ],
    "table STRING, rows INT, records_violating_rules INT, decision STRING",
))

# 4) Explain the failure and the next action using the actual records and rules.
offending = initial_evidence.orderBy(UNIQUENESS_COLUMN, "row_count").collect()
for record in offending:
    print(f"Rejected record {record[UNIQUENESS_COLUMN]}: {record['rejection_reason']}")
next_action = (
    f"Next action: no feature output is written from {INITIAL_TABLE}; the affected records "
    "and reasons are preserved (displayed above and in outputs/initial-rejection-evidence.json). "
    "The null income was not replaced with zero and no record was silently discarded. "
)
if corrected_decision == "ACCEPTED":
    next_action += (f"{CORRECTED_TABLE} passed the same checks, so it is the batch "
                    "used for the final run (stage 3).")
else:
    next_action += (f"{CORRECTED_TABLE} also failed the checks, so no batch is usable; "
                    "stop before feature output.")
print(next_action)

# 5) Preserve the rejection evidence durably in the Git folder.
offending_records = [{"source_table": INITIAL_TABLE, **r.asDict()} for r in offending]
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs" / "initial-rejection-evidence.json").write_text(
    json.dumps(offending_records, indent=2) + "\n", encoding="utf-8"
)

# 6) Gate for stage 3: features may only be built from an ACCEPTED batch.
assert initial_decision == "REJECTED", "the initial batch was expected to fail the checks"
USABLE_TABLE = CORRECTED_TABLE if corrected_decision == "ACCEPTED" else None
print(f"Batch for the final run: {USABLE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Stage 3: Save and verify return_features
import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]

# ---------------------------------------------------------------------------
# Stage 3 — Deliver a checked feature table: return_features
#
# Data contract (CHALLENGE.md, outcome 3):
#   - exactly the columns return_id, tax_year, deduction_to_income
#   - deduction_to_income = deduction / income; a zero-income return stays in
#     the table with a null ratio (zero income is valid, the ratio is empty)
#   - every accepted return kept exactly once; the filer identifier
#     (taxpayer_id) and the raw amounts (income, deduction) are excluded
#     under the course's need-to-know rule
#   - written only from the ACCEPTED batch decided in stage 2

from pyspark.sql import functions as F
import math

assert "USABLE_TABLE" in globals() and USABLE_TABLE == CORRECTED_TABLE, (
    "run stage 2 first: features may only be written from an ACCEPTED batch"
)

accepted = spark.table(USABLE_TABLE)  # returns_corrected: every return exactly once

features = (
    accepted
    .withColumn(
        "deduction_to_income",
        F.when(F.col("income") > 0, F.col("deduction") / F.col("income")),
        # income == 0 -> null ratio; the return itself is still kept
    )
    .select("return_id", "tax_year", "deduction_to_income")
)

# Save (overwrite keeps repeat runs identical, with no appended copies)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
features.write.mode("overwrite").saveAsTable(FEATURES_TABLE)
print(f"saved {FEATURES_TABLE}: {features.count()} returns")

# --- Verification: read the saved table back from the Catalog ---
readback = spark.table(FEATURES_TABLE)
display(readback.orderBy("return_id"))

# 1) Schema check: exactly the three contract columns (need-to-know rule:
#    no filer identifier, no raw amounts)
assert readback.columns == ["return_id", "tax_year", "deduction_to_income"], readback.columns

# 2) Every accepted return kept exactly once
assert readback.count() == accepted.count(), (readback.count(), accepted.count())
assert readback.groupBy("return_id").count().where(F.col("count") > 1).count() == 0
assert readback.where("return_id IS NULL OR tax_year IS NULL").count() == 0

# 3) Nonzero result verified against an expected value derived from the source
check_id = "R01"
src = accepted.where(F.col("return_id") == check_id).first()
expected_ratio = src["deduction"] / src["income"]
saved = readback.where(F.col("return_id") == check_id).first()
assert math.isclose(saved["deduction_to_income"], expected_ratio, rel_tol=1e-9), (
    saved["deduction_to_income"], expected_ratio
)
print(f"nonzero check: {check_id} deduction {src['deduction']} / income {src['income']} "
      f"= {expected_ratio}; saved value {saved['deduction_to_income']} matches")

# 4) Zero-income case: kept with a null ratio
zero_income_ids = [r["return_id"] for r in accepted.where(F.col("income") == 0).collect()]
for rid in zero_income_ids:
    row = readback.where(F.col("return_id") == rid).first()
    assert row is not None and row["deduction_to_income"] is None, rid
    print(f"zero-income return {rid} kept in the saved table with a null ratio")

print(f"all checks passed: {FEATURES_TABLE} holds {readback.count()} returns, one row each, "
      "columns exactly [return_id, tax_year, deduction_to_income]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Stage 4: Year tables, rerun evidence, exports and handoff
# Stage 4 — Make the handoff usable.
#
# From the SAVED features, split by tax year: features_train = 2022,
# features_validation = 2023, features_test = 2024. Every accepted return
# lands in exactly one table, in its assigned year. Tables are overwritten on
# each run (no appended copies), and every run's actual return IDs and values
# are kept in outputs/run-evidence.json so two complete successful runs can be
# compared. The handoff is written to SUBMISSION.md.

from pyspark.sql import functions as F
from datetime import datetime, timezone
import json

# 1) Split the saved features by year (read back from the Catalog)
saved_features = spark.table(FEATURES_TABLE)
YEAR_TABLES = {
    2022: f"{OUTPUT_SCHEMA}.features_train",
    2023: f"{OUTPUT_SCHEMA}.features_validation",
    2024: f"{OUTPUT_SCHEMA}.features_test",
}
unassigned = [
    r["tax_year"]
    for r in saved_features.select("tax_year").distinct()
    .where(~F.col("tax_year").isin(list(YEAR_TABLES))).collect()
]
assert not unassigned, f"returns outside the assigned years: {unassigned}"

for year, table in YEAR_TABLES.items():
    saved_features.where(F.col("tax_year") == year).write.mode("overwrite").saveAsTable(table)

# 2) Read the three saved tables back and verify the split
split_frames = {}
for year, table in YEAR_TABLES.items():
    frame = spark.table(table)
    assert frame.where(F.col("tax_year") != year).count() == 0, table
    print(f"{table}: {frame.count()} returns, all tax_year = {year}")
    split_frames[table] = frame

combined = None
for frame in split_frames.values():
    combined = frame if combined is None else combined.unionByName(frame)
assert combined.count() == saved_features.count() == combined.select("return_id").distinct().count(), (
    "every accepted return must appear exactly once across the three tables"
)
feature_ids = {r["return_id"] for r in saved_features.select("return_id").collect()}
split_ids = {r["return_id"] for r in combined.select("return_id").collect()}
assert feature_ids == split_ids, "the split tables must hold exactly the accepted returns"
print(f"split verified: {len(split_ids)} returns, each in exactly one table, in its assigned year")

# 3) Keep this run's actual IDs and values; compare the latest two complete runs
def rows(frame):
    return [[r["return_id"], r["tax_year"], r["deduction_to_income"]]
            for r in frame.orderBy("return_id").collect()]

try:
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    workspace_url = None
notebook_path = "/Workspace/Users/chad.t.vanorsdale.irs@catalyte.ai/to6-databricks-challenge-starter/notebook"
NOTEBOOK_ID = "127024866557542"  # workspace object id of this notebook
run_record = {
    "run_number": None,  # filled in below
    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "notebook_url": (f"https://{workspace_url}/editor/notebooks/{NOTEBOOK_ID}"
                     if workspace_url else notebook_path),
    "notebook_path": notebook_path,
    "stages_run": "all stages (Run all): profile, batch decision, return_features, year split",
    "tables": {
        FEATURES_TABLE: rows(saved_features),
        **{table: rows(frame) for table, frame in split_frames.items()},
    },
}
evidence_path = repo_root / "outputs" / "run-evidence.json"
runs = json.loads(evidence_path.read_text(encoding="utf-8")) if evidence_path.exists() else []
for i, earlier in enumerate(runs, start=1):  # older records were saved before run numbers existed
    earlier.setdefault("run_number", i)
run_record["run_number"] = len(runs) + 1
runs.append(run_record)
evidence_path.write_text(json.dumps(runs, indent=2) + "\n", encoding="utf-8")

# 3b) Side-by-side comparison of the latest two complete runs, row by row
comparison_path = repo_root / "outputs" / "run-comparison.csv"
runs_match = None
if len(runs) >= 2:
    run_a, run_b = runs[-2], runs[-1]
    comparison_rows = []
    for table in sorted(set(run_a["tables"]) | set(run_b["tables"])):
        a = {r[0]: r for r in run_a["tables"].get(table, [])}
        b = {r[0]: r for r in run_b["tables"].get(table, [])}
        for rid in sorted(set(a) | set(b)):
            ra, rb = a.get(rid), b.get(rid)
            comparison_rows.append({
                "table": table,
                "return_id": rid,
                f"run{run_a['run_number']}_tax_year": ra[1] if ra else None,
                f"run{run_b['run_number']}_tax_year": rb[1] if rb else None,
                f"run{run_a['run_number']}_deduction_to_income": ra[2] if ra else None,
                f"run{run_b['run_number']}_deduction_to_income": rb[2] if rb else None,
                "match": ra == rb,
            })
    # equal row counts per table as well (no appended copies)
    counts_match = all(len(run_a["tables"][t]) == len(run_b["tables"].get(t, []))
                       for t in run_a["tables"])
    runs_match = counts_match and all(r["match"] for r in comparison_rows)
    import pandas as pd
    comparison_df = pd.DataFrame(comparison_rows)
    comparison_df.to_csv(comparison_path, index=False)
    display(comparison_df)
    summary_line = (
        f"run {run_a['run_number']} ({run_a['timestamp_utc']}) vs run {run_b['run_number']} "
        f"({run_b['timestamp_utc']}): {sum(r['match'] for r in comparison_rows)}/"
        f"{len(comparison_rows)} rows match across {len(run_a['tables'])} tables -> "
        + ("IDENTICAL return IDs and values" if runs_match else "DIFFERENT — investigate")
    )
    print(summary_line)
else:
    summary_line = "first recorded run; rerun all stages to compare two complete successful runs"
    print(summary_line)

# 4) Small synthetic exports of the saved tables
for name, frame in [(FEATURES_TABLE, saved_features), *split_frames.items()]:
    export_path = repo_root / "outputs" / f"{name.split('.')[-1]}.csv"
    frame.orderBy("return_id").toPandas().to_csv(export_path, index=False)
    print(f"exported {export_path.name}")

# 5) Handoff: SUBMISSION.md — built from this run's actual results
def _profile_line(p):
    years = ", ".join(f"{y['tax_year']}: {y['rows']}" for y in p["returns_by_year"])
    missing = ", ".join(f"{c}={n}" for c, n in p["missing_required_values"].items())
    dups = ", ".join(f"{d['return_id']} x{d['occurrences']}" for d in p["duplicate_return_ids"]) or "none"
    zeros = ", ".join(p["zero_income_return_ids"]) or "none"
    return (f"| `{p['table']}` | {p['total_rows']} | {p['distinct_return_ids']} | {years} | "
            f"{missing} | {dups} | {zeros} |")

profile_table = "\n".join(
    ["| Batch | Rows | Distinct return_id | Returns by year | Missing required values | "
     "Duplicate return_id | Zero-income returns |",
     "| --- | --- | --- | --- | --- | --- | --- |"]
    + [_profile_line(p) for p in profiles]
)
rejection_table = "\n".join(
    ["| return_id | tax_year | income | deduction | Reason |", "| --- | --- | --- | --- | --- |"]
    + [f"| {r['return_id']} | {r['tax_year']} | {r['income']} | {r['deduction']} | "
       f"{r['rejection_reason']} |" for r in offending_records]
)
split_lines = "\n".join(
    f"- `{t}`: {len(run_record['tables'][t])} returns "
    f"({', '.join(r[0] for r in run_record['tables'][t])})"
    for t in YEAR_TABLES.values()
)
run_lines = "\n".join(
    f"- Run {r['run_number']}: {r['timestamp_utc']} — {r['notebook_url']} ({r.get('stages_run', 'all stages')})"
    for r in runs[-2:]
)

submission = f"""# Handoff — checked feature dataset (to6 challenge 3)

## Inputs
- `training.to6_challenge3_inputs.returns_initial` — initial delivery, REJECTED by the checks below
- `training.to6_challenge3_inputs.returns_corrected` — replacement delivery, ACCEPTED, used for the final run
- `training.to6_challenge3_inputs.parsed_source` — upstream source named by the supplied lineage view
- `training.to6_challenge3_inputs.source_lineage` — supplied lineage view

## Outputs
- `{FEATURES_TABLE}` — return_id, tax_year, deduction_to_income; every accepted return once
- `{YEAR_TABLES[2022]}` — tax_year 2022
- `{YEAR_TABLES[2023]}` — tax_year 2023
- `{YEAR_TABLES[2024]}` — tax_year 2024

## Ratio definition
`deduction_to_income = deduction / income`. A zero-income return keeps a null ratio
(zero income itself is valid data); nothing is coerced to zero.

## Checks performed
- Stage 2 — completeness (required values: return_id, tax_year, income, deduction) and
  uniqueness (each return exactly once) applied to both batches. Initial batch REJECTED
  (R01 duplicated; R02 missing income); affected records and reasons preserved in
  `outputs/initial-rejection-evidence.json`. Corrected batch ACCEPTED under the same rules.
- Stage 3 — features written only from the ACCEPTED batch; columns exactly
  [return_id, tax_year, deduction_to_income] (filer identifier and raw amounts excluded);
  nonzero ratio verified against the source (R01: 20/100 = 0.2); zero-income returns
  (R03, R09) present with null ratios; saved table read back from the Catalog.
- Stage 4 — every accepted return in exactly one year table, in its assigned year;
  each run's actual IDs and values recorded in `outputs/run-evidence.json` and the latest
  two complete runs compared; tables are overwritten each run (no appended copies).

## Results

### Stage 1 — full-batch profile (`outputs/input-profile.json`)
{profile_table}

### Stage 2 — batch decision
- `{INITIAL_TABLE}`: {initial_rows} rows -> **{initial_decision}**
- `{CORRECTED_TABLE}`: {corrected_rows} rows -> **{corrected_decision}** (same rules)

Affected initial records (`outputs/initial-rejection-evidence.json`):

{rejection_table}

### Stage 3 — `return_features` readback
- {saved_features.count()} returns, one row each; columns exactly `return_id, tax_year, deduction_to_income`
- Nonzero check: {check_id} = {src['deduction']} / {src['income']} = {expected_ratio} (saved value matches)
- Zero-income returns kept with null ratio: {', '.join(zero_income_ids)}

### Stage 4 — year split and repeat runs
{split_lines}
- {summary_line}

## Run references
{run_lines}

Full per-run IDs and values: `outputs/run-evidence.json`; row-by-row side-by-side
comparison: `outputs/run-comparison.csv`.

## Final exports (this Databricks Git folder)
- `notebook.py` — notebook source
- `SUBMISSION.md` — this handoff
- `outputs/input-profile.json` — stage 1 full-batch profile of both batches and lineage
- `outputs/initial-rejection-evidence.json` — rejected records with reasons
- `outputs/return_features.csv`, `outputs/features_train.csv`,
  `outputs/features_validation.csv`, `outputs/features_test.csv`
- `outputs/run-evidence.json` — per-run timestamps, notebook URL, IDs and values
- `outputs/run-comparison.csv` — side-by-side comparison of the two complete runs

## Could not verify
- The supplied `source_lineage` view is the only lineage evidence; its own description
  notes it does not claim a Catalog UI lineage check.
- Both compared runs executed in this interactive notebook on the attached compute; no
  separate Jobs run IDs exist for them.
"""
(repo_root / "SUBMISSION.md").write_text(submission, encoding="utf-8")
print("wrote SUBMISSION.md")