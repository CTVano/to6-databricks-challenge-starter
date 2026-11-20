# Handoff — checked feature dataset (to6 challenge 3)

## Inputs
- `training.to6_challenge3_inputs.returns_initial` — initial delivery, REJECTED by the checks below
- `training.to6_challenge3_inputs.returns_corrected` — replacement delivery, ACCEPTED, used for the final run
- `training.to6_challenge3_inputs.parsed_source` — upstream source named by the supplied lineage view
- `training.to6_challenge3_inputs.source_lineage` — supplied lineage view

## Outputs
- `training.to6_c3_562d757e9b79.return_features` — return_id, tax_year, deduction_to_income; every accepted return once
- `training.to6_c3_562d757e9b79.features_train` — tax_year 2022
- `training.to6_c3_562d757e9b79.features_validation` — tax_year 2023
- `training.to6_c3_562d757e9b79.features_test` — tax_year 2024

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
  each complete run exports its tables to its own folder `outputs/runs/run-<n>/`, and the
  two complete runs are compared from those committed files (row by row and by SHA-256
  checksum); tables are overwritten each run (no appended copies).

## Results

### Stage 1 — full-batch profile
Source file: `outputs/input-profile.json`

| Batch | Rows | Distinct return_id | Returns by year | Missing required values | Duplicate return_id | Zero-income returns |
| --- | --- | --- | --- | --- | --- | --- |
| `training.to6_challenge3_inputs.returns_initial` | 13 | 12 | 2022: 5, 2023: 4, 2024: 4 | return_id=0, tax_year=0, income=1, deduction=0 | R01 x2 | R03, R09 |
| `training.to6_challenge3_inputs.returns_corrected` | 12 | 12 | 2022: 4, 2023: 4, 2024: 4 | return_id=0, tax_year=0, income=0, deduction=0 | none | R03, R09 |

### Stage 2 — batch decision
Source file: `outputs/initial-rejection-evidence.json` (rejection run reference + affected records)

- `training.to6_challenge3_inputs.returns_initial`: 13 rows -> **REJECTED**
- `training.to6_challenge3_inputs.returns_corrected`: 12 rows -> **ACCEPTED** (same rules)

Affected initial records:

| return_id | tax_year | income | deduction | Reason |
| --- | --- | --- | --- | --- |
| R01 | 2022 | 100 | 20 | duplicate return_id: occurs 2 times; each return must occur exactly once |
| R01 | 2022 | 100 | 20 | duplicate return_id: occurs 2 times; each return must occur exactly once |
| R02 | 2022 | null | 33 | missing required value: income |

### Stage 3 — `return_features` readback
Source file: `outputs/return_features.csv` (same content as `outputs/runs/run-7/return_features.csv`)

- 12 returns, one row each; columns exactly `return_id, tax_year, deduction_to_income`
- Nonzero check: R01 = 20 / 100 = 0.2 (saved value matches)
- Zero-income returns kept with null ratio: R03, R09

Saved values (as exported):

| return_id | tax_year | deduction_to_income | Year table |
| --- | --- | --- | --- |
| R01 | 2022 | 0.2 | `features_train` |
| R02 | 2022 | 0.165 | `features_train` |
| R03 | 2022 | null | `features_train` |
| R04 | 2022 | 0.1475 | `features_train` |
| R05 | 2023 | 0.144 | `features_validation` |
| R06 | 2023 | 0.14166666666666666 | `features_validation` |
| R07 | 2023 | 0.14 | `features_validation` |
| R08 | 2023 | 0.13875 | `features_validation` |
| R09 | 2024 | null | `features_test` |
| R10 | 2024 | 0.137 | `features_test` |
| R11 | 2024 | 0.13636363636363635 | `features_test` |
| R12 | 2024 | 0.13583333333333333 | `features_test` |

### Stage 4 — year split and repeat runs
Source files: `outputs/features_train.csv`, `outputs/features_validation.csv`,
`outputs/features_test.csv`; comparison in `outputs/run-comparison.csv`

- `training.to6_c3_562d757e9b79.features_train`: 4 returns (R01, R02, R03, R04) — `outputs/features_train.csv`
- `training.to6_c3_562d757e9b79.features_validation`: 4 returns (R05, R06, R07, R08) — `outputs/features_validation.csv`
- `training.to6_c3_562d757e9b79.features_test`: 4 returns (R09, R10, R11, R12) — `outputs/features_test.csv`

**Repeat-run result:** run 6 (2026-11-20T19:24:36.340055+00:00) vs run 7 (2026-11-20T19:26:16.607638+00:00), compared from the export files `outputs/runs/run-6/` and `outputs/runs/run-7/`: 24/24 rows match and 4/4 files have identical SHA-256 checksums -> IDENTICAL return IDs and values

| Export file | Run 6 SHA-256 | Run 7 SHA-256 | Identical |
| --- | --- | --- | --- |
| `return_features.csv` | `382f97d69814056f…` | `382f97d69814056f…` | yes |
| `features_train.csv` | `85edc0fe84a53438…` | `85edc0fe84a53438…` | yes |
| `features_validation.csv` | `4f997086def1a1c9…` | `4f997086def1a1c9…` | yes |
| `features_test.csv` | `f133daffb5d300f2…` | `f133daffb5d300f2…` | yes |

## Run references

| Run | Timestamp (UTC) | Notebook | Result | Evidence file |
| --- | --- | --- | --- | --- |
| Rejection of `training.to6_challenge3_inputs.returns_initial` (Stage 2, run 7) | 2026-11-20T19:25:46.154097+00:00 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557542 | REJECTED — 3 affected records | `outputs/initial-rejection-evidence.json` |
| Complete successful run 6 | 2026-11-20T19:24:36.340055+00:00 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557542 | all stages succeeded | `outputs/runs/run-6/` |
| Complete successful run 7 | 2026-11-20T19:26:16.607638+00:00 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557542 | all stages succeeded | `outputs/runs/run-7/` |

All runs, with their actual IDs and values, are in `outputs/run-evidence.json`. Only the two
complete runs above are compared. Earlier records in that file are labelled and not used:

- Run 1 (2026-11-20T16:59:53.730390+00:00): superseded: earlier notebook version, before per-run export snapshots
- Run 2 (2026-11-20T17:00:47.830635+00:00): superseded: earlier notebook version, before per-run export snapshots
- Run 3 (2026-11-20T18:54:49.577970+00:00): failed: Stage 4 raised KeyError after this record was written; not a complete run
- Run 4 (2026-11-20T18:56:21.338658+00:00): superseded: complete run of an earlier notebook version, before per-run export snapshots
- Run 5 (2026-11-20T18:57:53.426139+00:00): superseded: complete run of an earlier notebook version, before per-run export snapshots

## Final exports (this Databricks Git folder)
- `notebook.py` — notebook source
- `SUBMISSION.md` — this handoff
- `outputs/input-profile.json` — Stage 1 full-batch profile of both batches and lineage
- `outputs/initial-rejection-evidence.json` — rejection run reference, rejected records and reasons
- `outputs/return_features.csv`, `outputs/features_train.csv`,
  `outputs/features_validation.csv`, `outputs/features_test.csv` — latest exports
- `outputs/runs/run-6/` and `outputs/runs/run-7/` — the same four exports
  from each of the two compared complete runs
- `outputs/run-comparison.csv` — row-by-row comparison of the two runs' export files
- `outputs/run-evidence.json` — every run's timestamp, notebook URL, status, IDs, values and checksums

## Could not verify
- The supplied `source_lineage` view is the only lineage evidence; its own description
  notes it does not claim a Catalog UI lineage check.
- Both compared runs executed in this interactive notebook on the attached compute; no
  separate Jobs run IDs exist for them.
