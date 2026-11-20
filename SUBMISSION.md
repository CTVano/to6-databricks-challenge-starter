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
  each run's actual IDs and values recorded in `outputs/run-evidence.json` and the latest
  two complete runs compared; tables are overwritten each run (no appended copies).

## Results

### Stage 1 — full-batch profile (`outputs/input-profile.json`)
| Batch | Rows | Distinct return_id | Returns by year | Missing required values | Duplicate return_id | Zero-income returns |
| --- | --- | --- | --- | --- | --- | --- |
| `training.to6_challenge3_inputs.returns_initial` | 13 | 12 | 2022: 5, 2023: 4, 2024: 4 | return_id=0, tax_year=0, income=1, deduction=0 | R01 x2 | R03, R09 |
| `training.to6_challenge3_inputs.returns_corrected` | 12 | 12 | 2022: 4, 2023: 4, 2024: 4 | return_id=0, tax_year=0, income=0, deduction=0 | none | R03, R09 |

### Stage 2 — batch decision
- `training.to6_challenge3_inputs.returns_initial`: 13 rows -> **REJECTED**
- `training.to6_challenge3_inputs.returns_corrected`: 12 rows -> **ACCEPTED** (same rules)

Affected initial records (`outputs/initial-rejection-evidence.json`):

| return_id | tax_year | income | deduction | Reason |
| --- | --- | --- | --- | --- |
| R01 | 2022 | 100 | 20 | duplicate return_id: occurs 2 times; each return must occur exactly once |
| R01 | 2022 | 100 | 20 | duplicate return_id: occurs 2 times; each return must occur exactly once |
| R02 | 2022 | None | 33 | missing required value: income |

### Stage 3 — `return_features` readback
- 12 returns, one row each; columns exactly `return_id, tax_year, deduction_to_income`
- Nonzero check: R01 = 20 / 100 = 0.2 (saved value matches)
- Zero-income returns kept with null ratio: R03, R09

### Stage 4 — year split and repeat runs
- `training.to6_c3_562d757e9b79.features_train`: 4 returns (R01, R02, R03, R04)
- `training.to6_c3_562d757e9b79.features_validation`: 4 returns (R05, R06, R07, R08)
- `training.to6_c3_562d757e9b79.features_test`: 4 returns (R09, R10, R11, R12)
- run 4 (2026-11-20T18:56:21.338658+00:00) vs run 5 (2026-11-20T18:57:53.426139+00:00): 24/24 rows match across 4 tables -> IDENTICAL return IDs and values

## Run references
- Run 4: 2026-11-20T18:56:21.338658+00:00 — https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557542 (all stages (Run all): profile, batch decision, return_features, year split)
- Run 5: 2026-11-20T18:57:53.426139+00:00 — https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866557542 (all stages (Run all): profile, batch decision, return_features, year split)

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
