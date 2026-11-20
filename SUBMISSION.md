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

## Run references
- Per-run timestamps and notebook URL: `outputs/run-evidence.json`.

## Final exports (this Databricks Git folder)
- `SUBMISSION.md` — this handoff
- `outputs/initial-rejection-evidence.json` — rejected records with reasons
- `outputs/return_features.csv`, `outputs/features_train.csv`,
  `outputs/features_validation.csv`, `outputs/features_test.csv`
- `outputs/run-evidence.json` — per-run IDs and values plus the two-run comparison

## Could not verify
- Nonnegativity of source amounts is asserted by the Catalog description; not re-checked
  against the raw values in this run.
- The supplied `source_lineage` view is the only lineage evidence; its own description
  notes it does not claim a Catalog UI lineage check.
- Both compared runs executed in this interactive notebook on the attached compute; no
  separate Jobs run IDs exist for them.
