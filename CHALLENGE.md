# Prepare the next team's case-selection dataset

The modelling team starts next week. A delivery of synthetic return records has arrived, but
the data owner warns that the first batch contains mistakes. Deliver a checked feature dataset:
reject the faulty batch, assess its replacement, and prepare separate training, validation and
test years. Your notebook and data contract will support later modelling and capstone work;
this task does not train a model or supply its target labels.

## Work in Databricks

Use Codex to direct and review the work through the trainer-provided connection. Implement
one notebook and the handoff files in Databricks, execute on Databricks compute, and submit the
platform-produced files to GitHub. Do not develop or run a solution locally and then import it.

The trainer provides a workspace, compute, read-only inputs, your own writable output schema,
a starter with connection/read/write examples, and the Codex session capture route. Create your
own private GitHub repository from the starter and use its `main` branch. Complete the ungraded
walkthrough of these operations before starting. Ask the
trainer to resolve missing access; configuring accounts, compute or MCP is not part of this task.

## Supplied data

`returns_initial` and `returns_corrected` are already parsed synthetic tables covering 2022–2024.
One row represents one return. `return_id` identifies the return; `taxpayer_id` identifies its
filer, who may have returns in several years. Required fields are `return_id`, `tax_year`,
`income` and `deduction`. Each return must occur exactly once. Amounts are nonnegative;
zero income is valid. You do not need to build an XML parser.

## Task and judgment requirements

Work toward these four connected outcomes. Choose the implementation yourself.

1. **Understand the input.** Use Catalog descriptions, at least three sample rows and the
   supplied lineage view to establish the row meaning, relevant fields, uniqueness identifier
   and upstream source. Keep a concise source reference and explanation in the notebook.
2. **Decide which batch is usable.** Required-value and uniqueness checks must reject the
   initial batch before feature output is written. Preserve the affected records and the reason
   for rejection. Assess `returns_corrected` with the same rules and use it for the final run.
   Removing checks, replacing unknown amounts with zero or silently discarding records is not
   an acceptable correction. Explain the failure and your next action using the actual records
   and rule; related observations may span your Codex conversation.
3. **Deliver a checked feature table.** Save `return_features` with exactly `return_id`,
   `tax_year` and `deduction_to_income`, where the ratio is deduction divided by income.
   A zero-income return stays in the table with a null ratio. Keep every accepted return once;
   exclude the filer identifier and raw amounts. Verify a nonzero result against an expected
   value you derive from the source, and check the saved table by reading it back, including a
   zero-income case.
4. **Make the handoff usable.** From the saved features, save `features_train` for 2022,
   `features_validation` for 2023 and `features_test` for 2024. Every accepted return must appear in exactly one of these
   tables, in its assigned year. Two complete successful runs must leave the same return IDs
   and values, without appended copies. Provide a short handoff naming inputs, outputs,
   the ratio definition, checks performed and anything you could not verify.

The checks above establish this task's data contract; they do not prove that every possible
source of model leakage has been eliminated. Later modelling work must add suitable labels
and assess which information is available at prediction time.

## Submit to GitHub

From Databricks, export the native notebook as readable source and produce `README.md` plus
small synthetic output exports. Include source/output table identifiers and actual run
references, retaining the initial rejection evidence. Use the trainer-provided connection to
commit these platform-produced files to your personal GitHub repository’s `main` branch, without local edits.
Check the exact submitted commit contains the notebook, handoff and exports. Submit the
repository's base URL (`https://github.com/{owner}/{repo}`), branch and final submitted commit SHA
in your Classroom submission. Keep native run references and table identifiers in the handoff;
do not edit it locally to add its own future containing commit hash.

Preserve your genuine Codex session through the course capture route. Ownership and Trait
assessment consider your attributable decisions and behavior there; agent replies and tool
activity provide context, not a substitute for your own judgment. Do not reconstruct a
conversation afterwards or include credentials. Genie logs are not required, and screenshots
cannot replace the code, saved outputs or interaction record.
