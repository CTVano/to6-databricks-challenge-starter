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

# Your investigation here.
Schema: to6_challenge3_inputs
  Description: Synthetic data that includes parsed source data, return data, and source lineage data
  Tables:
      returns_initial
        Description: Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output.

      returns_corrected
        Description: Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness.

      source_lineage
        Description: Source data lineage

      parsed_source
        Description: Synthetic parsed source: one row is one return; return_id identifies the return, taxpayer_id identifies a filer who may recur across years. Nonnegative amounts; zero income is valid.

Returns_Initial Sample Data Rows:
return_id	taxpayer_id	tax_year	income	deduction
R01	F0	2022	100	20
R01	F0	2022	100	20
R02	F1	2022	null	33
R03	F2	2022	0	46
R04	F3	2022	400	59
R05	F0	2023	500	72
R06	F1	2023	600	85
R07	F2	2023	700	98
R08	F3	2023	800	111
R09	F0	2024	0	124
R10	F1	2024	1000	137
R11	F2	2024	1100	150
R12	F3	2024	1200	163

Returns_Corrected Sample Rows:
    return_id	taxpayer_id	    tax_year	income	    deduction
    R01	        F0	            2022	    100	        20
    R02	        F1	            2022	    200	        33
    R03	        F2	            2022	    0	          46

Lineage:
  Returns_initial comes from parsed_source and returns_initial from to6_inputs_20261115
  Returns_corrected comes from to6_inputs_20261115 and to6_challenge3_inputs

Returns_initial has more data than corrected so some cleanup happened possibly due to duplicates
Returns_initial has duplicate return_id of 01 that was corrected in returns_corrected
Returns_initial has income as nulls instead of 0 that need to be corrected in returns_corrected
Returns_initial and returns_corrected has tax year distribution of 2022 to 2024

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# Your checks and decision here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.