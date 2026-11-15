# Start your first Databricks Challenge

Use Codex in your Cloud Workstation to create and execute a notebook inside Databricks, then
submit its platform-produced files to your own GitHub repository. This starter supplies access
operations and an ungraded smoke check. It contains no Challenge solution. Read the assignment
for the four required outcomes.

Complete the ungraded connection walkthrough first. Begin the task when your facilitator releases
the Classroom assignment; automatic ability scoring is not enabled for this assignment.

## Before you start

The training team provides a Cloud Workstation with Python 3.11+, Git, uv, Codex and the Databricks
CLI, plus your Databricks access and serverless compute entitlement. Use the course's “Set up your
machine and the training environment” post to sign in with your training Google account. Do not
put passwords, tokens or sign-in codes in Codex prompts or this repository.

1. Open [the course starter](https://github.com/GitRollTraining/to6-databricks-challenge-starter).
2. Select **Use this template → Create a new repository**. Select your personal GitHub account
   as owner, name the repository, choose **Private**, and create it. Do not select the course
   organization as owner. Keep the default `main` branch.
3. In your Cloud Workstation terminal, clone **your new repository**, using its HTTPS URL:

   ```sh
   git clone https://github.com/YOUR-USERNAME/YOUR-REPOSITORY.git
   cd YOUR-REPOSITORY
   ```

4. Open this folder in the Workstation editor. Keep this guide open as `START_HERE.md`;
   `README.md` will become your native handoff when you submit. [CHALLENGE.md](CHALLENGE.md)
   holds the assignment. In your private repository, open **Settings → Collaborators → Add people**
   and invite `TakalaWang` (the course delivery operator) so the submitted files and session
   record can be reviewed. Keep the repository private.

Replace the two uppercase values with your actual GitHub username and repository name in every
command. Implementation stays in Databricks; the local clone holds tools and the genuine Codex
session, not editable notebook solution files.

## One-time connection setup

The following installs platform tooling, not a solution. Run from your cloned starter root:

```sh
mkdir -p .tools
git clone https://github.com/databricks-solutions/ai-dev-kit.git .tools/ai-dev-kit
git -C .tools/ai-dev-kit checkout --detach 07f8e1ccf3dfc09a794eaf86fd15a5848f9d63d4
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt ./.tools/ai-dev-kit/databricks-tools-core ./.tools/ai-dev-kit/databricks-mcp-server
```

If `.tools/ai-dev-kit` already exists, check its revision rather than cloning over it. The wrapper
requires this exact revision and clean tracked server files. Tooling, environment and local
receipts are ignored by Git. Run the local checks with:

```sh
.venv/bin/python -m unittest discover -s tests -v
```

Sign in to the course Databricks workspace with the named profile (the CLI opens the browser):

```sh
databricks auth login --host 'https://dbc-48a16403-f66f.cloud.databricks.com?w=7474648218609329&account_id=d0c409e4-5865-49d6-86f5-241979d24355' --profile TO6
```

Use your own learner account, not an operator account. Do not set token or host environment
variables to override this profile. Set your own repository URL in the following command:

```sh
.venv/bin/python tools/databricks_platform.py init --repository https://github.com/YOUR-USERNAME/YOUR-REPOSITORY
```

If you already signed in to this course workspace under another profile, add `--profile YOUR-EXISTING-PROFILE`
to `init`. Use that existing authenticated profile; copying its host into a new profile does not
copy its OAuth login. Never copy tokens. Later commands automatically reuse the saved profile.

`init` reads your authenticated Databricks username and writes `.platform/config.json`. Read the
printed configuration before proceeding:

| Setting | Assigned value |
|---|---|
| Profile | `TO6` by default, or the existing signed-in profile you selected |
| Notebook | `/Users/<your Databricks username>/to6-challenge3/notebook` |
| Input schema | `training.to6_challenge3_inputs` |
| Input tables | `returns_initial` and `returns_corrected` in that schema |
| Supplied lineage view | `training.to6_challenge3_inputs.source_lineage` |
| Output schema | `training.to6_c3_` followed by the first 12 hex characters of SHA-256 of your exact Databricks username; `init` calculates it |
| Submission | Your repository URL, branch `main` |

The administrator must already have created your output schema and granted input read access.
Do not change the derived schema/path to another learner's values. Unity Catalog grants and
workspace permissions enforce isolation; this tool is **not** a SQL or Python sandbox. Both SQL
and notebook code run with your actual Databricks permissions. Use fully qualified table names.

### Authorize your own GitHub connection

The managed GitHub MCP service is named `system.ai.github`. Its endpoint in this workspace is:

`https://dbc-48a16403-f66f.cloud.databricks.com/ai-gateway/mcp-services/system.ai.github`

1. Open [Authorize GitHub for Databricks](https://dbc-48a16403-f66f.cloud.databricks.com/mcp-service-login?name=system.ai.github)
   while signed in to your training Databricks account.
2. Complete the GitHub authorization using **your own GitHub account**, the owner of the
   repository you just created. Permit the connection to access that private repository.
3. Return to the Workstation. Never paste authorization tokens into a prompt. This connection
   belongs to you; an operator's connected account cannot submit your work. Run:

```sh
.venv/bin/python tools/databricks_platform.py check
```

The check verifies the connected GitHub user owns your personal repository, then reads your Databricks identity, both input table descriptions/columns, your output schema and
GitHub's README on `main`. It does not write to external platforms. A successful check proves
these reads only. The walkthrough below tests compute/write/export; the lineage view and
session-record transfer have their own checks.

## Ungraded native round trip

Before this walkthrough, complete [SESSION_RECORD.md](SESSION_RECORD.md), steps 1–2.
Use its Databricks-specific finish steps after every submission. Remote GitHub commits do not
trigger your Workstation's Git hooks. Keep the genuine Codex conversation, including tool context;
screenshots and reconstructed chat are not substitutes.

Start Codex in this repository. Give it the following platform operating contract:

> Use `.venv/bin/python tools/databricks_platform.py execute` with JSON on stdin containing inline `code`.
> Author and update the notebook through that native operation, never a local solution file or
> `file_path`. Run only on Databricks. Read `.platform/config.json` for my exact paths/schema.
> Use `sql` for independent readback and `submit` for native exports. Ask before writes whose
> target I have not authorized. Never edit a downloaded/exported notebook locally.

For the connection smoke only, send this inline Python through `execute`, replacing
`YOUR_OUTPUT_SCHEMA` with the exact configuration value. No local `.py` file is created:

```python
import json
schema = "YOUR_OUTPUT_SCHEMA"
practice = schema + ".connection_smoke"
spark.createDataFrame([(1, "connected")], ["check_id", "message"]).write.mode("overwrite").saveAsTable(practice)
rows = [r.asDict() for r in spark.table(practice).collect()]
assert rows == [{"check_id": 1, "message": "connected"}]
dbutils.notebook.exit(json.dumps({"files": {
    "README.md": "Ungraded connection smoke. Native output: " + practice + "\n",
    "outputs/connection-smoke.json": json.dumps(rows)
}}))
```

The operation accepts exactly `{"code":"...inline notebook source..."}` on stdin. Codex can
serialize that JSON and pass it directly without writing a solution file. Open the returned
run URL and your native notebook in the Databricks browser. Confirm the run succeeded.

Independently read the saved practice table with `sql`, passing JSON on stdin:

```json
{"query":"SELECT * FROM YOUR_OUTPUT_SCHEMA.connection_smoke"}
```

Replace `YOUR_OUTPUT_SCHEMA` first. Also open the two supplied inputs in Catalog Explorer. For the supplied lineage view, open
**Catalog → training → to6_challenge3_inputs → source_lineage**, or use the same `sql` command
with `SELECT * FROM training.to6_challenge3_inputs.source_lineage`. This supplied source map is
separate from the Catalog UI's Lineage tab. The administrator owns missing permissions or metadata.

Now run:

```sh
.venv/bin/python tools/databricks_platform.py submit
.venv/bin/python tools/databricks_platform.py read notebook.py
.venv/bin/python tools/databricks_platform.py read README.md
```

`submit` obtains the native source and the successful run's notebook output. It creates a GitHub
commit on your own `main`, then reads every submitted file at that exact commit and checks its
bytes or Git blob hash. Open that commit in GitHub. A `verified: false` receipt means a commit
may already exist but its readback failed: inspect it before retrying. Complete SESSION_RECORD.md steps 3–5 and confirm the remote checkpoint too. Only then begin
the actual task.

## Work and submission contract

Use the same native notebook for your own implementation. The task's initial bad-batch failure
is expected: keep the failed native run and affected records/reason. The wrapper retains run IDs
in ignored local receipts; include the required evidence in the native handoff/output files so
reviewers receive it. A successful run must end with `dbutils.notebook.exit` containing JSON of
this shape, constructed **in Databricks**:

```json
{"files":{"README.md":"Your handoff text","outputs/your-output.json":"Your small synthetic output export"}}
```

The wrapper permits only `README.md` and paths under `outputs/`. It adds `notebook.py` from the
native SOURCE export and `outputs/native-run.json` with the execution reference. Do not supply
a notebook file or reserved run receipt yourself. Large/truncated notebook output is rejected;
keep these synthetic exports small. Failed latest execution, changed native source, unsuccessful
run status or mismatched GitHub readback cannot produce a verified submission receipt.

`sql` accepts a query and returns the actual managed SQL result. If it returns a pending
`statement_id`, call `.venv/bin/python tools/databricks_platform.py sql --statement-id ID` to read its
status; repeat while pending, without resubmitting the original query. It is not an access-control
filter. Use it for inspecting your inputs and saved outputs; never modify the shared inputs.
The approved output boundary is your derived schema, enforced by platform grants.

The task defines the actual deliverables and quality requirements. Record your final run references,
source/output identifiers and limitations in the native handoff. After `submit`, identify its
commit in the Classroom submission alongside your repository **base** URL. Do not edit the
handoff locally merely to add its own commit hash (a file cannot name its containing future hash).
Your genuine Codex session remains a separate required submission record. This starter does
not promise that automated grading is ready.

The practice table and `outputs/connection-smoke.json` may remain as ungraded connection evidence.
Submission updates the files named by the successful native run; it does not erase older exports.
List the files belonging to your final task delivery in the native README so a reviewer can
distinguish them from practice or earlier outputs.
