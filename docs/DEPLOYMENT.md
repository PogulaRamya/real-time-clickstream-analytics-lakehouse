# Deployment runbook

## 1. Prerequisites and credentials

Use separate development accounts and service identities. Required tools: Python 3.11, AWS CLI, Terraform 1.9+, Databricks CLI with bundle support, dbt-snowflake 1.9.1, Airflow 2.10.5 and Tableau Desktop/Server/Cloud.

Cloud runtime baseline: **Databricks Runtime 15.4 LTS, dedicated access mode**, Unity Catalog-enabled cluster. Verify the Snowflake Spark connector bundled with your runtime; attach a compatible Spark 3.5 / Scala 2.12 connector and JDBC driver if absent. Cloud compatibility is a deployment gate, not yet tested here.

Set up:

- AWS role/profile for producer; attach Terraform's producer IAM policy. No access keys in source.
- Databricks cluster instance profile with Terraform's consumer policy. Use default Kinesis polling (no EFO permissions supplied).
- UC catalog `clickstream_dev`, schemas `bronze`, `silver`, `gold`, `ops`, and managed volume `ops.checkpoints`. Grant the pipeline principal catalog/schema usage, table creation/modification, and volume read/write. Restrict bronze/silver to this principal and engineering group.
- Databricks account groups `clickstream-engineers` and `clickstream-analysts`. Include the pipeline service principal in engineering if it queries masked contact/quarantine columns.
- Snowflake loader and transformer service users with separate RSA key pairs. Run `snowflake/bootstrap.sql`, assign the matching roles and register each public key. Databricks secret scope `clickstream`: `snowflake-user`, `snowflake-private-key` (unencrypted PKCS8 PEM in secret storage). Never print or commit key contents. For encrypted keys, adapt the loader to obtain the passphrase from a separate secret.

## 2. Provision Kinesis (billable)

```bash
cd infra
terraform init
terraform plan -out=dev.plan
terraform apply dev.plan
cd ..
```

Eight shards provide headroom for the target record rate, but actual capacity depends on bytes/record, partition distribution and consumers. No VPC, Databricks workspace, IAM role trust policy or Snowflake account is created by Terraform. Attach generated IAM policies to your existing roles, including correct instance-profile trust. Do not use production roles.

## 3. Configure and start streaming

Authenticate the Databricks CLI using your organization's supported login. Export these non-secret bundle settings:

```bash
export BUNDLE_VAR_cluster_id='<dedicated-cluster-id>'
export BUNDLE_VAR_checkpoint_root='/Volumes/clickstream_dev/ops/checkpoints'
export BUNDLE_VAR_sf_account='<organization-account>'
databricks bundle validate
databricks bundle deploy
```

Use separate terminals for the long-running jobs (or start via Jobs UI):

1. `databricks bundle run bronze`. Produce a small sample to create bronze.
2. After `bronze.events` exists, `databricks bundle run silver`.
3. After `silver.events` exists, `databricks bundle run gold`.
4. Allow a short product window and a session to finalize. To test finalization quickly, generate new events with event time >32 minutes beyond the earlier session's last event, while respecting the future timestamp guard. Do not change production clocks; use a separate test dataset or a shortened session gap in an isolated test notebook.
5. Run `databricks/governance.sql` as owner, substituting your catalog. CDF must be enabled on both gold tables before exports. The masked contact table is an initial lookup snapshot; refresh separately if needed. Bronze is restricted rather than masked because it is a streaming source.

Each query has a unique durable checkpoint path. Do not share checkpoint paths, delete checkpoints, or run multiple copies of a job. Configure job notifications and retry policies in the workspace before unattended operation. Continuous job mode is not enabled by default; manually restart failed jobs from the same checkpoint.

## 4. Send events and measure throughput

```bash
python -m pip install -e '.[aws]'
python -m clickstream.generator --stream clickstream-dev --rate 100 --seconds 60
python -m clickstream.generator --stream clickstream-dev --rate 5000 --seconds 300
```

The live simulator timestamps each burst near current time and uses per-burst visitor identities. This is throughput-oriented synthetic traffic, not a realistic long-duration customer distribution. Local JSONL generation uses a deterministic event-time timeline and is for the local demo only; do not inject its future timeline into the live pipeline unchanged.

## 5. Export and dbt

Run `databricks bundle run export` after both gold tables exist and CDF is enabled. The first run exports a version-bound full snapshot, subsequent runs export CDF inserts/updates. Each table's merge and version cursor commit in one Snowflake transaction; failure before commit causes safe retry. Only one export may run at a time, including manual runs. The job's concurrency limit and Airflow's `max_active_runs=1` help enforce this.

```bash
python -m pip install dbt-core==1.9.1 dbt-snowflake==1.9.1
export SNOWFLAKE_ACCOUNT='<organization-account>'
export SNOWFLAKE_USER='<transformer-user>'
export SNOWFLAKE_PRIVATE_KEY_PATH='/secure/path/transformer-key.p8'
cd dbt
dbt debug --profiles-dir .
dbt build --profiles-dir .
dbt source freshness --profiles-dir .
dbt docs generate --profiles-dir .
```

`dbt build` includes the seed, models, snapshot and tests. Edit the seed catalog and run again to demonstrate SCD2 changes. Sample catalog has only three SKUs and is deliberately not joined into the metrics. Changes to historical data require `dbt build --full-refresh` after a coordinated export rebuild. The ten-minute incremental overlap is based on source `updated_at`, not event time.

## 6. Airflow

Install `airflow/requirements.txt` in an isolated Airflow 2.10 environment using the official matching Python constraints file for Airflow itself, then resolve and verify provider/dbt compatibility. Mount this repository at `/opt/airflow/clickstream` (or set `CLICKSTREAM_PROJECT_DIR`), copy the DAG to the DAGs folder, create `databricks_default` connection through a secret backend and set Airflow Variable `clickstream_export_job_id` to the numeric export job ID from bundle deployment. Provide dbt environment variables and key file on the worker, not the scheduler's git repository.

Turn on the DAG only after one successful manual export/build. Long-lived streaming jobs remain separate. Five-minute runs may be delayed by job runtime; monitor rather than promise a fixed end-to-end SLA. `dbt source freshness` will alert on intentionally idle traffic; pause the demo DAG when the producer is stopped.

## 7. Tableau

Follow `tableau/README.md`. Open and validate the supplied `.twb` template in Tableau Desktop, enter your Snowflake server and reader credentials, resolve field metadata if prompted, then publish to your project. Workbook XML cannot be validated with Tableau in this environment; acceptance requires opening it with your version. The publication script accepts a validated `.twb` or `.twbx` and uses a Tableau PAT from environment variables. Never put a PAT or Snowflake password in the workbook tracked by git.

## 8. Acceptance and cleanup

Run the checklist in OPERATIONS.md. Verify schema access using both engineer and analyst identities; inspect Unity Catalog lineage and compare source/sink counts. Enable Snowflake budget/resource monitoring and Databricks job alerts in your accounts. Stop producer and jobs, pause Airflow, suspend the warehouse, remove demo tables if desired, and use `terraform destroy` to remove the Kinesis stream and policies when finished. Destroying the stream removes retained events.
