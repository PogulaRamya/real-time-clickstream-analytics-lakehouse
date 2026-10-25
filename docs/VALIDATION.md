# Validation report

Validated locally on 2026-10-25:

| Check | Result |
|---|---|
| Python reference tests | 7 passed: deterministic events, duplicate replay, out-of-order arrival, invalid events, session boundary/ordered funnel, failed-entry retry, exhausted retries |
| Local demo | 10,000 valid unique events, 1 rejected event, 3,527 sessions, 505 ordered conversions; 100 duplicate events did not increase totals |
| Contract | 1,000 generated events passed JSON Schema including date-time format validation |
| Scala notebooks | All three notebook bodies compiled against Spark 3.5.3 / Delta 3.2.0; dbutils widget methods stubbed for compilation only |
| Spark gold smoke test | Actual notebook aggregation expressions executed on Spark 3.5.3; session splitting, ordered conversion, purchase counts and revenue assertions passed |
| dbt | Offline parse passed using dbt-core 1.9.1 and dbt-snowflake 1.9.1; no warehouse execution |
| Python files | Compilation passed |
| YAML / Tableau XML / Terraform HCL | Parsed successfully |
| Terraform formatting | Passed `terraform fmt -check` |
| Terraform provider validation | Blocked locally: downloaded AWS provider failed plugin handshake in this host; rerun `terraform init` and `terraform validate` in CI/deployment environment |

Not verified: live Kinesis ingestion, streaming watermark state eviction/restarts, Unity Catalog grants/masks/lineage behavior, Snowflake export transactions, dbt execution against Snowflake, Airflow scheduling, Tableau Desktop opening/publication, 5K events/sec throughput or cost.

CI checks Python/demo, Scala compilation and gold smoke test, dbt parsing, and Terraform provider validation. A green offline CI result still does not prove cloud end-to-end correctness. Follow OPERATIONS.md before making any production or performance claims.
