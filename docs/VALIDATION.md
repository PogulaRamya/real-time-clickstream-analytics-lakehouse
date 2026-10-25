# Validation report

Validated on 2026-10-25. All four [GitHub Actions jobs](https://github.com/PogulaRamya/real-time-clickstream-analytics-lakehouse/actions/runs/40180013090) passed on implementation commit `c0d28f960f762c682240944ad0683a0b4825fb6e`. The following documentation-only update does not alter tested code:

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
| Terraform provider validation | Passed in GitHub Actions. Local host could not start the AWS provider, but the GitHub runner initialized and validated it successfully. |

Not verified: live Kinesis ingestion, streaming watermark state eviction/restarts, Unity Catalog grants/masks/lineage behavior, Snowflake export transactions, dbt execution against Snowflake, Airflow scheduling, Tableau Desktop opening/publication, 5K events/sec throughput or cost.

CI checks Python/demo, Scala compilation and gold smoke test, dbt parsing, and Terraform provider validation. A green offline CI result still does not prove cloud end-to-end correctness. Follow OPERATIONS.md before making any production or performance claims.

