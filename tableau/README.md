# Tableau dashboard

`clickstream.twb` is an editable Snowflake connection/workbook template, not an already published dashboard. Its XML has been checked for well-formedness but has not been opened in Tableau Desktop here.

1. Open the workbook in Tableau Desktop. Edit both Snowflake connections: replace `YOUR_ACCOUNT.snowflakecomputing.com`, select `CLICKSTREAM_WH`, `CLICKSTREAM`, `ANALYTICS`, and authenticate with the reader service user/role or approved OAuth integration. Do not save passwords in the repository.
2. Refresh metadata. Product revenue and funnel sheets use the two dbt views. Use a live Snowflake connection; client refresh and caching still affect freshness.
3. Confirm dimensions/measures are recognized, format revenue as USD and timestamps as UTC. If Tableau upgrades the workbook XML, save the new version without credentials.
4. Add date filters to both sources; apply filters deliberately because the product and funnel data have different grains and finalization delays. Never join daily product metrics to sessions without aggregation.
5. Publish to a Tableau project with reader-only permissions and confirm totals against Snowflake.

Suggested finishing touches: conversion percentage KPI, product revenue bars, daily stage counts, revenue trend, device filter only after extending the model (current marts do not retain device), last-updated text and a visible `Final sessions lag inactivity by at least 32 min` caption.

Optional publishing helper:

```bash
pip install tableauserverclient==0.35
export TABLEAU_SERVER='https://your-pod.online.tableau.com'
export TABLEAU_SITE='your-site-content-url'
export TABLEAU_TOKEN_NAME='deployment-pat'
# Supply TABLEAU_TOKEN_SECRET through a secret manager or hidden shell prompt.
export TABLEAU_PROJECT_ID='<project-id>'
python tableau/publish.py /path/to/validated-workbook.twb
```

The helper publishes but does not embed Snowflake credentials. Configure connection authentication in Tableau's secure credential settings after publication. Publishing requires a real Tableau account/project and was not executed here.
