-- Run as catalog owner. Replace clickstream_dev with your catalog if different.
-- Run AFTER initial streaming table creation; keep bronze/silver restricted beforehand.
CREATE SCHEMA IF NOT EXISTS clickstream_dev.governance;
CREATE OR REPLACE FUNCTION clickstream_dev.governance.mask_payload(value STRING)
RETURN CASE WHEN is_account_group_member('clickstream-engineers') THEN value ELSE '[REDACTED]' END;
-- Keep the streaming source unmasked and restricted to the pipeline identity.
-- A separately governed contact lookup demonstrates column-level masking.
CREATE TABLE IF NOT EXISTS clickstream_dev.governance.customer_contacts USING DELTA AS
SELECT DISTINCT get_json_object(raw_json, '$.user_id') AS user_id,
                get_json_object(raw_json, '$.email') AS email
FROM clickstream_dev.bronze.events;
ALTER TABLE clickstream_dev.governance.customer_contacts ALTER COLUMN email
  SET MASK clickstream_dev.governance.mask_payload;
ALTER TABLE clickstream_dev.silver.quarantine ALTER COLUMN raw_json
  SET MASK clickstream_dev.governance.mask_payload;
ALTER TABLE clickstream_dev.silver.quarantine ALTER COLUMN email
  SET MASK clickstream_dev.governance.mask_payload;
-- Restrict bronze + silver schemas to engineering service principals/groups.
-- Analytics gets only gold; gold has no email, raw user_id, or raw event payload.
GRANT USE CATALOG ON CATALOG clickstream_dev TO `clickstream-analysts`;
GRANT USE SCHEMA ON SCHEMA clickstream_dev.gold TO `clickstream-analysts`;
GRANT SELECT ON SCHEMA clickstream_dev.gold TO `clickstream-analysts`;
ALTER TABLE clickstream_dev.gold.product_minute SET TBLPROPERTIES (delta.enableChangeDataFeed=true);
ALTER TABLE clickstream_dev.gold.sessions SET TBLPROPERTIES (delta.enableChangeDataFeed=true);
-- Databricks automatically captures supported UC table lineage; inspect Catalog Explorer.
-- Mask policies do NOT transfer to Snowflake. Export only non-PII gold data.
