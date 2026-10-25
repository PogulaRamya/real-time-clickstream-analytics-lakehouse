# Databricks notebook source
"""Incremental Delta CDF to Snowflake, with a cursor committed AFTER each merge.
Install snowflake-connector-python on this export job's cluster.
Enable CDF on gold before the first export. No overlapping runs allowed.
"""
import re
import uuid
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.conf.set("spark.sql.session.timeZone", "UTC")

for name in ("catalog", "sf_account", "sf_database", "sf_warehouse", "sf_role", "secret_scope"):
    dbutils.widgets.text(name, "")
values = {name: dbutils.widgets.get(name) for name in
          ("catalog", "sf_account", "sf_database", "sf_warehouse", "sf_role", "secret_scope")}
cat = values["catalog"]
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cat)
scope = values["secret_scope"]
key = serialization.load_pem_private_key(dbutils.secrets.get(scope, "snowflake-private-key").encode(), password=None)
der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
user = dbutils.secrets.get(scope, "snowflake-user")
conn = snowflake.connector.connect(account=values["sf_account"], user=user, private_key=der,
    database=values["sf_database"], warehouse=values["sf_warehouse"], role=values["sf_role"], schema="RAW")
options = dict(sfURL=f'{values["sf_account"]}.snowflakecomputing.com', sfUser=user,
    pem_private_key=dbutils.secrets.get(scope, "snowflake-private-key").replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\n", ""),
    sfDatabase=values["sf_database"], sfSchema="RAW", sfWarehouse=values["sf_warehouse"], sfRole=values["sf_role"])
try:
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS EXPORT_CURSORS (TABLE_NAME VARCHAR, VERSION NUMBER)")
    for table, keys in [("product_minute", ["WINDOW_START", "PRODUCT_ID"]), ("sessions", ["SESSION_ID"])]:
        name = f"{cat}.gold.{table}"
        end = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"]
        cur.execute("SELECT VERSION FROM EXPORT_CURSORS WHERE TABLE_NAME=%s", (table,))
        row = cur.fetchone()
        if row and row[0] >= end:
            continue
        if row:
            frame = (spark.read.format("delta").option("readChangeFeed", "true")
                     .option("startingVersion", row[0]+1).option("endingVersion", end).table(name)
                     .filter("_change_type IN ('insert','update_postimage')"))
            frame = (frame.withColumn("_rn", F.row_number().over(Window.partitionBy(*[k.lower() for k in keys])
                         .orderBy(F.desc("_commit_version"))))
                     .filter("_rn=1").drop("_rn", "_change_type", "_commit_version", "_commit_timestamp"))
        else:
            frame = spark.read.option("versionAsOf", end).table(name)
        # Read version-bound data, never a moving source snapshot.
        frame = frame.select(*[F.col(c).alias(c.upper()) for c in frame.columns])
        target = table.upper()
        stage = f"STAGE_{target}_{uuid.uuid4().hex.upper()}"
        frame.write.format("net.snowflake.spark.snowflake").options(**options).option("dbtable", stage).mode("overwrite").save()
        columns = frame.columns
        quoted = lambda c: '"' + c + '"'
        on = " AND ".join(f't.{quoted(k)}=s.{quoted(k)}' for k in keys)
        assignments = ",".join(f't.{quoted(c)}=s.{quoted(c)}' for c in columns if c not in keys)
        names = ",".join(map(quoted, columns))
        source = ",".join(f's.{quoted(c)}' for c in columns)
        try:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {target} LIKE {stage}")
            cur.execute("BEGIN")
            cur.execute(f"MERGE INTO {target} t USING {stage} s ON {on} WHEN MATCHED THEN UPDATE SET {assignments} WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({source})")
            cur.execute("DELETE FROM EXPORT_CURSORS WHERE TABLE_NAME=%s", (table,))
            cur.execute("INSERT INTO EXPORT_CURSORS VALUES (%s,%s)", (table,end))
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {stage}")
finally:
    conn.close()
