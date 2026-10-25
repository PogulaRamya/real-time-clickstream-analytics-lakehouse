"""Execute the actual gold transformation expressions on finite Spark fixtures.
This tests aggregation semantics, not streaming watermark or cloud integration.
Requires pyspark==3.5.3, JDK 17 and local loopback access.
"""
from pathlib import Path
import os
import subprocess
import tempfile
import zipfile
import pyspark

root = Path(pyspark.__file__).parent
source = Path('databricks/notebooks/03_gold.scala').read_text()
product = source[source.index('val product ='):source.index('val p =')]
sessions = source[source.index('val sessions ='):source.index('val s =')]
code = '''
object GoldSmoke {
 def main(args: Array[String]): Unit = {
  val spark = org.apache.spark.sql.SparkSession.builder.master("local[2]").appName("gold-smoke")
    .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  import spark.implicits._
  import org.apache.spark.sql.functions._
  try {
   val events = Seq(
    ("u1", "2026-01-01 00:03:00", "e4", "purchase", "sku1", 1234L),
    ("u1", "2026-01-01 00:00:00", "e1", "product_view", "sku1", 0L),
    ("u1", "2026-01-01 00:02:00", "e3", "checkout", "sku1", 0L),
    ("u1", "2026-01-01 00:01:00", "e2", "add_to_cart", "sku1", 0L),
    ("u1", "2026-01-01 01:00:00", "e5", "product_view", "sku1", 0L),
    ("u2", "2026-01-01 00:00:00", "e6", "purchase", "sku2", 900L),
    ("u2", "2026-01-01 00:01:00", "e7", "product_view", "sku2", 0L)
   ).toDF("user_id", "event_time", "event_id", "event_type", "product_id", "revenue_cents")
    .withColumn("event_time", to_timestamp(col("event_time")))
''' + product + sessions + '''
   assert(sessions.count() == 3, "30-minute gap must split sessions")
   assert(sessions.filter($"funnel_stage" === 4).count() == 1, "ordered funnel must ignore purchase-before-view")
   assert(product.agg(sum("revenue_cents")).first().getLong(0) == 2134L, "revenue must be preserved")
   assert(product.agg(sum("purchases")).first().getLong(0) == 2L, "product purchases must aggregate")
   println("GOLD_SMOKE_PASSED")
  } finally { spark.stop() }
 }
}
'''
with tempfile.TemporaryDirectory() as tmp:
    src = Path(tmp) / 'GoldSmoke.scala'
    src.write_text(code)
    cp = str(root / 'jars' / '*')
    subprocess.run(['java', '-cp', cp, 'scala.tools.nsc.Main', '-classpath', cp, '-d', tmp, str(src)], check=True)
    jar = str(Path(tmp) / 'smoke.jar')
    with zipfile.ZipFile(jar, 'w') as archive:
        for compiled in Path(tmp).glob('*.class'):
            archive.write(compiled, compiled.name)
    subprocess.run([str(root / 'bin' / 'spark-submit'), '--class', 'GoldSmoke', '--master', 'local[2]', jar],
                   env={**os.environ, 'SPARK_LOCAL_IP': '127.0.0.1'}, check=True)
