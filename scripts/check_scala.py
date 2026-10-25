"""Compile real notebook bodies with Spark/Delta jars; only dbutils is stubbed.
Usage: python scripts/check_scala.py /path/to/delta-spark_2.12.jar
Requires pyspark==3.5.3 and a JDK. This does not execute Databricks queries.
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import pyspark

jars = Path(pyspark.__file__).parent / 'jars'
classpath = os.pathsep.join([str(jars / '*'), sys.argv[1]])
with tempfile.TemporaryDirectory() as tmp:
    files = []
    for i, path in enumerate(sorted(Path('databricks/notebooks').glob('*.scala'))):
        target = Path(tmp) / f'Notebook{i}.scala'
        target.write_text('''
object NotebookNUMBER {
val spark: org.apache.spark.sql.SparkSession = null
object dbutils { object widgets {
  def text(k: String, v: String): Unit = ()
  def get(k: String): String = "compile_only"
}}
'''.replace('NUMBER', str(i)) + path.read_text() + '\n}\n')
        files.append(str(target))
    subprocess.run(['java', '-cp', classpath, 'scala.tools.nsc.Main', '-classpath', classpath,
                    '-d', tmp, *files], check=True)
print('Scala notebooks compiled against Spark 3.5.3 / Delta 3.2.0; runtime integration not exercised.')
