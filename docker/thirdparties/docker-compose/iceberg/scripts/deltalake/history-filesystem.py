# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Create the filesystem history fixture and its typed Spark reference.

Run with Spark 4.0 and Delta Spark 4.4.1. Use a dedicated warehouse and set
DELTALAKE_HISTORY_WAREHOUSE to its URI and DELTALAKE_HISTORY_REFERENCE to the
local output file. Use the same paths in deltalakeWarehouse and
deltalakeHistoryReference in the regression configuration.
"""

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession


warehouse = os.environ["DELTALAKE_HISTORY_WAREHOUSE"].rstrip("/")
reference = Path(os.environ["DELTALAKE_HISTORY_REFERENCE"])
spark = (SparkSession.builder.appName("doris-delta-history-fixture")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.warehouse.dir", warehouse)
         .enableHiveSupport()
         .getOrCreate())
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS delta_m1 LOCATION '{warehouse}/delta_m1'")
    spark.sql("DROP TABLE IF EXISTS delta_m1.history_fixture")
    spark.sql("""CREATE TABLE delta_m1.history_fixture (id BIGINT) USING DELTA
                 TBLPROPERTIES ('delta.enableInCommitTimestamps'='false')""")
    spark.sql("INSERT INTO delta_m1.history_fixture VALUES (1)")
    spark.sql("""ALTER TABLE delta_m1.history_fixture SET TBLPROPERTIES
                 ('delta.enableInCommitTimestamps'='true')""")
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "history-regression")
    spark.sql("INSERT INTO delta_m1.history_fixture VALUES (2)")
    history = spark.sql("DESCRIBE HISTORY delta_m1.history_fixture").orderBy("version")
    projection = ["unix_millis(timestamp)" if name == "timestamp" else name for name in history.columns]
    rows = [list(row.asDict(recursive=True).values()) for row in history.selectExpr(*projection).collect()]
    reference.write_text(json.dumps(rows), encoding="utf-8")
finally:
    spark.stop()
