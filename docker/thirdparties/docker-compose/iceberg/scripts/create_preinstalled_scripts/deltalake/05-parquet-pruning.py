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

"""One file with multiple row groups and pages, including a mapped/renamed column."""

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()
try:
    # Delta does not forward arbitrary DataFrameWriter options to ParquetOutputFormat.
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in {
        "parquet.block.size": "65536",
        "parquet.page.size": "1024",
        "parquet.page.row.count.limit": "100",
        "parquet.enable.dictionary": "false",
    }.items():
        conf.set(key, value)
    for mode in ("none", "name", "id"):
        table = f"delta_m1.parquet_pruning_{mode}"
        path = f"s3a://delta/warehouse/delta_m1/parquet_pruning_{mode}"
        spark.sql(f"""CREATE OR REPLACE TABLE {table} (id BIGINT, payload STRING)
            USING delta LOCATION '{path}'
            TBLPROPERTIES ('delta.enableDeletionVectors'='false',
                           'delta.columnMapping.mode'='{mode}')""")
        (spark.range(0, 10000, 1, 1).selectExpr("id", "repeat('x', 64) AS payload")
         .write.format("delta").mode("overwrite")
         .save(path))
        if mode != "none":
            spark.sql(f"ALTER TABLE {table} RENAME COLUMN id TO renamed")
        column = "id" if mode == "none" else "renamed"
        spark.sql(f"SELECT {column}, length(payload) FROM {table} "
                  f"WHERE {column} >= 100 AND {column} < 120 ORDER BY {column}").show()
finally:
    spark.stop()
