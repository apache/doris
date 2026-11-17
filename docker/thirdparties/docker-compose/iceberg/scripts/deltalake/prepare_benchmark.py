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

"""Register immutable SF1 Parquet fixtures in HMS and add Delta logs without rewriting data."""

import argparse
import json

from pyspark.sql import SparkSession


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--manifest", required=True)
parser.add_argument("--warehouse", default="s3a://delta/warehouse")
args = parser.parse_args()
with open(args.manifest) as source:
    manifest = json.load(source)
spark = SparkSession.builder.getOrCreate()
try:
    # This benchmark intentionally reads the same immutable files as plain Parquet and as Delta.
    # Disable Spark's format-mismatch guard for the plain Parquet reference on repeat preparation.
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
    for workload, tables in manifest.items():
        database = "delta_" + workload
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        for table, info in tables.items():
            path = f"{args.warehouse.rstrip('/')}/{database}/{table}"
            spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
            spark.sql(f"CREATE TABLE {database}.{table} USING PARQUET LOCATION '{path}/data'")
            parquet_rows = spark.table(f"{database}.{table}").count()
            spark.sql(f"CONVERT TO DELTA parquet.`{path}`")
            delta_rows = spark.read.format("delta").load(path).count()
            assert parquet_rows == delta_rows == info["rows"], (table, parquet_rows, delta_rows, info)
            provider = spark._jsparkSession.sessionState().catalog().externalCatalog().getTable(
                database, table).provider().get()
            assert provider.lower() == "parquet", (table, provider)
            print("DELTA_BENCHMARK_FIXTURE", database, table, parquet_rows, provider, flush=True)
finally:
    spark.stop()
