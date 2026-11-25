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

"""Create and retain a managed Delta fixture in the local OSS Unity Catalog."""

import json
import os
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

from pyspark.sql import SparkSession


spark = (SparkSession.builder.appName("doris-delta-uc-fixture")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.catalog.doris_delta", "io.unitycatalog.spark.UCSingleCatalog")
         .config("spark.sql.catalog.doris_delta.uri", os.environ["UC_TEST_SERVER"])
         .config("spark.sql.catalog.doris_delta.auth.type", "static")
         .config("spark.sql.catalog.doris_delta.auth.token", "local-test")
         .config("spark.sql.defaultCatalog", "doris_delta")
         .getOrCreate())
spark.sql("CREATE SCHEMA IF NOT EXISTS doris_delta.m1")
spark.sql("DROP TABLE IF EXISTS doris_delta.m1.uc_managed")
spark.sql("CREATE TABLE doris_delta.m1.uc_managed (id BIGINT, label STRING) USING DELTA")
spark.sql("INSERT INTO doris_delta.m1.uc_managed VALUES (1, 'before'), (2, 'removed')")
spark.sql("DELETE FROM doris_delta.m1.uc_managed WHERE id = 2")
spark.sql("INSERT INTO doris_delta.m1.uc_managed VALUES (3, 'after')")
# The UC commit remains authoritative until backfill. Spark normally publishes the identical
# numbered copy immediately. Hold just that copy in this local fixture to exercise the interval.
api = os.environ["UC_TEST_SERVER"] + "/api/2.1/unity-catalog"
with urlopen(api + "/delta/v1/catalogs/doris_delta/schemas/m1/tables/uc_managed", timeout=15) as response:
    state = json.load(response)
version = state["latest-table-version"]
commit = next(item for item in state["commits"] if item["version"] == version)
location = urlparse(state["metadata"]["location"])
assert location.scheme == "file", location.scheme
log = Path(unquote(location.path)) / "_delta_log"
staged = log / "_staged_commits" / commit["file-name"]
assert staged.is_file()
canonical = log / f"{version:020d}.json"
if canonical.exists():
    assert canonical.read_bytes() == staged.read_bytes()
    canonical.rename(canonical.with_suffix(".json.backfill-held"))
# A CRC is optional and represents the published numbered version. Keep the two copies together.
checksum = canonical.with_suffix(".crc")
if checksum.exists():
    checksum.rename(checksum.with_suffix(".crc.backfill-held"))
assert not canonical.exists()
print(f"UC accepted v{version}; staged commit present and numbered copy withheld")
spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
result = spark.sql("SELECT id, label FROM doris_delta.m1.uc_managed ORDER BY id").collect()
assert [(r.id, r.label) for r in result] == [(1, "before"), (3, "after")], result
print("Spark managed rows:", result)
# Capture an independent Spark reference while the same latest commit is still staged.
if reference := os.environ.get("UC_TEST_HISTORY_REFERENCE"):
    history = spark.sql("DESCRIBE HISTORY doris_delta.m1.uc_managed").orderBy("version")
    projection = ["unix_millis(timestamp)" if name == "timestamp" else name for name in history.columns]
    rows = [list(row.asDict(recursive=True).values()) for row in history.selectExpr(*projection).collect()]
    Path(reference).write_text(json.dumps(rows), encoding="utf-8")
spark.stop()
