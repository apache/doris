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

"""Prepare the managed history and external statistics fixtures in local OSS Unity Catalog."""

import argparse
import json
import os
import time
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser(description=__doc__)
mode = parser.add_mutually_exclusive_group()
mode.add_argument("--cdf", action="store_true", help="Prepare a managed change feed with an unbackfilled commit")
mode.add_argument("--history", action="store_true", help="Prepare uc_managed_history for the version-as-of suite")
mode.add_argument("--time-travel-edges", type=Path, metavar="NEW_DIR",
                  help="Prepare the dedicated UC and local filesystem time-travel regression fixtures")
mode.add_argument("--column-statistics-external", type=Path, metavar="SHARED_DIR",
                  help="Prepare the external table for the Unity Catalog column-statistics regression")
args = parser.parse_args()
spark = (SparkSession.builder.appName("doris-delta-uc-fixture")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.catalog.doris_delta", "io.unitycatalog.spark.UCSingleCatalog")
         .config("spark.sql.catalog.doris_delta.uri", os.environ["UC_TEST_SERVER"])
         .config("spark.sql.catalog.doris_delta.auth.type", "static")
         .config("spark.sql.catalog.doris_delta.auth.token", "local-test")
         .config("spark.sql.defaultCatalog", "doris_delta")
         .getOrCreate())
api = os.environ["UC_TEST_SERVER"].rstrip("/") + "/api/2.1/unity-catalog"


def seed_table(name):
    table = f"doris_delta.m1.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} (id BIGINT, label STRING) USING DELTA")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'before'), (2, 'removed')")
    spark.sql(f"DELETE FROM {table} WHERE id = 2")
    spark.sql(f"INSERT INTO {table} VALUES (3, 'after')")


def hold_numbered_copies(name, versions, wait_for_writer):
    with urlopen(f"{api}/delta/v1/catalogs/doris_delta/schemas/m1/tables/{name}", timeout=15) as response:
        state = json.load(response)
    latest = versions[-1]
    assert state["latest-table-version"] == latest, state["latest-table-version"]
    commits = {item["version"]: item["file-name"] for item in state["commits"]}
    assert set(versions) <= commits.keys(), (versions, list(commits))
    location = urlparse(state["metadata"]["location"])
    assert location.scheme == "file", location.scheme
    log = Path(unquote(location.path)) / "_delta_log"

    if wait_for_writer:
        # Spark backfills asynchronously in version order. The history fixture needs the last
        # publication to finish before it can hold this batch aside and proceed to the next phase.
        last_copy = log / f"{latest:020d}.json"
        deadline = time.monotonic() + 30
        while not last_copy.is_file():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Spark did not publish {last_copy}")
            time.sleep(0.1)
    for version in versions:
        staged = log / "_staged_commits" / commits[version]
        assert staged.is_file(), staged
        numbered = log / f"{version:020d}.json"
        if numbered.exists():
            assert numbered.read_bytes() == staged.read_bytes(), numbered
            numbered.replace(numbered.with_suffix(".json.backfill-held"))
        crc = numbered.with_suffix(".crc")
        if crc.exists():
            crc.replace(crc.with_suffix(".crc.backfill-held"))
    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    return log


def prepare_cdf():
    name = "uc_managed_cdf"
    table = f"doris_delta.m1.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"""CREATE TABLE {table} (id INT, p INT) USING DELTA PARTITIONED BY (p)
              TBLPROPERTIES ('delta.enableChangeDataFeed'='true')""")
    spark.sql(f"INSERT INTO {table} VALUES (11,10),(21,20)")
    spark.sql(f"UPDATE {table} SET id=12 WHERE id=11")
    end = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first().version
    log = hold_numbered_copies(name, [end], wait_for_writer=True)
    result = spark.sql(f"""SELECT id,p,_change_type,_commit_version,unix_millis(_commit_timestamp)
                       FROM table_changes('{table}',0,{end}) ORDER BY _commit_version,id,_change_type""")
    (log.parent / "_doris_cdf_reference.json").write_text(
        json.dumps([list(row) for row in result.collect()]), encoding="utf-8")


def prepare_history():
    name = "uc_managed_history"
    table = f"doris_delta.m1.{name}"
    seed_table(name)
    # UC must still know about v3 when the writer ratifies v4.
    hold_numbered_copies(name, [3], wait_for_writer=True)
    spark.sql(f"INSERT INTO {table} VALUES (4, 'newest')")
    log = hold_numbered_copies(name, [3, 4], wait_for_writer=True)

    expected = {}
    for tag in ("0", "1", "2", "3", "4", "latest"):
        at = "" if tag == "latest" else f" VERSION AS OF {tag}"
        frame = spark.sql(f"SELECT * FROM {table}{at} ORDER BY id")
        expected[tag] = {"schema": frame.dtypes, "rows": frame.collect(), "count": frame.count()}
    only_in_v4 = spark.sql(f"""SELECT n.id FROM {table} VERSION AS OF 4 n
        LEFT JOIN {table} VERSION AS OF 3 o ON n.id = o.id WHERE o.id IS NULL ORDER BY n.id""").collect()
    assert [row.id for row in only_in_v4] == [4], only_in_v4
    try:
        spark.sql(f"SELECT * FROM {table} VERSION AS OF 5").collect()
    except AnalysisException as error:
        assert error.getCondition() == "DELTA_VERSION_NOT_FOUND", error
    else:
        raise AssertionError("Spark accepted uncommitted version 5")

    # Publish the reference only after every Spark query on this table has succeeded.
    reference = log.parent / "_doris_history_expected.json"
    pending = reference.with_suffix(".tmp")
    pending.write_text(json.dumps({"versions": expected, "only_in_v4": only_in_v4}), encoding="utf-8")
    pending.replace(reference)


try:
    spark.sql("CREATE SCHEMA IF NOT EXISTS doris_delta.m1")
    if args.time_travel_edges is not None:
        from time_travel_edges import prepare
        prepare(spark, args.time_travel_edges, hold_numbered_copies)
    elif args.column_statistics_external is not None:
        location = args.column_statistics_external.resolve()
        table = "doris_delta.m1.uc_external_statistics"
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{location.as_uri()}'
                  AS SELECT * FROM VALUES (1L, 'first'), (2L, CAST(NULL AS STRING)), (3L, 'last') AS t(id, label)""")
        detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
        target = urlparse(detail.location)
        assert target.scheme == "file" and Path(unquote(target.path)).resolve() == location
    elif args.cdf:
        prepare_cdf()
    elif args.history:
        prepare_history()
    else:
        prepare_cdf()
        seed_table("uc_managed")
        hold_numbered_copies("uc_managed", [3], wait_for_writer=False)
        result = spark.sql("SELECT id, label FROM doris_delta.m1.uc_managed ORDER BY id").collect()
        assert [(r.id, r.label) for r in result] == [(1, "before"), (3, "after")], result
        print("Spark managed rows:", result)
        # Capture an independent Spark reference while the same latest commit is still staged.
        if reference := os.environ.get("UC_TEST_HISTORY_REFERENCE"):
            history = spark.sql("DESCRIBE HISTORY doris_delta.m1.uc_managed").orderBy("version")
            projection = ["unix_millis(timestamp)" if name == "timestamp" else name for name in history.columns]
            rows = [list(row.asDict(recursive=True).values()) for row in history.selectExpr(*projection).collect()]
            Path(reference).write_text(json.dumps(rows), encoding="utf-8")
finally:
    spark.stop()
