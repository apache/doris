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

"""Build Spark reference snapshots for test_deltalake_system_tables.

Run with the Delta 4.4 Spark fixture classpath and S3A configuration:
spark-submit system_tables.py REFERENCE.json --warehouse s3a://delta/warehouse [--uc-uri http://localhost:8080]
Set deltalakeSystemTablesFixture to the resulting file in the local regression configuration.
"""
import argparse
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

from pyspark.sql import SparkSession


def milliseconds(value):
    if value is None:
        return None
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (value.replace(tzinfo=timezone.utc) - epoch) // timedelta(milliseconds=1)


def scala_values(values):
    it = values.iterator()
    while it.hasNext():
        yield it.next()


def capture(spark, table, path_based):
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict(recursive=True)
    for field in ("createdAt", "lastModified"):
        detail[field] = milliseconds(detail[field])
    latest = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
    jvm = spark._jvm
    delta = jvm.io.delta.tables.DeltaTable
    source = (delta.forPath(spark._jsparkSession, detail["location"]) if path_based
              else delta.forName(spark._jsparkSession, table))
    snapshot = source.deltaLog().unsafeVolatileSnapshot()
    assert snapshot.version() == latest.version, (snapshot.version(), latest.version)
    protocol = snapshot.protocol()
    features = set()
    for feature_set in (protocol.readerFeatures(), protocol.writerFeatures()):
        if feature_set.isDefined():
            features.update(scala_values(feature_set.get()))
    physical = {}
    for name in detail["partitionColumns"]:
        metadata = snapshot.metadata().schema().apply(name).metadata()
        key = "delta.columnMapping.physicalName"
        physical[name] = metadata.getString(key) if metadata.contains(key) else name
    active = []
    partition_sums = {}
    for add in snapshot.allFiles().collectAsList():
        partition = {p: add.partitionValues().apply(physical[p]) for p in detail["partitionColumns"]}
        dv = add.deletionVector()
        dv_path = None
        if dv is not None and not dv.isInline():
            dv_path = dv.absolutePath(jvm.org.apache.hadoop.fs.Path(detail["location"])).toString()
        raw_stats = json.loads(add.stats()) if add.stats() else {}
        data_path = add.absolutePath(snapshot).toString()
        active.append([data_path, add.size(), add.modificationTime(), partition, raw_stats.get("numRecords"),
                       dv is not None, dv.cardinality() if dv else 0,
                       dv_path, dv.storageType() if dv else None])
        key = json.dumps(partition, sort_keys=True)
        totals = partition_sums.setdefault(key, [partition, 0, 0, 0, 0])
        totals[1] += 1
        totals[2] += add.size()
        totals[3] = (totals[3] + raw_stats["numRecords"]
                     if totals[3] is not None and "numRecords" in raw_stats else None)
        totals[4] += dv.cardinality() if dv else 0
    count = spark.table(table).count()
    assert len(active) == detail["numFiles"] and sum(f[1] for f in active) == detail["sizeInBytes"]
    assert sum(f[4] - f[6] for f in active) == count
    return dict(version=latest.version, timestamp=milliseconds(latest.timestamp), detail=detail,
                explicitFeatures=sorted(features), files=active, partitions=list(partition_sums.values()), count=count)


def write_fixture(spark, table, path_based):
    spark.sql(f"""CREATE TABLE {table} (id BIGINT, p STRING) USING DELTA PARTITIONED BY (p)
                  COMMENT 'Doris system table fixture'
                  TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'='true',
                                 'delta.enableInCommitTimestamps'='true', 'stage'='initial')""")
    versions = [capture(spark, table, path_based)]
    spark.sql(f"""INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES
                  (1L,'a'),(2L,'a'),(3L,'b'),(4L,NULL) t(id,p)""")
    spark.sql(f"INSERT INTO {table} VALUES (5L,'a'),(6L,'c')")
    versions.append(capture(spark, table, path_based))
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    # OSS UC rejects partition-column renames. Its mapped table still uses physical keys on disk.
    partition = "p"
    if path_based:
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN p TO region")
        partition = "region"
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('stage'='final')")
    spark.sql(f"DELETE FROM {table} WHERE {partition} = 'b'")
    versions.append(capture(spark, table, path_based))
    assert any(f[5] for f in versions[-1]["files"]), "fixture must contain an active DV"
    return versions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--uc-uri", default=os.environ.get("UC_TEST_SERVER"))
    args = parser.parse_args()
    os.environ["TZ"] = "UTC"
    time.tzset()
    builder = (SparkSession.builder.appName("doris-delta-system-tables")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    if args.uc_uri:
        builder = (builder.config("spark.sql.catalog.doris_delta", "io.unitycatalog.spark.UCSingleCatalog")
                   .config("spark.sql.catalog.doris_delta.uri", args.uc_uri)
                   .config("spark.sql.catalog.doris_delta.auth.type", "static")
                   .config("spark.sql.catalog.doris_delta.auth.token", "local-test"))
    spark = builder.getOrCreate()
    name = "sys_" + uuid.uuid4().hex[:12]
    try:
        base = args.warehouse.rstrip("/") + "/delta_system_tables/" + name
        versions = write_fixture(spark, f"delta.`{base}`", True)
        jvm = spark._jvm
        crc = jvm.org.apache.hadoop.fs.Path(f"{base}/_delta_log/{versions[-1]['version']:020d}.crc")
        fs = crc.getFileSystem(spark._jsc.hadoopConfiguration())
        if fs.exists(crc):
            assert fs.rename(crc, jvm.org.apache.hadoop.fs.Path(crc.toString() + ".held"))
        assert not fs.exists(crc)
        reference = {"filesystem": dict(database="delta_system_tables", table=name, versions=versions)}
        clustered = name + "_clustered"
        clustered_path = args.warehouse.rstrip("/") + "/delta_system_tables/" + clustered
        spark.sql(f"""CREATE TABLE delta.`{clustered_path}` (id BIGINT, details STRUCT<rank: INT>)
                      USING DELTA CLUSTER BY (details.rank, id)
                      TBLPROPERTIES ('delta.columnMapping.mode'='name')""")
        clustering = spark.sql(f"DESCRIBE DETAIL delta.`{clustered_path}`").first().clusteringColumns
        assert len(clustering) == 2
        reference["clustered"] = dict(database="delta_system_tables", table=clustered, columns=clustering)
        widened = name + "_wide"
        wide_table = f"delta.`{args.warehouse.rstrip('/')}/delta_system_tables/{widened}`"
        spark.sql(f"""CREATE TABLE {wide_table} (id BIGINT, p INT) USING DELTA PARTITIONED BY (p)
                      TBLPROPERTIES ('delta.enableTypeWidening'='true')""")
        spark.sql(f"INSERT INTO {wide_table} VALUES (1L,123),(2L,NULL)")
        spark.sql(f"ALTER TABLE {wide_table} ALTER COLUMN p TYPE BIGINT")
        spark.sql(f"INSERT INTO {wide_table} VALUES (3L,4294967297L)")
        rows = spark.sql(f"SELECT DISTINCT CAST(p AS STRING) AS p FROM {wide_table} ORDER BY p").collect()
        values = [row.p for row in rows]
        reference["widened"] = dict(database="delta_system_tables", table=widened, values=values)
        if args.uc_uri:
            table = f"doris_delta.m1.{name}"
            versions = write_fixture(spark, table, False)
            endpoint = (args.uc_uri.rstrip("/") + "/api/2.1/unity-catalog/delta/v1/"
                        + f"catalogs/doris_delta/schemas/m1/tables/{name}")
            with urlopen(endpoint, timeout=15) as response:
                info = json.load(response)
            v = versions[-1]["version"]
            commit = next(c for c in info["commits"] if c["version"] == v)
            loc = urlparse(info["metadata"]["location"])
            assert loc.scheme == "file"
            log = Path(unquote(loc.path)) / "_delta_log"
            staged = log / "_staged_commits" / commit["file-name"]
            assert staged.is_file()
            numbered = log / f"{v:020d}.json"
            if numbered.exists():
                assert numbered.read_bytes() == staged.read_bytes()
                numbered.rename(numbered.with_suffix(".json.held"))
            crc = numbered.with_suffix(".crc")
            if crc.exists():
                crc.rename(crc.with_suffix(".crc.held"))
            assert not numbered.exists() and not crc.exists()
            reference["uc"] = dict(database="m1", table=name, versions=versions)
        pending = args.reference.with_suffix(".tmp")
        pending.write_text(json.dumps(reference, ensure_ascii=False), encoding="utf-8")
        pending.replace(args.reference)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
