#!/usr/bin/env python3
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

"""Prepare isolated HMS metadata, a Doris catalog and a regression configuration."""

import argparse
import copy
import json
import os
import re
import time
from pathlib import Path
from urllib.request import urlopen

import pymysql
from hmsclient import hmsclient
from hmsclient.genthrift.hive_metastore import ttypes


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise argparse.ArgumentTypeError("Use a simple SQL identifier")
    return value


def prepare_hms(args, file_bytes):
    location = "s3a://%s/data" % args.bucket
    marker = "doris.split.early.stop.fixture"
    with hmsclient.HMSClient(host=args.hms_host, port=args.hms_port) as client:
        if args.database not in client.get_all_databases():
            client.create_database(ttypes.Database(name=args.database, locationUri=location,
                                                   description="Split early-stop acceptance fixture", parameters={}))
        if args.table not in client.get_all_tables(args.database):
            sd = ttypes.StorageDescriptor(cols=[ttypes.FieldSchema(name="id", type="bigint")],
                    location=location, inputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    compressed=False, numBuckets=-1, serdeInfo=ttypes.SerDeInfo(
                        serializationLib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", parameters={}),
                    bucketCols=[], sortCols=[], parameters={})
            count = args.partitions * args.files_per_partition
            client.create_table(ttypes.Table(dbName=args.database, tableName=args.table, owner="hive",
                    createTime=int(time.time()), lastAccessTime=0, retention=0, sd=sd,
                    partitionKeys=[ttypes.FieldSchema(name="p", type="int")], tableType="EXTERNAL_TABLE",
                    parameters={"EXTERNAL": "TRUE", marker: "1", "numRows": str(count),
                                "numFiles": str(count), "totalSize": str(count * file_bytes)}))
        table = client.get_table(args.database, args.table)
        if table.parameters.get(marker) != "1" or table.sd.location != location:
            raise ValueError("Refusing to reuse an existing table that is not this fixture")
        if int(table.parameters["numRows"]) != args.partitions * args.files_per_partition:
            raise ValueError("Existing fixture table has a different shape; use another table name")
        existing = set(client.get_partition_names(args.database, args.table, -1))
        expected = {"p=%d" % p for p in range(args.partitions)}
        if existing - expected:
            raise ValueError("Existing fixture contains unexpected partitions")
        pending = []
        for number in range(args.partitions):
            if "p=%d" % number in existing:
                continue
            sd = copy.deepcopy(table.sd)
            sd.location = location + "/p=%d" % number
            pending.append(ttypes.Partition(values=[str(number)], dbName=args.database, tableName=args.table,
                    createTime=int(time.time()), lastAccessTime=0, sd=sd, parameters={
                        "numRows": str(args.files_per_partition), "numFiles": str(args.files_per_partition),
                        "totalSize": str(args.files_per_partition * file_bytes)}))
            if len(pending) == 100:
                client.add_partitions(pending)
                pending.clear()
        if pending:
            client.add_partitions(pending)
        actual = len(client.get_partition_names(args.database, args.table, -1))
        if actual != args.partitions:
            raise ValueError("Unexpected HMS partition count: %d" % actual)
    return actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hms-host", default="127.0.0.1")
    parser.add_argument("--hms-port", type=int, required=True)
    parser.add_argument("--fe-host", default="127.0.0.1")
    parser.add_argument("--fe-port", type=int, required=True)
    parser.add_argument("--fe-http-port", type=int, required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password-env", default="DORIS_TEST_PASSWORD")
    parser.add_argument("--fixture-url", required=True)
    parser.add_argument("--database", type=identifier, default="doris_split_early_stop")
    parser.add_argument("--table", type=identifier, default="files_10000_partitions_100_files")
    parser.add_argument("--catalog", type=identifier, default="hive_split_early_stop")
    parser.add_argument("--bucket", default="stream-split-test")
    parser.add_argument("--partitions", type=int, default=10000)
    parser.add_argument("--files-per-partition", type=int, default=100)
    parser.add_argument("--file-cache", action="store_true", help="Enable Hive's shared file-list cache")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.partitions <= 0 or args.files_per_partition <= 0:
        parser.error("Fixture shape must be positive")
    args.fixture_url = args.fixture_url.rstrip("/")
    with urlopen(args.fixture_url + "/__metrics", timeout=5) as response:
        fixture = json.load(response)["fixture"]
    if fixture["mode"] != "logical_files":
        parser.error("prepare creates logical-fixture metadata; configure an existing table for a physical proxy")
    if fixture["files_per_partition"] != args.files_per_partition or fixture["bucket"] != args.bucket:
        parser.error("files-per-partition and bucket must match the running fixture")
    args.output.mkdir(parents=True, exist_ok=True)
    partitions = prepare_hms(args, fixture["file_bytes"])
    conn = pymysql.connect(host=args.fe_host, port=args.fe_port, user=args.user,
                           password=os.environ.get(args.password_env, ""))
    properties = {"type": "hms", "hive.metastore.uris": "thrift://%s:%d" % (args.hms_host, args.hms_port),
                  "s3.endpoint": args.fixture_url, "s3.region": "us-east-1", "s3.access_key": "test",
                  "s3.secret_key": "test", "use_path_style": "true", "enable_auto_analyze": "false",
                  "meta.cache.hive.file.enable": str(args.file_cache).lower()}
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("show catalogs")
            if any(row["CatalogName"] == args.catalog for row in cursor.fetchall()):
                raise ValueError("Catalog already exists; use a fresh --catalog name or drop your earlier fixture")
            sql_properties = ",".join("%s=%s" % (json.dumps(k), json.dumps(v)) for k, v in properties.items())
            cursor.execute("create catalog `%s` properties (%s)" % (args.catalog, sql_properties))
            cursor.execute("show backends")
            backends = cursor.fetchall()
            (args.output / "backends.json").write_text(json.dumps(backends, default=str, indent=2) + "\n")
    finally:
        conn.close()
    repo = Path(__file__).resolve().parents[3]
    config = (repo / "regression-test/conf/regression-conf.groovy").read_text()
    overrides = {"jdbcUrl": "jdbc:mysql://%s:%d/?useLocalSessionState=true&allowLoadLocalInfile=true"
                 % (args.fe_host, args.fe_port), "jdbcUser": args.user,
                 "jdbcPassword": os.environ.get(args.password_env, ""),
                 "feHttpAddress": "%s:%d" % (args.fe_host, args.fe_http_port),
                 "feHttpUser": args.user, "feHttpPassword": os.environ.get(args.password_env, ""),
                 "hiveSplitEarlyStopControlUrl": args.fixture_url, "hiveSplitEarlyStopCatalog": args.catalog,
                 "hiveSplitEarlyStopTable": "%s.%s.%s" % (args.catalog, args.database, args.table)}
    for key, value in overrides.items():
        # Groovy double-quoted strings interpolate '$'; the JSON string otherwise preserves escapes.
        config += "\n%s = %s\n" % (key, json.dumps(value).replace("$", "\\$"))
    config_path = args.output / "regression-conf.groovy"
    config_path.write_text(config)
    config_path.chmod(0o600)
    (args.output / "hms-fixture.json").write_text(json.dumps({
        "mode": "logical_files", "hms_host": args.hms_host, "hms_port": args.hms_port,
        "database": args.database, "table": args.table, "catalog": args.catalog,
        "partitions": partitions, "files_per_partition": args.files_per_partition,
        "logical_files": partitions * args.files_per_partition}, indent=2) + "\n")
    print("Prepared %d partitions; config: %s" % (partitions, config_path))


if __name__ == "__main__":
    main()
