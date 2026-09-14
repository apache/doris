// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_iceberg_remove_orphan_files", "p0,external,doris,external_docker,external_docker_doris") {
    if (context.config.otherConfigs.get("enableIcebergTest") != "true") {
        logger.info("disable iceberg test.")
        return
    }
    String catalog = "test_iceberg_remove_orphans"
    String db = "test_db"
    String localDb = "test_iceberg_orphan_inventory"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${restPort}',
        's3.access_key'='admin', 's3.secret_key'='password', 's3.endpoint'='http://${host}:${minioPort}',
        's3.region'='us-east-1')"""
    sql "CREATE DATABASE IF NOT EXISTS ${catalog}.${db}"
    sql "DROP TABLE IF EXISTS ${catalog}.${db}.events"
    sql "CREATE TABLE ${catalog}.${db}.events (id BIGINT) ENGINE=iceberg"
    sql "INSERT INTO ${catalog}.${db}.events VALUES (1)"
    String target = "${catalog}.${db}.events"
    String live = sql("SELECT file_path FROM ${target}\$files LIMIT 1")[0][0].toString()
    int dataPosition = live.indexOf("/data/")
    assertTrue(dataPosition > 0)
    String root = live.substring(0, dataPosition)
    String old = root + "/data/aborted-for-orphan-test.parquet"

    sql "SWITCH internal"
    sql "CREATE DATABASE IF NOT EXISTS ${localDb}"
    sql "DROP TABLE IF EXISTS ${localDb}.files"
    sql """CREATE TABLE ${localDb}.files (file_path VARCHAR(2048), last_modified DATETIMEV2(6))
        DUPLICATE KEY(file_path) DISTRIBUTED BY HASH(file_path) BUCKETS 1
        PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO ${localDb}.files VALUES
        ('${live}', '2019-01-01'), ('${old}', '2019-01-01'), ('${old}', '2019-01-01'),
        ('${root}/data/boundary.parquet', '2020-01-01'),
        ('s3://unrelated-bucket/excluded.parquet', '2019-01-01'),
        (NULL, '2019-01-01'), ('${root}/data/null-time.parquet', NULL)"""
    sql "DROP VIEW IF EXISTS ${localDb}.inventory"
    sql "CREATE VIEW ${localDb}.inventory AS SELECT file_path,last_modified FROM ${localDb}.files"
    def candidates = sql """ALTER TABLE ${target} EXECUTE remove_orphan_files(
        'file_list_view'='internal.${localDb}.inventory', 'older_than'='2020-01-01', 'dry_run'='true')"""
    assertEquals([[old], [old]], candidates)
    assertEquals([[1]], sql("SELECT id FROM ${target}"))

    sql "SET sql_select_limit=1"
    sql "SET default_order_by_limit=1"
    sql "SET dry_run_query=true"
    try {
        assertEquals([[old], [old]], sql("""ALTER TABLE ${target} EXECUTE remove_orphan_files(
            'file_list_view'='internal.${localDb}.inventory', 'older_than'='2020-01-01', 'dry_run'='true')"""))
    } finally {
        sql "SET dry_run_query=false"
        sql "SET sql_select_limit=9223372036854775807"
        sql "SET default_order_by_limit=-1"
    }

    sql "DROP VIEW IF EXISTS ${localDb}.one_file"
    sql """CREATE VIEW ${localDb}.one_file AS SELECT file_path,last_modified FROM ${localDb}.files
        WHERE file_path='${old}' LIMIT 1"""
    assertEquals([[old]], sql("""ALTER TABLE ${target} EXECUTE remove_orphan_files(
        'file_list_view'='internal.${localDb}.one_file', 'older_than'='2020-01-01', 'dry_run'='true')"""))

    sql "SET @iceberg_orphan_keep='${old}'"
    String connection = sql("SELECT connection_id()")[0][0].toString()
    def sessionPredicates = [
        "@iceberg_orphan_keep IS NULL OR file_path<>@iceberg_orphan_keep",
        "connection_id()<>${connection} OR file_path<>'${old}'",
        "last_query_id()='Not Available' OR file_path<>'${old}'"
    ]
    sessionPredicates.eachWithIndex { predicate, index ->
        String view = "${localDb}.session_inventory_${index}"
        sql "DROP VIEW IF EXISTS ${view}"
        sql "CREATE VIEW ${view} AS SELECT file_path,last_modified FROM ${localDb}.files WHERE ${predicate}"
        assertEquals([], sql("""ALTER TABLE ${target} EXECUTE remove_orphan_files(
            'file_list_view'='internal.${view}', 'older_than'='2020-01-01', 'dry_run'='true')"""))
    }
    sql "SET @iceberg_orphan_keep=NULL"

    // Empty output retains the one-column result schema; a referenced file is never a candidate.
    sql "DROP VIEW IF EXISTS ${localDb}.live_only"
    sql """CREATE VIEW ${localDb}.live_only AS SELECT file_path,last_modified FROM ${localDb}.files
        WHERE file_path='${live}'"""
    assertEquals([], sql("""ALTER TABLE ${target} EXECUTE remove_orphan_files(
        'file_list_view'='internal.${localDb}.live_only', 'older_than'='2020-01-01', 'dry_run'='true')"""))

    sql "DROP VIEW IF EXISTS ${localDb}.many_files"
    sql """CREATE VIEW ${localDb}.many_files AS
        SELECT CONCAT('${root}/data/orphan-', CAST(number AS STRING)) AS file_path,
        CAST('2019-01-01' AS DATETIMEV2(6)) AS last_modified FROM numbers('number'='30000')"""
    def sampled = sql """ALTER TABLE ${target} EXECUTE remove_orphan_files(
        'file_list_view'='internal.${localDb}.many_files', 'older_than'='2020-01-01',
        'dry_run'='true', 'stream_results'='true')"""
    assertEquals(20000, sampled.size())
    assertEquals(20000, sampled.collect { it[0] }.toSet().size())
    assertEquals(30000, sql("""ALTER TABLE ${target} EXECUTE remove_orphan_files(
        'file_list_view'='internal.${localDb}.many_files', 'older_than'='2020-01-01',
        'dry_run'='true', 'stream_results'='false')""").size())

    sql "DROP VIEW IF EXISTS ${localDb}.bad_types"
    sql """CREATE VIEW ${localDb}.bad_types AS SELECT CAST('bad' AS STRING) AS file_path,
        CAST(1 AS BIGINT) AS last_modified"""
    test {
        sql """ALTER TABLE ${target} EXECUTE remove_orphan_files(
            'file_list_view'='internal.${localDb}.bad_types', 'dry_run'='true')"""
        exception "last_modified DATETIME/DATETIMEV2"
    }
    test {
        sql "ALTER TABLE ${target} EXECUTE remove_orphan_files('max_concurrent_deletes'='0')"
        exception "max_concurrent_deletes"
    }
    test {
        sql "ALTER TABLE ${target} EXECUTE remove_orphan_files('unknown'='true')"
        exception "Unknown argument"
    }
    test {
        sql "ALTER TABLE ${target} EXECUTE remove_orphan_files('dry_run'='true') WHERE id=1"
        exception "does not support WHERE"
    }
}
