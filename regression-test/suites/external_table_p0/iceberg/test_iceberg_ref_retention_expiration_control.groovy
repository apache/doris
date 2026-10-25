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

suite("test_iceberg_ref_retention_expiration_control",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("Iceberg test is disabled")
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS iceberg_ref_retention_control"
    sql """CREATE CATALOG iceberg_ref_retention_control PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'rest',
        'uri' = 'http://${host}:${restPort}',
        's3.endpoint' = 'http://${host}:${minioPort}',
        's3.access_key' = 'admin', 's3.secret_key' = 'password',
        's3.region' = 'us-east-1')"""
    sql "SWITCH iceberg_ref_retention_control"
    sql "CREATE DATABASE IF NOT EXISTS iceberg_ref_retention_control_db"
    sql "USE iceberg_ref_retention_control_db"
    sql "DROP TABLE IF EXISTS retention_control"
    sql "CREATE TABLE retention_control (id INT, payload STRING) PROPERTIES ('format-version' = '2')"

    def currentSnapshot = {
        sql("SELECT snapshot_id FROM retention_control\$refs WHERE name = 'main'")[0][0].toString()
    }
    sql "INSERT INTO retention_control VALUES (1, 'tagged')"
    String tagSnapshot = currentSnapshot()
    sql "ALTER TABLE retention_control CREATE TAG protected_tag AS OF VERSION ${tagSnapshot}"
    def tagRows = sql("SELECT * FROM retention_control@tag(protected_tag) ORDER BY id")
    sql "INSERT INTO retention_control VALUES (2, 'branched')"
    String branchSnapshot = currentSnapshot()
    sql """ALTER TABLE retention_control CREATE BRANCH protected_branch
        AS OF VERSION ${branchSnapshot} WITH SNAPSHOT RETENTION 1 SNAPSHOTS"""
    def branchRows = sql("SELECT * FROM retention_control@branch(protected_branch) ORDER BY id")
    sql "INSERT INTO retention_control VALUES (3, 'unreferenced')"
    String unreferencedSnapshot = currentSnapshot()
    sql "INSERT INTO retention_control VALUES (4, 'current')"
    String mainSnapshot = currentSnapshot()
    def mainRows = sql("SELECT * FROM retention_control ORDER BY id")
    def refsBefore = sql("SELECT name, type, snapshot_id FROM retention_control\$refs ORDER BY name")

    // A cutoff older than the freshly written snapshots cannot test retention protection. The
    // unreferenced snapshot is a positive control proving that this call actually expires history.
    sql """ALTER TABLE retention_control EXECUTE expire_snapshots(
        'older_than' = '2100-01-01T00:00:00', 'retain_last' = '1')"""
    def retained = sql("SELECT snapshot_id FROM retention_control\$snapshots ORDER BY snapshot_id")
            .collect { it[0].toString() }
    assertEquals([tagSnapshot, branchSnapshot, mainSnapshot].sort(), retained.sort())
    assertFalse(retained.contains(unreferencedSnapshot))
    assertEquals(refsBefore,
            sql("SELECT name, type, snapshot_id FROM retention_control\$refs ORDER BY name"))
    assertEquals(tagRows, sql("SELECT * FROM retention_control@tag(protected_tag) ORDER BY id"))
    assertEquals(branchRows, sql("SELECT * FROM retention_control@branch(protected_branch) ORDER BY id"))
    assertEquals(mainRows, sql("SELECT * FROM retention_control ORDER BY id"))
    spark_iceberg "REFRESH TABLE demo.iceberg_ref_retention_control_db.retention_control"
    assertSparkDorisResultEquals(
            spark_iceberg("SELECT * FROM demo.iceberg_ref_retention_control_db.retention_control ORDER BY id"),
            mainRows)
}
