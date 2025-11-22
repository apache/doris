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

suite("test_iceberg_mtmv_with_partition_evolution", "p0,external,iceberg,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog_name = "iceberg_mtmv_catalog_evolution"
    String dbName = "regression_test_mtmv_partition_evolution_p0"
    String icebergDb = "iceberg_mtmv_partition_evolution"

    sql """drop catalog if exists ${catalog_name} """
    sql """CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch internal"""
    sql """drop database if exists ${dbName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    sql """drop database if exists ${catalog_name}.${icebergDb} force"""
    sql """create database ${catalog_name}.${icebergDb}"""

    def assertRefreshFailed = { mvName, expectedError ->
        Thread.sleep(2000)
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg " +
                "from tasks('type'='mv') where MvDatabaseName = '${dbName}' and MvName = '${mvName}' order by CreateTime DESC"
        String status = "NULL"
        List<List<Object>> result = []
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000
        while (timeoutTimestamp > System.currentTimeMillis()
                && (status == 'PENDING' || status == 'RUNNING' || status == 'NULL')) {
            result = sql(showTasks)
            if (result.isEmpty()) {
                Thread.sleep(1000)
                continue
            }
            status = result.get(0).get(4).toString()
            Thread.sleep(1000)
        }
        logger.info("MTMV ${mvName} refresh result: " + result?.getAt(0))
        assertTrue(status == "FAILED", "Task should fail but got status: " + status)
        String errorMsg = result.get(0).get(7).toString()
        logger.info("MTMV ${mvName} error message: " + errorMsg)
        assertTrue(errorMsg.contains(expectedError),
                "Error message should contain '${expectedError}' but got: " + errorMsg)
    }

    // Test partition evolution cases
    // Test 1: Switch from day to year on same column - should fail (any partition evolution should fail)
    String evolutionTable1 = "evolution_day_to_year"
    String mvEvolution1 = "mv_evolution_day_to_year"
    sql """drop table if exists ${catalog_name}.${icebergDb}.${evolutionTable1}"""
    sql """drop materialized view if exists ${mvEvolution1}"""

    sql """
        CREATE TABLE ${catalog_name}.${icebergDb}.${evolutionTable1} (
          ts DATETIME,
          value INT)
        ENGINE=iceberg
        PARTITION BY LIST (DAY(ts)) ();
    """
    sql """insert into ${catalog_name}.${icebergDb}.${evolutionTable1} values ('2024-01-15 10:00:00', 1), ('2024-02-20 11:00:00', 2)"""
    sql """CREATE MATERIALIZED VIEW ${mvEvolution1} BUILD DEFERRED REFRESH AUTO ON MANUAL partition by(`ts`) DISTRIBUTED BY RANDOM BUCKETS 2  PROPERTIES ('replication_num' = '1') as SELECT * FROM ${catalog_name}.${icebergDb}.${evolutionTable1}"""
    sql """REFRESH MATERIALIZED VIEW ${mvEvolution1} complete"""
    waitingMTMVTaskFinishedByMvName(mvEvolution1, dbName)
    qt_evolution_day_to_year_initial "select * from ${mvEvolution1} order by value"

    // Evolve partition from day to year on same column
    sql """ALTER TABLE ${catalog_name}.${icebergDb}.${evolutionTable1} DROP PARTITION KEY day(ts)"""
    sql """ALTER TABLE ${catalog_name}.${icebergDb}.${evolutionTable1} ADD PARTITION KEY year(ts)"""
    sql """insert into ${catalog_name}.${icebergDb}.${evolutionTable1} values ('2024-03-10 12:00:00', 3)"""

    sql """REFRESH MATERIALIZED VIEW ${mvEvolution1} complete"""
    assertRefreshFailed(mvEvolution1, "is not a valid pct table anymore")

    sql """drop materialized view if exists ${mvEvolution1}"""
    sql """drop table if exists ${catalog_name}.${icebergDb}.${evolutionTable1}"""

    // Test 2: Change partition column from c1 to c2 - should fail
    String evolutionTable2 = "evolution_change_column"
    String mvEvolution2 = "mv_evolution_change_column"
    sql """drop table if exists ${catalog_name}.${icebergDb}.${evolutionTable2}"""
    sql """drop materialized view if exists ${mvEvolution2}"""

    sql """
        CREATE TABLE ${catalog_name}.${icebergDb}.${evolutionTable2} (
          c1 DATETIME,
          c2 DATETIME,
          value INT)
        ENGINE=iceberg
        PARTITION BY LIST (YEAR(c1)) ();
    """
    sql """insert into ${catalog_name}.${icebergDb}.${evolutionTable2} values ('2024-01-15 10:00:00', '2024-01-16 10:00:00', 1), ('2024-02-20 11:00:00', '2024-02-21 11:00:00', 2)"""
    sql """CREATE MATERIALIZED VIEW ${mvEvolution2} BUILD DEFERRED REFRESH AUTO ON MANUAL partition by(`c1`) DISTRIBUTED BY RANDOM BUCKETS 2  PROPERTIES ('replication_num' = '1') as SELECT * FROM ${catalog_name}.${icebergDb}.${evolutionTable2}"""
    sql """REFRESH MATERIALIZED VIEW ${mvEvolution2} complete"""
    waitingMTMVTaskFinishedByMvName(mvEvolution2, dbName)
    qt_evolution_change_column_initial "select * from ${mvEvolution2} order by value"

    // Evolve partition from c1 to c2 - this should make the table invalid
    sql """ALTER TABLE ${catalog_name}.${icebergDb}.${evolutionTable2} DROP PARTITION KEY year(c1)"""
    sql """ALTER TABLE ${catalog_name}.${icebergDb}.${evolutionTable2} ADD PARTITION KEY year(c2)"""
    sql """insert into ${catalog_name}.${icebergDb}.${evolutionTable2} values ('2024-03-10 12:00:00', '2024-03-11 12:00:00', 3)"""

    sql """REFRESH MATERIALIZED VIEW ${mvEvolution2} complete"""
    assertRefreshFailed(mvEvolution2, "is not a valid pct table anymore")

    sql """drop materialized view if exists ${mvEvolution2}"""
    sql """drop table if exists ${catalog_name}.${icebergDb}.${evolutionTable2}"""

    // Test 3: Switch from year to identity - should fail
    String evolutionTable3 = "evolution_year_to_identity"
    String mvEvolution3 = "mv_evolution_year_to_identity"
    sql """drop table if exists ${catalog_name}.${icebergDb}.${evolutionTable3}"""
    sql """drop materialized view if exists ${mvEvolution3}"""

    sql """
        CREATE TABLE ${catalog_name}.${icebergDb}.${evolutionTable3} (
          ts DATETIME,
          value INT)
        ENGINE=iceberg
        PARTITION BY LIST (YEAR(ts)) ();
    """
    sql """insert into ${catalog_name}.${icebergDb}.${evolutionTable3} values ('2024-01-15 10:00:00', 1), ('2024-02-20 11:00:00', 2)"""
    sql """CREATE MATERIALIZED VIEW ${mvEvolution3} BUILD DEFERRED REFRESH AUTO ON MANUAL partition by(`ts`) DISTRIBUTED BY RANDOM BUCKETS 2  PROPERTIES ('replication_num' = '1') as SELECT * FROM ${catalog_name}.${icebergDb}.${evolutionTable3}"""
    sql """REFRESH MATERIALIZED VIEW ${mvEvolution3} complete"""
    waitingMTMVTaskFinishedByMvName(mvEvolution3, dbName)
    qt_evolution_year_to_identity_initial "select * from ${mvEvolution3} order by value"

    // Evolve partition from year to identity - this should make the table invalid
    sql """ALTER TABLE ${catalog_name}.${icebergDb}.${evolutionTable3} DROP PARTITION KEY year(ts)"""
    sql """ALTER TABLE ${catalog_name}.${icebergDb}.${evolutionTable3} ADD PARTITION KEY ts"""

    sql """REFRESH MATERIALIZED VIEW ${mvEvolution3} complete"""
    assertRefreshFailed(mvEvolution3, "is not a valid pct table anymore")

    sql """drop materialized view if exists ${mvEvolution3}"""
    sql """drop table if exists ${catalog_name}.${icebergDb}.${evolutionTable3}"""

    sql """ drop catalog if exists ${catalog_name} """
}

