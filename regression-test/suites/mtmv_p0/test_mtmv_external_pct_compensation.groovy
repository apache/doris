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

suite("test_mtmv_external_pct_compensation", "p0,external,mtmv,external_docker,external_docker_doris") {
    // A query answered partly from the MV and partly from a base partition that changed since the refresh
    // pins the base side to the partitions the snapshot records, by the whole key of each: an external
    // table partitioned by (x, d) whose MV is partitioned by d alone must be read as
    // (x = .. AND d = ..), not as the MV's d column carrying the first key value -- a value of x, which
    // no row of the partition has, so the partition's rows are read as none and its committed rows go
    // missing from the query.
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_mtmv_external_pct_compensation_catalog"
    String paimonDb = "test_mtmv_external_pct_compensation_db"
    String paimonTable = "test_mtmv_external_pct_compensation_e"
    String mvName = "test_mtmv_external_pct_compensation_mv"

    // Two January partitions (different x, same d) and two February ones. The February side is large so
    // that answering the query from the base table alone is visibly more expensive than joining the MV's
    // February partition with the base table's January ones.
    spark_paimon_multi """
        create database if not exists paimon.${paimonDb};
        drop table if exists paimon.${paimonDb}.${paimonTable};
        create table paimon.${paimonDb}.${paimonTable} (x date, d date, v int) using paimon
            partitioned by (x, d);
        insert into paimon.${paimonDb}.${paimonTable} (x, d, v) values
            (date '2021-01-01', date '2024-01-01', 10),
            (date '2022-01-01', date '2024-01-01', 7),
            (date '2021-01-01', date '2024-02-01', 20),
            (date '2022-01-01', date '2024-02-01', 1);
        insert into paimon.${paimonDb}.${paimonTable} (x, d, v)
            select date '2022-01-01', date '2024-02-01', 1 from range(199);
    """

    sql """drop materialized view if exists ${mvName}"""
    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh/',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1",
            "s3.path.style.access" = "true"
        );
    """
    sql """SET materialized_view_rewrite_enable_contain_external_table = true"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY(d)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "grace_period" = "0"
        )
        AS SELECT d, SUM(v) AS total FROM ${catalogName}.${paimonDb}.${paimonTable} GROUP BY d
    """
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_initial """SELECT d, total FROM ${mvName}"""

    def querySql = """SELECT d, SUM(v) AS total
        FROM ${catalogName}.${paimonDb}.${paimonTable}
        WHERE d IN ('2024-01-01', '2024-02-01')
        GROUP BY d"""

    // Committed after the refresh, so the MV's January partition no longer matches the base table's and the
    // query is answered from both: the untouchable February side from the MV, January from the base table.
    sql """INSERT INTO ${catalogName}.${paimonDb}.${paimonTable}
        VALUES ('2021-01-01', '2024-01-01', 5)"""

    mv_rewrite_success(querySql, mvName)
    explain {
        sql(querySql)
        contains "VUNION"
    }
    // January 10 + 7 (recorded) + 5 (committed since); February 20 + 199 * 1. Pinned by the first key
    // alone -- x's value on the d column -- the base side reads no January row and January is missing.
    order_qt_after_append """SELECT d, SUM(v) AS total
        FROM ${catalogName}.${paimonDb}.${paimonTable}
        WHERE d IN ('2024-01-01', '2024-02-01')
        GROUP BY d"""

    sql """drop materialized view if exists ${mvName}"""
    sql """drop catalog if exists ${catalogName}"""
    spark_paimon_multi """
        drop table if exists paimon.${paimonDb}.${paimonTable};
        drop database if exists paimon.${paimonDb};
    """
}
