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

suite("test_mtmv_external_pct_read_scope", "p0,external,mtmv,external_docker,external_docker_doris") {
    // A refresh reads each table the MV is partitioned on -- olap or external -- from exactly the base
    // partitions its snapshot records for the MV partition (MTMVPartitionUtil#mappedBasePartitions). The sync
    // window can drop an external table's partition from that mapping while an olap partition it meets is
    // kept whole, so the MV partition's own keys still name a value only the external table was windowed out
    // of. Reading such a table through the MV partition's key range instead reads the windowed-out
    // partition, records no partition for it, and leaves the rows it materialized in the MV partition when
    // the base side later drops it -- while the partition is still judged synchronized, so no refresh plans
    // it again.
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_mtmv_external_pct_read_scope_catalog"
    String paimonDb = "test_mtmv_external_pct_read_scope_db"
    String paimonTable = "test_mtmv_external_pct_read_scope_e"
    String aTable = "test_mtmv_external_pct_read_scope_a"
    String mvName = "test_mtmv_external_pct_read_scope_mv"
    String dbName = context.config.getDbNameByFile(context.file)

    // The external side: one partition per d, the 2020 one outside the 2 YEAR window below, the 2038 one
    // inside it.
    spark_paimon_multi """
        create database if not exists paimon.${paimonDb};
        drop table if exists paimon.${paimonDb}.${paimonTable};
        create table paimon.${paimonDb}.${paimonTable} (k int, v int, d date) using paimon partitioned by (d);
        insert into paimon.${paimonDb}.${paimonTable}
            values (1, 10, date '2020-01-01'), (1, 20, date '2038-01-01');
    """

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${aTable}"""
    // The olap side holds both keys in one partition, and a multi-key list partition survives the window
    // whole when any of its keys is inside it: the MV partition the two tables merge into keeps 2020 in its
    // own keys while the external table's 2020 partition is windowed out of the mapping.
    sql """
        CREATE TABLE ${aTable} (
            d DATE NOT NULL,
            k INT NOT NULL,
            v INT
        )
        UNIQUE KEY(d, k)
        PARTITION BY LIST(d) (
            PARTITION a_kept VALUES IN (("2020-01-01"), ("2038-01-01"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """
    sql """INSERT INTO ${aTable} VALUES ("2020-01-01", 1, 1), ("2038-01-01", 1, 2)"""

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

    order_qt_base_join """SELECT a.d, a.k, e.v FROM ${aTable} a
        JOIN ${catalogName}.${paimonDb}.${paimonTable} e ON a.d = e.d"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY(d)
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "partition_sync_limit" = "2",
            "partition_sync_time_unit" = "YEAR",
            "refresh_partition_num" = "1"
        )
        AS SELECT a.d AS d, a.k AS k, e.v AS v
           FROM ${aTable} a
           JOIN ${catalogName}.${paimonDb}.${paimonTable} e ON a.d = e.d
    """
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)

    // Only the 2038 partition is read from the external side, so the olap table's 2020 row -- which the MV
    // partition's own keys do name -- must not reach it.
    order_qt_initial_mv """SELECT d, k, v FROM ${mvName}"""

    // Dropping the windowed-out partition remotely changes no partition this MV partition is recorded with,
    // so it stays synchronized; that is only correct because the rows of that partition were never read.
    spark_paimon_multi """
        alter table paimon.${paimonDb}.${paimonTable} drop partition (d = date '2020-01-01');
    """
    sql """refresh catalog ${catalogName}"""

    order_qt_after_drop_base_join """SELECT a.d, a.k, e.v FROM ${aTable} a
        JOIN ${catalogName}.${paimonDb}.${paimonTable} e ON a.d = e.d"""
    order_qt_after_drop_mv """SELECT d, k, v FROM ${mvName}"""
    order_qt_after_drop_sync """SELECT PartitionName, SyncWithBaseTables FROM
        partitions('catalog' = 'internal', 'database' = '${dbName}', 'table' = '${mvName}')"""

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${aTable}"""
    sql """drop catalog if exists ${catalogName}"""
}
