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

suite("test_paimon_mtmv", "p0,external,mtmv,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }
    String suiteName = "test_paimon_mtmv"
    String catalogName = "${suiteName}_catalog"
    String mvName = "${suiteName}_mv"
    String dbName = context.config.getDbNameByFile(context.file)

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='paimon',
            'warehouse' = 's3://warehouse/wh/',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1"
        );"""

    order_qt_base_table """ select * from ${catalogName}.test_paimon_spark.test_tb_mix_format ; """

    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`par`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${catalogName}.`test_paimon_spark`.test_tb_mix_format;
        """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_a"))
    assertTrue(showPartitionsResult.toString().contains("p_b"))

    // refresh one partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_a);
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_one_partition "SELECT * FROM ${mvName} "

    //refresh auto
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_auto "SELECT * FROM ${mvName} "
    order_qt_is_sync_before_rebuild "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

   // rebuild catalog, should not Affects MTMV
    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );
    """
    order_qt_is_sync_after_rebuild "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

    // should refresh normal after catalog rebuild
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_complete_rebuild "SELECT * FROM ${mvName} "

    sql """drop materialized view if exists ${mvName};"""

     // not have partition
     sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${catalogName}.`test_paimon_spark`.test_tb_mix_format;
        """
    order_qt_not_partition_before "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    //should can refresh auto
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_not_partition "SELECT * FROM ${mvName} "
    order_qt_not_partition_after "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """drop materialized view if exists ${mvName};"""
    sql """drop catalog if exists ${catalogName}"""

}

