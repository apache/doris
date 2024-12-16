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

suite("test_iceberg_mtmv", "p0,external,iceberg,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    logger.info("enabled: " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    logger.info("externalEnvIp: " + externalEnvIp)
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    logger.info("rest_port: " + rest_port)
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    logger.info("minio_port: " + minio_port)

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "iceberg_mtmv_catalog";
        String mvName = "test_iceberg_mtmv"
        String dbName = "regression_test_mtmv_p0"
        String icebergDb = "format_v2"
        String icebergTable = "sample_mor_parquet"
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

        order_qt_catalog """select count(*) from ${catalog_name}.${icebergDb}.${icebergTable}"""
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${catalog_name}.${icebergDb}.${icebergTable};
            """

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv "SELECT count(*) FROM ${mvName}"

        sql """drop materialized view if exists ${mvName};"""
        sql """ drop catalog if exists ${catalog_name} """
    }

    // Test partition refresh.
    // Use hms catalog to avoid rest catalog fail to write caused by sqlite database file locked.
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hivePrefix = "hive2";
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
        String warehouse = "${default_fs}/warehouse"

        String catalog_name = "iceberg_mtmv_catalog_hms";
        String mvUnpartition = "test_iceberg_unpartition"
        String mvName1 = "test_iceberg_mtmv_ts"
        String mvName2 = "test_iceberg_mtmv_d"
        String dbName = "regression_test_mtmv_partition_p0"
        String icebergDb = "iceberg_mtmv_partition"
        String icebergTable1 = "tstable"
        String icebergTable2 = "dtable"
        sql """drop catalog if exists ${catalog_name} """
        sql """create catalog if not exists ${catalog_name} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            'fs.defaultFS' = '${default_fs}',
            'warehouse' = '${warehouse}',
            'use_meta_cache' = 'true'
        );"""

        sql """switch internal"""
        sql """drop database if exists ${dbName}"""
        sql """create database if not exists ${dbName}"""
        sql """use ${dbName}"""

        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable1}"""
        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable2}"""
        sql """create database if not exists ${catalog_name}.${icebergDb}"""
        sql """
            CREATE TABLE ${catalog_name}.${icebergDb}.${icebergTable1} (
              ts DATETIME,
              value INT)
            ENGINE=iceberg
            PARTITION BY LIST (DAY(ts)) ();
        """
        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-26 01:02:03', 1), ('2024-10-27 01:02:03', 2), ('2024-10-27 21:02:03', 3)"""
        sql """CREATE MATERIALIZED VIEW ${mvName1} BUILD DEFERRED REFRESH AUTO ON MANUAL partition by(`ts`) DISTRIBUTED BY RANDOM BUCKETS 2  PROPERTIES ('replication_num' = '1') as SELECT * FROM ${catalog_name}.${icebergDb}.${icebergTable1}"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} complete"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh1 "select * from ${mvName1} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-26 21:02:03', 4)"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh2 """select * from ${mvName1} order by value"""

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-26 01:22:03', 5), ('2024-10-27 01:12:03', 6);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} partitions(p_20241026000000_20241027000000);"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh3 """select * from ${mvName1} order by value"""

        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh4 """select * from ${mvName1} order by value"""

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-28 01:22:03', 7);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} partitions(p_20241026000000_20241027000000);"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh5 """select * from ${mvName1} order by value"""

        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh6 """select * from ${mvName1} order by value"""

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values (null, 8);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1)
        qt_test_ts_refresh_null """select * from ${mvName1} order by value"""

        def showPartitionsResult = sql """show partitions from ${mvName1}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_20241026000000_20241027000000"))
        assertTrue(showPartitionsResult.toString().contains("p_20241027000000_20241028000000"))
        assertTrue(showPartitionsResult.toString().contains("p_20241028000000_20241029000000"))

        sql """drop materialized view if exists ${mvName1};"""

        // Test unpartitioned mtmv
        sql """
            CREATE MATERIALIZED VIEW ${mvUnpartition}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${catalog_name}.${icebergDb}.${icebergTable1};
        """
        sql """
            REFRESH MATERIALIZED VIEW ${mvUnpartition} complete
        """
        def jobName = getJobName(dbName, mvUnpartition);
        waitingMTMVTaskFinished(jobName)
        qt_unpartition "SELECT * FROM ${mvUnpartition} order by value"

        sql """drop materialized view if exists ${mvUnpartition};"""
        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable1}"""


        sql """
            CREATE TABLE ${catalog_name}.${icebergDb}.${icebergTable2} (
              d DATE,
              value INT)
            ENGINE=iceberg
            PARTITION BY LIST (MONTH(d)) ();
        """
        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-08-26', 1), ('2024-09-17', 2), ('2024-09-27', 3);"""
        sql """CREATE MATERIALIZED VIEW ${mvName2} BUILD DEFERRED REFRESH AUTO ON MANUAL partition by(`d`) DISTRIBUTED BY RANDOM BUCKETS 2  PROPERTIES ('replication_num' = '1') as SELECT * FROM ${catalog_name}.${icebergDb}.${icebergTable2}"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} complete"""
        waitingMTMVTaskFinishedByMvName(mvName2)
        qt_test_d_refresh1 "select * from ${mvName2} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-09-01', 4);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} auto"""
        waitingMTMVTaskFinishedByMvName(mvName2)
        qt_test_d_refresh2 "select * from ${mvName2} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-08-22', 5), ('2024-09-30', 6);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} partitions(p_20240801_20240901);"""
        waitingMTMVTaskFinishedByMvName(mvName2)
        qt_test_d_refresh3 "select * from ${mvName2} order by value"
        sql """REFRESH MATERIALIZED VIEW ${mvName2} partitions(p_20240901_20241001);"""
        waitingMTMVTaskFinishedByMvName(mvName2)
        qt_test_d_refresh4 "select * from ${mvName2} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-10-28', 7);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} auto"""
        waitingMTMVTaskFinishedByMvName(mvName2)
        qt_test_d_refresh5 "select * from ${mvName2} order by value"

        showPartitionsResult = sql """show partitions from ${mvName2}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_20240801_20240901"))
        assertTrue(showPartitionsResult.toString().contains("p_20240901_20241001"))
        assertTrue(showPartitionsResult.toString().contains("p_20241001_20241101"))

        sql """drop materialized view if exists ${mvName2};"""
        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable2}"""

        sql """ drop catalog if exists ${catalog_name} """
    }
}

