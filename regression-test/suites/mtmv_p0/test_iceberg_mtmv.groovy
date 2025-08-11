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
        String catalog_name = "iceberg_mtmv_catalog_hms";
        String mvUnpartition = "test_iceberg_unpartition"
        String mvName1 = "test_iceberg_mtmv_ts"
        String mvName2 = "test_iceberg_mtmv_d"
        String dbName = "regression_test_mtmv_partition_p0"
        String icebergDb = "iceberg_mtmv_partition"
        String icebergTable1 = "tstable"
        String icebergTable2 = "dtable"
        String icebergTable3 = "union_test"
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
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh1 "select * from ${mvName1} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-26 21:02:03', 4)"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh2 """select * from ${mvName1} order by value"""

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-26 01:22:03', 5), ('2024-10-27 01:12:03', 6);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} partitions(p_20241026000000_20241027000000);"""
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh3 """select * from ${mvName1} order by value"""

        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh4 """select * from ${mvName1} order by value"""

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values ('2024-10-28 01:22:03', 7);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} partitions(p_20241026000000_20241027000000);"""
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh5 """select * from ${mvName1} order by value"""

        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh6 """select * from ${mvName1} order by value"""

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable1} values (null, 8);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName1} auto"""
        waitingMTMVTaskFinishedByMvName(mvName1, dbName)
        qt_test_ts_refresh_null """select * from ${mvName1} order by value"""

        qt_test_iceberg_table_partition_ts """show partitions from ${catalog_name}.${icebergDb}.${icebergTable1};"""

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
        waitingMTMVTaskFinishedByMvName(mvName2, dbName)
        qt_test_d_refresh1 "select * from ${mvName2} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-09-01', 4);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} auto"""
        waitingMTMVTaskFinishedByMvName(mvName2, dbName)
        qt_test_d_refresh2 "select * from ${mvName2} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-08-22', 5), ('2024-09-30', 6);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} partitions(p_20240801_20240901);"""
        waitingMTMVTaskFinishedByMvName(mvName2, dbName)
        qt_test_d_refresh3 "select * from ${mvName2} order by value"
        sql """REFRESH MATERIALIZED VIEW ${mvName2} partitions(p_20240901_20241001);"""
        waitingMTMVTaskFinishedByMvName(mvName2, dbName)
        qt_test_d_refresh4 "select * from ${mvName2} order by value"

        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable2} values ('2024-10-28', 7);"""
        sql """REFRESH MATERIALIZED VIEW ${mvName2} auto"""
        waitingMTMVTaskFinishedByMvName(mvName2, dbName)
        qt_test_d_refresh5 "select * from ${mvName2} order by value"

        qt_test_iceberg_table_partition_d """show partitions from ${catalog_name}.${icebergDb}.${icebergTable2};"""

        showPartitionsResult = sql """show partitions from ${mvName2}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_20240801_20240901"))
        assertTrue(showPartitionsResult.toString().contains("p_20240901_20241001"))
        assertTrue(showPartitionsResult.toString().contains("p_20241001_20241101"))

        sql """drop materialized view if exists ${mvName2};"""
        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable2}"""

        // Test rewrite and union partitions
        sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
        String mvSql = "SELECT par,count(*) as num FROM ${catalog_name}.${icebergDb}.${icebergTable3} group by par"
        String mvName = "union_mv"
        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable3}"""
        sql """
            CREATE TABLE ${catalog_name}.${icebergDb}.${icebergTable3} (
              id int,
              value int,
              par datetime
            ) ENGINE=iceberg
            PARTITION BY LIST (day(par)) ();
        """
        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable3} values (1, 1, "2024-01-01"), (2, 1, "2024-01-01"), (3, 1, "2024-01-01"), (4, 1, "2024-01-01")"""
        sql """insert into ${catalog_name}.${icebergDb}.${icebergTable3} values (1, 2, "2024-01-02"), (2, 2, "2024-01-02"), (3, 2, "2024-01-02")"""
        sql """analyze table ${catalog_name}.${icebergDb}.${icebergTable3} with sync"""

        sql """drop materialized view if exists ${mvName};"""
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`par`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS ${mvSql}
        """

        def showPartitions = sql """show partitions from ${mvName}"""
        logger.info("showPartitions: " + showPartitions.toString())
        assertTrue(showPartitions.toString().contains("p_20240101000000_20240102000000"))
        assertTrue(showPartitions.toString().contains("p_20240102000000_20240103000000"))

        // refresh one partiton
        sql """REFRESH MATERIALIZED VIEW ${mvName} partitions(p_20240101000000_20240102000000);"""
        waitingMTMVTaskFinishedByMvName(mvName, dbName)
        order_qt_refresh_one_partition "SELECT * FROM ${mvName} "
        def explainOnePartition = sql """ explain  ${mvSql} """
        logger.info("explainOnePartition: " + explainOnePartition.toString())
        assertTrue(explainOnePartition.toString().contains("VUNION"))
        order_qt_refresh_one_partition_rewrite "${mvSql}"
        mv_rewrite_success("${mvSql}", "${mvName}")

        //refresh auto
        sql """REFRESH MATERIALIZED VIEW ${mvName} auto"""
        waitingMTMVTaskFinishedByMvName(mvName, dbName)
        order_qt_refresh_auto "SELECT * FROM ${mvName} "
        def explainAllPartition = sql """ explain  ${mvSql}; """
        logger.info("explainAllPartition: " + explainAllPartition.toString())
        assertTrue(explainAllPartition.toString().contains("VOlapScanNode"))
        order_qt_refresh_all_partition_rewrite "${mvSql}"
        mv_rewrite_success("${mvSql}", "${mvName}")

        sql """drop materialized view if exists ${mvName};"""
        sql """drop table if exists ${catalog_name}.${icebergDb}.${icebergTable3}"""

        sql """use ${catalog_name}.test_db"""
        qt_evolution2 "show partitions from replace_partition2"
        qt_evolution3 "show partitions from replace_partition3"
        qt_evolution4 "show partitions from replace_partition4"
        qt_evolution5 "show partitions from replace_partition5"

        test {
            sql "show partitions from replace_partition1"
            // check exception message contains
            exception "is not a supported partition table"
        }

        test {
            sql "show partitions from no_partition"
            // check exception message contains
            exception "is not a supported partition table"
        }

        test {
            sql "show partitions from not_support_trans"
            // check exception message contains
            exception "is not a supported partition table"
        }

        test {
            sql "show partitions from drop_partition1"
            // check exception message contains
            exception "is not a supported partition table"
        }

        test {
            sql "show partitions from drop_partition2"
            // check exception message contains
            exception "is not a supported partition table"
        }

        test {
            sql "show partitions from add_partition1"
            // check exception message contains
            exception "is not a supported partition table"
        }

        test {
            sql "show partitions from add_partition2"
            // check exception message contains
            exception "is not a supported partition table"
        }

        sql """ drop catalog if exists ${catalog_name} """
    }
}

