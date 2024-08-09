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

suite("test_hive_multi_partition_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        // prepare data in hive
        def hive_database = "test_hive_multi_partition_mtmv_db"
        def hive_table = "partition2"

        def autogather_off_str = """ set hive.stats.column.autogather = false; """
        def autogather_on_str = """ set hive.stats.column.autogather = true; """
        def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
        def drop_database_str = """ drop database if exists ${hive_database}"""
        def create_database_str = """ create database ${hive_database}"""
        def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                        `k1` int)
                                        PARTITIONED BY (
                                        `year` int,
                                        `region` string)
                                        STORED AS ORC;
                                """
        def add_partition_str = """
                                    alter table ${hive_database}.${hive_table} add if not exists
                                    partition(year=2020,region="bj")
                                    partition(year=2020,region="sh")
                                    partition(year=2021,region="bj")
                                    partition(year=2021,region="sh")
                                    partition(year=2022,region="bj")
                                    partition(year=2022,region="sh")
                                """
        def insert_str1 = """insert into ${hive_database}.${hive_table} PARTITION(year=2020,region="bj") values(1)"""
        def insert_str2 = """insert into ${hive_database}.${hive_table} PARTITION(year=2020,region="sh") values(2)"""
        def insert_str3 = """insert into ${hive_database}.${hive_table} PARTITION(year=2021,region="bj") values(3)"""
        def insert_str4 = """insert into ${hive_database}.${hive_table} PARTITION(year=2021,region="sh") values(4)"""
        def insert_str5 = """insert into ${hive_database}.${hive_table} PARTITION(year=2022,region="bj") values(5)"""
        def insert_str6 = """insert into ${hive_database}.${hive_table} PARTITION(year=2022,region="sh") values(6)"""

        logger.info("hive sql: " + autogather_off_str)
        hive_docker """ ${autogather_off_str} """
        logger.info("hive sql: " + drop_table_str)
        hive_docker """ ${drop_table_str} """
        logger.info("hive sql: " + drop_database_str)
        hive_docker """ ${drop_database_str} """
        logger.info("hive sql: " + create_database_str)
        hive_docker """ ${create_database_str}"""
        logger.info("hive sql: " + create_table_str)
        hive_docker """ ${create_table_str} """
        logger.info("hive sql: " + add_partition_str)
        hive_docker """ ${add_partition_str} """
        logger.info("hive sql: " + insert_str1)
        hive_docker """ ${insert_str1} """
        logger.info("hive sql: " + insert_str2)
        hive_docker """ ${insert_str2} """
        logger.info("hive sql: " + insert_str3)
        hive_docker """ ${insert_str3} """
        logger.info("hive sql: " + insert_str4)
        hive_docker """ ${insert_str4} """
        logger.info("hive sql: " + insert_str5)
        hive_docker """ ${insert_str5} """
        logger.info("hive sql: " + insert_str6)
        hive_docker """ ${insert_str6} """


        // prepare catalog
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_${hivePrefix}_multi_partition_mtmv_catalog"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        order_qt_select_base_table "SELECT * FROM ${catalog_name}.${hive_database}.${hive_table}"


        // prepare mtmv
        def mvName = "test_hive_multi_partition_mtmv"
        def dbName = "regression_test_mtmv_p0"
        sql """drop materialized view if exists ${mvName};"""

        // partition by year
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`year`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT k1,year,region FROM ${catalog_name}.${hive_database}.${hive_table};
            """
        def showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_2020"))
        assertTrue(showPartitionsResult.toString().contains("p_2021"))
        assertTrue(showPartitionsResult.toString().contains("p_2022"))
        assertEquals(showPartitionsResult.size(),3)
        // refresh p_2020
        sql """
                REFRESH MATERIALIZED VIEW ${mvName} partitions(p_2020);
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_year_2020 "SELECT * FROM ${mvName} order by k1,year,region"

        // refresh complete
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_year_complete "SELECT * FROM ${mvName} order by k1,year,region"

        sql """drop materialized view if exists ${mvName};"""
        // partition by region
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`region`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT k1,year,region FROM ${catalog_name}.${hive_database}.${hive_table};
            """
        showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_bj"))
        assertTrue(showPartitionsResult.toString().contains("p_sh"))
        assertEquals(showPartitionsResult.size(),2)
        // refresh p_bj
        sql """
                REFRESH MATERIALIZED VIEW ${mvName} partitions(p_bj);
            """
        jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_region_bj "SELECT * FROM ${mvName} order by k1,year,region"

        // refresh complete
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_region_complete "SELECT * FROM ${mvName} order by k1,year,region"

        // hive data change
        def insert_str7 = """
                    insert into ${hive_database}.${hive_table} PARTITION(year=2020,region="bj") values(7);
                    """
        logger.info("hive sql: " + insert_str7)
        hive_docker """ ${insert_str7} """
        sql """
                REFRESH catalog ${catalog_name}
            """
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_bj);
        """
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_data_change "SELECT * FROM ${mvName} order by k1,year,region"

        // hive add partition year
        def add_partition2023_bj_str = """
                                        alter table ${hive_database}.${hive_table} add if not exists
                                        partition(year=2023,region="bj");
                                    """
        logger.info("hive sql: " + add_partition2023_bj_str)
        hive_docker """ ${add_partition2023_bj_str} """
        sql """
                REFRESH catalog ${catalog_name}
            """
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
        waitingMTMVTaskFinished(jobName)
        showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertEquals(showPartitionsResult.size(),2)

        // hive add partition region
        def add_partition2023_tj_str = """
                                            alter table ${hive_database}.${hive_table} add if not exists
                                            partition(year=2023,region="tj");
                                        """
        logger.info("hive sql: " + add_partition2023_tj_str)
        hive_docker """ ${add_partition2023_tj_str} """
        sql """
                REFRESH catalog ${catalog_name}
            """
            sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
            waitingMTMVTaskFinished(jobName)
            showPartitionsResult = sql """show partitions from ${mvName}"""
            logger.info("showPartitionsResult: " + showPartitionsResult.toString())
            assertEquals(showPartitionsResult.size(),3)
            assertTrue(showPartitionsResult.toString().contains("p_tj"))

        // hive drop partition
        def drop_partition2023_bj_str = """
                                            alter table ${hive_database}.${hive_table} drop if exists
                                            partition(year=2023,region="bj");
                                        """
        logger.info("hive sql: " + drop_partition2023_bj_str)
        hive_docker """ ${drop_partition2023_bj_str} """
        sql """
                REFRESH catalog ${catalog_name}
            """
            sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
            waitingMTMVTaskFinished(jobName)
            showPartitionsResult = sql """show partitions from ${mvName}"""
            logger.info("showPartitionsResult: " + showPartitionsResult.toString())
            assertTrue(showPartitionsResult.toString().contains("p_bj"))
            assertTrue(showPartitionsResult.toString().contains("p_sh"))
            assertTrue(showPartitionsResult.toString().contains("p_tj"))

        def drop_partition2023_tj_str = """
                                            alter table ${hive_database}.${hive_table} drop if exists
                                            partition(year=2023,region="tj");
                                        """
        logger.info("hive sql: " + drop_partition2023_tj_str)
        hive_docker """ ${drop_partition2023_tj_str} """
        sql """
                REFRESH catalog ${catalog_name}
            """
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
        waitingMTMVTaskFinished(jobName)
        showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_bj"))
        assertTrue(showPartitionsResult.toString().contains("p_sh"))
        assertFalse(showPartitionsResult.toString().contains("p_tj"))

        logger.info("hive sql: " + autogather_on_str)
        hive_docker """ ${autogather_on_str} """
        sql """drop materialized view if exists ${mvName};"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}

