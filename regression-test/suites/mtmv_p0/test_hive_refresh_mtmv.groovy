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

suite("test_hive_refresh_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        // prepare data in hive
        def hive_database = "mtmv_test_db"
        def hive_table = "test_hive_refresh_mtmv_t1"

        def autogather_off_str = """ set hive.stats.column.autogather = false; """
        def autogather_on_str = """ set hive.stats.column.autogather = true; """
        def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
        def drop_database_str = """ drop database if exists ${hive_database}"""
        def create_database_str = """ create database ${hive_database}"""
        def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                        user_id INT,
                                        num INT
                                    )
                                    partitioned by(year int )
                                    STORED AS ORC;
                                """
        def add_partition_str = """
                                    alter table ${hive_database}.${hive_table} add if not exists
                                    partition(year=2020);
                                """
        def insert_str = """ insert into ${hive_database}.${hive_table} PARTITION(year=2020) values(1,1)"""

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
        logger.info("hive sql: " + insert_str)
        hive_docker """ ${insert_str} """


        // prepare catalog
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_refresh_mtmv_test_catalog"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        order_qt_test "SELECT * FROM ${catalog_name}.${hive_database}.${hive_table}"


        // prepare mtmv
        def mvName = "test_hive_refresh_mtmv"
        def dbName = "regression_test_mtmv_p0"
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`year`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT user_id,num,year FROM ${catalog_name}.${hive_database}.${hive_table};
            """
        def showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_2020"))

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} auto
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_1 "SELECT * FROM ${mvName} order by user_id"

        // hive data change
        logger.info("hive sql: " + insert_str)
        hive_docker """ ${insert_str} """
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv_2 "SELECT * FROM ${mvName} order by user_id"

        // hive add partition
        def add_partition2021_str = """
                                        alter table ${hive_database}.${hive_table} add if not exists
                                        partition(year=2021);
                                    """
        logger.info("hive sql: " + add_partition2021_str)
        hive_docker """ ${add_partition2021_str} """
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
        waitingMTMVTaskFinished(jobName)
        showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_2020"))
        assertTrue(showPartitionsResult.toString().contains("p_2021"))

        // hive drop partition
        def drop_partition2021_str = """
                                            alter table ${hive_database}.${hive_table} drop if exists
                                            partition(year=2021);
                                        """
        logger.info("hive sql: " + drop_partition2021_str)
        hive_docker """ ${drop_partition2021_str} """
            sql """
                REFRESH MATERIALIZED VIEW ${mvName} auto
            """
            waitingMTMVTaskFinished(jobName)
            showPartitionsResult = sql """show partitions from ${mvName}"""
            logger.info("showPartitionsResult: " + showPartitionsResult.toString())
            assertTrue(showPartitionsResult.toString().contains("p_2020"))
            assertFalse(showPartitionsResult.toString().contains("p_2021"))

            // hive rename column name
            def rename_column_str = """
                                        alter table ${hive_database}.${hive_table} CHANGE COLUMN num num1 int;
                                    """
        logger.info("hive sql: " + rename_column_str)
        hive_docker """ ${rename_column_str} """
            sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
            waitingMTMVTaskFinishedNotNeedSuccess(jobName)
            order_qt_task_error "select Status from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime DESC limit 1"

        // hive recover column name
        def recover_column_str = """
                                    alter table ${hive_database}.${hive_table} CHANGE COLUMN num1 num int;
                                """
        logger.info("hive sql: " + recover_column_str)
        hive_docker """ ${recover_column_str} """
        // schema change need refresh catalog
        sql """
                REFRESH catalog ${catalog_name}
            """
            sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
        waitingMTMVTaskFinishedNotNeedSuccess(jobName)
        order_qt_task_recover "select Status from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime DESC limit 1"

        logger.info("hive sql: " + autogather_on_str)
        hive_docker """ ${autogather_on_str} """

        sql """drop materialized view if exists ${mvName};"""

        sql """drop catalog if exists ${catalog_name}"""
    }
}

