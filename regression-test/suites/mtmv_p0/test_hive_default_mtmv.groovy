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

suite("test_hive_default_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        // prepare data in hive
        def hive_database = "mtmv_default_partition_db"
        def hive_table = "test_hive_default_mtmv_t1"

        def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
        def drop_database_str = """ drop database if exists ${hive_database}"""
        def create_database_str = """ create database ${hive_database}"""
        def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                        user_id INT,
                                        num INT
                                    )
                                    partitioned by(year int,month int)
                                    STORED AS ORC;
                                """

        def insert_str1 = """
                            insert into ${hive_database}.${hive_table} PARTITION(year=2020,month) values(1,1,1);
                            """
        def insert_str2 = """
                            insert into ${hive_database}.${hive_table} PARTITION(year=2020,month) values(2,2,null);
                            """

        logger.info("hive sql: " + drop_table_str)
        hive_docker """ ${drop_table_str} """
        logger.info("hive sql: " + drop_database_str)
        hive_docker """ ${drop_database_str} """
        logger.info("hive sql: " + create_database_str)
        hive_docker """ ${create_database_str}"""
        logger.info("hive sql: " + create_table_str)
        hive_docker """ ${create_table_str} """
        logger.info("hive sql: " + insert_str1)
        hive_docker """ ${insert_str1} """
        logger.info("hive sql: " + insert_str2)
        hive_docker """ ${insert_str2} """


        // prepare catalog
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_default_mtmv_test_catalog"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        order_qt_test "SELECT * FROM ${catalog_name}.${hive_database}.${hive_table} order by num"


        // prepare mtmv
        def mvName = "test_hive_default_mtmv"
        def dbName = "regression_test_mtmv_p0"
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`month`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT user_id,num,year,month FROM ${catalog_name}.${hive_database}.${hive_table};
            """
        def showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_NULL"))
        assertTrue(showPartitionsResult.toString().contains("p_1"))

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} partitions(p_NULL);
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_null "SELECT * FROM ${mvName} order by num"

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} partitions(p_1);
            """
        waitingMTMVTaskFinished(jobName)
        order_qt_1 "SELECT * FROM ${mvName} order by num"

        sql """drop materialized view if exists ${mvName};"""

        sql """drop catalog if exists ${catalog_name}"""
    }
}

