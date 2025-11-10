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

suite("test_hive_pct_mtmv", "p0,external,hive,external_docker,external_docker_hive,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    String suiteName = "test_hive_pct_mtmv"
    String dbName = "${suiteName}_db"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName = "${suiteName}_mv"
    String mvDbName = context.config.getDbNameByFile(context.file)
    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)

        def autogather_off_str = """ set hive.stats.column.autogather = false; """
        def autogather_on_str = """ set hive.stats.column.autogather = true; """
        def drop_table_str1 = """ drop table if exists ${dbName}.${tableName1} """
        def drop_table_str2 = """ drop table if exists ${dbName}.${tableName2} """
        def drop_database_str = """ drop database if exists ${dbName}"""
        def create_database_str = """ create database ${dbName}"""
        def create_table_str1 = """ CREATE TABLE ${dbName}.${tableName1} (
                                        `k1` int)
                                        PARTITIONED BY (
                                        `year` int,
                                        `region` string)
                                        STORED AS ORC;
                                """
        def add_partition_str1 = """
                                    alter table ${dbName}.${tableName1} add if not exists
                                    partition(year=2020,region="bj")
                                    partition(year=2020,region="sh")
                                    partition(year=2021,region="bj")
                                    partition(year=2021,region="sh")
                                """
        def insert_str1 = """insert into ${dbName}.${tableName1} PARTITION(year=2020,region="bj") values(1)"""
        def insert_str2 = """insert into ${dbName}.${tableName1} PARTITION(year=2020,region="sh") values(2)"""
        def insert_str3 = """insert into ${dbName}.${tableName1} PARTITION(year=2021,region="bj") values(3)"""
        def insert_str4 = """insert into ${dbName}.${tableName1} PARTITION(year=2021,region="sh") values(4)"""

        def create_table_str2 = """ CREATE TABLE ${dbName}.${tableName2} (
                                                `k1` int,
                                                `year` int
                                                )
                                                PARTITIONED BY (
                                                `region` string)
                                                STORED AS ORC;
                                        """
        def add_partition_str2 = """
                                    alter table ${dbName}.${tableName2} add if not exists
                                    partition(region="bj")
                                    partition(region="sh")
                                    partition(region="tj")
                                """
        def insert_str5 = """insert into ${dbName}.${tableName2} PARTITION(region="bj") values(5,2020)"""
        def insert_str6 = """insert into ${dbName}.${tableName2} PARTITION(region="sh") values(6,2020)"""
        def insert_str7 = """insert into ${dbName}.${tableName2} PARTITION(region="tj") values(7,2020)"""

        logger.info("hive sql: " + autogather_off_str)
        hive_docker """ ${autogather_off_str} """
        logger.info("hive sql: " + drop_table_str1)
        hive_docker """ ${drop_table_str1} """
        logger.info("hive sql: " + drop_table_str2)
        hive_docker """ ${drop_table_str2} """
        logger.info("hive sql: " + drop_database_str)
        hive_docker """ ${drop_database_str} """
        logger.info("hive sql: " + create_database_str)
        hive_docker """ ${create_database_str}"""
        logger.info("hive sql: " + create_table_str1)
        hive_docker """ ${create_table_str1} """
        logger.info("hive sql: " + add_partition_str1)
        hive_docker """ ${add_partition_str1} """
        logger.info("hive sql: " + insert_str1)
        hive_docker """ ${insert_str1} """
        logger.info("hive sql: " + insert_str2)
        hive_docker """ ${insert_str2} """
        logger.info("hive sql: " + insert_str3)
        hive_docker """ ${insert_str3} """
        logger.info("hive sql: " + insert_str4)
        hive_docker """ ${insert_str4} """
        logger.info("hive sql: " + create_table_str2)
        hive_docker """ ${create_table_str2} """
        logger.info("hive sql: " + add_partition_str2)
        hive_docker """ ${add_partition_str2} """
        logger.info("hive sql: " + insert_str5)
        hive_docker """ ${insert_str5} """
        logger.info("hive sql: " + insert_str6)
        hive_docker """ ${insert_str6} """
        logger.info("hive sql: " + insert_str7)
        hive_docker """ ${insert_str7} """

        // prepare catalog
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_${suiteName}_catalog"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        order_qt_select_base_table "SELECT k1,year,region FROM ${catalog_name}.${dbName}.${tableName1} union all SELECT k1,year,region FROM ${catalog_name}.${dbName}.${tableName2} "


        // prepare mtmv
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`region`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT k1,year,region FROM ${catalog_name}.${dbName}.${tableName1} union all SELECT k1,year,region FROM ${catalog_name}.${dbName}.${tableName2};
            """
        order_qt_partitions_1 "select PartitionName,UnsyncTables from partitions('catalog'='internal','database'='${mvDbName}','table'='${mvName}') order by PartitionId desc;"
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} AUTO
            """
        waitingMTMVTaskFinishedByMvName(mvName)
        order_qt_select_mv1 "SELECT * from ${mvName}"

        // hive data change
        def insert_str8 = """
                    insert into ${dbName}.${tableName1} PARTITION(year=2020,region="bj") values(7);
                    """
        logger.info("hive sql: " + insert_str8)
        hive_docker """ ${insert_str8} """
        sql """
                REFRESH catalog ${catalog_name}
            """
        sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_bj);
        """
        waitingMTMVTaskFinishedByMvName(mvName)
        order_qt_select_mv2 "SELECT * from ${mvName}"

        sql """drop materialized view if exists ${mvName};"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}

