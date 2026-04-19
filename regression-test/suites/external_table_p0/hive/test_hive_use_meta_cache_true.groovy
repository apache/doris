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

suite("test_hive_use_meta_cache_true", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            def test_use_meta_cache = { Boolean use_meta_cache ->
                String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
                String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
                String use_meta_cache_string = use_meta_cache ? "true" : "false"
                String catalog = getHiveTempName("test_${hivePrefix}_use_meta_cache_${use_meta_cache}", "catalog")
                String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

                sql """drop catalog if exists ${catalog}"""
                sql """create catalog if not exists ${catalog} properties (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                    'use_meta_cache' = '${use_meta_cache_string}'
                );"""

                // create from Doris, the cache will be filled immediately
                String database= getHiveTempName("test_use_meta_cache_db_${use_meta_cache}", "db")
                String table = getHiveTempName("test_use_meta_cache_tbl_${use_meta_cache}", "tbl")
                String database_hive = getHiveTempName("test_use_meta_cache_db_hive_${use_meta_cache}", "db")
                String table_hive = getHiveTempName("test_use_meta_cache_tbl_hive_${use_meta_cache}", "tbl")
                String partitioned_table_hive = getHiveTempName("test_use_meta_cache_partitioned_tbl_hive_${use_meta_cache}", "tbl")
                String anotherTableCreationTest = getHiveTempName("another_table_creation_test", "tbl")

                sql "switch ${catalog}"
                sql "drop database if exists ${database} force"
                sql "drop database if exists ${database_hive} force"
                def result01 = sql "show databases like '%${database}%'"
                assertTrue(result01.isEmpty(), "Database should not exist before creation")
                sql "drop database if exists ${database} force"
                sql "create database ${database}"
                def result02 = sql "show databases like '%${database}%'"
                assertTrue(result02.size() == 1, "Database should exist after creation")
                sql "use ${database}"
                sql "create table ${table} (k1 int)"
                def result03 = sql "show tables"
                assertTrue(result03.size() == 1, "Table should exist after creation")
                sql "drop table ${table}"
                def result04 = sql "show tables"
                assertTrue(result04.isEmpty(), "Table should not exist after drop")
                sql "drop database ${database}"
                def result05 = sql "show databases like '%${database}%'"
                assertTrue(result05.isEmpty(), "Database should not exist after drop")

                // create from Hive, the cache has different behavior
                def result11 = sql "show databases like '%${database_hive}%'"
                assertTrue(result11.isEmpty(), "Hive database should not exist before creation")
                hive_docker "drop database if exists ${database_hive}"
                hive_docker "create database ${database_hive}"
                // not see
                def result12 = sql "show databases like '%${database_hive}%'"
                assertTrue(result12.isEmpty(), "Hive database should not be visible without refresh when cache enabled")
                if (use_meta_cache) {
                    // if use meta cache, can use
                    sql "use ${database_hive}"
                    sql "refresh catalog ${catalog}"
                } else {
                    // if not use meta cache, can not use
                    sql "refresh catalog ${catalog}"
                    sql "show databases"
                    sql "use ${database_hive}"
                }

                // can see
                def result13 = sql "show databases like '%${database_hive}%'"
                assertTrue(result13.size() == 1, "Hive database should be visible after refresh")
                // show tables first to fill cache
                def result14 = sql "show tables"
                assertTrue(result14.isEmpty(), "Tables should be empty initially")
                hive_docker "create table ${database_hive}.${table_hive} (k1 int)"
                // not see
                def result15 = sql "show tables"
                assertTrue(result15.isEmpty(), "Hive table should not be visible without refresh")
                if (use_meta_cache) {
                    // but can select
                    sql "select * from ${table_hive}"
                    // after select, the names cache is refresh, so can see
                    def result16 = sql "show tables"
                    assertTrue(result16.size() == 1, "Table should be visible after select")
                    sql "refresh database ${database_hive}"
                } else {
                    // if not use meta cache, can not select
                    sql "refresh database ${database_hive}"
                    sql "select * from ${table_hive}"
                    // after select, the names cache is refresh, so can see
                    def result16 = sql "show tables"
                    assertTrue(result16.size() == 1, "Table should be visible after select")
                }
                // can see
                def result17 = sql "show tables"
                assertTrue(result17.size() == 1, "Table should be visible")

                // another table creation test only for use_meta_cache=true
                // the main point is to select the table first before creation.
                if (use_meta_cache) {
                    // 0. create env
                    hive_docker "drop table if exists ${database_hive}.${anotherTableCreationTest}"
                    // 1. select a non exist table
                    test {
                        sql "select * from ${anotherTableCreationTest}";
                        exception "does not exist in database"
                    }
                    // 2. use hive to create this table
                    hive_docker "create table ${database_hive}.${anotherTableCreationTest} (k1 int)"
                    // 3. use doris to select, can see
                    qt_aother_test_sql "select * from ${anotherTableCreationTest}";
                    // 4. drop table
                    sql "drop table ${anotherTableCreationTest}";
                }

                // test Hive Metastore table partition file listing
                hive_docker "create table ${database_hive}.${partitioned_table_hive} (k1 int) partitioned by (p1 string)"
                sql "refresh catalog ${catalog}"
                order_qt_sql08 "show partitions from ${partitioned_table_hive}"
                hive_docker "alter table ${database_hive}.${partitioned_table_hive} add partition (p1='part1')"
                hive_docker "alter table ${database_hive}.${partitioned_table_hive} add partition (p1='part2')"
                // can see because partition file listing is not cached
                order_qt_sql09 "show partitions from ${partitioned_table_hive}"

                // before drop table, we can see the tables
                order_qt_sql091 "show tables"
                // drop tables
                hive_docker "drop table ${database_hive}.${partitioned_table_hive}"
                hive_docker "drop table ${database_hive}.${table_hive}"
                // still can see
                order_qt_sql10 "show tables"
                sql "refresh database ${database_hive}"
                // can not see
                order_qt_sql11 "show tables"

                // drop database
                hive_docker "drop database ${database_hive}"
                // still can see
                order_qt_sql12 "show databases like '%${database_hive}%'";
                sql "refresh catalog ${catalog}"
                // can not see
                order_qt_sql13 "show databases like '%${database_hive}%'";

                cleanupHiveDockerDatabase(database_hive, true)
                try_sql """drop catalog if exists ${catalog}"""
            }
            test_use_meta_cache(true)
        } finally {
        }
    }
}
