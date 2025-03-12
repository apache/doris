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

suite("test_hive_drop_db", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_${hivePrefix}_drop_db_ctl"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String db_name = "test_${hivePrefix}_drop_db"

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            sql """switch ${catalog_name}"""

            sql """set enable_fallback_to_original_planner=false;"""

            sql """drop database if exists ${db_name} force"""
            sql """create database ${db_name}"""
            sql """use ${db_name}"""
            sql """ 
                    CREATE TABLE test_hive_drop_db_tbl1 (
                      `col1` BOOLEAN,
                      `col2` TINYINT
                    );
                """
            sql """insert into test_hive_drop_db_tbl1 values(true, 1);"""
            qt_sql_tbl1 """select * from test_hive_drop_db_tbl1"""
            sql """ 
                    CREATE TABLE test_hive_drop_db_tbl2 (
                      `col1` BOOLEAN,
                      `col2` TINYINT
                    );
                """
            sql """insert into test_hive_drop_db_tbl2 values(false, 2);"""
            qt_sql_tbl2 """select * from test_hive_drop_db_tbl2"""
            sql """ 
                    CREATE TABLE test_hive_drop_db_tbl3 (
                      `col1` BOOLEAN,
                      `col2` TINYINT
                    );
                """
            qt_sql_tbl3 """select * from test_hive_drop_db_tbl3"""

            // drop db with tables
            test {
                sql """drop database ${db_name}"""
                exception """One or more tables exist"""
            }

            // drop db froce with tables
            sql """drop database ${db_name} force"""

            // refresh catalog
            sql """refresh catalog ${catalog_name}"""
            // should be empty
            test {
                sql """show tables from ${db_name}"""
                exception "Unknown database"
            }
    
            // use tvf to check if table is dropped
            String tbl1_path = "hdfs://${externalEnvIp}:${hdfs_port}/user/hive/warehouse/test_${hivePrefix}_drop_db.db/test_hive_drop_db_tbl1"
            String tbl2_path = "hdfs://${externalEnvIp}:${hdfs_port}/user/hive/warehouse/test_${hivePrefix}_drop_db.db/test_hive_drop_db_tbl2"
            String tbl3_path = "hdfs://${externalEnvIp}:${hdfs_port}/user/hive/warehouse/test_${hivePrefix}_drop_db.db/test_hive_drop_db_tbl3"

            qt_test_1 """ select * from HDFS(
                  "uri" = "${tbl1_path}/*",
                  "format" = "orc"); """
            qt_test_2 """ select * from HDFS(
                  "uri" = "${tbl1_path}/*",
                  "format" = "orc"); """
            qt_test_3 """ select * from HDFS(
                  "uri" = "${tbl1_path}/*",
                  "format" = "orc"); """

        } finally {
        }
    }
}
