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


suite("test_show_where", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    String ex_db_name = "doris_test";
    String ex_tb0 = "ex_tb0";
    String ex_tb1 = "ex_tb1";
    String catalog_name = "test_show_where_mysql_jdbc_catalog";
    try {
        sql  """ drop database if exists ${ex_db_name} """
        sql  """ create database ${ex_db_name} """

        sql  """ drop table if exists `${ex_db_name}`.`${ex_tb0}` """
        sql  """
                CREATE TABLE `${ex_db_name}`.`${ex_tb0}` (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """
        sql  """
                CREATE TABLE `${ex_db_name}`.`${ex_tb1}` (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """
        sql """ use ${ex_db_name}"""

        qt_select "show databases where schema_name= '${ex_db_name}'"
        qt_select "show tables"
        qt_select "show tables where table_name= '${ex_tb0}'"
        qt_select "show tables from ${ex_db_name}"
        qt_select "show tables from internal.${ex_db_name}"


        String enabled = context.config.otherConfigs.get("enableJdbcTest")
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            
            sql """drop catalog if exists ${catalog_name} """

            // if use 'com.mysql.cj.jdbc.Driver' here, it will report: ClassNotFound
            sql """ CREATE CATALOG ${catalog_name} PROPERTIES (
                    "type"="jdbc",
                    "jdbc.user"="root",
                    "jdbc.password"="123456",
                    "jdbc.jdbc_url" = "jdbc:mysql://127.0.0.1:${mysql_port}/doris_test?useSSL=false",
                    "jdbc.driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
                    "jdbc.driver_class" = "com.mysql.cj.jdbc.Driver");
                """

            sql """switch ${catalog_name}"""
            sql """ use ${ex_db_name}"""

            qt_select "show databases where schema_name= '${ex_db_name}'"
            qt_select "show tables"
            qt_select "show tables where table_name= '${ex_tb0}'"
            qt_select "show tables from ${ex_db_name}"
            qt_select "show tables from internal.${ex_db_name}"
            qt_select "show tables from ${catalog_name}.${ex_db_name}"


            sql """switch internal"""
            sql """ use ${ex_db_name}"""

            qt_select "show databases where schema_name= '${ex_db_name}'"
            qt_select "show tables"
            qt_select "show tables where table_name= '${ex_tb1}'"
            qt_select "show tables from internal.${ex_db_name}"
            qt_select "show tables from ${catalog_name}.${ex_db_name}"

        }

    } finally {
        try_sql("DROP DATABASE IF EXISTS `internal`.`${ex_db_name}` FORCE")

        try_sql("DROP CATALOG IF EXISTS `${catalog_name}`")
    }
}

