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

suite("test_pg_jdbc_catalog", "p0") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "jdbc_resource_catalog_pg";
        String catalog_name = "pg_jdbc_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_schema_name = "doris_test";
        String ex_schema_name2 = "catalog_pg_test";
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String inDorisTable = "doris_in_tb";

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

        sql """create resource if not exists ${resource_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://127.0.0.1:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/postgresql-42.5.0.jar",
            "driver_class" = "org.postgresql.Driver"
        );"""

        sql """CREATE CATALOG ${catalog_name} WITH RESOURCE jdbc_resource_catalog_pg"""

        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """switch ${catalog_name}"""
        sql """ use ${ex_schema_name}"""

        order_qt_test0  """ select * from test3 order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select id, name from test3; """
        order_qt_in_tb  """ select id, name from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_test1  """ select * from test1 order by k8; """
        order_qt_test2  """ select * from test2 order by id; """
        order_qt_test3  """ select * from test2_item order by id; """
        order_qt_test4  """ select * from test2_view order by id; """
        order_qt_test5  """ select * from test3 order by id; """
        order_qt_test6  """ select * from test4 order by id; """
        order_qt_test7  """ select * from test5 order by id; """
        order_qt_test8  """ select * from test6 order by id; """
        order_qt_test9  """ select * from test7 order by id; """
        order_qt_test10  """ select * from test8 order by id; """
        order_qt_test11  """ select * from test9 order by id1; """

        sql """ use ${ex_schema_name2}"""
        order_qt_test12  """ select * from test10 order by id; """
        order_qt_test13  """ select * from test11 order by id; """
        order_qt_test14  """ select * from test12 order by id; """

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists jdbc_resource_catalog_pg"""

        // test old create-catalog syntax for compatibility
        sql """ CREATE CATALOG ${catalog_name} PROPERTIES (
            "type"="jdbc",
            "jdbc.user"="postgres",
            "jdbc.password"="123456",
            "jdbc.jdbc_url" = "jdbc:postgresql://127.0.0.1:${pg_port}/postgres?useSSL=false&currentSchema=doris_test",
            "jdbc.driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/postgresql-42.5.0.jar",
            "jdbc.driver_class" = "org.postgresql.Driver");
        """

        sql """switch ${catalog_name}"""
        sql """use ${ex_schema_name}"""
        order_qt_test_old  """ select * from test3 order by id; """
        sql """drop resource if exists jdbc_resource_catalog_pg"""
    }
}
