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

suite("test_oracle_jdbc_catalog", "p0") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String resource_name = "oracle_catalog_resource";
        String catalog_name = "oracle_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "DORIS_TEST";
        String ex_db_name_lower_case = ex_db_name.toLowerCase();
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE";
        String test_insert = "TEST_INSERT";

        String inDorisTable = "doris_in_tb";

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

        sql """create resource if not exists ${resource_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:${oracle_port}:${SID}",
                    "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/ojdbc8.jar",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        sql """CREATE CATALOG ${catalog_name} WITH RESOURCE ${resource_name}"""

        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字",
                `age` INT NULL COMMENT "年龄"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """switch ${catalog_name}"""
        sql """ use ${ex_db_name}"""

        order_qt_test0  """ select * from STUDENT order by ID; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select ID, NAME, AGE from STUDENT; """
        order_qt_in_tb  """ select id, name, age from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_test1  """ select * from TEST_NUM order by ID; """
        order_qt_test2  """ select * from TEST_CHAR order by ID; """
        order_qt_test3  """ select * from TEST_INT order by ID; """
        order_qt_test5  """ select * from TEST_DATE order by ID; """
        order_qt_test6  """ select * from TEST_TIMESTAMP order by ID; """
        order_qt_test7  """ select * from TEST_NUMBER order by ID; """
        order_qt_test8  """ select * from TEST_NUMBER2 order by ID; """
        order_qt_test9  """ select * from TEST_NUMBER3 order by ID; """
        order_qt_test10  """ select * from TEST_NUMBER4 order by ID; """

        // The result of TEST_RAW will change
        // So instead of qt, we're using sql here.
        sql  """ select * from TEST_RAW order by ID; """

        // test insert
        String uuid1 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid1}', 'doris1', 18) """
        order_qt_test_insert1 """ select name, age from ${test_insert} where id = '${uuid1}' order by age """

        String uuid2 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid2}', 'doris2', 19), ('${uuid2}', 'doris3', 20) """
        order_qt_test_insert2 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        sql """ insert into ${test_insert} select * from ${test_insert} where id = '${uuid2}' """
        order_qt_test_insert3 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

        // test only_specified_database argument
        sql """create resource if not exists ${resource_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:${oracle_port}:${SID}",
                    "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/ojdbc8.jar",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "only_specified_database" = "true"
        );"""
        sql """ CREATE CATALOG ${catalog_name} WITH RESOURCE ${resource_name} """
        sql """ switch ${catalog_name} """

        qt_specified_database   """ show databases; """
        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

        // test lower_case_table_names argument
        sql """create resource if not exists ${resource_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:${oracle_port}:${SID}",
                    "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/ojdbc8.jar",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver",
                    "lower_case_table_names" = "true"
        );"""
        sql """ CREATE CATALOG ${catalog_name} WITH RESOURCE ${resource_name} """
        sql """ switch ${catalog_name} """
        sql """ use ${ex_db_name_lower_case}"""

        qt_lower_case_table_names1  """ select * from test_num order by ID; """
        qt_lower_case_table_names2  """ select * from test_char order by ID; """
        qt_lower_case_table_names3  """ select * from test_int order by ID; """

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

    }
}
