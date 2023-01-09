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
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE"

        String inDorisTable = "doris_in_tb";

        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists ${resource_name}"""

        // TODO(ftw): modify driver_url and jdbc_url
        sql """create resource if not exists ${resource_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:${oracle_port}:${SID}",
                    "driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/ojdbc6.jar",
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

        // The result of TEST_RAW will change
        // So instead of qt, we're using sql here.
        sql  """ select * from TEST_RAW order by ID; """


        sql """drop catalog if exists ${catalog_name} """
        sql """drop resource if exists jdbc_resource_catalog_pg"""
    }
}