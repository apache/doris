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

suite("test_mysql_jdbc_catalog", "p0") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        sql """admin set frontend config ("enable_multi_catalog" = "true")"""

        String catalog_name = "mysql_jdbc_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String inDorisTable = "doris_in_tb";
        String ex_tb0 = "ex_tb0";
        String ex_tb1 = "ex_tb1";
        String ex_tb2 = "ex_tb2";
        String ex_tb3 = "ex_tb3";
        String ex_tb4 = "ex_tb4";
        String ex_tb5 = "ex_tb5";
        String ex_tb6 = "ex_tb6";
        String ex_tb7 = "ex_tb7";
        String ex_tb8 = "ex_tb8";
        String ex_tb9 = "ex_tb9";
        String ex_tb10 = "ex_tb10";
        String ex_tb11 = "ex_tb11";
        String ex_tb12 = "ex_tb12";
        String ex_tb13 = "ex_tb13";
        String ex_tb14 = "ex_tb14";
        String ex_tb15 = "ex_tb15";
        String ex_tb16 = "ex_tb16";
        String ex_tb17 = "ex_tb17";
        String ex_tb18 = "ex_tb18";
        String ex_tb19 = "ex_tb19";


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

        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """switch ${catalog_name}"""
        qt_db_amount """ show databases; """

        sql """ use ${ex_db_name}"""

        order_qt_ex_tb0  """ select id, name from ${ex_tb0} order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select id, name from ${ex_tb0}; """
        order_qt_in_tb  """ select id, name from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_ex_tb1  """ select * from ${ex_tb1} order by id; """
        order_qt_ex_tb2  """ select * from ${ex_tb2} order by id; """
        order_qt_ex_tb3  """ select * from ${ex_tb3} order by game_code; """
        order_qt_ex_tb4  """ select * from ${ex_tb4} order by products_id; """
        order_qt_ex_tb5  """ select * from ${ex_tb5} order by id; """
        order_qt_ex_tb6  """ select * from ${ex_tb6} order by id; """
        order_qt_ex_tb7  """ select * from ${ex_tb7} order by id; """
        order_qt_ex_tb8  """ select * from ${ex_tb8} order by uid; """
        order_qt_ex_tb9  """ select * from ${ex_tb9} order by c_date; """
        order_qt_ex_tb10  """ select * from ${ex_tb10} order by aa; """
        order_qt_ex_tb11  """ select * from ${ex_tb11} order by aa; """
        order_qt_ex_tb12  """ select * from ${ex_tb12} order by cc; """
        order_qt_ex_tb13  """ select * from ${ex_tb13} order by name; """
        order_qt_ex_tb14  """ select * from ${ex_tb14} order by tid; """
        order_qt_ex_tb15  """ select * from ${ex_tb15} order by col1; """
        order_qt_ex_tb16  """ select * from ${ex_tb16} order by id; """
        order_qt_ex_tb17  """ select * from ${ex_tb17} order by id; """
        order_qt_ex_tb18  """ select * from ${ex_tb18} order by num_tinyint; """
        order_qt_ex_tb19  """ select * from ${ex_tb19} order by date_value; """


        sql """admin set frontend config ("enable_multi_catalog" = "false")"""
    }
}