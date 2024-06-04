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

suite("test_mysql_jdbc_catalog_nereids", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "mysql_jdbc_catalog_nereids";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String inDorisTable = "doris_in_tb_nereids";
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
        String ex_tb20 = "ex_tb20";
        String test_insert = "test_insert";
        String test_insert2 = "test_insert2";

        sql """create database if not exists ${internal_db_name}; """

        sql """ADMIN SET FRONTEND CONFIG ("enable_decimal_conversion" = "true");"""
        sql """drop catalog if exists ${catalog_name} """

	    sql """set enable_nereids_planner=true;"""
	    sql """set enable_fallback_to_original_planner=false;"""

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""
        sql """use internal.${internal_db_name}"""
        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """switch ${catalog_name}"""
        sql """ use ${ex_db_name}"""

        explain {
            sql("""select id from ${ex_tb0} where id = 111;""")
            contains "WHERE ((`id` = 111))"
        }
        qt_ex_tb0_where """select id from ${ex_tb0} where id = 111;"""
        order_qt_ex_tb0  """ select id, name from ${ex_tb0} order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select id, name from ${ex_tb0}; """
        // order_qt_in_tb  """ select id, name from ${internal_db_name}.${inDorisTable} order by id; """
        sql """switch internal;"""
        order_qt_in_tb  """ select id, name from ${internal_db_name}.${inDorisTable} order by id; """

        sql """switch ${catalog_name}"""
        sql """ use ${ex_db_name}"""
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
        order_qt_ex_tb20  """ select * from ${ex_tb20} order by decimal_normal; """
        order_qt_information_schema """ show tables from information_schema; """

        // test insert
        String uuid1 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid1}', 'doris1', 18) """
        order_qt_test_insert1 """ select name, age from ${test_insert} where id = '${uuid1}' order by age """

        String uuid2 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid2}', 'doris2', 19), ('${uuid2}', 'doris3', 20) """
        order_qt_test_insert2 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        sql """ insert into ${test_insert} select * from ${test_insert} where id = '${uuid2}' """
        order_qt_test_insert3 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        String uuid3 = UUID.randomUUID().toString();
        sql """ INSERT INTO ${test_insert2} VALUES
                ('${uuid3}', true, 'abcHa1.12345', '1.123450xkalowadawd', '2022-10-01', 3.14159, 1, 2, 0, 100000, 1.2345678, 24.000, '07:09:51', '2022', '2022-11-27 07:09:51', '2022-11-27 07:09:51'); """
        order_qt_test_insert4 """ select k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14,k15 from ${test_insert2} where id = '${uuid3}' """

        sql """ drop catalog if exists ${catalog_name} """

        // test old create-catalog syntax for compatibility
        sql """ CREATE CATALOG ${catalog_name} PROPERTIES (
            "type"="jdbc",
            "jdbc.user"="root",
            "jdbc.password"="123456",
            "jdbc.jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "jdbc.driver_url" = "${driver_url}",
            "jdbc.driver_class" = "com.mysql.cj.jdbc.Driver");
        """
        sql """ switch ${catalog_name} """
        sql """ use ${ex_db_name} """
        order_qt_ex_tb1  """ select * from ${ex_tb1} order by id; """
        sql """ drop catalog if exists ${catalog_name} """
    }
}
