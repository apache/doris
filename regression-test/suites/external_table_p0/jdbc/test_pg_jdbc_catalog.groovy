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

suite("test_pg_jdbc_catalog", "p0,external,pg,external_docker,external_docker_pg") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "pg_jdbc_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_schema_name = "doris_test";
        String ex_schema_name2 = "catalog_pg_test";
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String inDorisTable = "test_pg_jdbc_doris_in_tb";
        String test_insert = "test_insert";

        sql """create database if not exists ${internal_db_name}; """

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver"
        );"""
        sql """use ${internal_db_name}"""
        sql  """ drop table if exists ${internal_db_name}.${inDorisTable} """
        sql  """
              CREATE TABLE ${internal_db_name}.${inDorisTable} (
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
        order_qt_wkb_test  """ select * from wkb_test order by id; """
        order_qt_dt_test  """ select * from dt_test order by 1; """
        order_qt_json_test  """ select * from json_test order by 1; """
        order_qt_jsonb_test  """ select * from jsonb_test order by 1; """
        order_qt_filter1  """ select * from test10 where 1 = 1  order by id; """
        order_qt_filter2  """ select * from test10 where id = 1 order by id; """
        order_qt_filter3  """ select * from test10 where 1 = 1 and id = 1 order by id; """

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

        // test only_specified_database argument
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver",
            "only_specified_database" = "true"
        );"""
        sql """switch ${catalog_name} """
        qt_specified_database_1 """ show databases; """

        sql """drop catalog if exists ${catalog_name} """

        // test only_specified_database and include_database_list argument
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "doris_test"
        );"""
        sql """switch ${catalog_name} """
        qt_specified_database_2 """ show databases; """

        sql """drop catalog if exists ${catalog_name} """

        // test only_specified_database and exclude_database_list argument
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver",
            "only_specified_database" = "true",
            "exclude_database_list" = "doris_test"
        );"""
        sql """switch ${catalog_name} """
        qt_specified_database_3 """ show databases; """

        sql """drop catalog if exists ${catalog_name} """

        // test include_database_list and exclude_database_list have overlapping items case
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="postgres",
            "password"="123456",
            "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "org.postgresql.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "doris_test",
            "exclude_database_list" = "doris_test"
        );"""
        sql """switch ${catalog_name} """
        qt_specified_database_4 """ show databases; """

        sql """drop catalog if exists ${catalog_name} """

        sql """ CREATE CATALOG ${catalog_name} PROPERTIES (
            "type"="jdbc",
            "jdbc.user"="postgres",
            "jdbc.password"="123456",
            "jdbc.jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?useSSL=false&currentSchema=doris_test",
            "jdbc.driver_url" = "${driver_url}",
            "jdbc.driver_class" = "org.postgresql.Driver");
        """

        sql """ switch ${catalog_name} """
        sql """ use ${ex_schema_name} """
        order_qt_test_old  """ select * from test3 order by id; """
        sql """ drop catalog if exists ${catalog_name} """
    }
}
