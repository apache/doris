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
        String test_all_types = "test_all_types";
        String test_insert_all_types = "test_pg_insert_all_types";
        String test_ctas = "test_pg_ctas";

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
        order_qt_show_db """ show databases from ${catalog_name}; """
        sql """use ${internal_db_name}"""
        sql  """ drop table if exists ${internal_db_name}.${inDorisTable} """
        sql  """
              CREATE TABLE ${internal_db_name}.${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """
        sql  """ drop table if exists ${internal_db_name}.${test_insert_all_types} """
        sql """
            CREATE TABLE ${internal_db_name}.${test_insert_all_types} (
                `id` INT NOT NULL,
                `char_value` VARCHAR(*) NULL,
                `varchar_value` TEXT NULL,
                `date_value` DATE NULL,
                `smallint_value` SMALLINT NULL,
                `int_value` INT NULL,
                `bigint_value` BIGINT NULL,
                `timestamp_value` DATETIME(6) NULL,
                `decimal_value` DECIMAL(10, 3) NULL,
                `bit_value` BOOLEAN NULL,
                `real_value` FLOAT NULL,
                `cidr_value` TEXT NULL,
                `inet_value` TEXT NULL,
                `macaddr_value` TEXT NULL,
                `bitn_value` TEXT NULL,
                `bitnv_value` TEXT NULL,
                `serial4_value` INT NOT NULL,
                `jsonb_value` JSON NULL,
                `point_value` TEXT NULL,
                `line_value` TEXT NULL,
                `lseg_value` TEXT NULL,
                `box_value` TEXT NULL,
                `path_value` TEXT NULL,
                `polygon_value` TEXT NULL,
                `circle_value` TEXT NULL
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
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
        order_qt_filter4  """ select * from test3 where name not like '%abc%' order by id; """

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
        order_qt_partition_1_0 "select * from person_r;"
        order_qt_partition_1_1 "select * from person_r1;"
        order_qt_partition_1_2 "select * from person_r2;"
        order_qt_partition_1_3 "select * from person_r3;"
        order_qt_partition_1_4 "select * from person_r4;"
        order_qt_partition_2_0 "select * from tb_test_alarm;"
        order_qt_partition_2_1 "select * from tb_test_alarm_2020_09;"
        order_qt_partition_2_2 "select * from tb_test_alarm_2020_10;"
        order_qt_partition_2_3 "select * from tb_test_alarm_2020_11;"
        order_qt_partition_2_4 "select * from tb_test_alarm_2020_12;"
        order_qt_num_zero "select * from num_zero;"

        // test insert
        String uuid1 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid1}', 'doris1', 18) """
        order_qt_test_insert1 """ select name, age from ${test_insert} where id = '${uuid1}' order by age """

        String uuid2 = UUID.randomUUID().toString();
        sql """ insert into ${test_insert} values ('${uuid2}', 'doris2', 19), ('${uuid2}', 'doris3', 20) """
        order_qt_test_insert2 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        sql """ insert into ${test_insert} select * from ${test_insert} where id = '${uuid2}' """
        order_qt_test_insert3 """ select name, age from ${test_insert} where id = '${uuid2}' order by age """

        // test select all types
        order_qt_select_all_types """select * from ${test_all_types}; """

        order_qt_select_all_types_tvf """ select * from query('catalog' = '${catalog_name}', 'query' = 'select * from catalog_pg_test.${test_all_types};') order by 1"""

        // test test ctas
        sql """ drop table if exists internal.${internal_db_name}.${test_ctas} """
        sql """ create table internal.${internal_db_name}.${test_ctas}
                PROPERTIES("replication_num" = "1")
                AS select * from ${test_all_types};
            """

        order_qt_ctas """select * from internal.${internal_db_name}.${test_ctas};"""

        order_qt_ctas_desc """desc internal.${internal_db_name}.${test_ctas};"""

        // test insert into internal.db.tbl
        sql """ insert into internal.${internal_db_name}.${test_insert_all_types} select * from ${test_all_types}; """
        order_qt_select_insert_all_types """ select * from internal.${internal_db_name}.${test_insert_all_types} order by id; """
        

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
