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

suite("test_sqlserver_jdbc_catalog", "p0,external,sqlserver,external_docker,external_docker_sqlserver") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "sqlserver_catalog";
        String internal_db_name = "sqlserver_jdbc_catalog_p0";
        String ex_db_name = "dbo";
        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");

        String inDorisTable = "test_sqlserver_doris_in_tb";

        sql """ drop catalog if exists ${catalog_name} """

        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""
        sql """drop database if exists ${internal_db_name}"""
        sql """create database if not exists ${internal_db_name}"""
        sql """use ${internal_db_name}"""
        sql  """ drop table if exists ${inDorisTable} """
        sql  """
              CREATE TABLE ${inDorisTable} (
                `id` INT NULL COMMENT "主键id",
                `name` string NULL COMMENT "名字",
                `age` INT NULL COMMENT "年龄"
                ) DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """

        sql """ switch ${catalog_name} """
        sql """ use ${ex_db_name} """

        order_qt_test0  """ select * from student order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select * from student; """
        order_qt_in_tb  """ select id, name, age from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_test1  """ select * from test_int order by id; """
        order_qt_test2  """ select * from test_float order by id; """
        order_qt_test3  """ select * from test_char order by id; """
        order_qt_test5  """ select * from test_time order by id; """
        order_qt_test6  """ select * from test_money order by id; """
        order_qt_test7  """ select * from test_decimal order by id; """
        order_qt_test8  """ select * from test_text order by id; """
        order_qt_dt  """ select * from DateAndTime; """
        order_qt_filter1  """ select * from test_char where 1 = 1 order by id; """
        order_qt_filter2  """ select * from test_char where 1 = 1 and id = 1  order by id; """
        order_qt_filter3  """ select * from test_char where id = 1  order by id; """
        order_qt_id """ select count(*) from (select * from t_id) as a; """


        sql """ drop catalog if exists ${catalog_name} """

        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;trustServerCertificate=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""

        order_qt_sql """ show databases from ${catalog_name} """

        sql """ drop catalog if exists ${catalog_name} """
    }
}
