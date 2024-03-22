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

suite("test_mariadb_jdbc_catalog", "p0,external,mariadb,external_docker,external_docker_mariadb") {
    qt_sql """select current_catalog()"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "mariadb_jdbc_catalog";
        String internal_db_name = "regression_test_jdbc_catalog_p0";
        String ex_db_name = "doris_test";
        String mariadb_port = context.config.otherConfigs.get("mariadb_10_port");
        String inDorisTable = "test_mariadb_jdbc_doris_in_tb";
        String ex_tb0 = "ex_tb0";
        String test_insert = "test_insert";
        String test_insert2 = "test_insert2";
        String auto_default_t = "auto_default_t";
        String dt = "dt";

        sql """create database if not exists ${internal_db_name}; """

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mariadb_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
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

        qt_sql """select current_catalog()"""
        sql """switch ${catalog_name}"""
        qt_sql """select current_catalog()"""
        sql """ use ${ex_db_name}"""

        order_qt_ex_tb0  """ select id, name from ${ex_tb0} order by id; """
        sql  """ insert into internal.${internal_db_name}.${inDorisTable} select id, name from ${ex_tb0}; """
        order_qt_in_tb  """ select id, name from internal.${internal_db_name}.${inDorisTable} order by id; """

        order_qt_information_schema """ show tables from information_schema; """
        order_qt_auto_default_t """insert into ${auto_default_t}(name) values('a'); """
        order_qt_dt """select * from ${dt}; """

        // test all types supported by Mariadb
        sql """use doris_test;"""
        qt_mysql_all_types """select * from all_types order by tinyint_u;"""

        sql """ drop catalog if exists ${catalog_name} """

    }
}

