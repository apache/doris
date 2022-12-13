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
//import com.mysql.cj.jdbc.Driver
suite("test_external_resource_mysql", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalMysqlTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extMysqlHost = context.config.otherConfigs.get("extMysqlHost")
        String extMysqlPort = context.config.otherConfigs.get("extMysqlPort")
        String extMysqlUser = context.config.otherConfigs.get("extMysqlUser")
        String extMysqlPassword = context.config.otherConfigs.get("extMysqlPassword")
        String mysqlResourceName = "jdbc_resource_mysql_57"
        String mysqlDatabaseName01 = "external_mysql_database01"
        String mysqlTableName01 = "external_mysql_table01"

        sql """drop resource if exists ${mysqlResourceName};"""
        sql """
            create external resource ${mysqlResourceName}
            properties (
                "type"="jdbc",
                "user"="${extMysqlUser}",
                "password"="${extMysqlPassword}",
                "jdbc_url"="jdbc:mysql://${extMysqlHost}:${extMysqlPort}/doris_test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true&serverTimezone=Asia/Shanghai&useSSL=false",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
                "driver_class"="com.mysql.cj.jdbc.Driver"
            );
            """
        sql """drop database if exists ${mysqlDatabaseName01};"""
        sql """create database ${mysqlDatabaseName01};"""
        sql """use ${mysqlDatabaseName01};"""
        sql """drop table if exists ${mysqlTableName01}"""
        sql """
            CREATE EXTERNAL TABLE ${mysqlTableName01} (
                `id` int,
                `name` varchar(128)
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${mysqlResourceName}",
            "table" = "ex_tb0",
            "table_type"="mysql"
            );
            """

        def res = sql """select count(*) from ${mysqlTableName01};"""
        logger.info("recoding select: " + res.toString())

        sql """drop table if exists ${mysqlTableName01}"""
        sql """drop database if exists ${mysqlDatabaseName01};"""

    }
}







