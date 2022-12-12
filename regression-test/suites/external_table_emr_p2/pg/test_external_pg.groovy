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
//import org.postgresql.Driver
suite("test_external_pg", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalPgTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extPgHost = context.config.otherConfigs.get("extPgHost")
        String extPgPort = context.config.otherConfigs.get("extPgPort")
        String extPgUser = context.config.otherConfigs.get("extPgUser")
        String extPgPassword = context.config.otherConfigs.get("extPgPassword")
        String jdbcResourcePg14 = "jdbc_resource_pg_14"
        String jdbcPg14Database1 = "jdbc_pg_14_database1"
        String jdbcPg14Table1 = "jdbc_pg_14_table1"


        sql """drop database if exists ${jdbcPg14Database1};"""
        sql """create database ${jdbcPg14Database1};"""
        sql """use ${jdbcPg14Database1};"""
        sql """drop resource if exists ${jdbcResourcePg14};"""
        sql """
            create external resource ${jdbcResourcePg14}
            properties (
                "type"="jdbc",
                "user"="${extPgUser}",
                "password"="${extPgPassword}",
                "jdbc_url"="jdbc:postgresql://${extPgHost}:${extPgPort}/exter_test?currentSchema=public",
                "driver_url"="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/postgresql-42.5.0.jar",
                "driver_class"="org.postgresql.Driver"
            );
            """

        sql """drop table if exists ${jdbcPg14Table1}"""
        sql """
            CREATE EXTERNAL TABLE ${jdbcPg14Table1} (
                id int,
                name char(100)              
            ) ENGINE=JDBC
            PROPERTIES (
            "resource" = "${jdbcResourcePg14}",
            "table" = "ext_pg0",
            "table_type"="postgresql"
            );
            """

        def res = sql """SELECT id, name FROM ${jdbcPg14Table1};"""
        logger.info("recoding select: " + res.toString())

        sql """drop table if exists ${jdbcPg14Table1}"""
        sql """drop database if exists ${jdbcPg14Database1};"""
        sql """drop resource if exists ${jdbcResourcePg14};"""

    }
}
