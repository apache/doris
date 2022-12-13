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
suite("test_external_es", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extEsHost = context.config.otherConfigs.get("extEsHost")
        String extEsPort = context.config.otherConfigs.get("extEsPort")
        String extEsUser = context.config.otherConfigs.get("extEsUser")
        String extEsPassword = context.config.otherConfigs.get("extEsPassword")
        String jdbcPg14Database1 = "jdbc_es_14_database1"
        String jdbcPg14Table1 = "jdbc_es_14_table1"


        sql """drop database if exists ${jdbcPg14Database1};"""
        sql """create database ${jdbcPg14Database1};"""
        sql """use ${jdbcPg14Database1};"""
        sql """drop table if exists ${jdbcPg14Table1};"""

        sql """
            CREATE EXTERNAL TABLE `${jdbcPg14Table1}` (
              `name` varchar(20) COMMENT "",
              `age` varchar(20) COMMENT ""
            ) ENGINE=ELASTICSEARCH
            PROPERTIES (
            "hosts" = "https://${extEsHost}:${extEsPort}",
            "index" = "helloworld",
            "user" = "${extEsUser}",
            "password" = "${extEsPassword}",
            "http_ssl_enabled" = "true"
            );
            """
        def res=sql """show create table ${jdbcPg14Table1};"""
        logger.info("recoding desc res: "+ res.toString())

        def res1=sql "select * from ${jdbcPg14Table1};"
        logger.info("recoding all: " + res1.toString())

        sql """drop table if exists ${jdbcPg14Table1};"""
        sql """drop database if exists ${jdbcPg14Database1};"""
    }
}
