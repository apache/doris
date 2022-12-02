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

suite("test_external_hive", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {

        try {
            String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
            String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")

            sql """admin set frontend config ("enable_multi_catalog" = "true")"""

            sql """drop database if exists external_hive_database;"""
            sql """create database external_hive_database;"""
            sql """use external_hive_database;"""
            sql """drop table if exists external_hive_table;"""

            sql """
            create table `external_hive_table` (
            `a` int NOT NULL COMMENT "",
            `b` char(10) NOT NULL COMMENT ""
            ) ENGINE=HIVE
            PROPERTIES
            (
            'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
            'database' = 'test',
            'table' = 'hive_test'
            );
            """
            def res = sql """select count(*) from external_hive_table"""
            logger.info("recoding select: " + res.toString())

            sql """drop table if exists external_hive_table;"""
            sql """drop database if exists external_hive_database;"""
        }finally{
            sql """admin set frontend config ("enable_multi_catalog" = "false")"""
        }
    }
}




