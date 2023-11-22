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

suite("test_hive_same_db_table_name", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_same_db_table_name"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        sql """switch internal;"""
        sql """create database if not exists multi_catalog;"""
        sql """use multi_catalog;"""
        sql """CREATE TABLE if not exists `region` (
               `r_regionkey` integer NOT NULL,
               `r_name` char(25) NOT NULL,
               `r_comment` varchar(152)
            ) distributed by hash(r_regionkey) buckets 1
            PROPERTIES (
               "replication_num" = "1" 
            );"""

        qt_1 "select * from region"
        qt_2 "select * from multi_catalog.region"
        qt_3 "select * from internal.multi_catalog.region"

        sql """use ${catalog_name}.multi_catalog;"""
        logger.info("switched to ${catalog_name}.multi_catalog")

        qt_4 "select * from region"
        qt_5 "select * from multi_catalog.region"
        qt_6 "select * from ${catalog_name}.multi_catalog.region"
    }
}

