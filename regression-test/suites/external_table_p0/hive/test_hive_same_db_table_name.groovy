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

suite("test_hive_same_db_table_name", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_same_db_table_name"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
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

