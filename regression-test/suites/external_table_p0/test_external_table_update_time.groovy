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

suite("test_external_table_update_time", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = hivePrefix + "_test_update_time_ctl"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""

        sql "drop database if exists test_update_time_db force";
        sql "create database test_update_time_db";
        sql "use test_update_time_db";
        sql "create table test_update_time_tbl(k1 int)"
        sql "insert into test_update_time_tbl values(1)"
        def result = sql """select UPDATE_TIME from  information_schema.tables where TABLE_SCHEMA="test_update_time_db" and TABLE_NAME="test_update_time_tbl""""
        def update_time1 = result[0][0];
        sleep(2000);
        sql "insert into test_update_time_tbl values(2)"
        result = sql """select UPDATE_TIME from  information_schema.tables where TABLE_SCHEMA="test_update_time_db" and TABLE_NAME="test_update_time_tbl""""
        def update_time2 = result[0][0];
        logger.info("get update times " + update_time1 + " vs. " + update_time2) 
        assertTrue(update_time2 > update_time1);
        sleep(2000);
        sql "truncate table test_update_time_tbl";
        result = sql """select UPDATE_TIME from  information_schema.tables where TABLE_SCHEMA="test_update_time_db" and TABLE_NAME="test_update_time_tbl""""
        def update_time3 = result[0][0];
        logger.info("get update times " + update_time2 + " vs. " + update_time3)
        assertTrue(update_time3 > update_time2);
    }
}

