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

suite("test_hive_empty_partition_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    String suiteName = "test_excluded_trigger_table_mtmv"
    String dbName = "default";
    String tableName = "${suiteName}_empty_table"
    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        // prepare catalog
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalogName = "${hivePrefix}_${suiteName}_catalog"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog if not exists ${catalogName} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """drop table if exists ${catalogName}.`${dbName}`.${tableName};"""
        sql """
            CREATE TABLE ${catalogName}.`${dbName}`.${tableName} (
              `col1` BOOLEAN COMMENT 'col1',
              `pt1` VARCHAR COMMENT 'pt1'
            )  ENGINE=hive
            PARTITION BY LIST (pt1) ()
            PROPERTIES (
              'file_format'='orc',
              'compression'='zlib'
            );
            """
        // partition
        sql """drop materialized view if exists ${mvName};"""
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                REFRESH AUTO ON MANUAL
                partition by(`pt1`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT user_id,num,year FROM ${catalogName}.${hive_database}.${hive_table};
            """
        waitingMTMVTaskFinishedByMvName(mvName)
        // not partition
        sql """drop materialized view if exists ${mvName};"""
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT user_id,num,year FROM ${catalogName}.${hive_database}.${hive_table};
            """
        waitingMTMVTaskFinishedByMvName(mvName)

        sql """drop materialized view if exists ${mvName};"""
        sql """drop table if exists ${catalogName}.`${dbName}`.${tableName};"""
        sql """drop catalog if exists ${catalogName}"""
    }
}

