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

suite("test_hive_use_meta_cache", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog = "test_${hivePrefix}_use_meta_cache"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog}"""
            sql """create catalog if not exists ${catalog} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""
            
            // create from Doris, the cache will be filled immediately
            String database= "test_use_meta_cache_db"
            String table = "test_use_meta_cache_tbl"
            String database_hive = "test_use_meta_cache_db_hive"
            String table_hive = "test_use_meta_cache_tbl_hive"
            sql "switch ${catalog}"
            sql "drop database if exists ${database}"
            sql "drop database if exists ${database_hive}"
            order_qt_sql01 "show databases like '%${database}%'";
            sql "drop database if exists ${database}"
            sql "create database ${database}"
            order_qt_sql02 "show databases like '%${database}%'";
            sql "use ${database}"
            sql "create table ${table} (k1 int)"
            order_qt_sql03 "show tables"
            sql "drop table ${table}"
            order_qt_sql04 "show tables"
            sql "drop database ${database}"
            order_qt_sql05 "show databases like '%${database}%'";
         
            // create from Hive, the cache has different behavior
            order_qt_sql01 "show databases like '%${database_hive}%'";
            hive_docker "drop database if exists ${database_hive}"
            hive_docker "create database ${database_hive}"
            // not see
            order_qt_sql02 "show databases like '%${database_hive}%'";
            // but can use
            sql "use ${database_hive}"
            sql "refresh catalog ${catalog}"
            // can see
            order_qt_sql03 "show databases like '%${database_hive}%'";
            // show tables first to fill cache
            order_qt_sql04 "show tables"
            hive_docker "create table ${database_hive}.${table_hive} (k1 int)"
            // not see
            order_qt_sql05 "show tables"
            // but can select
            sql "select * from ${table_hive}"
            // still not see
            order_qt_sql06 "show tables"
            sql "refresh database ${database_hive}"
            // can see
            order_qt_sql07 "show tables"
            hive_docker "drop table ${database_hive}.${table_hive}"
            // still can see
            order_qt_sql08 "show tables"
            sql "refresh database ${database_hive}"
            // can not see
            order_qt_sql09 "show tables"
            hive_docker "drop database ${database_hive}"
            // still can see
            order_qt_sql10 "show databases like '%${database_hive}%'";
            sql "refresh catalog ${catalog}"
            // can not see
            order_qt_sql11 "show databases like '%${database_hive}%'";
        } finally {
        }
    }
}


