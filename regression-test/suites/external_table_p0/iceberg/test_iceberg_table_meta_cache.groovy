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

suite("test_iceberg_table_meta_cache", "p0,external,doris,external_docker,external_docker_doris") {
    String catalog_name = "test_iceberg_meta_cache"
    String catalog_name_no_cache = "test_iceberg_meta_no_cache"

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive3"]) {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
            String warehouse = "${default_fs}/warehouse"

            // 1. test default catalog
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog ${catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = '${default_fs}',
                'warehouse' = '${warehouse}',
                'meta.cache.iceberg.manifest.enable' = 'true'
            );
            """
            sql """switch ${catalog_name}"""
            sql """drop database if exists test_iceberg_meta_cache_db"""
            sql """create database test_iceberg_meta_cache_db"""
            sql """
                CREATE TABLE test_iceberg_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                );
            """
            sql """insert into test_iceberg_meta_cache_db.sales values(1, 2.0)"""
            // select 1 row
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(2, 2.0)"""
            // still select 1 row
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(1, 3.0)"""
            // still select 1 row
            sql """select * from test_iceberg_meta_cache_db.sales"""
            sql """refresh table test_iceberg_meta_cache_db.sales"""
            // select 3 rows
            sql """select * from test_iceberg_meta_cache_db.sales"""
            sql """drop table test_iceberg_meta_cache_db.sales"""

            // 2. test catalog with meta.cache.iceberg.table.ttl-second
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            test {
                sql """
                create catalog ${catalog_name_no_cache} properties (
                    'type'='iceberg',
                    'iceberg.catalog.type'='hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                    'fs.defaultFS' = '${default_fs}',
                    'warehouse' = '${warehouse}',
                    'meta.cache.iceberg.manifest.enable' = 'false',
                    'meta.cache.iceberg.table.ttl-second' = '-2'
                );
                """
                exception "is wrong"
            }

            // disable iceberg table meta cache
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = '${default_fs}',
                'warehouse' = '${warehouse}',
                'meta.cache.iceberg.manifest.enable' = 'false',
                'meta.cache.iceberg.table.ttl-second' = '0'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            sql """drop database if exists test_iceberg_meta_cache_db"""
            sql """create database test_iceberg_meta_cache_db"""
            sql """
                CREATE TABLE test_iceberg_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                );
            """
            sql """insert into test_iceberg_meta_cache_db.sales values(1, 2.0)"""
            // select 1 row
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(2, 2.0)"""
            // select 2 rows
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(1, 3.0)"""
            // select 3 rows
            sql """select * from test_iceberg_meta_cache_db.sales"""
            sql """drop table test_iceberg_meta_cache_db.sales"""

            // test modify ttl property
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            // 1. create catalog with default property fisrt
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = '${default_fs}',
                'warehouse' = '${warehouse}',
                'meta.cache.iceberg.manifest.enable' = 'false'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            sql """drop database if exists test_iceberg_meta_cache_db"""
            sql """create database test_iceberg_meta_cache_db"""
            sql """
                CREATE TABLE test_iceberg_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                );
            """
            sql """insert into test_iceberg_meta_cache_db.sales values(1, 2.0)"""
            // select 1 row
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(2, 2.0)"""
            // still select 1 row
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // alter wrong catalog property
            test {
                sql """alter catalog ${catalog_name_no_cache} set properties ("meta.cache.iceberg.table.ttl-second" = "-2")"""
                exception "is wrong"
            }
            // alter catalog property, disable meta cache
            sql """alter catalog ${catalog_name_no_cache} set properties ("meta.cache.iceberg.table.ttl-second" = "0")"""
            // select 2 rows
            sql """select * from test_iceberg_meta_cache_db.sales"""
            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(3, 2.0)"""
            // select 3 row
            sql """select * from test_iceberg_meta_cache_db.sales"""

            // insert into new value
            sql """insert into test_iceberg_meta_cache_db.sales values(1, 3.0)"""
            // select 4 rows
            sql """select * from test_iceberg_meta_cache_db.sales"""
            sql """drop table test_iceberg_meta_cache_db.sales"""
            sql """drop database if exists test_iceberg_meta_cache_db"""
            sql """drop catalog if exists ${catalog_name_no_cache};"""
        }
    }
}
