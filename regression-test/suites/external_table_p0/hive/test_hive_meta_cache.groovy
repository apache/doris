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

suite("test_hive_meta_cache", "p0,external,hive,external_docker,external_docker_hive") {
    String catalog_name = "test_hive_meta_cache"
    String catalog_name_no_cache = "test_hive_meta_no_cache"

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive3"]) {
            setHivePrefix(hivePrefix)
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

            // 1. test default catalog
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );
            """
            sql """switch ${catalog_name}"""
            hive_docker """drop database if exists test_hive_meta_cache_db CASCADE"""
            hive_docker """create database test_hive_meta_cache_db"""
            hive_docker """
                CREATE TABLE test_hive_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                )
                PARTITIONED BY (year INT)
            """
            hive_docker """ set hive.stats.column.autogather=false """
            hive_docker """insert into test_hive_meta_cache_db.sales partition(year=2024) values(1, 2.0)"""
            // select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // insert into same partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(2, 2.0, 2024)"""
            // still select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // insert into new partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 3.0, 2025)"""
            // still select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            sql """refresh table test_hive_meta_cache_db.sales"""
            // select 3 rows
            order_qt_sql_3row """select * from test_hive_meta_cache_db.sales"""
            sql """drop table test_hive_meta_cache_db.sales"""

            // 2. test catalog with file.meta.cache.ttl-second
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            test {
                sql """
                create catalog ${catalog_name_no_cache} properties (
                    'type'='hms',
                    'hadoop.username' = 'hadoop',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                    'file.meta.cache.ttl-second' = '-2'
                );
                """
                exception "is wrong"
            }

            // disable file list cache
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'file.meta.cache.ttl-second' = '0'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            hive_docker """drop database if exists test_hive_meta_cache_db CASCADE"""
            hive_docker """create database test_hive_meta_cache_db"""
            hive_docker """
                CREATE TABLE test_hive_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                )
                PARTITIONED BY (year INT)
                STORED AS PARQUET;
            """
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 2.0, 2024)"""
            // select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // insert into same partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(2, 2.0, 2024)"""
            // select 2 rows
            order_qt_sql_2row """select * from test_hive_meta_cache_db.sales"""
            // insert into new partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 3.0, 2025)"""
            // still select 2 rows
            order_qt_sql_2row """select * from test_hive_meta_cache_db.sales"""
            sql """refresh table test_hive_meta_cache_db.sales"""
            // select 3 rows
            order_qt_sql_3row """select * from test_hive_meta_cache_db.sales"""
            sql """drop table test_hive_meta_cache_db.sales"""

            // 3. test catalog with partition.cache.ttl-second
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            test {
                sql """
                create catalog ${catalog_name_no_cache} properties (
                    'type'='hms',
                    'hadoop.username' = 'hadoop',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                    'partition.cache.ttl-second' = '-2'
                );
                """
                exception "is wrong"
            }

            // disable partition cache
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'partition.cache.ttl-second' = '0'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            hive_docker """drop database if exists test_hive_meta_cache_db CASCADE"""
            hive_docker """create database test_hive_meta_cache_db"""
            hive_docker """
                CREATE TABLE test_hive_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                )
                PARTITIONED BY (year INT)
                STORED AS PARQUET;
            """
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 2.0, 2024)"""
            // select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // insert into same partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(2, 2.0, 2024)"""
            // still select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // insert into new partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 3.0, 2025)"""
            // select 2 rows
            order_qt_sql_2row """select * from test_hive_meta_cache_db.sales"""
            sql """refresh table test_hive_meta_cache_db.sales"""
            // select 3 rows
            order_qt_sql_3row """select * from test_hive_meta_cache_db.sales"""
            sql """drop table test_hive_meta_cache_db.sales"""

            // test modify ttl property
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            // 1. create catalog with default property fisrt
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            hive_docker """drop database if exists test_hive_meta_cache_db CASCADE"""
            hive_docker """create database test_hive_meta_cache_db"""
            hive_docker """
                CREATE TABLE test_hive_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                )
                PARTITIONED BY (year INT)
                STORED AS PARQUET;
            """
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 2.0, 2024)"""
            // select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // insert into same partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(2, 2.0, 2024)"""
            // still select 1 row
            order_qt_sql_1row """select * from test_hive_meta_cache_db.sales"""
            // alter wrong catalog property
            test {
                sql """alter catalog ${catalog_name_no_cache} set properties ("file.meta.cache.ttl-second" = "-2")"""
                exception "is wrong"
            }
            // alter catalog property, disable file list cache
            sql """alter catalog ${catalog_name_no_cache} set properties ("file.meta.cache.ttl-second" = "0")"""
            // select 2 rows
            order_qt_sql_2row """select * from test_hive_meta_cache_db.sales"""
            // insert into same partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(3, 2.0, 2024)"""
            // select 3 row
            order_qt_sql_3row """select * from test_hive_meta_cache_db.sales"""

            // insert into new partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 3.0, 2025)"""
            // still select 3 rows
            order_qt_sql_3row """select * from test_hive_meta_cache_db.sales"""
            // alter wrong catalog property
            test {
                sql """alter catalog ${catalog_name_no_cache} set properties ("partition.cache.ttl-second" = "-2")"""
                exception "is wrong"
            }
            // alter catalog property, disable partition cache
            sql """alter catalog ${catalog_name_no_cache} set properties ("partition.cache.ttl-second" = "0")"""
            // select 4 rows
            order_qt_sql_4row """select * from test_hive_meta_cache_db.sales"""
            // insert into new partition
            hive_docker """insert into test_hive_meta_cache_db.sales values(1, 4.0, 2026)"""
            // select 5 rows
            order_qt_sql_5row """select * from test_hive_meta_cache_db.sales"""
            sql """drop table test_hive_meta_cache_db.sales"""

            // test schema cache
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            // 1. create catalog with default property fisrt
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            hive_docker """drop database if exists test_hive_meta_cache_db CASCADE"""
            hive_docker """create database test_hive_meta_cache_db"""
            hive_docker """
                CREATE TABLE test_hive_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                )
                PARTITIONED BY (year INT)
                STORED AS PARQUET;
            """
            // desc table, 3 columns
            qt_sql_3col "desc test_hive_meta_cache_db.sales";
            // add a new column in hive
            hive_docker "alter table test_hive_meta_cache_db.sales add columns(k3 string)"
            // desc table, still 3 columns
            qt_sql_3col "desc test_hive_meta_cache_db.sales";
            // refresh and check
            sql "refresh table test_hive_meta_cache_db.sales";
            // desc table, 4 columns
            qt_sql_4col "desc test_hive_meta_cache_db.sales";

            // create catalog without schema cache
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'schema.cache.ttl-second' = '0'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            // desc table, 4 columns
            qt_sql_4col "desc test_hive_meta_cache_db.sales";
            // add a new column in hive
            hive_docker "alter table test_hive_meta_cache_db.sales add columns(k4 string)"
            // desc table, 5 columns
            qt_sql_5col "desc test_hive_meta_cache_db.sales";

            // modify property
            // alter wrong catalog property
            test {
                sql """alter catalog ${catalog_name_no_cache} set properties ("schema.cache.ttl-second" = "-2")"""
                exception "is wrong"
            }
            sql """alter catalog ${catalog_name_no_cache} set properties ("schema.cache.ttl-second" = "0")"""
            // desc table, 5 columns
            qt_sql_5col "desc test_hive_meta_cache_db.sales";
            // add a new column in hive
            hive_docker "alter table test_hive_meta_cache_db.sales add columns(k5 string)"
            // desc table, 6 columns
            qt_sql_6col "desc test_hive_meta_cache_db.sales";
            sql """drop table test_hive_meta_cache_db.sales"""

            // test schema cache with get_schema_from_table
            sql """drop catalog if exists ${catalog_name_no_cache};"""
            // 1. create catalog with schema cache off and get_schema_from_table
            sql """
            create catalog ${catalog_name_no_cache} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'schema.cache.ttl-second' = '0',
                'get_schema_from_table' = 'true'
            );
            """
            sql """switch ${catalog_name_no_cache}"""
            hive_docker """drop database if exists test_hive_meta_cache_db CASCADE"""
            hive_docker """create database test_hive_meta_cache_db"""
            hive_docker """
                CREATE TABLE test_hive_meta_cache_db.sales (
                  id INT,
                  amount DOUBLE
                )
                PARTITIONED BY (year INT)
                STORED AS PARQUET;
            """
            // desc table, 3 columns
            qt_sql_3col "desc test_hive_meta_cache_db.sales";
            // show create table , 3 columns
            def sql_sct01_3col = sql "show create table test_hive_meta_cache_db.sales"
            println "${sql_sct01_3col}"
            assertTrue(sql_sct01_3col[0][1].contains("CREATE TABLE `sales`(\n  `id` int,\n  `amount` double)\nPARTITIONED BY (\n `year` int)"));
            
            // add a new column in hive
            hive_docker "alter table test_hive_meta_cache_db.sales add columns(k1 string)"
            // desc table, 4 columns
            qt_sql_4col "desc test_hive_meta_cache_db.sales";
            // show create table, 4 columns
            def sql_sct01_4col = sql "show create table test_hive_meta_cache_db.sales"
            println "${sql_sct01_4col}"
            assertTrue(sql_sct01_4col[0][1].contains("CREATE TABLE `sales`(\n  `id` int,\n  `amount` double,\n  `k1` string)\nPARTITIONED BY (\n `year` int)"));

            // open schema cache
            sql """alter catalog ${catalog_name_no_cache} set properties ("schema.cache.ttl-second" = "120")"""
            // add a new column in hive
            hive_docker "alter table test_hive_meta_cache_db.sales add columns(k2 string)"
            // desc table, 5 columns
            qt_sql_5col "desc test_hive_meta_cache_db.sales";
            // show create table, 5 columns
            def sql_sct01_5col = sql "show create table test_hive_meta_cache_db.sales"
            println "${sql_sct01_5col}"
            assertTrue(sql_sct01_5col[0][1].contains("CREATE TABLE `sales`(\n  `id` int,\n  `amount` double,\n  `k1` string,\n  `k2` string)\nPARTITIONED BY (\n `year` int)"));
            // add a new column in hive
            hive_docker "alter table test_hive_meta_cache_db.sales add columns(k3 string)"
            // desc table, still 5 columns
            qt_sql_5col "desc test_hive_meta_cache_db.sales";
            // show create table always see latest schema, 6 columns
            def sql_sct01_6col = sql "show create table test_hive_meta_cache_db.sales"
            println "${sql_sct01_6col}"
            assertTrue(sql_sct01_6col[0][1].contains("CREATE TABLE `sales`(\n  `id` int,\n  `amount` double,\n  `k1` string,\n  `k2` string,\n  `k3` string)\nPARTITIONED BY (\n `year` int)"));
            sql """drop table test_hive_meta_cache_db.sales"""
        }
    }
}

