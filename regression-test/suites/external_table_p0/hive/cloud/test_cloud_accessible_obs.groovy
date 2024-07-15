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

suite("test_cloud_accessible_obs", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableObjStorageTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String ak = context.config.otherConfigs.get("hwYunAk")
        String sk = context.config.otherConfigs.get("hwYunSk")
        String hms_catalog_name = "test_cloud_accessible_obs"
        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES ( 
                'type' = 'hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                'obs.endpoint' = 'obs.cn-north-4.myhuaweicloud.com',
                'obs.access_key' = '${ak}',
                'obs.secret_key' = '${sk}'
            );
        """

        logger.info("catalog " + hms_catalog_name + " created")
        sql """switch ${hms_catalog_name};"""
        logger.info("switched to catalog " + hms_catalog_name)
        sql """ CREATE DATABASE IF NOT EXISTS cloud_accessible """
        sql """ use cloud_accessible """

//        sql """
//            CREATE TABLE `types_obs`(
//              `hms_int` int,
//              `hms_smallint` smallint,
//              `hms_bigint` bigint,
//              `hms_double` double,
//              `hms_string` string,
//              `hms_decimal` decimal(12,4),
//              `hms_char` char(50),
//              `hms_varchar` varchar(50),
//              `hms_bool` boolean,
//              `hms_timstamp` timestamp,
//              `hms_date` date)
//            ROW FORMAT SERDE
//              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
//            WITH SERDEPROPERTIES (
//              'serialization.format' = '1')
//            STORED AS INPUTFORMAT
//              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
//            OUTPUTFORMAT
//              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
//            LOCATION
//              'obs://datalake-bench/base/types'
//        """
        qt_hms_q1 """ select * from types_obs order by hms_int """

//        sql """
//            CREATE TABLE `types_one_part_obs`(
//              `hms_int` int,
//              `hms_smallint` smallint,
//              `hms_bigint` bigint,
//              `hms_double` double,
//              `hms_string` string,
//              `hms_decimal` decimal(12,4),
//              `hms_char` char(50),
//              `hms_varchar` varchar(50),
//              `hms_bool` boolean,
//              `hms_timstamp` timestamp,
//              `hms_date` date)
//            PARTITIONED BY (
//             `dt` string)
//            ROW FORMAT SERDE
//              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
//            WITH SERDEPROPERTIES (
//              'serialization.format' = '1')
//            STORED AS INPUTFORMAT
//              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
//            OUTPUTFORMAT
//              'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
//            LOCATION
//              'obs://datalake-bench/base/types_one_part'
//        """
        qt_hms_q2 """ select * from types_one_part_obs order by hms_int """

//        sql """drop table types_obs;"""
//        sql """drop table types_one_part_obs;"""
        sql """drop database cloud_accessible;"""
        sql """drop catalog ${hms_catalog_name};"""
    }
}
