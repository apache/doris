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

suite("paimon_timestamp_types", "p0,external,doris,external_docker,external_docker_doris") {

    def ts_orc = """select * from ts_orc"""
    def ts_parquet = """select * from ts_parquet"""

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    // The timestamp type of paimon has no logical or converted type,
    // and is conflict with column type change from bigint to timestamp.
    // Deprecated currently.
    if (enabled == null || !enabled.equalsIgnoreCase("enable_deprecated_case")) {
        return
    }

    try {
        String catalog_name = "paimon_timestamp_types"
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='paimon',
                'warehouse' = 's3://warehouse/wh/',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use test_paimon_db;"""
        logger.info("use test_paimon_db")

        sql """set force_jni_scanner=true"""
        qt_c1 ts_orc
        qt_c2 ts_parquet

        sql """set force_jni_scanner=false"""
        qt_c3 ts_orc
        qt_c4 ts_parquet

    } finally {
        sql """set force_jni_scanner=false"""
    }
}


/*

--- flink-sql:

SET 'table.local-time-zone' = 'Asia/Shanghai';

create table ts_orc (
id int,
ts1 timestamp(1), 
ts2 timestamp(2), 
ts3 timestamp(3), 
ts4 timestamp(4),
ts5 timestamp(5), 
ts6 timestamp(6), 
ts7 timestamp(7), 
ts8 timestamp(8), 
ts9 timestamp(9),
ts11 timestamp_ltz(1), 
ts12 timestamp_ltz(2), 
ts13 timestamp_ltz(3), 
ts14 timestamp_ltz(4),
ts15 timestamp_ltz(5), 
ts16 timestamp_ltz(6), 
ts17 timestamp_ltz(7), 
ts18 timestamp_ltz(8), 
ts19 timestamp_ltz(9))
WITH ('file.format' = 'orc','write-only'='true');

create table ts_parquet (
id int,
ts1 timestamp(1), 
ts2 timestamp(2), 
ts3 timestamp(3), 
ts4 timestamp(4),
ts5 timestamp(5), 
ts6 timestamp(6), 
ts7 timestamp(7), 
ts8 timestamp(8), 
ts9 timestamp(9),
ts11 timestamp_ltz(1), 
ts12 timestamp_ltz(2), 
ts13 timestamp_ltz(3), 
ts14 timestamp_ltz(4),
ts15 timestamp_ltz(5), 
ts16 timestamp_ltz(6), 
ts17 timestamp_ltz(7), 
ts18 timestamp_ltz(8), 
ts19 timestamp_ltz(9))
WITH ('file.format' = 'parquet','write-only'='true');

insert into ts_orc values (
    1,
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789');

insert into ts_parquet values (
    1,
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789',
    timestamp '2024-01-02 10:04:05.123456789');

*/
