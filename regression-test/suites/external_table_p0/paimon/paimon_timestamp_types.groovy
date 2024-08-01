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

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
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
        sql """use flink_paimon;"""
        logger.info("use test_paimon_db")


        def test_ltz_ntz = { table -> 
            qt_ltz_ntz2 """ select * from ${table} """
            qt_ltz_ntz3 """ select cmap1 from ${table} """
            qt_ltz_ntz4 """ select cmap1['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz5 """ select cmap1['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz6 """ select cmap2 from ${table} """
            qt_ltz_ntz7 """ select cmap2['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz8 """ select cmap2['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz9 """ select cmap3 from ${table} """
            qt_ltz_ntz10 """ select cmap4 from ${table} """
            qt_ltz_ntz11 """ select cmap4['1'] from ${table} """
            qt_ltz_ntz12 """ select cmap4['2'] from ${table} """
            qt_ltz_ntz13 """ select cmap4['3'] from ${table} """
            qt_ltz_ntz14 """ select cmap4['4'] from ${table} """
            qt_ltz_ntz15 """ select carray1 from ${table} """
            qt_ltz_ntz16 """ select carray1[2] from ${table} """
            qt_ltz_ntz17 """ select carray2 from ${table} """
            qt_ltz_ntz18 """ select carray2[2] from ${table} """
            qt_ltz_ntz19 """ select crow1 from ${table} """
            qt_ltz_ntz20 """ select crow2 from ${table} """
            qt_ltz_ntz21 """ select crow3 from ${table} """
        }

        def test_ltz_ntz_simple = { table -> 
            qt_ltz_ntz_simple2 """ select * from ${table} """
            qt_ltz_ntz_simple3 """ select cmap1 from ${table} """
            qt_ltz_ntz_simple4 """ select cmap1['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple5 """ select cmap1['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple6 """ select cmap2 from ${table} """
            qt_ltz_ntz_simple7 """ select cmap2['2024-01-01 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple8 """ select cmap2['2024-01-03 10:12:34.123456'] from ${table} """
            qt_ltz_ntz_simple9 """ select carray1 from ${table} """
            qt_ltz_ntz_simple10 """ select carray1[2] from ${table} """
            qt_ltz_ntz_simple11 """ select carray2 from ${table} """
            qt_ltz_ntz_simple12 """ select carray2[2] from ${table} """
            qt_ltz_ntz_simple13 """ select crow from ${table} """
            qt_ltz_ntz_simple14 """ select STRUCT_ELEMENT(crow, 'crow1') from ${table} """
            qt_ltz_ntz_simple15 """ select STRUCT_ELEMENT(crow, 'crow2') from ${table} """
        }

        def test_scale = {
            def ts_scale_orc = """select * from ts_scale_orc"""
            def ts_scale_parquet = """select * from ts_scale_parquet"""
            qt_c1 ts_scale_orc
            qt_c2 ts_scale_parquet

        }

        sql """set force_jni_scanner=true"""
        test_scale()
        // test_ltz_ntz("test_timestamp_ntz_ltz_orc")
        // test_ltz_ntz("test_timestamp_ntz_ltz_parquet")
        test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_orc")
        // test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_parquet")

        sql """set force_jni_scanner=false"""
        test_scale()
        // test_ltz_ntz("test_timestamp_ntz_ltz_orc")
        // test_ltz_ntz("test_timestamp_ntz_ltz_parquet")
        test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_orc")
        test_ltz_ntz_simple("test_timestamp_ntz_ltz_simple_parquet")
    } finally {
        sql """set force_jni_scanner=false"""
    }

    // TODO:
    // 1. Fix: native read + parquet + timestamp(7/8/9) (ts7,ts8,ts9), it will be 8 hour more
    // 2. paimon bugs: native read + orc + timestamp_ltz.
    //                 In the Shanghai time zone, the read data will be 8 hours less, 
    //                 because the data written by Flink to the orc file is UTC, but the time zone saved in the orc file is Shanghai.
    //                 Currently, Paimon will not fix this problem, but recommends using the parquet format.
    // 3. paimon bugs: jni read + parquet + row types + timestamp.
    //                 Data of the timestamp type should be converted to the timestamp type, but paimon converted it to the long type.
    //                 Will be fixed in paimon0.9

}


/*

--- flink-sql:

SET 'table.local-time-zone' = 'Asia/Shanghai';

CREATE CATALOG paimon_minio WITH (
    'type' = 'paimon',
    'warehouse' = 's3://warehouse/wh',
    's3.endpoint' = 'http://172.21.0.101:19001',
    's3.access-key' = 'admin',
    's3.secret-key' = 'password',
    's3.path.style.access' = 'true'
);

use catalog paimon_minio;

CREATE DATABASE IF NOT EXISTS flink_paimon;

use flink_paimon;

create table ts_scale_orc (
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

create table ts_scale_parquet (
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

insert into ts_scale_orc values (
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

insert into ts_scale_parquet values (
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

create table test_timestamp_ntz_ltz_orc (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    cmap3 MAP<int, ROW<cmap_row1 timestamp>>,
    cmap4 MAP<int, ROW<cmap_rowid int, cmap_row1 timestamp, cmap_row2 TIMESTAMP_LTZ>>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow1 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>,
    crow2 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_row ROW<crow_row1 timestamp, crow_row2 TIMESTAMP_LTZ>>,
    crow3 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_array1 ARRAY<timestamp>, crow_array2 ARRAY<TIMESTAMP_LTZ>>
) with (
    'write-only' = 'true',
    'file.format' = 'orc'
);

create table test_timestamp_ntz_ltz_parquet (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    cmap3 MAP<int, ROW<cmap_row1 timestamp>>,
    cmap4 MAP<int, ROW<cmap_rowid int, cmap_row1 timestamp, cmap_row2 TIMESTAMP_LTZ>>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow1 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>,
    crow2 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_row ROW<crow_row1 timestamp, crow_row2 TIMESTAMP_LTZ>>,
    crow3 ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ, crow_array1 ARRAY<timestamp>, crow_array2 ARRAY<TIMESTAMP_LTZ>>
) with (
    'write-only' = 'true',
    'file.format' = 'parquet'
);

insert into test_timestamp_ntz_ltz_orc values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[1, ROW(timestamp '2024-01-01 10:12:34.123456'),
        2, ROW(timestamp '2024-01-02 10:12:34.123456'),
        3, ROW(timestamp '2024-01-03 10:12:34.123456')
        ],
    MAP[1, ROW(1, timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
        2, ROW(2, timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'),
        3, ROW(3, timestamp '2024-01-05 10:12:34.123456', timestamp '2024-01-06 10:12:34.123456'),
        4, ROW(4, timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456')
        ],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ROW(timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456')),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ARRAY[timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456', timestamp '2024-01-05 10:12:34.123456'], ARRAY[timestamp '2024-01-06 10:12:34.123456', timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456'])
);

-- Currently paimon does not support nested parquet formats
insert into test_timestamp_ntz_ltz_parquet values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[1, ROW(timestamp '2024-01-01 10:12:34.123456'),
        2, ROW(timestamp '2024-01-02 10:12:34.123456'),
        3, ROW(timestamp '2024-01-03 10:12:34.123456')
        ],
    MAP[1, ROW(1, timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
        2, ROW(2, timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'),
        3, ROW(3, timestamp '2024-01-05 10:12:34.123456', timestamp '2024-01-06 10:12:34.123456'),
        4, ROW(4, timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456')
        ],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456'),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ROW(timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456')),
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', ARRAY[timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456', timestamp '2024-01-05 10:12:34.123456'], ARRAY[timestamp '2024-01-06 10:12:34.123456', timestamp '2024-01-07 10:12:34.123456', timestamp '2024-01-08 10:12:34.123456'])
);

create table test_timestamp_ntz_ltz_simple_orc (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>
) with (
    'write-only' = 'true',
    'file.format' = 'orc'
);

create table test_timestamp_ntz_ltz_simple_parquet (
    id int,
    cmap1 MAP<timestamp, timestamp>,
    cmap2 MAP<TIMESTAMP_LTZ, TIMESTAMP_LTZ>,
    carray1 ARRAY<timestamp>,
    carray2 ARRAY<TIMESTAMP_LTZ>,
    crow ROW<crow1 timestamp, crow2 TIMESTAMP_LTZ>
) with (
    'write-only' = 'true',
    'file.format' = 'parquet'
);

insert into test_timestamp_ntz_ltz_simple_orc values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456')
);

insert into test_timestamp_ntz_ltz_simple_parquet values (
    1,
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    MAP[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456', timestamp '2024-01-04 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ARRAY[timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456', timestamp '2024-01-03 10:12:34.123456'],
    ROW(timestamp '2024-01-01 10:12:34.123456', timestamp '2024-01-02 10:12:34.123456')
);

*/
