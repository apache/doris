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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.


import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

suite("flink_connector_type") {

    def tableName1 = "test_types_source"
    def tableName2 = "test_types_sink"
    sql """DROP TABLE IF EXISTS ${tableName1}"""
    sql """DROP TABLE IF EXISTS ${tableName2}"""

    sql """
        CREATE TABLE `test_types_source` (
            `id` int,
            `c1` boolean,
            `c2` tinyint,
            `c3` smallint,
            `c4` int,
            `c5` bigint,
            `c6` largeint,
            `c7` float,
            `c8` double,
            `c9` decimal(12,4),
            `c10` date,
            `c11` datetime,
            `c12` char(1),
            `c13` varchar(256),
            `c14` Array<String>,
            `c15` Map<String, String>,
            `c16` Struct<name: String, age: int>,
            `c17` JSON,
            `c18` variant
        )
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true"
);
    """;

    sql """CREATE TABLE `test_types_sink` like `test_types_source` """

    sql """ INSERT INTO `test_types_source`
VALUES 
(
    1,  
    TRUE, 
    127,  
    32767, 
    2147483647, 
    9223372036854775807, 
    123456789012345678901234567890, 
    3.14,  
    2.7182818284, 
    12345.6789, 
    '2023-05-22',  
    '2023-05-22 12:34:56', 
    'A', 
    'Example text', 
    ['item1', 'item2', 'item3'], 
    {'key1': 'value1', 'key2': 'value2'}, 
    STRUCT('John Doe', 30),  
    '{"key": "value"}',  
    '{"A":"variant_value"}' 
),
(
    2,
    FALSE,
    -128,
    -32768,
    -2147483648,
    -9223372036854775808,
    -123456789012345678901234567890,
    -3.14,
    -2.7182818284,
    -12345.6789,
    '2024-01-01',
    '2024-01-01 00:00:00',
    'B',
    'Another example',
    ['item4', 'item5', 'item6'],
    {'key3': 'value3', 'key4': 'value4'},
    STRUCT('Jane Doe', 25),
    '{"another_key": "another_value"}',
    '{"B":"variant_value1"}' 
);"""

    def thisDb = sql """select database()""";
    thisDb = thisDb[0][0];
    logger.info("current database is ${thisDb}");
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
            "CREATE TABLE source_doris (" +
                    "`id` int,\n" +
                    "`c1` boolean,\n" +
                    "`c2` tinyint,\n" +
                    "`c3` smallint,\n" +
                    "`c4` int,\n" +
                    "`c5` bigint,\n" +
                    "`c6` string,\n" +
                    "`c7` float,\n" +
                    "`c8` double,\n" +
                    "`c9` decimal(12,4),\n" +
                    "`c10` date,\n" +
                    "`c11` TIMESTAMP,\n" +
                    "`c12` char(1),\n" +
                    "`c13` varchar(256),\n" +
                    "`c14` Array<String>,\n" +
                    "`c15` Map<String, String>,\n" +
                    "`c16` ROW<name String, age int>,\n" +
                    "`c17` STRING,\n" +
                    "`c18` STRING"
                    + ") "
                    + "WITH (\n"
                    + "  'connector' = 'doris',\n"
                    + "  'fenodes' = '" + context.config.feHttpAddress + "',\n"
                    + "  'table.identifier' = '${thisDb}.test_types_source',\n"
                    + "  'username' = '" + context.config.feHttpUser + "',\n"
                    + "  'password' = '" + context.config.feHttpPassword +  "'\n"
                    + ")");


    tEnv.executeSql(
            "CREATE TABLE doris_test_sink (" +
                    "`id` int,\n" +
                    "`c1` boolean,\n" +
                    "`c2` tinyint,\n" +
                    "`c3` smallint,\n" +
                    "`c4` int,\n" +
                    "`c5` bigint,\n" +
                    "`c6` string,\n" +
                    "`c7` float,\n" +
                    "`c8` double,\n" +
                    "`c9` decimal(12,4),\n" +
                    "`c10` date,\n" +
                    "`c11` TIMESTAMP,\n" +
                    "`c12` char(1),\n" +
                    "`c13` varchar(256),\n" +
                    "`c14` Array<String>,\n" +
                    "`c15` Map<String, String>,\n" +
                    "`c16` ROW<name String, age int>,\n" +
                    "`c17` STRING,\n" +
                    "`c18` STRING"
                    + ") "
                    + "WITH (\n"
                    + "  'connector' = 'doris',\n"
                    + "  'fenodes' = '" + context.config.feHttpAddress + "',\n"
                    + "  'table.identifier' = '${thisDb}.test_types_sink',\n"
                    + "  'username' = '" + context.config.feHttpUser + "',\n"
                    + "  'password' = '" + context.config.feHttpPassword +  "',\n"
                    + "  'sink.properties.format' = 'json',\n"
                    + "  'sink.properties.read_json_by_line' = 'true',\n"
                    + "  'sink.label-prefix' = 'label" + UUID.randomUUID() + "'"
                    + ")");

    TableResult tableResult = tEnv.executeSql("INSERT INTO doris_test_sink select * from source_doris");
    tableResult.await();
    logger.info("flink job execute finished.");
    qt_select """ select * from test_types_sink order by id"""
}
