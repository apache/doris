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

import static java.util.concurrent.TimeUnit.SECONDS
import org.awaitility.Awaitility

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

    logger.info("start delete local flink-doris-case.jar....")
    def delete_local_flink_jar = "rm -rf flink-doris-case.jar".execute()
    logger.info("start download regression/flink-doris-case.jar ....")
    logger.info("getS3Url: ${getS3Url()}")
    def download_flink_jar = "wget --quiet --continue --tries=5 ${getS3Url()}/regression/flink-doris-case.jar".execute().getText()

    def file = new File('flink-doris-case.jar')
    if (file.exists()) {
        def fileSize = file.length()
        logger.info("finish download flink-doris-case.jar, size " + fileSize)
    } else {
        logger.info("flink-doris-case.jar download failed")
        throw new Exception("File flink-doris-case.jar download failed.")
    }
    def systemJavaPath = ["bash", "-c", "which java"].execute().text.trim()
    logger.info("System java path: ${systemJavaPath}")

    def runtimeJavaHome = System.getProperty("java.home")
    logger.info("Runtime java home: ${runtimeJavaHome}")
    def javaPath = "${runtimeJavaHome}/bin/java"

    def javaVersion = System.getProperty("java.version")
    logger.info("Runtime java version: ${javaVersion}")

    def addOpens = ""
    if (javaVersion.startsWith("17")) {
        addOpens = "--add-opens=java.base/java.nio=ALL-UNNAMED  --add-opens=java.base/java.lang=ALL-UNNAMED"
    }

    def run_cmd = "${javaPath} ${addOpens} -cp flink-doris-case.jar org.apache.doris.FlinkConnectorTypeCase $context.config.feHttpAddress regression_test_flink_connector_p0 $context.config.feHttpUser"
    logger.info("run_cmd : $run_cmd")
    def run_flink_jar = run_cmd.execute().getText()
    logger.info("result: $run_flink_jar")
    // The publish in the commit phase is asynchronous
    Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).await().until(
            {
                def resultTbl = sql """ select count(1) from test_types_sink"""
                logger.info("retry test_types_sink  count: $resultTbl")
                resultTbl.size() >= 1
            })

    logger.info("flink job execute finished.");
    qt_select """ select * from test_types_sink order by id"""
}
