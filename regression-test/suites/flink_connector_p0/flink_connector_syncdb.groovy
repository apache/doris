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

suite("flink_connector_syncdb") {

    def tableName1 = "student1"
    def tableName2 = "student2"
    sql """DROP TABLE IF EXISTS ${tableName1}"""
    sql """DROP TABLE IF EXISTS ${tableName2}"""

    sql """
        CREATE TABLE `student1`
(
    `id`   int,
    `name` varchar(32),
    `age`  int
) 
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true",
"enable_unique_key_merge_on_write" = "false"
);
    """
    sql """
CREATE TABLE `student2`
(
    `id`   int,
    `name` varchar(32),
    `age`  int
) 
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true",
"enable_unique_key_merge_on_write" = "false"
);"""

    logger.info("start delete local flink-doris-syncdb.jar....")
    def delete_local_flink_jar = "rm -rf flink-doris-syncdb.jar".execute()
    logger.info("start download regression/flink-doris-syncdb.jar ....")
    logger.info("getS3Url: ${getS3Url()}")
    def download_flink_jar = "wget --quiet --continue --tries=5 ${getS3Url()}/regression/flink-doris-syncdb.jar".execute().getText()

    def file = new File('flink-doris-syncdb.jar')
    if (file.exists()) {
        def fileSize = file.length()
        logger.info("finish download flink-doris-syncdb.jar, size " + fileSize)
    } else {
        logger.info("flink-doris-syncdb.jar download failed")
        throw new Exception("File flink-doris-syncdb.jar download failed.")
    }

    def run_cmd = "java -cp flink-doris-syncdb.jar org.apache.doris.DatabaseFullSync $context.config.feHttpAddress regression_test_flink_connector_p0 $context.config.feHttpUser"
    logger.info("run_cmd : $run_cmd")
    def run_flink_jar = run_cmd.execute().getText()
    logger.info("result: $run_flink_jar")
    // The publish in the commit phase is asynchronous
    Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).await().until(
            {
                def resultTbl1 = sql """ select count(1) from $tableName1"""
                logger.info("retry $tableName1  count: $resultTbl1")
                def resultTbl2 = sql """ select count(1) from $tableName2"""
                logger.info("retry $tableName2 count: $resultTbl2")
                resultTbl1.size() >= 1 && resultTbl2.size >=1
            })

    qt_select """ select * from $tableName1 order by id"""
    qt_select """ select * from $tableName2 order by id"""
}
