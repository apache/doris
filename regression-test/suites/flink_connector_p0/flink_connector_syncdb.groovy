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
"light_schema_change" = "true"
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
"light_schema_change" = "true"
);"""

    logger.info("start delete local flink-doris-syncdb.jar....")
    def delete_local_flink_jar = "rm -rf flink-doris-syncdb.jar".execute()
    logger.info("start download flink-doris-syncdb.jar ....")
    logger.info("getS3Url: ${getS3Url()}")
    def download_flink_jar = "curl ${getS3Url()}/regression/flink-doris-syncdb.jar  --output flink-doris-syncdb.jar".execute().getText()
    logger.info("finish download flink-doris-syncdb.jar ...")
    def run_cmd = "java -cp flink-doris-syncdb.jar org.apache.doris.DatabaseFullSync $context.config.feHttpAddress regression_test_flink_connector_p0 $context.config.feHttpUser"
    logger.info("run_cmd : $run_cmd")
    def run_flink_jar = run_cmd.execute().getText()
    logger.info("result: $run_flink_jar")
    qt_select """ select * from $tableName1 order by id"""
    qt_select """ select * from $tableName2 order by id"""

}
