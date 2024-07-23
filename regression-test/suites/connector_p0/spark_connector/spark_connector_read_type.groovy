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

suite("spark_connector_read_type", "connector") {


    def tableReadName = "test_spark_read"
    def tableWriterName = "test_spark_writer"

    sql """DROP TABLE IF EXISTS $tableReadName"""
    sql """DROP TABLE IF EXISTS $tableWriterName"""

    sql """
       CREATE TABLE IF NOT EXISTS $tableReadName (
                                id int,
                                c_1 boolean,
                                c_2 tinyint,
                                c_3 smallint,
                                c_4 int,
                                c_5 bigint,
                                c_7 float,
                                c_8 double,
                                c_9 DECIMAL(4,2),
                                c_11 date,
                                c_12 datetime,
                                c_13 char(10),
                                c_14 varchar(10),
                                c_15 string ,
                                c_16 JSON,
                                c_17 Map<STRING, INT> null,
                                c_18 STRUCT<s_id:int(11), s_name:string, s_address:string> NULL,
                                c_19 array<int> null,
                                c_20 largeint
                            ) ENGINE=OLAP
                            DUPLICATE KEY(`id`)
                            COMMENT 'test'
                            DISTRIBUTED BY HASH(`id`) BUCKETS 1
                            PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1"
                            );
      """

    sql """
        INSERT INTO $tableReadName (id, c_1, c_2, c_3, c_4, c_5, c_7, c_8, c_9, c_11, c_12, c_13, c_14, c_15, c_16, c_17, c_18, c_19,c_20)VALUES
        (1, true, 1, 100, 1000, 1000000000, 3.14, 3.14159, 12.34, '2022-01-01', '2022-01-01 12:34:56', 'abcdefg', 'Hello', 'Sample text', '{"key": "value"}', '{"key1": 1, "key2": 2}', STRUCT(1, 'John', '123 Main St'), ARRAY(1, 2, 3),123456789),
        (2, false, 0, 99, 999, 999999999, 2.71, 2.71828, 56.78, '2022-02-02', '2022-02-02 10:20:30', 'xyz', 'World', 'Example text', '{"key": "value2"}', '{"key3": 3, "key4": 4}', STRUCT(2, 'Jane', '456 Elm St'), ARRAY(4, 5, 6),987654321),
        (3, true, 1, 75, 750, 75000, 0.75, 2.34, 5.67, '2024-03-06', '2024-03-06 15:45:00', 'pqr', 'xyz', 'data', '{ "keyA": "valueA", "keyB": "valueB" }', NULL, NULL, NULL, 987654321);
       """

    sql """
       CREATE TABLE IF NOT EXISTS $tableWriterName (
                                id int,
                                c_1 boolean,
                                c_2 tinyint,
                                c_3 smallint,
                                c_4 int,
                                c_5 bigint,
                                c_7 float,
                                c_8 double,
                                c_9 DECIMAL(4,2),
                                c_11 date,
                                c_12 datetime,
                                c_13 char(10),
                                c_14 varchar(10),
                                c_15 string ,
                                c_16 JSON,
                                c_17 Map<STRING, INT> null,
                                c_18 STRUCT<s_id:int(11), s_name:string, s_address:string> NULL,
                                c_19 array<int> null,
                                c_20 largeint
                            ) ENGINE=OLAP
                            DUPLICATE KEY(`id`)
                            COMMENT 'test'
                            DISTRIBUTED BY HASH(`id`) BUCKETS 1
                            PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1"
                            );
      """

    logger.info("start delete local spark doris demo jar...")
    def delete_local_spark_jar = "rm -rf spark-doris-read.jar".execute()
    logger.info("start download spark doris demo ...")
    logger.info("getS3Url ==== ${getS3Url()}")
    def download_spark_jar = "/usr/bin/curl ${getS3Url()}/regression/spark-doris-read-jar-with-dependencies.jar --output spark-doris-read.jar".execute().getText()
    logger.info("finish download spark doris demo ...")
    def run_cmd = "java -jar spark-doris-read.jar $context.config.feHttpAddress $context.config.feHttpUser regression_test_connector_p0_spark_connector.$tableReadName regression_test_connector_p0_spark_connector.$tableWriterName"
    logger.info("run_cmd : $run_cmd")
    def proc = run_cmd.execute()
    def sout = new StringBuilder()
    def serr = new StringBuilder()
    proc.consumeProcessOutput(sout, serr)
    proc.waitForOrKill(1200_000)
    if (proc.exitValue() != 0) {
      logger.warn("failed to execute jar: code=${proc.exitValue()}, " + "output: ${sout.toString()}, error: ${serr.toString()}")
    }

    qt_select """ select * from $tableWriterName order by id"""



}
