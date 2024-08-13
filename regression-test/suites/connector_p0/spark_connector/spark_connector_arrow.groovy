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

suite("spark_connector_for_arrow", "connector") {

    sql """use regression_test_connector_p0_spark_connector"""

    sql """
      CREATE TABLE IF NOT EXISTS `spark_connector_primitive` (
        `id` int(11) NOT NULL,
        `c_bool` boolean NULL,
        `c_tinyint` tinyint NULL,
        `c_smallint` smallint NULL,
        `c_int` int NULL,
        `c_bigint` bigint NULL,
        `c_largeint` largeint NULL,
        `c_float` float NULL,
        `c_double` double NULL,
        `c_decimal` DECIMAL(10, 5) NULL,
        `c_date` date NULL,
        `c_datetime` datetime(6) NULL,
        `c_char` char(10) NULL,
        `c_varchar` varchar(10) NULL,
        `c_string` string NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      )"""

    sql """
      CREATE TABLE IF NOT EXISTS `spark_connector_array` (
        `id` int(11) NOT NULL,
        `c_array_boolean` ARRAY<boolean> NULL,
        `c_array_tinyint` ARRAY<tinyint> NULL,
        `c_array_smallint` ARRAY<smallint> NULL,
        `c_array_int` ARRAY<int> NULL,
        `c_array_bigint` ARRAY<bigint> NULL,
        `c_array_largeint` ARRAY<largeint> NULL,
        `c_array_float` ARRAY<float> NULL,
        `c_array_double` ARRAY<double> NULL,
        `c_array_decimal` ARRAY<DECIMAL(10, 5)> NULL,
        `c_array_date` ARRAY<date> NULL,
        `c_array_datetime` ARRAY<datetime(6)> NULL,
        `c_array_char` ARRAY<char(10)> NULL,
        `c_array_varchar` ARRAY<varchar(10)> NULL,
        `c_array_string` ARRAY<string> NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );"""

    sql """
      CREATE TABLE IF NOT EXISTS `spark_connector_map` (
        `id` int(11) NOT NULL,
        `c_map_bool` Map<boolean,boolean> NULL,
        `c_map_tinyint` Map<tinyint,tinyint> NULL,
        `c_map_smallint` Map<smallint,smallint> NULL,
        `c_map_int` Map<int,int> NULL,
        `c_map_bigint` Map<bigint,bigint> NULL,
        `c_map_largeint` Map<largeint,largeint> NULL,
        `c_map_float` Map<float,float> NULL,
        `c_map_double` Map<double,double> NULL,
        `c_map_decimal` Map<DECIMAL(10, 5),DECIMAL(10, 5)> NULL,
        `c_map_date` Map<date,date> NULL,
        `c_map_datetime` Map<datetime(6),datetime(6)> NULL,
        `c_map_char` Map<char(10),char(10)> NULL,
        `c_map_varchar` Map<varchar(10),varchar(10)> NULL,
        `c_map_string` Map<string,string> NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );"""

    sql """
      CREATE TABLE IF NOT EXISTS `spark_connector_struct` (
        `id` int NOT NULL,
        `st` STRUCT<
            `c_bool`:boolean,
            `c_tinyint`:tinyint(4),
            `c_smallint`:smallint(6),
            `c_int`:int(11),
            `c_bigint`:bigint(20),
            `c_largeint`:largeint(40),
            `c_float`:float,
            `c_double`:double,
            `c_decimal`:DECIMAL(10, 5),
            `c_date`:date,
            `c_datetime`:datetime(6),
            `c_char`:char(10),
            `c_varchar`:varchar(10),
            `c_string`:string
          > NULL
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT 'OLAP'
      DISTRIBUTED BY HASH(`id`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );"""

    sql """DELETE FROM spark_connector_primitive where id > 0"""
    sql """DELETE FROM spark_connector_array where id > 0"""
    sql """DELETE FROM spark_connector_map where id > 0"""
    sql """DELETE FROM spark_connector_struct where id > 0"""

    def jar_name = "spark-doris-connector-3.1_2.12-1.3.0-SNAPSHOT-with-dependencies.jar"

    logger.info("start delete local spark doris demo jar...")
    def delete_local_spark_jar = "rm -rf ${jar_name}".execute()
    logger.info("start download spark doris demo ...")
    logger.info("getS3Url ==== ${getS3Url()}")
    def download_spark_jar = "/usr/bin/curl ${getS3Url()}/regression/${jar_name} --output ${jar_name}".execute().getText()
    logger.info("finish download spark doris demo ...")
    def run_cmd = "java -cp ${jar_name} org.apache.doris.spark.testcase.TestStreamLoadForArrowType $context.config.feHttpAddress $context.config.feHttpUser regression_test_connector_p0_spark_connector"
    logger.info("run_cmd : $run_cmd")
    def proc = run_cmd.execute()
    def sout = new StringBuilder()
    def serr = new StringBuilder()
    proc.consumeProcessOutput(sout, serr)
    proc.waitForOrKill(1200_000)
    if (proc.exitValue() != 0) {
      logger.warn("failed to execute jar: code=${proc.exitValue()}, " + "output: ${sout.toString()}, error: ${serr.toString()}")
    }

    qt_q01 """ select * from spark_connector_primitive """
    qt_q02 """ select * from spark_connector_array """
    qt_q03 """ select * from spark_connector_map """
    qt_q04 """ select * from spark_connector_struct """
}
