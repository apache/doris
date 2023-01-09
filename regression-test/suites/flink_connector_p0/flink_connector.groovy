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



suite("flink_connector") {

    def tableName = "flink_connector"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql new File("""${context.file.parent}/ddl/create.sql""").text
    logger.info("start delete local flink doris demo jar...")
    def delete_local_spark_jar = "rm -rf flink-doris-demo.jar".execute()
    logger.info("start download flink doris demo ...")
    logger.info("getS3Url ==== ${getS3Url()}")
    def download_spark_jar = "curl ${getS3Url()}/regression/flink-doris-demo.jar --output flink-doris-demo.jar".execute().getText()
    logger.info("finish download spark doris demo ...")
    def run_cmd = "java -cp flink-doris-demo.jar com.doris.DorisFlinkDfSinkDemo $context.config.feHttpAddress regression_test_flink_connector_p0.$tableName $context.config.feHttpUser"
    logger.info("run_cmd : $run_cmd")
    def run_flink_jar = run_cmd.execute().getText()
    logger.info("result: $run_flink_jar")
    qt_select """ select * from $tableName order by order_id"""

}
