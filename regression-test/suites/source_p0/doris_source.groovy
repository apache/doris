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

suite("doris_source") {

    def tableName = "doris_source"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql new File("""${context.file.parent}/ddl/create.sql""").text
    sql new File("""${context.file.parent}/ddl/insert.sql""").text
    logger.info("start delete local doris source jar...")
    def delete_local_doris_source_jar = "rm -rf doris-source-demo.jar".execute()
    logger.info("start download doris source demo ...")
    logger.info("getS3Url ==== ${getS3Url()}")
    def download_doris_source_jar = "curl ${getS3Url()}/regression/doris-source-demo.jar --output doris-source-demo.jar".execute().getText()
    logger.info("finish download doris source demo ...")
    def run_cmd = "java -cp doris-source-demo.jar org.apache.doris.sdk.DorisReaderExample $context.config.feHttpAddress $context.config.feHttpUser regression_test_source_p0 $tableName"
    logger.info("run_cmd : $run_cmd")
    def run_source_jar = run_cmd.execute().getText()
    logger.info("result: $run_source_jar")

}
