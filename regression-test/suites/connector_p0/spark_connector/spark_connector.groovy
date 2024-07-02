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

suite("spark_connector", "connector") {

    def tableName = "spark_connector"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql new File("""${context.file.parent}/ddl/create.sql""").text
    logger.info("start delete local spark doris demo jar...")
    def delete_local_spark_jar = "rm -rf spark-doris-demo.jar".execute()
    logger.info("start download spark doris demo ...")
    logger.info("getS3Url ==== ${getS3Url()}")
    def download_spark_jar = "/usr/bin/curl ${getS3Url()}/regression/spark-doris-connector-demo-jar-with-dependencies.jar --output spark-doris-demo.jar".execute().getText()
    logger.info("finish download spark doris demo ...")
    def run_cmd = "java -jar spark-doris-demo.jar $context.config.feHttpAddress $context.config.feHttpUser regression_test_connector_p0_spark_connector.$tableName"
    logger.info("run_cmd : $run_cmd")
    def proc = run_cmd.execute()
    def sout = new StringBuilder()
    def serr = new StringBuilder()
    proc.consumeProcessOutput(sout, serr)
    proc.waitForOrKill(1200_000)
    if (proc.exitValue() != 0) {
      logger.warn("failed to execute jar: code=${proc.exitValue()}, " + "output: ${sout.toString()}, error: ${serr.toString()}")
    }
    qt_select """ select * from $tableName order by order_id"""
}
