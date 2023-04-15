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

suite("test_cancel_schema_change") {
    def table = "lineorder"
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    def rows = 600037902

    // set fe configuration
    sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

    // create table if not exists
    sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    def loadLabel = table + '_' + uniqueID

    // load data from cos
    def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
    loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
    sql loadSql

    // check load state
    while (true) {
        def stateResult = sql "show load where Label = '${loadLabel}'"
        def loadState = stateResult[stateResult.size() - 1][2].toString()
        if ('CANCELLED'.equalsIgnoreCase(loadState)) {
            throw new IllegalStateException("load ${loadLabel} failed.")
        } else if ('FINISHED'.equalsIgnoreCase(loadState)) {
            break
        }
        sleep(5000)
    }
    def rowCount = sql "select count(*) from ${table}"
    assertEquals(rows, rowCount[0][0])

    sql """ ALTER TABLE ${table} ADD INDEX idx (`lo_orderdate`) USING BITMAP; """
    sql """ CANCEL ALTER TABLE COLUMN FROM ${table}; """
    def cancelRes = sql """ SHOW ALTER TABLE COLUMN WHERE TableName = "${table}" ORDER BY CreateTime DESC LIMIT 1; """
    sleep(3000)
    if ('CANCELLED'.equalsIgnoreCase(cancelRes[0][9])) {
        sql """ ALTER TABLE ${table} ADD INDEX idx (`lo_extendedprice`) USING BITMAP; """
    } else {
        throw new IllegalStateException("cancel failed.")
    }
}
