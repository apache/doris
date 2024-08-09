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

suite("load_two_step") {

    // Import once, use unique key, use seq and delete
    // Map[tableName, rowCount]
    def tables = [nation: 25, customer: 15000000, lineitem: 600037902, orders: 150000000, part: 20000000, partsupp: 80000000, region: 5, supplier: 1000000]
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    // set fe configuration
    sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    tables.each { table, rows ->
        sql """ DROP TABLE IF EXISTS $table """
        // create table if not exists
        sql new File("""${context.file.parentFile.parent}/ddl/${table}_sequence.sql""").text

        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parentFile.parent}/ddl/${table}_load_sequence.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            logger.info("load result is ${stateResult}")
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                sql 'sync'
                for (int i = 1; i <= 5; i++) {
                    def loadRowCount = sql "select count(1) from ${table}"
                    logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                    assertTrue(loadRowCount[0][0] == rows)
                }
                sql new File("""${context.file.parentFile.parent}/ddl/${table}_delete.sql""").text
                sql 'sync'
                for (int i = 1; i <= 5; i++) {
                    def loadRowCount = sql "select count(1) from ${table}"
                    logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                    assertTrue(loadRowCount[0][0] == 0)
                }
                break
            }
            sleep(5000)
        }
    }

    Thread.sleep(70000) // wait for row count report of the tables just loaded
    tables.each { table, rows ->
        sql """SET query_timeout = 1800"""
        sql """ ANALYZE TABLE $table WITH SYNC """
    }
}
