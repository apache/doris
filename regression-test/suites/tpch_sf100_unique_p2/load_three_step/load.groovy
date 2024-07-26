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

suite("load_three_step") {

    // Import multiple times,  use unique key, use seq and delete
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


    def uniqueID1 = Math.abs(UUID.randomUUID().hashCode()).toString()
    def uniqueID2 = Math.abs(UUID.randomUUID().hashCode()).toString()
    tables.each { table, rows ->
        sql """ DROP TABLE IF EXISTS $table """
        // create table if not exists
        sql new File("""${context.file.parentFile.parent}/ddl/${table}_sequence.sql""").text
        def loadLabel1 = table + "_" + uniqueID1
        // load data from cos
        def loadSql1 = new File("""${context.file.parentFile.parent}/ddl/${table}_load_sequence.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql1 = loadSql1.replaceAll("\\\$\\{loadLabel\\}", loadLabel1) + s3WithProperties
        sql loadSql1
        // check load state
        while (true) {
            def stateResult1 = sql "show load where Label = '${loadLabel1}'"
            logger.info("load result is ${stateResult1}")
            def loadState1 = stateResult1[stateResult1.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState1)) {
                throw new IllegalStateException("load ${loadLabel1} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState1)) {
                sql 'sync'
                for (int i = 1; i <= 5; i++) {
                    def loadRowCount = sql "select count(1) from ${table}"
                    logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                    assertTrue(loadRowCount[0][0] == rows)
                }
                def loadLabel2 = table + "_" + uniqueID2
                def loadSql2 = new File("""${context.file.parentFile.parent}/ddl/${table}_load_sequence.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
                loadSql2 = loadSql2.replaceAll("\\\$\\{loadLabel\\}", loadLabel2) + s3WithProperties
                sql loadSql2

                while (true) {
                    def stateResult2 = sql "show load where Label = '${loadLabel2}'"
                    logger.info("load result is ${stateResult2}")
                    def loadState2 = stateResult2[stateResult2.size() - 1][2].toString()
                    if ("CANCELLED".equalsIgnoreCase(loadState2)) {
                        throw new IllegalStateException("load ${loadLabel2} failed.")
                    } else if ("FINISHED".equalsIgnoreCase(loadState2)) {
                        sql 'sync'
                        for (int i = 1; i <= 5; i++) {
                            def loadRowCount = sql "select count(1) from ${table}"
                            logger.info("select ${table} numbers: ${loadRowCount[0][0]}".toString())
                            assertTrue(loadRowCount[0][0] == rows)
                        }
                        break;
                    }
                    sleep(5000)
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
