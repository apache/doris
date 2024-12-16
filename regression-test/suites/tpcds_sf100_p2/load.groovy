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

suite('load') {
    def tables = ["dbgen_version":1, "income_band":20, "ship_mode":20, "warehouse":15, "reason": 55,
                  "web_site":24, "call_center":30, "store":402, "promotion": 1000, 
                  "household_demographics":7200, "web_page":2040, "catalog_page":20400, 
                  "time_dim":86400, "date_dim":73049, "item":204000, "customer_demographics":1920800,
                  "customer_address":1000000, "customer":2000000, "web_returns":7197670,
                  "catalog_returns":14404374, "store_returns":28795080, "inventory":399330000,
                  "web_sales":72001237, "catalog_sales":143997065, "store_sales":287997024]
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
    
    sql "set exec_mem_limit=16G;"

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    tables.each { table, rows ->
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text

        // check row count
        def rowCount = sql "select count(*) from ${table}"
        if (rowCount[0][0] != rows) {
            def loadLabel = table + '_' + uniqueID
            sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text

            // load data from cos
            def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
            loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
            sql loadSql

            // check load state
            while (true) {
                def stateResult = sql "show load where Label = '${loadLabel}'"
                logger.info("load result ${stateResult}");
                def loadState = stateResult[stateResult.size() - 1][2].toString()
                if ('CANCELLED'.equalsIgnoreCase(loadState)) {
                    throw new IllegalStateException("load ${loadLabel} failed.")
                } else if ('FINISHED'.equalsIgnoreCase(loadState)) {
                    break
                }
                sleep(5000)
            }
            rowCount = sql "select count(*) from ${table}"
        }
        assertEquals(rows, rowCount[0][0])
    }

    sleep(70000) // wait for row count report of the tables just loaded
    tables.each { table, rows ->
        sql """SET query_timeout = 1800"""
        sql """ ANALYZE TABLE $table WITH SYNC """
    }
}
