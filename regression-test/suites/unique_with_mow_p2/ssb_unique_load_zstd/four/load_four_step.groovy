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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("load_four_step") {
    def tables = ["customer": ["""c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use""", 3000000, 1500000],
                  "lineorder": ["""lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
                                lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
                                lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy""", 600037902, 300018949],
                  "part": ["""p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy""", 1400000, 700000],
                  "date": ["""d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,
                           d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,
                           d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy""", 2556, 1278],
                  "supplier": ["""s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy""", 200000, 100000]]

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()

    // set fe configuration
    sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"

    tables.each { tableName, rows ->
        // create table
        sql """ DROP TABLE IF EXISTS $tableName """
        sql new File("""${context.file.parentFile.parent}/ddl/${tableName}_sequence_create.sql""").text

        // step 1: load data
        // step 2: load all data for 3 times
        for (j in 0..<2) {
            def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
            // load data from cos
            def loadLabel = tableName + '_' + uniqueID
            def loadSql = new File("""${context.file.parentFile.parent}/ddl/${tableName}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
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
            def rowCount = sql "select count(*) from ${tableName}"
            assertEquals(rows[1], rowCount[0][0])
        }

        // step 3: delete 50% data
        sql """ set delete_without_partition = true; """
        sql new File("""${context.file.parentFile.parent}/ddl/${tableName}_part_delete.sql""").text
        sql 'sync'
        for (int i = 1; i <= 5; i++) {
            def loadRowCount = sql "select count(1) from ${tableName}"
            logger.info("select ${tableName} numbers: ${loadRowCount[0][0]}".toString())
            assertTrue(loadRowCount[0][0] == rows[2])
        }

        // step 4: load full data again
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = tableName + '_' + uniqueID
        def loadSql = new File("""${context.file.parentFile.parent}/ddl/${tableName}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
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

        sql 'sync'
        for (int i = 1; i <= 5; i++) {
            def loadRowCount = sql "select count(1) from ${tableName}"
            logger.info("select ${tableName} numbers: ${loadRowCount[0][0]}".toString())
            assertTrue(loadRowCount[0][0] == rows[1])
        }
    }
}
