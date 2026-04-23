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

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq
suite("load") {
    // Map[tableName, rowCount]
    def tables = [customer: 15000000, lineitem: 600037902, nation: 25, orders: 150000000, part: 20000000, partsupp: 80000000, region: 5, supplier: 1000000]
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
    def loadAndWait = { String loadLabel, String loadSql ->
        sql loadSql
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            logger.info("load result is ${stateResult}")
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    // Load data twice to create overlapping rowsets with duplicate keys,
    // which triggers the MOR (Merge-On-Read) unique key merge semantics.
    // Also load DUP tables twice for read_mor_as_dup result comparison.
    for (int loadRound = 1; loadRound <= 2; loadRound++) {
        tables.each { table, rows ->
            if (loadRound == 1) {
                // drop and recreate MOR table on first round
                sql "DROP TABLE IF EXISTS ${table}"
                sql new File("""${context.file.parent}/ddl/${table}.sql""").text
                // drop and recreate DUP table on first round
                sql "DROP TABLE IF EXISTS ${table}_dup"
                sql new File("""${context.file.parent}/ddl/${table}_dup.sql""").text
            }
            // load MOR table
            def morLabel = table + "_round${loadRound}_" + uniqueID
            def morSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
            morSql = morSql.replaceAll("\\\$\\{loadLabel\\}", morLabel) + s3WithProperties
            loadAndWait(morLabel, morSql)

            // load DUP table
            def dupLabel = table + "_dup_round${loadRound}_" + uniqueID
            def dupSql = new File("""${context.file.parent}/ddl/${table}_dup_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
            dupSql = dupSql.replaceAll("\\\$\\{loadLabel\\}", dupLabel) + s3WithProperties
            loadAndWait(dupLabel, dupSql)
        }
    }

    Thread.sleep(70000) // wait for row count report of the tables just loaded
    tables.each { table, rows ->
        sql """SET query_timeout = 1800"""
        // verify MOR row count equals single-copy count (MOR merge deduplicates)
        def morRowCount = sql "select count(*) from ${table}"
        logger.info("MOR table ${table} row count after double load: ${morRowCount[0][0]}, expected: ${rows}")
        assertTrue(morRowCount[0][0] == rows, "MOR table ${table} row count ${morRowCount[0][0]} != expected ${rows} after MOR dedup")
        sql """ ANALYZE TABLE $table WITH SYNC """
        // verify DUP row count equals 2x (DUP keeps all rows)
        def dupRowCount = sql "select count(*) from ${table}_dup"
        logger.info("DUP table ${table}_dup row count after double load: ${dupRowCount[0][0]}, expected: ${rows * 2}")
        assertTrue(dupRowCount[0][0] == rows * 2, "DUP table ${table}_dup row count ${dupRowCount[0][0]} != expected ${rows * 2}")
        sql """ ANALYZE TABLE ${table}_dup WITH SYNC """
    }
}
