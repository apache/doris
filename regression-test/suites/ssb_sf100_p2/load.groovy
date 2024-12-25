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


suite('load') {
    def tables = [date: 2556, supplier: 200000, part: 1400000, customer: 3000000, lineorder: 600037902]
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

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    tables.each { table, rows ->
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text

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

    // lineorder_flat
    def table = "lineorder_flat"
    def table_rows = 600037902
    sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text
    def rowCount = sql "select count(*) from ${table}"
    if (rowCount[0][0] != table_rows) {
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
        sql "set insert_timeout=3600"
        sql "sync"
        def r = sql "select @@insert_timeout"
        assertEquals(3600, r[0][0])
        def year_cons = [
            'lo_orderdate<19930101',
            'lo_orderdate>=19930101 and lo_orderdate<19940101',
            'lo_orderdate>=19940101 and lo_orderdate<19950101',
            'lo_orderdate>=19950101 and lo_orderdate<19960101',
            'lo_orderdate>=19960101 and lo_orderdate<19970101',
            'lo_orderdate>=19970101 and lo_orderdate<19980101',
            'lo_orderdate>=19980101'
        ]
        for (String con in year_cons){
            sql """
            INSERT INTO lineorder_flat 
            SELECT LO_ORDERDATE, LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, 
                   LO_SUPPKEY, LO_ORDERPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, 
                   LO_EXTENDEDPRICE, LO_ORDTOTALPRICE, LO_DISCOUNT, LO_REVENUE, 
                   LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE, C_NAME, C_ADDRESS, 
                   C_CITY, C_NATION, C_REGION, C_PHONE, C_MKTSEGMENT, S_NAME, S_ADDRESS, 
                   S_CITY, S_NATION, S_REGION, S_PHONE, P_NAME, P_MFGR, P_CATEGORY, 
                   P_BRAND, P_COLOR, P_TYPE, P_SIZE, P_CONTAINER 
            FROM ( SELECT lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, 
                          lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, 
                          lo_extendedprice, lo_ordtotalprice, lo_discount, lo_revenue, 
                          lo_supplycost, lo_tax, lo_commitdate, lo_shipmode FROM lineorder WHERE ${con} ) l 
                INNER JOIN customer c ON (c.c_custkey = l.lo_custkey) 
                INNER JOIN supplier s ON (s.s_suppkey = l.lo_suppkey) 
                INNER JOIN part p ON (p.p_partkey = l.lo_partkey);"""
        }
        sql "sync"
        rowCount = sql "select count(*) from ${table}"
        assertEquals(table_rows, rowCount[0][0])
    }


}
