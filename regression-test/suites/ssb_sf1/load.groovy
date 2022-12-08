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
    def tables = ["customer", "lineorder", "part", "date", "supplier"]
    def columns = ["""c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use""",
                    """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
                    lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
                    lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy""",
                    """p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy""",
                    """d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,
                    d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,
                    d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy""",
                    """s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy"""]
//    for (String table in tables) {
//        sql """ DROP TABLE IF EXISTS $table """
//    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    }
    def i = 0
    for (String tableName in tables) {   
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', columns[i]


            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/ssb/sf1/${tableName}.tbl.split00.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', columns[i]

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/ssb/sf1/${tableName}.tbl.split01.gz"""
            

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
        log.info("yyq ${i}  ${tableName} ${columns[i]}".toString())
        i++
    }

    def table = "lineorder_flat"
    def table_rows = 6001215
    sql new File("""${context.file.parent}/ddl/${table}_create.sql""").text
    def rowCount = sql "select count(*) from ${table}"
    if (rowCount[0][0] != table_rows) {
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
        sql "set global query_timeout=3600"
        def r = sql "select @@query_timeout"
        assertEquals(3600, r[0][0])
        year_cons = [
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
        rowCount = sql "select count(*) from ${table}"
        assertEquals(table_rows, rowCount[0][0])
    }
}
