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

// syntax error:
// q06 q13 q15
// Test 23 suites, failed 3 suites

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq
suite("load") {
    def tables = [customer: ["c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment,temp"],
                  lineitem: ["l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,l_linestatus, l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,temp"],
                  nation  : ["n_nationkey, n_name, n_regionkey, n_comment, temp"],
                  orders  : ["o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, temp"],
                  part    : ["p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp"],
                  partsupp: ["ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,temp"],
                  region  : ["r_regionkey, r_name, r_comment,temp"],
                  supplier: ["s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment,temp"]]

    tables.forEach { tableName, columns ->
        sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName}_delete.sql""").text
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table "${tableName}"

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', "${columns[0]}"

            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/tpch/sf0.1/${tableName}.tbl.gz"""

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

    }

    Thread.sleep(70000) // wait for row count report of the tables just loaded
    tables.forEach { tableName, columns ->
        sql """ ANALYZE TABLE $tableName WITH SYNC """
    }

    def table = "revenue1"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/ddl/${table}.sql""").text

    sql """ sync """
}
