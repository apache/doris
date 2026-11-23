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

suite("test_be_fold_session_context") {
    sql "SET enable_sql_cache = false"
    def dateExpr = "str_to_date(concat('2026-11-', CAST(10 + crc32('a') % 10 AS STRING)), '%Y-%m-%d')"
    def dateQuery = "SELECT monthname(${dateExpr}) AS m, dayname(${dateExpr}) AS d"
    [false, true].each { fold ->
        sql "SET enable_fold_constant_by_be = ${fold}"
        ["zh_CN", "en_US"].each { locale ->
            sql "SET lc_time_names = '${locale}'"
            explain {
                sql dateQuery
                if (fold) {
                    notContains "crc32"
                    contains(locale == "zh_CN" ? "十一月" : "November")
                } else {
                    contains "crc32"
                }
            }
            quickRunTest("date_${fold}_${locale}", dateQuery, true)
        }
    }

    sql "DROP TABLE IF EXISTS test_be_fold_session_context_tbl"
    sql """CREATE TABLE test_be_fold_session_context_tbl (id INT, txt STRING)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO test_be_fold_session_context_tbl VALUES (1, 'abc'), (2, 'def')"
    def regexQuery = """SELECT id FROM test_be_fold_session_context_tbl
        WHERE txt = 'abc' AND ('abc' REGEXP '(?U)a+') ORDER BY id"""
    sql "SET enable_hyperscan_fallback = false"
    [false, true].each { fold ->
        sql "SET enable_fold_constant_by_be = ${fold}"
        test {
            sql regexQuery
            exception "hs_compile regex pattern error"
        }
    }
    sql "SET enable_hyperscan_fallback = true"
    [false, true].each { fold ->
        sql "SET enable_fold_constant_by_be = ${fold}"
        quickRunTest("regex_${fold}", regexQuery, true)
    }
}
