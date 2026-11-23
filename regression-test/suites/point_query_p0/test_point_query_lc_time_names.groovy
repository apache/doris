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

suite("test_point_query_lc_time_names", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_point_query_lc_time_names_tbl"
    sql """CREATE TABLE test_point_query_lc_time_names_tbl (id INT, d DATE)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                   "store_row_column" = "true")"""
    sql """INSERT INTO test_point_query_lc_time_names_tbl VALUES
        (1, '2026-11-17'), (2, '2026-03-02')"""
    sql "SET enable_sql_cache = false"
    def query = "SELECT id, monthname(d), dayname(d) FROM test_point_query_lc_time_names_tbl WHERE id = 1"
    [false, true].each { shortCircuit ->
        sql "SET enable_short_circuit_query = ${shortCircuit}"
        ["zh_CN", "en_US"].each { locale ->
            sql "SET lc_time_names = '${locale}'"
            if (shortCircuit) {
                explain {
                    sql query
                    contains "SHORT-CIRCUIT"
                }
            }
            quickRunTest("text_${shortCircuit}_${locale}", query, true)
        }
    }

    def oldLightweight = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'enable_lightweight_lookup_request'")[0][1]
    def database = sql("SELECT DATABASE()")[0][0]
    def prepareUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, database, false)
    try {
        [false, true].each { lightweight ->
            sql """ADMIN SET FRONTEND CONFIG ("enable_lightweight_lookup_request" = "${lightweight}")"""
            connectToDoris(context.config.jdbcUser, context.config.jdbcPassword, prepareUrl) {
                sql "SET enable_sql_cache = false"
                sql "SET enable_short_circuit_query = true"
                try (def statement = prepareStatement("""SELECT id, monthname(d), dayname(d)
                        FROM test_point_query_lc_time_names_tbl WHERE id = ?""")) {
                    assertEquals(statement.class, com.mysql.cj.jdbc.ServerPreparedStatement)
                    statement.setInt(1, 1)
                    // Keep the same prepared UUID across cold, warm and changed-locale requests.
                    ["zh_CN", "zh_CN", "en_US", "en_US", "zh_CN", "zh_CN"].eachWithIndex { locale, index ->
                        sql "SET lc_time_names = '${locale}'"
                        quickRunTest("prepared_${lightweight}_${index}", statement, true)
                    }
                }
            }
        }
    } finally {
        sql """ADMIN SET FRONTEND CONFIG ("enable_lightweight_lookup_request" = "${oldLightweight}")"""
    }
}
