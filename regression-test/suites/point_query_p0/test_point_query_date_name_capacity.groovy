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

suite("test_point_query_date_name_capacity", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_point_query_date_name_capacity"
    sql """CREATE TABLE test_point_query_date_name_capacity (
        id INT, dates ARRAY<DATE>, datetimes ARRAY<DATETIMEV2(6)>, timestamps ARRAY<TIMESTAMP_NS>)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                   "store_row_column" = "true")"""
    sql """INSERT INTO test_point_query_date_name_capacity VALUES
        (1, array_repeat(CAST('2026-09-20' AS DATE), 1024),
            array_repeat(CAST('2026-09-20 12:34:56.123456' AS DATETIMEV2(6)), 1024),
            array_repeat(CAST('2026-09-20 12:34:56.123456789' AS TIMESTAMP_NS), 1024)),
        (2, array(CAST('2026-09-20' AS DATE), NULL),
            array(CAST('2026-09-20 12:34:56' AS DATETIMEV2(6)), NULL),
            array(CAST('2026-09-20 12:34:56' AS TIMESTAMP_NS), NULL)),
        (3, [], [], []), (4, [NULL], [NULL], [NULL]), (5, NULL, NULL, NULL)"""
    sql "SET enable_sql_cache = false"
    def projection = """SELECT id, array_size(dates), monthname(dates[1]), dayname(dates[1]),
        length(array_join(array_map(x -> dayname(x), dates), '')),
        length(array_join(array_map(x -> monthname(x), dates), '')),
        length(array_join(array_map(x -> dayname(x), datetimes), '')),
        length(array_join(array_map(x -> monthname(x), datetimes), '')),
        length(array_join(array_map(x -> dayname(x), timestamps), '')),
        length(array_join(array_map(x -> monthname(x), timestamps), ''))
        FROM test_point_query_date_name_capacity"""
    [false, true].each { shortCircuit ->
        sql "SET enable_short_circuit_query = ${shortCircuit}"
        ["ru_RU", "zh_CN", "en_US"].each { locale ->
            sql "SET lc_time_names = '${locale}'"
            if (shortCircuit) {
                explain {
                    sql "${projection} WHERE id = 1"
                    contains "SHORT-CIRCUIT"
                }
            }
            (1..5).each { id ->
                quickRunTest("text_${shortCircuit}_${locale}_${id}", "${projection} WHERE id = ${id}", true)
            }
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
                try (def statement = prepareStatement("${projection} WHERE id = ?")) {
                    assertEquals(statement.class, com.mysql.cj.jdbc.ServerPreparedStatement)
                    statement.setInt(1, 1)
                    // Long UTF-8 names must remain safe on both cache hits and locale changes.
                    ["ru_RU", "ru_RU", "zh_CN", "zh_CN", "en_US", "en_US", "ru_RU"].eachWithIndex { locale, index ->
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
