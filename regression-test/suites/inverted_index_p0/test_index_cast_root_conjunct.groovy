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

// A conjunct that is only a cast of a column, such as CAST(s AS BOOLEAN), reads the column
// itself. When the index answers every other filter on that column and the column is not
// selected, the column must still be read for that conjunct. The same queries run with the
// index on and off, so both must match the same expected rows.
suite("test_index_cast_root_conjunct") {
    def table = "test_index_cast_root_conjunct"
    sql "DROP TABLE IF EXISTS ${table}"
    sql """
        CREATE TABLE ${table} (
            id INT,
            s VARCHAR(16),
            n VARCHAR(8),
            INDEX idx_s (s) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_n (n) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO ${table}
        SELECT number,
               if(number % 7 = 6, NULL, if(number % 2 = 0, 'true', 'false')),
               cast(number % 5 AS STRING)
        FROM numbers("number" = "300")
    """
    sql "sync"
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_condition_cache = false"
    sql "set enable_inverted_index_query_cache = false"

    def filters = [
        match       : "s MATCH 'true' AND CAST(s AS BOOLEAN)",
        match_any   : "s MATCH_ANY 'true false' AND CAST(s AS BOOLEAN)",
        match_not   : "s MATCH_ANY 'true false' AND NOT CAST(s AS BOOLEAN)",
        in_list     : "n IN ('0', '1') AND CAST(n AS BOOLEAN)",
        in_list_int : "n IN ('0', '1') AND CAST(n AS INT) = 1",
    ]
    filters.each { tag, filter ->
        [index_on: "true", index_off: "false"].each { mode, index ->
            sql "set enable_inverted_index_query = ${index}"
            "order_qt_${tag}_${mode}_count"("SELECT count(id), sum(id) FROM ${table} WHERE ${filter}")
            "order_qt_${tag}_${mode}_rows"("SELECT id FROM ${table} WHERE ${filter} ORDER BY id LIMIT 8")
            // Selecting the column reads it anyway.
            "order_qt_${tag}_${mode}_selected"("SELECT id, s, n FROM ${table} WHERE ${filter} ORDER BY id LIMIT 8")
        }
    }

    // SEARCH needs the index, so it has no index-off run; it must match the MATCH rows above.
    sql "set enable_inverted_index_query = true"
    order_qt_search_count "SELECT count(id), sum(id) FROM ${table} WHERE search('s:true') AND CAST(s AS BOOLEAN)"
    order_qt_search_rows "SELECT id FROM ${table} WHERE search('s:true') AND CAST(s AS BOOLEAN) ORDER BY id LIMIT 8"

    sql "DROP TABLE IF EXISTS ${table}"
}
