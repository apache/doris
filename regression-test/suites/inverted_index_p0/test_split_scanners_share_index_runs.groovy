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

// Every scanner of a split segment evaluates its index predicates over the whole segment, and
// concurrent identical index queries share one run. Each query runs with the segment split among
// eight scanners and with it read whole; both give the same rows.
suite("test_split_scanners_share_index_runs", "p0") {
    sql "DROP TABLE IF EXISTS test_split_scanners_share_index_runs"
    sql """
        CREATE TABLE test_split_scanners_share_index_runs (
            id INT,
            num INT,
            kw VARCHAR(32),
            txt TEXT,
            INDEX idx_num (num) USING INVERTED,
            INDEX idx_kw (kw) USING INVERTED,
            INDEX idx_txt (txt) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V2",
            "disable_auto_compaction" = "true"
        )
    """
    // One INSERT writes the tablet's only segment.
    sql """
        INSERT INTO test_split_scanners_share_index_runs
        SELECT number, number % 100, concat('k', number % 7),
               concat('the ', elt(number % 4 + 1, 'quick', 'lazy', 'cached', 'caching'),
                      ' fox ', number % 13)
        FROM numbers("number" = "16384")
    """
    sql "sync"

    // Eight scanners of at least 1024 rows split the 16384-row segment; without parallel scan one
    // scanner reads it whole. The result cache is off, so split scanners share runs, not entries.
    def split = "enable_parallel_scan=true, parallel_scan_min_rows_per_scanner=1024, " +
            "parallel_scan_max_scanners_count=8, enable_inverted_index_query_cache=false"
    def whole = "enable_parallel_scan=false, enable_inverted_index_query_cache=false"
    for (vars in [split, whole]) {
        order_qt_match_any """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE txt MATCH_ANY 'lazy'
        """
        order_qt_match_phrase """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE txt MATCH_PHRASE 'cached fox'
        """
        order_qt_match_regexp """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE txt MATCH_REGEXP 'cach.*'
        """
        order_qt_keyword """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE kw = 'k3'
        """
        order_qt_bkd """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE num > 90
        """
        order_qt_search_wildcard """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE search('txt:*ach*')
        """
        order_qt_compound """
            SELECT /*+ SET_VAR(${vars}) */ count(*), sum(id) FROM test_split_scanners_share_index_runs
            WHERE txt MATCH_ANY 'quick' OR num < 5
        """
    }
}
