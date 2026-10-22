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

// A parallel scan splits one tablet into several scanners. BM25 statistics must
// still cover the whole tablet, so rows with identical text get identical scores
// no matter which scanner reads them.
suite("test_bm25_score_parallel_scan_statistics", "p0") {
    def tableName = "test_bm25_score_parallel_scan_statistics"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id BIGINT,
            content TEXT,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"
        );
    """

    // Each load is one rowset with a different share of matching rows, so statistics
    // collected per scanner would differ between scanners.
    for (int batch = 0; batch < 8; batch++) {
        sql """
            INSERT INTO ${tableName}
            SELECT ${batch} * 2000 + number,
                   IF(number % ${batch * 3 + 2} = 0,
                      'we should try these jobs today',
                      'call the candidate about salary')
            FROM numbers("number" = "2000");
        """
    }
    sql "sync"

    def scoreQuery = { String vars ->
        sql """
            SELECT /*+ SET_VAR(${vars}) */ score() AS s
            FROM ${tableName}
            WHERE SEARCH('content:jobs OR content:try')
            ORDER BY score() DESC
            LIMIT 100000
        """
    }

    def parallel = scoreQuery("enable_parallel_scan=true, parallel_scan_min_rows_per_scanner=1024, parallel_scan_max_scanners_count=16")
    def serial = scoreQuery("enable_parallel_scan=false")

    def parallelScores = parallel.collect { it[0] }.unique()
    def serialScores = serial.collect { it[0] }.unique()

    assertTrue(parallel.size() > 0)
    assertEquals(serial.size(), parallel.size())
    assertEquals(1, serialScores.size())
    assertEquals(1, parallelScores.size(),
            "identical rows got different BM25 scores across scanners: ${parallelScores}")
    assertEquals(serialScores[0], parallelScores[0])

    sql "DROP TABLE IF EXISTS ${tableName}"
}
