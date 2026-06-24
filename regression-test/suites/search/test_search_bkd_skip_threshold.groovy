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

suite("test_search_bkd_skip_threshold", "p0") {
    sql "set enable_inverted_index_query_cache = false"
    sql "set inverted_index_skip_threshold = 50"

    sql "DROP TABLE IF EXISTS search_bkd_skip_threshold"

    sql """
        CREATE TABLE search_bkd_skip_threshold (
            id INT,
            score INT,
            INDEX idx_score (score) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql "INSERT INTO search_bkd_skip_threshold VALUES (1, 0)"

    def rows = sql """
        SELECT id, score
        FROM search_bkd_skip_threshold
        WHERE search('score:0')
        ORDER BY id
    """

    assertEquals([[1, 0]], rows)
}
