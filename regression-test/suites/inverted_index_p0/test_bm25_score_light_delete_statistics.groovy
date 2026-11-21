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

// Light deletes use versioned predicates instead of marking the deleted rows in the MoW bitmap.
// Compare with an independently loaded MoW table so both sides use the existing norms convention,
// including lengths above the exact range. No compaction is needed to make the scores agree.
suite("test_bm25_score_light_delete_statistics", "p0") {
    for (String format : ["V2", "V3", "SNII"]) {
        for (String phrase : format == "SNII" ? ["true"] : ["true", "false"]) {
            String prefix = "bm25_light_delete_${format.toLowerCase()}_${phrase}"
            String history = "${prefix}_history"
            String clean = "${prefix}_clean"
            for (String table : [history, clean]) {
                sql "DROP TABLE IF EXISTS ${table}"
                sql """CREATE TABLE ${table} (
                    id INT, tag VARCHAR(16), body TEXT NULL,
                    INDEX body_idx(body) USING INVERTED
                        PROPERTIES("parser"="english", "support_phrase"="${phrase}")
                ) ENGINE=OLAP UNIQUE KEY(id, tag)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "enable_mow_light_delete"="true", "disable_auto_compaction"="true",
                    "inverted_index_storage_format"="${format}")"""
            }
            String kept = "(1, 'keep', 'alpha'), (2, 'keep', CONCAT('alpha alpha ', REPEAT('omega ', 39))), (3, NULL, NULL)"
            sql "INSERT INTO ${clean} VALUES ${kept}"
            sql """INSERT INTO ${history} VALUES ${kept},
                (100, 'drop', REPEAT('alpha ', 256)), (101, 'archive', REPEAT('omega ', 257)),
                (102, 'keep', REPEAT('alpha ', 255))"""
            sql "INSERT INTO ${clean} VALUES (102, 'keep', REPEAT('alpha ', 255))"
            // Three additional rowsets exceed the forced-split threshold without redefining
            // the corpus through query predicates.
            for (int batch = 0; batch < 3; ++batch) {
                for (String table : [history, clean]) {
                    sql """INSERT INTO ${table} SELECT 1000 + ${batch} * 1100 + number, 'keep',
                        IF(number % 7 = 0, 'alpha beta', 'omega') FROM numbers('number'='1100')"""
                }
            }
            sql "DELETE FROM ${history} WHERE id >= 100 AND tag IN ('drop', 'archive')"
            sql "SYNC"

            def compare = { String phase ->
                assertEquals(sql("SELECT * FROM ${clean} ORDER BY id, tag"),
                             sql("SELECT * FROM ${history} ORDER BY id, tag"))
                for (String predicate : ["SEARCH('body:alpha')", "body MATCH_ANY 'alpha'"]) {
                    for (String vars : ["enable_parallel_scan=false",
                            "enable_parallel_scan=true, parallel_scan_min_rows_per_scanner=1024, parallel_scan_max_scanners_count=8"]) {
                        def scores = { String table ->
                            sql """SELECT /*+ SET_VAR(${vars}) */ id, tag, s FROM (
                                SELECT id, tag, score() AS s FROM ${table} WHERE ${predicate}
                                ORDER BY s DESC LIMIT 1000000) ranked ORDER BY id, tag"""
                        }
                        assertEquals(scores(clean), scores(history),
                                "${format}/${phrase}: ${phase}, ${predicate}, ${vars}")
                    }
                }
            }
            compare("initial compound light delete")
            // A newer row matching an older DELETE must remain in the corpus.
            for (String table : [history, clean]) {
                sql "INSERT INTO ${table} VALUES (100, 'drop', 'alpha beta')"
            }
            sql "SYNC"
            compare("insert after delete")
            // Overlap predicate deletes with a bitmap overwrite, then re-evaluate a new snapshot.
            for (String table : [history, clean]) {
                sql "INSERT INTO ${table} VALUES (2, 'keep', REPEAT('alpha ', 43))"
                sql "DELETE FROM ${table} WHERE tag IS NULL"
                sql "DELETE FROM ${table} WHERE id = 2"
            }
            sql "SYNC"
            compare("overwrite and NULL predicate")
            for (String table : [history, clean]) {
                sql "DELETE FROM ${table} WHERE id >= 0"
            }
            sql "SYNC"
            compare("all rows hidden")
            for (String table : [history, clean]) {
                sql "INSERT INTO ${table} VALUES (7, 'new', 'alpha alpha')"
            }
            sql "SYNC"
            compare("insert after empty")
        }
    }
}
