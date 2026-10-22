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

suite("test_bm25_visible_statistics", "p0") {
    // Statistics must use the captured collection even when a scanner reads one row.
    sql "SET enable_parallel_scan = true"
    sql "SET parallel_scan_min_rows_per_scanner = 1"
    sql "SET parallel_scan_max_scanners_count = 16"
    for (String format : ["V2", "V3"]) {
        for (String phrase : ["true", "false"]) {
            for (String history : ["dup_delete", "mow_light_delete", "mow_bitmap_delete",
                                   "delete_sign", "overwrite"]) {
                String prefix = "bm25_visible_${format.toLowerCase()}_${phrase}_${history}"
                String clean = "${prefix}_clean"
                String dirty = "${prefix}_history"
                String keyType = history == "dup_delete" ? "DUPLICATE" : "UNIQUE"
                String mow = history == "dup_delete" ? "" : """
                    "enable_unique_key_merge_on_write"="true",
                    "enable_mow_light_delete"="${history == 'mow_light_delete'}",
                """
                for (String table : [clean, dirty]) {
                    sql "DROP TABLE IF EXISTS ${table}"
                    sql """CREATE TABLE ${table} (
                        id INT, body TEXT,
                        INDEX body_idx(body) USING INVERTED
                            PROPERTIES("parser"="english", "support_phrase"="${phrase}")
                    ) ENGINE=OLAP ${keyType} KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                               ${mow} "inverted_index_storage_format"="${format}")"""
                }
                sql """INSERT INTO ${clean} VALUES
                    (1, 'alpha'), (2, CONCAT('alpha alpha ', REPEAT('omega ', 14)))"""
                for (int batch = 0; batch < 2; ++batch) {
                    String liveBody = batch == 0 ? "'alpha'" :
                        "CONCAT('alpha alpha ', REPEAT('omega ', 14))"
                    String oldBody = "CONCAT(REPEAT('alpha ', 240), REPEAT('omega ', 16))"
                    if (history == "delete_sign") {
                        sql """INSERT INTO ${dirty}(id, body, __DORIS_DELETE_SIGN__) VALUES
                            (${batch + 1}, ${liveBody}, 0), (${batch + 100}, ${oldBody}, 1)"""
                    } else {
                        sql """INSERT INTO ${dirty} VALUES
                            (${batch + 1}, ${liveBody}), (${batch + 100}, ${oldBody})"""
                    }
                }
                if (history == "overwrite") {
                    for (String table : [clean, dirty]) {
                        sql "INSERT INTO ${table} VALUES (100, 'omega'), (101, 'omega')"
                    }
                } else if (history != "delete_sign") {
                    sql "DELETE FROM ${dirty} WHERE id IN (100, 101)"
                }
                sql "SYNC"

                def verify = { String phase ->
                    assertEquals(sql("SELECT * FROM ${clean} ORDER BY id"),
                                 sql("SELECT * FROM ${dirty} ORDER BY id"), phase)
                    for (String predicate : ["SEARCH('body:alpha')", "body MATCH_ANY 'alpha'"]) {
                        // Equal-score MATCH_ANY hits without frequencies have unspecified TopK ties.
                        def limits = phrase == "false" && predicate == "body MATCH_ANY 'alpha'" ?
                            ["LIMIT 100"] : ["LIMIT 100", "LIMIT 1", "LIMIT 1 OFFSET 1"]
                        for (String limit : limits) {
                            def scored = { String table ->
                                sql """SELECT id, ROUND(s, 6) FROM (
                                    SELECT id, score() AS s FROM ${table} WHERE ${predicate}
                                    ORDER BY s DESC ${limit}) ranked ORDER BY id"""
                            }
                            assertEquals(scored(clean), scored(dirty),
                                "${phase}: ${format}, phrase=${phrase}, ${history}, ${predicate}, ${limit}")
                        }
                    }
                    def threshold = { String table ->
                        sql """SELECT id, ROUND(s, 6) FROM (
                            SELECT id, score() AS s FROM ${table}
                            WHERE SEARCH('body:alpha') AND score() >= 0.25
                            ORDER BY s DESC LIMIT 100) ranked ORDER BY id"""
                    }
                    assertEquals(threshold(clean), threshold(dirty), "${phase}: score threshold")
                }
                // No compaction is triggered anywhere in this test.
                verify("initial visible snapshot")
                verify("repeat query with immutable lengths cached")
                for (String table : [clean, dirty]) {
                    sql "DELETE FROM ${table} WHERE id = 2"
                }
                sql "SYNC"
                verify("delete changes N, df and average length immediately")
                for (String table : [clean, dirty]) {
                    // Length 41 cannot be recovered exactly from a quantized byte norm.
                    sql """INSERT INTO ${table} VALUES
                        (2, CONCAT('alpha ', REPEAT('omega ', 40))), (3, NULL), (4, '')"""
                }
                sql "SYNC"
                verify("later insert survives earlier delete; NULL and empty documents")
                for (String table : [clean, dirty]) {
                    sql "DELETE FROM ${table} WHERE id IN (1, 2, 3, 4, 100, 101)"
                }
                sql "SYNC"
                verify("all documents deleted")
                for (String table : [clean, dirty]) {
                    sql "INSERT INTO ${table} VALUES (1, 'alpha')"
                }
                sql "SYNC"
                verify("new live version after empty snapshot")
            }
        }
    }
}
