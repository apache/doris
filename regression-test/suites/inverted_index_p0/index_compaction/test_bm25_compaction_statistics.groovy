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

// Compare compacted indexes with independently rebuilt indexes. Exact scores are
// data-dependent; equivalence avoids pinning the BM25 formula in a golden file.
suite("test_bm25_compaction_statistics", "p0,nonConcurrent") {
    setBeConfigTemporary(["inverted_index_compaction_enable": "true"]) {
        for (String format : ["V2", "V3"]) {
            for (String phrase : ["true", "false"]) {
                for (String history : ["dup_delete", "mow_light_delete", "mow_bitmap_delete",
                                       "delete_sign", "overwrite"]) {
                    // Names encode the deliberately varied storage/visibility cases.
                    String prefix = "bm25_compaction_${format.toLowerCase()}_${phrase}_${history}"
                    String clean = "${prefix}_clean"
                    String compacted = "${prefix}_history"
                    String keyType = history == "dup_delete" ? "DUPLICATE" : "UNIQUE"
                    String mowProperties = history == "dup_delete" ? "" : """
                        "enable_unique_key_merge_on_write"="true",
                        "enable_mow_light_delete"="${history == 'mow_light_delete'}",
                    """
                    for (String table : [clean, compacted]) {
                        sql "DROP TABLE IF EXISTS ${table}"
                        sql """CREATE TABLE ${table} (
                            id INT, body TEXT,
                            INDEX body_idx(body) USING INVERTED
                                PROPERTIES("parser"="english", "support_phrase"="${phrase}")
                        ) ENGINE=OLAP ${keyType} KEY(id)
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                                   ${mowProperties}
                                   "inverted_index_storage_format"="${format}")"""
                    }
                    String visible = "(1, 'alpha'), (2, CONCAT('alpha alpha ', REPEAT('omega ', 14)))"
                    sql "INSERT INTO ${clean} VALUES ${visible}"
                    // Both source rowsets retain a document and discard a long document.
                    // This exercises posting remapping, not just dropping a whole rowset.
                    for (int batch = 0; batch < 2; ++batch) {
                        String body = batch == 0 ? "'alpha'" :
                            "CONCAT('alpha alpha ', REPEAT('omega ', 14))"
                        String deletedBody = "CONCAT(REPEAT('alpha ', 240), REPEAT('omega ', 16))"
                        if (history == "delete_sign") {
                            sql """INSERT INTO ${compacted}(id, body, __DORIS_DELETE_SIGN__)
                                VALUES (${batch + 1}, ${body}, 0), (${batch + 100}, ${deletedBody}, 1)"""
                        } else {
                            sql """INSERT INTO ${compacted} VALUES
                                (${batch + 1}, ${body}), (${batch + 100}, ${deletedBody})"""
                        }
                    }
                    if (history == "overwrite") {
                        sql "INSERT INTO ${compacted} VALUES (100, 'omega'), (101, 'omega')"
                        sql "INSERT INTO ${clean} VALUES (100, 'omega'), (101, 'omega')"
                    } else if (history != "delete_sign") {
                        sql "DELETE FROM ${compacted} WHERE id >= 100"
                    }
                    sql "SYNC"
                    def visibleRows = sql "SELECT * FROM ${clean} ORDER BY id"
                    assertEquals(visibleRows, sql("SELECT * FROM ${compacted} ORDER BY id"))
                    trigger_and_wait_compaction(compacted, "full")
                    assertEquals(visibleRows, sql("SELECT * FROM ${compacted} ORDER BY id"))
                    for (String predicate : ["SEARCH('body:alpha')", "body MATCH_ANY 'alpha'"]) {
                        // MATCH_ANY can return equal scores without stored frequencies.
                        // Compare all hits in that case; TopK among ties is unspecified.
                        def limits = phrase == "false" && predicate == "body MATCH_ANY 'alpha'" ?
                            ["LIMIT 10"] : ["LIMIT 10", "LIMIT 1", "LIMIT 1 OFFSET 1"]
                        for (String limit : limits) {
                            def scored = { String table ->
                                sql """SELECT id, ROUND(s, 6) FROM (
                                    SELECT id, score() AS s FROM ${table} WHERE ${predicate}
                                    ORDER BY s DESC ${limit}) ranked ORDER BY id"""
                            }
                            assertEquals(scored(clean), scored(compacted),
                                "${format}, support_phrase=${phrase}, ${history}, ${predicate}, ${limit}")
                        }
                    }
                }
            }
        }
    }
}
