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

// Keep write-path checks separate from the larger query/metric matrix.
suite("ann_empty_array_mow", "nonConcurrent") {
    setBeConfigTemporary([enable_ann_index_empty_array: true, ann_index_build_min_segment_rows: 0]) {
        try {
            sql "SET group_commit = off_mode"
            sql "SET ann_index_candidate_rows_threshold = 0"
            sql "SET ann_index_candidate_rows_percent_threshold = 0"
            sql "SET hnsw_ef_search = 256"
            for (def indexType : ['hnsw', 'ivf', 'ivf_on_disk']) {
                for (def lightDelete : [false, true]) {
                    logger.info("ANN MoW: index=${indexType}, lightDelete=${lightDelete}")
                    sql "DROP TABLE IF EXISTS ann_empty_array_mow"
                    def indexProperties = indexType == 'hnsw' ? '"max_degree"="16"' : '"nlist"="1"'
                    // Default [] makes the missing-key APPEND behavior explicit. A single tablet
                    // and disabled automatic compaction preserve versions until the manual check.
                    sql """
                        CREATE TABLE ann_empty_array_mow (
                            id INT NOT NULL,
                            vec ARRAY<FLOAT> NOT NULL DEFAULT '[]',
                            value INT,
                            INDEX ann_vec (vec) USING ANN PROPERTIES
                                ("index_type"="${indexType}", "metric_type"="l2_distance",
                                 "dim"="3", ${indexProperties})
                        ) UNIQUE KEY(id)
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES ("replication_num"="1",
                                    "enable_unique_key_merge_on_write"="true",
                                    "enable_mow_light_delete"="${lightDelete}",
                                    "disable_auto_compaction"="true")
                    """
                    // Enough valid vectors for IVF training; updated keys are in this same batch.
                    sql """INSERT INTO ann_empty_array_mow
                        SELECT number, array(CAST(number AS FLOAT), CAST(0 AS FLOAT), CAST(0 AS FLOAT)),
                               CAST(number AS INT)
                        FROM numbers("number"="96")"""
                    sql "INSERT INTO ann_empty_array_mow VALUES (100, [], 100), (101, [], 101)"

                    def checkRows = {
                        assertEquals(sql("SELECT id FROM ann_empty_array_mow WHERE l2_distance(vec, [0,0,0]) <= 3 ORDER BY id"),
                                     sql("SELECT id FROM ann_empty_array_mow WHERE l2_distance_approximate(vec, [0,0,0]) <= 3 ORDER BY id"))
                        qt_mow_rows "SELECT * FROM ann_empty_array_mow WHERE id < 4 OR id >= 100 ORDER BY id"
                        // Distances are distinct, so TopN IDs are stable. Empty rows are tested
                        // separately rather than adding an id sort key that could disable ANN TopN.
                        qt_mow_topn """SELECT id, l2_distance_approximate(vec, [0,0,0]) AS d
                            FROM ann_empty_array_mow ORDER BY d ASC NULLS LAST LIMIT 10"""
                        qt_mow_range """SELECT id FROM ann_empty_array_mow
                            WHERE l2_distance_approximate(vec, [0,0,0]) <= 3 ORDER BY id"""
                        qt_mow_missing """SELECT id FROM ann_empty_array_mow
                            WHERE l2_distance_approximate(vec, [0,0,0]) IS NULL ORDER BY id"""
                    }
                    checkRows()
                    sql "SET enable_unique_key_partial_update = true"
                    sql "SET partial_update_new_key_behavior = 'ERROR'"
                    // Both a valid vector and an empty vector must survive an omitted vec column.
                    sql "INSERT INTO ann_empty_array_mow(id, value) VALUES (0, 1000), (100, 1100)"
                    checkRows()
                    sql "INSERT INTO ann_empty_array_mow(id, vec) VALUES (0, []), (100, [0.5,0,0])"
                    checkRows()
                    for (def invalid : ['[1,2]', '[1,2,3,4]', '[NULL,2,3]', '[1,NULL,3]',
                                         '[1,2,NULL]', '[NULL,NULL,NULL]']) {
                        test {
                            // A valid update preceding the bad update must roll back too.
                            sql "INSERT INTO ann_empty_array_mow(id, vec) VALUES (1, [200,0,0]), (100, ${invalid})"
                            exception(invalid.contains('NULL') ? 'NULL elements' : 'Ann index expect array with')
                        }
                    }
                    checkRows()
                    test {
                        sql "INSERT INTO ann_empty_array_mow(id, value) VALUES (200, 200)"
                        exception "Can't append new rows in partial update"
                    }
                    sql "SET partial_update_new_key_behavior = 'APPEND'"
                    sql "INSERT INTO ann_empty_array_mow(id, value) VALUES (200, 200)"
                    checkRows()
                    sql "SET enable_unique_key_partial_update = false"

                    // Stream Load partial update uses a separate planning and sink entry point.
                    def partialLoad = { columns, rows, expectedError ->
                        streamLoad {
                            table "ann_empty_array_mow"
                            set 'partial_columns', 'true'
                            set 'partial_update_new_key_behavior', 'ERROR'
                            set 'columns', columns
                            set 'column_separator', '\t'
                            set 'max_filter_ratio', '0'
                            set 'strict_mode', 'true'
                            inputText rows
                            check { result, exception, startTime, endTime ->
                                if (exception != null) {
                                    throw exception
                                }
                                def json = parseJson(result)
                                if (expectedError == null) {
                                    assertEquals('Success', json.Status)
                                } else {
                                    assertTrue(json.Status != 'Success', result)
                                    assertTrue(json.Message.contains(expectedError), result)
                                }
                            }
                        }
                    }
                    partialLoad('id,value', '0\t2000\n100\t2100\n', null)
                    checkRows()
                    partialLoad('id,vec', '0\t[0,0,0]\n100\t[]\n', null)
                    checkRows()
                    partialLoad('id,vec', '0\t[1,null,3]\n', 'NULL elements')
                    partialLoad('id,vec', '0\t[1,2]\n', 'Ann index expect array with')
                    partialLoad('id,vec', '0\t\\N\n', 'filtered')
                    checkRows()

                    // Full-row MoW replacement must hide the old nearest-neighbor entry.
                    sql "INSERT INTO ann_empty_array_mow VALUES (0, [300,0,0], 300), (1, [], 301)"
                    checkRows()
                    trigger_and_wait_compaction('ann_empty_array_mow', 'full')
                    checkRows()

                    // Exercise both light-delete settings, empty rows, expressions and DELETE USING.
                    sql "DELETE FROM ann_empty_array_mow WHERE id = 2"
                    sql "DELETE FROM ann_empty_array_mow WHERE value = 301"
                    sql "DELETE FROM ann_empty_array_mow WHERE abs(value) = 2100"
                    sql """DELETE FROM ann_empty_array_mow t USING ann_empty_array_mow s
                        WHERE t.id = s.id AND s.id = 101"""
                    checkRows()
                    trigger_and_wait_compaction('ann_empty_array_mow', 'full')
                    checkRows()
                    sql "DELETE FROM ann_empty_array_mow WHERE id >= 0"
                    qt_all_deleted "SELECT * FROM ann_empty_array_mow ORDER BY id"
                    qt_deleted_topn """SELECT id FROM ann_empty_array_mow
                        ORDER BY l2_distance_approximate(vec, [0,0,0]) LIMIT 10"""
                    sql "INSERT INTO ann_empty_array_mow VALUES (2, [2,0,0], 2), (100, [], 100)"
                    checkRows()
                }
            }
        } finally {
            sql "SET enable_unique_key_partial_update = DEFAULT"
            sql "SET partial_update_new_key_behavior = DEFAULT"
            sql "SET group_commit = DEFAULT"
            sql "SET ann_index_candidate_rows_threshold = DEFAULT"
            sql "SET ann_index_candidate_rows_percent_threshold = DEFAULT"
            sql "SET hnsw_ef_search = DEFAULT"
        }
    }
}
