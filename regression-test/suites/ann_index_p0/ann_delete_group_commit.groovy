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

suite("ann_delete_group_commit", "nonConcurrent") {
    setBeConfigTemporary([enable_ann_index_empty_array: true, ann_index_build_min_segment_rows: 0]) {
        for (def indexType : ['hnsw', 'ivf', 'ivf_on_disk']) {
            logger.info("ANN load validation: index=${indexType}")
            def indexProperties = indexType == 'hnsw' ? '"max_degree"="16"' : '"nlist"="1"'
            sql "DROP TABLE IF EXISTS ann_delete_group_commit"
            // One bucket keeps these small correctness cases independent of tablet distribution.
            sql """
                CREATE TABLE ann_delete_group_commit (
                    id INT NOT NULL,
                    Vec ARRAY<FLOAT> NOT NULL,
                    vec2 ARRAY<FLOAT> NOT NULL,
                    value INT,
                    INDEX ann1 (Vec) USING ANN PROPERTIES
                        ("index_type"="${indexType}", "metric_type"="l2_distance", "dim"="3", ${indexProperties}),
                    INDEX ann2 (vec2) USING ANN PROPERTIES
                        ("index_type"="${indexType}", "metric_type"="l2_distance", "dim"="2", ${indexProperties})
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num"="1",
                    "enable_unique_key_merge_on_write"="true",
                    "enable_mow_light_delete"="false",
                    "group_commit_interval_ms"="200",
                    "disable_auto_compaction"="true"
                )
            """
            try {
                sql "SET group_commit = off_mode"
                sql """INSERT INTO ann_delete_group_commit VALUES
                    (1, [1,2,3], [1,2], 10), (2, [2,3,4], [2,3], 20),
                    (3, [3,4,5], [3,4], 30), (4, [4,5,6], [4,5], 40)"""
                sql "DELETE FROM ann_delete_group_commit WHERE id = 1"
                sql "DELETE FROM ann_delete_group_commit WHERE abs(value) = 20"
                sql """DELETE FROM ann_delete_group_commit t USING ann_delete_group_commit s
                       WHERE t.id = s.id AND s.id = 3"""
                assertEquals(sql("SELECT id FROM ann_delete_group_commit ORDER BY l2_distance(Vec, [1,2,3]) LIMIT 1"),
                             sql("SELECT id FROM ann_delete_group_commit ORDER BY l2_distance_approximate(Vec, [1,2,3]) LIMIT 1"))

                qt_after_delete "SELECT * FROM ann_delete_group_commit ORDER BY id"

                def loadRows = { mode, rows, expectSuccess, expectedError = 'Ann index expect array with' ->
                    streamLoad {
                        table "ann_delete_group_commit"
                        set 'group_commit', mode
                        set 'column_separator', '\t'
                        set 'columns', 'id,Vec,vec2,value'
                        // Invalid ANN vectors must fail even when ordinary row filtering is permitted.
                        set 'max_filter_ratio', '1'
                        inputText rows
                        check { exception, result, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            def json = parseJson(result)
                            if (expectSuccess) {
                                assertEquals('Success', json.Status)
                                if (mode != 'off_mode') {
                                    assertTrue(json.GroupCommit)
                                }
                            } else {
                                assertTrue(json.Status != 'Success', result)
                                assertTrue(json.Message.contains(expectedError), result)
                            }
                        }
                    }
                }
                setBeConfigTemporary([enable_ann_index_empty_array: false]) {
                    for (def mode : ['async_mode', 'sync_mode', 'off_mode']) {
                        loadRows(mode, '120\t[]\t[1,2]\t1\n', false,
                                 'enable_ann_index_empty_array=true')
                        loadRows(mode, '122\t[1,null,3]\t[1,2]\t1\n', false, 'NULL elements')
                        sql "SET group_commit = ${mode}"
                        test {
                            sql "INSERT INTO ann_delete_group_commit VALUES (121, [], [1,2], 1)"
                            exception "enable_ann_index_empty_array=true"
                        }
                    }
                    sql "SET group_commit = off_mode"
                }
                for (def mode : ['async_mode', 'sync_mode', 'off_mode']) {
                    // Nonempty short, overlong, and second-index mismatches must reach the caller.
                    loadRows(mode, '100\t[1,2]\t[1,2]\t1\n', false)
                    // Empty arrays are accepted in all modes and can subsequently be deleted.
                    loadRows(mode, '101\t[]\t[1,2]\t1\n', true)
                    awaitUntil(30, 1) {
                        sql("SELECT count(*) FROM ann_delete_group_commit WHERE id = 101")[0][0] == 1
                    }
                    sql "DELETE FROM ann_delete_group_commit WHERE id = 101"
                    loadRows(mode, '102\t[1,2,3,4]\t[1,2]\t1\n', false)
                    loadRows(mode, '103\t[1,2,3]\t[1]\t1\n', false)
                    // The good row in the same batch must not let a malformed row enter the WAL.
                    loadRows(mode, '104\t[1,2,3]\t[1,2]\t1\n105\t[1,2]\t[1,2]\t1\n', false)
                    // Enabling empty arrays never permits NULL coordinates, including a second
                    // indexed column or a bad row after an empty/valid row in the same request.
                    for (def invalid : ['[null,2,3]', '[1,null,3]', '[1,2,null]', '[null,null,null]']) {
                        loadRows(mode, "110\t${invalid}\t[1,2]\t1\n", false, 'NULL elements')
                    }
                    // A malformed first row must also abort the following valid row.
                    loadRows(mode, '116\t[null,2,3]\t[1,2]\t1\n117\t[1,2,3]\t[1,2]\t1\n',
                             false, 'NULL elements')
                    loadRows(mode, '111\t[1,2,3]\t[null,2]\t1\n', false, 'NULL elements')
                    loadRows(mode, '112\t[]\t[1,2]\t1\n113\t[1,2,3]\t[1,2]\t1\n114\t[1,2,null]\t[1,2]\t1\n',
                             false, 'NULL elements')
                    sql "SET group_commit = ${mode}"
                    test {
                        sql "INSERT INTO ann_delete_group_commit VALUES (115, [1,NULL,3], [1,2], 1)"
                        exception "NULL elements"
                    }
                    for (def invalid : ['[1,2]', '[1,2,3,4]', '[NULL,2,3]', '[1,2,NULL]', '[NULL,NULL,NULL]']) {
                        test {
                            sql "INSERT INTO ann_delete_group_commit VALUES (118, ${invalid}, [1,2], 1)"
                            exception(invalid.contains('NULL') ? 'NULL elements' : 'Ann index expect array with')
                        }
                        test {
                            sql "INSERT INTO ann_delete_group_commit SELECT 119, ${invalid}, [1,2], 1"
                            exception(invalid.contains('NULL') ? 'NULL elements' : 'Ann index expect array with')
                        }
                    }
                    test {
                        sql "INSERT INTO ann_delete_group_commit VALUES (142, NULL, [1,2], 142)"
                        exception 'null'
                    }
                    test {
                        sql "INSERT INTO ann_delete_group_commit SELECT 143, CAST(NULL AS ARRAY<FLOAT>), [1,2], 143"
                        exception 'null'
                    }
                    sql "INSERT INTO ann_delete_group_commit VALUES (140, [], [1,2], 140)"
                    sql "INSERT INTO ann_delete_group_commit SELECT 141, CAST([] AS ARRAY<FLOAT>), [1,2], 141"
                    awaitUntil(30, 1) {
                        sql("SELECT count(*) FROM ann_delete_group_commit WHERE id IN (140, 141)")[0][0] == 2
                    }
                    qt_insert_empty "SELECT * FROM ann_delete_group_commit WHERE id IN (140, 141) ORDER BY id"
                    sql "SET group_commit = off_mode"
                    sql "DELETE FROM ann_delete_group_commit WHERE id IN (140, 141)"
                    // A SQL NULL is a NOT NULL constraint violation, not a NULL coordinate.
                    streamLoad {
                        table "ann_delete_group_commit"
                        set 'group_commit', mode
                        set 'column_separator', '\t'
                        set 'columns', 'id,Vec,vec2,value'
                        set 'strict_mode', 'true'
                        set 'max_filter_ratio', '0'
                        inputText '130\t\\N\t[1,2]\t1\n'
                        check { exception, result, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            def json = parseJson(result)
                            assertTrue(json.Status != 'Success', result)
                            assertTrue(json.Message.toLowerCase().contains('filtered'), result)
                        }
                    }
                    // Unlike invalid ANN coordinates, an ordinary NOT NULL violation may be filtered.
                    streamLoad {
                        table "ann_delete_group_commit"
                        set 'group_commit', mode
                        set 'column_separator', '\t'
                        set 'columns', 'id,Vec,vec2,value'
                        set 'strict_mode', 'true'
                        set 'max_filter_ratio', '1'
                        inputText '131\t\\N\t[1,2]\t1\n132\t[1,2,3]\t[1,2]\t1\n'
                        check { exception, result, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            def json = parseJson(result)
                            assertEquals('Success', json.Status)
                            assertEquals(1, json.NumberFilteredRows)
                            assertEquals(1, json.NumberLoadedRows)
                        }
                    }
                    awaitUntil(30, 1) {
                        sql("SELECT count(*) FROM ann_delete_group_commit WHERE id = 132")[0][0] == 1
                    }
                    sql "SET group_commit = off_mode"
                    // Check after successful async publication, rather than immediately after rejection.
                    qt_after_rejected_batches "SELECT * FROM ann_delete_group_commit WHERE id >= 100 ORDER BY id"
                    sql "DELETE FROM ann_delete_group_commit WHERE id = 132"
                }
                // INSERT uses the same validation before its group commit RPC reports success to FE.
                sql "SET group_commit = async_mode"
                test {
                    sql "INSERT INTO ann_delete_group_commit VALUES (106, [1,2], [1,2], 1)"
                    exception "Ann index expect array with"
                }
                sql "SET group_commit = off_mode"
                loadRows('async_mode', '5\t[1,2,3]\t[1,2]\t50\n', true)
                awaitUntil(30, 1) {
                    sql("SELECT count(*) FROM ann_delete_group_commit WHERE id = 5")[0][0] == 1
                }
                qt_final_rows "SELECT * FROM ann_delete_group_commit ORDER BY id"
                setBeConfigTemporary([enable_ann_index_empty_array: false]) {
                    // Querying existing indexes does not check the writer flag.
                    sql "SELECT id FROM ann_delete_group_commit ORDER BY l2_distance_approximate(Vec, [1,2,3]) LIMIT 1"
                }
            } finally {
                sql "SET group_commit = off_mode"
            }
        }
    }
}
