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

suite("ann_delete_group_commit") {
    sql "DROP TABLE IF EXISTS ann_delete_group_commit"
    // One bucket keeps these small correctness cases independent of tablet distribution.
    sql """
        CREATE TABLE ann_delete_group_commit (
            id INT NOT NULL,
            Vec ARRAY<FLOAT> NOT NULL,
            vec2 ARRAY<FLOAT> NOT NULL,
            value INT,
            INDEX ann1 (Vec) USING ANN PROPERTIES
                ("index_type"="hnsw", "metric_type"="l2_distance", "dim"="3"),
            INDEX ann2 (vec2) USING ANN PROPERTIES
                ("index_type"="hnsw", "metric_type"="l2_distance", "dim"="2")
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num"="1",
            "enable_unique_key_merge_on_write"="true",
            "enable_mow_light_delete"="false",
            "group_commit_interval_ms"="200"
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
        assertEquals([[4]], sql("SELECT id FROM ann_delete_group_commit ORDER BY id"))
        assertEquals([[4]], sql("""SELECT id FROM ann_delete_group_commit
            ORDER BY l2_distance_approximate(Vec, [1,2,3]) LIMIT 1"""))

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
                        assertTrue(json.GroupCommit)
                    } else {
                        assertTrue(json.Status != 'Success', result)
                        assertTrue(json.Message.contains(expectedError), result)
                    }
                }
            }
        }
        for (def mode : ['async_mode', 'sync_mode', 'off_mode']) {
            // Short, empty, overlong, and second-index mismatches must reach the caller.
            loadRows(mode, '100\t[1,2]\t[1,2]\t1\n', false)
            loadRows(mode, '101\t[]\t[1,2]\t1\n', false)
            loadRows(mode, '102\t[1,2,3,4]\t[1,2]\t1\n', false)
            loadRows(mode, '103\t[1,2,3]\t[1]\t1\n', false)
            // The good row in the same batch must not let a malformed row enter the WAL.
            loadRows(mode, '104\t[1,2,3]\t[1,2]\t1\n105\t[1,2]\t[1,2]\t1\n', false)
            loadRows(mode, '110\t[null,2,3]\t[1,2]\t1\n', false,
                     'ANN vector cannot contain NULL elements')
            loadRows(mode, '111\t[1,2,3]\t[1,null]\t1\n', false,
                     'ANN vector cannot contain NULL elements')
            loadRows(mode, '112\t[1,2,3]\t[1,2]\t1\n113\t[1,2,null]\t[1,2]\t1\n', false,
                     'ANN vector cannot contain NULL elements')
        }
        // INSERT uses the same validation before its group commit RPC reports success to FE.
        sql "SET group_commit = async_mode"
        test {
            sql "INSERT INTO ann_delete_group_commit VALUES (106, [1,2], [1,2], 1)"
            exception "Ann index expect array with"
        }
        test {
            sql "INSERT INTO ann_delete_group_commit VALUES (114, [1,NULL,3], [1,2], 1)"
            exception "ANN vector cannot contain NULL elements"
        }
        sql "SET group_commit = off_mode"
        test {
            sql "INSERT INTO ann_delete_group_commit VALUES (115, [1,2,3], [NULL,2], 1)"
            exception "ANN vector cannot contain NULL elements"
        }
        loadRows('async_mode', '5\t[1,2,3]\t[1,2]\t50\n', true)
        awaitUntil(30, 1) {
            sql("SELECT count(*) FROM ann_delete_group_commit WHERE id = 5")[0][0] == 1
        }
        assertEquals([[4], [5]], sql("SELECT id FROM ann_delete_group_commit ORDER BY id"))
    } finally {
        sql "SET group_commit = off_mode"
    }

    if (!isCloudMode()) {
        // CREATE INDEX adds metadata without building old segments. This reproduces legacy
        // NULL coordinates without a debug hook that allows invalid new ANN writes.
        sql "DROP TABLE IF EXISTS ann_delete_legacy_null"
        sql """
            CREATE TABLE ann_delete_legacy_null (
                id INT NOT NULL,
                vec ARRAY<FLOAT> NOT NULL
            ) UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num"="1",
                "enable_unique_key_merge_on_write"="true",
                "enable_mow_light_delete"="false",
                "disable_auto_compaction"="true"
            )
        """
        // Ordinary arrays must continue to permit NULL elements before an ANN index is added.
        sql """INSERT INTO ann_delete_legacy_null VALUES
            (1, [NULL,2,3]), (2, [1,NULL,3]), (3, [1,2,NULL]), (4, [4,5,6])"""
        sql """CREATE INDEX ann_legacy ON ann_delete_legacy_null(vec) USING ANN PROPERTIES
            ("index_type"="hnsw", "metric_type"="l2_distance", "dim"="3")"""
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE TableName='ann_delete_legacy_null'
                ORDER BY CreateTime DESC LIMIT 1"""
            time 60
        }
        // New malformed vectors fail, but the legacy rows remain deletable through all rewrites.
        test {
            sql "INSERT INTO ann_delete_legacy_null VALUES (5, [NULL,2,3])"
            exception "ANN vector cannot contain NULL elements"
        }
        sql "DELETE FROM ann_delete_legacy_null WHERE id=1"
        sql "DELETE FROM ann_delete_legacy_null WHERE abs(id)=2"
        sql """DELETE FROM ann_delete_legacy_null t USING ann_delete_legacy_null s
            WHERE t.id=s.id AND s.id=3"""
        assertEquals([[4]], sql("SELECT id FROM ann_delete_legacy_null ORDER BY id"))
        assertEquals([[4]], sql("""SELECT id FROM ann_delete_legacy_null
            ORDER BY l2_distance_approximate(vec, [4,5,6]) LIMIT 1"""))
    }
}
