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

suite("test_inverted_index_v1_deprecated", "p0, nonConcurrent") {
    // AC1: explicit v1 (lowercase) must be rejected
    test {
        sql """
            CREATE TABLE test_v1_rejected (
              k INT,
              v STRING,
              INDEX idx_v (v) USING INVERTED
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "v1"
            )
        """
        exception "Inverted index V1 is deprecated and no longer allowed for new index creation."
    }

    // AC1 (case-insensitive): uppercase V1 must also be rejected
    test {
        sql """
            CREATE TABLE test_v1_rejected_upper (
              k INT,
              v STRING,
              INDEX idx_v (v) USING INVERTED
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "V1"
            )
        """
        exception "Inverted index V1 is deprecated and no longer allowed for new index creation."
    }

    // AC2: no format specified -> default succeeds and format is not V1
    // AC3: ALTER TABLE ADD INDEX on default table succeeds
    try {
        sql "DROP TABLE IF EXISTS test_v1_deprecated_default"
        sql """
            CREATE TABLE test_v1_deprecated_default (
              k INT,
              v STRING
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            )
        """
        def showCreate = sql "SHOW CREATE TABLE test_v1_deprecated_default"
        assertTrue(showCreate.size() > 0)
        assertFalse(showCreate[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))

        sql "ALTER TABLE test_v1_deprecated_default ADD INDEX idx_v (v) USING INVERTED"
        def showCreateAfter = sql "SHOW CREATE TABLE test_v1_deprecated_default"
        assertTrue(showCreateAfter[0][1].contains("idx_v"))
        assertFalse(showCreateAfter[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))
    } finally {
        sql "DROP TABLE IF EXISTS test_v1_deprecated_default"
    }

    // AC4: existing V1 table metadata load and read/write must not be affected.
    // Uses a deterministic fixture: temporarily unlock V1 creation via admin config,
    // seed the legacy table, verify reads/writes/deletes, then clean up.
    try {
        sql "DROP TABLE IF EXISTS test_v1_legacy_compat"
        sql "ADMIN SET FRONTEND CONFIG ('allow_inverted_index_v1_creation' = 'true')"
        sql """
            CREATE TABLE test_v1_legacy_compat (
              k INT,
              v STRING,
              w STRING,
              INDEX idx_v (v) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "v1"
            )
        """
        sql "ADMIN SET FRONTEND CONFIG ('allow_inverted_index_v1_creation' = 'false')"

        def showCreate = sql "SHOW CREATE TABLE test_v1_legacy_compat"
        assertTrue(showCreate[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))

        // Verify basic DML works on a V1 table (tests metadata compat, not BE index reads)
        sql "INSERT INTO test_v1_legacy_compat VALUES (1, 'hello world', 'a'), (2, 'foo bar', 'b')"
        sql "sync"
        def cnt = sql "SELECT COUNT(*) FROM test_v1_legacy_compat WHERE k = 1"
        assertTrue(cnt[0][0] == 1)

        sql "INSERT INTO test_v1_legacy_compat VALUES (3, 'compat write test', 'c')"
        sql "sync"
        def cntAfter = sql "SELECT COUNT(*) FROM test_v1_legacy_compat WHERE k = 3"
        assertTrue(cntAfter[0][0] == 1)

        sql "DELETE FROM test_v1_legacy_compat WHERE k = 3"
        sql "sync"
        def cntDel = sql "SELECT COUNT(*) FROM test_v1_legacy_compat WHERE k = 3"
        assertTrue(cntDel[0][0] == 0)

        // Verify BE reads V1 inverted index: disable full-scan fallback so success
        // proves the index file is actually being consulted (not a brute-force scan).
        def matchCnt = sql """
            SELECT /*+ SET_VAR(enable_match_without_inverted_index = 0) */
            COUNT(*) FROM test_v1_legacy_compat WHERE v MATCH 'hello'
        """
        assertEquals(1, matchCnt[0][0])

        def phraseCnt = sql """
            SELECT /*+ SET_VAR(enable_match_without_inverted_index = 0) */
            COUNT(*) FROM test_v1_legacy_compat WHERE v MATCH_PHRASE 'hello world'
        """
        assertEquals(1, phraseCnt[0][0])

        def noMatchCnt = sql """
            SELECT /*+ SET_VAR(enable_match_without_inverted_index = 0) */
            COUNT(*) FROM test_v1_legacy_compat WHERE v MATCH 'notexist'
        """
        assertEquals(0, noMatchCnt[0][0])

        // ADD INDEX on a V1 table must be rejected when V1 creation is disabled
        try {
            sql "ALTER TABLE test_v1_legacy_compat ADD INDEX idx_w (w) USING INVERTED"
            fail("Expected exception for ADD INDEX on V1 table with allow_inverted_index_v1_creation=false")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Inverted index V1 is deprecated and no longer allowed for new index creation."))
        }
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('allow_inverted_index_v1_creation' = 'false')"
        sql "DROP TABLE IF EXISTS test_v1_legacy_compat"
    }
}
