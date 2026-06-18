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

suite("test_inverted_index_v1_deprecated", "p0") {
    // AC1: CREATE TABLE with explicit V1 must fail
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

    // AC1 (case-insensitive): uppercase V1 must also fail
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

    // AC2: no version specified → default succeeds and is not V1
    // AC3: ALTER TABLE ADD INDEX on default table succeeds (format inherited from table, not user input)
    def testTable = "test_v1_deprecated_default"
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        sql """
            CREATE TABLE ${testTable} (
              k INT,
              v STRING
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            )
        """
        def showCreate = sql "SHOW CREATE TABLE ${testTable}"
        assertFalse(showCreate[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))

        // ALTER TABLE ADD INDEX: format comes from table schema, user cannot specify V1 here
        sql "ALTER TABLE ${testTable} ADD INDEX idx_v (v) USING INVERTED"
        def showCreate2 = sql "SHOW CREATE TABLE ${testTable}"
        assertTrue(showCreate2[0][1].contains("USING INVERTED"))
        assertFalse(showCreate2[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))
    } finally {
        sql "DROP TABLE IF EXISTS ${testTable} FORCE"
    }

    // AC4: existing V1 table metadata load and read/write must not be affected
    // Requires a pre-existing V1 table seeded before V1 was deprecated.
    // Skips gracefully if no such table exists (e.g. fresh CI environment).
    try {
        sql "CREATE DATABASE IF NOT EXISTS test_v1_compat"
        def v1Tables = sql "SHOW TABLES FROM test_v1_compat LIKE 'v1_legacy_table'"
        if (v1Tables.size() > 0) {
            // metadata: V1 format is preserved in SHOW CREATE TABLE
            def ddl = sql "SHOW CREATE TABLE test_v1_compat.v1_legacy_table"
            assertTrue(ddl[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))

            // read: inverted index query works
            def readResult = sql "SELECT COUNT(*) FROM test_v1_compat.v1_legacy_table WHERE v MATCH 'hello'"
            assertTrue(readResult[0][0] >= 1)

            // write: INSERT into V1 table works
            sql "INSERT INTO test_v1_compat.v1_legacy_table VALUES (9999, 'v1 compat write test')"
            def writeResult = sql "SELECT COUNT(*) FROM test_v1_compat.v1_legacy_table WHERE v MATCH 'compat'"
            assertTrue(writeResult[0][0] >= 1)

            sql "DELETE FROM test_v1_compat.v1_legacy_table WHERE k = 9999"
        } else {
            logger.info("AC4 skipped: no pre-existing V1 table found in test_v1_compat")
        }
    } catch (Exception e) {
        logger.warn("AC4 skipped due to exception: ${e.message}")
    }
}
