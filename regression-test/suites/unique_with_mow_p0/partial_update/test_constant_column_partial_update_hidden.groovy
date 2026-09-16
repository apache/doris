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

suite("test_constant_column_partial_update_hidden", "p0,nonConcurrent") {
    // Direct __DORIS_COMMIT_TSO_COL__ assertions follow the existing time-travel suites, which
    // currently run only in the shared-nothing deployment.
    if (isCloudMode()) {
        return
    }

    sql "SET show_hidden_columns = true"

    def readHiddenState = { String tableName ->
        def rows = sql """
            SELECT id, payload, stable_value, c_default,
                   __DORIS_VERSION_COL__, __DORIS_DELETE_SIGN__, __DORIS_COMMIT_TSO_COL__
            FROM ${tableName}
            ORDER BY id
        """
        return rows.collectEntries { row ->
            [(Integer.parseInt(row[0].toString())): [
                    payload: row[1].toString(),
                    stableValue: Integer.parseInt(row[2].toString()),
                    defaultValue: Integer.parseInt(row[3].toString()),
                    version: Long.parseLong(row[4].toString()),
                    deleteSign: Long.parseLong(row[5].toString()),
                    commitTso: Long.parseLong(row[6].toString())
            ]]
        }
    }

    def readOnlyHiddenState = { String tableName ->
        def rows = sql """
            SELECT id, __DORIS_VERSION_COL__, __DORIS_DELETE_SIGN__, __DORIS_COMMIT_TSO_COL__
            FROM ${tableName}
            ORDER BY id
        """
        return rows.collectEntries { row ->
            [(Integer.parseInt(row[0].toString())): [
                    version: Long.parseLong(row[1].toString()),
                    deleteSign: Long.parseLong(row[2].toString()),
                    commitTso: Long.parseLong(row[3].toString())
            ]]
        }
    }

    def checkHiddenValues = { Map state ->
        state.each { id, row ->
            assertTrue(row.version > 0, "id=${id} has an invalid version ${row.version}")
            assertEquals(0L, row.deleteSign)
            assertTrue(row.commitTso > 0, "id=${id} has an invalid commit tso ${row.commitTso}")
        }
    }

    for (boolean useRowStore : [false, true]) {
        String suffix = useRowStore ? "row_store" : "column_store"
        String tableName = "test_constant_partial_update_hidden_${suffix}"

        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        sql """
            CREATE TABLE ${tableName} (
                id INT NOT NULL,
                payload VARCHAR(64) NOT NULL,
                stable_value INT NOT NULL
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true",
                "store_row_column" = "${useRowStore}",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """

        // Case 1: rows written before c_default exists must synthesize it after ALTER.
        sql """
            INSERT INTO ${tableName} VALUES
                (1, 'old-1', 100),
                (2, 'old-2', 200)
        """
        sql "SYNC"
        def beforeAddHidden = readOnlyHiddenState(tableName)

        sql "ALTER TABLE ${tableName} ADD COLUMN c_default INT NOT NULL DEFAULT '10'"
        waitForSchemaChangeDone({
            sql "SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1"
            time 600
        })

        if (useRowStore) {
            order_qt_partial_hidden_after_add_row_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        } else {
            order_qt_partial_hidden_after_add_column_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        }

        def afterAdd = readHiddenState(tableName)
        checkHiddenValues(afterAdd)
        assertEquals(10, afterAdd[1].defaultValue)
        assertEquals(10, afterAdd[2].defaultValue)
        assertEquals(beforeAddHidden, readOnlyHiddenState(tableName))

        // Case 2: omit c_default while updating an old row. The updated row must keep the ALTER default;
        // the untouched row must retain its original hidden-column values.
        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO ${tableName}(id, payload) VALUES (1, 'old-1-partial')"
        sql "SET enable_unique_key_partial_update = false"
        sql "SYNC"

        def afterOldRowPartialUpdate = readHiddenState(tableName)
        checkHiddenValues(afterOldRowPartialUpdate)
        assertEquals(10, afterOldRowPartialUpdate[1].defaultValue)
        assertEquals(100, afterOldRowPartialUpdate[1].stableValue)
        assertTrue(afterOldRowPartialUpdate[1].version > afterAdd[1].version)
        assertTrue(afterOldRowPartialUpdate[1].commitTso > afterAdd[1].commitTso)
        assertEquals(afterAdd[2].version, afterOldRowPartialUpdate[2].version)
        assertEquals(afterAdd[2].commitTso, afterOldRowPartialUpdate[2].commitTso)

        if (useRowStore) {
            order_qt_partial_hidden_omit_default_row_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        } else {
            order_qt_partial_hidden_omit_default_column_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        }

        // Case 3: a post-ALTER row has a physical c_default. Omitting it preserves 30, while an explicit
        // partial update changes only c_default and keeps the other columns.
        sql "INSERT INTO ${tableName} VALUES (3, 'new-3', 300, 30)"
        sql "SYNC"
        def physicalRow = readHiddenState(tableName)

        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO ${tableName}(id, payload) VALUES (3, 'new-3-partial')"
        sql "SET enable_unique_key_partial_update = false"
        sql "SYNC"

        def afterPhysicalRowPartialUpdate = readHiddenState(tableName)
        checkHiddenValues(afterPhysicalRowPartialUpdate)
        assertEquals(30, afterPhysicalRowPartialUpdate[3].defaultValue)
        assertEquals(300, afterPhysicalRowPartialUpdate[3].stableValue)
        assertTrue(afterPhysicalRowPartialUpdate[3].version > physicalRow[3].version)
        assertTrue(afterPhysicalRowPartialUpdate[3].commitTso > physicalRow[3].commitTso)

        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO ${tableName}(id, c_default) VALUES (3, 40)"
        sql "SET enable_unique_key_partial_update = false"
        sql "SYNC"

        def afterExplicitUpdate = readHiddenState(tableName)
        checkHiddenValues(afterExplicitUpdate)
        assertEquals('new-3-partial', afterExplicitUpdate[3].payload)
        assertEquals(300, afterExplicitUpdate[3].stableValue)
        assertEquals(40, afterExplicitUpdate[3].defaultValue)
        assertTrue(afterExplicitUpdate[3].version > afterPhysicalRowPartialUpdate[3].version)
        assertTrue(afterExplicitUpdate[3].commitTso > afterPhysicalRowPartialUpdate[3].commitTso)

        if (useRowStore) {
            order_qt_partial_hidden_explicit_default_row_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        } else {
            order_qt_partial_hidden_explicit_default_column_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        }

        // Case 4: reusing the name creates a new column UID. In particular, key 3 must not recover the
        // dropped physical value 40 when a partial update omits the new c_default.
        sql "ALTER TABLE ${tableName} DROP COLUMN c_default"
        waitForSchemaChangeDone({
            sql "SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1"
            time 600
        })
        sql "ALTER TABLE ${tableName} ADD COLUMN c_default INT NOT NULL DEFAULT '20'"
        waitForSchemaChangeDone({
            sql "SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1"
            time 600
        })

        def afterReAdd = readHiddenState(tableName)
        checkHiddenValues(afterReAdd)
        assertEquals(20, afterReAdd[1].defaultValue)
        assertEquals(20, afterReAdd[2].defaultValue)
        assertEquals(20, afterReAdd[3].defaultValue)
        assertEquals(afterExplicitUpdate[1].version, afterReAdd[1].version)
        assertEquals(afterExplicitUpdate[1].commitTso, afterReAdd[1].commitTso)
        assertEquals(afterExplicitUpdate[2].version, afterReAdd[2].version)
        assertEquals(afterExplicitUpdate[2].commitTso, afterReAdd[2].commitTso)
        assertEquals(afterExplicitUpdate[3].version, afterReAdd[3].version)
        assertEquals(afterExplicitUpdate[3].commitTso, afterReAdd[3].commitTso)

        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO ${tableName}(id, payload) VALUES (3, 'new-3-after-readd')"
        sql "SET enable_unique_key_partial_update = false"
        sql "SYNC"

        def afterReAddPartialUpdate = readHiddenState(tableName)
        checkHiddenValues(afterReAddPartialUpdate)
        assertEquals(20, afterReAddPartialUpdate[3].defaultValue)
        assertEquals(300, afterReAddPartialUpdate[3].stableValue)
        assertTrue(afterReAddPartialUpdate[3].version > afterReAdd[3].version)
        assertTrue(afterReAddPartialUpdate[3].commitTso > afterReAdd[3].commitTso)
        assertEquals(afterReAdd[1].version, afterReAddPartialUpdate[1].version)
        assertEquals(afterReAdd[1].commitTso, afterReAddPartialUpdate[1].commitTso)
        assertEquals(afterReAdd[2].version, afterReAddPartialUpdate[2].version)
        assertEquals(afterReAdd[2].commitTso, afterReAddPartialUpdate[2].commitTso)

        if (useRowStore) {
            order_qt_partial_hidden_after_readd_row_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        } else {
            order_qt_partial_hidden_after_readd_column_store """
                SELECT id, payload, stable_value, c_default, __DORIS_DELETE_SIGN__
                FROM ${tableName}
                ORDER BY id
            """
        }
    }

    sql "SET show_hidden_columns = false"
}
