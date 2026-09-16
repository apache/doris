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

suite("test_constant_hidden_column_statistics", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_constant_hidden_column_statistics FORCE"
    // MIN/MAX statistics pushdown on UNIQUE tables currently uses the merge-on-read plan with its
    // delete-sign filter. Keep this suite on MOR so the explain assertions below prove that the
    // hidden constant column reaches the statistics iterator.
    sql """
        CREATE TABLE test_constant_hidden_column_statistics (
            `k` INT NOT NULL,
            `v` INT NOT NULL
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    sql "SET show_hidden_columns = true"

    def readHiddenState = {
        def rows = sql """
            SELECT k, __DORIS_VERSION_COL__
            FROM test_constant_hidden_column_statistics
            ORDER BY k
        """
        return rows.collectEntries { row ->
            [(Integer.parseInt(row[0].toString())): [
                    version: Long.parseLong(row[1].toString())
            ]]
        }
    }

    def readHiddenMinMax = {
        def row = sql """
            SELECT MIN(__DORIS_VERSION_COL__), MAX(__DORIS_VERSION_COL__)
            FROM test_constant_hidden_column_statistics
        """
        return row[0].collect { value -> Long.parseLong(value.toString()) }
    }

    // Case 1: the first rowset predates c_default. Its hidden version is captured before ALTER so the
    // test can verify that schema evolution does not replace it with a persisted placeholder.
    sql """
        INSERT INTO test_constant_hidden_column_statistics VALUES
            (1, 10),
            (2, 20)
    """
    sql "SYNC"

    def beforeAlter = readHiddenState()
    assertEquals(2, beforeAlter.size())
    assertEquals(beforeAlter[1].version, beforeAlter[2].version)
    assertTrue(beforeAlter[1].version > 0)
    def oldVersion = beforeAlter[1].version

    sql """
        ALTER TABLE test_constant_hidden_column_statistics
        ADD COLUMN `c_default` INT NOT NULL DEFAULT "10"
    """
    waitForSchemaChangeDone({
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE IndexName = 'test_constant_hidden_column_statistics'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    })

    order_qt_hidden_after_alter_add_default """
        SELECT k, v, c_default
        FROM test_constant_hidden_column_statistics
        ORDER BY k
    """

    def afterAlter = readHiddenState()
    assertEquals(beforeAlter, afterAlter)

    // Case 2: with only the pre-ALTER rowset, the hidden version is a rowset-scoped constant. Its
    // statistics are therefore [value, value], just like the ALTER-added column default.
    sql "SET enable_pushdown_minmax_on_unique = false"
    def oldMinMaxWithoutPushdown = readHiddenMinMax()

    sql "SET enable_pushdown_minmax_on_unique = true"
    explain {
        sql("SELECT MIN(__DORIS_VERSION_COL__) FROM test_constant_hidden_column_statistics")
        contains "pushAggOp=MINMAX"
    }
    def oldMinMaxWithPushdown = readHiddenMinMax()

    assertEquals(oldMinMaxWithoutPushdown, oldMinMaxWithPushdown)
    assertEquals([oldVersion, oldVersion], oldMinMaxWithPushdown)

    // Case 3: these predicates are evaluated against the constant zonemaps of the historical rowset.
    // c_default in the same predicates is also a constant because it was added after the write.
    order_qt_hidden_initial_version_equal """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ = ${oldVersion} AND c_default = 10
        ORDER BY k
    """
    order_qt_hidden_initial_version_above_max """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ > ${oldVersion}
        ORDER BY k
    """
    // Case 4: add a physical post-ALTER rowset and verify that a later read gets its own rowset version
    // instead of reusing the constant cached for the first rowset.
    sql """
        INSERT INTO test_constant_hidden_column_statistics
            (k, v, c_default) VALUES (3, 30, 30)
    """
    sql "SYNC"

    order_qt_hidden_after_post_alter_insert """
        SELECT k, v, c_default
        FROM test_constant_hidden_column_statistics
        ORDER BY k
    """

    def afterInsert = readHiddenState()
    assertEquals(beforeAlter[1], afterInsert[1])
    assertEquals(beforeAlter[2], afterInsert[2])
    assertTrue(afterInsert[3].version > oldVersion)
    def insertVersion = afterInsert[3].version

    sql "SET enable_pushdown_minmax_on_unique = false"
    def insertMinMaxWithoutPushdown = readHiddenMinMax()
    sql "SET enable_pushdown_minmax_on_unique = true"
    def insertMinMaxWithPushdown = readHiddenMinMax()

    assertEquals(insertMinMaxWithoutPushdown, insertMinMaxWithPushdown)
    assertEquals([oldVersion, insertVersion], insertMinMaxWithPushdown)

    order_qt_hidden_version_range_after_insert """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ > ${oldVersion}
        ORDER BY k
    """
    // Case 5: a MOR upsert updates key 2 while leaving key 1 visible in the original rowset. Keeping
    // one visible row in every rowset makes pushed and non-pushed MIN/MAX directly comparable.
    sql """
        INSERT INTO test_constant_hidden_column_statistics
            (k, v, c_default) VALUES (2, 200, 20)
    """
    sql "SYNC"

    order_qt_hidden_after_update """
        SELECT k, v, c_default
        FROM test_constant_hidden_column_statistics
        ORDER BY k
    """

    def afterUpdate = readHiddenState()
    assertEquals(beforeAlter[1], afterUpdate[1])
    assertEquals(afterInsert[3], afterUpdate[3])
    assertTrue(afterUpdate[2].version > insertVersion)
    def updateVersion = afterUpdate[2].version

    sql "SET enable_pushdown_minmax_on_unique = false"
    def updateMinMaxWithoutPushdown = readHiddenMinMax()
    sql "SET enable_pushdown_minmax_on_unique = true"
    explain {
        sql("SELECT MAX(__DORIS_VERSION_COL__) FROM test_constant_hidden_column_statistics")
        contains "pushAggOp=MINMAX"
    }
    def updateMinMaxWithPushdown = readHiddenMinMax()

    assertEquals(updateMinMaxWithoutPushdown, updateMinMaxWithPushdown)
    assertEquals([oldVersion, updateVersion], updateMinMaxWithPushdown)

    // Case 6: exact, range, and empty predicates cover the per-rowset hidden-column zonemaps after the
    // old constant rowset, post-ALTER insert, and update rowsets coexist.
    order_qt_hidden_final_version_equal_old """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ = ${oldVersion} AND c_default = 10
        ORDER BY k
    """
    order_qt_hidden_final_version_equal_insert """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ = ${insertVersion} AND c_default = 30
        ORDER BY k
    """
    order_qt_hidden_final_version_equal_update """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ = ${updateVersion} AND c_default = 20
        ORDER BY k
    """
    order_qt_hidden_final_version_above_old """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ > ${oldVersion}
        ORDER BY k
    """
    order_qt_hidden_final_version_above_max """
        SELECT k, c_default
        FROM test_constant_hidden_column_statistics
        WHERE __DORIS_VERSION_COL__ > ${updateVersion}
        ORDER BY k
    """

    sql "SET enable_pushdown_minmax_on_unique = false"
    sql "SET show_hidden_columns = false"
}
