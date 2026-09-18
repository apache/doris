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

suite("test_commit_tso_reader_paths", "nonConcurrent") {
    sql "SET show_hidden_columns = true"
    sql "SET enable_common_expr_pushdown = true"

    // Obtain the oracle without first reading the hidden column and warming its reader.
    // Commit TSOs are allocated at runtime, so compare against rowset metadata instead of a golden file.
    def latestCommitTso = { tabletId ->
        def rows = sql_return_maparray """
            SELECT COMMIT_TSO FROM information_schema.rowsets
            WHERE TABLET_ID = ${tabletId} ORDER BY TXN_ID DESC LIMIT 1
        """
        def range = rows[0]["COMMIT_TSO"].toString() =~ /\[(-?\d+)-(-?\d+)\]/
        assertTrue(range.matches())
        assertEquals(range[0][1], range[0][2])
        def tso = range[0][2] as Long
        assertTrue(tso > 0)
        tso
    }

    sql "DROP TABLE IF EXISTS test_commit_tso_reader_dup FORCE"
    sql """
        CREATE TABLE test_commit_tso_reader_dup (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "binlog.enable" = "true",
                    "binlog.format" = "ROW", "disable_auto_compaction" = "true")
    """
    sql "INSERT INTO test_commit_tso_reader_dup VALUES (1, 7), (2, 8), (3, 9)"
    def dupTablet = (sql_return_maparray "SHOW TABLETS FROM test_commit_tso_reader_dup")[0]["TabletId"]
    def dupTso = latestCommitTso(dupTablet)
    // Cross-column OR reaches the common-expression path before the data iterator is created.
    check_sql_equal("""SELECT k, __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_dup
                         WHERE __DORIS_COMMIT_TSO_COL__ > 0 OR v = 99 ORDER BY k""",
                    "SELECT k, CAST(${dupTso} AS BIGINT) FROM test_commit_tso_reader_dup ORDER BY k")
    check_sql_equal("""SELECT k, __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_dup
                         WHERE __DORIS_COMMIT_TSO_COL__ > ${dupTso} OR v = 7 ORDER BY k""",
                    "SELECT k, CAST(${dupTso} AS BIGINT) FROM test_commit_tso_reader_dup WHERE v = 7 ORDER BY k")
    check_sql_equal("SELECT MIN(__DORIS_COMMIT_TSO_COL__), MAX(__DORIS_COMMIT_TSO_COL__) FROM test_commit_tso_reader_dup",
                    "SELECT CAST(${dupTso} AS BIGINT), CAST(${dupTso} AS BIGINT)")
    check_sql_equal("SELECT k FROM test_commit_tso_reader_dup WHERE __DORIS_COMMIT_TSO_COL__ > ${dupTso} ORDER BY k",
                    "SELECT k FROM test_commit_tso_reader_dup WHERE FALSE ORDER BY k")

    sql "DROP TABLE IF EXISTS test_commit_tso_reader_mow FORCE"
    sql """
        CREATE TABLE test_commit_tso_reader_mow (k INT, v INT, w INT)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                    "store_row_column" = "true", "binlog.enable" = "true",
                    "binlog.format" = "ROW", "disable_auto_compaction" = "true")
    """
    sql "INSERT INTO test_commit_tso_reader_mow VALUES (1, 7, 10), (2, 8, 20), (3, 9, 30)"
    def mowTablet = (sql_return_maparray "SHOW TABLETS FROM test_commit_tso_reader_mow")[0]["TabletId"]
    def firstTso = latestCommitTso(mowTablet)
    // Warm the point-query row cache without requesting TSO, then request TSO from the same key.
    sql "SELECT v FROM test_commit_tso_reader_mow WHERE k = 1"
    check_sql_equal("SELECT __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_mow WHERE k = 1",
                    "SELECT CAST(${firstTso} AS BIGINT)")
    // Full row store still resolves TSO through its source rowset when ordinary column-store
    // access is disabled. Exercise both an empty JSONB projection and mixed output slot order.
    check_sql_equal("""SELECT /*+ SET_VAR(enable_short_circuit_query_access_column_store=false) */
                         __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_mow WHERE k = 1""",
                    "SELECT CAST(${firstTso} AS BIGINT)")
    check_sql_equal("""SELECT /*+ SET_VAR(enable_short_circuit_query_access_column_store=false) */
                         __DORIS_COMMIT_TSO_COL__, v FROM test_commit_tso_reader_mow WHERE k = 1""",
                    "SELECT CAST(${firstTso} AS BIGINT), 7")
    check_sql_equal("""SELECT /*+ SET_VAR(enable_short_circuit_query_access_column_store=false) */
                         v, __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_mow WHERE k = 2""",
                    "SELECT 8, CAST(${firstTso} AS BIGINT)")

    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_commit_tso_reader_mow(k, v) VALUES (1, 70)"
    sql "SET enable_unique_key_partial_update = false"
    def secondTso = latestCommitTso(mowTablet)
    assertTrue(secondTso > firstTso)
    check_sql_equal("""SELECT k, w, __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_mow
                         WHERE __DORIS_COMMIT_TSO_COL__ > 0 OR v = 99 ORDER BY k""",
                    """SELECT k, w, CAST(IF(k = 1, ${secondTso}, ${firstTso}) AS BIGINT)
                         FROM test_commit_tso_reader_mow ORDER BY k""")
    // Compare two-phase row-id fetch with a metadata-based oracle, including a row-store table.
    check_sql_equal("""SELECT /*+ SET_VAR(enable_two_phase_read_opt=true) */
                         k, w, __DORIS_COMMIT_TSO_COL__ FROM test_commit_tso_reader_mow
                         ORDER BY v DESC, k LIMIT 2""",
                    """SELECT k, w, CAST(IF(k = 1, ${secondTso}, ${firstTso}) AS BIGINT)
                         FROM test_commit_tso_reader_mow ORDER BY v DESC, k LIMIT 2""")
}
