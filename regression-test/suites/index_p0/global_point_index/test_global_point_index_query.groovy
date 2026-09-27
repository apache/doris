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

import java.util.regex.Matcher
import java.util.regex.Pattern

// Query results must be the same with and without GLOBAL_POINT pruning, for every write path
// (sink-node load, local load, compaction output, historical rowsets without an index), and the
// index must actually skip data.
suite("test_global_point_index_query") {
    def tbl = "test_global_point_index_query"
    def buckets = 8

    def countOf = { String predicate, boolean prune ->
        sql "SET enable_global_point_index_prune = ${prune}"
        return (sql "SELECT count(*) FROM ${tbl} WHERE ${predicate}")[0][0] as long
    }

    // The same answer with pruning on and off, and the expected one.
    def checkCount = { String predicate, long expected ->
        assertEquals(expected, countOf(predicate, false), "prune off: ${predicate}")
        assertEquals(expected, countOf(predicate, true), "prune on: ${predicate}")
    }

    def explainOf = { String predicate ->
        sql "SET enable_global_point_index_prune = true"
        return (sql "EXPLAIN SELECT * FROM ${tbl} WHERE ${predicate}").collect { it[0] }.join("\n")
    }

    def selectedTablets = { String explainString ->
        Matcher m = Pattern.compile("tablets=(\\d+)/(\\d+)").matcher(explainString)
        assertTrue(m.find(), "no tablets= line in: ${explainString}")
        return m.group(1).toInteger()
    }

    def loadBatch = { int from, int to ->
        def values = (from..<to).collect { i ->
            def ev = (i % 10 == 9) ? "NULL" : "${i * 7}"
            "(${i}, ${ev}, 'event-${i}')"
        }
        sql "INSERT INTO ${tbl} VALUES ${values.join(', ')}"
    }

    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            id BIGINT NOT NULL,
            ev INT NULL,
            name VARCHAR(64) NULL,
            INDEX idx_ev (ev) USING GLOBAL_POINT,
            INDEX idx_name (name) USING GLOBAL_POINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS ${buckets}
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_condition_cache = false"

    // Rowsets written through both load paths.
    sql "SET enable_memtable_on_sink_node = true"
    loadBatch(0, 400)
    sql "SET enable_memtable_on_sink_node = false"
    loadBatch(400, 800)
    sql "SET enable_memtable_on_sink_node = true"
    sql "SYNC"

    def checkAll = {
        checkCount("ev = 7", 1)            // id 1
        checkCount("ev = ${799 * 7}", 0)   // id 799 has a NULL ev
        checkCount("ev = ${798 * 7}", 1)
        checkCount("ev = 5", 0)            // never written
        checkCount("ev IN (7, 14, 5)", 2)
        checkCount("name = 'event-42'", 1)
        checkCount("name = 'event-4242'", 0)
        checkCount("name IN ('event-1', 'event-401', 'nope')", 2)
        checkCount("ev IS NULL", 80)
        checkCount("ev > 5", 720 - 1)       // not EQ/IN: not pruned, still correct
    }
    checkAll()

    if (isCloudMode()) {
        // A value that was never written is a miss on every tablet, up to bloom false positives.
        def absent = explainOf("ev = 5")
        assertTrue(absent.contains("globalPointIndex: ev"), absent)
        assertTrue(selectedTablets(absent) < buckets, absent)
        // One matching row lives in one tablet; the others are pruned.
        def present = explainOf("name = 'event-42'")
        assertTrue(present.contains("globalPointIndex: name"), present)
        assertTrue(selectedTablets(present) < buckets, present)
        // The session variable turns it off.
        sql "SET enable_global_point_index_prune = false"
        def off = (sql "EXPLAIN SELECT * FROM ${tbl} WHERE ev = 5").collect { it[0] }.join("\n")
        assertFalse(off.contains("globalPointIndex"), off)
        assertEquals(buckets, selectedTablets(off))
    }

    // Compaction rewrites the rowsets and rebuilds the blooms.
    trigger_and_wait_compaction(tbl, "full")
    checkAll()

    // An index added later only covers new rowsets; the older ones must still be read.
    sql "SET enable_add_index_for_new_data = true"
    def tbl2 = "test_global_point_index_query_add"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """
        CREATE TABLE ${tbl2} (
            id BIGINT NOT NULL,
            name VARCHAR(64) NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql "INSERT INTO ${tbl2} VALUES (1, 'old-1'), (2, 'old-2')"
    sql "CREATE INDEX idx_name ON ${tbl2}(name) USING GLOBAL_POINT"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tbl2}" ORDER BY CreateTime DESC LIMIT 1"""
        time 120
    }
    sql "INSERT INTO ${tbl2} VALUES (3, 'new-3')"
    sql "SYNC"
    for (def prune : [false, true]) {
        sql "SET enable_global_point_index_prune = ${prune}"
        assertEquals(1L, (sql "SELECT count(*) FROM ${tbl2} WHERE name = 'old-1'")[0][0] as long)
        assertEquals(1L, (sql "SELECT count(*) FROM ${tbl2} WHERE name = 'new-3'")[0][0] as long)
        assertEquals(0L, (sql "SELECT count(*) FROM ${tbl2} WHERE name = 'none'")[0][0] as long)
    }

    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql "DROP TABLE IF EXISTS ${tbl}"
}
