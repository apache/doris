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

suite("test_build_index_read_time_hidden_column", "nonConcurrent") {
    // BUILD INDEX rebuilds the indexes of existing rowsets through IndexBuilder in local mode only.
    if (isCloudMode()) {
        return
    }

    def timeout = 60000
    def delta_time = 1000

    // SHOW ALTER TABLE COLUMN lists the job CREATE INDEX creates; it must exist and finish.
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        def finished = false
        for (int t = 0; t < OpTimeout && !finished; t += delta_time) {
            def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            assertFalse(alter_res.any { it[9] == "CANCELLED" }, "alter job cancelled: ${alter_res}")
            finished = !alter_res.isEmpty() && alter_res.every { it[9] == "FINISHED" }
            if (!finished) {
                sleep(delta_time)
            }
        }
        assertTrue(finished, "wait_for_latest_op_on_table_finish timeout")
        sleep(3000) // wait change table state to normal
    }

    def build_index_job_ids = { table_name ->
        sql("""SHOW BUILD INDEX WHERE TableName = "${table_name}";""").collect { it[0] }
    }

    // SHOW BUILD INDEX also lists the jobs of a dropped table with the same name, so the wait is
    // keyed to the jobs BUILD INDEX adds on top of `jobs_before`. No new job is not success: the
    // result queries below can be answered by a plain scan, so the index build itself has to be
    // observed finishing before they prove anything.
    def wait_for_build_index_finish = { table_name, jobs_before, OpTimeout ->
        def finished = false
        for (int t = 0; t < OpTimeout && !finished; t += delta_time) {
            def jobs = sql("""SHOW BUILD INDEX WHERE TableName = "${table_name}";""")
                    .findAll { !(it[0] in jobs_before) }
            logger.info(table_name + " build index jobs: " + jobs)
            assertFalse(jobs.any { it[7] == "CANCELLED" }, "build index job cancelled: ${jobs}")
            finished = !jobs.isEmpty() && jobs.every { it[7] == "FINISHED" }
            if (!finished) {
                sleep(delta_time)
            }
        }
        assertTrue(finished, "wait_for_build_index_finish timeout")
    }

    // The inverted index must be the thing answering the predicate: read the scan profile of the
    // query and require RowsInvertedIndexFiltered to be reported with the expected count.
    def assert_rows_filtered_by_inverted_index = { String query, int expectedFiltered ->
        def profileId = null
        for (int t = 0; t < timeout && profileId == null; t += delta_time) {
            def profiles = new groovy.json.JsonSlurper().parseText(http_get("/rest/v1/query_profile/"))
            assertEquals(0, profiles.code)
            def profile = profiles.data.rows.find { it["Sql Statement"].contains(query) }
            if (profile != null) {
                profileId = profile["Profile ID"]
            } else {
                sleep(delta_time)
            }
        }
        assertNotNull(profileId, "no profile found for: " + query)
        def profileDetail = http_get("/rest/v1/query_profile/" + profileId)
        def matcher = (profileDetail =~ /RowsInvertedIndexFiltered:&nbsp;&nbsp;(\d+)/)
        def filtered = []
        while (matcher.find()) {
            filtered << Integer.parseInt(matcher.group(1))
        }
        assertFalse(filtered.isEmpty(), "RowsInvertedIndexFiltered missing from profile of: " + query)
        filtered.each { assertEquals(expectedFiltered, it) }
    }

    sql "drop table if exists build_index_hidden_version"
    sql """
        create table build_index_hidden_version (
            k int,
            v int
        ) unique key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    // Five singleton rowsets at versions 2..6, then one cumulative compaction merges them into a
    // multi-version rowset whose column storage holds the materialized per-row versions.
    for (int i = 1; i <= 5; ++i) {
        sql "insert into build_index_hidden_version values (${i}, ${i * 10})"
    }
    trigger_and_wait_compaction("build_index_hidden_version", "cumulative")

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
    for (def tablet : sql_return_maparray("show tablets from build_index_hidden_version")) {
        def (code, out, err) = be_show_tablet_status(
                backendIdToIp[tablet.BackendId], backendIdToHttpPort[tablet.BackendId], tablet.TabletId)
        assertEquals(0, code)
        def rowsets = parseJson(out.trim()).rowsets
        long lastVersion = tablet.Version as long
        String compactedRowset = "[${lastVersion - 4}-${lastVersion}] "
        assertTrue(rowsets.any { it.startsWith(compactedRowset) },
                "Expected compacted ${compactedRowset}: ${rowsets}")
    }

    sql "set show_hidden_columns = true"
    order_qt_versions_before_index """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version
    """

    // The online index build reads the compacted rowset through SegmentIterator. It must index the
    // stored per-row versions, not a synthesized placeholder, because a multi-version rowset has no
    // read-time constant and queries consult the index as-is.
    sql "create index version_idx on build_index_hidden_version(__DORIS_VERSION_COL__) using inverted"
    wait_for_latest_op_on_table_finish("build_index_hidden_version", timeout)
    def jobs_before_build = build_index_job_ids("build_index_hidden_version")
    sql "build index version_idx on build_index_hidden_version"
    wait_for_build_index_finish("build_index_hidden_version", jobs_before_build, timeout)

    sql "set enable_inverted_index_query = true"
    // The BKD hit estimate of a five-row segment exceeds the default 50% skip threshold and would
    // bypass the index; a threshold of 0 disables that bypass so the index answers the predicates.
    sql "set inverted_index_skip_threshold = 0"
    sql "set enable_profile = true"
    def version_3 = "select k, __DORIS_VERSION_COL__ from build_index_hidden_version where __DORIS_VERSION_COL__ = 3"
    order_qt_index_version_3 version_3
    assert_rows_filtered_by_inverted_index(version_3, 4)
    def version_in = "select k, __DORIS_VERSION_COL__ from build_index_hidden_version where __DORIS_VERSION_COL__ in (2, 6)"
    order_qt_index_version_in version_in
    assert_rows_filtered_by_inverted_index(version_in, 3)
    // The materialized VERSION zone map of the compacted segment is [2, 6], so the segment-level
    // statistics prune this predicate before the index is consulted; the index content itself is
    // proven by the two filtered-row counts above, which an index of placeholder zeros fails.
    order_qt_index_version_0 """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version where __DORIS_VERSION_COL__ = 0
    """
    order_qt_versions_after_index """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version
    """
    sql "set enable_profile = false"
    sql "set show_hidden_columns = false"
}
