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
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if (alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size()
            def finished_num = 0
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_build_index_on_partition_finish timeout")
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
    sql "build index version_idx on build_index_hidden_version"
    wait_for_build_index_on_partition_finish("build_index_hidden_version", timeout)

    sql "set enable_inverted_index_query = true"
    order_qt_index_version_3 """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version where __DORIS_VERSION_COL__ = 3
    """
    order_qt_index_version_in """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version where __DORIS_VERSION_COL__ in (2, 6)
    """
    order_qt_index_version_0 """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version where __DORIS_VERSION_COL__ = 0
    """
    order_qt_versions_after_index """
        select k, __DORIS_VERSION_COL__ from build_index_hidden_version
    """
    sql "set show_hidden_columns = false"
}
