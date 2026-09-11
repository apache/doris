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

// Lifecycle of a segment whose index file is legitimately empty.
//
// An all-NULL VARIANT column extracts no subcolumn, so the only indexed column
// opens no logical index and the segment's index file is closed with nothing in
// it. The invariant, in both directions:
//
//     schema owns an inverted index  <=>  every segment owns an index file
//
// An empty index file (E-6004 "is empty") is therefore expected wherever the
// schema still has the index, and NO index file at all (E-6003 "not found") is
// expected once the schema has none. Every producer has to keep the first half
// true -- load, compaction, light and direct schema change, BUILD INDEX -- and
// every consumer has to accept an empty index file instead of failing on it.
suite("test_empty_index_file_lifecycle", "p0") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    // show_nested_index_file reports the first rowset it cannot open, so there is
    // one status per tablet: "E-6004" empty index file, "E-6003" none at all,
    // "OK" an index file with real indexes in it.
    def index_file_status = { String tableName ->
        def statuses = [] as Set
        for (def tablet : sql_return_maparray(" show tablets from ${tableName} ")) {
            String ip = backendId_to_backendIP.get(tablet.BackendId)
            String port = backendId_to_backendHttpPort.get(tablet.BackendId)
            def (code, out, err) = http_client("GET", String.format(
                    "http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tablet.TabletId))
            logger.info("show_nested_index_file tablet=${tablet.TabletId}: code=${code}, out=${out}, err=${err}")
            statuses.add(code == 500 ? parseJson(out.trim()).status : "OK")
        }
        return statuses
    }

    // Both SHOW lists are empty for a table that never had such a job, so this
    // waits for "nothing pending" rather than for a FINISHED row to appear.
    def wait_alter_done = { String tableName ->
        for (int i = 0; i < 600; i++) {
            def jobs = sql_return_maparray(""" SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" """) +
                       sql_return_maparray(""" SHOW BUILD INDEX WHERE TableName = "${tableName}" """)
            def cancelled = jobs.findAll { it.State == "CANCELLED" }
            assertTrue(cancelled.isEmpty(), "job cancelled on ${tableName}: ${cancelled}")
            if (jobs.every { it.State == "FINISHED" }) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "schema change or index job on ${tableName} did not finish")
    }

    // The rewrite lands asynchronously on the BE, so poll the invariant instead
    // of racing the FE job record.
    def assert_index_file_status = { String tableName, String expected, String step ->
        def observed = null
        for (int i = 0; i < 180; i++) {
            observed = index_file_status(tableName)
            if (observed == ([expected] as Set)) {
                return
            }
            sleep(1000)
        }
        assertEquals([expected] as Set, observed, "after ${step}")
    }

    def assert_readable = { String tableName ->
        assertEquals(2, sql(" select count(*) from ${tableName} ")[0][0] as int)
        assertEquals(1, sql(" select count(*) from ${tableName} where id = 1 ")[0][0] as int)
    }

    // v2 covers both write paths -- memtable on the sink node streams the index
    // file through StreamSinkFileWriter, the local path writes it through
    // LocalFileWriter -- and v3 covers the other storage format.
    [["v2", true], ["v2", false], ["v3", false]].each { fmt, sinkNode ->
        def tableName = "test_empty_idx_lifecycle_${fmt}_sink${sinkNode ? 1 : 0}"
        sql " drop table if exists ${tableName} "
        sql """
            CREATE TABLE ${tableName} (
                `id`  bigint NOT NULL,
                `v`   variant NULL,
                INDEX v_idx (`v`) USING INVERTED
            ) DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "inverted_index_storage_format" = "${fmt}",
                "disable_auto_compaction" = "true"
            );
        """
        sql " set enable_memtable_on_sink_node = ${sinkNode} "

        // 1. load: the only indexed column is all NULL, so the index file is empty
        sql " insert into ${tableName} values (1, NULL) "
        sql " insert into ${tableName} values (2, NULL) "
        sql "sync"
        assert_index_file_status(tableName, "E-6004", "load")
        assert_readable(tableName)

        // 2. compaction: two empty-index inputs produce one empty-index output
        trigger_and_wait_compaction(tableName, "full")
        assert_index_file_status(tableName, "E-6004", "full compaction")
        assert_readable(tableName)

        // 3. light schema change: a default-valued ADD COLUMN keeps the segments
        //    and their index files (locally it hard-links them)
        sql """ ALTER TABLE ${tableName} ADD COLUMN c INT DEFAULT "7" """
        wait_alter_done(tableName)
        assert_index_file_status(tableName, "E-6004", "light schema change")
        assert_readable(tableName)

        // 4. direct schema change: a type change rewrites every rowset, so the
        //    output segments write their own empty index files
        sql " ALTER TABLE ${tableName} MODIFY COLUMN c BIGINT "
        wait_alter_done(tableName)
        assert_index_file_status(tableName, "E-6004", "direct schema change")
        assert_readable(tableName)

        if (!isCloudMode()) {
            // 5. ADD INDEX + BUILD INDEX runs IndexBuilder over the existing index
            //    files. An empty one means "nothing to carry over", exactly like a
            //    rowset written before any index existed; failing on it aborts the
            //    whole ALTER with "[E-6004] ... is empty".
            sql " ALTER TABLE ${tableName} ADD INDEX id_idx (`id`) USING INVERTED "
            wait_alter_done(tableName)
            build_index_on_table("id_idx", tableName)
            wait_alter_done(tableName)
            assert_index_file_status(tableName, "OK", "BUILD INDEX over an empty index file")
            assert_readable(tableName)

            // 6. dropping one of two indexes leaves an index in the schema, so the
            //    rewritten segments keep an index file -- empty again, because the
            //    surviving index is the all-NULL VARIANT one
            sql " ALTER TABLE ${tableName} DROP INDEX id_idx "
            wait_alter_done(tableName)
            assert_index_file_status(tableName, "E-6004", "dropping one of two indexes")
            assert_readable(tableName)
        }

        // 7. dropping the LAST index leaves a schema that owns no index file, so
        //    the rewritten segments must carry none. An empty one here would be an
        //    orphan: link, copy, upload, remove and CRC all skip the index file of
        //    a rowset whose schema has no index, so nothing would ever carry it
        //    along or clean it up.
        sql " ALTER TABLE ${tableName} DROP INDEX v_idx "
        wait_alter_done(tableName)
        if (!isCloudMode()) {
            assert_index_file_status(tableName, "E-6003", "dropping the last index")
            trigger_and_wait_compaction(tableName, "full")
            assert_index_file_status(tableName, "E-6003", "compacting a table with no index")
        }
        assert_readable(tableName)
    }
}
