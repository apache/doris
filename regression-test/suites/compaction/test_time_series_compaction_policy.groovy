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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_time_series_compaction_polciy", "p0") {
    def tableName = "test_time_series_compaction_polciy"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def get_rowset_count = { tablets ->
        int rowsetCount = 0
        for (def tablet in tablets) {
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        return rowsetCount
    }

    // Repeatedly trigger cumulative compaction and poll until the TOTAL rowset
    // count of all replicas drops to totalTarget, or timeout. Returns the last
    // observed total count.
    //
    // Why re-trigger in the loop instead of trigger-once-then-poll: BE filters
    // compaction candidate rowsets by the FE-pushed partition visible version
    // (Tablet::_pick_visible_rowsets_to_compaction). For a freshly created table
    // this cache is usually unset (no filtering), but FE's periodic
    // UpdateVisibleVersionTask (piggybacked on the ~60s tablet report, carrying a
    // snapshot that itself can be up to 60s stale) may land between the inserts
    // and the trigger. The pick then sees truncated candidates and returns
    // E-2000 "_input_rowsets is empty" even though a mergeable run of empty
    // rowsets exists, and a single fire-and-forget trigger is permanently lost.
    // The stale value self-heals on the next report cycle (<= ~60s), so
    // re-triggering until the deadline converges. It also absorbs E-216
    // TRY_LOCK_FAILED when a trigger races with the previous round's task.
    //
    // Why perReplicaCeiling: only a replica whose own rowset count is still above
    // the expected stable count of this phase is re-triggered. Blindly
    // re-triggering every replica could merge the NEXT empty-rowset run early
    // (e.g. drive 26 straight down to 22 in the first phase) and break the exact
    // count assertions.
    def compact_until_rowset_count = { tabletsList, perReplicaCeiling, totalTarget, timeoutSec ->
        long deadline = System.currentTimeMillis() + timeoutSec * 1000
        int total = -1
        while (true) {
            total = 0
            for (def tablet in tabletsList) {
                def (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                int cnt = ((List<String>) tabletJson.rowsets).size()
                total += cnt
                if (cnt > perReplicaCeiling) {
                    def be_host = backendId_to_backendIP["${tablet.BackendId}"]
                    def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]
                    curl("POST", "http://${be_host}:${be_port}/api/compaction/run?tablet_id=${tablet.TabletId}&compact_type=cumulative")
                }
            }
            if (total <= totalTarget || System.currentTimeMillis() >= deadline) {
                return total
            }
            Thread.sleep(2000)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "compaction_policy" = "time_series"
        );
    """

    for (int i = 0; i < 1005; i++) {
        sql """ INSERT INTO ${tableName} VALUES (${i}, "andy", "andy love apple", 100); """
    }
    
    def triggered_tablets = sql_return_maparray """show tablets from ${tableName}"""
    for (tablet in triggered_tablets) {
        def be_host = backendId_to_backendIP["${tablet.BackendId}"]
        def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]
        curl("POST", "http://${be_host}:${be_port}/api/compaction/run?tablet_id=${tablet.TabletId}&compact_type=cumulative")
    }

    Thread.sleep(10000)
    for (tablet in triggered_tablets) {
        def be_host = backendId_to_backendIP["${tablet.BackendId}"]
        def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]

        def (exit_code, stdout, stderr) = be_get_compaction_status(be_host, be_port, tablet.TabletId)
        assert exit_code == 0: "get compaction status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
        def compactionStatus = parseJson(stdout.trim())
        logger.info("compaction status: ${compactionStatus}")
        assert compactionStatus.status.toLowerCase() == "success": "compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${compactionStatus.status}"
    }
    tableName = "test_time_series_compaction_polciy_2"

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "compaction_policy" = "time_series"
        );
    """
    // insert 16 lines, BUCKETS = 2
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """

    qt_sql_1 """ select count() from ${tableName} """

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    int replicaNum = 1
    def dedup_tablets = deduplicate_tablets(tablets)
    if (dedup_tablets.size() > 0) {
        replicaNum = Math.round(tablets.size() / dedup_tablets.size())
        if (replicaNum != 1 && replicaNum != 3) {
            assert(false)
        }
    }

    // BUCKETS = 2
    // before cumulative compaction, there are 17 * 2 = 34 rowsets.
    int rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 34 * replicaNum)

    // trigger cumulative compactions for all tablets in table
    // after cumulative compaction, there is only 26 rowset.
    // 5 consecutive empty versions are merged into one empty version
    // 34 - 2*4 = 26
    // per-replica: each tablet merges its first run of 5 empties, 17 -> 13
    rowsetCount = compact_until_rowset_count.call(tablets, 13, 26 * replicaNum, 120)
    assert (rowsetCount == 26 * replicaNum) : "expected ${26 * replicaNum} rowsets, got ${rowsetCount}"

    // trigger cumulative compactions for all tablets in ${tableName}
    // after cumulative compaction, there is only 22 rowset.
    // 26 - 4 = 22
    // per-replica: only the tablet holding the id=100 bucket still has a second
    // run of 5 consecutive empty rowsets, 13 -> 9; the other stays at 13
    rowsetCount = compact_until_rowset_count.call(tablets, 9, 22 * replicaNum, 120)
    assert (rowsetCount == 22 * replicaNum) : "expected ${22 * replicaNum} rowsets, got ${rowsetCount}"

    qt_sql_2 """ select count() from ${tableName}"""
    if (isCloudMode()) {
        return;
    }
    sql """ alter table ${tableName} set ("time_series_compaction_file_count_threshold"="10")"""
    sql """sync"""
    // trigger cumulative compactions for all tablets in ${tableName}
    // after cumulative compaction, there is only 11 rowset.
    // per-replica: with file_count_threshold=10 the tablet holding the id=1
    // bucket merges all its candidates into one, 13 -> 2; the other tablet's
    // compaction score stays below the threshold and it remains at 9
    rowsetCount = compact_until_rowset_count.call(tablets, 2, 11 * replicaNum, 120)
    assert (rowsetCount == 11 * replicaNum) : "expected ${11 * replicaNum} rowsets, got ${rowsetCount}"
    qt_sql_3 """ select count() from ${tableName}"""

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "compaction_policy" = "time_series",
            "time_series_compaction_time_threshold_seconds" = "70"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """

    Thread.sleep(75000)
    trigger_and_wait_compaction(tableName, "cumulative")
}
