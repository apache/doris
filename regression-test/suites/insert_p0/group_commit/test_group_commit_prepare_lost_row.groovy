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

import com.mysql.cj.jdbc.StatementImpl
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

// Deterministically reproduce the group-commit lost-row race using a REAL server-side
// prepared statement that reuses one group commit plan -- no load_id is mocked.
//
// Why the load_id "repeats" even though FE is strictly serial:
//   With a reused group commit plan, query_id / load_id / fragment_instance_id are baked into
//   the reused (serialized) plan bytes, so every insert RPC runs under the SAME load_id X. The
//   BE uses load_id X as the per-queue routing/registration key in
//   LoadBlockQueue::_load_ids_to_write_dep, and remove_load_id(X) is issued more than once per
//   key, from sites that are NOT all ordered before the FE response:
//     * wind_up()            -- synchronous, before the RPC returns (this is what makes FE-serial
//                               look safe);
//     * the sink local-state destructor -- runs during ASYNC fragment teardown, AFTER the RPC
//                               has already returned to FE.
//   FE serialization only orders the producers' synchronous paths; it does NOT order one load's
//   async teardown remove(X) against the NEXT load's add_load_id(X). So statement N's teardown
//   remove(X) can land after statement N+1 has re-registered the shared key X, erasing N+1's
//   in-flight registration. The internal consumer then sees "no writer in flight"
//   (_load_ids_to_write_dep.empty()), commits the queue, and N+1's later add_block lands on the
//   already-committed queue -- add_block has no _need_commit guard, returns OK, and the row is
//   silently lost while FE reports success.
//
// A single connection drives the prepared-statement executes serially, so we only need to widen
// the (real) async timing window with two BE debug points:
//   * GroupCommitBlockSink.delay_teardown_remove_load_id : hold statement N's teardown remove(X)
//     until statement N+1 has joined the same queue (group_commit_load_count >= 2), so the
//     teardown remove(X) deterministically clobbers N+1's shared-key registration.
//   * LoadBlockQueue.add_block.block_reuse_second : hold N+1's add_block until the consumer has
//     committed the queue; when released, N+1's block is appended to the committed queue.
//
// Requires the BE debug-point patch in be/src/exec/operator/group_commit_block_sink_operator.cpp
// and be/src/load/group_commit/group_commit_mgr.cpp.
//
// Result:
//   With the per-sink load_id-regeneration fix (UniqueId::gen_uid() in
//   GroupCommitBlockSinkOperatorX::init) every sink gets a unique load_id, so the teardown
//   remove targets a different key and cannot clobber the next load -> both rows commit -> this
//   case PASSES (id=2 and id=3 both present). Revert that one line and the reused load_id is
//   shared again, the race regresses, id=3 is dropped, and this case FAILS.
suite("test_group_commit_prepare_lost_row", "nonConcurrent") {
    def dbName = "regression_test_insert_p0"
    def table = dbName + ".test_group_commit_prepare_lost_row"
    def dpDelayRemove = "GroupCommitBlockSink.delay_teardown_remove_load_id"
    def dpBlockSecond = "LoadBlockQueue.add_block.block_reuse_second"
    // large enough that a freshly created queue is NOT need_commit before the next load joins,
    // yet small enough that the consumer commits while the 2nd load's add_block is held.
    def intervalMs = 1000

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "drop table if exists ${table}"
    sql """
        CREATE TABLE ${table} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "group_commit_interval_ms" = "${intervalMs}",
            "replication_num" = "1"
        );
    """

    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    // server-side prepared statement + async group commit (so the plan can be reused)
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName, false) +
            "&sessionVariables=group_commit=async_mode"
    logger.info("jdbc url: " + url)

    // Pre-capture the BE http endpoints on a FREE connection, BEFORE the dance starts.
    // During the id=2/id=3 dance the ONLY JDBC connection is tied up by thread tB's blocked
    // executeBatch (held at add_block). So we must NOT disable the debug points via
    // GetDebugPoint().disableDebugPointForAllBEs(...) -- that internally runs `sql "show backends"`
    // on the SAME (busy) connection and deadlocks: the disable waits for the connection held by
    // tB, while tB waits for the disable to release its add_block. Instead, disable via raw HTTP
    // (no JDBC at all) against this pre-captured list.
    def beIps = [:]
    def bePorts = [:]
    getBackendIpHttpPort(beIps, bePorts)
    def beHttpList = []
    beIps.each { id, ip -> beHttpList.add([ip as String, bePorts[id] as int]) }
    logger.info("BE http endpoints (pre-captured): " + beHttpList)
    def disableDpOnAllBEs = { String name ->
        beHttpList.each { hp ->
            DebugPoint.disableDebugPoint(hp[0] as String, hp[1] as int, NodeType.BE, name)
        }
    }

    def insertOne = { stmt, int id, String name, Integer score ->
        stmt.setInt(1, id)
        stmt.setString(2, name)
        if (score == null) {
            stmt.setNull(3, java.sql.Types.INTEGER)
        } else {
            stmt.setInt(3, score)
        }
        stmt.addBatch()
    }

    def serverInfoOf = { stmt ->
        def results = ((StatementImpl) stmt).results
        return results != null ? results.getServerInfo() : null
    }

    connect(user, password, url) {
        def insert_stmt = prepareStatement """ INSERT INTO ${table} VALUES(?, ?, ?) """
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, insert_stmt.class)

        try {
            // 1. prime the prepared plan: the first execute creates and caches the reusable
            //    serialized plan (and bakes load_id X). Not a reuse yet.
            insertOne(insert_stmt, 1, "a", 10)
            insert_stmt.executeBatch()
            logger.info("id=1 serverInfo: " + serverInfoOf(insert_stmt))
            // let the id=1 queue commit and tear down before the dance, so the next execute
            // creates a fresh queue rather than joining id=1's.
            sleep(intervalMs + 2000)

            // arm the debug points only for the id=2 / id=3 dance.
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().enableDebugPointForAllBEs(dpDelayRemove)
            GetDebugPoint().enableDebugPointForAllBEs(dpBlockSecond)

            // 2. statement A (id=2): reuses the plan -> shares load_id X. Creates a fresh queue
            //    Q, appends its block (1st add_block on Q proceeds), and returns. Its async
            //    teardown remove(X) is now held until a 2nd load joins Q.
            insertOne(insert_stmt, 2, "b", 20)
            insert_stmt.executeBatch()
            def infoA = serverInfoOf(insert_stmt)
            logger.info("id=2 (A) serverInfo: " + infoA)
            // the shared load_id only exists when the plan is actually reused
            assertTrue(infoA != null && infoA.contains("reuse_group_commit_plan"),
                    "id=2 must reuse the group commit plan (shared load_id); serverInfo=" + infoA)

            // 3. statement B (id=3): reuses the plan (same load_id X), joins Q via add_load_id(X)
            //    -> this releases A's held teardown remove(X), which erases B's shared-key
            //    registration. B then blocks at add_block until we release it.
            insertOne(insert_stmt, 3, "c", 30)
            def errB = new java.util.concurrent.atomic.AtomicReference<String>(null)
            def tB = Thread.start {
                try {
                    insert_stmt.executeBatch()
                    logger.info("id=3 (B) serverInfo: " + serverInfoOf(insert_stmt))
                } catch (Throwable e) {
                    errB.set(e.getMessage())
                    logger.info("id=3 (B) executeBatch threw: " + e.getMessage())
                }
            }

            // 4. while B is held at add_block: A's teardown clobbered the shared key, so the
            //    consumer commits Q (interval elapses) with only A's row.
            sleep(intervalMs + 2000)

            // 5. release B's add_block: pre-fix it lands on the already-committed queue and is
            //    silently lost; with the fix B has its own load_id and queue so nothing is lost.
            // disable via raw HTTP (NOT sql) -- the JDBC connection is held by tB's blocked
            // executeBatch, so routing this through `sql "show backends"` would deadlock.
            disableDpOnAllBEs(dpBlockSecond)
            disableDpOnAllBEs(dpDelayRemove)
            tB.join()
            logger.info("id=3 (B) error (if any): " + errB.get())
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            try { insert_stmt.close() } catch (Throwable ignore) {}
        }
    }

    // both reused-plan rows must be present. If the lost-row race regresses, id=3 is dropped.
    def rows = null
    for (int i = 0; i < 20; i++) {
        sleep(1000)
        rows = sql "select id, name, score from ${table} where id in (2, 3) order by id"
        logger.info("rows(id in 2,3) = " + rows + ", retry = " + i)
        if (rows.size() >= 2) break
    }
    assertEquals(2, rows.size(),
            "group commit lost a reused-plan row: expected id=2 and id=3, got " + rows)
}
