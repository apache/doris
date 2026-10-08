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

import org.apache.doris.regression.suite.ClusterOptions
import java.util.concurrent.TimeUnit

suite("test_delete_bitmap_lock_with_restart", "docker") {
    if (!isCloudMode()) {
        return
    }
    final String LOAD_BARRIER = "CloudEngineCalcDeleteBitmapTask.handle.block_for_restart"
    final String JOB_BARRIER = "CloudMetaMgr.get_delete_bitmap_update_lock.block_for_restart"
    final int LOCK_TTL_SECONDS = 60
    final int RECOVERY_TIMEOUT_SECONDS = 180

    // Isolate all six combinations: a failed/restarted operation must not supply another
    // scenario's lock, debug point, rowsets or schema-change state.
    ["load", "compaction", "schema_change"].each { operation ->
        ["fe", "be"].each { restartNode ->
            def options = new ClusterOptions()
            options.feNum = 1
            options.beNum = 1
            options.cloudMode = true
            options.enableDebugPoints()
            options.feConfigs += [
                    'cloud_cluster_check_interval_second=1',
                    'heartbeat_interval_second=1',
                    "delete_bitmap_lock_expiration_seconds=${LOCK_TTL_SECONDS}",
            ]
            options.beConfigs += ["delete_bitmap_lock_expiration_seconds=${LOCK_TTL_SECONDS}"]

            docker(options) {
                String tableName = "bitmap_restart_${operation}_${restartNode}"
                String token = UUID.randomUUID().toString()
                String backgroundLabel = "bitmap_background_${token}"
                String barrier = operation == "load" ? LOAD_BARRIER : JOB_BARRIER
                def background = null
                def recovery = null
                def backgroundResult = null
                def heldLock = null
                def lastLock = null
                def lastTxn = null
                def lastJob = null
                def backend = cluster.getAllBackends()[0]
                def ms = cluster.getAllMetaservices()[0]
                File beLog = new File(backend.getLogFilePath())

                def httpJson = { String address, boolean allowMissing = false, String method = "GET" ->
                    def connection = new URL(address).openConnection()
                    connection.requestMethod = method
                    connection.connectTimeout = 5000
                    connection.readTimeout = 10000
                    try {
                        int status = connection.responseCode
                        String body = status >= 400 ? connection.errorStream?.text : connection.inputStream.text
                        // get_value reports an absent KV through its error response, not a lock.
                        if (allowMissing && body?.contains("kv not found")) {
                            return null
                        }
                        assertEquals(200, status, "HTTP request failed: ${body}")
                        return parseJson(body)
                    } finally {
                        connection.disconnect()
                    }
                }
                def stream = { String label ->
                    def outcome = [:]
                    streamLoad {
                        table tableName
                        set 'label', label
                        set 'timeout', '120'
                        connectTimeout 10000
                        socketTimeout 150000
                        set 'column_separator', ','
                        set 'columns', 'id, name, score'
                        file 'test_stream_load.csv'
                        check { result, exception, startTime, endTime ->
                            // The interrupted request may lose its response. Its persisted
                            // transaction state below determines the outcome, not the HTTP error.
                            outcome = [response: result ? parseJson(result) : null, error: exception]
                        }
                    }
                    return outcome
                }

                try {
                    sql """CREATE TABLE ${tableName} (
                        id INT NOT NULL, name VARCHAR(10), score INT
                    ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("enable_unique_key_merge_on_write"="true",
                                "disable_auto_compaction"="true", "replication_num"="1")"""
                    for (int id = 1; id <= 5; id++) {
                        sql "INSERT INTO ${tableName} VALUES (${id}, 'seed', ${id * 10})"
                    }
                    sql 'SYNC'
                    def tablet = sql_return_maparray("SHOW TABLETS FROM ${tableName}")[0]
                    long tabletId = tablet.TabletId.toString().toLong()
                    def tabletInfo = sql_return_maparray("SHOW TABLET ${tabletId}")[0]
                    long tableId = tabletInfo.TableId.toString().toLong()
                    def readLock = {
                        // The isolated Docker fixture uses the default v1 table-level lock.
                        // Observe the persisted owner/expiration instead of inferring it from sleep.
                        httpJson("http://${ms.host}:${ms.httpPort}/MetaService/http/get_value" +
                                "?token=greedisgood9999&key_type=MetaDeleteBitmapUpdateLock" +
                                "&instance_id=default_instance_id&table_id=${tableId}&partition_id=-1", true)
                    }
                    def compactionStatus = {
                        httpJson("http://${backend.host}:${backend.httpPort}/api/compaction/run_status?tablet_id=${tabletId}")
                    }
                    def triggerCompaction = {
                        def response = httpJson("http://${backend.host}:${backend.httpPort}/api/compaction/run" +
                                "?tablet_id=${tabletId}&compact_type=cumulative", false, "POST")
                        assertEquals("success", response.status.toString().toLowerCase(),
                                "compaction submission failed: ${response}")
                    }

                    GetDebugPoint().enableDebugPointForAllBEs(barrier,
                            [tablet_id: tabletId, table_id: tableId, token: token, timeout: 180])
                    if (operation == "load") {
                        background = thread("bitmap-background-${token}") {
                            try {
                                return stream(backgroundLabel)
                            } catch (Exception e) {
                                return [error: e]
                            }
                        }
                    } else if (operation == "compaction") {
                        triggerCompaction()
                    } else {
                        sql "ALTER TABLE ${tableName} MODIFY COLUMN score VARCHAR(100)"
                    }

                    // Both conditions are required: BE reached the targeted barrier AND MS
                    // still records that exact owner with an unexpired lock. Starting a thread
                    // or seeing an ALTER/compaction submission succeed is not a handshake.
                    awaitUntil(60, 0.2) {
                        String text = beLog.exists() ? beLog.text : ''
                        def marker = text =~ /delete bitmap restart barrier token=${token} lock_id=(-?\d+)/
                        if (!marker.find()) {
                            return false
                        }
                        long owner = marker.group(1).toLong()
                        lastLock = readLock()
                        if (lastLock == null || lastLock.lock_id.toString().toLong() != owner ||
                                lastLock.expiration.toString().toLong() <= System.currentTimeMillis().intdiv(1000)) {
                            return false
                        }
                        if (operation == "load") {
                            def transactions = sql_return_maparray("SHOW TRANSACTION WHERE LABEL='${backgroundLabel}'")
                            if (transactions.size() != 1 || transactions[0].TransactionId.toString().toLong() != owner) {
                                return false
                            }
                        }
                        heldLock = lastLock
                        return true
                    }
                    logger.info("Restart ${restartNode} during ${operation}: heldLock=${heldLock}, token=${token}")
                    if (restartNode == "fe") {
                        cluster.restartFrontends()
                    } else {
                        cluster.restartBackends()
                    }
                    context.reconnectFe()
                    // FE restart leaves BE debug points intact. Release before the recovery load
                    // so the new request cannot inherit the old artificial delay.
                    GetDebugPoint().disableDebugPointForAllBEs(barrier)
                    lastLock = readLock()
                    long remainingMs = heldLock.expiration.toString().toLong() * 1000L - System.currentTimeMillis()
                    logger.info("After restart: oldLock=${heldLock}, currentLock=${lastLock}, " +
                            "oldLockRemainingMs=${Math.max(0L, remainingMs)}")

                    // Start the competing load immediately; waiting for the old lock first
                    // would hide a failure to recover while a stale lock is still present.
                    long recoveryStartMs = System.currentTimeMillis()
                    recovery = thread("bitmap-recovery-${token}") {
                        def result = stream("bitmap_recovery_${token}")
                        result.elapsedMs = System.currentTimeMillis() - recoveryStartMs
                        return result
                    }
                    if (background != null) {
                        backgroundResult = background.get(RECOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        logger.info("Interrupted load outcome: ${backgroundResult}")
                        awaitUntil(RECOVERY_TIMEOUT_SECONDS) {
                            def rows = sql_return_maparray("SHOW TRANSACTION WHERE LABEL='${backgroundLabel}'")
                            lastTxn = rows.isEmpty() ? null : rows[0]
                            lastTxn != null && lastTxn.TransactionStatus.toString() in ['VISIBLE', 'ABORTED']
                        }
                        if (backgroundResult.response?.Status?.toString()?.equalsIgnoreCase('Success')) {
                            assertEquals('VISIBLE', lastTxn.TransactionStatus.toString())
                        }
                    }
                    if (operation == "schema_change") {
                        // Do not silently accept CANCELLED: the schema job must recover and finish.
                        awaitUntil(RECOVERY_TIMEOUT_SECONDS) {
                            def jobs = sql_return_maparray("SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1")
                            lastJob = jobs.isEmpty() ? null : jobs[0]
                            assert lastJob?.State != 'CANCELLED' : "schema change cancelled: ${lastJob}"
                            lastJob?.State == 'FINISHED'
                        }
                    }
                    // Restart may consume part or all of the lease. Accept release, replacement,
                    // or expiry of the old owner; do not demand another fixed 10/30 seconds.
                    awaitUntil(RECOVERY_TIMEOUT_SECONDS) {
                        lastLock = readLock()
                        lastLock == null || lastLock.lock_id != heldLock.lock_id ||
                                lastLock.expiration.toString().toLong() <= System.currentTimeMillis().intdiv(1000)
                    }
                    try {
                        def result = recovery.get(RECOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        assertEquals('success', result.response?.Status?.toString()?.toLowerCase(),
                                "recovery load failed: ${result}")
                        assertTrue(result.elapsedMs < RECOVERY_TIMEOUT_SECONDS * 1000L,
                                "recovery load exceeded its completion budget: ${result}")
                        awaitUntil(30) {
                            def txns = sql_return_maparray("SHOW TRANSACTION WHERE LABEL='bitmap_recovery_${token}'")
                            txns.size() == 1 && txns[0].TransactionStatus == 'VISIBLE'
                        }
                    } finally {
                        if (!recovery.isDone()) {
                            recovery.cancel(true)
                        }
                    }
                    if (operation == "compaction") {
                        awaitUntil(RECOVERY_TIMEOUT_SECONDS) {
                            def status = compactionStatus()
                            assertEquals('success', status.status.toString().toLowerCase())
                            !status.run_status
                        }
                        // A BE restart kills the old in-memory compaction. Verify a fresh
                        // compaction can complete after recovery, rather than requiring its survival.
                        // Supply fresh deltas even if the original FE-restart compaction
                        // finished successfully. A successful submission alone is not completion.
                        for (int i = 0; i < 6; i++) {
                            sql "INSERT INTO ${tableName} VALUES (1, 'seed', 10)"
                        }
                        sql 'SYNC'
                        sql "SELECT SUM(CAST(score AS INT)) FROM ${tableName}"
                        def tabletStatus = {
                            httpJson("http://${backend.host}:${backend.httpPort}/api/compaction/show?tablet_id=${tabletId}")
                        }
                        def beforeRows = []
                        awaitUntil(30) {
                            beforeRows = tabletStatus().rowsets
                            beforeRows != null && beforeRows.size() >= 6
                        }
                        triggerCompaction()
                        awaitUntil(RECOVERY_TIMEOUT_SECONDS) {
                            def status = compactionStatus()
                            assertEquals('success', status.status.toString().toLowerCase())
                            !status.run_status && tabletStatus().rowsets.size() < beforeRows.size()
                        }
                    }
                    def rows = sql "SELECT id, name, CAST(score AS INT) FROM ${tableName} ORDER BY id"
                    assertEquals([[1, 'seed', 10], [2, 'seed', 20], [3, 'seed', 30],
                                  [4, 'seed', 40], [5, 'e', 90], [6, 'f', 100]].toString(), rows.toString())
                } finally {
                    logger.info("${operation}/${restartNode}: heldLock=${heldLock}, lastLock=${lastLock}, " +
                            "lastTxn=${lastTxn}, lastJob=${lastJob}, background=${backgroundResult}")
                    try {
                        GetDebugPoint().disableDebugPointForAllBEs(barrier)
                    } finally {
                        def cleanupErrors = []
                        [recovery, background].findAll { it != null }.each { task ->
                            try {
                                task.get(RECOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            } catch (Exception e) {
                                cleanupErrors << e
                            } finally {
                                if (!task.isDone()) {
                                    task.cancel(true)
                                }
                            }
                        }
                        assert cleanupErrors.isEmpty() : "background task cleanup failed: ${cleanupErrors}"
                    }
                }
            }
        }
    }
}
