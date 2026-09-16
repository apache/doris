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

import org.apache.doris.regression.suite.Suite
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility;

Suite.metaClass.be_get_compaction_status{ String ip, String port, String tablet_id,
                                          Integer timeout_sec = 10, Integer max_retries = 10  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status?tablet_id=%s", ip, port, tablet_id),
            null, timeout_sec, context.config.feHttpUser, context.config.feHttpPassword, max_retries)
}

Suite.metaClass.be_get_overall_compaction_status{ String ip, String port  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status", ip, port))
}

Suite.metaClass.be_show_tablet_status{ String ip, String port, String tablet_id,
                                      Integer timeout_sec = 10, Integer max_retries = 10  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/show?tablet_id=%s", ip, port, tablet_id),
            null, timeout_sec, context.config.feHttpUser, context.config.feHttpPassword, max_retries)
}

Suite.metaClass._be_run_compaction = { String ip, String port, String tablet_id, String compact_type ->
    return curl("POST", String.format("http://%s:%s/api/compaction/run?tablet_id=%s&compact_type=%s",
            ip, port, tablet_id, compact_type))
}

Suite.metaClass.be_run_base_compaction = { String ip, String port, String tablet_id  /* param */->
    return _be_run_compaction(ip, port, tablet_id, "base")
}

logger.info("Added 'be_run_base_compaction' function to Suite")

Suite.metaClass.be_run_cumulative_compaction = { String ip, String port, String tablet_id  /* param */->
    return _be_run_compaction(ip, port, tablet_id, "cumulative")
}

logger.info("Added 'be_run_cumulative_compaction' function to Suite")

Suite.metaClass.be_run_full_compaction = { String ip, String port, String tablet_id  /* param */->
    return _be_run_compaction(ip, port, tablet_id, "full")
}

Suite.metaClass.be_run_full_compaction_by_table_id = { String ip, String port, String table_id  /* param */->
    return curl("POST", String.format("http://%s:%s/api/compaction/run?table_id=%s&compact_type=full", ip, port, table_id))
}

logger.info("Added 'be_run_full_compaction' function to Suite")
Suite.metaClass.trigger_and_wait_compaction = { String table_name, String compaction_type, int timeout_seconds=300,
                                                String[] ignored_errors=[], Collection tablet_ids=[],
                                                String[] retryable_errors=[] ->
    if (!(compaction_type in ["cumulative", "base", "full"])) {
        throw new IllegalArgumentException("invalid compaction type: ${compaction_type}, supported types: cumulative, base, full")
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def tablets = sql_return_maparray """show tablets from ${table_name}"""
    if (!tablet_ids.isEmpty()) {
        def requestedTabletIds = tablet_ids.collect { "${it}" }.toSet()
        tablets = tablets.findAll { requestedTabletIds.contains("${it.TabletId}") }
        def foundTabletIds = tablets.collect { "${it.TabletId}" }.toSet()
        assert foundTabletIds == requestedTabletIds:
                "Unable to find all requested tablets for ${table_name}, requested: ${requestedTabletIds}, found: ${foundTabletIds}"
    }
    def exit_code, stdout, stderr

    def auto_compaction_disabled = sql("show create table ${table_name}")[0][1].contains('"disable_auto_compaction" = "true"')
    def is_time_series_compaction = sql("show create table ${table_name}")[0][1].contains('"compaction_policy" = "time_series"')

    // 1. cache compaction status
    def be_tablet_compaction_status = [:]
    for (tablet in tablets) {
        def be_host = backendId_to_backendIP["${tablet.BackendId}"]
        def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]
        (exit_code, stdout, stderr) = be_show_tablet_status(be_host, be_port, tablet.TabletId)
        assert exit_code == 0: "get tablet status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"

        def tabletStatus = parseJson(stdout.trim())
        be_tablet_compaction_status.put("${be_host}-${tablet.TabletId}", tabletStatus)
    }
    // 2. trigger compaction
    def triggered_tablets = []
    for (tablet in tablets) {
        def be_host = backendId_to_backendIP["${tablet.BackendId}"]
        def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]
        long triggerDeadline = System.currentTimeMillis() + timeout_seconds * 1000L
        while (true) {
            switch (compaction_type) {
                case "cumulative":
                    (exit_code, stdout, stderr) = be_run_cumulative_compaction(be_host, be_port, tablet.TabletId)
                    break
                case "base":
                    (exit_code, stdout, stderr) = be_run_base_compaction(be_host, be_port, tablet.TabletId)
                    break
                case "full":
                    (exit_code, stdout, stderr) = be_run_full_compaction(be_host, be_port, tablet.TabletId)
                    break
            }
            assert exit_code == 0: "trigger compaction failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
            def trigger_status = parseJson(stdout.trim())
            def status_lower = trigger_status.status.toLowerCase()
            if (status_lower == "success" || status_lower == "already_exist") {
                triggered_tablets.add(tablet)
                break
            } else if (retryable_errors.any { error -> status_lower.contains(error.toLowerCase()) }) {
                assert System.currentTimeMillis() < triggerDeadline:
                        "retry trigger compaction timeout, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${trigger_status.status}"
                logger.info("retry transient compaction trigger failure, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${trigger_status.status}")
                // The transient trigger failure updates the tablet's failure timestamp.
                // Refresh the baseline so it cannot be mistaken for completion of the
                // later successful asynchronous trigger.
                (exit_code, stdout, stderr) = be_show_tablet_status(be_host, be_port, tablet.TabletId)
                assert exit_code == 0:
                        "refresh tablet status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
                be_tablet_compaction_status.put("${be_host}-${tablet.TabletId}", parseJson(stdout.trim()))
                sleep(1000)
                continue
            } else if (!auto_compaction_disabled) {
                // ignore the error if auto compaction enabled
            } else if (status_lower.contains("e-2000") || status_lower.contains("e-2010")
                    || status_lower.contains("e-808")) {
                // ignore this tablet compaction.
                // e-2000/e-2010: cumulative has no suitable version;
                // e-808 (BE_NO_SUITABLE_VERSION): base compaction has nothing to merge on this
                // replica (e.g. only [0-1]+[2-y] with an empty [0-1]) — a by-design no-op, the
                // base analogue of e-2000. Replica layouts can legitimately diverge here when a
                // lagging publish made an earlier cumulative trigger a no-op on one replica.
            } else if (ignored_errors.any { error -> status_lower.contains(error.toLowerCase()) }) {
                // ignore this tablet compaction if the error is in the ignored_errors list
            } else {
                throw new Exception("trigger compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${trigger_status.status}")
            }
            break
        }
    }

    // 3. wait all compaction finished
    def running = triggered_tablets.size() > 0
    def toLongOrNull = { value ->
        if (value == null) {
            return null
        }
        try {
            return value.toString().trim().toLong()
        } catch (Throwable ignored) {
            return null
        }
    }
    // Parallel suites own their worker failures; do not capture uncaught exceptions from other suites.
    Awaitility.await().dontCatchUncaughtExceptions().atMost(timeout_seconds, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
        for (tablet in triggered_tablets) {
            def be_host = backendId_to_backendIP["${tablet.BackendId}"]
            def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]

            // Awaitility owns the retry loop. Keep each HTTP probe bounded so an
            // inner curl retry cannot consume the entire compaction timeout.
            (exit_code, stdout, stderr) = be_get_compaction_status(be_host, be_port, tablet.TabletId, 5, 1)
            if (exit_code != 0) {
                logger.warn("get compaction status failed, will retry, be host: ${be_host}, tablet id: ${tablet.TabletId}, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}")
                return false
            }
            def compactionStatus = parseJson(stdout.trim())
            assert compactionStatus.status.toLowerCase() == "success": "compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${compactionStatus.status}"
            // running is true means compaction is still running
            running = compactionStatus.run_status

            if (!is_time_series_compaction) {
                (exit_code, stdout, stderr) = be_show_tablet_status(be_host, be_port, tablet.TabletId, 5, 1)
                if (exit_code != 0) {
                    logger.warn("get tablet status failed, will retry, be host: ${be_host}, tablet id: ${tablet.TabletId}, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}")
                    return false
                }
                def tabletStatus = parseJson(stdout.trim())
                def oldStatus = be_tablet_compaction_status.get("${be_host}-${tablet.TabletId}")
                // The HTTP trigger is asynchronous in cloud mode. run_status may already be
                // false at the first poll, so also require a completion marker for this run.
                def handedOffToBaseCompactionAfterDeleteVersion = false
                def completedByBaseCompactionAfterDeleteVersion = false
                if (compaction_type == "cumulative") {
                    def oldCumulativePoint = toLongOrNull(oldStatus["cumulative point"])
                    def newCumulativePoint = toLongOrNull(tabletStatus["cumulative point"])
                    def lastCumulativeStatus = "${tabletStatus["last cumulative status"]}".toLowerCase()
                    def baseSuccessTimeChanged = oldStatus["last base success time"] != tabletStatus["last base success time"]
                    def cumulativeSuccessTimeChanged =
                            oldStatus["last cumulative success time"] != tabletStatus["last cumulative success time"]
                    handedOffToBaseCompactionAfterDeleteVersion = lastCumulativeStatus.contains("e-2010") &&
                            oldCumulativePoint != null && newCumulativePoint != null &&
                            newCumulativePoint > oldCumulativePoint
                    completedByBaseCompactionAfterDeleteVersion =
                            handedOffToBaseCompactionAfterDeleteVersion &&
                            (baseSuccessTimeChanged || cumulativeSuccessTimeChanged)
                }
                def successTimeUnchanged = oldStatus["last ${compaction_type} success time"] ==
                        tabletStatus["last ${compaction_type} success time"]
                def failureTimeUnchanged = oldStatus["last ${compaction_type} failure time"] ==
                        tabletStatus["last ${compaction_type} failure time"]
                def completionTimestampChanged = !successTimeUnchanged || !failureTimeUnchanged
                def compactionFinished = completedByBaseCompactionAfterDeleteVersion ||
                        (!handedOffToBaseCompactionAfterDeleteVersion && completionTimestampChanged)
                running = running || !compactionFinished
                if (running) {
                    logger.info("compaction is still running, be host: ${be_host}, tablet id: ${tablet.TabletId}, run status: ${compactionStatus.run_status}, old status: ${oldStatus}, new status: ${tabletStatus}")
                    return false
                }
            } else {
                // Time series compaction sometimes doesn't update compaction success time,
                // so solely check run_status for it.
                if (running) {
                    logger.info("compaction is still running, be host: ${be_host}, tablet id: ${tablet.TabletId}")
                    return false
                }
            }
        }
        return true
    })

    assert !running: "wait compaction timeout, be host: ${be_host}"
}
