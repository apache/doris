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

Suite.metaClass.be_get_compaction_status{ String ip, String port, String tablet_id  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status?tablet_id=%s", ip, port, tablet_id))
}

Suite.metaClass.be_get_overall_compaction_status{ String ip, String port  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status", ip, port))
}

Suite.metaClass.be_show_tablet_status{ String ip, String port, String tablet_id  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/show?tablet_id=%s", ip, port, tablet_id))
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

Suite.metaClass.be_run_binlog_compaction = { String ip, String port, String tablet_id  /* param */->
    return _be_run_compaction(ip, port, tablet_id, "binlog")
}

Suite.metaClass.be_run_full_compaction_by_table_id = { String ip, String port, String table_id  /* param */->
    return curl("POST", String.format("http://%s:%s/api/compaction/run?table_id=%s&compact_type=full", ip, port, table_id))
}

logger.info("Added 'be_run_full_compaction' function to Suite")
logger.info("Added 'be_run_binlog_compaction' function to Suite")
Suite.metaClass.trigger_and_wait_compaction = { String table_name, String compaction_type, int timeout_seconds=300, String[] ignored_errors=[] ->
    if (!(compaction_type in ["cumulative", "base", "full", "binlog"])) {
        throw new IllegalArgumentException("invalid compaction type: ${compaction_type}, supported types: cumulative, base, full, binlog")
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def tablets = sql_return_maparray """show tablets from ${table_name}"""
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
    def isIgnoredCompactionStatus = { status ->
        def status_lower = "${status}".toLowerCase()
        return ignored_errors.any { error -> status_lower.contains(error.toLowerCase()) }
    }
    def isNoopCompactionStatus = { type, status ->
        def status_lower = "${status}".toLowerCase()
        switch (type) {
            case "full":
                return status_lower.contains("no suitable version") ||
                        status_lower.contains("e-808") ||
                        status_lower.contains("e-2008")
            case "cumulative":
                return status_lower.contains("e-2000") &&
                        !status_lower.contains("job tablet busy")
            case "binlog":
                return status_lower.contains("e-2012")
            case "base":
                return false
            default:
                return false
        }
    }
    // 2. trigger compaction
    def triggered_tablets = []
    for (tablet in tablets) {
        def be_host = backendId_to_backendIP["${tablet.BackendId}"]
        def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]
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
            case "binlog":
                (exit_code, stdout, stderr) = be_run_binlog_compaction(be_host, be_port, tablet.TabletId)
                break
        }
        assert exit_code == 0: "trigger compaction failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
        def trigger_status = parseJson(stdout.trim())
        if (trigger_status.status.toLowerCase() != "success") {
            def status_lower = trigger_status.status.toLowerCase()
            if (status_lower == "already_exist") {
                triggered_tablets.add(tablet) // compaction already in queue, treat it as successfully triggered
            } else if (!auto_compaction_disabled) {
                // ignore the error if auto compaction enabled
            } else if (status_lower.contains("e-2010")) {
                // cumulative compaction handed delete-version rowsets to base compaction, so still wait below.
                triggered_tablets.add(tablet)
            } else if (isNoopCompactionStatus(compaction_type, trigger_status.status)) {
                // ignore this tablet compaction.
            } else if (isIgnoredCompactionStatus(trigger_status.status)) {
                // ignore this tablet compaction if the error is in the ignored_errors list
            } else {
                throw new Exception("trigger compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${trigger_status.status}")
            }
        } else {
            triggered_tablets.add(tablet)
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
    Awaitility.await().atMost(timeout_seconds, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until(() -> {
        for (tablet in triggered_tablets) {
            def be_host = backendId_to_backendIP["${tablet.BackendId}"]
            def be_port = backendId_to_backendHttpPort["${tablet.BackendId}"]

            (exit_code, stdout, stderr) = be_get_compaction_status(be_host, be_port, tablet.TabletId)
            assert exit_code == 0: "get compaction status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
            def compactionStatus = parseJson(stdout.trim())
            assert compactionStatus.status.toLowerCase() == "success": "compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${compactionStatus.status}"
            // running is true means compaction is still running
            running = compactionStatus.run_status

            if (!is_time_series_compaction) {
                (exit_code, stdout, stderr) = be_show_tablet_status(be_host, be_port, tablet.TabletId)
                assert exit_code == 0: "get tablet status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
                def tabletStatus = parseJson(stdout.trim())
                def oldStatus = be_tablet_compaction_status.get("${be_host}-${tablet.TabletId}")
                // last compaction success/failure time isn't updated, indicates compaction is not started(so we treat it as running and wait)
                def handedOffToBaseCompactionAfterDeleteVersion = false
                def completedByBaseCompactionAfterDeleteVersion = false
                if (compaction_type == "cumulative") {
                    def oldCumulativePoint = toLongOrNull(oldStatus["cumulative point"])
                    def newCumulativePoint = toLongOrNull(tabletStatus["cumulative point"])
                    def lastCumulativeStatus = "${tabletStatus["last cumulative status"]}".toLowerCase()
                    def baseSuccessTimeChanged = oldStatus["last base success time"] != tabletStatus["last base success time"]
                    def cumulativeSuccessTimeChanged =
                            oldStatus["last cumulative success time"] != tabletStatus["last cumulative success time"]
                    // E-2010 advances the cumulative point and delegates delete-version rowsets to base compaction.
                    // The cumulative request itself is complete once the handoff is visible; callers that need the
                    // delegated work compacted should explicitly trigger base compaction after this helper returns.
                    handedOffToBaseCompactionAfterDeleteVersion = lastCumulativeStatus.contains("e-2010") &&
                            oldCumulativePoint != null && newCumulativePoint != null &&
                            newCumulativePoint > oldCumulativePoint
                    completedByBaseCompactionAfterDeleteVersion =
                            handedOffToBaseCompactionAfterDeleteVersion &&
                            (baseSuccessTimeChanged || cumulativeSuccessTimeChanged)
                }
                def success_time_unchanged = (oldStatus["last ${compaction_type} success time"] == tabletStatus["last ${compaction_type} success time"])
                def failure_time_unchanged = (oldStatus["last ${compaction_type} failure time"] == tabletStatus["last ${compaction_type} failure time"])
                def cumulative_completion_count_unchanged = compaction_type != "cumulative" ||
                        oldStatus["cumulative compaction completed count"] ==
                                tabletStatus["cumulative compaction completed count"]
                def currentCompactionStatus = tabletStatus["last ${compaction_type} status"]
                def statusOk = "${currentCompactionStatus}".toLowerCase().contains("[ok]")
                def terminalStatusChanged = !failure_time_unchanged ||
                        !cumulative_completion_count_unchanged
                def compactionFailureNonFatal = terminalStatusChanged &&
                        (isNoopCompactionStatus(compaction_type, currentCompactionStatus) ||
                                isIgnoredCompactionStatus(currentCompactionStatus))
                def baseFailureTimeChanged = handedOffToBaseCompactionAfterDeleteVersion &&
                        oldStatus["last base failure time"] != tabletStatus["last base failure time"]
                def baseFailureIgnored = baseFailureTimeChanged && isIgnoredCompactionStatus(tabletStatus["last base status"])
                if (!running && !handedOffToBaseCompactionAfterDeleteVersion &&
                        terminalStatusChanged && !statusOk && !compactionFailureNonFatal) {
                    throw new Exception("compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, " +
                            "run status: ${compactionStatus.run_status}, old status: ${oldStatus}, new status: ${tabletStatus}")
                }
                if (!running && handedOffToBaseCompactionAfterDeleteVersion && baseFailureTimeChanged &&
                        !baseFailureIgnored) {
                    throw new Exception("base compaction failed after cumulative E-2010 handoff, be host: ${be_host}, " +
                            "tablet id: ${tablet.TabletId}, run status: ${compactionStatus.run_status}, " +
                            "old status: ${oldStatus}, new status: ${tabletStatus}")
                }
                def compactionFinished = handedOffToBaseCompactionAfterDeleteVersion ||
                        completedByBaseCompactionAfterDeleteVersion ||
                        compactionFailureNonFatal || baseFailureIgnored ||
                        !success_time_unchanged ||
                        (!cumulative_completion_count_unchanged && statusOk)
                running = running || !compactionFinished
                if (running) {
                    logger.info("compaction is still running, be host: ${be_host}, tablet id: ${tablet.TabletId}, run status: ${compactionStatus.run_status}, old status: ${oldStatus}, new status: ${tabletStatus}")
                    return false
                }
            } else {
                // time series compaction sometimes doesn't update compaction success time
                // so use the completed count as the terminal signal after run_status becomes false.
                if (running) {
                    logger.info("compaction is still running, be host: ${be_host}, tablet id: ${tablet.TabletId}")
                    return false
                }
                (exit_code, stdout, stderr) = be_show_tablet_status(be_host, be_port, tablet.TabletId)
                assert exit_code == 0: "get tablet status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
                def tabletStatus = parseJson(stdout.trim())
                def oldStatus = be_tablet_compaction_status.get("${be_host}-${tablet.TabletId}")
                def handedOffToBaseCompactionAfterDeleteVersion = false
                def completedByBaseCompactionAfterDeleteVersion = false
                def cumulativePointChanged = false
                if (compaction_type == "cumulative") {
                    def oldCumulativePoint = toLongOrNull(oldStatus["cumulative point"])
                    def newCumulativePoint = toLongOrNull(tabletStatus["cumulative point"])
                    def lastCumulativeStatus = "${tabletStatus["last cumulative status"]}".toLowerCase()
                    def baseSuccessTimeChanged = oldStatus["last base success time"] != tabletStatus["last base success time"]
                    def cumulativeSuccessTimeChanged =
                            oldStatus["last cumulative success time"] != tabletStatus["last cumulative success time"]
                    cumulativePointChanged = oldCumulativePoint != null && newCumulativePoint != null &&
                            newCumulativePoint > oldCumulativePoint
                    handedOffToBaseCompactionAfterDeleteVersion =
                            lastCumulativeStatus.contains("e-2010") && cumulativePointChanged
                    completedByBaseCompactionAfterDeleteVersion =
                            handedOffToBaseCompactionAfterDeleteVersion &&
                            (baseSuccessTimeChanged || cumulativeSuccessTimeChanged)
                }
                def success_time_unchanged = (oldStatus["last ${compaction_type} success time"] == tabletStatus["last ${compaction_type} success time"])
                def failure_time_unchanged = (oldStatus["last ${compaction_type} failure time"] == tabletStatus["last ${compaction_type} failure time"])
                def cumulative_completion_count_unchanged = compaction_type != "cumulative" ||
                        oldStatus["cumulative compaction completed count"] ==
                                tabletStatus["cumulative compaction completed count"]
                def currentCompactionStatus = tabletStatus["last ${compaction_type} status"]
                def statusOk = "${currentCompactionStatus}".toLowerCase().contains("[ok]")
                def terminalStatusChanged = !failure_time_unchanged ||
                        !cumulative_completion_count_unchanged
                def compactionFailureNonFatal = terminalStatusChanged &&
                        (isNoopCompactionStatus(compaction_type, currentCompactionStatus) ||
                                isIgnoredCompactionStatus(currentCompactionStatus))
                def baseFailureTimeChanged = handedOffToBaseCompactionAfterDeleteVersion &&
                        oldStatus["last base failure time"] != tabletStatus["last base failure time"]
                def baseFailureIgnored = baseFailureTimeChanged && isIgnoredCompactionStatus(tabletStatus["last base status"])
                if (!handedOffToBaseCompactionAfterDeleteVersion &&
                        terminalStatusChanged && !statusOk && !compactionFailureNonFatal) {
                    throw new Exception("compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, " +
                            "run status: ${compactionStatus.run_status}, old status: ${oldStatus}, new status: ${tabletStatus}")
                }
                if (handedOffToBaseCompactionAfterDeleteVersion && baseFailureTimeChanged &&
                        !baseFailureIgnored) {
                    throw new Exception("base compaction failed after cumulative E-2010 handoff, be host: ${be_host}, " +
                            "tablet id: ${tablet.TabletId}, run status: ${compactionStatus.run_status}, " +
                            "old status: ${oldStatus}, new status: ${tabletStatus}")
                }
                def compactionFinished = handedOffToBaseCompactionAfterDeleteVersion ||
                        completedByBaseCompactionAfterDeleteVersion ||
                        compactionFailureNonFatal || baseFailureIgnored ||
                        (!handedOffToBaseCompactionAfterDeleteVersion &&
                                (!success_time_unchanged ||
                                        (!cumulative_completion_count_unchanged && statusOk)))
                running = !compactionFinished
                if (running) {
                    logger.info("compaction is still running, be host: ${be_host}, tablet id: ${tablet.TabletId}, run status: ${compactionStatus.run_status}, old status: ${oldStatus}, new status: ${tabletStatus}")
                    return false
                }
            }
        }
        return true
    })

    assert !running: "wait compaction timeout, be host: ${be_host}"
}
