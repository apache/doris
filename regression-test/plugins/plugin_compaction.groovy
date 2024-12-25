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

Suite.metaClass.be_run_full_compaction_by_table_id = { String ip, String port, String table_id  /* param */->
    return curl("POST", String.format("http://%s:%s/api/compaction/run?table_id=%s&compact_type=full", ip, port, table_id))
}

logger.info("Added 'be_run_full_compaction' function to Suite")

Suite.metaClass.trigger_and_wait_compaction = { String table_name, String compaction_type, int timeout_seconds=300 ->
    if (!(compaction_type in ["cumulative", "base", "full"])) {
        throw new IllegalArgumentException("invalid compaction type: ${compaction_type}, supported types: cumulative, base, full")
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
        }
        assert exit_code == 0: "trigger compaction failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
        def trigger_status = parseJson(stdout.trim())
        if (trigger_status.status.toLowerCase() != "success") {
            if (trigger_status.status.toLowerCase() == "already_exist") {
                triggered_tablets.add(tablet) // compaction already in queue, treat it as successfully triggered
            } else if (!auto_compaction_disabled) {
                // ignore the error if auto compaction enabled
            } else {
                throw new Exception("trigger compaction failed, be host: ${be_host}, tablet id: ${tablet.TabletId}, status: ${trigger_status.status}")
            }
        } else {
            triggered_tablets.add(tablet)
        }
    }

    // 3. wait all compaction finished
    def running = triggered_tablets.size() > 0
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

            if (!isCloudMode() && !is_time_series_compaction) {
                (exit_code, stdout, stderr) = be_show_tablet_status(be_host, be_port, tablet.TabletId)
                assert exit_code == 0: "get tablet status failed, exit code: ${exit_code}, stdout: ${stdout}, stderr: ${stderr}"
                def tabletStatus = parseJson(stdout.trim())
                def oldStatus = be_tablet_compaction_status.get("${be_host}-${tablet.TabletId}")
                // last compaction success time isn't updated, indicates compaction is not started(so we treat it as running and wait)
                running = running || (oldStatus["last ${compaction_type} success time"] == tabletStatus["last ${compaction_type} success time"])
                if (running) {
                    logger.info("compaction is still running, be host: ${be_host}, tablet id: ${tablet.TabletId}, run status: ${compactionStatus.run_status}, old status: ${oldStatus}, new status: ${tabletStatus}")
                    return false
                }
            } else {
                // 1. cloud mode doesn't show compaction success time in tablet status for the time being,
                // 2. time series compaction sometimes doesn't update compaction success time
                // so we solely check run_status for these two cases
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
