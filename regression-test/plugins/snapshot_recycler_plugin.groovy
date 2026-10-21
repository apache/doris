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
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.suite.SuiteCluster

def recyclerRequest = { Suite suite, String address, String method, String path, String payload = null ->
    String out
    suite.httpTest {
        endpoint address
        uri "/RecyclerService/http/${path}".toString()
        if (method == 'GET') {
            op 'get'
        } else {
            body payload
        }
        check { code, response ->
            assert code == 200 : "Recycler request failed: path=${path}, status=${code}, body=${response}"
            assert response?.trim() : "Empty recycler response: path=${path}"
            out = response.trim()
        }
    }
    return out
}

// The snapshot Docker suites set both scheduling intervals to 10 seconds.
Suite.metaClass.recycleAndCheckSnapshot = { String recyclerAddress, String instanceId,
                                           int maxRounds = 10, int maxPolls = 60,
                                           boolean requireCheck = true ->
    Suite suite = delegate as Suite
    assert maxRounds > 0 && maxPolls > 0
    String token = "greedisgood9999"
    String encodedInstanceId = URLEncoder.encode(instanceId, 'UTF-8')
    def lastJobs = [:]

    def request = { String method, String path, String payload = null ->
        // A successful trigger only enqueues a job.
        return recyclerRequest(suite, recyclerAddress, method, path, payload)
    }
    def waitJob = { String kind, Closure ready ->
        for (int poll = 0; poll < maxPolls; poll++) {
            def info = new JsonSlurper().parseText(request('GET',
                    "${kind}_job_info?token=${token}&instance_id=${encodedInstanceId}".toString()))
            assert info.status in ['IDLE', 'BUSY'] : "Invalid ${kind} job: instance=${instanceId}, ${info}"
            lastJobs[kind] = info
            if (ready(info)) return info
            suite.sleep(1000L)
        }
        assert false : "Timed out waiting for ${kind}: instance=${instanceId}, jobs=${lastJobs}"
    }
    def runJob = { String kind ->
        def before = waitJob(kind) { info ->
            // Manual requests also obey the scheduling interval. These suites disable automatic
            // scheduling with recycle_whitelist=dummy, so a rejected trigger would never run.
            info.status == 'IDLE' && System.currentTimeMillis() -
                    (info.last_ctime_ms ?: '0').toString().toLong() > 10000L
        }
        long previousFinish = (before.last_finish_time_ms ?: '0').toString().toLong()
        String response = kind == 'recycle'
                ? request('POST', "recycle_instance?token=${token}".toString(),
                        JsonOutput.toJson([instance_ids: [instanceId]]))
                : request('GET', "check_instance?token=${token}&instance_id=${encodedInstanceId}".toString())
        assert response.equalsIgnoreCase('OK') : "Failed to trigger ${kind}: instance=${instanceId}, ${response}"
        def finished = waitJob(kind) { info ->
            info.status == 'IDLE' &&
                    (info.last_finish_time_ms ?: '0').toString().toLong() > previousFinish
        }
        return finished.last_success_time_ms != null &&
                finished.last_finish_time_ms.toString().toLong() == finished.last_success_time_ms.toString().toLong()
    }

    for (int round = 1; round <= maxRounds; round++) {
        suite.getLogger().info("Starting snapshot recycle/check: instance=${instanceId}, round=${round}/${maxRounds}")
        boolean recycled = runJob('recycle')
        // Check cloned instances through SQL below: their inherited snapshot files do not
        // have snapshot keys in the derived instance, so the recycler checker rejects them.
        boolean checked = !requireCheck || runJob('check')
        suite.getLogger().info("Snapshot recycle/check: instance=${instanceId}, round=${round}/${maxRounds}, "
                + "recycled=${recycled}, checked=${checked}, checkRequired=${requireCheck}, jobs=${lastJobs}")
        if (recycled && checked) return
    }
    assert false : "Recycle/check failed after ${maxRounds} rounds: instance=${instanceId}, jobs=${lastJobs}"
}

// Used only inside bounded waits for snapshot expiry or complete instance deletion.
// A fully deleted instance has no checker job to run; callers assert its metadata disappears.
Suite.metaClass.triggerSnapshotRecycleWhenIdle = { String recyclerAddress, String instanceId ->
    Suite suite = delegate as Suite
    String token = "greedisgood9999"
    String encodedInstanceId = URLEncoder.encode(instanceId, 'UTF-8')
    String out = recyclerRequest(suite, recyclerAddress, 'GET',
            "recycle_job_info?token=${token}&instance_id=${encodedInstanceId}".toString())
    def info = new JsonSlurper().parseText(out)
    assert info.status in ['IDLE', 'BUSY'] : "Invalid recycle job for ${instanceId}: ${info}"
    if (info.status != 'IDLE' || System.currentTimeMillis() -
            (info.last_ctime_ms ?: '0').toString().toLong() <= 10000L) return false
    String triggerOut = recyclerRequest(suite, recyclerAddress, 'POST',
            "recycle_instance?token=${token}".toString(), JsonOutput.toJson([instance_ids: [instanceId]]))
    assert triggerOut.equalsIgnoreCase('OK') : "Failed to trigger recycle for ${instanceId}: ${triggerOut}"
    return true
}

// Record all remaining OLAP rows before recycling, then read them again without caches.
// Callers pass the current instance IDs after rollback and omit clusters whose creation failed.
// A null cluster allows checking a predecessor that still owns snapshots but has no running FE.
Suite.metaClass.checkSnapshotRecycle = { SuiteCluster recyclerCluster, Map instanceClusters ->
    Suite suite = delegate as Suite
    assert !instanceClusters.isEmpty()
    def recyclers = recyclerCluster.getAllRecyclers(true)
    assert !recyclers.isEmpty() : "No live recycler in ${recyclerCluster.name}"
    def (ip, port) = recyclers[0].getHttpAddress()
    String recyclerAddress = "${ip}:${port}"
    def readData = { SuiteCluster dataCluster ->
        def data = [:]
        suite.connectWithDockerCluster(dataCluster) {
            def tables = suite.sql("""
                SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables
                WHERE ENGINE = 'Doris' AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', '__internal_schema')
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            tables.each { table ->
                String name = table.collect { '`' + it.toString().replace('`', '``') + '`' }.join('.')
                def rows = suite.sql("""SELECT /*+ SET_VAR(enable_sql_cache=false,
                        enable_query_cache=false, enable_file_cache=false, enable_page_cache=false) */ * FROM ${name}""")
                // Sort serialized rows instead of using a set: duplicate multiplicities must survive.
                data[name] = rows.collect { JsonOutput.toJson(it) }.sort()
            }
        }
        return data
    }
    def expected = [:]
    instanceClusters.each { instanceId, dataCluster ->
        if (dataCluster != null) {
            expected[instanceId] = readData(dataCluster)
            suite.getLogger().info("Snapshot data baseline: instance=${instanceId}, tables=${expected[instanceId].size()}, "
                    + "rows=${expected[instanceId].values().sum { it.size() } ?: 0}")
        }
    }
    instanceClusters.each { instanceId, dataCluster ->
        if (dataCluster != null && dataCluster != recyclerCluster) {
            suite.recycleAndCheckSnapshot(recyclerAddress, instanceId.toString(), 10, 60, false)
        } else {
            suite.recycleAndCheckSnapshot(recyclerAddress, instanceId.toString())
        }
    }
    instanceClusters.each { instanceId, dataCluster ->
        if (dataCluster != null) {
            def actual = readData(dataCluster)
            assert actual == expected[instanceId] : "Data changed after recycling: instance=${instanceId}, " +
                    "expected=${expected[instanceId]}, actual=${actual}"
        }
    }
}

logger.info("Added snapshot recycle/check helpers to Suite")
