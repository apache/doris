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
import org.apache.doris.regression.suite.ClusterOptions

// Run with a cloud image containing the checker debug points below. For a pre-fix
// reproduction, keep the debug points but revert the reconciliation changes.
suite('test_compute_group_name_reconciliation', 'cloud_p0,docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs += ['cloud_cluster_check_interval_second=1', 'heartbeat_interval_second=1']

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msEndpoint = "${ms.host}:${ms.httpPort}"
        def feLog = new File(cluster.getFeByIndex(1).getLogFilePath())
        def physicalPause = 'CloudClusterChecker.checkCloudBackends.pause'
        def instancePause = 'CloudInstanceStatusChecker.runAfterCatalogReady.pause'
        def stalePause = 'CloudInstanceStatusChecker.afterGetInstance.pause'
        def debug = GetDebugPoint()

        def alterCluster = { operation, group ->
            httpTest {
                endpoint msEndpoint
                uri "/MetaService/http/${operation}?token=${token}"
                body JsonOutput.toJson([instance_id: 'default_instance_id', cluster: group])
                check { code, response ->
                    def result = parseJson(response)
                    assertTrue(code == 200 && result.code == 'OK', "${operation}: ${response}")
                }
            }
        }
        def remoteGroup = { name ->
            def group = get_instance(ms).clusters.find { it.cluster_name == name }
            assertNotNull(group, "remote compute group ${name}")
            group
        }
        def pause = { point, phase, params = [:] ->
            debug.enableDebugPointForAllFEs(point, params + [phase: phase, timeout: '180'])
            // A unique phase proves this cycle reached the pause; no timing guesses.
            awaitUntil(30) { feLog.text.contains("${point} phase=${phase}") }
        }
        def resume = { point -> debug.disableDebugPointForAllFEs(point) }
        def waitPhysical = { name, id, removedIds ->
            awaitUntil(60) {
                def bes = sql_return_maparray('SHOW BACKENDS')
                def tags = bes.collect { parseJson(it.Tag) }
                def groups = sql_return_maparray('SHOW COMPUTE GROUPS')
                tags.any { it.compute_group_id == id && it.compute_group_name == name } &&
                        !tags.any { removedIds.contains(it.compute_group_id) } &&
                        groups.any { it.Name == name && it.BackendNum.toInteger() == 1 }
            }
            sql "USE @${name}"
        }

        try {
            cluster.addBackend(1, 'cg_reused')
            cluster.addBackend(1, 'cg_spare')
            def old = remoteGroup('cg_reused')
            def spare = remoteGroup('cg_spare')
            waitPhysical('cg_reused', old.cluster_id, [])
            waitPhysical('cg_spare', spare.cluster_id, [])
            sql 'USE @compute_cluster'
            sql 'DROP TABLE IF EXISTS test_compute_group_name_reconciliation'
            sql '''CREATE TABLE test_compute_group_name_reconciliation (k INT)
                   DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')'''
            sql 'INSERT INTO test_compute_group_name_reconciliation VALUES (1), (2), (3)'

            // 1. A single checker cycle sees a same-name replacement on distinct BEs.
            pause(physicalPause, 'recreate')
            alterCluster('drop_cluster', [cluster_id: old.cluster_id, cluster_name: 'cg_reused'])
            alterCluster('drop_cluster', [cluster_id: spare.cluster_id, cluster_name: 'cg_spare'])
            alterCluster('add_cluster', [cluster_id: 'cg_recreated_id', cluster_name: 'cg_reused',
                    type: 'COMPUTE', nodes: spare.nodes])
            resume(physicalPause)
            waitPhysical('cg_reused', 'cg_recreated_id', [old.cluster_id, spare.cluster_id])
            order_qt_recreated 'SELECT k FROM test_compute_group_name_reconciliation'

            // 2. Rename A and reuse its old name for B before the next checker cycle.
            pause(physicalPause, 'rename')
            alterCluster('rename_cluster', [cluster_id: 'cg_recreated_id', cluster_name: 'cg_renamed'])
            alterCluster('add_cluster', [cluster_id: 'cg_reuse_after_rename_id', cluster_name: 'cg_reused',
                    type: 'COMPUTE', nodes: old.nodes])
            resume(physicalPause)
            waitPhysical('cg_renamed', 'cg_recreated_id', [old.cluster_id, spare.cluster_id])
            order_qt_renamed 'SELECT k FROM test_compute_group_name_reconciliation'
            waitPhysical('cg_reused', 'cg_reuse_after_rename_id', [old.cluster_id, spare.cluster_id])
            order_qt_reused 'SELECT k FROM test_compute_group_name_reconciliation'

            // 3. Hold an old virtual-group response while the physical checker installs
            // a newer same-name physical group. Then let the old response overwrite it.
            cluster.addBackend(1, 'cg_snapshot_spare')
            def snapshotSpare = remoteGroup('cg_snapshot_spare')
            waitPhysical('cg_snapshot_spare', snapshotSpare.cluster_id, [])
            sql 'USE @compute_cluster'
            pause(instancePause, 'before_virtual')
            alterCluster('add_cluster', [cluster_id: 'cg_stale_virtual_id', cluster_name: 'cg_snapshot',
                    type: 'VIRTUAL', cluster_names: ['compute_cluster', 'cg_renamed'],
                    cluster_policy: [type: 'ActiveStandby', active_cluster_name: 'compute_cluster',
                            standby_cluster_names: ['cg_renamed']]])
            debug.enableDebugPointForAllFEs(stalePause,
                    [cluster_id: 'cg_stale_virtual_id', phase: 'stale_response', timeout: '180'])
            resume(instancePause)
            awaitUntil(30) { feLog.text.contains("${stalePause} phase=stale_response") }
            alterCluster('drop_cluster', [cluster_id: 'cg_stale_virtual_id', cluster_name: 'cg_snapshot'])
            alterCluster('drop_cluster', [cluster_id: snapshotSpare.cluster_id, cluster_name: 'cg_snapshot_spare'])
            alterCluster('add_cluster', [cluster_id: 'cg_current_physical_id', cluster_name: 'cg_snapshot',
                    type: 'COMPUTE', nodes: snapshotSpare.nodes])
            waitPhysical('cg_snapshot', 'cg_current_physical_id', [snapshotSpare.cluster_id])
            sql 'USE @compute_cluster'
            pause(physicalPause, 'hold_repair')
            debug.enableDebugPointForAllFEs(instancePause, [phase: 'after_stale', timeout: '180'])
            resume(stalePause)
            awaitUntil(30) { feLog.text.contains("${instancePause} phase=after_stale") }
            awaitUntil(30) {
                sql_return_maparray('SHOW COMPUTE GROUPS').any {
                    it.Name == 'cg_snapshot' && it.SubComputeGroups.contains('cg_renamed')
                }
            }

            // Current instance data removes V. P's BEs remain, but its name is now missing.
            resume(instancePause)
            awaitUntil(60) {
                !sql_return_maparray('SHOW COMPUTE GROUPS').any { it.Name == 'cg_snapshot' }
            }
            // No node or name change in MS: only periodic mapping repair can restore P.
            resume(physicalPause)
            waitPhysical('cg_snapshot', 'cg_current_physical_id', [snapshotSpare.cluster_id])
            order_qt_repaired 'SELECT k FROM test_compute_group_name_reconciliation'

            // Check stability after several subsequent physical AND instance cycles.
            3.times {
                def logOffset = feLog.text.length()
                awaitUntil(30) {
                    def subsequent = feLog.text.substring(logOffset)
                    subsequent.contains('daemon cluster get cluster info succ') &&
                            subsequent.contains('finished to cloud instance checker')
                }
                waitPhysical('cg_snapshot', 'cg_current_physical_id', [snapshotSpare.cluster_id])
            }
        } finally {
            resume(stalePause)
            resume(instancePause)
            resume(physicalPause)
        }
    }
}
