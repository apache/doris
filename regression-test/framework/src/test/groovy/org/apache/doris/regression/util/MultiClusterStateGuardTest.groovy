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

package org.apache.doris.regression.util

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.*

class MultiClusterStateGuardTest {
    private static def copy(value) {
        new JsonSlurper().parseText(JsonOutput.toJson(value))
    }

    private static Map cluster(String id) {
        [type: 'COMPUTE', cluster_id: id, cluster_name: "group_${id}".toString(),
         cluster_status: 'NORMAL', public_endpoint: "public_${id}".toString(),
         private_endpoint: "private_${id}".toString(), properties: [key: 'value'], ctime: 1,
         nodes: [[cloud_unique_id: "be_${id}".toString(), ip: '127.0.0.1', heartbeat_port: 9050,
                  status: 'NODE_STATUS_RUNNING', ctime: 1],
                 [cloud_unique_id: "be2_${id}".toString(), ip: '127.0.0.2', heartbeat_port: 9051,
                  status: 'NODE_STATUS_RUNNING', ctime: 1]]]
    }

    private static Map fixture(List<Map> clusters = [cluster('a'), cluster('b')], String defaultGroup = 'group_a') {
        def state = [clusters: copy(clusters), defaultGroup: defaultGroup, events: [],
                     failDrop: [], failAdd: [], failWait: false, instanceStatus: 'NORMAL']
        state.guard = new MultiClusterStateGuard(
            listClusters: { state.clusters },
            restoreInstance: { state.events << 'instance'; state.instanceStatus = 'NORMAL' },
            dropCluster: { c ->
                state.events << "drop:${c.cluster_id}".toString()
                if (c.cluster_id in state.failDrop) { throw new IOException('drop failed') }
                state.clusters.removeAll { it.cluster_id == c.cluster_id }
            },
            addCluster: { c ->
                state.events << "add:${c.cluster_id}".toString()
                if (c.cluster_id in state.failAdd) { throw new IOException('add failed') }
                c.ctime = 100
                c.nodes.each { it.ctime = 100 }
                state.clusters << c
            },
            readDefault: { state.defaultGroup },
            setDefault: { saved -> state.events << 'default'; state.defaultGroup = saved },
            awaitState: { expected, expectedDefault ->
                state.events << "wait:${expectedDefault}".toString()
                if (state.failWait) { throw new IOException('FE timeout') }
                assertEquals(MultiClusterStateGuard.topology(expected), MultiClusterStateGuard.topology(state.clusters))
                if (expectedDefault != null) { assertEquals(expectedDefault, state.defaultGroup) }
            }
        )
        state
    }

    @Test
    void sharedBaselineUsesFirstTwoConfiguredNodesWithoutSharingMutableSnapshots() {
        String config = 'be0:9050:8040:id0,be1:9051:8041:id1,be2:9052:8042:id2'
        def baseline = MultiClusterStateGuard.sharedBaseline(config)
        assertEquals(['regression_cluster_name0', 'regression_cluster_name1'], baseline*.cluster_name)
        assertEquals(['regression_cluster_id0', 'regression_cluster_id1'], baseline*.cluster_id)
        assertEquals(['id0', 'id1'], baseline.collect { it.nodes[0].cloud_unique_id })
        assertEquals(8041, baseline[1].nodes[0].http_port)
        baseline[0].nodes.clear()
        assertEquals(1, MultiClusterStateGuard.sharedBaseline(config)[0].nodes.size())
    }

    @Test
    void acceptsOptionalBrpcPortAndSharedCloudId() {
        def nodes = MultiClusterStateGuard.configuredBackends('be0:9050:8040:shared:8060,be1:9050:8040:shared')
        assertEquals(['shared', 'shared'], nodes*.cloud_unique_id)
        assertEquals(['be0', 'be1'], nodes*.ip)
        assertEquals(2, MultiClusterStateGuard.sharedBaseline('be0:9050:8040:shared:8060,be1:9050:8040:shared:8060').size())
    }

    @Test
    void initializerReplacesExistingFixtureAndRollsBackFailedReplacement() {
        def state = fixture()
        state.guard.run(true) {
            assertEquals('', state.defaultGroup)
            state.clusters = [cluster('canonical')]
            state.defaultGroup = 'group_canonical'
        }
        assertEquals(['canonical'], state.clusters*.cluster_id)
        assertEquals('group_canonical', state.defaultGroup)
        state = fixture()
        def original = copy(state.clusters)
        assertThrows(IOException) {
            state.guard.run(true) {
                state.clusters = [cluster('canonical')]
                state.defaultGroup = 'group_canonical'
                throw new IOException('load failed')
            }
        }
        assertEquals(MultiClusterStateGuard.topology(original), MultiClusterStateGuard.topology(state.clusters))
        assertEquals('group_a', state.defaultGroup)
    }

    @Test
    void sharedBaselineRejectsMissingMalformedOrDuplicateNodes() {
        [null, '', 'be0:9050:8040:id0', 'be0:9050:8040:id0,invalid',
         'be0:0:8040:id0,be1:9050:8040:id1', 'be0:9050:8040:id0:invalid,be1:9050:8040:id0',
         'be0:9050:8040:id0,be0:9050:8041:id1'].each { config ->
            assertThrows(IllegalArgumentException) { MultiClusterStateGuard.sharedBaseline(config) }
        }
    }

    @Test
    void successRestoresFullMetadataAndDefaultAfterTopology() {
        def state = fixture()
        def original = copy(state.clusters)
        assertEquals(42, state.guard.run(false) {
            state.clusters[0].nodes.remove(0)
            state.clusters[0].properties.key = 'changed'
            state.clusters << cluster('temporary')
            state.defaultGroup = 'group_temporary'
            state.instanceStatus = 'OVERDUE'
            42
        })
        assertEquals(MultiClusterStateGuard.topology(original), MultiClusterStateGuard.topology(state.clusters))
        assertEquals('group_a', state.defaultGroup)
        assertEquals('NORMAL', state.instanceStatus)
        assertEquals(['default', 'wait:', 'instance', 'drop:a', 'drop:temporary', 'add:a', 'wait:null', 'default', 'wait:group_a'], state.events)
    }

    @Test
    void failureAfterDeletingGroupsRestoresSnapshotAndRethrowsOriginal() {
        def state = fixture()
        def original = copy(state.clusters)
        def failure = new AssertionError('original failure')
        assertSame(failure, assertThrows(AssertionError) {
            state.guard.run(false) {
                state.clusters.clear()
                state.clusters << cluster('temporary')
                state.defaultGroup = 'group_temporary'
                throw failure
            }
        })
        assertEquals(MultiClusterStateGuard.topology(original), MultiClusterStateGuard.topology(state.clusters))
        assertEquals('group_a', state.defaultGroup)
        assertEquals(0, failure.suppressed.length)
    }

    @Test
    void unchangedTopologyIsNotRecreated() {
        def state = fixture()
        state.guard.run(false) {
            state.clusters.reverse(true)
            state.clusters[0].ctime = 123
            state.clusters[0].nodes.reverse(true)
        }
        assertEquals(['default', 'wait:', 'instance', 'wait:null', 'default', 'wait:group_a'], state.events)
    }

    @Test
    void cleanupFailuresAreSuppressedAndOtherGroupsAreStillRestored() {
        def state = fixture()
        def failure = new AssertionError('query failed')
        state.failDrop = ['temporary']
        state.failAdd = ['a']
        assertSame(failure, assertThrows(AssertionError) {
            state.guard.run(false) {
                state.failWait = true
                state.clusters = [cluster('temporary')]
                throw failure
            }
        })
        assertTrue(state.events.contains('add:b'))
        assertTrue(state.events.contains('default'))
        assertEquals(1, failure.suppressed.length)
        assertEquals(4, failure.suppressed[0].suppressed.length)
    }

    @Test
    void cleanupFailureFailsAnOtherwiseSuccessfulSuite() {
        def state = fixture()
        def failure = assertThrows(IllegalStateException) { state.guard.run(false) { state.failWait = true; 42 } }
        assertEquals(2, failure.suppressed.length)
        assertTrue(state.events.contains('default'))
    }

    @Test
    void bootstrapKeepsSuccessfulInitialSetupButRollsBackFailure() {
        def state = fixture([], '')
        state.guard.run(true) { state.clusters << cluster('a'); state.defaultGroup = 'group_a' }
        assertEquals(['a'], state.clusters*.cluster_id)
        assertEquals('group_a', state.defaultGroup)

        state = fixture([], '')
        assertThrows(IOException) {
            state.guard.run(true) { state.clusters << cluster('a'); throw new IOException('load failed') }
        }
        assertTrue(state.clusters.isEmpty())
        assertEquals('', state.defaultGroup)

        state = fixture([], '')
        state.guard.run(false) { state.clusters << cluster('a') }
        assertTrue(state.clusters.isEmpty())
    }

    @Test
    void rejectsInvalidInitialStateBeforeRunningBody() {
        def suspended = cluster('a'); suspended.cluster_status = 'SUSPENDED'
        def decommissioning = cluster('a'); decommissioning.nodes[0].status = 'NODE_STATUS_DECOMMISSIONING'
        [fixture([cluster('a')], 'missing'), fixture([suspended]), fixture([decommissioning]),
         fixture([[type: 'VIRTUAL']])].each { state ->
            assertThrows(IllegalStateException) { state.guard.run(false) { fail('body must not run') } }
            assertTrue(state.events.isEmpty())
        }
    }

    @Test
    void feMustSeeExactNodeMappingAndAllHeartbeats() {
        def expected = [cluster('a')]
        def groups = [[cluster: 'group_a']]
        def nodes = [[Host: '127.0.0.1', HeartbeatPort: 9050, Alive: 'true', Tag: JsonOutput.toJson([cloud_cluster_id: 'a', cloud_cluster_name: 'group_a',
                        cloud_cluster_status: 'NORMAL', cloud_unique_id: 'be_a'])],
                     [Host: '127.0.0.2', HeartbeatPort: 9051, Alive: true, Tag: [compute_group_id: 'a',
                        compute_group_name: 'group_a', compute_group_status: 'NORMAL', cloud_unique_id: 'be2_a']]]
        assertTrue(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, [], nodes))
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, groups, nodes.take(1)))
        nodes[1].Tag.remove('compute_group_status')
        assertTrue(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
        nodes[1].Tag.compute_group_status = 'NORMAL'
        nodes[1].Alive = false
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
        nodes[1].Alive = true
        nodes[1].HeartbeatPort = 9052
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
        nodes[1].HeartbeatPort = 9051
        nodes[1].Tag.compute_group_status = 'SUSPENDED'
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
        nodes[1].Tag.compute_group_status = 'NORMAL'
        nodes[1].SystemDecommissioned = true
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
        nodes[1].SystemDecommissioned = false
        nodes[1].Tag.compute_group_id = 'wrong'
        assertFalse(MultiClusterStateGuard.frontendMatches(expected, groups, nodes))
    }
}
