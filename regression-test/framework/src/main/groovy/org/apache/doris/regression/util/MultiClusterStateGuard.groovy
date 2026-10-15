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

/** Restores the dedicated multi-cluster test instance; never used for docker/upgrade tests. */
class MultiClusterStateGuard {
    Closure listClusters
    Closure dropCluster
    Closure addCluster
    Closure readDefault
    Closure setDefault
    Closure awaitState
    Closure restoreInstance = {}

    /** Parse legacy IP:heartbeat:http:cloud-id entries with an optional brpc port. */
    static List<Map> configuredBackends(String configuredBes) {
        if (!configuredBes?.trim()) {
            throw new IllegalArgumentException('Missing multiClusterBes configuration')
        }
        def nodes = []
        configuredBes.split(',').eachWithIndex { entry, index ->
            def parts = entry.trim().split(':', -1)
            if (!(parts.length in [4, 5]) || parts.any { !it.trim() }) {
                throw new IllegalArgumentException("Invalid multiClusterBes entry at index ${index}")
            }
            def ports = [parts[1], parts[2]] + (parts.length == 5 ? [parts[4]] : [])
            if (ports.any { !it.isInteger() || it.toInteger() < 1 || it.toInteger() > 65535 }) {
                throw new IllegalArgumentException("Invalid BE port at index ${index}")
            }
            nodes << [ip: parts[0], heartbeat_port: parts[1].toInteger(), http_port: parts[2].toInteger(),
             cloud_unique_id: parts[3]]
        }
        // cloud_unique_id identifies a cloud deployment, not an individual BE process.
        if (nodes.collect { [it.ip, it.heartbeat_port] }.toSet().size() != nodes.size()) {
            throw new IllegalArgumentException('multiClusterBes contains duplicate BE endpoints')
        }
        return nodes
    }

    /** Shared fixture: the first two configured BEs each belong to one compute group. */
    static List<Map> sharedBaseline(String configuredBes) {
        def nodes = configuredBackends(configuredBes)
        if (nodes.size() < 2) {
            throw new IllegalArgumentException('The shared multi-cluster baseline requires at least two BEs')
        }
        return (0..1).collect { index ->
            [type: 'COMPUTE', cluster_name: "regression_cluster_name${index}".toString(),
             cluster_id: "regression_cluster_id${index}".toString(), cluster_status: 'NORMAL',
             nodes: [nodes[index]]]
        }
    }

    private static Object copy(Object value) {
        return new JsonSlurper().parseText(JsonOutput.toJson(value))
    }

    // MS refreshes these timestamps when recreating a cluster or node.
    static Object topology(Object value) {
        if (value instanceof Map) {
            def result = new TreeMap()
            value.each { key, item ->
                if (!(key in ['ctime', 'mtime'])) {
                    result[key] = topology(item)
                }
            }
            return result
        }
        if (value instanceof List) {
            return value.collect { topology(it) }.sort { a, b ->
                JsonOutput.toJson(a) <=> JsonOutput.toJson(b)
            }
        }
        return value
    }

    static boolean frontendMatches(List<Map> expected, List<Map> groups, List<Map> backends) {
        if ((groups.collect { it.cluster } as Set) != (expected.collect { it.cluster_name } as Set)) {
            return false
        }
        def expectedNodes = expected.collectMany { cluster ->
            (cluster.nodes ?: []).collect { node ->
                [cluster.cluster_id.toString(), (node.ip ?: node.host).toString(), node.heartbeat_port.toString(),
                        cluster.cluster_name.toString(), cluster.cluster_status.toString(), node.cloud_unique_id.toString()]
            }
        }.sort { a, b -> a.toString() <=> b.toString() }
        // Backend.getCloudClusterStatus treats an absent status tag as NORMAL.
        def actualNodes = backends.collect { backend ->
            def tag = backend.Tag instanceof Map ? backend.Tag : new JsonSlurper().parseText(backend.Tag.toString())
            [(tag.compute_group_id ?: tag.cloud_cluster_id)?.toString(), backend.Host.toString(),
                    backend.HeartbeatPort.toString(), (tag.compute_group_name ?: tag.cloud_cluster_name)?.toString(),
                    (tag.compute_group_status ?: tag.cloud_cluster_status ?: 'NORMAL').toString(), tag.cloud_unique_id?.toString()]
        }.sort { a, b -> a.toString() <=> b.toString() }
        return expectedNodes == actualNodes && backends.every {
            it.Alive.toString().toBoolean() && !it.SystemDecommissioned?.toString()?.toBoolean()
        }
    }

    private List<Map> computeClusters() {
        List<Map> clusters = listClusters.call()
        if (clusters.any { it.type == 'VIRTUAL' }) {
            throw new IllegalStateException('Legacy multi-cluster state restoration does not support virtual clusters')
        }
        return clusters.findAll { it.type == 'COMPUTE' }
    }

    /** initializeBaseline keeps a successful load fixture, and rolls back a failed one. */
    def run(boolean initializeBaseline, Closure body) {
        List<Map> original = copy(computeClusters()) as List<Map>
        if (original.any { it.cluster_status != 'NORMAL' ||
                (it.nodes ?: []).any { n -> n.status && n.status != 'NODE_STATUS_RUNNING' } }) {
            throw new IllegalStateException('Multi-cluster tests require normal clusters and running nodes')
        }
        String originalDefault = readDefault.call()
        // A stale default cannot be restored with SET PROPERTY. Reject it before destructive tests.
        if (originalDefault && !original.any { it.cluster_name == originalDefault }) {
            throw new IllegalStateException("Initial default compute group does not exist: ${originalDefault}")
        }
        Throwable failure = null
        try {
            // Tests can replace the groups. New connections must not inherit a deleted group.
            setDefault.call('')
            awaitState.call(original, '')
            return body.call()
        } catch (Throwable t) {
            failure = t
            throw t
        } finally {
            def errors = []
            def attempt = { Closure action ->
                try {
                    action.call()
                } catch (Throwable t) {
                    errors.add(t)
                }
            }
            attempt { restoreInstance.call() }
            List<Map> expected = original
            String expectedDefault = originalDefault
            attempt {
                if (initializeBaseline && failure == null) {
                    // The load suite intentionally leaves its successful setup for subsequent suites.
                    expected = copy(computeClusters()) as List<Map>
                    expectedDefault = readDefault.call()
                } else {
                    def expectedById = original.collectEntries { [(it.cluster_id): it] }
                    computeClusters().each { current ->
                        def saved = expectedById[current.cluster_id]
                        if (saved == null || topology(saved) != topology(current)) {
                            attempt { dropCluster.call(current) }
                        }
                    }
                    // Re-read after deletion; a failed drop must not prevent restoring other groups.
                    def currentById = computeClusters().collectEntries { [(it.cluster_id): it] }
                    original.each { saved ->
                        if (!currentById.containsKey(saved.cluster_id)) {
                            attempt { addCluster.call(copy(saved)) }
                        }
                    }
                }
            }
            // Restore metadata and FE node maps before restoring the default property.
            // Even if a wait fails, attempt the remaining cleanup and report every failure.
            attempt { awaitState.call(expected, null) }
            attempt { setDefault.call(expectedDefault) }
            attempt { awaitState.call(expected, expectedDefault) }
            if (!errors.isEmpty()) {
                Throwable restoreFailure = new IllegalStateException('Failed to restore multi-cluster test state')
                errors.each { restoreFailure.addSuppressed(it) }
                if (failure != null) {
                    failure.addSuppressed(restoreFailure)
                } else {
                    throw restoreFailure
                }
            }
        }
    }
}
