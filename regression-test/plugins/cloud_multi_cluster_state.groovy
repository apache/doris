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
import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.Suite
import org.apache.doris.regression.util.MultiClusterStateGuard

// Shared by the load fixture and cases that reuse or deliberately recreate its groups.
Suite.metaClass.multiClusterBaseline = { ->
    MultiClusterStateGuard.sharedBaseline(delegate.context.config.multiClusterBes)
}

Suite.metaClass.checkMultiClusterBaseline = { List<Map> baseline ->
    def suite = delegate
    suite.assertTrue(MultiClusterStateGuard.frontendMatches(baseline,
            suite.sql_return_maparray('SHOW CLUSTERS'), suite.sql_return_maparray('SHOW BACKENDS')),
            'Expected shared regression_cluster_name0/1 groups with their configured BEs alive; ' +
                    'run the multi_cluster_s3_load/load fixture first and check prior suite cleanup')
}

// Only for serial legacy suites on the dedicated multiClusterInstance.
// Docker/VCG/upgrade suites have their own cluster lifecycle.
Suite.metaClass.withRestoredMultiClusterState = { boolean initializeBaseline, Closure action ->
    def suite = delegate
    def config = suite.context.config
    def nodes = MultiClusterStateGuard.configuredBackends(config.multiClusterBes)
    if (!config.multiClusterInstance) {
        throw new IllegalStateException('Missing dedicated multi-cluster test instance/BE configuration')
    }
    def request = { String operation, Map payload ->
        def result
        suite.httpTest {
            printResponse false
            endpoint config.metaServiceHttpAddress
            uri "/MetaService/http/${operation}?token=${config.metaServiceToken}" +
                    "&instance_id=${java.net.URLEncoder.encode(config.multiClusterInstance, 'UTF-8')}"
            body JsonOutput.toJson([instance_id: config.multiClusterInstance] + payload)
            check { code, response ->
                result = suite.parseJson(response)
                suite.assertTrue(code == 200 && result.code == 'OK',
                        "Multi-cluster ${operation} failed: ${result.code}")
            }
        }
        result
    }
    def readInstanceStatus = { request('get_instance', [:]).result.status }
    if (readInstanceStatus() != 'NORMAL') {
        throw new IllegalStateException('Multi-cluster tests require a normal dedicated instance')
    }
    def fetchClusters = { request('get_cluster', [cloud_unique_id: nodes[0].cloud_unique_id]).result.cluster }
    def frontends = suite.sql_return_maparray('SHOW FRONTENDS')
    if (frontends.isEmpty() || frontends.any { !it.Alive.toString().toBoolean() }) {
        throw new IllegalStateException('All configured frontends must be alive before changing cluster state')
    }
    def readDefault = {
        def rows = suite.sql_return_maparray('SHOW PROPERTY')
        def row = rows.find { it.Key == 'default_cloud_cluster' }
        if (row == null) {
            throw new IllegalStateException('SHOW PROPERTY did not return default_cloud_cluster')
        }
        row.Value?.toString() ?: ''
    }
    def guard = new MultiClusterStateGuard(
        listClusters: fetchClusters,
        restoreInstance: {
            if (readInstanceStatus() != 'NORMAL') {
                request('set_instance_status', [op: 'SET_NORMAL'])
            }
        },
        dropCluster: { saved ->
            request('drop_cluster', [cluster: [type: 'COMPUTE',
                    cluster_name: saved.cluster_name, cluster_id: saved.cluster_id, nodes: []]])
        },
        addCluster: { saved -> request('add_cluster', [cluster: saved]) },
        readDefault: readDefault,
        setDefault: { String saved ->
            def literal = saved.replace('\\', '\\\\').replace("'", "''")
            suite.sql("SET PROPERTY 'default_cloud_cluster' = '${literal}'")
        },
        awaitState: { List<Map> expected, String expectedDefault ->
            long deadline = System.nanoTime() + 120_000_000_000L
            Throwable lastError = null
            while (System.nanoTime() < deadline) {
                try {
                    if (readInstanceStatus() != 'NORMAL') {
                        throw new IllegalStateException('MS instance status has not returned to normal')
                    }
                    def actual = fetchClusters().findAll { it.type == 'COMPUTE' }
                    if (MultiClusterStateGuard.topology(actual) != MultiClusterStateGuard.topology(expected)) {
                        throw new IllegalStateException('MS compute groups do not match the saved topology')
                    }
                    for (def frontend : frontends) {
                        boolean tls = config.otherConfigs.get('enableTLS')?.toString()?.toBoolean() ?: false
                        def url = tls ? Config.buildUrlWithDb(frontend.Host as String, frontend.QueryPort as int,
                                suite.context.dbName, config.otherConfigs.get('keyStorePath')?.toString(),
                                config.otherConfigs.get('keyStorePassword')?.toString(),
                                config.otherConfigs.get('trustStorePath')?.toString(),
                                config.otherConfigs.get('trustStorePassword')?.toString()) :
                                Config.buildUrlWithDb(frontend.Host as String, frontend.QueryPort as int, suite.context.dbName)
                        url += '&connectTimeout=5000&socketTimeout=5000'
                        suite.connect(config.jdbcUser, config.jdbcPassword, url) {
                            def visibleGroups = suite.sql_return_maparray('SHOW CLUSTERS')
                            def visibleNodes = suite.sql_return_maparray('SHOW BACKENDS')
                            if (!MultiClusterStateGuard.frontendMatches(expected, visibleGroups, visibleNodes)) {
                                throw new IllegalStateException("FE ${frontend.Host} has not restored groups/nodes/heartbeats")
                            }
                            if (expectedDefault != null && readDefault() != expectedDefault) {
                                throw new IllegalStateException("FE ${frontend.Host} has not restored the user default group")
                            }
                        }
                    }
                    return
                } catch (Exception e) {
                    lastError = e
                }
                Thread.sleep(500)
            }
            throw new IllegalStateException('Timed out restoring multi-cluster state on MS and all frontends', lastError)
        }
    )
    guard.run(initializeBaseline, action)
}
