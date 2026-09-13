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

suite("test_audit_log_hint_compute_group_docker", "docker") {
    def options = new ClusterOptions(cloudMode: true, feNum: 1, beNum: 1, msNum: 1)
    options.feConfigs += ['cloud_cluster_check_interval_second=1']

    docker(options) {
        def hintComputeGroup = "audit_hint_compute_group"
        cluster.addBackend(1, hintComputeGroup)

        def computeGroups = sql_return_maparray "SHOW CLUSTERS"
        assertEquals(2, computeGroups.size())
        def sessionComputeGroup = computeGroups.find { it.is_current == "TRUE" }.cluster
        assertNotNull(sessionComputeGroup)
        assertTrue(computeGroups.any { it.cluster == hintComputeGroup })

        try {
            sql "set global enable_audit_plugin = true"
            sql "use @${sessionComputeGroup}"
            sql "truncate table __internal_schema.audit_log"

            def marker = "audit_hint_cg_docker_marker_7F3A2B"
            sql """select /*+ SET_VAR(cloud_cluster = '${hintComputeGroup}') */
                       1, '${marker}'"""

            def retry = 60
            def query = """select count(*)
                            from __internal_schema.audit_log
                            where stmt like '%${marker}%'
                              and compute_group = '${hintComputeGroup}'"""
            def found = (sql "${query}")[0][0] as long
            while (found == 0) {
                if (retry-- < 0) {
                    throw new RuntimeException("audit_log row for the hint query was not found in the "
                            + "hint Compute Group")
                }
                sleep(3000)
                sql "call flush_audit_log()"
                found = (sql "${query}")[0][0] as long
            }

            assertTrue(found >= 1)
        } finally {
            sql "set global enable_audit_plugin = false"
        }
    }
}
