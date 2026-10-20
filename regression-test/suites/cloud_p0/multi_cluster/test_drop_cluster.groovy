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

suite("test_drop_cluster") {
    withRestoredMultiClusterState(false) {
        def baseline = multiClusterBaseline()
        checkMultiClusterBaseline(baseline)
        def dropBaseline = {
            baseline.each { group -> drop_cluster.call(group.cluster_name, group.cluster_id) }
        }
        def addBaseline = {
            baseline.each { group ->
                def node = group.nodes[0]
                add_cluster.call(node.cloud_unique_id, node.ip, node.heartbeat_port.toString(),
                        group.cluster_name, group.cluster_id)
            }
        }

        // Deletion/recreation is the behavior under test; retain it using the shared groups.
        dropBaseline()
        sleep(20000)
        assertTrue(sql('SHOW CLUSTERS').isEmpty())

        addBaseline()
        sleep(20000)
        checkMultiClusterBaseline(baseline)

        for (int i = 0; i < 10; i++) {
            dropBaseline()
            addBaseline()
        }
        sleep(20000)
        checkMultiClusterBaseline(baseline)

        baseline.each { group ->
            sql "USE @${group.cluster_name}"
            def current = sql('SHOW CLUSTERS').find { it[0] == group.cluster_name }
            assertNotNull(current, "Recreated compute group ${group.cluster_name} is missing")
            assertTrue(current[1].toString().equalsIgnoreCase('true'))
        }
    }
}
