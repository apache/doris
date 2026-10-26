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
import org.apache.doris.regression.util.NodeType

suite("test_disable_load_selected_replica_failure", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 1
    options.beNum = 5
    options.feConfigs += ["disable_tablet_scheduler=true"]
    options.enableDebugPoints()

    docker(options) {
        def backendIPs = [:]
        def backendPorts = [:]
        getBackendIpHttpPort(backendIPs, backendPorts)
        def backendIds = backendIPs.keySet().sort { it.toLong() }
        def sharedBackend = backendIds[0]
        def disabledBackends = backendIds[1..2]
        def tags = ["shared", "left", "left", "right", "right"]
        backendIds.eachWithIndex { backendId, i ->
            sql """ALTER SYSTEM MODIFY BACKEND "${backendId}" SET ("tag.location" = "${tags[i]}")"""
        }

        def originalMemtableOnSink = sql("select @@enable_memtable_on_sink_node")[0][0]
        def legacyPoint = "LoadChannel.add_batch.failed"
        def v2Point = "LoadStreamWriter.close.cancelled"
        def debugPoint = GetDebugPoint()
        try {
            sql "set enable_memtable_on_sink_node = false"
            sql "drop table if exists disable_load_empty_auto_partition"
            sql """
                create table disable_load_empty_auto_partition (city varchar(20), id int)
                duplicate key(city, id)
                auto partition by list(city) ()
                distributed by hash(id) buckets 1
                properties (
                    "replication_allocation" =
                        "tag.location.shared: 1, tag.location.left: 1, tag.location.right: 1"
                )
            """
            // The synthetic partition/tablet 0 has one destination even though real partitions require two.
            sql "insert into disable_load_empty_auto_partition values ('BEIJING', 1)"
            sql """select assert_true(count(*) = 1 and min(id) = 1)
                    from disable_load_empty_auto_partition"""

            sql "drop table if exists disable_load_selected_failure_legacy"
            sql """
                create table disable_load_selected_failure_legacy (k int, v int)
                duplicate key(k)
                partition by range(k) (
                    partition p_a values less than ("10"),
                    partition p_b values less than ("30")
                        ("replication_allocation" = "tag.location.shared: 1, tag.location.right: 2")
                )
                distributed by hash(k) buckets 1
                properties (
                    "replication_allocation" = "tag.location.shared: 1, tag.location.left: 2",
                    "min_load_replica_num" = "1"
                )
            """
            sql "drop table if exists disable_load_selected_failure_v2"
            sql """
                create table disable_load_selected_failure_v2 (k int, v int)
                duplicate key(k)
                partition by range(k) (
                    partition p_a values less than ("10"),
                    partition p_b values less than ("30")
                        ("replication_allocation" = "tag.location.shared: 1, tag.location.right: 2")
                )
                distributed by hash(k) buckets 1
                properties (
                    "replication_allocation" = "tag.location.shared: 1, tag.location.left: 2",
                    "min_load_replica_num" = "1"
                )
            """

            disabledBackends.each { backendId ->
                sql """ALTER SYSTEM MODIFY BACKEND "${backendId}" SET ("disable_load" = "true")"""
            }

            // Seed B before enabling the shared-backend fault.
            sql "insert into disable_load_selected_failure_legacy values (21, 100)"
            debugPoint.enableDebugPoint(backendIPs[sharedBackend],
                    backendPorts[sharedBackend] as int, NodeType.BE, legacyPoint)
            test {
                sql "insert into disable_load_selected_failure_legacy values (1, 200), (22, 200)"
                exception "fault injection"
            }
            sql """select assert_true(count(*) = 1 and min(k) = 21 and min(v) = 100)
                    from disable_load_selected_failure_legacy"""
            debugPoint.disableDebugPoint(backendIPs[sharedBackend],
                    backendPorts[sharedBackend] as int, NodeType.BE, legacyPoint)
            sql "insert into disable_load_selected_failure_legacy values (1, 200), (22, 200)"
            sql """select assert_true(count(*) = 3 and sum(k) = 44 and sum(v) = 500)
                    from disable_load_selected_failure_legacy"""

            sql "set enable_memtable_on_sink_node = true"
            // Seed B before enabling the V2 late-close fault.
            sql "insert into disable_load_selected_failure_v2 values (21, 100)"
            debugPoint.enableDebugPoint(backendIPs[sharedBackend],
                    backendPorts[sharedBackend] as int, NodeType.BE, v2Point)
            test {
                sql "insert into disable_load_selected_failure_v2 values (1, 200), (22, 200)"
                exception "flush segment failed"
            }
            sql """select assert_true(count(*) = 1 and min(k) = 21 and min(v) = 100)
                    from disable_load_selected_failure_v2"""
            debugPoint.disableDebugPoint(backendIPs[sharedBackend],
                    backendPorts[sharedBackend] as int, NodeType.BE, v2Point)
            sql "insert into disable_load_selected_failure_v2 values (1, 200), (22, 200)"
            sql """select assert_true(count(*) = 3 and sum(k) = 44 and sum(v) = 500)
                    from disable_load_selected_failure_v2"""
        } finally {
            try {
                debugPoint.disableDebugPoint(backendIPs[sharedBackend],
                        backendPorts[sharedBackend] as int, NodeType.BE, legacyPoint)
                debugPoint.disableDebugPoint(backendIPs[sharedBackend],
                        backendPorts[sharedBackend] as int, NodeType.BE, v2Point)
            } finally {
                disabledBackends.each { backendId ->
                    sql """ALTER SYSTEM MODIFY BACKEND "${backendId}" SET ("disable_load" = "false")"""
                }
                sql "set enable_memtable_on_sink_node = '${originalMemtableOnSink}'"
            }
        }
    }
}
