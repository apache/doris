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

import org.apache.doris.regression.util.NodeType

suite("test_insert_quorum_split_reports", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    def backendIPs = [:]
    def backendPorts = [:]
    getBackendIpHttpPort(backendIPs, backendPorts)
    if (backendIPs.size() < 3) {
        return
    }

    def closePoint = "LoadStream.close_load.force_last_source"
    def failurePoint = "TabletStream.add_segment.unknown_segid"
    def debugPoint = GetDebugPoint()
    def failedBackend = null
    def sessionVariables = ["enable_memtable_on_sink_node", "enable_local_shuffle",
            "parallel_pipeline_task_num", "load_stream_per_node", "query_timeout"]
    def savedVariables = sessionVariables.collectEntries { name ->
        [(name): sql("select @@${name}")[0][0]]
    }
    try {
        sql "set enable_memtable_on_sink_node = true"
        sql "set enable_local_shuffle = false"
        sql "set parallel_pipeline_task_num = 1"
        sql "set load_stream_per_node = 1"
        sql "set query_timeout = 60"
        sql "drop table if exists insert_quorum_split_reports_source"
        sql "drop table if exists insert_quorum_split_reports_target"
        // Spread the scan over multiple BEs. The close injection fails the load
        // explicitly if execution nevertheless opens streams from only one source.
        sql """create table insert_quorum_split_reports_source (k int, v bigint)
                duplicate key(k) distributed by hash(k) buckets ${backendIPs.size() * 4}
                properties("replication_num" = "1")"""
        sql """create table insert_quorum_split_reports_target (k int, v bigint)
                duplicate key(k) distributed by hash(k) buckets 5
                properties("replication_num" = "3")"""
        sql """insert into insert_quorum_split_reports_source
                select number, number * 11 + 5 from numbers("number" = "2048")"""

        def replicas = sql_return_maparray "show tablets from insert_quorum_split_reports_target"
        failedBackend = replicas.collect { it.BackendId.toString() }.unique().min { it.toLong() }
        def tableId = getTableId("insert_quorum_split_reports_target").toString()
        // All healthy destinations report to the largest source ID. The failed
        // destination reports to the smallest, which receives no healthy results.
        debugPoint.enableDebugPointForAllBEs(closePoint,
                [table_id: tableId, pick_max: "true", wait_ms: "30000", timeout: "90"])
        debugPoint.enableDebugPoint(backendIPs[failedBackend], backendPorts[failedBackend] as int,
                NodeType.BE, closePoint,
                [table_id: tableId, pick_max: "false", require_failure: "true",
                 wait_ms: "30000", timeout: "90"])
        debugPoint.enableDebugPoint(backendIPs[failedBackend], backendPorts[failedBackend] as int,
                NodeType.BE, failurePoint, [timeout: "90"])

        // Before the fix, the source receiving the failed destination's final
        // report rejects the INSERT with success=0, required=2.
        sql "insert into insert_quorum_split_reports_target select * from insert_quorum_split_reports_source"

        // require_failure checks the actual final response, avoiding a race with
        // background replica repair when checking the FE's failed-version metadata.
        qt_rows "select count(*) from insert_quorum_split_reports_target"
        qt_mismatches """select count(*) from (
                (select k, v from insert_quorum_split_reports_source
                 except select k, v from insert_quorum_split_reports_target)
                union all
                (select k, v from insert_quorum_split_reports_target
                 except select k, v from insert_quorum_split_reports_source)
                ) differences"""
    } finally {
        try {
            if (failedBackend != null) {
                debugPoint.disableDebugPoint(backendIPs[failedBackend], backendPorts[failedBackend] as int,
                        NodeType.BE, failurePoint)
            }
        } finally {
            try {
                debugPoint.disableDebugPointForAllBEs(closePoint)
            } finally {
                savedVariables.each { name, value -> sql "set ${name} = '${value}'" }
            }
        }
    }
}
