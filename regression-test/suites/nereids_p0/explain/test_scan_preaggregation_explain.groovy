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

suite("test_scan_preaggregation_explain") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use nereids_test_query_db"

    sql "DROP TABLE IF EXISTS test_scan_preaggregation"
    sql """ CREATE TABLE `test_scan_preaggregation` (
              `k1` int(11) NULL,
              `k2` int(11) NULL,
              `k3` int(11) NULL,
              `v1` int(11) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`, `k3`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "disable_auto_compaction" = "false"
            ); """
    sql """ insert into test_scan_preaggregation values
            (1, 2, 3, 4),
            (1, 5, 6, 7); """
    explain {
        sql("select k1, min(k2), max(k3) from test_scan_preaggregation where k1 = 0 group by k1;")
        contains "PREAGGREGATION: ON"
    }

    qt_right_when_preagg_on "select k1, min(k2), max(k3) from test_scan_preaggregation where k1 = 1 group by k1;"
    qt_right_when_preagg_off "select k1, sum(k2) from test_scan_preaggregation group by k1;"
}
