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

suite("redundant_conjuncts") {
    sql """
    DROP TABLE IF EXISTS redundant_conjuncts;
    """
    sql """
    CREATE TABLE IF NOT EXISTS `redundant_conjuncts` (
      `k1` int(11) NULL COMMENT "",
      `v1` int(11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`, `v1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 10
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """
    
    qt_redundant_conjuncts """
    EXPLAIN SELECT /*+SET_VAR(enable_nereids_planner=false, REWRITE_OR_TO_IN_PREDICATE_THRESHOLD=2, parallel_fragment_exec_instance_num = 1) */ v1 FROM redundant_conjuncts WHERE k1 = 1 AND k1 = 1;
    """

    qt_redundant_conjuncts_gnerated_by_extract_common_filter """
    EXPLAIN SELECT /*+SET_VAR(enable_nereids_planner=false, REWRITE_OR_TO_IN_PREDICATE_THRESHOLD=100, parallel_fragment_exec_instance_num = 1) */ v1 FROM redundant_conjuncts WHERE k1 = 1 OR k1 = 2;
    """
}
