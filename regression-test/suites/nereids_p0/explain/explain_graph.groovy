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

suite("explain_graph") {
    // filter about invisible column "DORIS_DELETE_SIGN = 0" has no impaction on partition pruning
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set partition_pruning_expand_threshold=10;"
    sql "set ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "drop table if exists T1;"
    sql """
        CREATE TABLE T1 (
        a INT NULL,
        b INT NULL,
        c INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000"
        ); """
    
        sql "drop table if exists T2;"
        sql """
        CREATE TABLE T2 (
        a INT NULL,
        b INT NULL,
        c INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000"
        ); """
        // make sure "explain graph" could work with "insert select" 
        sql "explain graph insert into T2 select * from T1"
}