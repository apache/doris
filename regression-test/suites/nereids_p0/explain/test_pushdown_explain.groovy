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

suite("test_pushdown_explain") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use nereids_test_query_db"
    
    explain {
        sql("select k1 from baseall where k1 = 1")
        contains "PREDICATES:"
    }
    qt_select "select k1 from baseall where k1 = 1"

    sql "DROP TABLE IF EXISTS test_lineorder"
    sql """ CREATE TABLE `test_lineorder` (
        `lo_orderkey` INT NOT NULL COMMENT '\"\"',
        `lo_linenumber` INT NOT NULL COMMENT '\"\"',
        `lo_shipmode` VARCHAR(11) NOT NULL COMMENT '\"\"'
    ) ENGINE=OLAP
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "colocate_with" = "groupa1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
    ); """
    sql """ insert into test_lineorder values(1,2,"asd"); """
    explain {
        sql("select count(1) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(*) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(1) - count(lo_shipmode) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(lo_orderkey) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
    explain {
        sql("select count(cast(lo_orderkey as bigint)) from test_lineorder;")
        contains "pushAggOp=COUNT"
    }
}
