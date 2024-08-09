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

suite("test_forbid_unknown_col_stats") {
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "drop table if exists test_forbid_unknown_col_stats_tbl"
    sql """
        create table test_forbid_unknown_col_stats_tbl(
        `r_regionkey` int(11) NOT NULL,
        `r_name` Array<int>,
        `r_comment` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`r_regionkey`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
        """
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set forbid_unknown_col_stats = true"
    sql "alter table test_forbid_unknown_col_stats_tbl modify column r_regionkey set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='5');"
    sql "select r_regionkey from test_forbid_unknown_col_stats_tbl;"

    sql "select r_name from test_forbid_unknown_col_stats_tbl;"

    test{
        sql "select * from test_forbid_unknown_col_stats_tbl;"
        exception "tables with unknown column stats: OlapScanNode{id=0, tid=0, tblName=test_forbid_unknown_col_stats_tbl, keyRanges=, preds= limit=-1}"
    }

    sql "select count() from __internal_schema.column_statistics"
    sql "select count() from information_schema.views"
    
}
