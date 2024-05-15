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

suite("test_global_partition_topn_plan") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "DROP TABLE IF EXISTS test_global_partition_topn_plan"
    sql """ CREATE TABLE `test_global_partition_topn_plan` (
	    c1 int, c2 int, c3 int
    )ENGINE=OLAP
    distributed by hash(c1) buckets 10
    properties(
        "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """ alter table test_global_partition_topn_plan modify column c1 set stats('row_count'='52899687', 'ndv'='52899687', 'num_nulls'='0', 'min_value'='1', 'max_value'='52899687', 'data_size'='4'); """
    sql """ alter table test_global_partition_topn_plan modify column c2 set stats('row_count'='52899687', 'ndv'='23622730', 'num_nulls'='0', 'min_value'='1', 'max_value'='52899687', 'data_size'='4'); """
    sql """ alter table test_global_partition_topn_plan modify column c3 set stats('row_count'='52899687', 'ndv'='2', 'num_nulls'='0', 'min_value'='0', 'max_value'='1', 'data_size'='4'); """

    sql "SET global_partition_topn_threshold=2"
    explain {
        sql("shape plan select rn from (select row_number() over (partition by c2 order by c3) as rn from test_global_partition_topn_plan) tmp where rn <= 100");
        contains"PhysicalPartitionTopN"
        notContains"PhysicalQuickSort"
    }

    sql "SET global_partition_topn_threshold=3"
    explain {
        sql("shape plan select rn from (select row_number() over (partition by c2 order by c3) as rn from test_global_partition_topn_plan) tmp where rn <= 100");
        contains"PhysicalPartitionTopN"
        contains"PhysicalQuickSort"
    }

    sql "SET global_partition_topn_threshold=100"
    explain {
        sql("shape plan select rn from (select row_number() over (partition by c3 order by c2) as rn from test_global_partition_topn_plan) tmp where rn <= 100");
        contains"PhysicalPartitionTopN"
        notContains"PhysicalQuickSort"
    }

    sql "SET global_partition_topn_threshold=2"
    explain {
        sql("shape plan select rn from (select row_number() over (partition by c2, c3 order by c1) as rn from test_global_partition_topn_plan) tmp where rn <= 100");
        contains"PhysicalPartitionTopN"
        notContains"PhysicalQuickSort"
    }

    sql "SET global_partition_topn_threshold=3"
    explain {
        sql("shape plan select rn from (select row_number() over (partition by c2, c3 order by c1) as rn from test_global_partition_topn_plan) tmp where rn <= 100");
        contains"PhysicalPartitionTopN"
        contains"PhysicalQuickSort"
    }

    sql "SET global_partition_topn_threshold=2"
    explain {
        sql("shape plan select rn from (select row_number() over (partition by c2 + c3 order by c1) as rn from test_global_partition_topn_plan) tmp where rn <= 100");
        contains"PhysicalPartitionTopN"
        contains"PhysicalQuickSort"
    }
}
