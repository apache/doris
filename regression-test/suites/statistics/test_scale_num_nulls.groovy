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

suite("test_scale_num_nulls") {
    // For an OlapTable, when only a subset of partitions is selected, 
    // the num_nulls value in column statistics needs to be scaled proportionally.
    sql """
        drop table if exists ptable;
        CREATE TABLE `ptable` (
        `id` int NULL,
        `val` int NULL,
        `d` date NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`d`)
        (PARTITION p201701 VALUES [('2017-01-01'), ('2017-02-01')),
        PARTITION p201702 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703 VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`id`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V3",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        ); 

        insert into ptable values 
        (1, null, '2017-01-01'), (11,2, '2017-01-01'), (111,2, '2017-01-01'), (1111,2, '2017-01-01'), 
        (2, null, '2017-02-01'), (22,2, '2017-02-01'), (222,2, '2017-02-01'), (2222,2, '2017-02-01'),
        (3, null, '2017-03-01'), (33,2, '2017-03-01'), (333,2, '2017-03-01'), (333,2, '2017-03-01');

        analyze table ptable with sync;
        """
        def colStats = sql "show column stats ptable";
        def memo = sql "explain memo plan select * from ptable where d='2017-01-01'"
        // check numNulls=1.0000 for column val, which is scaled from 3 to 1 according to the partition pruning result.
        assertTrue(memo.toString().contains("val#1 -> ndv=1.0000, min=2.000000(2), max=2.000000(2), count=4.0000, numNulls=1.0000"),
            "numNulls should be 1.0000, but is " + memo.toString() + "\n column stats: \n" + colStats.toString());
}

