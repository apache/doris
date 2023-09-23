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

suite("colocate_join_with_rollup", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false" 

    sql """ DROP TABLE IF EXISTS test_query_colocate1 """
    sql """ DROP TABLE IF EXISTS test_query_colocate2 """
    sql """ DROP TABLE IF EXISTS test_query_no_colocate """

    sql """
        CREATE TABLE `test_query_colocate1` (
              `datekey` int(11) NULL,
              `rollup_1_condition` int null,
              `rollup_2_condition` int null,
              `sum_col1` bigint(20) SUM NULL,
              `sum_col2` bigint(20) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`datekey`,`rollup_1_condition`,`rollup_2_condition`)
            COMMENT ""
            PARTITION BY RANGE(`datekey`)
            (PARTITION p20220102 VALUES [("20220101"), ("20220102")),
            PARTITION p20220103 VALUES [("20220102"), ("20220103")))
            DISTRIBUTED BY HASH(`datekey`) BUCKETS 1
            rollup (
            rollup_1(datekey, sum_col1),
            rollup_2(datekey, sum_col2)
            )
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2",
                "colocate_with" = "group1"
            );
    """

    sql """
        CREATE TABLE `test_query_colocate2` (
              `datekey` int(11) NULL,
              `rollup_1_condition` int null,
              `rollup_2_condition` int null,
              `sum_col1` bigint(20) SUM NULL,
              `sum_col2` bigint(20) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`datekey`,`rollup_1_condition`,`rollup_2_condition`)
            COMMENT ""
            PARTITION BY RANGE(`datekey`)
            (PARTITION p20220102 VALUES [("20220101"), ("20220102")),
            PARTITION p20220103 VALUES [("20220102"), ("20220103")))
            DISTRIBUTED BY HASH(`datekey`) BUCKETS 1
            rollup (
            rollup_1(datekey, sum_col1),
            rollup_2(datekey, sum_col2)
            )
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2",
                "colocate_with" = "group1"
            );
    """

    sql """
        CREATE TABLE `test_query_no_colocate` (
              `datekey` int(11) NULL,
              `rollup_1_condition` int null,
              `rollup_2_condition` int null,
              `sum_col1` bigint(20) SUM NULL,
              `sum_col2` bigint(20) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`datekey`,`rollup_1_condition`,`rollup_2_condition`)
            COMMENT ""
            PARTITION BY RANGE(`datekey`)
            (PARTITION p20220102 VALUES [("20220101"), ("20220110")),
            PARTITION p20220103 VALUES [("20220110"), ("20220120")))
            DISTRIBUTED BY HASH(`datekey`) BUCKETS 5
            rollup (
            rollup_1(datekey, sum_col1),
            rollup_2(datekey, sum_col2)
            )
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
            );
    """

    sql """insert into test_query_colocate1 values
            (20220101, 102, 200, 200, 100),
            (20220101, 101, 200, 200, 100),
            (20220101, 102, 202, 200, 100),
            (20220101, 101, 202, 200, 100);"""

    sql """insert into test_query_colocate2 values
            (20220101, 102, 200, 200, 100),
            (20220101, 101, 200, 200, 100),
            (20220101, 102, 202, 200, 100),
            (20220101, 101, 202, 200, 100);"""

    sql """insert into test_query_no_colocate values
            (20220101, 102, 200, 200, 100),
            (20220102, 101, 200, 200, 100),
            (20220103, 102, 202, 200, 100),
            (20220104, 101, 202, 200, 100),
            (20220105, 102, 200, 200, 100),
            (20220106, 101, 200, 200, 100),
            (20220107, 102, 202, 200, 100),
            (20220108, 101, 202, 200, 100);"""

    explain {
        sql("""select sum_col1,sum_col2 
            from 
            (select datekey,sum(sum_col1) as sum_col1 from test_query_colocate1 where datekey=20220101 group by datekey) t1
            left join
            (select datekey,sum(sum_col2) as sum_col2 from test_query_colocate1 where datekey=20220101 group by datekey) t2
            on t1.datekey = t2.datekey""")
        contains "COLOCATE"
    }

    explain {
        sql("""select sum_col1,sum_col2 
            from 
            (select datekey,sum(sum_col1) as sum_col1 from test_query_colocate1 where datekey=20220101 group by datekey) t1
            left join
            (select datekey,sum(sum_col1) as sum_col2 from test_query_colocate2 where datekey=20220101 group by datekey) t2
            on t1.datekey = t2.datekey""")
        contains "COLOCATE"
    }

    explain {
        sql("""select sum_col1,sum_col2 
            from 
            (select datekey,sum(sum_col1) as sum_col1 from test_query_colocate1 where datekey=20220101 group by datekey) t1
            left join
            (select datekey,sum(sum_col2) as sum_col2 from test_query_colocate2 where datekey=20220101 group by datekey) t2
            on t1.datekey = t2.datekey""")
        contains "COLOCATE"
    }

    explain {
        // hit same rollup, colocate
        sql("""select sum_col1,sum_col2 
            from 
            (select datekey,sum(sum_col1) as sum_col1 from test_query_no_colocate group by datekey) t1
            left join
            (select datekey,sum(sum_col1) as sum_col2 from test_query_no_colocate group by datekey) t2
            on t1.datekey = t2.datekey""")
        contains "COLOCATE"
    }

    explain {
        // hit same base table, colocate
        sql("""select * 
            from 
            (select datekey from test_query_no_colocate ) t1
            left join
            (select datekey from test_query_no_colocate ) t2
            on t1.datekey = t2.datekey""")
        contains "COLOCATE"
    }

    // ISSUE #18263
    // explain {
    //     // hit diffrent rollup, no colocate
    //     sql("""select sum_col1,sum_col2 
    //             from 
    //             (select datekey,sum(sum_col1) as sum_col1 from test_query_no_colocate group by datekey) t1
    //             left join
    //             (select datekey,sum(sum_col2) as sum_col2 from test_query_no_colocate group by datekey) t2
    //             on t1.datekey = t2.datekey""")
    //     notContains "COLOCATE"
    // }

    sql """ DROP TABLE IF EXISTS test_query_colocate1 """
    sql """ DROP TABLE IF EXISTS test_query_colocate2 """
    sql """ DROP TABLE IF EXISTS test_query_no_colocate """
}
