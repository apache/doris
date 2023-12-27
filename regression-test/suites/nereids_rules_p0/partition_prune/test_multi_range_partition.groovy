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

suite("test_multi_range_partition") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set partition_pruning_expand_threshold=10;"
    sql "drop table if exists pt"
    sql """
        CREATE TABLE `pt` (
        `k1` int(11) NULL COMMENT "",
        `k2` int(11) NULL COMMENT "",
        `k3` int(11) NULL COMMENT ""
        ) 
        PARTITION BY RANGE(`k1`, `k2`)
        (PARTITION p1 VALUES LESS THAN ("3", "1"),
        PARTITION p2 VALUES [("3", "1"), ("7", "10")),
        PARTITION p3 VALUES [("7", "10"), ("10", "15")))
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES ('replication_num' = '1');
        """
    sql "insert into pt values (7, 0, 0);"
    sql "insert into pt(k1) values (7);"
    sql "insert into pt(k1) values (0);"
    sql "insert into pt values (7, 11, 0);"

    // basic test
    explain{
        sql "select * from pt where k1=7;"
        contains "partitions=2/3 (p2,p3)"
    }

    explain{
        sql "select * from pt where k1=10;"
        contains "partitions=1/3 (p3)"
    }

    explain{
        sql "select * from pt where k1=11;"
        contains "VEMPTYSET"
    }

    explain{
        sql "select * from pt where k1=-1;"
        contains "partitions=1/3 (p1)"
    }

    // ===============function(part_key)===================
    explain{
        sql "select * from pt where 2*k1=20; --漏裁 p1"
        contains "partitions=2/3 (p1,p3)"
    }
    explain{
        sql "select * from pt where 2*(k1+1)=22; --漏裁 p1"
        contains "partitions=2/3 (p1,p3)"
    }
    explain{
        sql "select * from pt where sin(k1)=0"
        contains "artitions=3/3 (p1,p2,p3)"
    }

    // fix BUG: p1 missed
    explain{
        sql "select * from pt where sin(k1)<>0"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    // ============= in predicate ======================
    explain{
        sql "select * from pt where k1 in (7, 8);"
        contains "partitions=2/3 (p2,p3)"
    }

    explain{
        sql "select * from pt where k1 in (15, 18);"
        contains "VEMPTYSET"
    }

    // =========== is null ===================
    explain{
        sql "select * from pt where k1 is null"
        contains "partitions=1/3 (p1)"
    }

    // fix BUG: p1 missed
    explain{
        sql "select * from pt where k1 is not null"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    //======== the second partition key =========
    explain{
        sql "select * from pt where k1=7 and (k1>k2);"
        contains "partitions=1/3 (p2)"
    }

    explain {
        sql "select * from pt where k1=7 and not (k1>k2);"
        contains "partitions=2/3 (p2,p3)"
    }

    // p3 NOT pruned
    explain {
        sql "select * from pt where k1=7 and (k1<k2);"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql "select * from pt where k1=7 and k1=k2"
        contains "partitions=1/3 (p2)"
    }

    //fix BUG: p2 missed
    explain {
        sql "select * from pt where k1=7 and k1<>k2"
        contains "partitions=2/3 (p2,p3)"
    }

    //p3 NOT pruned
    explain {
        sql "select * from pt where k1=7 and (k1 > cast(k2 as bigint));"
        contains "partitions=2/3 (p2,p3)"
    }

    //fix BUG: p2 missed
    explain {
        sql "select * from pt where k1=7 and not (k2 is null);"
        contains "partitions=2/3 (p2,p3)"
    }
    
    //p3 NOT pruned
    explain {
        sql "select * from pt where k1=7 and not (k2 is not null);"
        contains "partitions=2/3 (p2,p3)"
    }

    //fix BUG: p2 missed
    explain {
        sql "select * from pt where k1=7 and k2 not in (1, 2);"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql "select * from pt where k1=7 and k2 in (1, 12);"
        contains "partitions=2/3 (p2,p3)"
    }

    //fix BUG: p2,p3 pruned
    explain {
        sql "select * from pt where k1=7 and k2 not in (1, 12)"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql" select * from pt where k1=7 and k2 in (0);"
        contains "partitions=1/3 (p2)"
    }

    explain {
        sql "select * from pt where k1=7 and k2 in (11)"
        contains "partitions=1/3 (p3)"
    }

    explain {
        sql "select * from pt where k1=7 and k2 in (null);"
        contains "VEMPTYSET"
    }

    explain {
        sql "select * from pt where k1=7 and k2 not in (null);"
        contains "VEMPTYSET"
    }

    explain {
        sql "select * from pt where k1=10 and k2 in (30);"
        contains "VEMPTYSET"
    }

    explain {
        sql "select * from pt where k1=7 and k1 > k3;"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql "select * from pt where k1=7 and k1 <> k3;"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql "select * from pt where k2 in (null);"
        contains "VEMPTYSET"
    }

    // p1/p2/p3 NOT pruned
    explain {
        sql "select * from pt where k2 not in (null)"
        contains "VEMPTYSET"
    }

    explain {
        sql "select * from pt where k2 in (0)"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    explain {
        sql "select * from pt where k2 > 100"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    explain {
        sql "select * from pt where k2 < -1"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    explain {
        sql "select * from pt where k1=7 and (k3 is null)"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql "select * from pt where k1=7 and not (k3 is null);"
        contains "partitions=2/3 (p2,p3)"
    }

    
    // test if a range is not sliced to multiple single point
    // for example: range [3,7) is sliced to [3,3], [4,4],[5,5],[6,6]
    sql "set partition_pruning_expand_threshold=1;"

    explain {
        sql "select * from pt where k1 < 5;"
        contains "partitions=2/3 (p1,p2)"
    }

    explain {
        sql "select * from pt where not k1 < 5;"
        contains "partitions=2/3 (p2,p3)"
    }

    explain {
        sql "select * from pt where k2 < 5;"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    explain {
        sql "select * from pt where not k2 < 5;"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    explain {
        sql "select * from pt where k3 < 5;"
        contains "partitions=3/3 (p1,p2,p3)"
    }

    explain {
        sql "select * from pt where not k3 < 5;"
        contains "partitions=3/3 (p1,p2,p3)"
    }
}
