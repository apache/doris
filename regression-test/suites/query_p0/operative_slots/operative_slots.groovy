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

suite("operative_slots") {
    sql """
    drop table if exists vt;
    CREATE TABLE IF NOT EXISTS vt (
                `user_id` int NOT NULL COMMENT "用户id",
                `name` STRING COMMENT "用户年龄",
                `v` VARIANT NULL
                )
                DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");

    insert into vt values (6, 'doris6', '{"k1" : 100, "k2": 1}'), (7, 'doris7', '{"k1" : 2, "k2": 2}');

    drop table if exists t;
    CREATE TABLE `t` (
    `k` int NULL,
    `v1` bigint NULL,
    `v2` bigint NULL 
    ) ENGINE=OLAP
    UNIQUE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    insert into t values (1, 2, 3);

    set disable_join_reorder = true;
    """


    explain {
        sql "physical plan select * from t join[broadcast] vt on t.k = vt.v['k1'];"
        contains("operativeSlots([k#0, __DORIS_DELETE_SIGN__#3])")
        contains("operativeSlots([v['k1']#17])")
        // expect plan
        //    PhysicalResultSink[311] ( outputExprs=[k#0, v1#1, user_id#4, name#5, v#6] )
        //    +--PhysicalDistribute[306]@6 ( stats=1, distributionSpec=DistributionSpecGather )
        //    +--PhysicalProject[301]@6 ( stats=1, projects=[k#0, v1#1, user_id#4, name#5, v#6] )
        //    +--PhysicalHashJoin[296]@5 ( stats=1, type=INNER_JOIN, hashCondition=[(k#0 = expr_cast(element_at(v, 'k1') as INT)#7)], otherCondition=[], markCondition=[], hint=[broadcast] )
        //            |--PhysicalProject[280]@2 ( stats=1, projects=[k#0, v1#1] )
        //            |  +--PhysicalFilter[275]@1 ( stats=1, predicates=(__DORIS_DELETE_SIGN__#2 = 0) )
        //    |     +--PhysicalOlapScan[t operativeSlots([k#0, __DORIS_DELETE_SIGN__#2])]@0 ( stats=1 )
        //    +--PhysicalDistribute[291]@4 ( stats=1, distributionSpec=DistributionSpecReplicated )
        //          +--PhysicalProject[286]@4 ( stats=1, projects=[user_id#4, name#5, v#6, cast(v['k1']#15 as INT) AS `expr_cast(element_at(v, 'k1') as INT)`#7] )
        //              +--PhysicalOlapScan[vt operativeSlots([v['k1']#15])]@3 ( stats=1 )
    }

    explain {
        sql "physical plan select * from t where v1=0;"
        contains("operativeSlots([v1#1, __DORIS_DELETE_SIGN__#3]")
    }

    explain {
        sql "physical plan select sum(k) from t group by v1;"
        contains("operativeSlots([k#0, v1#1, __DORIS_DELETE_SIGN__#3])")
    }

    explain {
        sql "physical plan select rank() over (partition by v2 order by v1) from t;"
        contains("operativeSlots([v1#1, v2#2, __DORIS_DELETE_SIGN__#3])")
    }
}
