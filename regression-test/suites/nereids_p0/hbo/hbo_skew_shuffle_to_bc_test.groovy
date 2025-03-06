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

suite("hbo_skew_shuffle_to_bc_test") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql """drop table if exists hbo_skew_shuffle_to_bc_test1;"""
    sql """create table hbo_skew_shuffle_to_bc_test1(a int, b int) distributed by hash(a, b) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_skew_shuffle_to_bc_test1 select number, 1 from numbers("number" = "1000");"""
    sql """insert into hbo_skew_shuffle_to_bc_test1 select number, number from numbers("number" = "10000000");"""
    sql """analyze table hbo_skew_shuffle_to_bc_test1 with full with sync;"""

    sql """drop table if exists hbo_skew_shuffle_to_bc_test2;"""
    sql """create table hbo_skew_shuffle_to_bc_test2(a int, b int) distributed by hash(a, b) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_skew_shuffle_to_bc_test2 select number, number from numbers("number" = "2000000");"""
    sql """insert into hbo_skew_shuffle_to_bc_test2 select 1, 1 from numbers("number" = "2000000");"""
    sql """insert into hbo_skew_shuffle_to_bc_test2 select number, number from numbers("number" = "6000000");"""
    sql """analyze table hbo_skew_shuffle_to_bc_test2 with full with sync;"""

    // increase the parallel to make enough skew between different instances
    sql "set parallel_pipeline_task_num=100;"
    sql "set hbo_rfsafe_threshold=1.0;"
    sql "set enable_hbo_optimization=false;"
    /**
     +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                 |
     +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 8.8669105036E7                                                                                                                                                                                                                                                           |
     | PhysicalResultSink[345] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                                           |
     | +--PhysicalHashAggregate[340]@6 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )                       |
     |    +--PhysicalDistribute[335]@8 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                                            |
     |       +--PhysicalHashAggregate[330]@8 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )                    |
     |          +--PhysicalProject[325]@5 ( stats=10,000,000, projects=[1 AS `1`#8] )                                                                                                                                                                                                  |
     |             +--PhysicalHashJoin[320]@4 ( stats=10,000,000, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#3->[b#1](ndv/size = 6028862/8388608) , RF1[b#3->[b#1](ndv/size = 6028862/8388608) ] )                       |
     |                |--PhysicalDistribute[304]@1 ( stats=10,001,000, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[1], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[1]], exprIdToEquivalenceSet={1=0} ) ) |
     |                |  +--PhysicalProject[299]@1 ( stats=10,001,000, projects=[b#1] )                                                                                                                                                                                                |
     |                |     +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test1]@0 ( stats=10,001,000, RFs= RF0 RF1 )                                                                                                                                                                     |
     |                +--PhysicalDistribute[315]@3 ( stats=10,000,000, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[3], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[3]], exprIdToEquivalenceSet={3=0} ) ) |
     |                   +--PhysicalProject[310]@3 ( stats=10,000,000, projects=[b#3] )                                                                                                                                                                                                |
     |                      +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test2]@2 ( stats=10,000,000 )                                                                                                                                                                                   |
     +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_skew_shuffle_to_bc_test1 t1, hbo_skew_shuffle_to_bc_test2 t2 where t1.b = t2.b;"
        contains("stats=10,001,000, distributionSpec=DistributionSpecHash")
        contains("stats=10,000,000, distributionSpec=DistributionSpecHash")
    }

    sql "select count(1) from hbo_skew_shuffle_to_bc_test1 t1, hbo_skew_shuffle_to_bc_test2 t2 where t1.b = t2.b;"
    sleep(3000);
    sql "set enable_hbo_optimization=true;"
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                    |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 6.081327675576689E9                                                                                                                                                                                                                                         |
     | PhysicalResultSink[337] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                              |
     | +--PhysicalHashAggregate[332]@6 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )     |
     |    +--PhysicalDistribute[327]@8 ( stats=(hbo)100, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[322]@8 ( stats=(hbo)100, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[317]@5 ( stats=2,010,002,000, projects=[1 AS `1`#8] )                                                                                                                                                                                  |
     |             +--PhysicalHashJoin[312]@4 ( stats=2,010,002,000, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#3->[b#1](ndv/size = 6028862/8388608) , RF1[b#3->[b#1](ndv/size = 6028862/8388608) ] )       |
     |                |--PhysicalProject[296]@1 ( stats=10,001,000, projects=[b#1] )                                                                                                                                                                                      |
     |                |  +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test1]@0 ( stats=10,001,000, RFs= RF0 RF1 )                                                                                                                                                           |
     |                +--PhysicalDistribute[307]@3 ( stats=10,000,000, distributionSpec=DistributionSpecReplicated )                                                                                                                                                      |
     |                   +--PhysicalProject[302]@3 ( stats=10,000,000, projects=[b#3] )                                                                                                                                                                                   |
     |                      +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test2]@2 ( stats=10,000,000 )                                                                                                                                                                      |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_skew_shuffle_to_bc_test1 t1, hbo_skew_shuffle_to_bc_test2 t2 where t1.b = t2.b;"
        contains("stats=10,001,000, projects=[b#1]")
        contains("stats=10,000,000, distributionSpec=DistributionSpecReplicated")
        contains("stats=(hbo)100, aggPhase=LOCAL")
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

}
