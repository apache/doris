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

suite("hbo_agg_stage_opt_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql """drop table if exists hbo_agg_stage_opt_test;"""
    sql """create table hbo_agg_stage_opt_test(a int, b int, c int, d int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_agg_stage_opt_test select 1,1,1,1 from numbers("number" = "10000000");"""
    sql """analyze table hbo_agg_stage_opt_test with full with sync;"""
    sql """alter table hbo_agg_stage_opt_test modify column a set stats('row_count'='10000000', 'ndv'='10000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'data_size'='2.3043232E7');"""
    sql """alter table hbo_agg_stage_opt_test modify column b set stats('row_count'='10000000', 'ndv'='10000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'data_size'='2.3043232E7');"""
    sql """alter table hbo_agg_stage_opt_test modify column c set stats('row_count'='10000000', 'ndv'='10000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'data_size'='2.3043232E7');"""
    sql """alter table hbo_agg_stage_opt_test modify column d set stats('row_count'='10000000', 'ndv'='10000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='10000000', 'data_size'='2.3043232E7');"""
    sql "set hbo_rfsafe_threshold=1.0;"
    sql "set enable_hbo_optimization=false;";
    sql "set global enable_hbo_info_collection=true;";
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                 |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.633333343543334E8                                                                                                                                                                                                                                                                         |
     | PhysicalResultSink[165] ( outputExprs=[b#1, c#2, __count_2#4] )                                                                                                                                                                                                                                    |
     | +--PhysicalDistribute[160]@2 ( stats=100,000,000, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                        |
     |    +--PhysicalHashAggregate[155]@2 ( stats=100,000,000, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, count(partial_count(*)#5) AS `count(*)`#4], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false )   |
     |       +--PhysicalDistribute[150]@4 ( stats=100,000,000, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[1, 2], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[1], [2]], exprIdToEquivalenceSet={1=0, 2=1} ) )               |
     |          +--PhysicalHashAggregate[145]@4 ( stats=100,000,000, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, partial_count(*) AS `partial_count(*)`#5], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false ) |
     |             +--PhysicalProject[140]@1 ( stats=100,000,000, projects=[b#1, c#2] )                                                                                                                                                                                                                   |
     |                +--PhysicalOlapScan[hbo_agg_stage_opt_test]@0 ( stats=100,000,000 )
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select b, c, count(1) from hbo_agg_stage_opt_test group by b, c;"
        contains("stats=10,000,000, aggPhase=GLOBAL")
    }

    sql "select b, c, count(1) from hbo_agg_stage_opt_test group by b, c;"
    sleep(3000)
    sql "set enable_hbo_optimization=true;";
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                            |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 3.0000000365433335E8                                                                                                                                                                                                                                                                   |
     | PhysicalResultSink[165] ( outputExprs=[b#1, c#2, __count_2#4] )                                                                                                                                                                                                                               |
     | +--PhysicalDistribute[160]@2 ( stats=(hbo)1, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                        |
     |    +--PhysicalHashAggregate[155]@2 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, count(partial_count(*)#5) AS `count(*)`#4], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false )   |
     |       +--PhysicalDistribute[150]@4 ( stats=(hbo)1, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[1, 2], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[1], [2]], exprIdToEquivalenceSet={1=0, 2=1} ) )               |
     |          +--PhysicalHashAggregate[145]@4 ( stats=(hbo)1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, partial_count(*) AS `partial_count(*)`#5], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false ) |
     |             +--PhysicalProject[140]@1 ( stats=100,000,000, projects=[b#1, c#2] )                                                                                                                                                                                                              |
     |                +--PhysicalOlapScan[hbo_agg_stage_opt_test]@0 ( stats=100,000,000 )
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+*/
    explain {
        sql "physical plan select b, c, count(1) from hbo_agg_stage_opt_test group by b, c;"
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

}
