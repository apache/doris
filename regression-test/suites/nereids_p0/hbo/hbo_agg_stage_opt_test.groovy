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

suite("hbo_agg_stage_opt_test") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql """drop table if exists hbo_agg_stage_opt_test;"""
    sql """create table hbo_agg_stage_opt_test(a int, b int, c int, d int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_agg_stage_opt_test select 1,1,1,1 from numbers("number" = "100000000");"""
    sql """analyze table hbo_agg_stage_opt_test with full with sync;"""
    sql """alter table hbo_agg_stage_opt_test modify column a set stats('row_count'='100000000', 'ndv'='100000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='100000000', 'data_size'='2.3043232E7');"""
    sql """alter table hbo_agg_stage_opt_test modify column b set stats('row_count'='100000000', 'ndv'='100000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='100000000', 'data_size'='2.3043232E7');"""
    sql """alter table hbo_agg_stage_opt_test modify column c set stats('row_count'='100000000', 'ndv'='100000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='100000000', 'data_size'='2.3043232E7');"""
    sql """alter table hbo_agg_stage_opt_test modify column d set stats('row_count'='100000000', 'ndv'='100000000', 'num_nulls'='0', 'min_value'='1', 'max_value'='100000000', 'data_size'='2.3043232E7');"""
    sql "set hbo_rfsafe_threshold=1.0;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                 |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.383333343543334E8                                                                                                                                                                                                                                                                      |
     | PhysicalResultSink[152] ( outputExprs=[b#1, c#2, __count_2#4] )                                                                                                                                                                                                                                 |
     | +--PhysicalHashAggregate[147]@2 ( stats=100,000,000, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, count(partial_count(*)#5) AS `count(*)`#4], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false )   |
     |    +--PhysicalDistribute[142]@4 ( stats=100,000,000, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[1, 2], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[1], [2]], exprIdToEquivalenceSet={1=0, 2=1} ) )               |
     |       +--PhysicalHashAggregate[137]@4 ( stats=100,000,000, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, partial_count(*) AS `partial_count(*)`#5], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[132]@1 ( stats=100,000,000, projects=[b#1, c#2] )                                                                                                                                                                                                                   |
     |             +--PhysicalOlapScan[hbo_agg_stage_opt_test]@0 ( stats=100,000,000 )                                                                                                                                                                                                                 |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select b, c, count(1) from hbo_agg_stage_opt_test group by b, c;"
        contains("stats=100,000,000, aggPhase=LOCAL")
        contains("stats=100,000,000, aggPhase=GLOBAL")
    }

    sql "select b, c, count(1) from hbo_agg_stage_opt_test group by b, c;"
    sleep(3000)
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                            |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 3.0000000340433335E8                                                                                                                                                                                                                                                                |
     | PhysicalResultSink[152] ( outputExprs=[b#1, c#2, __count_2#4] )                                                                                                                                                                                                                            |
     | +--PhysicalHashAggregate[147]@2 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, count(partial_count(*)#5) AS `count(*)`#4], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false )   |
     |    +--PhysicalDistribute[142]@4 ( stats=(hbo)1, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[1, 2], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[1], [2]], exprIdToEquivalenceSet={1=0, 2=1} ) )               |
     |       +--PhysicalHashAggregate[137]@4 ( stats=(hbo)1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[b#1, c#2], outputExpr=[b#1, c#2, partial_count(*) AS `partial_count(*)`#5], partitionExpr=Optional[[b#1, c#2]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[132]@1 ( stats=100,000,000, projects=[b#1, c#2] )                                                                                                                                                                                                              |
     |             +--PhysicalOlapScan[hbo_agg_stage_opt_test]@0 ( stats=100,000,000 )                                                                                                                                                                                                            |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+*/
    explain {
        sql "physical plan select b, c, count(1) from hbo_agg_stage_opt_test group by b, c;"
        contains("stats=(hbo)1, aggPhase=LOCAL")
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

}
