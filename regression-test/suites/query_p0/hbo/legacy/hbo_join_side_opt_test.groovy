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

suite("hbo_join_side_opt_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_join_side_opt_test1;"
    sql """create table hbo_join_side_opt_test1(a int, b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_join_side_opt_test1 select number, number from numbers("number" = "10000");"""
    sql """insert into hbo_join_side_opt_test1 select 1,1 from numbers("number" = "10000000");"""
    sql """analyze table hbo_join_side_opt_test1 with full with sync;"""

    sql "drop table if exists hbo_join_side_opt_test2;"
    sql """create table hbo_join_side_opt_test2(a int, b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_join_side_opt_test2 select number, number from numbers("number" = "10000");"""
    sql """analyze table hbo_join_side_opt_test2 with full with sync;"""
    sql "set hbo_rfsafe_threshold=1.0;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    sleep(3000)
    
    sql "set enable_hbo_optimization=false;"
    sql "set global enable_hbo_info_collection=true;"
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 1.0030532524420181E8                                                                                                                                                                                                                                  |
     | PhysicalResultSink[391] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                        |
     | +--PhysicalHashAggregate[386]@7 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[381]@9 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[376]@9 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[371]@6 ( stats=1,004.59, projects=[1 AS `1`#8] )                                                                                                                                                                                 |
     |             +--PhysicalHashJoin[366]@5 ( stats=1,004.59, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#1->[b#3](ndv/size = 1002/1024) , RF1[b#1->[b#3](ndv/size = 1002/1024) ] )                  |
     |                |--PhysicalProject[345]@4 ( stats=100,000, projects=[b#3] )                                                                                                                                                                                   |
     |                |  +--PhysicalOlapScan[hbo_join_side_opt_test2]@3 ( stats=100,000, RFs= RF0 RF1 )                                                                                                                                                             |
     |                +--PhysicalDistribute[361]@2 ( stats=1,002.8, distributionSpec=DistributionSpecReplicated )                                                                                                                                                   |
     |                   +--PhysicalProject[356]@2 ( stats=1,002.8, projects=[b#1] )                                                                                                                                                                                |
     |                      +--PhysicalFilter[351]@1 ( stats=1,002.8, predicates=(a#0 = 1) )                                                                                                                                                                        |
     |                         +--PhysicalOlapScan[hbo_join_side_opt_test1]@0 ( stats=100,100,000 )                                                                                                                                                                 |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+*/
    explain {
        sql "physical plan select count(1) from hbo_join_side_opt_test1 s1, hbo_join_side_opt_test2 s2 where s1.b = s2.b and s1.a = 1;"
        contains("stats=1, aggPhase=GLOBAL")
    }

    sql "select count(1) from hbo_join_side_opt_test1 s1, hbo_join_side_opt_test2 s2 where s1.b = s2.b and s1.a = 1;"
    sleep(3000)
    sql "set enable_hbo_optimization=true;"
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                   |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.0043000903602E8                                                                                                                                                                                                                                          |
     | PhysicalResultSink[388] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                             |
     | +--PhysicalHashAggregate[383]@7 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[378]@9 ( stats=(hbo)1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[373]@9 ( stats=(hbo)1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[368]@6 ( stats=100,000,001, projects=[1 AS `1`#8] )                                                                                                                                                                                   |
     |             +--PhysicalHashJoin[363]@5 ( stats=(hbo)100,000,001, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[] )                                                                                                              |
     |                |--PhysicalProject[347]@2 ( stats=100,000,001, projects=[b#1] )                                                                                                                                                                                    |
     |                |  +--PhysicalFilter[342]@1 ( stats=(hbo)100,000,001, predicates=(a#0 = 1) )                                                                                                                                                                       |
     |                |     +--PhysicalOlapScan[hbo_join_side_opt_test1]@0 ( stats=100,100,000 )                                                                                                                                                                         |
     |                +--PhysicalDistribute[358]@4 ( stats=100,000, distributionSpec=DistributionSpecReplicated )                                                                                                                                                        |
     |                   +--PhysicalProject[353]@4 ( stats=100,000, projects=[b#3] )                                                                                                                                                                                     |
     |                      +--PhysicalOlapScan[hbo_join_side_opt_test2]@3 ( stats=100,000 )
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_join_side_opt_test1 s1, hbo_join_side_opt_test2 s2 where s1.b = s2.b and s1.a = 1;"
        contains("stats=(hbo)10,000,001, predicates=(a#0 = 1)")
        contains("stats=10,000, distributionSpec=DistributionSpecReplicated")
        contains("stats=(hbo)10,000,001, type=INNER_JOIN")
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

}
