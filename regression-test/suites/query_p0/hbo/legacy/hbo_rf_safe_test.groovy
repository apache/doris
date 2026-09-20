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

suite("hbo_rf_safe_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_rf_safe_test1;"
    sql """create table hbo_rf_safe_test1(a int, b int) duplicate key (a) distributed by hash(b) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_rf_safe_test1 select number, number from numbers("number" = "10000000");"""
    sql """analyze table hbo_rf_safe_test1 with full with sync;"""

    sql "drop table if exists hbo_rf_safe_test2;"
    sql """create table hbo_rf_safe_test2(a int, b int) duplicate key (a) distributed by hash(b) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_rf_safe_test2 select number, number from numbers("number" = "10000000");"""
    sql """analyze table hbo_rf_safe_test2 with full with sync;"""

    // exclude min/max rf to ensure the bloom filter can be generated
    sql "set runtime_filter_type=IN_OR_BLOOM_FILTER;"
    sql "set hbo_rfsafe_threshold=1.0;"
    sql "set global enable_hbo_info_collection=true;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    sql "select count(1) from hbo_rf_safe_test1 t1, hbo_rf_safe_test2 t2 where t1.b = t2.b and t2.a < 20000;"
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                   |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 3.0106000503602E8                                                                                                                                                                                                                                          |
     | PhysicalResultSink[375] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                             |
     | +--PhysicalHashAggregate[370]@7 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[365]@9 ( stats=(hbo)1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[360]@9 ( stats=(hbo)1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[355]@6 ( stats=200,000, projects=[1 AS `1`#8] )                                                                                                                                                                                       |
     |             +--PhysicalHashJoin[350]@5 ( stats=200,000, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#3->[b#1](ndv/size = 200000/262144) ] )                                                           |
     |                |--PhysicalProject[329]@1 ( stats=100,000,000, projects=[b#1] )                                                                                                                                                                                    |
     |                |  +--PhysicalOlapScan[hbo_rf_safe_test1]@0 ( stats=100,000,000, RFs= RF0 )                                                                                                                                                                        |
     |                +--PhysicalDistribute[345]@4 ( stats=200,000, distributionSpec=DistributionSpecReplicated )                                                                                                                                                        |
     |                   +--PhysicalProject[340]@4 ( stats=200,000, projects=[b#3] )                                                                                                                                                                                     |
     |                      +--PhysicalFilter[335]@3 ( stats=200,000, predicates=(a#2 < 200000) )                                                                                                                                                                        |
     |                         +--PhysicalOlapScan[hbo_rf_safe_test2]@2 ( stats=100,000,000 )                                                                                                                                                                            |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        // if rfSafeRatio == 1.0, rf safe, hit hbo cache
        sql "physical plan select count(1) from hbo_rf_safe_test1 t1, hbo_rf_safe_test2 t2 where t1.b = t2.b and t2.a < 20000;"
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

    sql "set hbo_rfsafe_threshold=0.5;"

    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 3.0106000503602E8                                                                                                                                                                                                                                     |
     | PhysicalResultSink[375] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                        |
     | +--PhysicalHashAggregate[370]@7 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[365]@9 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[360]@9 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[355]@6 ( stats=200,000, projects=[1 AS `1`#8] )                                                                                                                                                                                  |
     |             +--PhysicalHashJoin[350]@5 ( stats=200,000, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#3->[b#1](ndv/size = 200000/262144) ] )                                                      |
     |                |--PhysicalProject[329]@1 ( stats=100,000,000, projects=[b#1] )                                                                                                                                                                               |
     |                |  +--PhysicalOlapScan[hbo_rf_safe_test1]@0 ( stats=100,000,000, RFs= RF0 )                                                                                                                                                                   |
     |                +--PhysicalDistribute[345]@4 ( stats=200,000, distributionSpec=DistributionSpecReplicated )                                                                                                                                                   |
     |                   +--PhysicalProject[340]@4 ( stats=200,000, projects=[b#3] )                                                                                                                                                                                |
     |                      +--PhysicalFilter[335]@3 ( stats=200,000, predicates=(a#2 < 200000) )                                                                                                                                                                   |
     |                         +--PhysicalOlapScan[hbo_rf_safe_test2]@2 ( stats=100,000,000 )                                                                                                                                                                       |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        // if rfSafeRatio == 0.5, rf unsafe, not hit hbo cache
        sql "physical plan select count(1) from hbo_rf_safe_test1 t1, hbo_rf_safe_test2 t2 where t1.b = t2.b and t2.a < 20000;"
        contains("stats=1, aggPhase=GLOBAL")
    }

}



