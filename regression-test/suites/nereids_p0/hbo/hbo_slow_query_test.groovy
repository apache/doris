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

suite("hbo_slow_query_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_slow_query_test;"
    sql """create table hbo_slow_query_test(a int, b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_slow_query_test select number, number from numbers("number" = "10000000");"""
    sql """analyze table hbo_slow_query_test with full with sync;"""
    
    sql "set hbo_rfsafe_threshold=1.0;"
    sql "set global enable_hbo_info_collection=true;"
    sql "set enable_hbo_optimization=true;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10000"); """
    sql "select count(1) from hbo_slow_query_test where a > 0;"
    sleep(3000)
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 3.0000000303602004E8                                                                                                                                                                                                                                  |
     | PhysicalResultSink[233] ( outputExprs=[__count_0#2] )                                                                                                                                                                                                        |
     | +--PhysicalHashAggregate[228]@3 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#7) AS `count(*)`#2], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[223]@5 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[218]@5 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#7], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[213]@2 ( stats=100,000,000, projects=[1 AS `1`#6] )                                                                                                                                                                              |
     |             +--PhysicalFilter[208]@1 ( stats=100,000,000, predicates=(a#0 > 0) )                                                                                                                                                                             |
     |                +--PhysicalOlapScan[hbo_slow_query_test]@0 ( stats=100,000,000 )                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_slow_query_test where a > 0;"
        contains("stats=1, aggPhase=GLOBAL")
    }

    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    sql "select count(1) from hbo_slow_query_test where a > 0;"
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                   |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 3.0000000103602004E8                                                                                                                                                                                                                                       |
     | PhysicalResultSink[233] ( outputExprs=[__count_0#2] )                                                                                                                                                                                                             |
     | +--PhysicalHashAggregate[228]@3 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#7) AS `count(*)`#2], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[223]@5 ( stats=(hbo)1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[218]@5 ( stats=(hbo)1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#7], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[213]@2 ( stats=99,999,999, projects=[1 AS `1`#6] )                                                                                                                                                                                    |
     |             +--PhysicalFilter[208]@1 ( stats=99,999,999, predicates=(a#0 > 0) )                                                                                                                                                                                   |
     |                +--PhysicalOlapScan[hbo_slow_query_test]@0 ( stats=100,000,000 )                                                                                                                                                                                   |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_slow_query_test where a > 0;"
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

}
