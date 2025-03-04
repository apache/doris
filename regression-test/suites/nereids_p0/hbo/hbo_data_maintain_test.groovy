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

suite("hbo_data_maintain_test") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_data_maintain_test1;"
    sql "create table hbo_data_maintain_test1(a int, b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"
    sql "insert into hbo_data_maintain_test1 select number, number from numbers("number" = "100000");"
    sql "insert into hbo_data_maintain_test1 select 1,1 from numbers("number" = "100000000");"
    sql "analyze table hbo_data_maintain_test1 with full with sync;"

    sql "drop table if exists hbo_data_maintain_test2;"
    sql "create table hbo_data_maintain_test2(a int, b int) distributed by hash(a) buckets 32 properties("replication_num"="1");"
    sql "insert into hbo_data_maintain_test2 select number, number from numbers("number" = "100000");"
    sql "analyze table hbo_data_maintain_test2 with full with sync;"


    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 1.0030532524420181E8                                                                                                                                                                                                                                  |
     | PhysicalResultSink[387] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                        |
     | +--PhysicalHashAggregate[382]@7 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[377]@9 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[372]@9 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[367]@6 ( stats=1,004.59, projects=[1 AS `1`#8] )                                                                                                                                                                                 |
     |             +--PhysicalHashJoin[362]@5 ( stats=1,004.59, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#1->[b#3](ndv/size = 1002/1024) , RF1[b#1->[b#3](ndv/size = 1002/1024) ] )                  |
     |                |--PhysicalProject[341]@4 ( stats=100,000, projects=[b#3] )                                                                                                                                                                                   |
     |                |  +--PhysicalOlapScan[hbo_data_maintain_test2]@3 ( stats=100,000, RFs= RF0 RF1 )                                                                                                                                                             |
     |                +--PhysicalDistribute[357]@2 ( stats=1,002.8, distributionSpec=DistributionSpecReplicated )                                                                                                                                                   |
     |                   +--PhysicalProject[352]@2 ( stats=1,002.8, projects=[b#1] )                                                                                                                                                                                |
     |                      +--PhysicalFilter[347]@1 ( stats=1,002.8, predicates=(a#0 = 1) )                                                                                                                                                                        |
     |                         +--PhysicalOlapScan[hbo_data_maintain_test1]@0 ( stats=100,100,000 )                                                                                                                                                                 |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_data_maintain_test1 s1, hbo_data_maintain_test2 s2 where s1.b = s2.b and s1.a = 1;"
        nocontains("(hbo)")
        contains("PhysicalFilter[347]@1 ( stats=1,002.8, predicates=(a#0 = 1) )")
        contains("PhysicalHashJoin[362]@5 ( stats=1,004.59, type=INNER_JOIN")
    }

    sql "select count(1) from hbo_data_maintain_test1 s1, hbo_data_maintain_test2 s2 where s1.b = s2.b and s1.a = 1;"


    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                   |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.0043000903602E8                                                                                                                                                                                                                                          |
     | PhysicalResultSink[384] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                             |
     | +--PhysicalHashAggregate[379]@7 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[374]@9 ( stats=(hbo)1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[369]@9 ( stats=(hbo)1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[364]@6 ( stats=100,000,001, projects=[1 AS `1`#8] )                                                                                                                                                                                   |
     |             +--PhysicalHashJoin[359]@5 ( stats=100,000,001, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[] )                                                                                                                   |
     |                |--PhysicalProject[343]@2 ( stats=100,000,001, projects=[b#1] )                                                                                                                                                                                    |
     |                |  +--PhysicalFilter[338]@1 ( stats=100,000,001, predicates=(a#0 = 1) )                                                                                                                                                                            |
     |                |     +--PhysicalOlapScan[hbo_data_maintain_test1]@0 ( stats=100,100,000 )                                                                                                                                                                         |
     |                +--PhysicalDistribute[354]@4 ( stats=100,000, distributionSpec=DistributionSpecReplicated )                                                                                                                                                        |
     |                   +--PhysicalProject[349]@4 ( stats=100,000, projects=[b#3] )                                                                                                                                                                                     |
     |                      +--PhysicalOlapScan[hbo_data_maintain_test2]@3 ( stats=100,000 )                                                                                                                                                                             |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_data_maintain_test1 s1, hbo_data_maintain_test2 s2 where s1.b = s2.b and s1.a = 1;"
        contains("PhysicalFilter[338]@1 ( stats=100,000,001, predicates=(a#0 = 1) )")
        contains("PhysicalHashJoin[359]@5 ( stats=100,000,001, type=INNER_JOIN")
        contains("PhysicalHashAggregate[379]@7 ( stats=(hbo)1, aggPhase=GLOBAL")
    }

    // data maintain
    sql "delete from hbo_data_maintain_test1 where a = 1;"

    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 1.0030532524420181E8                                                                                                                                                                                                                                  |
     | PhysicalResultSink[387] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                        |
     | +--PhysicalHashAggregate[382]@7 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[377]@9 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[372]@9 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[367]@6 ( stats=1,004.59, projects=[1 AS `1`#8] )                                                                                                                                                                                 |
     |             +--PhysicalHashJoin[362]@5 ( stats=1,004.59, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#1->[b#3](ndv/size = 1002/1024) , RF1[b#1->[b#3](ndv/size = 1002/1024) ] )                  |
     |                |--PhysicalProject[341]@4 ( stats=100,000, projects=[b#3] )                                                                                                                                                                                   |
     |                |  +--PhysicalOlapScan[hbo_data_maintain_test2]@3 ( stats=100,000, RFs= RF0 RF1 )                                                                                                                                                             |
     |                +--PhysicalDistribute[357]@2 ( stats=1,002.8, distributionSpec=DistributionSpecReplicated )                                                                                                                                                   |
     |                   +--PhysicalProject[352]@2 ( stats=1,002.8, projects=[b#1] )                                                                                                                                                                                |
     |                      +--PhysicalFilter[347]@1 ( stats=1,002.8, predicates=(a#0 = 1) )                                                                                                                                                                        |
     |                         +--PhysicalOlapScan[hbo_data_maintain_test1]@0 ( stats=100,100,000 )                                                                                                                                                                 |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_data_maintain_test1 s1, hbo_data_maintain_test2 s2 where s1.b = s2.b and s1.a = 1;"
        // data of table has been changed and can't hit hbo cache
        nocontains("(hbo)")
    }

    sql "select count(1) from hbo_data_maintain_test1 s1, hbo_data_maintain_test2 s2 where s1.b = s2.b and s1.a = 1;"

    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                   |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 1.0030000302102E8                                                                                                                                                                                                                                          |
     | PhysicalResultSink[387] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                             |
     | +--PhysicalHashAggregate[382]@7 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )    |
     |    +--PhysicalDistribute[377]@9 ( stats=(hbo)0, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[372]@9 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[367]@6 ( stats=0, projects=[1 AS `1`#8] )                                                                                                                                                                                             |
     |             +--PhysicalHashJoin[362]@5 ( stats=0, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#1->[b#3](ndv/size = 1/1) , RF1[b#1->[b#3](ndv/size = 1/1) ] )                                          |
     |                |--PhysicalProject[341]@4 ( stats=100,000, projects=[b#3] )                                                                                                                                                                                        |
     |                |  +--PhysicalOlapScan[hbo_data_maintain_test2]@3 ( stats=100,000, RFs= RF0 RF1 )                                                                                                                                                                  |
     |                +--PhysicalDistribute[357]@2 ( stats=0, distributionSpec=DistributionSpecReplicated )                                                                                                                                                              |
     |                   +--PhysicalProject[352]@2 ( stats=0, projects=[b#1] )                                                                                                                                                                                           |
     |                      +--PhysicalFilter[347]@1 ( stats=0, predicates=(a#0 = 1) )                                                                                                                                                                                   |
     |                         +--PhysicalOlapScan[hbo_data_maintain_test1]@0 ( stats=100,100,000 )                                                                                                                                                                      |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_data_maintain_test1 s1, hbo_data_maintain_test2 s2 where s1.b = s2.b and s1.a = 1;"
        // hit hbo cache again
        contains("PhysicalHashAggregate[382]@7 ( stats=(hbo)1, aggPhase=GLOBAL")
    }
}



