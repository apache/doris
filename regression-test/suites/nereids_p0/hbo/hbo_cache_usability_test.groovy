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

suite("hbo_cache_usability_test") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists cache_usability_store_sales_p;"
    sql """CREATE TABLE `cache_usability_store_sales_p` (
    `ss_item_sk` bigint NOT NULL,
    `ss_ticket_number` bigint NOT NULL,
    `ss_sold_date_sk` bigint NULL,
    `ss_customer_sk` bigint NULL,
    `ss_store_sk` bigint NULL,
    `ss_wholesale_cost` decimal(7,2) NULL,
    `ss_list_price` decimal(7,2) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ss_item_sk`, `ss_ticket_number`)
    PARTITION BY RANGE(`ss_sold_date_sk`)
    (
    PARTITION `p1` VALUES LESS THAN ("2450846"),
    PARTITION `p2` VALUES LESS THAN ("2450874"),
    PARTITION `p3` VALUES LESS THAN ("2450905"),
    PARTITION `p4` VALUES LESS THAN ("2450935"),
    PARTITION `p5` VALUES LESS THAN ("2450966"),
    PARTITION `p6` VALUES LESS THAN ("2450996"),
    PARTITION `p7` VALUES LESS THAN ("2451027"),
    PARTITION `p8` VALUES LESS THAN ("2451058"),
    PARTITION `p9` VALUES LESS THAN ("2451088"),
    PARTITION `p10` VALUES LESS THAN ("2451119"),
    PARTITION `p11` VALUES LESS THAN ("2451149"),
    PARTITION `p12` VALUES LESS THAN ("2451180"),
    PARTITION `p13` VALUES LESS THAN ("2451211"),
    PARTITION `p14` VALUES LESS THAN ("2451239"),
    PARTITION `p15` VALUES LESS THAN ("2451270"),
    PARTITION `p16` VALUES LESS THAN ("2451300"),
    PARTITION `p17` VALUES LESS THAN ("2451331"),
    PARTITION `p18` VALUES LESS THAN ("2451361"),
    PARTITION `p19` VALUES LESS THAN ("2451392"),
    PARTITION `p20` VALUES LESS THAN ("2451423"),
    PARTITION `p21` VALUES LESS THAN ("2451453"),
    PARTITION `p22` VALUES LESS THAN ("2451484"),
    PARTITION `p23` VALUES LESS THAN ("2451514"),
    PARTITION `p24` VALUES LESS THAN ("2451545"),
    PARTITION `p25` VALUES LESS THAN ("2451576"),
    PARTITION `p26` VALUES LESS THAN ("2451605"),
    PARTITION `p27` VALUES LESS THAN ("2451635"),
    PARTITION `p28` VALUES LESS THAN ("2451666"),
    PARTITION `p29` VALUES LESS THAN ("2451696"),
    PARTITION `p30` VALUES LESS THAN ("2451726"),
    PARTITION `p31` VALUES LESS THAN ("2451756"),
    PARTITION `p32` VALUES LESS THAN ("2451787"),
    PARTITION `p33` VALUES LESS THAN ("2451817"),
    PARTITION `p34` VALUES LESS THAN ("2451848"),
    PARTITION `p35` VALUES LESS THAN ("2451877"),
    PARTITION `p36` VALUES LESS THAN ("2451906"),
    PARTITION `p37` VALUES LESS THAN ("2451937"),
    PARTITION `p38` VALUES LESS THAN ("2451968"),
    PARTITION `p39` VALUES LESS THAN ("2451999"),
    PARTITION `p40` VALUES LESS THAN ("2452031"),
    PARTITION `p41` VALUES LESS THAN ("2452062"),
    PARTITION `p42` VALUES LESS THAN ("2452092"),
    PARTITION `p43` VALUES LESS THAN ("2452123"),
    PARTITION `p44` VALUES LESS THAN ("2452154"),
    PARTITION `p45` VALUES LESS THAN ("2452184"),
    PARTITION `p46` VALUES LESS THAN ("2452215"),
    PARTITION `p47` VALUES LESS THAN ("2452245"),
    PARTITION `p48` VALUES LESS THAN ("2452276"),
    PARTITION `p49` VALUES LESS THAN ("2452307"),
    PARTITION `p50` VALUES LESS THAN ("2452335"),
    PARTITION `p51` VALUES LESS THAN ("2452366"),
    PARTITION `p52` VALUES LESS THAN ("2452396"),
    PARTITION `p53` VALUES LESS THAN ("2452427"),
    PARTITION `p54` VALUES LESS THAN ("2452457"),
    PARTITION `p55` VALUES LESS THAN ("2452488"),
    PARTITION `p56` VALUES LESS THAN ("2452519"),
    PARTITION `p57` VALUES LESS THAN ("2452549"),
    PARTITION `p58` VALUES LESS THAN ("2452580"),
    PARTITION `p59` VALUES LESS THAN ("2452610"),
    PARTITION `p60` VALUES LESS THAN ("2452641"),
    PARTITION `p61` VALUES LESS THAN ("2452672"),
    PARTITION `p62` VALUES LESS THAN ("2452700"),
    PARTITION `p63` VALUES LESS THAN ("2452731"),
    PARTITION `p64` VALUES LESS THAN ("2452761"),
    PARTITION `p65` VALUES LESS THAN ("2452792"),
    PARTITION `p66` VALUES LESS THAN ("2452822"),
    PARTITION `p67` VALUES LESS THAN ("2452853"),
    PARTITION `p68` VALUES LESS THAN ("2452884"),
    PARTITION `p69` VALUES LESS THAN ("2452914"),
    PARTITION `p70` VALUES LESS THAN ("2452945"),
    PARTITION `p71` VALUES LESS THAN ("2452975"),
    PARTITION `p72` VALUES LESS THAN (MAXVALUE)
    )
    DISTRIBUTED BY HASH(ss_item_sk, ss_ticket_number) BUCKETS 1
    PROPERTIES (
            "replication_num" = "1"
    );"""

    sql "drop table if exists cache_usability_date_dim;"
    sql """CREATE TABLE `cache_usability_date_dim` (
    `d_date_sk` bigint NOT NULL,
    `d_date_id` char(16) NOT NULL,
    `d_date` date NULL,
    `d_year` int NULL,
    `d_dow` int NULL,
    `d_moy` int NULL,
    ) ENGINE=OLAP
    DUPLICATE KEY(`d_date_sk`)
    DISTRIBUTED BY HASH(`d_date_sk`) BUCKETS 12
    PROPERTIES (
            "replication_num" = "1"
    );"""

    sql "drop table if exists cache_usability_item;"
    sql """CREATE TABLE `cache_usability_item` (
    `i_item_sk` bigint NOT NULL,
    `i_item_id` char(16) NOT NULL,
    `i_brand_id` int NULL,
    `i_brand` char(50) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`i_item_sk`)
    DISTRIBUTED BY HASH(`i_item_sk`) BUCKETS 12
    PROPERTIES (
            "replication_num" = "1"
    );"""

    sql "set disable_nereids_rules='PRUNE_EMPTY_PARTITION';"
    sql "set hbo_rfsafe_threshold=1.0;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    sql "set enable_hbo_optimization=false;"
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                        |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 14.881003333333332                                                                                                                                                                                                                                                                              |
     | PhysicalResultSink[461] ( outputExprs=[ss_store_sk#4, __count_1#13] )                                                                                                                                                                                                                                  |
     | +--PhysicalHashAggregate[456]@8 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#14) AS `count(*)`#13], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )   |
     |    +--PhysicalDistribute[451]@10 ( stats=1, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[446]@10 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#14], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[441]@7 ( stats=1, projects=[ss_store_sk#4] )                                                                                                                                                                                                                               |
     |             +--PhysicalHashJoin[436]@6 ( stats=1, type=INNER_JOIN, hashCondition=[(ss_sold_date_sk#2 = d_date_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[d_date_sk#7->[ss_sold_date_sk#2](ndv/size = 1/1) , RF1[d_date_sk#7->[ss_sold_date_sk#2](ndv/size = 1/1) ] )             |
     |                |--PhysicalProject[415]@2 ( stats=1, projects=[ss_sold_date_sk#2, ss_store_sk#4] )                                                                                                                                                                                                      |
     |                |  +--PhysicalFilter[410]@1 ( stats=1, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                                  |
     |                |     +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=4, RFs= RF0 RF1 )                                                                                                                                                                                                   |
     |                +--PhysicalDistribute[431]@5 ( stats=0.25, distributionSpec=DistributionSpecReplicated )                                                                                                                                                                                                |
     |                   +--PhysicalProject[426]@5 ( stats=0.25, projects=[d_date_sk#7] )                                                                                                                                                                                                                     |
     |                      +--PhysicalFilter[421]@4 ( stats=0.25, predicates=AND[(d_date_sk#7 >= 2451100),(d_date_sk#7 <= 2451200)] )                                                                                                                                                                        |
     |                         +--PhysicalOlapScan[cache_usability_date_dim]@3 ( stats=1 )                                                                                                                                                                                                                                    |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_date_dim where ss_sold_date_sk = d_date_sk and ss_sold_date_sk between 2451100 and 2451200 group by ss_store_sk;"
        contains("stats=1, type=INNER_JOIN")
        contains("stats=1, aggPhase=LOCAL")
        contains("stats=1, aggPhase=GLOBAL")
    }

    sql "set enable_hbo_optimization=true;"
    sql "select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_date_dim where ss_sold_date_sk = d_date_sk and ss_sold_date_sk between 2451100 and 2451200 group by ss_store_sk;"
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                             |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 4.02142                                                                                                                                                                                                                                                                                              |
     | PhysicalResultSink[498] ( outputExprs=[ss_store_sk#4, __count_1#13] )                                                                                                                                                                                                                                       |
     | +--PhysicalHashAggregate[493]@8 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#14) AS `count(*)`#13], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )   |
     |    +--PhysicalDistribute[488]@10 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[483]@10 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#14], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[478]@7 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                                                    |
     |             +--PhysicalHashJoin[473]@6 ( stats=0, type=INNER_JOIN, hashCondition=[(ss_sold_date_sk#2 = d_date_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[d_date_sk#7->[ss_sold_date_sk#2](ndv/size = 1/1) , RF1[d_date_sk#7->[ss_sold_date_sk#2](ndv/size = 1/1) ] )                  |
     |                |--PhysicalDistribute[452]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[2], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[2]], exprIdToEquivalenceSet={2=0} ) )                                      |
     |                |  +--PhysicalProject[447]@2 ( stats=0, projects=[ss_sold_date_sk#2, ss_store_sk#4] )                                                                                                                                                                                                        |
     |                |     +--PhysicalFilter[442]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                                    |
     |                |        +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0, RFs= RF0 RF1 )                                                                                                                                                                                                     |
     |                +--PhysicalDistribute[468]@5 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[7], shuffleType=EXECUTION_BUCKETED, tableId=1740488762102, selectedIndexId=1740488762103, partitionIds=[1740488762101], equivalenceExprIds=[[7]], exprIdToEquivalenceSet={7=0} ) )   |
     |                   +--PhysicalProject[463]@5 ( stats=0, projects=[d_date_sk#7] )                                                                                                                                                                                                                             |
     |                      +--PhysicalFilter[458]@4 ( stats=0, predicates=AND[(d_date_sk#7 >= 2451100),(d_date_sk#7 <= 2451200)] )                                                                                                                                                                                |
     |                         +--PhysicalOlapScan[cache_usability_date_dim]@3 ( stats=1 )                                                                                                                                                                                                                                         |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_date_dim where ss_sold_date_sk = d_date_sk and ss_sold_date_sk between 2451100 and 2451200 group by ss_store_sk;"
        contains("stats=(hbo)0, aggPhase=LOCAL")
        contains("stats=(hbo)0, aggPhase=GLOBAL")
    }

    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                       |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 10.369593333333334                                                                                                                                                                                                                                                                             |
     | PhysicalResultSink[452] ( outputExprs=[ss_store_sk#4, __count_1#11] )                                                                                                                                                                                                                                 |
     | +--PhysicalHashAggregate[447]@7 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#12) AS `count(*)`#11], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )  |
     |    +--PhysicalDistribute[442]@9 ( stats=1, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[437]@9 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#12], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[432]@6 ( stats=1, projects=[ss_store_sk#4] )                                                                                                                                                                                                                              |
     |             +--PhysicalHashJoin[427]@5 ( stats=1, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) , RF1[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) ] )                           |
     |                |--PhysicalProject[406]@4 ( stats=1, projects=[i_item_sk#7] )                                                                                                                                                                                                                          |
     |                |  +--PhysicalOlapScan[cache_usability_item]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                               |
     |                +--PhysicalDistribute[422]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[0], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[0]], exprIdToEquivalenceSet={0=0} ) )                                  |
     |                   +--PhysicalProject[417]@2 ( stats=0, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                       |
     |                      +--PhysicalFilter[412]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                              |
     |                         +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0 )                                                                                                                                                                                                             |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item where ss_item_sk = i_item_sk and ss_sold_date_sk between 2451100 and 2451200 group by ss_store_sk;"
        // TODO: actually to find the ss filter entry in hbo cache but not with (hbo) mark
        contains("stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)]")
    }

    sql "select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item where ss_item_sk = i_item_sk and ss_sold_date_sk between 2451100 and 2451200 group by ss_store_sk;"
    sleep(3000)
    /**
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                            |
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.02126                                                                                                                                                                                                                                                                                             |
     | PhysicalResultSink[449] ( outputExprs=[ss_store_sk#4, __count_1#11] )                                                                                                                                                                                                                                      |
     | +--PhysicalHashAggregate[444]@7 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#12) AS `count(*)`#11], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )  |
     |    +--PhysicalDistribute[439]@9 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[434]@9 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#12], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[429]@6 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                                                   |
     |             +--PhysicalHashJoin[424]@5 ( stats=0, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) , RF1[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) ] )                                |
     |                |--PhysicalProject[403]@4 ( stats=1, projects=[i_item_sk#7] )                                                                                                                                                                                                                               |
     |                |  +--PhysicalOlapScan[cache_usability_item]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                                    |
     |                +--PhysicalDistribute[419]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[0], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[0]], exprIdToEquivalenceSet={0=0} ) )                                       |
     |                   +--PhysicalProject[414]@2 ( stats=0, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                            |
     |                      +--PhysicalFilter[409]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                                   |
     |                         +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0 )                                                                                                                                                                                                                  |
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item where ss_item_sk = i_item_sk and ss_sold_date_sk between 2451100 and 2451200 group by ss_store_sk;"
        contains("stats=(hbo)0, aggPhase=GLOBAL")
        contains("stats=(hbo)0, aggPhase=LOCAL")
    }

    /**
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                            |
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.02126                                                                                                                                                                                                                                                                                             |
     | PhysicalResultSink[449] ( outputExprs=[ss_store_sk#4, __count_1#11] )                                                                                                                                                                                                                                      |
     | +--PhysicalHashAggregate[444]@7 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#12) AS `count(*)`#11], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )  |
     |    +--PhysicalDistribute[439]@9 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[434]@9 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#12], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[429]@6 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                                                   |
     |             +--PhysicalHashJoin[424]@5 ( stats=0, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) , RF1[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) ] )                                |
     |                |--PhysicalProject[403]@4 ( stats=1, projects=[i_item_sk#7] )                                                                                                                                                                                                                               |
     |                |  +--PhysicalOlapScan[cache_usability_item]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                                    |
     |                +--PhysicalDistribute[419]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[0], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[0]], exprIdToEquivalenceSet={0=0} ) )                                       |
     |                   +--PhysicalProject[414]@2 ( stats=0, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                            |
     |                      +--PhysicalFilter[409]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451101),(ss_sold_date_sk#2 <= 2451201)] )                                                                                                                                                                   |
     |                         +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0 )                                                                                                                                                                                                                  |
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item where ss_item_sk = i_item_sk and ss_sold_date_sk between 2451101 and 2451201 group by ss_store_sk;"
        contains("stats=(hbo)0, aggPhase=GLOBAL")
        contains("stats=(hbo)0, aggPhase=LOCAL")
    }

    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                        |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 9.433513333333334                                                                                                                                                                                                                                                                               |
     | PhysicalResultSink[511] ( outputExprs=[ss_store_sk#4, __count_1#13] )                                                                                                                                                                                                                                  |
     | +--PhysicalHashAggregate[506]@8 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#14) AS `count(*)`#13], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )   |
     |    +--PhysicalDistribute[501]@10 ( stats=1, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[496]@10 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#14], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[491]@7 ( stats=1, projects=[ss_store_sk#4] )                                                                                                                                                                                                                               |
     |             +--PhysicalHashJoin[486]@6 ( stats=1, type=INNER_JOIN, hashCondition=[(ss_sold_date_sk#2 = d_date_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_sold_date_sk#2->[d_date_sk#7](ndv/size = 1/1) , RF1[ss_sold_date_sk#2->[d_date_sk#7](ndv/size = 1/1) ] )             |
     |                |--PhysicalProject[465]@5 ( stats=0.06, projects=[d_date_sk#7] )                                                                                                                                                                                                                        |
     |                |  +--PhysicalFilter[460]@4 ( stats=0.06, predicates=AND[(d_date_sk#7 >= 2451100),(d_date_sk#7 <= 2451200),(d_moy#12 >= 2),(d_moy#12 <= 12)] )                                                                                                                                          |
     |                |     +--PhysicalOlapScan[cache_usability_date_dim]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                         |
     |                +--PhysicalDistribute[481]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[2], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[2]], exprIdToEquivalenceSet={2=0} ) )                                   |
     |                   +--PhysicalProject[476]@2 ( stats=0, projects=[ss_sold_date_sk#2, ss_store_sk#4] )                                                                                                                                                                                                   |
     |                      +--PhysicalFilter[471]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                               |
     |                         +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0 )                                                                                                                                                                                                              |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_date_dim where ss_sold_date_sk = d_date_sk and ss_sold_date_sk between 2451100 and 2451200 and d_moy between 2 and 12 group by ss_store_sk;"
        contains("stats=0.06, predicates=AND[(d_date_sk#7 >= 2451100),(d_date_sk#7 <= 2451200),(d_moy#12 >= 2),(d_moy#12 <= 12)]")
    }

    sql "select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_date_dim where ss_sold_date_sk = d_date_sk and ss_sold_date_sk between 2451100 and 2451200 and d_moy between 2 and 12 group by ss_store_sk;"
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                             |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 4.02168                                                                                                                                                                                                                                                                                              |
     | PhysicalResultSink[510] ( outputExprs=[ss_store_sk#4, __count_1#13] )                                                                                                                                                                                                                                       |
     | +--PhysicalHashAggregate[505]@8 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#14) AS `count(*)`#13], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )   |
     |    +--PhysicalDistribute[500]@10 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[495]@10 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#14], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[490]@7 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                                                    |
     |             +--PhysicalHashJoin[485]@6 ( stats=0, type=INNER_JOIN, hashCondition=[(ss_sold_date_sk#2 = d_date_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[d_date_sk#7->[ss_sold_date_sk#2](ndv/size = 1/1) , RF1[d_date_sk#7->[ss_sold_date_sk#2](ndv/size = 1/1) ] )                  |
     |                |--PhysicalDistribute[464]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[2], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[2]], exprIdToEquivalenceSet={2=0} ) )                                      |
     |                |  +--PhysicalProject[459]@2 ( stats=0, projects=[ss_sold_date_sk#2, ss_store_sk#4] )                                                                                                                                                                                                        |
     |                |     +--PhysicalFilter[454]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                                    |
     |                |        +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0, RFs= RF0 RF1 )                                                                                                                                                                                                     |
     |                +--PhysicalDistribute[480]@5 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[7], shuffleType=EXECUTION_BUCKETED, tableId=1740488762102, selectedIndexId=1740488762103, partitionIds=[1740488762101], equivalenceExprIds=[[7]], exprIdToEquivalenceSet={7=0} ) )   |
     |                   +--PhysicalProject[475]@5 ( stats=0, projects=[d_date_sk#7] )                                                                                                                                                                                                                             |
     |                      +--PhysicalFilter[470]@4 ( stats=0, predicates=AND[(d_date_sk#7 >= 2451100),(d_date_sk#7 <= 2451200),(d_moy#12 >= 2),(d_moy#12 <= 12)] )                                                                                                                                               |
     |                         +--PhysicalOlapScan[cache_usability_date_dim]@3 ( stats=1 )                                                                                                                                                                                                                                         |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_date_dim where ss_sold_date_sk = d_date_sk and ss_sold_date_sk between 2451100 and 2451200 and d_moy between 2 and 12 group by ss_store_sk;"
        contains("stats=(hbo)0, aggPhase=GLOBAL")
        contains("stats=0, predicates=AND[(d_date_sk#7 >= 2451100),(d_date_sk#7 <= 2451200),(d_moy#12 >= 2),(d_moy#12 <= 12)]");
    }

    /**
     +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                           |
     +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 15.870833333333334                                                                                                                                                                                                                                                                                 |
     | PhysicalResultSink[735] ( outputExprs=[ss_store_sk#4, __count_1#15] )                                                                                                                                                                                                                                     |
     | +--PhysicalHashAggregate[730]@13 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#16) AS `count(*)`#15], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )     |
     |    +--PhysicalDistribute[725]@15 ( stats=1, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                               |
     |       +--PhysicalHashAggregate[720]@15 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#16], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )    |
     |          +--PhysicalProject[715]@12 ( stats=1, projects=[ss_store_sk#4] )                                                                                                                                                                                                                                 |
     |             +--PhysicalHashJoin[710]@11 ( stats=1, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#11)], otherCondition=[], markCondition=[], runtimeFilters=[RF2[i_item_sk#11->[ss_item_sk#0, i_item_sk#7](ndv/size = 1/1) , RF3[i_item_sk#11->[ss_item_sk#0, i_item_sk#7](ndv/size = 1/1) ] ) |
     |                |--PhysicalProject[694]@7 ( stats=1, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                              |
     |                |  +--PhysicalHashJoin[689]@6 ( stats=1, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) , RF1[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) ] )                         |
     |                |     |--PhysicalProject[668]@5 ( stats=0.5, projects=[i_item_sk#7] )                                                                                                                                                                                                                      |
     |                |     |  +--PhysicalFilter[663]@4 ( stats=0.5, predicates=(i_brand_id#9 > 1003001) )                                                                                                                                                                                                       |
     |                |     |     +--PhysicalOlapScan[cache_usability_item]@3 ( stats=1, RFs= RF0 RF1 RF2 RF3 )                                                                                                                                                                                                                  |
     |                |     +--PhysicalDistribute[684]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[0], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[0]], exprIdToEquivalenceSet={0=0} ) )                                |
     |                |        +--PhysicalProject[679]@2 ( stats=0, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                     |
     |                |           +--PhysicalFilter[674]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                            |
     |                |              +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0, RFs= RF2 RF3 )                                                                                                                                                                                             |
     |                +--PhysicalProject[705]@10 ( stats=0.5, projects=[i_item_sk#11] )                                                                                                                                                                                                                          |
     |                   +--PhysicalFilter[700]@9 ( stats=0.5, predicates=(i_brand_id#13 < 10014017) )                                                                                                                                                                                                           |
     |                      +--PhysicalOlapScan[cache_usability_item]@8 ( stats=1 )                                                                                                                                                                                                                                              |
     +-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item i1, cache_usability_item i2 where ss_item_sk = i1.i_item_sk and ss_item_sk = i2.i_item_sk and ss_sold_date_sk between 2451100 and 2451200 and i1.i_brand_id > 1003001 and i2.i_brand_id < 10014017 group by ss_store_sk;"
        contains("stats=0.5, predicates=(i_brand_id#9 > 1003001) )")
        contains("stats=0.5, predicates=(i_brand_id#13 < 10014017) )")
    }

    sql "select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item i1, cache_usability_item i2 where ss_item_sk = i1.i_item_sk and ss_item_sk = i2.i_item_sk and ss_sold_date_sk between 2451100 and 2451200 and i1.i_brand_id > 1003001 and i2.i_brand_id < 10014017 group by ss_store_sk;"
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                             |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 7.022499999999999                                                                                                                                                                                                                                                                                    |
     | PhysicalResultSink[778] ( outputExprs=[ss_store_sk#4, __count_1#15] )                                                                                                                                                                                                                                       |
     | +--PhysicalHashAggregate[773]@13 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(partial_count(*)#16) AS `count(*)`#15], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false )  |
     |    +--PhysicalDistribute[768]@15 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                            |
     |       +--PhysicalHashAggregate[763]@15 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, partial_count(*) AS `partial_count(*)`#16], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[758]@12 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                                                   |
     |             +--PhysicalHashJoin[753]@11 ( stats=0, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#11)], otherCondition=[], markCondition=[], runtimeFilters=[RF2[ss_item_sk#0->[i_item_sk#11](ndv/size = 1/1) , RF3[ss_item_sk#0->[i_item_sk#11](ndv/size = 1/1) ] )                             |
     |                |--PhysicalProject[711]@10 ( stats=0, projects=[i_item_sk#11] )                                                                                                                                                                                                                              |
     |                |  +--PhysicalFilter[706]@9 ( stats=0, predicates=(i_brand_id#13 < 10014017) )                                                                                                                                                                                                               |
     |                |     +--PhysicalOlapScan[cache_usability_item]@8 ( stats=1, RFs= RF2 RF3 )                                                                                                                                                                                                                                  |
     |                +--PhysicalProject[748]@7 ( stats=0, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                                |
     |                   +--PhysicalHashJoin[743]@6 ( stats=0, type=INNER_JOIN, hashCondition=[(ss_item_sk#0 = i_item_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) , RF1[ss_item_sk#0->[i_item_sk#7](ndv/size = 1/1) ] )                           |
     |                      |--PhysicalProject[722]@5 ( stats=0, projects=[i_item_sk#7] )                                                                                                                                                                                                                          |
     |                      |  +--PhysicalFilter[717]@4 ( stats=0, predicates=(i_brand_id#9 > 1003001) )                                                                                                                                                                                                           |
     |                      |     +--PhysicalOlapScan[cache_usability_item]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                            |
     |                      +--PhysicalDistribute[738]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[0], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[0]], exprIdToEquivalenceSet={0=0} ) )                                  |
     |                         +--PhysicalProject[733]@2 ( stats=0, projects=[ss_item_sk#0, ss_store_sk#4] )                                                                                                                                                                                                       |
     |                            +--PhysicalFilter[728]@1 ( stats=0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                              |
     |                               +--PhysicalOlapScan[cache_usability_store_sales_p partitions(4/72)]@0 ( stats=0 )                                                                                                                                                                                                             |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from cache_usability_store_sales_p, cache_usability_item i1, cache_usability_item i2 where ss_item_sk = i1.i_item_sk and ss_item_sk = i2.i_item_sk and ss_sold_date_sk between 2451100 and 2451200 and i1.i_brand_id > 1003001 and i2.i_brand_id < 10014017 group by ss_store_sk;"
        contains("stats=(hbo)0, aggPhase=GLOBAL")
        contains("stats=0, predicates=(i_brand_id#9 > 1003001) )")
        contains("stats=0, predicates=(i_brand_id#13 < 10014017) )")
    }

}
