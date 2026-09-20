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

suite("hbo_parameterization_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists store_sales_p;"
    sql """CREATE TABLE `store_sales_p` (
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

    sql "drop table if exists store;"
    sql """
    CREATE TABLE IF NOT EXISTS store (
        s_store_sk bigint not null,
        s_store_id char(16) not null,
        s_zip char(10)
    )
    DUPLICATE KEY(s_store_sk)
    DISTRIBUTED BY HASH(s_store_sk) BUCKETS 1
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql "set disable_nereids_rules='PRUNE_EMPTY_PARTITION';"
    sql "set hbo_rfsafe_threshold=1.0;"
    sql "set global enable_hbo_info_collection=true;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    // group by 1 order by 1 will not impact the hbo cache matching since hbo is processed during cbo stage
    // and these kinds of info have been processed during rewriting, including real column replacement and
    // constant folding
    sql "select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 group by 1;"
    sleep(3000)

    /**
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                          |
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 9.02126                                                                                                                                                                                                                                                                              |
     | PhysicalResultSink[463] ( outputExprs=[s_zip#9, __count_1#10] )                                                                                                                                                                                                                             |
     | +--PhysicalDistribute[458]@7 ( stats=(hbo)0, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                      |
     |    +--PhysicalHashAggregate[453]@7 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, count(partial_count(*)#11) AS `count(*)`#10], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false )  |
     |       +--PhysicalDistribute[448]@9 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[9], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[9]], exprIdToEquivalenceSet={9=0} ) )                          |
     |          +--PhysicalHashAggregate[443]@9 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, partial_count(*) AS `partial_count(*)`#11], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false ) |
     |             +--PhysicalProject[438]@6 ( stats=0, projects=[s_zip#9] )                                                                                                                                                                                                                       |
     |                +--PhysicalHashJoin[433]@5 ( stats=(hbo)0, type=INNER_JOIN, hashCondition=[(ss_store_sk#4 = s_store_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) , RF1[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) ] )   |
     |                   |--PhysicalProject[412]@4 ( stats=1, projects=[s_store_sk#7, s_zip#9] )                                                                                                                                                                                                   |
     |                   |  +--PhysicalOlapScan[store]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                 |
     |                   +--PhysicalDistribute[428]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                     |
     |                      +--PhysicalProject[423]@2 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                        |
     |                         +--PhysicalFilter[418]@1 ( stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                            |
     |                            +--PhysicalOlapScan[store_sales_p partitions(4/72)]@0 ( stats=4 )
     +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 group by 1;"
        contains("stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)]")
        contains("stats=(hbo)0, type=INNER_JOIN")
        contains("stats=(hbo)0, aggPhase=GLOBAL")
    }

    /**
     +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                  |
     +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.00126                                                                                                                                                                                                                                                                      |
     | PhysicalResultSink[432] ( outputExprs=[ss_store_sk#4, __count_1#10] )                                                                                                                                                                                                               |
     | +--PhysicalDistribute[427]@7 ( stats=0, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                   |
     |    +--PhysicalHashAggregate[422]@7 ( stats=0, aggPhase=LOCAL, aggMode=INPUT_TO_RESULT, maybeUseStreaming=false, groupByExpr=[ss_store_sk#4], outputExpr=[ss_store_sk#4, count(*) AS `count(*)`#10], partitionExpr=Optional[[ss_store_sk#4]], topnFilter=false, topnPushDown=false ) |
     |       +--PhysicalProject[417]@6 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                               |
     |          +--PhysicalHashJoin[412]@5 ( stats=(hbo)0, type=INNER_JOIN, hashCondition=[(ss_store_sk#4 = s_store_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) , RF1[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) ] ) |
     |             |--PhysicalProject[391]@4 ( stats=1, projects=[s_store_sk#7] )                                                                                                                                                                                                          |
     |             |  +--PhysicalOlapScan[store]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                               |
     |             +--PhysicalDistribute[407]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                   |
     |                +--PhysicalProject[402]@2 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                      |
     |                   +--PhysicalFilter[397]@1 ( stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                          |
     |                      +--PhysicalOlapScan[store_sales_p partitions(4/72)]@0 ( stats=0 )
     +----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select ss_store_sk, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 group by 1;"
        contains("stats=(hbo)0, type=INNER_JOIN")
        contains("stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)]")
    }

    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                                 |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 5.02126                                                                                                                                                                                                                                                                                  |
     | PhysicalResultSink[520] ( outputExprs=[s_zip#9, __count_1#10] )                                                                                                                                                                                                                                 |
     | +--PhysicalQuickSort[515]@8 ( stats=(hbo)0, orderKeys=[s_zip#9 asc null first], phase=GATHER_SORT )                                                                                                                                                                                             |
     |    +--PhysicalDistribute[510]@7 ( stats=(hbo)0, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                       |
     |       +--PhysicalHashAggregate[505]@7 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, count(partial_count(*)#11) AS `count(*)`#10], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false )   |
     |          +--PhysicalDistribute[500]@11 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[9], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[9]], exprIdToEquivalenceSet={9=0} ) )                          |
     |             +--PhysicalHashAggregate[495]@11 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, partial_count(*) AS `partial_count(*)`#11], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false ) |
     |                +--PhysicalProject[490]@6 ( stats=0, projects=[s_zip#9] )                                                                                                                                                                                                                        |
     |                   +--PhysicalHashJoin[485]@5 ( stats=(hbo)0, type=INNER_JOIN, hashCondition=[(ss_store_sk#4 = s_store_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) , RF1[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) ] )    |
     |                      |--PhysicalProject[464]@4 ( stats=1, projects=[s_store_sk#7, s_zip#9] )                                                                                                                                                                                                    |
     |                      |  +--PhysicalOlapScan[store]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                                  |
     |                      +--PhysicalDistribute[480]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                      |
     |                         +--PhysicalProject[475]@2 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                         |
     |                            +--PhysicalFilter[470]@1 ( stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                             |
     |                               +--PhysicalOlapScan[store_sales_p partitions(4/72)]@0 ( stats=0 )
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 group by 1 order by 1;"
        contains("stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)]")
        contains("stats=(hbo)0, type=INNER_JOIN")
        contains("stats=(hbo)0, aggPhase=GLOBAL")
    }

    // 20 in varchar(20) will not be parameterized since it is a function but not literal
    // substring("3190400", 1, 5) has been constant folded in rewriting stage, so will not be seen in parameterization also
    sql """select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 and cast(s_zip as varchar(20)) = substring("3190400", 1, 5) group by s_zip;"""
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                           |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 4.02218                                                                                                                                                                                                                                                                               |
     | PhysicalResultSink[524] ( outputExprs=[s_zip#9, __count_1#10] )                                                                                                                                                                                                                              |
     | +--PhysicalDistribute[519]@8 ( stats=(hbo)0, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                       |
     |    +--PhysicalHashAggregate[514]@8 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, count(partial_count(*)#11) AS `count(*)`#10], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false )   |
     |       +--PhysicalDistribute[509]@10 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[9], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[9]], exprIdToEquivalenceSet={9=0} ) )                          |
     |          +--PhysicalHashAggregate[504]@10 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, partial_count(*) AS `partial_count(*)`#11], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false ) |
     |             +--PhysicalProject[499]@7 ( stats=0, projects=[s_zip#9] )                                                                                                                                                                                                                        |
     |                +--PhysicalHashJoin[494]@6 ( stats=(hbo)0, type=INNER_JOIN, hashCondition=[(ss_store_sk#4 = s_store_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) , RF1[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) ] )    |
     |                   |--PhysicalProject[473]@5 ( stats=0, projects=[s_store_sk#7, s_zip#9] )                                                                                                                                                                                                    |
     |                   |  +--PhysicalFilter[468]@4 ( stats=(hbo)0, predicates=(substring(cast(s_zip#9 as VARCHAR(20)), 1, 20) = '31904') )                                                                                                                                                        |
     |                   |     +--PhysicalOlapScan[store]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                                               |
     |                   +--PhysicalDistribute[489]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=STORAGE_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                      |
     |                      +--PhysicalProject[484]@2 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                         |
     |                         +--PhysicalFilter[479]@1 ( stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                             |
     |                            +--PhysicalOlapScan[store_sales_p partitions(4/72)]@0 ( stats=0 )
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql """physical plan select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 and cast(s_zip as varchar(20)) = substring("3190400", 1, 5) group by s_zip;"""
        contains("stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)]")
        contains("stats=(hbo)0, predicates=(substring(cast(s_zip#9 as VARCHAR(20)), 1, 20) = '31904')")
        contains("stats=(hbo)0, type=INNER_JOIN")
        contains("stats=(hbo)0, aggPhase=GLOBAL")
    }

    // format string like 'date_format('2024-01-01', '%y')' will not be seen in parameterization since it has been folded during rewriting
    sql """select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 and cast(s_zip as varchar(20)) = date_format('2024-01-01', '%y') group by s_zip;"""
    sleep(3000)
    /**
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                           |
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 4.02218                                                                                                                                                                                                                                                                                               |
     | PhysicalResultSink[534] ( outputExprs=[s_zip#9, __count_1#10] )                                                                                                                                                                                                                                              |
     | +--PhysicalDistribute[529]@8 ( stats=(hbo)0, distributionSpec=DistributionSpecGather )                                                                                                                                                                                                                       |
     |    +--PhysicalHashAggregate[524]@8 ( stats=(hbo)0, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, count(partial_count(*)#11) AS `count(*)`#10], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false )                   |
     |       +--PhysicalDistribute[519]@10 ( stats=(hbo)0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[9], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[9]], exprIdToEquivalenceSet={9=0} ) )                                          |
     |          +--PhysicalHashAggregate[514]@10 ( stats=(hbo)0, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=true, groupByExpr=[s_zip#9], outputExpr=[s_zip#9, partial_count(*) AS `partial_count(*)`#11], partitionExpr=Optional[[s_zip#9]], topnFilter=false, topnPushDown=false )                 |
     |             +--PhysicalProject[509]@7 ( stats=0, projects=[s_zip#9] )                                                                                                                                                                                                                                        |
     |                +--PhysicalHashJoin[504]@6 ( stats=(hbo)0, type=INNER_JOIN, hashCondition=[(ss_store_sk#4 = s_store_sk#7)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) , RF1[ss_store_sk#4->[s_store_sk#7](ndv/size = 1/1) ] )                    |
     |                   |--PhysicalDistribute[483]@5 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[7], shuffleType=EXECUTION_BUCKETED, tableId=1744890042536, selectedIndexId=1744890042537, partitionIds=[1744890042535], equivalenceExprIds=[[7]], exprIdToEquivalenceSet={7=0} ) ) |
     |                   |  +--PhysicalProject[478]@5 ( stats=0, projects=[s_store_sk#7, s_zip#9] )                                                                                                                                                                                                                 |
     |                   |     +--PhysicalFilter[473]@4 ( stats=(hbo)0, predicates=(substring(cast(s_zip#9 as VARCHAR(20)), 1, 20) = '24') )                                                                                                                                                                        |
     |                   |        +--PhysicalOlapScan[store operativeSlots([s_store_sk#7, s_zip#9])]@3 ( stats=1, RFs= RF0 RF1 )                                                                                                                                                                                    |
     |                   +--PhysicalDistribute[499]@2 ( stats=0, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[4], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[4]], exprIdToEquivalenceSet={4=0} ) )                                    |
     |                      +--PhysicalProject[494]@2 ( stats=0, projects=[ss_store_sk#4] )                                                                                                                                                                                                                         |
     |                         +--PhysicalFilter[489]@1 ( stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)] )                                                                                                                                                             |
     |                            +--PhysicalOlapScan[store_sales_p partitions(4/72) operativeSlots([ss_sold_date_sk#2, ss_store_sk#4])]@0 ( stats=0 )
     +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql """physical plan select s_zip, count(1) from store_sales_p, store where ss_store_sk = s_store_sk and ss_sold_date_sk between 2451100 and 2451200 and cast(s_zip as varchar(20)) = date_format('2024-01-01', '%y') group by s_zip;"""
        contains("stats=(hbo)0, predicates=AND[(ss_sold_date_sk#2 >= 2451100),(ss_sold_date_sk#2 <= 2451200)]")
        contains("stats=(hbo)0, predicates=(substring(cast(s_zip#9 as VARCHAR(20)), 1, 20) = '24')")
        contains("stats=(hbo)0, type=INNER_JOIN")
        contains("stats=(hbo)0, aggPhase=GLOBAL")
    }

}
