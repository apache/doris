
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
suite("push_down_one_row_join") {
    sql """
        drop table if exists t1;
        create table t1
        (id int, val1 string, val2 string) distributed by hash(id) buckets 3
        properties("replication_num" = "1");
        insert into t1 values (1, 'a', 'x'), (1, 'b', 'y'), (1, 'a', 'z');

        drop table if exists t2;
        create table t2 
        (id int, val1 string, val2 string) distributed by hash(id) buckets 3
        properties("replication_num" = "1");
        insert into t2 values (1, 'a', 'x'), (1, 'b', 'y'), (1, 'a', 'z');

        drop table if exists t3;
        create table t3 
        (id int, val1 string, val2 string) distributed by hash(id) buckets 3
        properties("replication_num" = "1");
        insert into t3 values (1, 'a', 'z'), (1, 'b', 'z'), (1, 'a', 'z');

        set runtime_filter_mode=OFF;
    """

    qt_shape_push_through_setop_cte """
        explain shape plan
        select id 
        from (
            select id
            from t1
            except
            select id 
            from t2) temp
        where id > (select id - 3 from t3 limit 1); 
        """

    qt_exec_push_through_setop_cte """
        select id 
        from (
            select id
            from t1
            except
            select id 
            from t2) temp
        where id > (select id - 3 from t3 limit 1); 
        """

    qt_shape_push_through_setop_inline """
        explain shape plan
        select id 
        from (
            select id
            from t1
            except
            select 2) temp
        where id > (select id - 3 from t3 limit 1); 
        """

    qt_exec_push_through_setop_inline """
        select id 
        from (
            select id
            from t1
            except
            select 2) temp
        where id > (select id - 3 from t3 limit 1); 
        """
    qt_exec_check_join_condition_push_down """
        select id 
        from (
            select id, val1, val2
            from t1
            union all
            select id, val2, val1
            from t2) temp
        where val1 >= (select val2 from t3 limit 1); 
        """
    
    explain{
        sql """
        select id 
        from (
            select id, val1, val2
            from t1
            union all
            select id, val2, val1
            from t2) temp
        where val1 >= (select val2 from t3 limit 1); 
        """
        contains "join conjuncts: (val2[#22] >= val2[#23])"
        contains "join conjuncts: (val1[#12] >= val2[#13])"
        /*
        check nested loop join with pushed down join condition
        for t2 join consumer, the join condition is t2.val2>t3.val2, not t2.val1>t3.val2
        
        | PhysicalCTEAnchor ( cteId=CTEId#0 )                                                                                                                   |
        | |--PhysicalCTEProducer[508] ( stats=1, cteId=CTEId#0 )                                                                                                |
        | |  +--PhysicalAssertNumRows@4 ( assertNumRowsElement=AssertNumRowsElement ( desiredNumOfRows=1, assertion=EQ ) )                                      |
        | |     +--PhysicalLimit[500]@3 ( limit=1, offset=0, phase=GLOBAL, stats=1 )                                                                            |
        | |        +--PhysicalDistribute[496]@2 ( stats=1, distributionSpec=DistributionSpecGather )                                                            |
        | |           +--PhysicalLimit[492]@2 ( limit=1, offset=0, phase=LOCAL, stats=1 )                                                                       |
        | |              +--PhysicalProject[488]@1 ( stats=3, projects=[val2#14] )                                                                              |
        | |                 +--PhysicalOlapScan[t3]@0 ( stats=3, operativeSlots=[], virtualColumns=[] )                                                         |
        | +--PhysicalResultSink[560] ( outputExprs=[id#6] )                                                                                                     |
        |    +--PhysicalUnion[556]@16 ( stats=3, qualifier=ALL, outputs=[id#6], regularChildrenOutputs=[[id#0], [id#3]], constantExprsList=[] )                 |
        |       |--PhysicalDistribute[530]@10 ( stats=1.5, distributionSpec=DistributionSpecExecutionAny )                                                      |
        |       |  +--PhysicalProject[526]@10 ( stats=1.5, projects=[id#0] )                                                                                    |
        |       |     +--PhysicalNestedLoopJoin[522]@9 ( stats=1.5, type=INNER_JOIN, hashCondition=[], otherCondition=[(val1#1 > val2#15)], markCondition=[] )  |
        |       |        |--PhysicalProject[513]@7 ( stats=3, projects=[id#0, val1#1] )                                                                         |
        |       |        |  +--PhysicalOlapScan[t1]@6 ( stats=3, operativeSlots=[val1#1], virtualColumns=[] )                                                   |
        |       |        +--PhysicalDistribute[518]@8 ( stats=1, distributionSpec=DistributionSpecReplicated )                                                  |
        |       |           +--PhysicalCTEConsumer[514] ( stats=1, cteId=CTEId#0, map={val2#15=val2#14} )                                                       |
        |       +--PhysicalDistribute[552]@15 ( stats=1.5, distributionSpec=DistributionSpecExecutionAny )                                                      |
        |          +--PhysicalProject[548]@15 ( stats=1.5, projects=[id#3] )                                                                                    |
        |             +--PhysicalNestedLoopJoin[544]@14 ( stats=1.5, type=INNER_JOIN, hashCondition=[], otherCondition=[(val2#5 > val2#16)], markCondition=[] ) |
        |                |--PhysicalProject[535]@12 ( stats=3, projects=[id#3, val2#5] )                                                                        |
        |                |  +--PhysicalOlapScan[t2]@11 ( stats=3, operativeSlots=[val2#5], virtualColumns=[] )                                                  |
        |                +--PhysicalDistribute[540]@13 ( stats=1, distributionSpec=DistributionSpecReplicated )                                                 |
        |                   +--PhysicalCTEConsumer[536] ( stats=1, cteId=CTEId#0, map={val2#16=val2#14} )                                                       |
        |                                                                                                                                                       |
        */
    }
}
