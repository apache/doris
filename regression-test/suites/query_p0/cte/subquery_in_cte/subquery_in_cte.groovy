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
suite("subquery_in_cte") {
    sql """
    drop table if exists t1;
    
    create table t1(a1 int,b1 int)
    properties("replication_num" = "1");

    insert into t1 values(1,2);

    drop table if exists t2;
    
    create table t2(a2 int,b2 int)
    properties("replication_num" = "1");

    insert into t2 values(1,3);
    """

    sql"""
    with cte1 as (
    select t1.a1, t1.b1
    from t1
    where t1.a1 > 0 and exists (select 1 from t2 where t1.a1 = t2.a2 or t1.a1 = t2.b2)
    )
    select * from cte1 union all select * from cte1;
    """

    /* TEST PURPOSE: APPLY_TO_JOIN should be applied on CTE producer before REWRITE_CTE_CHILDREN,
    considering the following analyzed plan, StatsDerive reports NPE when visiting LogicalFilter[26],
    a1#1 is not from its child's outputs.
    ----------------------------------------------------------------------------------------------------------------
    Explain String(Nereids Planner)
        LogicalResultSink[74] ( outputExprs=[a1#9, b1#10] )
        +--LogicalCteAnchor[73] ( cteId=CTEId#0 )
        |--LogicalCteProducer[63] ( cteId=CTEId#0 )
        |  +--LogicalSubQueryAlias ( qualifier=[cte1] )
        |     +--LogicalProject[36] ( distinct=false, projects=[a1#1, b1#2] )
        |        +--LogicalProject[35] ( distinct=false, projects=[a1#1, b1#2] )
        |           +--LogicalFilter[34] ( predicates=(a1#1 > 0) )
        |              +--LogicalProject[33] ( distinct=false, projects=[a1#1, b1#2] )
        |                 +--LogicalApply ( correlationSlot=[a1#1], correlationFilter=Optional.empty, isMarkJoin=false, isMarkJoinSlotNotNull=false, MarkJoinSlotReference=empty )
        |                    |--LogicalOlapScan ( qualified=internal.test.t1, indexName=<index_not_selected>, selectedIndexId=1767221999857, preAgg=UNSET, operativeCol=[], virtualColumns=[] )
        |                    +--LogicalProject[27] ( distinct=false, projects=[1 AS `1`#0] )
        |                       +--LogicalFilter[26] ( predicates=OR[(a1#1 = a2#3),(a1#1 = b2#4)] )
        |                          +--LogicalOlapScan ( qualified=internal.test.t2, indexName=<index_not_selected>, selectedIndexId=1767221999881, preAgg=UNSET, operativeCol=[], virtualColumns=[] )
        +--LogicalUnion ( qualifier=ALL, outputs=[a1#9, b1#10], regularChildrenOutputs=[[a1#5, b1#6], [a1#7, b1#8]], constantExprsList=[], hasPushedFilter=false )
            |--LogicalProject[65] ( distinct=false, projects=[a1#5, b1#6] )
            |  +--LogicalCteConsumer[64] ( cteId=CTEId#0, relationId=RelationId#0, name=cte1 )
            +--LogicalProject[67] ( distinct=false, projects=[a1#7, b1#8] )
                +--LogicalCteConsumer[66] ( cteId=CTEId#0, relationId=RelationId#1, name=cte1 )
    */
}
