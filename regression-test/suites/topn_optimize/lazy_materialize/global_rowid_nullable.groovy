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

suite("global_rowid_nullable") {
    sql """
        CREATE TABLE IF NOT EXISTS t1 (
            k1 INT NULL,
            k2 INT NULL,
            v1 INT NULL,
            v2 INT NULL
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            'replication_num' = '1'
        );
        insert into t1 values
        (1, 1, 10, 100),
        (2, 2, 20, 200),
        (3, 3, 30, 300);

        CREATE TABLE IF NOT EXISTS t2 (
            k1 INT NULL,
            k2 INT NULL,
            v1 INT NULL,
            v2 INT NULL
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            'replication_num' = '1'
        );
        insert into t2 values
        (1, 1, 10, 100),
        (2, 2, 20, 200),
        (3, 3, 30, 300);

        CREATE TABLE IF NOT EXISTS t3 (
            k1 INT NULL,
            k2 INT NULL,
            v1 INT NULL,
            v2 INT NULL
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES (
            'replication_num' = '1'
        );
        insert into t3 values
        (1, 1, 10, 100),
        (2, 2, 20, 200),
        (3, 3, 30, 300);

    set disable_join_reorder = true;
    """


/**
the plan of the following sql should be like:
PhysicalResultSink[608] ( outputExprs=[k1#0, x#12] )
+--PhysicalProject[605]@ ( stats=null, projects=[k1#0, x#12] )
   +--PhysicalLazyMaterialize [Output= ([k1#0, x#12]), lazySlots= ([x#12])]
      +--PhysicalTopN[603]@12 ( stats=1, limit=1, offset=0, orderKeys=[k1#0 asc null first], phase=MERGE_SORT )
         +--PhysicalDistribute[600]@ ( stats=1, distributionSpec=DistributionSpecGather )
            +--PhysicalTopN[597]@14 ( stats=1, limit=1, offset=0, orderKeys=[k1#0 asc null first], phase=LOCAL_SORT )
               +--PhysicalProject[594]@11 ( stats=3.01, projects=[k1#0, __DORIS_GLOBAL_ROWID_COL__t3#13] )
                  +--PhysicalHashJoin[592]@ ( stats=3.01, type=INNER_JOIN, hashCondition=[(k1#0 = k1#4)], otherCondition=[], markCondition=[], RFs=[RF0[k1#4->[k1#0](ndv/size = 3/4) , RF1[k1#4->[k1#0](ndv/size = 3/4) ] )
                     |--PhysicalProject[518]@1 ( stats=3, projects=[k1#0] )
                     |  +--PhysicalOlapScan[t1]@0 ( stats=3, operativeSlots=[k1#0], virtualColumns=[], JRFs= RF0 RF1 )
                     +--PhysicalDistribute[589]@ ( stats=3.01, distributionSpec=DistributionSpecReplicated )
                        +--PhysicalProject[586]@9 ( stats=3.01, projects=[k1#4, __DORIS_GLOBAL_ROWID_COL__t3#13] )
                           +--PhysicalFilter[584]@7 ( stats=3.01, predicates=OR[(k1#8 > 0),k1#8 IS NULL] )
                              +--PhysicalHashJoin[581]@ ( stats=6, type=LEFT_OUTER_JOIN, hashCondition=[(k1#4 = k1#8)], otherCondition=[], markCondition=[] )
                                 |--PhysicalProject[523]@3 ( stats=3, projects=[k1#4] )
                                 |  +--PhysicalOlapScan[t2]@2 ( stats=3, operativeSlots=[k1#4], virtualColumns=[] )
                                 +--PhysicalDistribute[578]@ ( stats=6, distributionSpec=DistributionSpecReplicated )
                                    +--PhysicalProject[575]@5 ( stats=6, projects=[k1#8, __DORIS_GLOBAL_ROWID_COL__t3#13] )
                                       +--PhysicalLazyMaterializeOlapScan[PhysicalOlapScan[t3]@4 ( stats=6, operativeSlots=[k1#8], virtualColumns=[] )]
 after  join[581] global rowid column of t3 should be nullable.
 we construct new context when visiting project[586]. this case verifies that we handle nullable property of global rowid column correctly passed through project[586] in such scenario. 
*/
    sql """
        select t1.k1, tmp2.x
        from t1 join (
            select tmp.k1, tmp.v1 as x from (
            select t2.k1, t3.v1 from t2 left outer join t3 on t2.k1 = t3.k1 where t3.k1 > 0 or t3.k1 is null
            ) as tmp

        ) as tmp2 on t1.k1 = tmp2.k1
        order by t1.k1
        limit 1;
    """

}