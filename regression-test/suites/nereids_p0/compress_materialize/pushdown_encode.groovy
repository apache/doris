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

suite("pushdown_encode") {
// push down encode slot
    sql """
    set enable_compress_materialize=true;
    drop table if exists t1;
    CREATE TABLE t1 (
    `k1` int NOT NULL,
    `v1` char(5) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    insert into t1 values (1, "a"), (2, "b");

    drop table if exists t2;
    CREATE TABLE t2 (
    `k2` int NOT NULL,
    `v2` char(5) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k2`)
    DISTRIBUTED BY HASH(`k2`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    insert into t2 values (3, "c"), (4, "d"), (2, "b");

    set disable_join_reorder=true;
    """

    explain{
        sql """
            physical plan 
            select v1
            from (select sum(k1) as k, v1 from t1 group by v1) t
            order by v1;
        """
        contains("orderKeys=[encode_as_bigint(v1)#4 asc null first]")
        contains("projects=[decode_as_varchar(encode_as_bigint(v1)#3) AS `v1`#1, encode_as_bigint(v1)#3 AS `encode_as_bigint(v1)`#4]")
        contains("groupByExpr=[encode_as_bigint(v1)#3]")
        contains("projects=[encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#3]")
    } 

    order_qt_exec_sort_agg """
        select v1
        from (select sum(k1) as k, v1 from t1 group by v1) t
        order by v1;
        """

    explain{
        sql """
            physical plan
            select v1
            from t1
            where k1 > 0
            order by v1;
            """
        contains("orderKeys=[encode_as_bigint(v1)#2 asc null first]")
        contains("projects=[decode_as_varchar(encode_as_bigint(v1#1)) AS `decode_as_varchar(encode_as_bigint(v1))`#1, encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#2]")
    }
    
    order_qt_exec_sort_filter """
    select v1
    from t1
    where k1 > 0
    order by v1;
    """

    explain{
        sql """
        physical plan 
        select v1
        from t1 join t2 on v1=v2
        group by v1;
        """
        contains("groupByExpr=[encode_as_bigint(v1)#4]")
        contains("projects=[encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#4]")
    }

    order_qt_exec_agg_join"""
    select v1
    from t1 join t2 on v1=v2
    group by v1;
    """

    explain {
        sql"""
            physical plan
            select v1
            from t1 join t2 on v1=v2
            group by v1;
            """
        contains("groupByExpr=[encode_as_bigint(v1)#4]")
        contains("projects=[encode_as_bigint(v1)#4]")
        contains("hashCondition=[(encode_as_bigint(v1)#4 = encode_as_bigint(v2)#5)]")
        contains("projects=[encode_as_bigint(v2#3) AS `encode_as_bigint(v2)`#5]")
        contains("projects=[encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#4]")
    }
    
    explain {
        // because of "length(v1)>0", encode not pushed down through join
        sql """
            physical plan
            select v1
            from t1 left join t2 on v1=v2 and length(v1)>0
            group by v1;
            """
        contains("hashCondition=[(v1#1 = v2#3)], otherCondition=[(length(v1#1) > 0)]")
    }

    order_qt_agg_join_2 """
        select v1, sum(k2)
        from t1 join t2 on v1=v2
        group by v1;"""

    explain {
        sql """physical plan
            select v1, sum(k2)
            from t1 join t2 on v1=v2
            group by v1;"""
        contains("projects=[decode_as_varchar(encode_as_bigint(v1)#5) AS `v1`#1, sum(k2)#4]")
        contains("groupByExpr=[encode_as_bigint(v1)#5]")
        contains("projects=[encode_as_bigint(v1)#5, k2#2]")
        contains("hashCondition=[(encode_as_bigint(v1)#5 = encode_as_bigint(v2)#6)]")
        contains("projects=[encode_as_bigint(v2#3) AS `encode_as_bigint(v2)`#6, k2#2]")
        contains("projects=[encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#5]")
    }

    explain {
        sql """
            physical plan
            select v1, sum(k2)
            from t1 right outer join t2 on v1 < v2
            group by v1;
            """
        contains("groupByExpr=[encode_as_bigint(v1)#5]")
        contains("projects=[encode_as_bigint(v1)#5, k2#2]")
        contains("otherCondition=[(encode_as_bigint(v1)#5 < encode_as_bigint(v2)#6)]")
        contains("projects=[encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#5]")
        contains("projects=[encode_as_bigint(v2#3) AS `encode_as_bigint(v2)`#6, k2#2]")
    }

   
    explain {
        sql """
            physical plan
            select v1, sum(k2)
            from 
               (select t1.k1, t1.v1 from t1 join t2 on v1 < concat(v2,'a')) t3
               join t2 on t3.k1=t2.k2
            group by v1;
            """
        contains("otherCondition=[(v1#1 < concat(v2, 'a')#8)]")
        contains("projects=[k1#0, encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#7]")

        // +--PhysicalHashJoin[730]@7 ( stats=3, type=INNER_JOIN, hashCondition=[(k1#0 = k2#4)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[k2#4->[k1#0](ndv/size = 3/4) , RF1[k2#4->[k1#0](ndv/size = 3/4) ] )
        //               |--PhysicalProject[714]@4 ( stats=3, projects=[k1#0, encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#7] )                                                                                  
        //               |  +--PhysicalNestedLoopJoin[709]@3 ( stats=3, type=INNER_JOIN, hashCondition=[], otherCondition=[(v1#1 < concat(v2, 'a')#8)], markCondition=[] )                                           
        //               |     |--PhysicalProject[698]@2 ( stats=3, projects=[concat(v2#3, 'a') AS `concat(v2, 'a')`#8] )                                                                                            
        //               |     |  +--PhysicalOlapScan[t2]@1 ( stats=3 )                                                                                                                                            
        //               |     +--PhysicalDistribute[704]@0 ( stats=2, distributionSpec=DistributionSpecReplicated )                                                                                               
        //               |        +--PhysicalOlapScan[t1]@0 ( stats=2, RFs= RF0 RF1 )                                                                                                                        
        //               +--PhysicalDistribute[725]@6 ( stats=3, distributionSpec=DistributionSpecReplicated )                                                                                               
        //                  +--PhysicalProject[720]@6 ( stats=3, projects=[k2#4] )                                                                                                                           
        //                     +--PhysicalOlapScan[t2]@5 ( stats=3 )      
    } 


    order_qt_nlj """
            select v1, sum(k2)
            from t1 right outer join t2 on v1 < v2 and v2>"abc"
            group by v1;
            """

    // not push through join, because v2>"abc"
    sql """            
            select v1, sum(k2)
            from t1 right outer join t2 on v1 = v2 and (k1!=k2 or v2>"abc")
            group by v1;
            """
    explain {
        sql """  
            shape plan          
            select v1, sum(k2)
            from t1 right outer join t2 on v1 = v2 and (k1!=k2 or v2>"abc")
            group by v1;
            """
        contains "hashCondition=((t1.v1 = t2.v2))"
    }

    explain {
        sql """
             physical plan
            select  k
            from (
                (select k1 as k, v1 as v from t1)
                union all 
                (select k2 as k, v2 as v from t2)
                ) T
            order by v;
           """
        contains("orderKeys=[encode_as_bigint(v)#10 asc null first]")
        contains("outputs=[k#8, encode_as_bigint(v)#10], regularChildrenOutputs=[[k#2, encode_as_bigint(v)#11], [k#6, encode_as_bigint(v)#12]]")
        contains("projects=[k1#0 AS `k`#2, encode_as_bigint(v1#1) AS `encode_as_bigint(v)`#11]")
        contains("projects=[k2#4 AS `k`#6, encode_as_bigint(v2#5) AS `encode_as_bigint(v)`#12]")
    }

    order_qt_union """
            select  k
            from (
                (select k1 as k, v1 as v from t1)
                union all 
                (select k2 as k, v2 as v from t2)
                ) T
            order by v;
            """
    order_qt_intersect """
        select  k
        from (
            (select k1 as k, v1 as v from t1)
            intersect 
            (select k2 as k, v2 as v from t2)
            ) T
        order by v;
        """

    order_qt_except """
        select  k
        from (
            (select k1 as k, v1 as v from t1)
            except 
            (select k2 as k, v2 as v from t2)
            ) T
        order by v;
        """

    order_qt_agg_sort """ 
        select v1
        from (select v1 from t1 where k1 > 0 order by v1 limit 10) t group by v1
        """
    
    explain{
        sql """ 
        physical plan
        select v1
        from (select v1 from t1 where k1 > 0 order by v1 limit 10) t group by v1
        """
        contains("projects=[decode_as_varchar(encode_as_bigint(v1#1)) AS `decode_as_varchar(encode_as_bigint(v1))`#1, encode_as_bigint(v1#1) AS `encode_as_bigint(v1)`#3]")
    }

    // if encodeBody is used in windowExpression, do not push encode down

    sql """
        SELECT v1, k1, k2
            , sum(k2) OVER (PARTITION BY v1 ORDER BY k1) AS w_sum
        FROM t1
            JOIN t2 ON k1 = k2 - 2
        ORDER BY k1, v1, w_sum;
    """

    explain {
        sql """
        physical plan
        SELECT v1, k1, k2
            , sum(k2) OVER (PARTITION BY v1 ORDER BY k1) AS w_sum
        FROM t1
            JOIN t2 ON k1 = k2 - 2
        ORDER BY k1, v1, w_sum;
        """
        contains("orderKeys=[k1#0 asc null first, encode_as_bigint(v1)#5 asc null first, w_sum#4 asc null first]")
    }
    
}