/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("push_topn_to_agg") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql "set topn_opt_limit_threshold=1024"
    // limit -> agg
        // verify switch
    sql "set push_topn_to_agg=false"
    explain{
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey limit 4;"
        multiContains ("sortByGroupKey:false", 2)
    }
    sql "set push_topn_to_agg=true"
    explain{
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey limit 4;"
        multiContains ("sortByGroupKey:true", 2)
        notContains("STREAMING")
    }

    // when apply this opt, trun off STREAMING
    // limit -> proj -> agg, 
    explain{
        sql "select sum(c_custkey), c_name from customer group by c_name limit 6;"
        multiContains ("sortByGroupKey:true", 2)
        notContains("STREAMING")
    }

    // topn -> agg
    explain{
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey order by o_custkey limit 8;"
        multiContains ("sortByGroupKey:true", 2)
        notContains("STREAMING")
    }

    // order keys are part of group keys, 
    // 1. adjust group keys (o_custkey, o_clerk) -> o_clerk, o_custkey
    // 2. append o_custkey to order key 
    explain{
        sql "select sum(o_shippriority)  from orders group by o_custkey, o_clerk order by o_clerk limit 11;"
        check { String explainStr ->
            assertTrue(explainStr.contains("sortByGroupKey:true"))
            assertTrue(explainStr.find("group by: o_clerk\\[#\\d+\\], o_custkey\\[#\\d+\\]") != null)
            assertTrue(explainStr.find("order by: o_clerk\\[#\\d+\\] ASC, o_custkey\\[#\\d+\\] ASC") != null)
        }
    }


    // one distinct 
    explain {
        sql "select sum(distinct o_shippriority) from orders group by o_orderkey limit 13; "
        contains("VTOP-N")
        contains("order by: o_orderkey")
        multiContains("sortByGroupKey:true", 1)
    }

    // multi distinct 
    explain {
        sql "select count(distinct o_clerk), sum(distinct o_shippriority) from orders group by o_orderkey limit 14; "
        contains("VTOP-N")
        contains("order by: o_orderkey")
        //multiContains("sortByGroupKey:true", 2)
    }

    // use group key as sort key to enable topn-push opt
    explain {
        sql "select sum(o_shippriority) from orders group by o_clerk limit 14; "
        contains("sortByGroupKey:true")
    }

    // group key is expression
    explain {
        sql "select sum(o_shippriority), o_clerk+1 from orders group by o_clerk+1 limit 15; "
        contains("sortByGroupKey:true")
    }

    // order key is not part of group key
    explain {
        sql "select o_custkey, sum(o_shippriority) from orders group by o_custkey order by o_custkey+1 limit 16; "
        contains("sortByGroupKey:false")
        notContains("sortByGroupKey:true")
    }

    // topn + one phase agg
    explain {
        sql "select sum(ps_availqty), ps_partkey, ps_suppkey from partsupp group by ps_partkey, ps_suppkey order by ps_partkey, ps_suppkey limit 18;"
        contains("sortByGroupKey:true")
    }

    qt_shape_distinct_agg "explain shape plan select o_custkey, o_shippriority from orders group by o_custkey, o_shippriority limit 1";

    qt_shape_distinct "explain shape plan select distinct o_custkey from orders group by o_custkey limit 1"

    explain {
        sql "select o_custkey, o_shippriority from orders group by o_custkey, o_shippriority limit 1"
        multiContains("limit: 1", 3)
    }
    /**
    "limit 1" in 3 plan nodes:
    4:VEXCHANGE/ 3:VAGGREGATE (merge finalize) / 1:VAGGREGATE (update serialize)
+--------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                |
|   OUTPUT EXPRS:                                                                |
|     o_custkey[#11]                                                             |
|   PARTITION: UNPARTITIONED                                                     |
|                                                                                |
|   HAS_COLO_PLAN_NODE: false                                                    |
|                                                                                |
|   VRESULT SINK                                                                 |
|      MYSQL_PROTOCAL                                                            |
|                                                                                |
|   4:VEXCHANGE                                                                  |
|      offset: 0                                                                 |
|      limit: 1                                                                  |
|      distribute expr lists: o_custkey[#11]                                     |
|                                                                                |
| PLAN FRAGMENT 1                                                                |
|                                                                                |
|   PARTITION: HASH_PARTITIONED: o_custkey[#10]                                  |
|                                                                                |
|   HAS_COLO_PLAN_NODE: true                                                     |
|                                                                                |
|   STREAM DATA SINK                                                             |
|     EXCHANGE ID: 04                                                            |
|     UNPARTITIONED                                                              |
|                                                                                |
|   3:VAGGREGATE (merge finalize)(233)                                           |
|   |  group by: o_custkey[#10]                                                  |
|   |  sortByGroupKey:false                                                      |
|   |  cardinality=50,000                                                        |
|   |  limit: 1                                                                  |
|   |  distribute expr lists: o_custkey[#10]                                     |
|   |                                                                            |
|   2:VEXCHANGE                                                                  |
|      offset: 0                                                                 |
|      distribute expr lists:                                                    |
|                                                                                |
| PLAN FRAGMENT 2                                                                |
|                                                                                |
|   PARTITION: HASH_PARTITIONED: O_ORDERKEY[#0]                                  |
|                                                                                |
|   HAS_COLO_PLAN_NODE: false                                                    |
|                                                                                |
|   STREAM DATA SINK                                                             |
|     EXCHANGE ID: 02                                                            |
|     HASH_PARTITIONED: o_custkey[#10]                                           |
|                                                                                |
|   1:VAGGREGATE (update serialize)(223)                                         |
|   |  STREAMING                                                                 |
|   |  group by: o_custkey[#9]                                                   |
|   |  sortByGroupKey:false                                                      |
|   |  cardinality=50,000                                                        |
|   |  limit: 1                                                                  |
|   |  distribute expr lists:                                                    |
|   |                                                                            |
|   0:VOlapScanNode(213)                                                         |
|      TABLE: regression_test_nereids_tpch_p0.orders(orders), PREAGGREGATION: ON |
|      partitions=1/1 (orders)                                                   |
|      tablets=3/3, tabletList=1738740551790,1738740551792,1738740551794         |
|      cardinality=150000, avgRowSize=165.10652, numNodes=1                      |
|      pushAggOp=NONE                                                            |
|      final projections: O_CUSTKEY[#1]                                          |
|      final project output tuple id: 1                                          |
|                                                                                |
|                                                                                |
|                                                                                |
| ========== STATISTICS ==========                                               |
| planed with unknown column statistics                                          |
+--------------------------------------------------------------------------------+
    **/
}