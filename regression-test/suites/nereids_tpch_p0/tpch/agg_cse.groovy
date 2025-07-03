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

suite("agg_cse") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    qt_1 """
    select sum(r_regionkey + r_regionkey), avg(r_regionkey + r_regionkey), sum(r_regionkey + (r_regionkey+1)) 
    from region group by r_name order by 1, 2, 3;
    """
    explain{
        sql """
            select sum(r_regionkey + r_regionkey), avg(r_regionkey + r_regionkey), sum(r_regionkey + (r_regionkey+1)) 
            from region group by r_name;
        """
        contains("intermediate projections:")                                                                                                                                                             
    // expect plan: intermediate projections in OlapScanNode
    //    
    //    0:VOlapScanNode(168)                                                                                                                                                                       
    //       TABLE: tpch.region(region), PREAGGREGATION: ON                                                                                                                                          
    //       partitions=1/1 (region)                                                                                                                                                                 
    //       tablets=3/3, tabletList=135142,135144,135146                                                                                                                                            
    //       cardinality=5, avgRowSize=2978.0, numNodes=1                                                                                                                                            
    //       pushAggOp=NONE                                                                                                                                                                          
    //       final projections: r_name[#3], ((r_regionkey + r_regionkey)[#5] + 1), (r_regionkey + r_regionkey)[#5]                                                                                   
    //       final project output tuple id: 2                                                                                                                                                        
    //       intermediate projections: R_NAME[#1], R_REGIONKEY[#0], (R_REGIONKEY[#0] + R_REGIONKEY[#0])                                                                                              
    //       intermediate tuple id: 1  
    }

    explain{
        sql """
            select sum(r_regionkey), avg(r_regionkey), r_name
            from region group by r_name;
        """
        notContains("intermediate projections:")                                                                                                                                                             
    }


    explain{
    sql """
        select sum(a + a) , avg(a+a), sum(a+a+1)
        from (
            select r_regionkey as a, r_name
            from region
        ) T
        group by r_name;
        """
    contains("intermediate projections:")                                                                                                                                                             
    }
    qt_agg_cse_subquery """
            select sum(a + a) , avg(a+a), sum(a+a+1)
                from (
                    select r_regionkey as a, r_name
                    from region
                ) T
                group by r_name
                order by 1, 2, 3;
            """
    explain {
        sql """
            select count(distinct k2,k3),count(*), sum(k2+k3), avg(k2+k3) from 
            nereids_test_query_db.baseall group by k1
            """
        contains("final projections: k1[#1], k2[#2], k3[#3], (k2[#2] + k3[#3])")
    //  expect plan: final projections: k1[#1], k2[#2], k3[#3], (k2[#2] + k3[#3])
    //
    //  0:VOlapScanNode(147)
    //  TABLE: nereids_test_query_db.baseall(baseall), PREAGGREGATION: OFF. Reason: can't turn preAgg on for aggregate function count(*)
    //  partitions=1/1 (baseall)
    //  tablets=5/5, tabletList=43307,43309,43311 ...
    //  cardinality=16, avgRowSize=4418.75, numNodes=1
    //  pushAggOp=NONE
    //  final projections: k1[#1], k2[#2], k3[#3], (k2[#2] + k3[#3])
    //  final project output tuple id: 1
    }
    qt_agg_cse_distinct """
        select count(distinct k2,k3),count(*), sum(k2+k3), avg(k2+k3) from 
            nereids_test_query_db.baseall group by k1 order by 1, 2,3,4
        """
}
