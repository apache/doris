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

suite("rf5") {
    def getRuntimeFilterCountFromPlan = { plan -> {
            int count = 0
            plan.eachMatch("RF\\d+\\[") {
                ch -> count ++
            }
            return count
    }}
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set runtime_filter_mode='GLOBAL'"

    sql 'set exec_mem_limit=21G'
    sql 'SET enable_pipeline_engine = true'
    sql 'set parallel_pipeline_task_num=8'

    sql 'set be_number_for_test=3'

    sql 'set enable_runtime_filter_prune=false'

    String query = """
    explain physical plan
    select 
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '1' year
    group by
        n_name
    order by
        revenue desc;
    """
    String plan1 = sql "${query}"


    sql 'set enable_runtime_filter_prune=true'
    
    String plan2 = sql "${query}"
    log.info("tcph_sf1000 h5 before prune:\n" + plan1)
    log.info("tcph_sf1000 h5 after prune:\n" + plan2)
    
    assertEquals(6, getRuntimeFilterCountFromPlan(plan1))
    assertEquals(4, getRuntimeFilterCountFromPlan(plan2))
}
