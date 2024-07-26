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

suite("q18") {
    if (isCloudMode()) {
        return
    }
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set runtime_filter_mode='GLOBAL'"

    sql 'set exec_mem_limit=21G'
    sql 'SET enable_pipeline_engine = true'
    sql 'set parallel_pipeline_task_num=8'


    
    sql 'set be_number_for_test=3'
    sql "set runtime_filter_type=8"
sql 'set forbid_unknown_col_stats=true'
sql 'set enable_runtime_filter_prune=true'
sql 'set enable_stats=true'
sql 'set enable_left_zig_zag=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    qt_select """
    explain shape plan
    select 
    c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            sum(l_quantity)
    from
            customer,
            orders,
            lineitem
    where
            o_orderkey  in  (
                    select
                            l_orderkey
                    from
                            lineitem
                    group  by
                            l_orderkey  having
                                    sum(l_quantity)  >  300
            )
            and  c_custkey  =  o_custkey
            and  o_orderkey  =  l_orderkey
    group  by
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
    order  by
            o_totalprice  desc,
            o_orderdate
    limit  100;
    """
}
