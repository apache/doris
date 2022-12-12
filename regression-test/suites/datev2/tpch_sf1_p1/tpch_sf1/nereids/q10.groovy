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

suite("tpch_sf1_q10_nereids") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=8589934592'


    qt_select """
    select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20;
    """

    qt_select """
    select /*+SET_VAR(exec_mem_limit=8589934592, parallel_fragment_exec_instance_num=16, enable_vectorized_engine=true, batch_size=4096, disable_join_reorder=true, enable_cost_based_join_reorder=false, enable_projection=true) */
        c_custkey,
        c_name,
        sum(t1.l_extendedprice * (1 - t1.l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        (
            select o_custkey,l_extendedprice,l_discount from lineitem, orders
            where l_orderkey = o_orderkey
            and o_orderdate >= date '1993-10-01'
            and o_orderdate < date '1993-10-01' + interval '3' month
            and l_returnflag = 'R'
        ) t1,
        nation
    where
        c_custkey = t1.o_custkey
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20;

    """

}
