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

suite("q1.1") {
    if (isCloudMode()) {
        return
    }
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G' 
    sql 'SET enable_pipeline_engine = true'
    sql 'set parallel_pipeline_task_num=8'
    sql "set enable_parallel_result_sink=false;"
    
sql 'set be_number_for_test=3'
sql 'set enable_runtime_filter_prune=false'
sql 'set runtime_filter_type=8'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"



    qt_select """
    explain shape plan
    SELECT
        LO_ORDERDATE,
        LO_ORDERKEY,
        LO_LINENUMBER,
        LO_CUSTKEY,
        LO_PARTKEY,
        LO_SUPPKEY,
        LO_ORDERPRIORITY,
        LO_SHIPPRIORITY,
        LO_QUANTITY,
        LO_EXTENDEDPRICE,
        LO_ORDTOTALPRICE,
        LO_DISCOUNT,
        LO_REVENUE,
        LO_SUPPLYCOST,
        LO_TAX,
        LO_COMMITDATE,
        LO_SHIPMODE,
        C_NAME,
        C_ADDRESS,
        C_CITY,
        C_NATION,
        C_REGION,
        C_PHONE,
        C_MKTSEGMENT,
        S_NAME,
        S_ADDRESS,
        S_CITY,
        S_NATION,
        S_REGION,
        S_PHONE,
        P_NAME,
        P_MFGR,
        P_CATEGORY,
        P_BRAND,
        P_COLOR,
        P_TYPE,
        P_SIZE,
        P_CONTAINER
    FROM (
        SELECT
            lo_orderkey,
            lo_linenumber,
            lo_custkey,
            lo_partkey,
            lo_suppkey,
            lo_orderdate,
            lo_orderpriority,
            lo_shippriority,
            lo_quantity,
            lo_extendedprice,
            lo_ordtotalprice,
            lo_discount,
            lo_revenue,
            lo_supplycost,
            lo_tax,
            lo_commitdate,
            lo_shipmode
        FROM lineorder
    ) l
    INNER JOIN customer c
    ON (c.c_custkey = l.lo_custkey)
    INNER JOIN supplier s
    ON (s.s_suppkey = l.lo_suppkey)
    INNER JOIN part p
    ON (p.p_partkey = l.lo_partkey);
    """
}
