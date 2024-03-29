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

suite("test_rf_limit") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set runtime_filter_type=8'
    sql 'set disable_join_reorder=true;'
    qt_drop_drop_1 """
    explain shape plan
    select count() from (select * from lineitem limit 1) t join orders on l_orderkey=o_orderkey;
    """

    qt_drop_rf2 """
    explain shape plan
    select count(1)
    from (select l_orderkey from lineitem join supplier on s_suppkey=l_suppkey limit 1) as T 
        join orders on T.l_orderkey=o_orderkey ;
    """

    //keep rf, reduce effort for AGG
    qt_keep_1 """
    explain shape plan select count(1)
    from (select l_orderkey, sum(l_linenumber) from lineitem group by l_orderkey limit 1) as T 
        join orders on T.l_orderkey=o_orderkey ;
    """

    // keep rf, reduce hash table
    qt_keep_2 """
    explain shape plan
    select count(1)
    from (select l_orderkey from supplier join lineitem on s_suppkey=l_suppkey limit 1) as T 
        join orders on T.l_orderkey=o_orderkey ;
    """

    // keep rf, reduce inner table size in NLJ
    qt_nlj_1 """
    explain shape plan
    select count(1)
    from (select l_orderkey from supplier join lineitem on s_suppkey>l_suppkey limit 1) as T 
        join orders on T.l_orderkey=o_orderkey;
    """

    // drop rf
    qt_nlj_2 """
    explain shape plan
    select count(1)
    from (select l_orderkey from lineitem join supplier on s_suppkey>l_suppkey limit 1) as T 
        join orders on T.l_orderkey=o_orderkey ;
    """
}

