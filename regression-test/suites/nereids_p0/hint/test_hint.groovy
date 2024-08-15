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

suite("test_hint") {
    // create database and tables
    sql 'DROP DATABASE IF EXISTS test_hint'
    sql 'CREATE DATABASE IF NOT EXISTS test_hint'
    sql 'use test_hint'

    // setting planner to nereids
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=1'
    sql 'set parallel_pipeline_task_num=1'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql "set ignore_shape_nodes='PhysicalProject'"
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set runtime_filter_mode=OFF'

    // create tables
    sql """drop table if exists t1;"""
    sql """drop table if exists t2;"""

    sql """create table t1 (c1 int, c11 int) distributed by hash(c1) buckets 3 properties('replication_num' = '1');"""
    sql """create table t2 (c2 int, c22 int) distributed by hash(c2) buckets 3 properties('replication_num' = '1');"""

// test hint positions, remove join in order to make sure shape stable when no use hint
    qt_select1_1 """explain shape plan select /*+ leading(t2 broadcast t1) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select1_2 """explain shape plan /*+ leading(t2 broadcast t1) */ select count(*) from t1;"""

    qt_select1_3 """explain shape plan select /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ count(*) from t1;"""

    qt_select1_4 """explain shape plan/*+DBP: ROUTE={GROUP_ID(zjaq)}*/ select count(*) from t1;"""

    qt_select1_5 """explain shape plan /*+ leading(t2 broadcast t1) */ select /*+ leading(t2 broadcast t1) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select1_6 """explain shape plan/*+DBP: ROUTE={GROUP_ID(zjaq)}*/ select /*+ leading(t2 broadcast t1) */ count(*) from t1 join t2 on c1 = c2;"""

    qt_select1_7 """explain shape plan /*+ leading(t2 broadcast t1) */ select /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ count(*) from t1;"""

    qt_select1_8 """explain shape plan /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ select /*+DBP: ROUTE={GROUP_ID(zjaq)}*/ count(*) from t1;"""

}
