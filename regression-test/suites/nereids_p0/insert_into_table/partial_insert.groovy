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

suite('partial_insert') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set parallel_fragment_exec_instance_num=13'

    sql 'use nereids_insert_into_table_test'

    sql 'truncate table dup_t'
    sql 'insert into dup_t (kint, ksint) select kint, ksint from src'
    qt_sql_dup 'select * from dup_t order by kint'

    sql 'truncate table agg_t'
    sql 'insert into agg_t (kint, ksint) select kint, ksint from src'
    qt_sql_agg 'select * from agg_t order by kint'

    sql 'truncate table uni_t'
    sql 'insert into uni_t (kint, ksint) select kint, ksint from src'
    qt_sql_uni 'select * from uni_t order by kint'
}