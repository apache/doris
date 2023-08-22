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

suite('nereids_insert_random') {
    sql 'use nereids_insert_into_table_test'
    sql 'clean label from nereids_insert_into_table_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    sql '''insert into dup_t_type_cast_rd
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src'''
    sql 'sync'
    qt_11 'select * from dup_t_type_cast_rd order by id, kint'

    sql '''insert into dup_t_type_cast_rd with label label_dup_type_cast_cte_rd
            with cte as (select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from dup_t_type_cast_rd order by id, kint'

    sql '''insert into dup_t_type_cast_rd partition (p1, p2) with label label_dup_type_cast_rd
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id < 4'''
    sql 'sync'
    qt_13 'select * from dup_t_type_cast_rd order by id, kint'

    sql 'set delete_without_partition=true'
    sql '''delete from dup_t_type_cast_rd where id is not null'''
    sql '''delete from dup_t_type_cast_rd where id is null'''
}
