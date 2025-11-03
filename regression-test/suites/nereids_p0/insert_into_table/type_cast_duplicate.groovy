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

suite("nereids_insert_duplicate") {
    sql 'use nereids_insert_into_table_test'
    sql 'clean label from nereids_insert_into_table_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    sql '''insert into dup_t_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src'''
    sql 'sync'
    qt_11 'select * from dup_t_type_cast order by id, kint'

    sql '''insert into dup_t_type_cast with label label_dup_type_cast_cte
            with cte as (select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from dup_t_type_cast order by id, kint'

    sql '''insert into dup_t_type_cast partition (p1, p2) with label label_dup_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id < 4'''
    sql 'sync'
    qt_13 'select * from dup_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_t_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src'''
    sql 'sync'
    qt_21 'select * from dup_light_sc_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_t_type_cast with label label_dup_light_sc_type_cast_cte
            with cte as (select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src)
            select * from cte'''
    sql 'sync'
    qt_22 'select * from dup_light_sc_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_t_type_cast partition (p1, p2) with label label_dup_light_sc_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id < 4'''
    sql 'sync'
    qt_23 'select * from dup_light_sc_t_type_cast order by id, kint'

    sql '''insert into dup_not_null_t_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id is not null'''
    sql 'sync'
    qt_31 'select * from dup_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_not_null_t_type_cast with label label_dup_not_null_type_cast_cte
            with cte as (select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_32 'select * from dup_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_not_null_t_type_cast partition (p1, p2) with label label_dup_not_null_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id < 4 and id is not null'''
    sql 'sync'
    qt_33 'select * from dup_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_not_null_t_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id is not null'''
    sql 'sync'
    qt_41 'select * from dup_light_sc_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_not_null_t_type_cast with label label_dup_light_sc_not_null_type_cast_cte
            with cte as (select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_42 'select * from dup_light_sc_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_not_null_t_type_cast partition (p1, p2) with label label_dup_light_sc_not_null_type_cast
            select id, ktint, ksint, kint, kbint, kdtv2, kdtm, kdbl from src where id < 4 and id is not null'''
    sql 'sync'
    qt_43 'select * from dup_light_sc_not_null_t_type_cast order by id, kint'

    sql 'set delete_without_partition=true'
    sql '''delete from dup_t_type_cast where id is not null'''
    sql '''delete from dup_t_type_cast where id is null'''
    sql '''delete from dup_light_sc_t_type_cast where id is not null'''
    sql '''delete from dup_light_sc_t_type_cast where id is null'''
    sql '''delete from dup_not_null_t_type_cast where id is not null'''
    sql '''delete from dup_not_null_t_type_cast where id is null'''
    sql '''delete from dup_light_sc_not_null_t_type_cast where id is not null'''
    sql '''delete from dup_light_sc_not_null_t_type_cast where id is null'''
}