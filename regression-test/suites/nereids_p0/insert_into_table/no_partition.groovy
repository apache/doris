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

suite('nereids_insert_no_partition') {
    // TODO: reopen this case in conf, currently delete fill failed
    //  because nereids generate a true predicate and be could not support it.
    sql 'use nereids_insert_into_table_test'
    sql 'clean label from nereids_insert_into_table_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    explain {
        // TODO: test turn off pipeline when dml, remove it if pipeline sink is ok
        sql '''
            insert into uni_light_sc_mow_not_null_nop_t with t as(
            select * except(kaint, kmintint, kjson) from src where id is not null)
            select * from t left semi join t t2 on t.id = t2.id;
        '''

        notContains("MultiCastDataSinks")
    }

    sql '''insert into agg_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_11 'select * from agg_nop_t order by id, kint'

    sql '''insert into agg_nop_t with label label_agg_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from agg_nop_t order by id, kint'

    sql '''insert into agg_light_sc_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_21 'select * from agg_light_sc_nop_t order by id, kint'

    sql '''insert into agg_light_sc_nop_t with label label_agg_light_sc_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_22 'select * from agg_light_sc_nop_t order by id, kint'

    sql '''insert into agg_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_31 'select * from agg_not_null_nop_t order by id, kint'

    sql '''insert into agg_not_null_nop_t with label label_agg_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_32 'select * from agg_not_null_nop_t order by id, kint'

    sql '''insert into agg_light_sc_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_41 'select * from agg_light_sc_not_null_nop_t order by id, kint'

    sql '''insert into agg_light_sc_not_null_nop_t with label label_agg_light_sc_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_42 'select * from agg_light_sc_not_null_nop_t order by id, kint'

    sql '''insert into dup_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_11 'select * from dup_nop_t order by id, kint'

    sql '''insert into dup_nop_t with label label_dup_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from dup_nop_t order by id, kint'

    sql '''insert into dup_light_sc_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_21 'select * from dup_light_sc_nop_t order by id, kint'

    sql '''insert into dup_light_sc_nop_t with label label_dup_light_sc_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_22 'select * from dup_light_sc_nop_t order by id, kint'

    sql '''insert into dup_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_31 'select * from dup_not_null_nop_t order by id, kint'

    sql '''insert into dup_not_null_nop_t with label label_dup_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_32 'select * from dup_not_null_nop_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_41 'select * from dup_light_sc_not_null_nop_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_nop_t with label label_dup_light_sc_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_42 'select * from dup_light_sc_not_null_nop_t order by id, kint'

    sql '''insert into uni_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_11 'select * from uni_nop_t order by id, kint'

    sql '''insert into uni_nop_t with label label_uni_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from uni_nop_t order by id, kint'

    sql '''insert into uni_light_sc_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    qt_21 'select * from uni_light_sc_nop_t order by id, kint'

    sql '''insert into uni_light_sc_nop_t with label label_uni_light_sc_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_22 'select * from uni_light_sc_nop_t order by id, kint'

    sql '''insert into uni_mow_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_31 'select * from uni_mow_nop_t order by id, kint'

    sql '''insert into uni_mow_nop_t with label label_uni_mow_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_32 'select * from uni_mow_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_41 'select * from uni_light_sc_mow_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_nop_t with label label_uni_light_sc_mow_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_42 'select * from uni_light_sc_mow_nop_t order by id, kint'

    sql '''insert into uni_mow_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_51 'select * from uni_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_mow_not_null_nop_t with label label_uni_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_52 'select * from uni_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_61 'select * from uni_light_sc_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_nop_t with label label_uni_light_sc_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_62 'select * from uni_light_sc_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_mow_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_71 'select * from uni_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_mow_not_null_nop_t with label label_uni_mow_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_72 'select * from uni_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_81 'select * from uni_light_sc_mow_not_null_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_nop_t with label label_uni_light_sc_mow_not_null_cte_nop
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_82 'select * from uni_light_sc_mow_not_null_nop_t order by id, kint'

    // test light_schema_change
    sql 'alter table agg_light_sc_nop_t rename column ktint ktinyint'
    sql 'alter table agg_light_sc_not_null_nop_t rename column ktint ktinyint'

    sql '''insert into agg_light_sc_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_lsc1 'select * from agg_light_sc_nop_t order by id, kint'

    sql '''insert into agg_light_sc_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_lsc2 'select * from agg_light_sc_not_null_nop_t order by id, kint'

    // test light_schema_change
    sql 'alter table dup_light_sc_nop_t rename column ktint ktinyint'
    sql 'alter table dup_light_sc_not_null_nop_t rename column ktint ktinyint'

    sql '''insert into dup_light_sc_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_lsc1 'select * from dup_light_sc_nop_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_lsc2 'select * from dup_light_sc_not_null_nop_t order by id, kint'

    // test light_schema_change
    sql 'alter table uni_light_sc_nop_t rename column ktint ktinyint'
    sql 'alter table uni_light_sc_mow_nop_t rename column ktint ktinyint'
    sql 'alter table uni_light_sc_not_null_nop_t rename column ktint ktinyint'
    sql 'alter table uni_light_sc_mow_not_null_nop_t rename column ktint ktinyint'

    sql '''insert into uni_light_sc_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_lsc1 'select * from uni_light_sc_nop_t order by id, kint'

    sql '''insert into uni_light_sc_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_lsc2 'select * from uni_light_sc_not_null_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_nop_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_lsc3 'select * from uni_light_sc_mow_nop_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_nop_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_lsc4 'select * from uni_light_sc_mow_not_null_nop_t order by id, kint'

    sql 'set delete_without_partition=true'
    sql '''delete from agg_nop_t where id is not null'''
    sql '''delete from agg_nop_t where id is null'''
    sql '''delete from agg_light_sc_nop_t where id is not null'''
    sql '''delete from agg_light_sc_nop_t where id is null'''
    sql '''delete from agg_not_null_nop_t where id is not null'''
    sql '''delete from agg_not_null_nop_t where id is null'''
    sql '''delete from agg_light_sc_not_null_nop_t where id is not null'''
    sql '''delete from agg_light_sc_not_null_nop_t where id is null'''
    sql '''delete from dup_nop_t where id is not null'''
    sql '''delete from dup_nop_t where id is null'''
    sql '''delete from dup_light_sc_nop_t where id is not null'''
    sql '''delete from dup_light_sc_nop_t where id is null'''
    sql '''delete from dup_not_null_nop_t where id is not null'''
    sql '''delete from dup_not_null_nop_t where id is null'''
    sql '''delete from dup_light_sc_not_null_nop_t where id is not null'''
    sql '''delete from dup_light_sc_not_null_nop_t where id is null'''
    sql '''delete from uni_nop_t where id is not null'''
    sql '''delete from uni_nop_t where id is null'''
    sql '''delete from uni_light_sc_nop_t where id is not null'''
    sql '''delete from uni_light_sc_nop_t where id is null'''
    sql '''delete from uni_mow_nop_t where id is not null'''
    sql '''delete from uni_mow_nop_t where id is null'''
    sql '''delete from uni_light_sc_mow_nop_t where id is not null'''
    sql '''delete from uni_light_sc_mow_nop_t where id is null'''

    // TODO turn off fallback when storage layer support true predicate
    sql '''set enable_fallback_to_original_planner=true'''
    sql '''delete from uni_mow_not_null_nop_t where id is not null'''
    sql '''set enable_fallback_to_original_planner=false'''

    sql '''delete from uni_mow_not_null_nop_t where id is null'''

    // TODO turn off fallback when storage layer support true predicate
    sql '''set enable_fallback_to_original_planner=true'''
    sql '''delete from uni_light_sc_mow_not_null_nop_t where id is not null'''
    sql '''set enable_fallback_to_original_planner=false'''

    sql '''delete from uni_light_sc_mow_not_null_nop_t where id is null'''
    sql 'alter table agg_light_sc_nop_t rename column ktinyint ktint'
    sql 'alter table agg_light_sc_not_null_nop_t rename column ktinyint ktint'
    sql 'alter table dup_light_sc_nop_t rename column ktinyint ktint'
    sql 'alter table dup_light_sc_not_null_nop_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_nop_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_mow_nop_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_not_null_nop_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_mow_not_null_nop_t rename column ktinyint ktint'
}