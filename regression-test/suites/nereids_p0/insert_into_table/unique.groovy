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

suite("nereids_insert_unique") {
    sql 'use nereids_insert_into_table_test'
    sql 'clean label from nereids_insert_into_table_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql 'set enable_strict_consistency_dml=true'

    sql '''insert into uni_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_11 'select * from uni_t order by id, kint'

    sql '''insert into uni_t with label label_uni_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_12 'select * from uni_t order by id, kint'

    sql '''insert into uni_t partition (p1, p2) with label label_uni
            select * except(kaint, kmintint, kjson) from src where id < 4'''
    sql 'sync'
    qt_13 'select * from uni_t order by id, kint'

    sql '''insert into uni_light_sc_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_21 'select * from uni_light_sc_t order by id, kint'

    sql '''insert into uni_light_sc_t with label label_uni_light_sc_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_22 'select * from uni_light_sc_t order by id, kint'

    sql '''insert into uni_light_sc_t partition (p1, p2) with label label_uni_light_sc
            select * except(kaint, kmintint, kjson) from src where id < 4'''
    sql 'sync'
    qt_23 'select * from uni_light_sc_t order by id, kint'

    sql '''insert into uni_mow_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_31 'select * from uni_mow_t order by id, kint'

    sql '''insert into uni_mow_t with label label_uni_mow_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_32 'select * from uni_mow_t order by id, kint'

    sql '''insert into uni_mow_t partition (p1, p2) with label label_uni_mow
            select * except(kaint, kmintint, kjson) from src where id < 4'''
    sql 'sync'
    qt_33 'select * from uni_mow_t order by id, kint'

    sql '''insert into uni_light_sc_mow_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_41 'select * from uni_light_sc_mow_t order by id, kint'

    sql '''insert into uni_light_sc_mow_t with label label_uni_light_sc_mow_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte'''
    sql 'sync'
    qt_42 'select * from uni_light_sc_mow_t order by id, kint'

    sql '''insert into uni_light_sc_mow_t partition (p1, p2) with label label_uni_light_sc_mow
            select * except(kaint, kmintint, kjson) from src where id < 4'''
    sql 'sync'
    qt_43 'select * from uni_light_sc_mow_t order by id, kint'

    sql '''insert into uni_mow_not_null_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_51 'select * from uni_mow_not_null_t order by id, kint'

    sql '''insert into uni_mow_not_null_t with label label_uni_not_null_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_52 'select * from uni_mow_not_null_t order by id, kint'

    sql '''insert into uni_mow_not_null_t partition (p1, p2) with label label_uni_not_null
            select * except(kaint, kmintint, kjson) from src where id < 4 and id is not null'''
    sql 'sync'
    qt_53 'select * from uni_mow_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_61 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t with label label_uni_light_sc_not_null_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_62 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t partition (p1, p2) with label label_uni_light_sc_not_null
            select * except(kaint, kmintint, kjson) from src where id < 4 and id is not null'''
    sql 'sync'
    qt_63 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    sql '''insert into uni_mow_not_null_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_71 'select * from uni_mow_not_null_t order by id, kint'

    sql '''insert into uni_mow_not_null_t with label label_uni_mow_not_null_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_72 'select * from uni_mow_not_null_t order by id, kint'

    sql '''insert into uni_mow_not_null_t partition (p1, p2) with label label_uni_mow_not_null
            select * except(kaint, kmintint, kjson) from src where id < 4 and id is not null'''
    sql 'sync'
    qt_73 'select * from uni_mow_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_81 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t with label label_uni_light_sc_mow_not_null_cte
            with cte as (select * except(kaint, kmintint, kjson) from src)
            select * from cte where id is not null'''
    sql 'sync'
    qt_82 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t partition (p1, p2) with label label_uni_light_sc_mow_not_null
            select * except(kaint, kmintint, kjson) from src where id < 4 and id is not null'''
    sql 'sync'
    qt_83 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    // test light_schema_change
    sql 'alter table uni_light_sc_t rename column ktint ktinyint'
    sql 'alter table uni_light_sc_mow_t rename column ktint ktinyint'
    sql 'alter table uni_light_sc_not_null_t rename column ktint ktinyint'
    sql 'alter table uni_light_sc_mow_not_null_t rename column ktint ktinyint'

    sql '''insert into uni_light_sc_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_lsc1 'select * from uni_light_sc_t order by id, kint'

    sql '''insert into uni_light_sc_not_null_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_lsc2 'select * from uni_light_sc_not_null_t order by id, kint'

    sql '''insert into uni_light_sc_mow_t
            select * except(kaint, kmintint, kjson) from src'''
    sql 'sync'
    qt_lsc3 'select * from uni_light_sc_mow_t order by id, kint'

    sql '''insert into uni_light_sc_mow_not_null_t
            select * except(kaint, kmintint, kjson) from src where id is not null'''
    sql 'sync'
    qt_lsc4 'select * from uni_light_sc_mow_not_null_t order by id, kint'

    sql 'alter table uni_light_sc_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_mow_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_not_null_t rename column ktinyint ktint'
    sql 'alter table uni_light_sc_mow_not_null_t rename column ktinyint ktint'

}