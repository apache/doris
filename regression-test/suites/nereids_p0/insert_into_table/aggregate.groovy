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

suite("nereids_insert_agglicate") {
    sql 'use nereids_insert_into_table_test'

    // DDL
    sql '''
        create table agg_t (
            `id` int null,
            `kbool` boolean null max,
            `ktint` tinyint(4) null max,
            `ksint` smallint(6) null max,
            `kint` int(11) null max,
            `kbint` bigint(20) null max,
            `klint` largeint(40) null max,
            `kfloat` float null max,
            `kdbl` double null max,
            `kdcml` decimal(9, 3) null replace,
            `kchr` char(10) null replace,
            `kvchr` varchar(10) null replace,
            `kstr` string null replace,
            `kdt` date null replace,
            `kdtv2` datev2 null replace,
            `kdtm` datetime null replace,
            `kdtmv2` datetimev2(0) null replace,
            `kdcml32v3` decimalv3(7, 3) null replace,
            `kdcml64v3` decimalv3(10, 5) null replace,
            `kdcml128v3` decimalv3(20, 8) null replace
        ) engine=OLAP
        aggregate key(id)
        distributed by hash(id) buckets 4
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table agg_light_sc_t (
            `id` int null,
            `kbool` boolean null max,
            `ktint` tinyint(4) null max,
            `ksint` smallint(6) null max,
            `kint` int(11) null max,
            `kbint` bigint(20) null max,
            `klint` largeint(40) null max,
            `kfloat` float null max,
            `kdbl` double null max,
            `kdcml` decimal(9, 3) null replace,
            `kchr` char(10) null replace,
            `kvchr` varchar(10) null replace,
            `kstr` string null replace,
            `kdt` date null replace,
            `kdtv2` datev2 null replace,
            `kdtm` datetime null replace,
            `kdtmv2` datetimev2(0) null replace,
            `kdcml32v3` decimalv3(7, 3) null replace,
            `kdcml64v3` decimalv3(10, 5) null replace,
            `kdcml128v3` decimalv3(20, 8) null replace
        ) engine=OLAP
        agglicate key(id)
        distributed by hash(id) buckets 4
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        properties (
           "replication_num"="1"
           "light_schema_change"="true"
        )
    '''

    sql '''
        create table agg_not_null_t (
            `id` int not null,
            `kbool` boolean not null max,
            `ktint` tinyint(4) not null max,
            `ksint` smallint(6) not null max,
            `kint` int(11) not null max,
            `kbint` bigint(20) not null max,
            `klint` largeint(40) not null max,
            `kfloat` float not null max,
            `kdbl` double not null max,
            `kdcml` decimal(9, 3) not null replace,
            `kchr` char(10) not null replace,
            `kvchr` varchar(10) not null replace,
            `kstr` string not null replace,
            `kdt` date not null replace,
            `kdtv2` datev2 not null replace,
            `kdtm` datetime not null replace,
            `kdtmv2` datetimev2(0) not null replace,
            `kdcml32v3` decimalv3(7, 3) not null replace,
            `kdcml64v3` decimalv3(10, 5) not null replace,
            `kdcml128v3` decimalv3(20, 8) not null replace
        ) engine=OLAP
        agglicate key(id)
        distributed by hash(id) buckets 4
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table agg_light_sc_not_null_t (
            `id` int not null,
            `kbool` boolean not null max,
            `ktint` tinyint(4) not null max,
            `ksint` smallint(6) not null max,
            `kint` int(11) not null max,
            `kbint` bigint(20) not null max,
            `klint` largeint(40) not null max,
            `kfloat` float not null max,
            `kdbl` double not null max,
            `kdcml` decimal(9, 3) not null replace,
            `kchr` char(10) not null replace,
            `kvchr` varchar(10) not null replace,
            `kstr` string not null replace,
            `kdt` date not null replace,
            `kdtv2` datev2 not null replace,
            `kdtm` datetime not null replace,
            `kdtmv2` datetimev2(0) not null replace,
            `kdcml32v3` decimalv3(7, 3) not null replace,
            `kdcml64v3` decimalv3(10, 5) not null replace,
            `kdcml128v3` decimalv3(20, 8) not null replace
        ) engine=OLAP
        agglicate key(id)
        distributed by hash(id) buckets 4
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''
    // DDL end

    sql 'enable_nereids_planner=true'
    sql 'enable_fallback_to_original_planner=false'

    sql '''insert into agg_t
            select * except(kaint) from src order by id, kint'''
    qt_11 'select * from agg_t'

    sql '''insert into agg_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint'''
    qt_12 'select * from agg_t'

    sql '''insert into agg_t partition (p1, p2) with label label_agg
            select * except(kaint) from src order by id, kint where id < 4'''
    qt_13 'select * from agg_t'

    sql '''insert into agg_light_sc_t
            select * except(kaint) from src order by id, kint'''
    qt_21 'select * from agg_light_sc_t'

    sql '''insert into agg_light_sc_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint'''
    qt_22 'select * from agg_light_sc_t'

    sql '''insert into agg_light_sc_t partition (p1, p2) with label label_agg_light_sc
            select * except(kaint) from src order by id, kint id < 4'''
    qt_23 'select * from agg_light_sc_t'

    sql '''insert into agg_not_null_t
            select * except(kaint) from src order by id, kint where id is not null'''
    qt_31 'select * from agg_not_null_t'

    sql '''insert into agg_not_null_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint where id is not null'''
    qt_32 'select * from agg_not_null_t'

    sql '''insert into agg_not_null_t partition (p1, p2) with label label_agg_not_null
            select * except(kaint) from src order by id, kint id < 4 where id is not null'''
    qt_33 'select * from agg_not_null_t'

    sql '''insert into agg_t_light_sc_not_null_t
            select * except(kaint) from src order by id, kint where id is not null'''
    qt_41 'select * from agg_light_sc_not_null_t'

    sql '''insert into agg_t_light_sc_not_null_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint where id is not null'''
    qt_42 'select * from agg_light_sc_not_null_t'

    sql '''insert into agg_t_light_sc_not_null_t partition (p1, p2) with label label_agg_light_sc_not_null
            select * except(kaint) from src order by id, kint where id < 4 where id is not null'''
    qt_43 'select * from agg_light_sc_not_null_t'
}