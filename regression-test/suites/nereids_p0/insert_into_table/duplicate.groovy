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

        def tables = ['dup_t', 'dup_light_sc_t', 'dup_not_null_t', 'dup_light_sc_not_null_t']

    for (t in tables) {
        sql "drop table if exists ${t}"
    }

    sql 'clean label from nereids_insert_into_table_test'

    // DDL
    sql '''
        create table dup_t (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(12, 6) null,
            `kchr` char(10) null,
            `kvchr` varchar(10) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2` datetimev2(0) null,
            `kdcml32v3` decimalv3(7, 3) null,
            `kdcml64v3` decimalv3(10, 5) null,
            `kdcml128v3` decimalv3(20, 8) null
        ) engine=OLAP
        duplicate key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("15")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table dup_light_sc_t (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(12, 6) null,
            `kchr` char(10) null,
            `kvchr` varchar(10) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2` datetimev2(0) null,
            `kdcml32v3` decimalv3(7, 3) null,
            `kdcml64v3` decimalv3(10, 5) null,
            `kdcml128v3` decimalv3(20, 8) null
        ) engine=OLAP
        duplicate key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("15")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''

    sql '''
        create table dup_not_null_t (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimal(12, 6) not null,
            `kchr` char(10) not null,
            `kvchr` varchar(10) not null,
            `kstr` string not null,
            `kdt` date not null,
            `kdtv2` datev2 not null,
            `kdtm` datetime not null,
            `kdtmv2` datetimev2(0) not null,
            `kdcml32v3` decimalv3(7, 3) not null,
            `kdcml64v3` decimalv3(10, 5) not null,
            `kdcml128v3` decimalv3(20, 8) not null
        ) engine=OLAP
        duplicate key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("15")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table dup_light_sc_not_null_t (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimal(12, 6) not null,
            `kchr` char(10) not null,
            `kvchr` varchar(10) not null,
            `kstr` string not null,
            `kdt` date not null,
            `kdtv2` datev2 not null,
            `kdtm` datetime not null,
            `kdtmv2` datetimev2(0) not null,
            `kdcml32v3` decimalv3(7, 3) not null,
            `kdcml64v3` decimalv3(10, 5) not null,
            `kdcml128v3` decimalv3(20, 8) not null
        ) engine=OLAP
        duplicate key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("15")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''
    // DDL end

    sql 'set enable_nereids_planner=false'
    sql 'set enable_fallback_to_original_planner=false'

    sql '''insert into dup_t
            select * except(kaint) from src'''
    qt_11 'select * from dup_t order by id, kint'

    sql '''insert into dup_t with label label_dup_cte
            with cte as (select * except(kaint) from src)
            select * from cte'''
    qt_12 'select * from dup_t order by id, kint'

    sql '''insert into dup_t partition (p1, p2) with label label_dup
            select * except(kaint) from src where id < 4'''
    qt_13 'select * from dup_t order by id, kint'

    sql '''insert into dup_light_sc_t
            select * except(kaint) from src'''
    qt_21 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_light_sc_t with label label_dup_light_sc_cte
            with cte as (select * except(kaint) from src)
            select * from cte'''
    qt_22 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_light_sc_t partition (p1, p2) with label label_dup_light_sc
            select * except(kaint) from src where id < 4'''
    qt_23 'select * from dup_light_sc_t order by id, kint'

    sql '''insert into dup_not_null_t
            select * except(kaint) from src where id is not null'''
    qt_31 'select * from dup_not_null_t order by id, kint'

    sql '''insert into dup_not_null_t with label label_dup_not_null_cte
            with cte as (select * except(kaint) from src)
            select * from cte where id is not null'''
    qt_32 'select * from dup_not_null_t order by id, kint'

    sql '''insert into dup_not_null_t partition (p1, p2) with label label_dup_not_null
            select * except(kaint) from src where id < 4 and id is not null'''
    qt_33 'select * from dup_not_null_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t
            select * except(kaint) from src where id is not null'''
    qt_41 'select * from dup_light_sc_not_null_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t with label label_dup_light_sc_not_null_cte
            with cte as (select * except(kaint) from src)
            select * from cte where id is not null'''
    qt_42 'select * from dup_light_sc_not_null_t order by id, kint'

    sql '''insert into dup_light_sc_not_null_t partition (p1, p2) with label label_dup_light_sc_not_null
            select * except(kaint) from src where id < 4 and id is not null'''
    qt_43 'select * from dup_light_sc_not_null_t order by id, kint'
}