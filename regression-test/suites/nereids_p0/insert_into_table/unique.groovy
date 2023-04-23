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

    // DDL
    sql '''
        create table uni_t (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(9, 3) null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table uni_light_sc_t (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(9, 3) null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''

    sql '''
        create table uni_mow_t (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(9, 3) null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table uni_light_sc_mow_t (
            `id` int null,
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcml` decimal(9, 3) null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''

    sql '''
        create table uni_not_null_t (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimal(9, 3) not null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table uni_light_sc_not_null_t (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimal(9, 3) not null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''

    sql '''
        create table uni_mow_not_null_t (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimal(9, 3) not null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1"
        )
    '''

    sql '''
        create table uni_light_sc_mow_not_null_t (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcml` decimal(9, 3) not null,
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
        unique key(id)
        partition by range(id) (
            partition p1 values less than ("3"),
            partition p2 values less than ("5"),
            partition p3 values less than ("7"),
            partition p4 values less than ("9")
        )
        distributed by hash(id) buckets 4
        properties (
           "replication_num"="1",
           "light_schema_change"="true"
        )
    '''
    // DDL end

    sql 'enable_nereids_planner=false'
    sql 'enable_fallback_to_original_planner=false'

    sql '''insert into uni_t
            select * except(kaint) from src order by id, kint'''
    qt_11 'select * from uni_t'

    sql '''insert into uni_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint'''
    qt_12 'select * from uni_t'

    sql '''insert into uni_t partition (p1, p2) with label label_uni
            select * except(kaint) from src order by id, kint where id < 4'''
    qt_13 'select * from uni_t'

    sql '''insert into uni_light_sc_t
            select * except(kaint) from src order by id, kint'''
    qt_21 'select * from uni_light_sc_t'

    sql '''insert into uni_light_sc_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint'''
    qt_22 'select * from uni_light_sc_t'

    sql '''insert into uni_light_sc_t partition (p1, p2) with label label_uni_light_sc
            select * except(kaint) from src order by id, kint id < 4'''
    qt_23 'select * from uni_light_sc_t'

    sql '''insert into uni_mow_t
            select * except(kaint) from src order by id, kint'''
    qt_31 'select * from uni_mow_t'

    sql '''insert into uni_mow_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint'''
    qt_32 'select * from uni_mow_t'

    sql '''insert into uni_mow_t partition (p1, p2) with label label_uni
            select * except(kaint) from src order by id, kint where id < 4'''
    qt_33 'select * from uni_mow_t'

    sql '''insert into uni_light_sc_mow_t
            select * except(kaint) from src order by id, kint'''
    qt_41 'select * from uni_light_sc_mow_t'

    sql '''insert into uni_light_sc_mow_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint'''
    qt_42 'select * from uni_light_sc_mow_t'

    sql '''insert into uni_light_sc_mow_t partition (p1, p2) with label label_uni_light_sc
            select * except(kaint) from src order by id, kint id < 4'''
    qt_43 'select * from uni_light_sc_mow_t'

    sql '''insert into uni_mow_not_null_t
            select * except(kaint) from src order by id, kint where id is not null'''
    qt_51 'select * from uni_mow_not_null_t'

    sql '''insert into uni_mow_not_null_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint where id is not null'''
    qt_52 'select * from uni_mow_not_null_t'

    sql '''insert into uni_mow_not_null_t partition (p1, p2) with label label_uni_not_null
            select * except(kaint) from src order by id, kint id < 4 where id is not null'''
    qt_53 'select * from uni_mow_not_null_t'

    sql '''insert into uni_t_light_sc_mow_not_null_t
            select * except(kaint) from src order by id, kint where id is not null'''
    qt_61 'select * from uni_light_sc_mow_not_null_t'

    sql '''insert into uni_t_light_sc_mow_not_null_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint where id is not null'''
    qt_62 'select * from uni_light_sc_mow_not_null_t'

    sql '''insert into uni_t_light_sc_mow_not_null_t partition (p1, p2) with label label_uni_light_sc_not_null
            select * except(kaint) from src order by id, kint where id < 4 where id is not null'''
    qt_63 'select * from uni_light_sc_mow_not_null_t'

    sql '''insert into uni_mow_not_null_t
            select * except(kaint) from src order by id, kint where id is not null'''
    qt_71 'select * from uni_mow_not_null_t'

    sql '''insert into uni_mow_not_null_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint where id is not null'''
    qt_72 'select * from uni_mow_not_null_t'

    sql '''insert into uni_mow_not_null_t partition (p1, p2) with label label_uni_not_null
            select * except(kaint) from src order by id, kint id < 4 where id is not null'''
    qt_73 'select * from uni_mow_not_null_t'

    sql '''insert into uni_t_light_sc_mow_not_null_t
            select * except(kaint) from src order by id, kint where id is not null'''
    qt_81 'select * from uni_light_sc_mow_not_null_t'

    sql '''insert into uni_t_light_sc_mow_not_null_t
            with cte as (select * except(kaint) from src)
            select * from cte order by id, kint where id is not null'''
    qt_82 'select * from uni_light_sc_mow_not_null_t'

    sql '''insert into uni_t_light_sc_mow_not_null_t partition (p1, p2) with label label_uni_light_sc_not_null
            select * except(kaint) from src order by id, kint where id < 4 where id is not null'''
    qt_83 'select * from uni_light_sc_mow_not_null_t'
}