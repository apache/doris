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

    def tables = ['dup_t_type_cast', 'dup_light_sc_t_type_cast', 'dup_not_null_t_type_cast', 'dup_light_sc_not_null_t_type_cast']

    for (t in tables) {
        sql "drop table if exists ${t}"
    }

    // DDL
    sql '''
        create table dup_t_type_cast (
            `id` int null,
            `kint` int(11) null,
            `kdbl` double null,
            `kdcml` decimal(20, 6) null,
            `kvchr` varchar(20) null,
            `kdt` date null,
            `kdtmv2` datetimev2(0) null,
            `kdcml32v3` decimalv3(7, 3) null
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
        create table dup_light_sc_t_type_cast (
            `id` int null,
            `kint` int(11) null,
            `kdbl` double null,
            `kdcml` decimal(20, 6) null,
            `kvchr` varchar(20) null,
            `kdt` date null,
            `kdtmv2` datetimev2(0) null,
            `kdcml32v3` decimalv3(7, 3) null
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
        create table dup_not_null_t_type_cast (
            `id` int not null,
            `kint` int(11) not null,
            `kdbl` double not null,
            `kdcml` decimal(20, 6) not null,
            `kvchr` varchar(20) not null,
            `kdt` date not null,
            `kdtmv2` datetimev2(0) not null,
            `kdcml32v3` decimalv3(7, 3) not null
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
        create table dup_light_sc_not_null_t_type_cast (
            `id` int not null,
            `kint` int(11) not null,
            `kdbl` double not null,
            `kdcml` decimal(20, 6) not null,
            `kvchr` varchar(20) not null,
            `kdt` date not null,
            `kdtmv2` datetimev2(0) not null,
            `kdcml32v3` decimalv3(7, 3) not null
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

    sql '''insert into dup_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src'''
    qt_11 'select * from dup_t_type_cast order by id, kint'

    sql '''insert into dup_t_type_cast with label label_dup_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte'''
    qt_12 'select * from dup_t_type_cast order by id, kint'

    sql '''insert into dup_t_type_cast partition (p1, p2) with label label_dup
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4'''
    qt_13 'select * from dup_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src'''
    qt_21 'select * from dup_light_sc_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_t_type_cast with label label_dup_light_sc_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte'''
    qt_22 'select * from dup_light_sc_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_t_type_cast partition (p1, p2) with label label_dup_light_sc
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4'''
    qt_23 'select * from dup_light_sc_t_type_cast order by id, kint'

    sql '''insert into dup_not_null_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id is not null'''
    qt_31 'select * from dup_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_not_null_t_type_cast with label label_dup_not_null_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte where id is not null'''
    qt_32 'select * from dup_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_not_null_t_type_cast partition (p1, p2) with label label_dup_not_null
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4 and id is not null'''
    qt_33 'select * from dup_not_null_t_type_cast order by id, kint'

    sql '''insert into dup"_light_sc_not_null_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id is not null'''
    qt_41 'select * from dup_light_sc_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_not_null_t_type_cast with label label_dup_light_sc_not_null_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte where id is not null'''
    qt_42 'select * from dup_light_sc_not_null_t_type_cast order by id, kint'

    sql '''insert into dup_light_sc_not_null_t_type_cast partition (p1, p2) with label label_dup_light_sc_not_null
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4 and id is not null'''
    qt_43 'select * from dup_light_sc_not_null_t_type_cast order by id, kint'
}