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

suite("nereids_insert_aggregate_type_cast") {
    sql 'use nereids_insert_into_table_test'

    def tables = ['agg_t_type_cast', 'agg_light_sc_t_type_cast', 'agg_not_null_t_type_cast', 'agg_light_sc_not_null_t_type_cast']

    for (t in tables) {
        sql "drop table if exists ${t}"
    }

    // DDL
    sql '''
        create table agg_t_type_cast (
            `id` int null,
            `kint` int(11) max null,
            `kdbl` double max null,
            `kdcml` decimal(20, 6) replace null,
            `kvchr` varchar(20) replace null,
            `kdt` date replace null,
            `kdtmv2` datetimev2(0) replace null,
            `kdcml32v3` decimalv3(7, 3) replace null
        ) engine=OLAP
        aggregate key(id)
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
        create table agg_light_sc_t_type_cast (
            `id` int null,
            `kint` int(11) max null,
            `kdbl` double max null,
            `kdcml` decimal(20, 6) replace null,
            `kvchr` varchar(20) replace null,
            `kdt` date replace null,
            `kdtmv2` datetimev2(0) replace null,
            `kdcml32v3` decimalv3(7, 3) replace null
        ) engine=OLAP
        aggregate key(id)
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
        create table agg_not_null_t_type_cast (
            `id` int not null,
            `kint` int(11) max not null,
            `kdbl` double max not null,
            `kdcml` decimal(20, 6) replace not null,
            `kvchr` varchar(20) replace not null,
            `kdt` date replace not null,
            `kdtmv2` datetimev2(0) replace not null,
            `kdcml32v3` decimalv3(7, 3) replace not null
        ) engine=OLAP
        aggregate key(id)
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
        create table agg_light_sc_not_null_t_type_cast (
            `id` int not null,
            `kint` int(11) max not null,
            `kdbl` double max not null,
            `kdcml` decimal(20, 6) replace not null,
            `kvchr` varchar(20) replace not null,
            `kdt` date replace not null,
            `kdtmv2` datetimev2(0) replace not null,
            `kdcml32v3` decimalv3(7, 3) replace not null
        ) engine=OLAP
        aggregate key(id)
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

    sql '''insert into agg_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src'''
    qt_11 'select * from agg_t_type_cast order by id, kint'

    sql '''insert into agg_t_type_cast with label label_agg_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte'''
    qt_12 'select * from agg_t_type_cast order by id, kint'

    sql '''insert into agg_t_type_cast partition (p1, p2) with label label_agg
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4'''
    qt_13 'select * from agg_t_type_cast order by id, kint'

    sql '''insert into agg_light_sc_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src'''
    qt_21 'select * from agg_light_sc_t_type_cast order by id, kint'

    sql '''insert into agg_light_sc_t_type_cast with label label_agg_light_sc_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte'''
    qt_22 'select * from agg_light_sc_t_type_cast order by id, kint'

    sql '''insert into agg_light_sc_t_type_cast partition (p1, p2) with label label_agg_light_sc
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4'''
    qt_23 'select * from agg_light_sc_t_type_cast order by id, kint'

    sql '''insert into agg_not_null_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id is not null'''
    qt_31 'select * from agg_not_null_t_type_cast order by id, kint'

    sql '''insert into agg_not_null_t_type_cast with label label_agg_not_null_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte where id is not null'''
    qt_32 'select * from agg_not_null_t_type_cast order by id, kint'

    sql '''insert into agg_not_null_t_type_cast partition (p1, p2) with label label_agg_not_null
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4 and id is not null'''
    qt_33 'select * from agg_not_null_t_type_cast order by id, kint'

    sql '''insert into agg_light_sc_not_null_t_type_cast
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id is not null'''
    qt_41 'select * from agg_light_sc_not_null_t_type_cast order by id, kint'

    sql '''insert into agg_light_sc_not_null_t_type_cast with label label_agg_light_sc_not_null_cte
            with cte as (select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src)
            select * from cte where id is not null'''
    qt_42 'select * from agg_light_sc_not_null_t_type_cast order by id, kint'

    sql '''insert into agg_light_sc_not_null_t_type_cast partition (p1, p2) with label label_agg_light_sc_not_null
            select id, ktint, ksint, kint, kbint, klint, kfloat, kdbl from src where id < 4 and id is not null'''
    qt_43 'select * from agg_light_sc_not_null_t_type_cast order by id, kint'
}