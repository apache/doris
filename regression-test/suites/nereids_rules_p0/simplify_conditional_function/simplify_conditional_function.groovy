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
suite("simplify_conditional_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "drop table if exists test_simplify_conditional_function"
    sql """create table test_simplify_conditional_function(c int null, b double null, a varchar(100) not null) distributed by hash(c)
    properties("replication_num"="1");
    """
    sql "insert into test_simplify_conditional_function values(1,2.7,'abc'),(2,7.8,'ccd'),(null,8,'qwe'),(9,null,'ab')"

    qt_test_coalesce_null_begin1 "select coalesce(null, null, a, b, c),a,b from test_simplify_conditional_function order by 1,2,3"
    qt_test_coalesce_null_begin2 "select coalesce(null, null, a) from test_simplify_conditional_function order by 1"
    qt_test_coalesce_null_begin3 "select coalesce(null, null, c, a) from test_simplify_conditional_function order by 1"
    qt_test_coalesce_nonnull_begin "select coalesce(c, a) from test_simplify_conditional_function order by 1"
    qt_test_coalesce_nullalbe_begin "select coalesce(b, a, null) from test_simplify_conditional_function order by 1"
    qt_test_coalesce_null_null "select coalesce(null, null) from test_simplify_conditional_function order by 1"
    qt_test_coalesce_null "select coalesce(null) from test_simplify_conditional_function order by 1"
    qt_test_coalesce_nonnull "select coalesce(c),a,b from test_simplify_conditional_function order by 1,2,3"
    qt_test_coalesce_nullable "select coalesce(b) from test_simplify_conditional_function order by 1"

    qt_test_nvl_null_nullable "select ifnull(null, a) from test_simplify_conditional_function order by 1 "
    qt_test_nvl_null_nonnullable "select ifnull(null, c) from test_simplify_conditional_function order by 1 "
    qt_test_nvl_nullable_nullable "select ifnull(a, b) from test_simplify_conditional_function order by 1 "
    qt_test_nvl_nonnullable_null "select ifnull(c, null) from test_simplify_conditional_function order by 1 "
    qt_test_nvl_null_null "select ifnull(null, null) from test_simplify_conditional_function order by 1 "

    qt_test_nullif_null_nullable "select nullif(null, a),c from test_simplify_conditional_function order by 1,2 "
    qt_test_nullif_null_nonnullable "select nullif(null, c) from test_simplify_conditional_function order by 1 "
    qt_test_nullif_nonnullable_null "select nullif(c, null) from test_simplify_conditional_function order by 1 "
    qt_test_nullif_nullable_null "select nullif(a, null),c from test_simplify_conditional_function order by 1,2 "
    qt_test_nullif_nullable_nonnullable "select nullif(a, c) from test_simplify_conditional_function order by 1 "
    qt_test_nullif_null_null "select nullif(null, null) from test_simplify_conditional_function order by 1 "

    qt_test_outer_ref_coalesce "select c1 from (select coalesce(null,a,c) c1,a,b from test_simplify_conditional_function order by c1,a,b limit 2) t group by c1 order by c1"
    qt_test_outer_ref_nvl "select c1 from (select ifnull(null, c) c1 from test_simplify_conditional_function order by 1 limit 2) t group by c1 order by c1"
    qt_test_outer_ref_nullif "select c1 from (select nullif(a, null) c1,c from test_simplify_conditional_function order by c1,c limit 2 ) t group by c1 order by c1"

    qt_test_nullable_nullif "SELECT COUNT( DISTINCT NULLIF ( 1, NULL ) ), COUNT( DISTINCT 72 )"

    sql "drop table if exists table_20_30_utf8_partitions2_keys3_properties4_distributed_by5"
    sql """create table table_20_30_utf8_partitions2_keys3_properties4_distributed_by5 (
        pk int,
        col_varchar_64__undef_signed_not_null varchar(64),
        col_date_undef_signed_not_null date,
        col_datetime_3__undef_signed_not_null datetime(3),
        col_datetime_0__undef_signed datetime(0)
    ) distributed by hash(pk) properties("replication_num"="1");"""

    sql """drop table if exists table_24_50_utf8_partitions2_keys3_properties4_distributed_by5"""
    sql """create table table_24_50_utf8_partitions2_keys3_properties4_distributed_by5 (
        pk int,
        col_varchar_64__undef_signed varchar(64),
        col_datetime_6__undef_signed_not_null datetime(6),
        col_datetime_0__undef_signed datetime(0)
    ) distributed by hash(pk) properties("replication_num"="1");"""

    sql """insert into table_20_30_utf8_partitions2_keys3_properties4_distributed_by5 values
        (1,'k1','2012-01-01','2012-02-02 01:02:03', '2010-01-01 00:00:00'),
        (2,'k2','2015-06-01', null, '2011-01-01 00:00:00'),
        (3,'k3','2010-03-03','2010-04-04 04:04:04','2009-01-01 00:00:00'),
        (4,'k4','2018-12-12', null, null),
        (5,'k5','2020-07-07','2020-08-08 08:08:08','2015-05-05 05:05:05')"""

    sql """insert into table_24_50_utf8_partitions2_keys3_properties4_distributed_by5 values
        (101,'k1','2011-11-11 11:11:11', null),
        (102,'k2','2016-06-06 06:06:06','2000-01-01 00:00:00'),
        (103,'kx','2009-09-09 09:09:09', null),
        (104,'k4','2019-09-09 09:09:09','2012-12-12 12:12:12'),
        (105,'zz','2021-01-01 01:01:01', null)"""

    qt_test_coalesce_custom """
    select
            coalesce(
                    t1.col_datetime_3__undef_signed_not_null,
                    t2.col_datetime_6__undef_signed_not_null
            ),
            coalesce(t2.col_datetime_0__undef_signed, t1.col_datetime_0__undef_signed)
    from
            (select * from table_20_30_utf8_partitions2_keys3_properties4_distributed_by5) as t1
    left join
            table_24_50_utf8_partitions2_keys3_properties4_distributed_by5 as t2
            on t1.col_varchar_64__undef_signed_not_null = t2.col_varchar_64__undef_signed
    where t1.col_date_undef_signed_not_null > '2011-05-12'
    order by t1.pk nulls last, t2.pk nulls last
    limit 8"""
}