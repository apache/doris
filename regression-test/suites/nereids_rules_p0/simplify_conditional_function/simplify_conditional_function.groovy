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
}