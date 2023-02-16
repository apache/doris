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

suite("nereids_window_fn") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    qt_sql_dense_rank "select ktint, ksint, dense_rank() over(partition by ktint order by ksint) as rank from fn_test"
    qt_sql_dense_rank_notnull "select ktint, ksint, dense_rank() over(partition by ktint order by ksint) as rank from fn_test_not_nullable"

    qt_sql_first_value "select ktint, ksint, first_value(kint) over(partition by ktint order by ksint) as fv from fn_test"
    qt_sql_first_value_notnull "select ktint, ksint, first_value(kint) over(partition by ktint order by ksint) as fv from fn_test_not_nullable"

    qt_sql_lag "select ktint, ksint, lag(kint, 2, 1) over(partition by ktint order by ksint) as lag from fn_test"
    qt_sql_lag_notnull "select ktint, ksint, lag(kint, 2, 1) over(partition by ktint order by ksint) as lag from fn_test_not_nullable"

    qt_sql_last_value "select ktint, ksint, last_value(kint) over(partition by ktint order by ksint) as lv from fn_test"
    qt_sql_last_value_notnull "select ktint, ksint, last_value(kint) over(partition by ktint order by ksint) as lv from fn_test_not_nullable"

    qt_sql_lead "select ktint, ksint, lead(kint, 2, 1) over(partition by ktint order by ksint) as ld from fn_test"
    qt_sql_lead_notnull "select ktint, ksint, lead(kint, 2, 1) over(partition by ktint order by ksint) as ld from fn_test_not_nullable"

    qt_sql_ntile "select ktint, ksint, ntile(3) over(partition by ktint order by ksint) as nt from fn_test"
    qt_sql_ntile_notnull "select ktint, ksint, ntile(3) over(partition by ktint order by ksint) as nt from fn_test_not_nullable"

    qt_sql_rank "select ktint, ksint, rank() over(partition by ktint order by ksint) as rank from fn_test"
    qt_sql_rank_notnull "select ktint, ksint, rank() over(partition by ktint order by ksint) as rank from fn_test_not_nullable"

    qt_sql_row_number "select ktint, ksint, row_number() over(partition by ktint order by ksint) as rn from fn_test"
    qt_sql_row_number_notnull "select ktint, ksint, row_number() over(partition by ktint order by ksint) as rn from fn_test_not_nullable"

}