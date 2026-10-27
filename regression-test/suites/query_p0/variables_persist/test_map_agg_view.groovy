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

// Arithmetic on a decimal wider than decimal128 (the output of a view created with
// enable_decimal256 on, or a decimal256 column) after enable_decimal256 is turned off.
suite("test_map_agg_view") {
    multi_sql """
    set enable_decimal256=true;
    drop table if exists test_map_agg_view;
    create table test_map_agg_view (
        k int,
        d decimalv3(38, 10),
        d256 decimalv3(76, 10),
        m map<string, decimalv3(38, 10)>
    ) properties("replication_num" = "1");
    """
    sql """
    insert into test_map_agg_view values
    (1, 1.5, 1.5, map('a', 1.5)),
    (2, 2.25, 2.25, map('a', 2.25));
    """

    multi_sql """
    set enable_decimal256=true;
    drop view if exists v_map_agg_sum_map;
    create view v_map_agg_sum_map as select sum_map(m) as sm from test_map_agg_view;
    drop view if exists v_map_agg_avg_map;
    create view v_map_agg_avg_map as select avg_map(m) as am from test_map_agg_view;
    drop view if exists v_map_agg_sum;
    create view v_map_agg_sum as select sum(d) as s from test_map_agg_view;
    """
    qt_sum_map_plus_256_create "select element_at(map_values(sm), 1) + cast(1 as decimal(38, 10)) from v_map_agg_sum_map"

    sql "set enable_decimal256=false;"
    qt_sum_map_from_view "select sm from v_map_agg_sum_map"
    qt_sum_map_plus "select element_at(map_values(sm), 1) + cast(1 as decimal(38, 10)) from v_map_agg_sum_map"
    qt_avg_map_plus "select element_at(map_values(am), 1) + cast(1 as decimal(38, 10)) from v_map_agg_avg_map"
    qt_sum_minus "select s - cast(1 as decimal(38, 10)) from v_map_agg_sum"
    order_qt_col_arithmetic "select k, d256 + 1, d256 - cast(1 as decimal(38, 10)), d256 % cast(1 as decimal(38, 10)) from test_map_agg_view"

    sql "set enable_decimal256=true;"
    qt_sum_map_from_view_256 "select sm from v_map_agg_sum_map"
    qt_sum_map_plus_256 "select element_at(map_values(sm), 1) + cast(1 as decimal(38, 10)) from v_map_agg_sum_map"
    qt_avg_map_plus_256 "select element_at(map_values(am), 1) + cast(1 as decimal(38, 10)) from v_map_agg_avg_map"
    qt_sum_minus_256 "select s - cast(1 as decimal(38, 10)) from v_map_agg_sum"
    order_qt_col_arithmetic_256 "select k, d256 + 1, d256 - cast(1 as decimal(38, 10)), d256 % cast(1 as decimal(38, 10)) from test_map_agg_view"

    // a view created with enable_decimal256 off and queried with it on
    multi_sql """
    set enable_decimal256=false;
    drop view if exists v_map_agg_sum_map_reverse;
    create view v_map_agg_sum_map_reverse as select sum_map(m) as sm from test_map_agg_view;
    set enable_decimal256=true;
    """
    qt_sum_map_plus_reverse "select element_at(map_values(sm), 1) + cast(1 as decimal(38, 10)) from v_map_agg_sum_map_reverse"
}
