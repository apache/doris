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
suite("eliminate_group_by") {
//    sql "set disable_nereids_rules='ELIMINATE_GROUP_BY'"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalQuickSort'"
    sql "drop table if exists test_unique2;"
    sql """create table test_unique2(a int not null, b int) unique key(a) distributed by hash(a) properties("replication_num"="1");"""
    sql "insert into test_unique2 values(1,2),(2,2),(3,4),(4,4),(5,null);"
    qt_count_star "select a,count(*) from test_unique2 group by a order by 1,2;"
    qt_count_1 "select a,count(1) from test_unique2 group by a order by 1,2;"
    qt_avg "select a,avg(b) from test_unique2 group by a order by 1,2;"
    qt_expr "select a,max(a+1),avg(abs(a+100)),sum(a+b) from test_unique2 group by a order by 1,2,3,4;"
    qt_window "select a,avg(sum(b) over(partition by b order by a)) from test_unique2 group by a order by 1,2"
    qt_two_args_func_min_by "select min_by(3,b),min_by(b,2),min_by(b,a), min_by(null,a), min_by(b, null), min_by(null, null) from test_unique2 group by a order by 1,2,3,4,5,6 "
    qt_two_args_func_max_by "select max_by(3,a),max_by(a,2),max_by(b,a), max_by(null,a), max_by(b, null), max_by(null, null) from test_unique2 group by a order by 1,2,3,4,5,6 "
    qt_two_args_func_avg_weighted  "select avg_weighted(b,2), avg_weighted(b,a), avg_weighted(null,a), avg_weighted(b, null), avg_weighted(null, null) from test_unique2 group by a order by 1,2,3,4,5 "
    qt_two_args_func_percentile "select percentile(b, null), percentile(null, null),percentile(b, 0.3) from test_unique2 group by a order by 1,2,3"
    qt_stddev "select a,stddev(b),stddev(null) from test_unique2 group by a order by 1,2,3;"
    qt_stddev_samp "select a,stddev_samp(b),stddev_samp(null) from test_unique2 group by a order by 1,2,3;"
    qt_variance "select a,variance(b),variance(null) from test_unique2 group by a order by 1,2,3;"
    qt_variance_samp "select a,variance_samp(b),variance_samp(null) from test_unique2 group by a order by 1,2,3;"
    qt_sum0 "select a,sum0(b),sum0(null) from test_unique2 group by a order by 1,2,3;"
    qt_median "select a,median(b),any_value(b),percentile(a,0.1),percentile(b,0.9),percentile(b,0.4) from test_unique2 group by a order by 1,2,3,4,5,6;"

    qt_count_star_shape "explain shape plan select a,count(*) from test_unique2 group by a order by 1,2;"
    qt_count_1_shape "explain shape plan select a,count(1) from test_unique2 group by a order by 1,2;"
    qt_avg_shape "explain shape plan select a,avg(b) from test_unique2 group by a order by 1,2;"
    qt_expr_shape "explain shape plan select a,max(a+1),avg(abs(a+100)),sum(a+b) from test_unique2 group by a order by 1,2,3,4;"
    qt_window_shape "explain shape plan select a,avg(sum(b) over(partition by b order by a)) from test_unique2 group by a order by 1,2"
    qt_two_args_func_min_by_shape "explain shape plan select min_by(3,b),min_by(b,2),min_by(b,a), min_by(null,a), min_by(b, null), min_by(null, null) from test_unique2 group by a order by 1,2,3,4,5,6 "
    qt_two_args_func_max_by_shape "explain shape plan select max_by(3,a),max_by(a,2),max_by(b,a), max_by(null,a), max_by(b, null), max_by(null, null) from test_unique2 group by a order by 1,2,3,4,5,6 "
    qt_two_args_func_avg_weighted_shape  "explain shape plan select avg_weighted(b,2), avg_weighted(b,a), avg_weighted(null,a), avg_weighted(b, null), avg_weighted(null, null) from test_unique2 group by a order by 1,2,3,4,5 "
    qt_two_args_func_percentile_shape "explain shape plan select percentile(b, null), percentile(null, null),percentile(b, 0.3) from test_unique2 group by a order by 1,2,3"
    qt_stddev_shape "explain shape plan select a,stddev(b),stddev(null) from test_unique2 group by a order by 1,2,3;"
    qt_stddev_samp_shape "explain shape plan select a,stddev_samp(b),stddev_samp(null) from test_unique2 group by a order by 1,2,3;"
    qt_variance_shape "explain shape plan select a,variance(b),variance(null) from test_unique2 group by a order by 1,2,3;"
    qt_variance_samp_shape "explain shape plan select a,variance_samp(b),variance_samp(null) from test_unique2 group by a order by 1,2,3;"
    qt_sum0_shape "explain shape plan select a,sum0(b),sum0(null) from test_unique2 group by a order by 1,2,3;"
    qt_median_shape "explain shape plan select a,median(b),any_value(b),percentile(a,0.1),percentile(b,0.9),percentile(b,0.4) from test_unique2 group by a order by 1,2,3,4,5,6;"
}