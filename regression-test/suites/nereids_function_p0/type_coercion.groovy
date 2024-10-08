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
suite("function_type_coercion") {
    sql """set enable_fold_constant_by_be=false""" // remove this if array<double> BE return result be fixed.

    // scalar function
    qt_greatest """select greatest(1, 2222, '333')"""
    qt_least """select least(5,2000000,'3.0023')"""
    qt_if """select if (1, 2222, 33)"""
    qt_array_product """select array_product(array(1, 2, '3000'))"""
    qt_array_avg """select array_avg(array(1, 2, '3000'))"""
    qt_array_pushfront """select array_pushfront(array(1,2,3,555555), '4444')"""
    qt_array_pushback """select array_pushback(array(1,2,3,555555), '4444')"""
    qt_array_difference """select array_difference(array(1,2,'200'))"""
    qt_array_enumerate_uniq """select array_enumerate_uniq([1,1,1],['1','1','1.0'])"""
    qt_array_cum_sum """select array_cum_sum(array('1', '2', '3000'))"""
    qt_pmod """select pmod(2, '1.0')"""
    qt_nullif """SELECT nullif(13, -4851)"""

    // agg function
    sql """drop table if exists test_agg_signature"""

    sql """
        create table test_agg_signature (
            id int,
            c1 text,
            c2 text
        )
        properties (
            "replication_num" = "1"
        )
    """

    sql """insert into test_agg_signature values (1, "10", "65537"), (2, "129", "134"), (3, "65548", "3")"""

    qt_topn_weighted """select topn_weighted(12345678.12345678900000000000000000000, null, 2147483648)"""
    qt_corr """select corr(c1, c2) from test_agg_signature"""
    qt_covar """select covar(c1, c2) from test_agg_signature"""
    qt_covar_samp """select covar_samp(c1, c2) from test_agg_signature"""
    qt_group_bit_and """select group_bit_and(c1) from test_agg_signature"""
    qt_group_bit_or """select group_bit_or(c1) from test_agg_signature"""
    qt_group_bit_xor """select group_bit_xor(c1) from test_agg_signature"""
    qt_stddev """select stddev(c1) from test_agg_signature"""
    qt_stddev_samp """select stddev_samp(c1) from test_agg_signature"""
}
