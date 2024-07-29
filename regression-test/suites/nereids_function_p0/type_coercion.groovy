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
}
