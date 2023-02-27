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

suite("test_array_functions_by_literal") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // array_contains function
    // Nereids does't support array function
    // qt_sql "select array_contains([1,2,3], 1)"
    // Nereids does't support array function
    // qt_sql "select array_contains([1,2,3], 4)"
    // Nereids does't support array function
    // qt_sql "select array_contains([1,2,3,NULL], 1)"
    // Nereids does't support array function
    // qt_sql "select array_contains([1,2,3,NULL], NULL)"
    // Nereids does't support array function
    // qt_sql "select array_contains([], 1)"
    // Nereids does't support array function
    // qt_sql "select array_contains([], NULL)"
    // Nereids does't support array function
    // qt_sql "select array_contains(NULL, 1)"
    // Nereids does't support array function
    // qt_sql "select array_contains(NULL, NULL)"
    // Nereids does't support array function
    // qt_sql "select array_contains([true], false)"

    // array_position function
    // Nereids does't support array function
    // qt_sql "select array_position([1,2,3], 1)"
    // Nereids does't support array function
    // qt_sql "select array_position([1,2,3], 3)"
    // Nereids does't support array function
    // qt_sql "select array_position([1,2,3], 4)"
    // Nereids does't support array function
    // qt_sql "select array_position([NULL,2,3], 2)"
    // Nereids does't support array function
    // qt_sql "select array_position([NULL,2,3], NULL)"
    // Nereids does't support array function
    // qt_sql "select array_position([], 1)"
    // Nereids does't support array function
    // qt_sql "select array_position([], NULL)"
    // Nereids does't support array function
    // qt_sql "select array_position(NULL, 1)"
    // Nereids does't support array function
    // qt_sql "select array_position(NULL, NULL)"
    // Nereids does't support array function
    // qt_sql "select array_position([null], 0)"
    // Nereids does't support array function
    // qt_sql "select array_position([0], null)"
    // Nereids does't support array function
    // qt_sql "select array_position([null, '1'], '')"
    // Nereids does't support array function
    // qt_sql "select array_position([''], null)"
    // Nereids does't support array function
    // qt_sql "select array_position([false, NULL, true], true)"

    // element_at function
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,3], 1)"
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,3], 3)"
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,3], 4)"
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,3], -1)"
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,3], NULL)"
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,NULL], 3)"
    // Nereids does't support array function
    // qt_sql "select element_at([1,2,NULL], 2)"
    // Nereids does't support array function
    // qt_sql "select element_at([], -1)"
    // Nereids does't support array function
    // qt_sql "select element_at([true, NULL, false], 2)"

    // array subscript function
    // Nereids does't support array function
    // qt_sql "select [1,2,3][1]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][3]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][4]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][-1]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][NULL]"
    // Nereids does't support array function
    // qt_sql "select [1,2,NULL][3]"
    // Nereids does't support array function
    // qt_sql "select [1,2,NULL][2]"
    // Nereids does't support array function
    // qt_sql "select [][-1]"
    // Nereids does't support array function
    // qt_sql "select [true, false]"

    // array_aggregation function
    // Nereids does't support array function
    // qt_sql "select array_avg([1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_sum([1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_min([1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_max([1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_product([1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_avg([1,2,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_sum([1,2,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_min([1,2,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_max([1,2,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_product([1,2,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_avg([])"
    // Nereids does't support array function
    // qt_sql "select array_sum([])"
    // Nereids does't support array function
    // qt_sql "select array_min([])"
    // Nereids does't support array function
    // qt_sql "select array_max([])"
    // Nereids does't support array function
    // qt_sql "select array_product([])"
    // Nereids does't support array function
    // qt_sql "select array_avg([null])"
    // Nereids does't support array function
    // qt_sql "select array_sum([null])"
    // Nereids does't support array function
    // qt_sql "select array_min([null])"
    // Nereids does't support array function
    // qt_sql "select array_max([null])"
    // Nereids does't support array function
    // qt_sql "select array_product([null])"
    // Nereids does't support array function
    // qt_sql "select array_product([1.12, 3.45, 4.23])"
    // Nereids does't support array function
    // qt_sql "select array_product([1.12, 3.45, -4.23])"

    // array_distinct function
    // Nereids does't support array function
    // qt_sql "select array_distinct([1,1,2,2,3,3])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([1,1,2,2,3,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([1,1,3,3,null, null, null])"
    // Nereids does't support array function
    // qt_sql "select array_distinct(['a','a','a'])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([null, 'a','a','a', null])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([true, false, false, null])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([null,null])"
    // Nereids does't support array function
    // qt_sql "select array_distinct([1, 0, 0, null])"


    // array_remove function
    // Nereids does't support array function
    // qt_sql "select array_remove([1,2,3], 1)"
    // Nereids does't support array function
    // qt_sql "select array_remove([1,2,3,null], 1)"
    // Nereids does't support array function
    // qt_sql "select array_remove(['a','b','c'], 'a')"
    // Nereids does't support array function
    // qt_sql "select array_remove(['a','b','c',null], 'a')"
    // Nereids does't support array function
    // qt_sql "select array_remove([true, false, false], false)"
 
    // array_sort function
    // Nereids does't support array function
    // qt_sql "select array_sort([1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_sort([3,2,1])"
    // Nereids does't support array function
    // qt_sql "select array_sort([1,2,3,null])"
    // Nereids does't support array function
    // qt_sql "select array_sort([null,1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_sort(['a','b','c'])"
    // Nereids does't support array function
    // qt_sql "select array_sort(['c','b','a'])"
    // Nereids does't support array function
    // qt_sql "select array_sort([true, false, true])"
    // Nereids does't support array function
    // qt_sql "select array_sort([])"

    // array_overlap function
    // Nereids does't support array function
    // qt_sql "select arrays_overlap([1,2,3], [4,5,6])"
    // Nereids does't support array function
    // qt_sql "select arrays_overlap([1,2,3], [3,4,5])"
    // Nereids does't support array function
    // qt_sql "select arrays_overlap([1,2,3,null], [3,4,5])"
    // Nereids does't support array function
    // qt_sql "select arrays_overlap([true], [false])"
    // Nereids does't support array function
    // qt_sql "select arrays_overlap([], [])"

    // array_binary function
    // Nereids does't support array function
    // qt_sql "select array_union([1,2,3], [2,3,4])"
    // Nereids does't support array function
    // qt_sql "select array_except([1,2,3], [2,3,4])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([1,2,3], [2,3,4])"
    // Nereids does't support array function
    // qt_sql "select array_union([1,2,3], [2,3,4,null])"
    // Nereids does't support array function
    // qt_sql "select array_except([1,2,3], [2,3,4,null])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([1,2,3], [2,3,4,null])"
    // Nereids does't support array function
    // qt_sql "select array_union([true], [false])"
    // Nereids does't support array function
    // qt_sql "select array_except([true, false], [true])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([false, true], [false])"
    // Nereids does't support array function
    // qt_sql "select array_union([], [])"
    // Nereids does't support array function
    // qt_sql "select array_except([], [])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([], [])"
    // Nereids does't support array function
    // qt_sql "select array_union([], [1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_except([], [1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([], [1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_union([null], [1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_except([null], [1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([null], [1,2,3])"
    // Nereids does't support array function
    // qt_sql "select array_union([1], [100000000])"
    // Nereids does't support array function
    // qt_sql "select array_except([1], [100000000])"
    // Nereids does't support array function
    // qt_sql "select array_intersect([1], [100000000])"

    // arrat_slice function
    // Nereids does't support array function
    // qt_sql "select [1,2,3][1:1]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][1:3]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][1:5]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][2:]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][-2:]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][2:-1]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][0:]"
    // Nereids does't support array function
    // qt_sql "select [1,2,3][-5:]"
    // Nereids does't support array function
    // qt_sql "select [true, false, false][2:]"

    // array_join function 
    // Nereids does't support array function
    // qt_sql "select array_join([1, 2, 3], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['1', '2', '3', null], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join([null, '1', '2', '3', null], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['', '2', '3'], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['1', '2', ''], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['1', '2', '', null], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['', '', '3'], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['1', '2', '', ''], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join([null, null, '1', '2', '', '', null], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join([null, null, 1, 2, '', '', null], '_', 'any')"
    // Nereids does't support array function
    // qt_sql "select array_join([''], '_')"
    // Nereids does't support array function
    // qt_sql "select array_join(['', ''], '_')"
    // Nereids does't support array function
    // qt_sql "select array_with_constant(3, '_')"
    // Nereids does't support array function
    // qt_sql "select array_with_constant(2, '1')"
    // Nereids does't support array function
    // qt_sql "select array_with_constant(4, 1223)"
    // Nereids does't support array function
    // qt_sql "select array_with_constant(8, null)"
    // array_compact function
    // Nereids does't support array function
    // qt_sql "select array_compact([1, 2, 3, 3, null, null, 4, 4])"
    // Nereids does't support array function
    // qt_sql "select array_compact([null, null, null])"
    // Nereids does't support array function
    // qt_sql "select array_compact([1.2, 1.2, 3.4, 3.3, 2.1])"
    // Nereids does't support array function
    // qt_sql "select array_compact(['a','b','c','c','d'])"
    // Nereids does't support array function
    // qt_sql "select array_compact(['aaa','aaa','bbb','ccc','ccccc',null, null,'dddd'])"
    // Nereids does't support array function
    // qt_sql "select array_compact(['2015-03-13','2015-03-13'])"

    // Nereids does't support array function
    // qt_sql "select array(8, null)"
    // Nereids does't support array function
    // qt_sql "select array('a', 1, 2)"
    // Nereids does't support array function
    // qt_sql "select array(null, null, null)"
    // abnormal test
    // Nereids does't support array function
    // try {
    //     sql "select array_intersect([1, 2, 3, 1, 2, 3], '1[3, 2, 5]')"
    // } catch (Exception ex) {
    //     assert("${ex}".contains("errCode = 2, detailMessage = No matching function with signature: array_intersect"))
    // }
}
