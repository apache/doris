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

suite("test_bitmap_function", "query") {

    sql """ SET enable_vectorized_engine = TRUE; """

    // BITMAP_AND
    qt_sql """ select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(2))) cnt """
    qt_sql """ select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(1))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_and(to_bitmap(1), to_bitmap(1))) """
    qt_sql """ select bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'))) """
    qt_sql """ select bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'),bitmap_empty())) """
    qt_sql """ select bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'),NULL)) """

    // BITMAP_CONTAINS
    qt_sql """ select bitmap_contains(to_bitmap(1),2) cnt """
    qt_sql """ select bitmap_contains(to_bitmap(1),1) cnt """

    // BITMAP_EMPTY
    qt_sql """ select bitmap_count(bitmap_empty()) """

    // BITMAP_FROM_STRING
    qt_sql """ select bitmap_to_string(bitmap_empty()) """
    qt_sql """ select bitmap_to_string(bitmap_from_string("0, 1, 2")) """
    qt_sql """ select bitmap_from_string("-1, 0, 1, 2") """

    // BITMAP_HAS_ANY
    qt_sql """ select bitmap_has_any(to_bitmap(1),to_bitmap(2)) cnt """
    qt_sql """ select bitmap_has_any(to_bitmap(1),to_bitmap(1)) cnt """

    // BITMAP_HAS_ALL
    qt_sql """ select bitmap_has_all(bitmap_from_string("0, 1, 2"), bitmap_from_string("1, 2")) cnt """
    qt_sql """ select bitmap_has_all(bitmap_empty(), bitmap_from_string("1, 2")) cnt """

    // BITMAP_HASH
    qt_sql """ select bitmap_count(bitmap_hash('hello')) """
    qt_sql """ select bitmap_count(bitmap_hash('')) """
    qt_sql """ select bitmap_count(bitmap_hash(null)) """

    // BITMAP_OR
    qt_sql """ select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(2))) cnt """
    qt_sql """ select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(1))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) """
    qt_sql """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2), to_bitmap(10), to_bitmap(0), NULL)) """
    qt_sql """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2), to_bitmap(10), to_bitmap(0), bitmap_empty())) """
    qt_sql """ select bitmap_to_string(bitmap_or(to_bitmap(10), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'))) """

    // bitmap_and_count
    qt_sql """ select bitmap_and_count(bitmap_from_string('1,2,3'),bitmap_empty()) """
    qt_sql """ select bitmap_and_count(bitmap_from_string('1,2,3'),bitmap_from_string('1,2,3')) """
    qt_sql """ select bitmap_and_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) """
    qt_sql """ select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5')) """
    qt_sql """ select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'),bitmap_empty()) """
    qt_sql """ select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'), NULL) """

    // bitmap_or_count
    qt_sql """ select bitmap_or_count(bitmap_from_string('1,2,3'),bitmap_empty()) """
    qt_sql """ select bitmap_or_count(bitmap_from_string('1,2,3'),bitmap_from_string('1,2,3'))"""
    qt_sql """ select bitmap_or_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) """
    qt_sql """ select bitmap_or_count(bitmap_from_string('1,2,3'), bitmap_from_string('3,4,5'), to_bitmap(100), bitmap_empty()) """
    qt_sql """ select bitmap_or_count(bitmap_from_string('1,2,3'), bitmap_from_string('3,4,5'), to_bitmap(100), NULL) """

    // BITMAP_XOR
    qt_sql """ select bitmap_count(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'))) """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),bitmap_empty())) """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),NULL)) """

    // BITMAP_XOR_COUNT
    qt_sql """ select bitmap_xor_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) """
    qt_sql """ select bitmap_xor_count(bitmap_from_string('1,2,3'),bitmap_from_string('1,2,3')) """
    qt_sql """ select bitmap_xor_count(bitmap_from_string('1,2,3'),bitmap_from_string('4,5,6')) """
    qt_sql """ select (bitmap_xor_count(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'))) """
    qt_sql """ select (bitmap_xor_count(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),bitmap_empty())) """
    qt_sql """ select (bitmap_xor_count(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),NULL)) """

    // BITMAP_NOT
    qt_sql """ select bitmap_count(bitmap_not(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_not(bitmap_from_string('2,3,5'),bitmap_from_string('1,2,3,4'))) """

    // BITMAP_AND_NOT
    qt_sql """ select bitmap_count(bitmap_and_not(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5'))) cnt """

    // BITMAP_AND_NOT_COUNT
    qt_sql """ select bitmap_and_not_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) cnt """

    // BITMAP_SUBSET_IN_RANGE
    qt_sql """ select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 0, 9)) value """
    qt_sql """ select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 2, 3)) value """

    // BITMAP_SUBSET_LIMIT
    qt_sql """ select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 0, 3)) value """
    qt_sql """ select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 4, 3)) value """

    // SUB_BITMAP
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3)) value """
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), -3, 2)) value """
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 2, 100)) value """

    // BITMAP_TO_STRING
    qt_sql """ select bitmap_to_string(null) """
    qt_sql """ select bitmap_to_string(bitmap_empty()) """
    qt_sql """ select bitmap_to_string(to_bitmap(1)) """
    qt_sql """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) """

    // BITMAP_UNION
    def bitmapUnionTable = "test_bitmap_union"
    sql """ DROP TABLE IF EXISTS ${bitmapUnionTable} """
    sql """ create table ${bitmapUnionTable} (page_id int,user_id bitmap bitmap_union) aggregate key (page_id) distributed by hash (page_id) PROPERTIES("replication_num" = "1") """

    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(1)); """
    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(2)); """
    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(3)); """
    sql """ insert into ${bitmapUnionTable} values(2, to_bitmap(1)); """
    sql """ insert into ${bitmapUnionTable} values(2, to_bitmap(2)); """

    qt_sql """ select page_id, bitmap_union(user_id) from ${bitmapUnionTable} group by page_id order by page_id """
    qt_sql """ select page_id, bitmap_count(bitmap_union(user_id)) from ${bitmapUnionTable} group by page_id order by page_id """
    qt_sql """ select page_id, count(distinct user_id) from ${bitmapUnionTable} group by page_id order by page_id """

    sql """ drop table ${bitmapUnionTable} """

    // TO_BITMAP
    qt_sql """ select bitmap_count(to_bitmap(10)) """
    qt_sql """ select bitmap_to_string(to_bitmap(-1)) """

    // BITMAP_MAX
    qt_sql """ select bitmap_max(bitmap_from_string('')) value; """
    qt_sql """ select bitmap_max(bitmap_from_string('1,9999999999')) value """

}
