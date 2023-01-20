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

suite("test_bitmap_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
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
    qt_sql_bitmap_hash1 """ select bitmap_count(bitmap_hash('hello')) """
    qt_sql_bitmap_hash2  """ select bitmap_count(bitmap_hash('')) """
    qt_sql_bitmap_hash3  """ select bitmap_count(bitmap_hash(null)) """

    // BITMAP_HASH64
    qt_sql_bitmap_hash64_1 """ select bitmap_count(bitmap_hash64('hello')) """
    qt_sql_bitmap_hash64_2  """ select bitmap_count(bitmap_hash64('')) """
    qt_sql_bitmap_hash64_3  """ select bitmap_count(bitmap_hash64(null)) """

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
    sql """ create table if not exists ${bitmapUnionTable} (page_id int,user_id bitmap bitmap_union) aggregate key (page_id) distributed by hash (page_id) PROPERTIES("replication_num" = "1") """

    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(1)); """
    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(2)); """
    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(3)); """
    sql """ insert into ${bitmapUnionTable} values(2, to_bitmap(1)); """
    sql """ insert into ${bitmapUnionTable} values(2, to_bitmap(2)); """

    qt_sql """ select page_id, bitmap_union(user_id) from ${bitmapUnionTable} group by page_id order by page_id """
    qt_sql """ select page_id, bitmap_count(bitmap_union(user_id)) from ${bitmapUnionTable} group by page_id order by page_id """
    qt_sql """ select page_id, count(distinct user_id) from ${bitmapUnionTable} group by page_id order by page_id """

    sql """ drop table ${bitmapUnionTable} """

    // BITMAP_XOR
    qt_sql """ select bitmap_count(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) cnt; """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))); """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'))); """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),bitmap_empty())); """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),NULL)); """

    // TO_BITMAP
    qt_sql """ select bitmap_count(to_bitmap(10)) """
    qt_sql """ select bitmap_to_string(to_bitmap(-1)) """

    // BITMAP_MAX
    qt_sql """ select bitmap_max(bitmap_from_string('')) value; """
    qt_sql """ select bitmap_max(bitmap_from_string('1,9999999999')) value """

    // INTERSECT_COUNT
    def intersectCountTable = "test_intersect_count"
    sql """ DROP TABLE IF EXISTS ${intersectCountTable} """
    sql """ create table if not exists ${intersectCountTable} (dt int (11),page varchar (10),user_id bitmap BITMAP_UNION ) DISTRIBUTED BY HASH(dt) BUCKETS 2 PROPERTIES("replication_num" = "1") """


    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(1)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(2)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(3)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(4)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(5)); """
    sql """ insert into ${intersectCountTable} values(4,"110001", to_bitmap(1)); """
    sql """ insert into ${intersectCountTable} values(4,"110001", to_bitmap(2)); """
    sql """ insert into ${intersectCountTable} values(4,"110001", to_bitmap(3)); """

    qt_sql """ select dt,bitmap_to_string(user_id) from ${intersectCountTable} where dt in (3,4) order by dt desc; """
    qt_sql """ select intersect_count(user_id,dt,3,4) from ${intersectCountTable}; """

    // ARTHOGONAL_BITMAP_****
    def arthogonalBitmapTable = "test_arthogonal_bitmap"
    sql """ DROP TABLE IF EXISTS ${arthogonalBitmapTable} """
    sql """ CREATE TABLE IF NOT EXISTS ${arthogonalBitmapTable} ( tag_group bigint(20) NULL COMMENT "标签组", tag_value_id varchar(64) NULL COMMENT "标签值", tag_range int(11) NOT NULL DEFAULT "0" COMMENT "", partition_sign varchar(32) NOT NULL COMMENT "分区标识", bucket int(11) NOT NULL COMMENT "分桶字段", confidence tinyint(4) NULL DEFAULT "100" COMMENT "置信度", members bitmap BITMAP_UNION NULL COMMENT "人群") ENGINE=OLAP AGGREGATE KEY(tag_group, tag_value_id, tag_range, partition_sign, bucket, confidence) COMMENT "dmp_tag_map" PARTITION BY LIST(partition_sign) (PARTITION p202203231 VALUES IN ("2022-03-23-1"), PARTITION p202203251 VALUES IN ("2022-03-25-1"), PARTITION p202203261 VALUES IN ("2022-03-26-1"), PARTITION p202203271 VALUES IN ("2022-03-27-1"), PARTITION p202203281 VALUES IN ("2022-03-28-1"), PARTITION p202203291 VALUES IN ("2022-03-29-1"), PARTITION p202203301 VALUES IN ("2022-03-30-1"), PARTITION p202203311 VALUES IN ("2022-03-31-1"), PARTITION p202204011 VALUES IN ("2022-04-01-1"), PARTITION crowd VALUES IN ("crowd"), PARTITION crowd_tmp VALUES IN ("crowd_tmp"), PARTITION extend_crowd VALUES IN ("extend_crowd"), PARTITION partition_sign VALUES IN ("online_crowd")) DISTRIBUTED BY HASH(bucket) BUCKETS 64 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2");"""

    qt_sql """ select orthogonal_bitmap_intersect(members, tag_group, 1150000, 1150001, 390006) from ${arthogonalBitmapTable} where  tag_group in ( 1150000, 1150001, 390006); """
    qt_sql """ select orthogonal_bitmap_intersect_count(members, tag_group, 1150000, 1150001, 390006) from ${arthogonalBitmapTable} where  tag_group in ( 1150000, 1150001, 390006); """
    qt_sql """ select orthogonal_bitmap_union_count(members) from ${arthogonalBitmapTable} where  tag_group in ( 1150000, 1150001, 390006);  """

    // Nereids does't support array function
    // qt_sql """ select bitmap_to_array(user_id) from ${intersectCountTable} order by dt desc; """
    // Nereids does't support array function
    // qt_sql """ select bitmap_to_array(bitmap_empty()); """
    // Nereids does't support array function
    // qt_sql """ select bitmap_to_array(bitmap_from_string('100,200,3,4')); """

    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,2,3,4,5'), 0, 3)) value; """
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1'), 0, 3)) value;  """
    qt_sql """ select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('100'), 0, 3)) value;  """
    qt_sql """ select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('20221103'), 0, 20221104)) date_list_bitmap;  """

    sql "drop table if exists d_table;"
    sql """
        create table d_table (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql "insert into d_table select -4,-4,-4,'d';"
    try_sql "select bitmap_union(to_bitmap_with_check(k2)) from d_table;"
    qt_sql "select bitmap_union(to_bitmap(k2)) from d_table;"
}
