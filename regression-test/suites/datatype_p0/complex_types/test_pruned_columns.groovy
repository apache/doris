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

suite("test_pruned_columns") {
    sql """DROP TABLE IF EXISTS `tbl_test_pruned_columns`"""
    sql """
        CREATE TABLE `tbl_test_pruned_columns` (
            `id` int NULL,
            `s` struct<city:text,data:array<map<int,struct<a:int,b:double>>>, value:int> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into `tbl_test_pruned_columns` values
            (1, named_struct('city', 'beijing', 'data', array(map(1, named_struct('a', 10, 'b', 20.0), 2, named_struct('a', 30, 'b', 40))), 'value', 1)),
            (2, named_struct('city', 'shanghai', 'data', array(map(2, named_struct('a', 50, 'b', 40.0), 1, named_struct('a', 70, 'b', 80))), 'value', 2)),
            (3, named_struct('city', 'guangzhou', 'data', array(map(1, named_struct('a', 90, 'b', 60.0), 2, named_struct('a', 110, 'b', 40))), 'value', 3)),
            (4, named_struct('city', 'shenzhen', 'data', array(map(2, named_struct('a', 130, 'b', 20.0), 1, named_struct('a', 150, 'b', 40))), 'value', 4)),
            (5, named_struct('city', 'hangzhou', 'data', array(map(1, named_struct('a', 170, 'b', 80.0), 2, named_struct('a', 190, 'b', 40))), 'value', 5)),
            (6, named_struct('city', 'nanjing', 'data', array(map(2, named_struct('a', 210, 'b', 60.0), 1, named_struct('a', 230, 'b', 40))), 'value', 6)),
            (7, named_struct('city', 'tianjin', 'data', array(map(1, named_struct('a', 250, 'b', 20.0), 2, named_struct('a', 270, 'b', 40))), 'value', 7)),
            (8, named_struct('city', 'chongqing', 'data', array(map(2, named_struct('a', 290, 'b', 80.0), 1, named_struct('a', 310, 'b', 40))), 'value', 8)),
            (9, named_struct('city', 'wuhan', 'data', array(map(1, named_struct('a', 330, 'b', 60.0), 2, named_struct('a', 350, 'b', 40))), 'value', 9)),
            (10, named_struct('city', 'xian', 'data', array(map(2, named_struct('a', 370, 'b', 20.0), 1, named_struct('a', 390, 'b', 40))), 'value', 10)),
            (11, named_struct('city', 'changsha', 'data', array(map(1, named_struct('a', 410, 'b', 80.0), 2, named_struct('a', 430, 'b', 40))), 'value', 11)),
            (12, named_struct('city', 'qingdao', 'data', array(map(2, named_struct('a', 450, 'b', 60.0), 1, named_struct('a', 470, 'b', 40))), 'value', 12)),
            (13, named_struct('city', 'dalian', 'data', array(map(1, named_struct('a', 490, 'b', 20.0), 2, named_struct('a', 510, 'b', 40))), 'value', 13));
    """

    qt_sql """
        select * from `tbl_test_pruned_columns` order by 1;
    """

    qt_sql1 """
        select b.id, array_map(x -> struct_element(map_values(x)[1], 'a'), struct_element(s, 'data')) from `tbl_test_pruned_columns` t join (select 1 id) b on t.id = b.id order by 1;
    """

    qt_sql2 """
        select id, struct_element(s, 'city') from `tbl_test_pruned_columns` order by 1;
    """

    qt_sql3 """
        select id, struct_element(s, 'data') from `tbl_test_pruned_columns` order by 1;
    """

    qt_sql4 """
        select id, struct_element(s, 'data') from `tbl_test_pruned_columns` where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 order by 1;
    """

    qt_sql5 """
        select id, struct_element(s, 'city') from `tbl_test_pruned_columns` where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 order by 1;
    """

    qt_sql5_1 """
        select /*+ set enable_prune_nested_column = 1; */ sum(s.value) from `tbl_test_pruned_columns` where id in(1,2,3,4,8,9,10,11,13);
    """

    qt_sql5_2 """
        select /*+ set enable_prune_nested_column = 0; */ sum(s.value) from `tbl_test_pruned_columns` where id in(1,2,3,4,8,9,10,11,13);
    """

    sql """DROP TABLE IF EXISTS `tbl_test_pruned_columns_map`"""
    sql """
        CREATE TABLE `tbl_test_pruned_columns_map` (
            `id` bigint NULL,
            `dynamic_attributes` map<text,struct<attribute_value:text,confidence_score:double,last_updated:datetime>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into `tbl_test_pruned_columns_map` values
            (1, '{"theme_preference":{"attribute_value":"light", "confidence_score":0.41, "last_updated":"2025-11-03 15:32:33"}, "language_setting":{"attribute_value":"es", "confidence_score":0.65, "last_updated":"2025-11-03 15:32:33"}}'),
            (2, '{"theme_preference":{"attribute_value":"light", "confidence_score":0.99, "last_updated":"2025-11-03 15:32:33"}, "language_setting":{"attribute_value":"es", "confidence_score":0.92, "last_updated":"2025-11-03 15:32:33"}}');
    """

    qt_sql6 """
        select count(struct_element(dynamic_attributes['theme_preference'], 'confidence_score')) from `tbl_test_pruned_columns_map`;
    """

    qt_sql7 """
        select struct_element(dynamic_attributes['theme_preference'], 'confidence_score') from `tbl_test_pruned_columns_map` order by id;
    """

    // test light schema change with nested complex types
    sql """
        DROP TABLE IF EXISTS nested_sc_tbl;
        CREATE TABLE nested_sc_tbl (
            `id` BIGINT,
            `s_info` STRUCT<a:INT, b:VARCHAR(20)>,
            `arr_s` ARRAY<STRUCT<x:INT, y:INT>>,
            `map_s` MAP<VARCHAR, STRUCT<m:INT, n:FLOAT>>
        ) 
        UNIQUE KEY(`id`) 
        DISTRIBUTED BY HASH(`id`) BUCKETS 4 
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true" 
        );
    """
    sql """
        ALTER TABLE nested_sc_tbl  MODIFY COLUMN s_info STRUCT<a:INT, b:VARCHAR(25), c:INT>;
    """
    sql """
        INSERT INTO nested_sc_tbl VALUES (1, struct(10, 'v1_struct', 100), array(struct(100, 200)), map('k1', struct(1, 1.1)));
    """
    sql """
        ALTER TABLE nested_sc_tbl MODIFY COLUMN arr_s ARRAY<STRUCT<x:INT, y:INT, z:VARCHAR(10)>>;
    """
    sql """
        INSERT INTO nested_sc_tbl VALUES (3, struct(30.5, 'v3', 888), array(struct(500, 600, 'added_z'), struct(501, 601, 'added_z_2')), map('k3', struct(3, 3.3)));
    """

    qt_sql8 """
        select struct_element(element_at(arr_s, 1), 'z') as inner_z FROM nested_sc_tbl ORDER BY id;
    """
}