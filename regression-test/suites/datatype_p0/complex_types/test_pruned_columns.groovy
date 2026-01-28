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
    sql "set batch_size = 32;"
    sql """DROP TABLE IF EXISTS `tbl_test_pruned_columns`"""
    sql """
        CREATE TABLE `tbl_test_pruned_columns` (
            `id` int NULL,
            `s` struct<city:text,data:array<map<int,struct<a:int,b:double>>>, value:int> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into `tbl_test_pruned_columns`
        select 
            number as id,
            named_struct(
                'city', 
                case (number % 10)
                    when 0 then 'beijing'
                    when 1 then 'shanghai'
                    when 2 then 'shenzhen'
                    when 3 then 'guangzhou'
                    when 4 then 'hangzhou'
                    when 5 then 'chengdu'
                    when 6 then 'wuhan'
                    when 7 then 'xian'
                    when 8 then 'nanjing'
                    else null
                end,
                'data', 
                array(
                    map(
                        1, named_struct('a', number * 10, 'b', (number * 10 + number % 5) * 1.0),
                        2, named_struct('a', number * 10 + 20, 'b', (number % 10 + 1) * 10.0)
                    ),
                    map(
                        (number % 3 + 1), named_struct('a', number * 5, 'b', number * 2.5),
                        (number % 5 + 2), named_struct('a', number * 3, 'b', number * 1.5)
                    )
                ),
                'value',
                number
            ) as s
        from numbers("number" = "3000");
    """

    qt_sql """
        select struct_element(s, 'city'), count() from `tbl_test_pruned_columns` group by struct_element(s, 'city') order by 1, 2;
    """

    qt_sql1 """
        select
            b.id
            , array_map(x -> struct_element(map_values(x)[1], 'a')
            , struct_element(s, 'data'))
        from `tbl_test_pruned_columns` t join (select 1 id) b on t.id = b.id
        order by 1, 2 limit 0, 20;
    """

    qt_sql1_1 """
        select
            b.id
            , array_map(x -> struct_element(map_values(x)[1], 'a')
            , struct_element(s, 'data'))
        from `tbl_test_pruned_columns` t join (select 1 id) b on t.id = b.id
        order by 1, 2 limit 100, 20;
    """

    qt_sql1_2 """
        select
            b.id
            , array_map(x -> struct_element(map_values(x)[1], 'a')
            , struct_element(s, 'data'))
        from `tbl_test_pruned_columns` t join (select 1 id) b on t.id = b.id
        order by 1 desc, 2 limit 100, 20;
    """

    qt_sql2 """
        select id, struct_element(s, 'city') from `tbl_test_pruned_columns` order by 1 limit 0, 20;
    """

    qt_sql2_1 """
        select id, struct_element(s, 'city') from `tbl_test_pruned_columns` order by 1 limit 100, 20;
    """

    qt_sql2_2 """
        select id, struct_element(s, 'city') from `tbl_test_pruned_columns` order by 1 desc limit 0, 20;
    """

    qt_sql3 """
        select id, struct_element(s, 'data') from `tbl_test_pruned_columns` order by 1 limit 0, 20;
    """

    qt_sql3_1 """
        select id, struct_element(s, 'data') from `tbl_test_pruned_columns` order by 1 limit 200, 20;
    """

    qt_sql3_2 """
        select id, struct_element(s, 'data') from `tbl_test_pruned_columns` order by 1 desc limit 0, 20;
    """

    qt_sql4 """
        select
            id
            , struct_element(s, 'data')
        from `tbl_test_pruned_columns`
        where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 
        order by 1 limit 0, 20;
    """

    qt_sql4_1 """
        select
            id
            , struct_element(s, 'data')
        from `tbl_test_pruned_columns`
        where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 
        order by 1 limit 100, 20;
    """

    qt_sql4_2 """
        select
            id
            , struct_element(s, 'data')
        from `tbl_test_pruned_columns`
        where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 
        order by 1 desc limit 0, 20;
    """

    qt_sql5 """
        select
            id
            , struct_element(s, 'city')
        from `tbl_test_pruned_columns`
        where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 
        order by 1, 2 limit 0, 20;
    """

    qt_sql5_1 """
        select
            id
            , struct_element(s, 'city')
        from `tbl_test_pruned_columns`
        where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 
        order by 1, 2 limit 100, 20;
    """

    qt_sql5_2 """
        select
            id
            , struct_element(s, 'city')
        from `tbl_test_pruned_columns`
        where struct_element(struct_element(s, 'data')[1][2], 'b') = 40 
        order by 1 desc, 2 limit 0, 20;
    """

    qt_sql5_3 """
        select /*+ set enable_prune_nested_column = 1; */ sum(s.value) from `tbl_test_pruned_columns` where id in(1,2,3,4,8,9,10,11,13);
    """

    qt_sql5_4 """
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
        select
            id
            , struct_element(struct_element(s, 'data')[2][3], 'b')
        from `tbl_test_pruned_columns`
        where struct_element(s, 'city') = 'chengdu'
        order by 1, 2 limit 0, 20;
    """

    qt_sql6_1 """
        select
            id
            , struct_element(struct_element(s, 'data')[2][3], 'b')
        from `tbl_test_pruned_columns`
        where struct_element(s, 'city') = 'chengdu'
        order by 1, 2 limit 100, 20;
    """

    qt_sql6_2 """
        select
            id
            , struct_element(struct_element(s, 'data')[2][3], 'b')
        from `tbl_test_pruned_columns`
        where struct_element(s, 'city') = 'chengdu'
        order by 1 desc, 2 limit 0, 20;
    """

    qt_sql7 """
        select count(struct_element(dynamic_attributes['theme_preference'], 'confidence_score')) from `tbl_test_pruned_columns_map`;
    """

    qt_sql8 """
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

    qt_sql9 """
        select struct_element(element_at(arr_s, 1), 'z') as inner_z FROM nested_sc_tbl ORDER BY id;
    """
}