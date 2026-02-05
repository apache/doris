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

suite("posexplode") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """ DROP TABLE IF EXISTS table_test """
    sql """
        CREATE TABLE IF NOT EXISTS `table_test`(
                   `id` INT NULL,
                   `name` TEXT NULL,
                   `score` array<string> NULL
                 ) ENGINE=OLAP
                 DUPLICATE KEY(`id`)
                 COMMENT 'OLAP'
                 DISTRIBUTED BY HASH(`id`) BUCKETS 1
                 PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // insert values
    sql """ insert into table_test values (0, "zhangsan", ["Chinese","Math","English"]); """
    sql """ insert into table_test values (1, "lisi", ["null"]); """
    sql """ insert into table_test values (2, "wangwu", ["88a","90b","96c"]); """
    sql """ insert into table_test values (3, "lisi2", [null]); """
    sql """ insert into table_test values (4, "amory", NULL); """

    qt_sql """ select * from table_test order by id; """
    order_qt_explode_sql """ select id,name,score, k,v from table_test lateral view posexplode(score) tmp as k,v order by id;"""
    order_qt_explode_outer_sql """ select id,name,score, k,v from table_test lateral view posexplode_outer(score) tmp as k,v order by id; """
    order_qt_explode_sql2 """ select id,name,score, k,v from table_test lateral view posexplode(non_nullable(score)) tmp as k,v lateral view posexplode_outer(score) tmp as k1,v1 where score is not null order by id;"""
    order_qt_explode_outer_sql2 """ select id,name,score, k,v from table_test lateral view posexplode(score) tmp as k1,v1 lateral view posexplode(non_nullable(score)) tmp as k,v where score is not null order by id; """
    order_qt_explode_sql3 """ select id,name,score, k,v from table_test lateral view posexplode_outer(non_nullable(score)) tmp as k,v lateral view posexplode_outer(score) tmp as k1,v1 where score is not null order by id;"""
    order_qt_explode_sql4 """ select id,name,score, k,v from table_test lateral view posexplode(score) tmp as k,v lateral view posexplode_outer(non_nullable(score)) tmp as k1,v1 where score is not null order by id;"""

    // multi lateral view
    order_qt_explode_sql_multi """ select id,name,score, k,v,k1,v1 from table_test lateral view posexplode_outer(score) tmp as k,v lateral view posexplode(score) tmp2 as k1,v1 order by id;"""

    // test with alias
    order_qt_explode_sql_alias """ select id,name,score, tmp.k, tmp.v from table_test lateral view posexplode(score) tmp as k,v order by id;"""
    order_qt_explode_outer_sql_alias """ select id,name,score, tmp.k, tmp.v from table_test lateral view posexplode_outer(score) tmp as k,v order by id; """

    order_qt_explode_sql_alias_multi """ select id,name,score, tmp.k, tmp.v, tmp2.k, tmp2.v from table_test lateral view posexplode_outer(score) tmp as k,v lateral view posexplode(score) tmp2 as k,v order by id;"""

    sql """ DROP TABLE IF EXISTS table_test_not """
    sql """
        CREATE TABLE IF NOT EXISTS `table_test_not`(
                   `id` INT NULL,
                   `name` TEXT NULL,
                   `score` array<string> not NULL
                 ) ENGINE=OLAP
                 DUPLICATE KEY(`id`)
                 COMMENT 'OLAP'
                 DISTRIBUTED BY HASH(`id`) BUCKETS 1
                 PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // insert values
    sql """ insert into table_test_not values (0, "zhangsan", ["Chinese","Math","English"]); """
    sql """ insert into table_test_not values (1, "lisi", ["null"]); """
    sql """ insert into table_test_not values (2, "wangwu", ["88a","90b","96c"]); """
    sql """ insert into table_test_not values (3, "lisi2", [null]); """
    sql """ insert into table_test_not values (4, "liuba", []); """

    qt_sql """ select * from table_test_not order by id; """
    order_qt_explode_sql_not """ select id,name,score, k,v from table_test_not lateral view posexplode(score) tmp as k,v order by id;"""
    order_qt_explode_outer_sql_not """ select id,name,score, k,v from table_test_not lateral view posexplode_outer(score) tmp as k,v order by id; """
    order_qt_explode_sql_alias_multi2 """ select * from table_test_not lateral view posexplode(score) tmp as e1 lateral view posexplode(score) tmp2 as e2 order by id;"""
    sql """ set batch_size = 1; """
    order_qt_explode_sql_alias_multi3 """ select * from table_test_not lateral view posexplode(score) tmp as e1 lateral view posexplode(score) tmp2 as e2 order by id;"""

    sql """ DROP TABLE IF EXISTS test_posexplode_multi_args"""
    sql """
        CREATE TABLE `test_posexplode_multi_args`(
                   `id` INT NULL,
                   `name` TEXT NULL,
                   `tags1` array<string> NULL,
                   `tags2` array<string> NULL
                 )  PROPERTIES ("replication_num" = "1");
    """
    sql """
    insert into test_posexplode_multi_args values
      (1, "jack", ["t1_a","t1_b"], ["t2_a","t2_b","t2_c"]),
      (2, "tom",  ["t1_c"],        ["t2_d","t2_e"]),
      (3, "mary", null,            ["t2_f"]),
      (4, "lily", ["t1_d","t1_e"], null),
      (5, "alice", null, null);
    """

    order_qt_pos_exp_multi_args_all """
    select * from test_posexplode_multi_args;
    """
    order_qt_pos_exp_multi_args0 """
    select id, name, tags1, tags2, k1, k2, k3 from test_posexplode_multi_args
      lateral view posexplode(tags1, tags2) tmp1 as k1, k2, k3 order by 1, 2;
    """
    order_qt_pos_exp_multi_args1 """
    select id, name, tags1, tags2, s1 from test_posexplode_multi_args
      lateral view posexplode(tags1, tags2) tmp1 as s1 order by 1, 2;
    """
    order_qt_pos_exp_multi_args_outer0 """
    select id, name, tags1, tags2, k1, k2, k3 from test_posexplode_multi_args
      lateral view posexplode_outer(tags1, tags2) tmp1 as k1, k2, k3 order by 1, 2;
    """
    order_qt_pos_exp_multi_args_outer1 """
    select id, name, tags1, tags2, s1 from test_posexplode_multi_args
      lateral view posexplode_outer(tags1, tags2) tmp1 as s1 order by 1, 2;
    """

    order_qt_pos_exp_multi_args_multi_lateral0 """
    select id,name,tags1,tags2, k1, k2, k3, k4, k5, k6 from test_posexplode_multi_args
      lateral view posexplode(non_nullable(tags1), non_nullable(tags2)) tmp as k1,k2,k3
      lateral view posexplode_outer(tags1, tags2) tmp as k4,k5,k6
    where tags1 is not null and tags2 is not null order by 1,2;
    """
    order_qt_pos_exp_multi_args_multi_lateral1 """
    select id,name,tags1,tags2, s1, s2 from test_posexplode_multi_args
      lateral view posexplode(non_nullable(tags1), non_nullable(tags2)) tmp as s1
      lateral view posexplode_outer(tags1, tags2) tmp as s2
    where tags1 is not null and tags2 is not null order by 1,2;
    """

    sql """ DROP TABLE IF EXISTS test_posexplode_mixed_nullable"""
    sql """
        CREATE TABLE `test_posexplode_mixed_nullable`(
                   `id` INT NULL,
                   `tags_not_null1` array<string> NOT NULL,
                   `tags_not_null2` array<string> NOT NULL,
                   `tags_null1` array<string> NULL,
                   `tags_null2` array<string> NULL,
                 )  PROPERTIES ("replication_num" = "1");
    """
    sql """
    insert into test_posexplode_mixed_nullable values
      (1, ["t_not_null1_a","t_not_null1_b"],  ["t_not_null2_a","t_not_null2_b"], ["t_null1_a","t_null1_b"],  ["t_null2_a","t_null2_b"]),
      (2, ["t_not_null1_c"],                  ["t_not_null2_c","t_not_null2_d"], ["t_null1_c"],              ["t_null2_c","t_null2_d"]),
      (3, ["t_not_null1_d", "t_not_null1_e"], ["t_not_null2_e"],                 ["t_null1_d", "t_null1_e"], ["t_null2_e"]),
      (4, ["t_not_null1_f"], ["t_not_null2_f"], null,            ["t_null2_f"]),
      (5, ["t_not_null1_g"], ["t_not_null2_g"], ["t_null1_f"], null),
      (6, ["t_not_null1_h"], ["t_not_null2_h"], null, null);
    """
    order_qt_mixed_nullable_all """
    select * from test_posexplode_mixed_nullable;
    """
    order_qt_mixed_nullable_not_null0 """
    select id, st from test_posexplode_mixed_nullable lateral view posexplode(tags_not_null1) tmp as st order by 1, 2;
    """
    order_qt_mixed_nullable_not_null1 """
    select id, pos, tmp0 from test_posexplode_mixed_nullable lateral view posexplode(tags_not_null1) tmp as pos, tmp0 order by 1, 2, 3;
    """
    order_qt_mixed_nullable_not_null2 """
    select id, st from test_posexplode_mixed_nullable lateral view posexplode(tags_not_null1, tags_not_null2) tmp as st order by 1, 2;
    """
    order_qt_mixed_nullable_not_null3 """
    select id, pos, tmp0, tmp1 from test_posexplode_mixed_nullable lateral view posexplode(tags_not_null1, tags_not_null2) tmp as pos, tmp0, tmp1 order by 1, 2, 3, 4;
    """

    order_qt_mixed_nullable_not_null_outer0 """
    select id, st from test_posexplode_mixed_nullable lateral view posexplode_outer(tags_not_null1) tmp as st order by 1, 2;
    """
    order_qt_mixed_nullable_not_null_outer1 """
    select id, pos, tmp0 from test_posexplode_mixed_nullable lateral view posexplode_outer(tags_not_null1) tmp as pos, tmp0 order by 1, 2, 3;
    """
    order_qt_mixed_nullable_not_null_outer2 """
    select id, st from test_posexplode_mixed_nullable lateral view posexplode_outer(tags_not_null1, tags_not_null2) tmp as st order by 1, 2;
    """
    order_qt_mixed_nullable_not_null_outer3 """
    select id, pos, tmp0, tmp1 from test_posexplode_mixed_nullable lateral view posexplode_outer(tags_not_null1, tags_not_null2) tmp as pos, tmp0, tmp1 order by 1, 2, 3, 4;
    """

    order_qt_mixed_nullable2 """
    select id, st from test_posexplode_mixed_nullable lateral view posexplode(tags_not_null1, tags_null1) tmp as st order by 1, 2;
    """
    order_qt_mixed_nullable3 """
    select id, pos, tmp0, tmp1 from test_posexplode_mixed_nullable lateral view posexplode(tags_not_null1, tags_null1) tmp as pos, tmp0, tmp1 order by 1, 2, 3, 4;
    """

    order_qt_mixed_nullable_outer2 """
    select id, st from test_posexplode_mixed_nullable lateral view posexplode_outer(tags_not_null1, tags_null1) tmp as st order by 1, 2;
    """
    order_qt_mixed_nullable_outer3 """
    select id, pos, tmp0, tmp1 from test_posexplode_mixed_nullable lateral view posexplode_outer(tags_not_null1, tags_null1) tmp as pos, tmp0, tmp1 order by 1, 2, 3, 4;
    """

    qt_fix_return_type """
    SELECT 
    *
    FROM (
        SELECT 
            1 AS id,
            ARRAY(1, 2, 3) AS arr_int,
            ARRAY(CAST(10.5 AS DECIMAL(10,2)), CAST(20.0 AS DECIMAL(10,2))) AS arr_decimal
    ) t lateral view posexplode(t.arr_int, t.arr_decimal) t2 AS v order by 1,2,3,4;
    """
}
