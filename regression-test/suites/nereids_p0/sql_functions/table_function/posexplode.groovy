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

}
