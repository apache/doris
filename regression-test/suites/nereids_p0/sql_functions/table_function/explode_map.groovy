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

suite("explode_map") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """ DROP TABLE IF EXISTS sdu """
    sql """
        CREATE TABLE IF NOT EXISTS `sdu`(
                   `id` INT NULL,
                   `name` TEXT NULL,
                   `score` MAP<TEXT,INT> NULL
                 ) ENGINE=OLAP
                 DUPLICATE KEY(`id`)
                 COMMENT 'OLAP'
                 DISTRIBUTED BY HASH(`id`) BUCKETS 1
                 PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // insert values
    sql """ insert into sdu values (0, "zhangsan", {"Chinese":"80","Math":"60","English":"90"}); """
    sql """ insert into sdu values (1, "lisi", {"null":null}); """
    sql """ insert into sdu values (2, "wangwu", {"Chinese":"88","Math":"90","English":"96"}); """
    sql """ insert into sdu values (3, "lisi2", {null:null}); """
    sql """ insert into sdu values (4, "amory", NULL); """

    qt_sql """ select * from sdu order by id; """
    order_qt_explode_sql """ select name, k,v from sdu lateral view explode_map(score) tmp as k,v order by id;"""
    order_qt_explode_outer_sql """ select name, k,v from sdu lateral view explode_map_outer(score) tmp as k,v order by id; """

    // multi lateral view
    order_qt_explode_sql_multi """ select name, k,v,k1,v1 from sdu lateral view explode_map_outer(score) tmp as k,v lateral view explode_map(score) tmp2 as k1,v1 order by id;"""

    // test with alias
    order_qt_explode_sql_alias """ select name, tmp.k, tmp.v from sdu lateral view explode_map(score) tmp as k,v order by id;"""
    order_qt_explode_outer_sql_alias """ select name, tmp.k, tmp.v from sdu lateral view explode_map_outer(score) tmp as k,v order by id; """

    order_qt_explode_sql_alias_multi """ select name, tmp.k, tmp.v, tmp2.k, tmp2.v from sdu lateral view explode_map_outer(score) tmp as k,v lateral view explode_map(score) tmp2 as k,v order by id;"""
}
