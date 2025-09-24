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
            `s` struct<city:text,data:array<map<int,struct<a:int,b:double>>>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into `tbl_test_pruned_columns` values
            (1, named_struct('city', 'beijing', 'data', array(map(1, named_struct('a', 10, 'b', 20.0), 2, named_struct('a', 30, 'b', 40))))),
            (2, named_struct('city', 'shanghai', 'data', array(map(2, named_struct('a', 50, 'b', 40.0), 1, named_struct('a', 70, 'b', 80)))));
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
}