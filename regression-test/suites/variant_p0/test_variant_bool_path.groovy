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

// A Variant path that holds only JSON booleans keeps them as booleans: they read back as
// true/false, and equality and grouping see two values. That must hold however the rows reach
// storage - in one load, one load per row, spread over several tablets - and must survive a
// full compaction.
suite("test_variant_bool_path", "p0") {
    sql "drop table if exists test_variant_bool_path"
    sql """
        create table test_variant_bool_path (
            id int,
            var variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """
    sql """
        insert into test_variant_bool_path (id, var) values
            (1, parse_to_variant('{"k": true}')),
            (2, parse_to_variant('{"k": false}')),
            (3, parse_to_variant('{"k": true}')),
            (4, parse_to_variant('{"k": false}')),
            (5, parse_to_variant('{"other": 1}'))
    """
    qt_sql_values """
        select id, var['k'] from test_variant_bool_path where var['k'] is not null order by id
    """
    qt_sql_group """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_path where var['k'] is not null
        group by var['k'] order by first_id
    """
    qt_sql_equality """
        select id from test_variant_bool_path where var['k'] = parse_to_variant('true') order by id
    """
    qt_sql_variant_group """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_path group by var order by first_id
    """

    // One load per row, so every rowset carries the path alone, then a full compaction merges them.
    sql "drop table if exists test_variant_bool_path_rowsets"
    sql """
        create table test_variant_bool_path_rowsets (
            id int,
            var variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """insert into test_variant_bool_path_rowsets values (1, parse_to_variant('{"k": true}'))"""
    sql """insert into test_variant_bool_path_rowsets values (2, parse_to_variant('{"k": false}'))"""
    sql """insert into test_variant_bool_path_rowsets values (3, parse_to_variant('{"k": true}'))"""
    sql """insert into test_variant_bool_path_rowsets values (4, parse_to_variant('{"k": false}'))"""
    qt_sql_rowsets_values_before_compaction """
        select id, var['k'] from test_variant_bool_path_rowsets order by id
    """
    qt_sql_rowsets_group_before_compaction """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_path_rowsets group by var['k'] order by first_id
    """
    trigger_and_wait_compaction("test_variant_bool_path_rowsets", "full")
    qt_sql_rowsets_values_after_compaction """
        select id, var['k'] from test_variant_bool_path_rowsets order by id
    """
    qt_sql_rowsets_group_after_compaction """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_path_rowsets group by var['k'] order by first_id
    """

    // One load spread over several tablets.
    sql "drop table if exists test_variant_bool_path_buckets"
    sql """
        create table test_variant_bool_path_buckets (
            id int,
            var variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1")
    """
    sql """
        insert into test_variant_bool_path_buckets values
            (1, parse_to_variant('{"k": true}')), (2, parse_to_variant('{"k": false}')),
            (3, parse_to_variant('{"k": true}')), (4, parse_to_variant('{"k": false}')),
            (5, parse_to_variant('{"k": true}')), (6, parse_to_variant('{"k": false}')),
            (7, parse_to_variant('{"k": true}')), (8, parse_to_variant('{"k": false}'))
    """
    qt_sql_buckets_values """
        select id, var['k'] from test_variant_bool_path_buckets order by id
    """
    qt_sql_buckets_group """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_path_buckets group by var['k'] order by first_id
    """
}
