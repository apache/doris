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

// A Variant path that holds only JSON booleans keeps them as booleans. A path that mixes booleans
// with plain numbers has conflicting types, and the storage layer resolves that conflict by giving
// the path one numeric type: the booleans are then stored and read back as 1/0. This suite pins
// that accepted behaviour so it cannot change silently - it is deliberately not asserting that
// true/false survive a type conflict.
//
// This is a storage limitation, tracked separately from the relational operators, and not a
// contract: the recorded values say what the storage layer does today, not what Variant equality
// promises. The conflict is resolved per segment, so the same value can read back differently
// depending on how the rows were batched, which tablet they hashed to, and whether compaction has
// merged them yet. The second half of the suite records exactly those differences.
suite("test_variant_bool_numeric_mix", "p0") {
    sql "drop table if exists test_variant_bool_numeric_mix"

    sql """
        create table test_variant_bool_numeric_mix (
            id int,
            var variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """

    // "pure": only booleans, so the path keeps a boolean type.
    // "fwd": a boolean arrives before the numbers.
    // "rev": a number arrives before the boolean.
    sql """
        insert into test_variant_bool_numeric_mix (id, var) values
            (1, parse_to_variant('{"pure": true}')),
            (2, parse_to_variant('{"pure": false}')),
            (3, parse_to_variant('{"fwd": true}')),
            (4, parse_to_variant('{"fwd": 1}')),
            (5, parse_to_variant('{"fwd": 0}')),
            (6, parse_to_variant('{"rev": 1}')),
            (7, parse_to_variant('{"rev": 0}')),
            (8, parse_to_variant('{"rev": true}'))
    """

    // A boolean-only path reads back as true/false.
    qt_sql_pure_values """
        select id, var['pure']
        from test_variant_bool_numeric_mix
        where var['pure'] is not null
        order by id
    """

    // Mixed paths: whatever the conflict resolution produces is what these record.
    qt_sql_fwd_values """
        select id, var['fwd']
        from test_variant_bool_numeric_mix
        where var['fwd'] is not null
        order by id
    """
    qt_sql_rev_values """
        select id, var['rev']
        from test_variant_bool_numeric_mix
        where var['rev'] is not null
        order by id
    """

    // Grouping follows the stored values, so a boolean that degraded to 1/0 groups with the
    // numbers. min(id) stands in for the group key so the result is deterministic.
    qt_sql_fwd_group """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_numeric_mix
        where var['fwd'] is not null
        group by var['fwd']
        order by first_id
    """
    qt_sql_rev_group """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_numeric_mix
        where var['rev'] is not null
        group by var['rev']
        order by first_id
    """

    // One load per row: every rowset sees a single type, so nothing conflicts until compaction
    // merges the rowsets and resolves the path type across them.
    sql "drop table if exists test_variant_bool_numeric_mix_rowsets"
    sql """
        create table test_variant_bool_numeric_mix_rowsets (
            id int,
            var variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """insert into test_variant_bool_numeric_mix_rowsets values (1, parse_to_variant('{"k": true}'))"""
    sql """insert into test_variant_bool_numeric_mix_rowsets values (2, parse_to_variant('{"k": 1}'))"""
    sql """insert into test_variant_bool_numeric_mix_rowsets values (3, parse_to_variant('{"k": false}'))"""
    sql """insert into test_variant_bool_numeric_mix_rowsets values (4, parse_to_variant('{"k": 0}'))"""
    qt_sql_rowsets_values_before_compaction """
        select id, var['k'] from test_variant_bool_numeric_mix_rowsets order by id
    """
    qt_sql_rowsets_group_before_compaction """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_numeric_mix_rowsets group by var['k'] order by first_id
    """
    trigger_and_wait_compaction("test_variant_bool_numeric_mix_rowsets", "full")
    qt_sql_rowsets_values_after_compaction """
        select id, var['k'] from test_variant_bool_numeric_mix_rowsets order by id
    """
    qt_sql_rowsets_group_after_compaction """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_numeric_mix_rowsets group by var['k'] order by first_id
    """

    // One load spread over several tablets: each tablet resolves the conflict among the rows that
    // hashed to it, so a boolean only degrades where a number shares its tablet. hash(id) places
    // the ids as {6}, {1, 7}, {3, 5} and {2, 4, 8}: ids 3 and 5 are a boolean-only tablet, while
    // the booleans 1 and 8 share a tablet with numbers - 1 ahead of its number like "fwd", 8 behind
    // its numbers like "rev".
    sql "drop table if exists test_variant_bool_numeric_mix_buckets"
    sql """
        create table test_variant_bool_numeric_mix_buckets (
            id int,
            var variant
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """
        insert into test_variant_bool_numeric_mix_buckets values
            (1, parse_to_variant('{"k": true}')), (2, parse_to_variant('{"k": 1}')),
            (3, parse_to_variant('{"k": false}')), (4, parse_to_variant('{"k": 0}')),
            (5, parse_to_variant('{"k": true}')), (6, parse_to_variant('{"k": 0}')),
            (7, parse_to_variant('{"k": 1}')), (8, parse_to_variant('{"k": true}'))
    """
    qt_sql_buckets_values """
        select id, var['k'] from test_variant_bool_numeric_mix_buckets order by id
    """
    qt_sql_buckets_group """
        select count(*) as cnt, min(id) as first_id
        from test_variant_bool_numeric_mix_buckets group by var['k'] order by first_id
    """
}
