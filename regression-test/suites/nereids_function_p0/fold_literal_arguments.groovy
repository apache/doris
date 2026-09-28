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

// Arguments that must be literals are folded during analysis,
// so a foldable constant expression is accepted like the literal it evaluates to.
suite("fold_literal_arguments") {
    sql "drop table if exists fold_literal_arguments_t"
    sql """
        create table fold_literal_arguments_t (k int, v double, s varchar(64), dt datetime)
        duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')
    """
    sql """
        insert into fold_literal_arguments_t values
        (1, 1.5, 'a,b,c', '2024-03-15 10:00:00'), (2, 2.5, 'abc', '2024-04-15 10:00:00'),
        (3, 3.5, 'ab', '2024-05-15 10:00:00'), (4, 4.5, 'x', '2024-06-01 00:00:00')
    """

    // scalar functions
    qt_sha2 "select sha2('abc', 200 + 56)"
    qt_split_by_regexp "select split_by_regexp('a,b,c', ',', 1 + 1)"
    qt_regexp_replace "select regexp_replace('abc', 'a', 'b', concat('', ''))"
    qt_regexp_replace_one "select regexp_replace_one('abc', 'a', 'b', concat('', ''))"
    qt_tokenize """select tokenize('hello world', concat('"parser"="', 'english"'))"""
    qt_rand "select rand(1 + 1) = rand(2), random(1, 5 + 5) between 1 and 10"
    order_qt_uniform "select number, uniform(1, 5 + 5, number) between 1 and 10 from numbers('number' = '3')"
    order_qt_width_bucket "select k, width_bucket(v, 0, 10, 2 + 3) from fold_literal_arguments_t"
    qt_array_apply "select array_apply([1, 2, 3], concat('>', '='), 2)"
    order_qt_date_trunc "select k, date_trunc(dt, concat('mon', 'th')), date_trunc(concat('ye', 'ar'), dt) from fold_literal_arguments_t"
    qt_now """select length(cast(now(1 + 2) as string)), length(cast(utc_timestamp(1 + 2) as string)),
            length(cast(utc_time(1 + 2) as string))"""

    // a string date value is not folded when the time unit is already a literal,
    // so the derived return type does not change
    sql "drop table if exists fold_literal_arguments_ctas"
    sql """
        create table fold_literal_arguments_ctas properties('replication_num' = '1') as
        select 1 k, date_trunc(concat('2024-03-15', ' 10:00:00'), 'month') c1,
            date_trunc(concat('2024-03-15', ' 10:00:00.123'), 'month') c2
    """
    qt_date_trunc_string_value_type "desc fold_literal_arguments_ctas"
    // the time unit next to a typed date literal is folded; when both arguments are foldable strings, only the
    // time unit is folded, so the derived return type is the same as with a literal time unit
    qt_date_trunc_typed_date """select date_trunc(DATE '2024-03-15', concat('mon', 'th')),
            date_trunc(concat('ye', 'ar'), TIMESTAMP '2024-03-15 10:00:00')"""
    sql "drop table if exists fold_literal_arguments_ctas_zoned"
    sql """
        create table fold_literal_arguments_ctas_zoned properties('replication_num' = '1') as
        select 1 k, date_trunc(concat('2024-01-01 01:02:03+08:00', ''), 'month') c1,
            date_trunc(concat('2024-01-01 01:02:03+08:00', ''), concat('mon', 'th')) c2,
            date_trunc(concat('mon', 'th'), concat('2024-01-01 01:02:03+08:00', '')) c3
    """
    qt_date_trunc_both_foldable_type "desc fold_literal_arguments_ctas_zoned"

    // aggregate functions
    qt_sequence_match "select sequence_match(concat('(?1)', '(?2)'), dt, k = 1, k = 2) from fold_literal_arguments_t"
    qt_sequence_count "select sequence_count(concat('(?1)', '(?2)'), dt, k = 1, k = 2) from fold_literal_arguments_t"
    qt_orthogonal_bitmap_expr_calculate """select bitmap_to_string(orthogonal_bitmap_expr_calculate(
            to_bitmap(k), cast(k as varchar), concat('1', '|2'))) from fold_literal_arguments_t"""
    qt_orthogonal_bitmap_expr_calculate_count """select orthogonal_bitmap_expr_calculate_count(
            to_bitmap(k), cast(k as varchar), concat('1', '|2')) from fold_literal_arguments_t"""
    // a STRING formula is accepted before the type coercion casts it to VARCHAR
    qt_orthogonal_bitmap_expr_calculate_string """select bitmap_to_string(orthogonal_bitmap_expr_calculate(
            to_bitmap(k), cast(k as varchar), concat(cast('1' as string), cast('|2' as string))))
            from fold_literal_arguments_t"""
    qt_orthogonal_bitmap_expr_calculate_count_string """select orthogonal_bitmap_expr_calculate_count(
            to_bitmap(k), cast(k as varchar), concat(cast('1' as string), cast('|2' as string)))
            from fold_literal_arguments_t"""
    qt_topn """select topn(s, 1 + 1) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""

    // INSERT ... VALUES does not run the rewrite phase
    sql "drop table if exists fold_literal_arguments_insert"
    sql """
        create table fold_literal_arguments_insert (k int, s string)
        duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')
    """
    sql """insert into fold_literal_arguments_insert values
            (1, sha2('abc', 200 + 56)), (2, regexp_replace('abc', 'a', 'b', concat('', '')))"""
    order_qt_insert_values "select * from fold_literal_arguments_insert"

    // the folded value is still validated
    test {
        sql "select sha2('abc', 200 + 100)"
        exception "sha2 functions only support digest length of"
    }
    test {
        sql "select split_by_regexp('a,b,c', ',', 0 - 1)"
        exception "must be a positive constant"
    }
    test {
        sql "select array_apply([1, 2, 3], concat('>', '>'), 2)"
        exception "op support =, >=, <=, >, <, !="
    }
    test {
        sql "select now(3 + 7)"
        exception "Precision of NOW must be between 0 and"
    }

    // a non-constant argument is still rejected
    test {
        sql "select sha2(s, k) from fold_literal_arguments_t"
        exception "the second parameter of sha2 must be a literal"
    }
    test {
        // a constant expression FE cannot fold (crc32 has no FE executor)
        sql "select sha2('abc', 256 + crc32(''))"
        exception "the second parameter of sha2 must be a literal"
    }
    test {
        sql "select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar), s) from fold_literal_arguments_t"
        exception "must be a string literal"
    }

    // without constant folding, the arguments are still literals when they reach the rewrite checks
    sql "set debug_skip_fold_constant = true"
    qt_topn_skip_fold """select topn(s, 1 + 1) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""
    qt_topn_array_skip_fold """select topn_array(s, 1 + 1) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""
    qt_topn_weighted_skip_fold "select topn_weighted(s, k, 1 + 1) from fold_literal_arguments_t"
    qt_sha2_skip_fold "select sha2('abc', 200 + 56)"
    qt_array_apply_skip_fold "select array_apply([1, 2, 3], concat('>', '='), 2)"
    order_qt_date_trunc_skip_fold "select k, date_trunc(dt, concat('mon', 'th')) from fold_literal_arguments_t"
}
