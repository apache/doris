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

// A constant expression is accepted for an argument that a function requires to be constant.
// FE folds the argument before type coercion only when the signature depends on its value (the precision of now,
// the time unit of the string forms of date_trunc). The value of any other argument FE can evaluate is validated
// like the literal it evaluates to, and a constant expression FE cannot fold is evaluated and validated by BE.
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
    // when both arguments are foldable strings, only the time unit is folded before type coercion, so the derived
    // return type is the same as with a literal time unit
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
    // a formula that is not a string is cast to VARCHAR
    qt_orthogonal_bitmap_expr_calculate_numeric """select bitmap_to_string(orthogonal_bitmap_expr_calculate(
            to_bitmap(k), cast(k as varchar), 1 + 1)) from fold_literal_arguments_t"""
    qt_topn """select topn(s, 1 + 1) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""

    // INSERT ... VALUES does not run the rewrite phase, so the arguments are not folded on FE
    sql "drop table if exists fold_literal_arguments_insert"
    sql """
        create table fold_literal_arguments_insert (k int, s string)
        duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')
    """
    sql """insert into fold_literal_arguments_insert values
            (1, sha2('abc', 200 + 56)), (2, regexp_replace('abc', 'a', 'b', concat('', ''))),
            (3, date_trunc(DATE '2024-03-15', cast(null as varchar))),
            (4, date_trunc(cast(null as varchar), DATE '2024-03-15')),
            (5, regexp_replace('abc', '[', 'x', cast(null as string))),
            (6, regexp_replace_one('abc', '[', 'x', cast(null as string)))"""
    order_qt_insert_values "select * from fold_literal_arguments_insert"

    // the value FE can evaluate is validated like the literal
    test {
        sql "select sha2('abc', 200 + 100)"
        exception "sha2 functions only support digest length of"
    }
    test {
        sql "select sha2('abc', cast(null as int))"
        exception "sha2 functions only support digest length of"
    }
    test {
        sql "select split_by_regexp('a,b,c', ',', 0 - 1)"
        exception "must be a positive constant"
    }
    test {
        // a typed NULL is an integral constant, but not a positive one
        sql "select split_by_regexp('a,b,c', ',', cast(null as int))"
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
    test {
        sql "select date_trunc(dt, concat('mon', 'x')) from fold_literal_arguments_t"
        exception "date_trunc function time unit param only support argument is"
    }
    // constant folding removes these functions, so the value is validated before it
    test {
        sql "select sha2(null, 200 + 100)"
        exception "sha2 functions only support digest length of"
    }
    test {
        sql "select tokenize(null, concat('par', 'ser'))"
        exception "tokenize second argument must be properties format"
    }
    // an aggregate function is validated once the rewrite has folded its arguments
    test {
        sql "select topn(s, 1 - 1) from fold_literal_arguments_t"
        exception "must be a constant positive integer"
    }
    test {
        sql "select sequence_match(concat('(?1)', '(?9)'), dt, k = 1, k = 2) from fold_literal_arguments_t"
        exception "Event number 9 is out of range"
    }
    // a typed NULL pattern folds to a NULL literal, which must be rejected explicitly: BE's nullable aggregate
    // wrapper would otherwise skip every row for a NULL pattern instead of raising an error
    test {
        sql "select sequence_match(cast(null as string), dt, k = 1, k = 2) from fold_literal_arguments_t"
        exception "must be string constant, but it is null"
    }
    test {
        sql "select sequence_count(cast(null as string), dt, k = 1, k = 2) from fold_literal_arguments_t"
        exception "must be string constant, but it is null"
    }
    test {
        sql """select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar),
                cast(null as varchar)) from fold_literal_arguments_t"""
        exception "must not be null"
    }
    test {
        sql """select orthogonal_bitmap_expr_calculate_count(to_bitmap(k), cast(k as varchar),
                cast(null as varchar)) from fold_literal_arguments_t"""
        exception "must not be null"
    }
    // FE cannot fold crc32, so the NULL formula reaches BE's nullable aggregate wrapper.
    test {
        sql """select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar),
                if(crc32('') = 0, cast(null as varchar), '1')) from fold_literal_arguments_t"""
        exception "must not be null"
    }
    test {
        sql """select orthogonal_bitmap_expr_calculate_count(to_bitmap(k), cast(k as varchar),
                if(crc32('') = 0, cast(null as varchar), '1')) from fold_literal_arguments_t"""
        exception "must not be null"
    }

    // a non-constant argument is still rejected
    test {
        sql "select sha2(s, k) from fold_literal_arguments_t"
        exception "the second parameter of sha2 must be a constant"
    }
    test {
        sql "select orthogonal_bitmap_expr_calculate(to_bitmap(k), cast(k as varchar), s) from fold_literal_arguments_t"
        exception "must be a string constant"
    }

    // a constant of another type is rejected before the type coercion casts it, like the literal
    test {
        sql "select sha2('abc', 255.5 + 0.5)"
        exception "the second parameter of sha2 must be an integer"
    }
    test {
        sql "select sha2('abc', 256.0 + crc32(''))"
        exception "the second parameter of sha2 must be an integer"
    }
    test {
        sql "select split_by_regexp('a,b,c', ',', 2.0 + crc32(''))"
        exception "must be a positive constant"
    }

    // a constant expression FE cannot fold is evaluated by BE (crc32 and lpad have no FE executor, crc32('') is 0)
    qt_sha2_be "select sha2('abc', 256 + crc32(''))"
    qt_split_by_regexp_be "select split_by_regexp('a,b,c', ',', 2 + crc32(''))"
    qt_regexp_replace_be "select regexp_replace('abc', 'a', 'b', lpad('', 0, ' '))"
    qt_regexp_replace_one_be "select regexp_replace_one('abc', 'a', 'b', lpad('', 0, ' '))"
    qt_tokenize_be """select tokenize('hello world', lpad('"parser"="english"', 18, ' '))"""
    qt_rand_be "select rand(1 + crc32('')) = rand(1), random(1, 10 + crc32('')) between 1 and 10"
    order_qt_uniform_be "select number, uniform(1, 10 + crc32(''), number) between 1 and 10 from numbers('number' = '3')"
    order_qt_width_bucket_be "select k, width_bucket(v, 0, 10, 5 + crc32('')) from fold_literal_arguments_t"
    qt_array_apply_be "select array_apply([1, 2, 3], lpad('=', 2, '>'), 2)"
    order_qt_date_trunc_be "select k, date_trunc(dt, lpad('nth', 5, 'mo')), date_trunc(lpad('ar', 4, 'ye'), dt) from fold_literal_arguments_t"
    // beside a constant string date value too, in both argument orders, and the string date value derives the
    // same return type as beside a literal time unit
    qt_date_trunc_string_date_be """select date_trunc('2024-03-15 10:00:00', lpad('nth', 5, 'mo')),
            date_trunc(lpad('ar', 4, 'ye'), '2024-03-15 10:00:00'),
            date_trunc(concat('2024-03-15', ' 10:00:00'), lpad('nth', 5, 'mo')),
            date_trunc(lpad('ar', 4, 'ye'), concat('2024-03-15', ' 10:00:00'))"""
    sql "drop table if exists fold_literal_arguments_ctas_be_unit"
    sql """
        create table fold_literal_arguments_ctas_be_unit properties('replication_num' = '1') as
        select 1 k, date_trunc('2024-01-01 01:02:03+08:00', 'month') c1,
            date_trunc('2024-01-01 01:02:03+08:00', lpad('nth', 5, 'mo')) c2,
            date_trunc(lpad('nth', 5, 'mo'), '2024-01-01 01:02:03+08:00') c3,
            date_trunc(concat('2024-03-15', ' 10:00:00'), lpad('nth', 5, 'mo')) c4
    """
    qt_date_trunc_string_date_be_type "desc fold_literal_arguments_ctas_be_unit"
    order_qt_date_trunc_string_date_be_value "select * from fold_literal_arguments_ctas_be_unit"
    qt_sequence_match_be "select sequence_match(lpad('(?2)', 8, '(?1)'), dt, k = 1, k = 2) from fold_literal_arguments_t"
    qt_sequence_count_be "select sequence_count(lpad('(?2)', 8, '(?1)'), dt, k = 1, k = 2) from fold_literal_arguments_t"
    qt_orthogonal_bitmap_expr_calculate_be """select bitmap_to_string(orthogonal_bitmap_expr_calculate(
            to_bitmap(k), cast(k as varchar), lpad('|2', 3, '1'))) from fold_literal_arguments_t"""
    qt_orthogonal_bitmap_expr_calculate_count_be """select orthogonal_bitmap_expr_calculate_count(
            to_bitmap(k), cast(k as varchar), lpad('|2', 3, '1')) from fold_literal_arguments_t"""
    // A nullable formula expression that evaluates to a non-NULL value must still aggregate normally.
    qt_orthogonal_bitmap_expr_calculate_nullable_be """select bitmap_to_string(orthogonal_bitmap_expr_calculate(
            to_bitmap(k), cast(k as varchar), if(crc32('') = 0, '1', cast(null as varchar))))
            from fold_literal_arguments_t"""
    qt_orthogonal_bitmap_expr_calculate_count_nullable_be """select orthogonal_bitmap_expr_calculate_count(
            to_bitmap(k), cast(k as varchar), if(crc32('') = 0, '1', cast(null as varchar)))
            from fold_literal_arguments_t"""
    qt_topn_be """select topn(s, 2 + crc32('')), topn_array(s, 2 + crc32('')) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""
    qt_topn_weighted_be "select topn_weighted(s, k, 2 + crc32('')) from fold_literal_arguments_t"

    // BE validates the value it evaluates
    test {
        sql "select split_by_regexp('a,b,c', ',', crc32('') - 1)"
        exception "must be a positive constant"
    }
    test {
        sql "select tokenize('hello world', lpad('x', 1, 'x'))"
        exception "tokenize second argument must be properties format"
    }
    test {
        sql "select sequence_match(lpad('(?9)', 4, '('), dt, k = 1, k = 2) from fold_literal_arguments_t"
        exception "Event number 9 is out of range"
    }
    test {
        sql "select sequence_count(lpad('(?9)', 4, '('), dt, k = 1, k = 2) from fold_literal_arguments_t"
        exception "Event number 9 is out of range"
    }
    test {
        // FE accepts this pattern, but BE cannot parse two consecutive time conditions
        sql "select sequence_match('(?1)(?t>1)(?t<5)(?2)', dt, k = 1, k = 2) from fold_literal_arguments_t"
        exception "Temporal condition should be preceded by an event condition"
    }
    test {
        sql """select tokenize('hello world', concat('"char_filter_type"="x', lpad('', 0, ' '), '"'))"""
        exception "Invalid 'char_filter_type'"
    }
    test {
        sql "select topn(s, crc32('')) from fold_literal_arguments_t"
        exception "must be a constant positive integer"
    }
    test {
        sql "select topn_array(s, crc32('') - 1) from fold_literal_arguments_t"
        exception "must be a constant positive integer"
    }
    test {
        sql "select topn_weighted(s, k, crc32('')) from fold_literal_arguments_t"
        exception "must be a constant positive integer"
    }
    test {
        sql "select date_trunc(cast('2024-03-15' as date), concat('month', crc32('')))"
        exception "Illegal second argument"
    }
    test {
        sql "select date_trunc(concat('month', crc32('')), cast('2024-03-15' as date))"
        exception "Illegal second argument"
    }
    test {
        sql "select sha2('abc', 300 + crc32(''))"
        exception "sha2's digest length only support"
    }
    test {
        sql "select width_bucket(v, 0, 10, crc32('')) from fold_literal_arguments_t"
        exception "must be a positive integer value"
    }
    test {
        sql "select array_apply([1, 2, 3], lpad('>', 2, '>'), 2)"
        exception "unsupported op"
    }
    test {
        sql "select date_trunc(dt, lpad('x', 3, 'mo')) from fold_literal_arguments_t"
        exception "Illegal second argument"
    }
    test {
        sql "select date_trunc('2024-03-15 10:00:00', lpad('x', 3, 'mo'))"
        exception "Illegal second argument"
    }
    test {
        // date_trunc is pushed into the IF branches, and FE does not fold the illegal time unit
        sql "select date_trunc(cast('2024-03-15 10:00:00' as datetime), if(crc32('') = 0, 'xx', 'month'))"
        exception "Illegal second argument"
    }
    // a constant BE does not evaluate in open, such as an arithmetic or IF expression, is read in execute
    qt_open_non_constant_be """select rand(1 + crc32('')) = rand(1), random(1, 10 + crc32('')) between 1 and 10,
            uniform(1, 10 + crc32(''), crc32('x')) between 1 and 10,
            regexp_replace('abc', 'a', 'b', if(1 + crc32('') > 0, '', 'x')),
            date_trunc(cast('2024-03-15 10:00:00' as datetime), if(1 + crc32('') > 0, 'month', 'x'))"""
    // array_apply and uniform read an IF or size expression, which is not a ColumnConst, from the first row
    qt_non_column_const_be """select array_apply([1, 2, 3], if(1 + crc32('') > 0, '>', '<'), if(1 + crc32('') > 0, 1, 2)),
            uniform(1, size(array(crc32(''))) + 9, crc32('x')) between 1 and 10"""
    // a constant argument BE evaluates to a full column beside constant ones, over several rows
    order_qt_full_column_constant_be """select number, sha2('abc', if(crc32('') = 0, 256, 224)),
            split_by_regexp('a,b,c', ',', if(crc32('') = 0, 2, 3)),
            split_by_regexp('a,b,c', ',', uniform(1, 10, crc32('x'))) is not null
            from numbers('number' = '3')"""
    order_qt_array_apply_const_source """select number,
            array_apply(array_repeat(1, 16), '>', if(crc32('') = 0, 2, 3))
            from numbers('number' = '3')"""
    order_qt_regexp_replace_const_source """select number,
            length(regexp_replace(repeat('x', 16), '^.*\$', '', if(crc32('') = 0, '', 'IGNORE_INVALID_ESCAPE'))),
            length(regexp_replace_one(repeat('x', 16), '^.*\$', '', if(crc32('') = 0, '', 'IGNORE_INVALID_ESCAPE')))
            from numbers('number' = '3')"""
    order_qt_date_trunc_open_non_constant_be """select k, date_trunc(dt, if(1 + crc32('') > 0, 'month', 'x'))
            from fold_literal_arguments_t"""
    // the options BE evaluates to a full column reach the regex compiled for each row of an empty constant pattern
    order_qt_regexp_replace_empty_pattern_be """select number,
            regexp_replace('a', '', '\\\\x', if(uniform(1, 2, crc32('x')) > 0, 'ignore_invalid_escape', '')),
            regexp_replace_one('a', '', '\\\\x', if(uniform(1, 2, crc32('x')) > 0, 'ignore_invalid_escape', ''))
            from numbers('number' = '3')"""

    // FE needs the precision to derive the return type
    test {
        sql "select now(1 + crc32(''))"
        exception "NOW precision argument must be a constant literal"
    }

    // without constant folding, FE still validates the values before type coercion, and BE evaluates the arguments
    sql "set debug_skip_fold_constant = true"
    qt_topn_skip_fold """select topn(s, 1 + 1) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""
    qt_topn_array_skip_fold """select topn_array(s, 1 + 1) from
            (select 'a' s union all select 'a' union all select 'b' union all select 'b' union all select 'b' union all select 'c') t"""
    qt_topn_weighted_skip_fold "select topn_weighted(s, k, 1 + 1) from fold_literal_arguments_t"
    qt_sha2_skip_fold "select sha2('abc', 200 + 56)"
    qt_array_apply_skip_fold "select array_apply([1, 2, 3], concat('>', '='), 2)"
    order_qt_date_trunc_skip_fold "select k, date_trunc(dt, concat('mon', 'th')) from fold_literal_arguments_t"
    qt_now_skip_fold "select length(cast(now(1 + 2) as string))"
    test {
        sql "select sha2('abc', 200 + 100)"
        exception "sha2 functions only support digest length of"
    }
}
