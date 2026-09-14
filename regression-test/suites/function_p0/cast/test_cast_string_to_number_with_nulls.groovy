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

suite("test_cast_string_to_number_with_nulls") {
    sql "drop table if exists test_cast_string_to_number_with_nulls_bigint;"
    sql """
        create table test_cast_string_to_number_with_nulls_bigint (
            id int,
            s varchar(16)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """

    sql "drop table if exists test_cast_string_to_number_with_nulls_decimal;"
    sql """
        create table test_cast_string_to_number_with_nulls_decimal (
            id int,
            s varchar(16)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """

    sql "set enable_strict_cast=true;"

    // if(s = '-', NULL, s) marks rows null without clearing their bytes from the nested string
    // column, so a strict cast must still read every row from its own offsets.
    sql "truncate table test_cast_string_to_number_with_nulls_bigint;"
    sql "insert into test_cast_string_to_number_with_nulls_bigint values (1, '-'), (2, '2628');"
    qt_bigint_null_row_then_value """
        select id, cast(if(s = '-', NULL, s) as bigint) as v
        from test_cast_string_to_number_with_nulls_bigint order by id;
    """

    sql "truncate table test_cast_string_to_number_with_nulls_decimal;"
    sql "insert into test_cast_string_to_number_with_nulls_decimal values (1, '-'), (2, '2628');"
    qt_decimal_null_row_then_value """
        select id, cast(if(s = '-', NULL, s) as decimal(10,0)) as v
        from test_cast_string_to_number_with_nulls_decimal order by id;
    """

    // A run of null rows must not accumulate onto the next value either.
    sql "truncate table test_cast_string_to_number_with_nulls_bigint;"
    sql """
        insert into test_cast_string_to_number_with_nulls_bigint values
            (1, '-'), (2, '-'), (3, '-'), (4, '-'), (5, '-'), (6, '2628');
    """
    qt_bigint_null_run_then_value """
        select id, cast(if(s = '-', NULL, s) as bigint) as v
        from test_cast_string_to_number_with_nulls_bigint order by id;
    """

    sql "truncate table test_cast_string_to_number_with_nulls_decimal;"
    sql """
        insert into test_cast_string_to_number_with_nulls_decimal values
            (1, '-'), (2, '-'), (3, '-'), (4, '-'), (5, '-'), (6, '2628');
    """
    qt_decimal_null_run_then_value """
        select id, cast(if(s = '-', NULL, s) as decimal(10,0)) as v
        from test_cast_string_to_number_with_nulls_decimal order by id;
    """

    // A non-null row that is not a number is still rejected, quoting that row's own bytes.
    sql "truncate table test_cast_string_to_number_with_nulls_bigint;"
    sql "insert into test_cast_string_to_number_with_nulls_bigint values (1, '-'), (2, '2628');"
    test {
        sql """
            select id, cast(s as bigint) as v
            from test_cast_string_to_number_with_nulls_bigint order by id;
        """
        exception "parse number fail, string: '-'"
    }

    // INSERT ... SELECT takes the strict path from enable_insert_strict, independently of the
    // session-scoped enable_strict_cast used by the queries above.
    sql "set enable_strict_cast=false;"
    sql "truncate table test_cast_string_to_number_with_nulls_bigint;"
    sql "insert into test_cast_string_to_number_with_nulls_bigint values (1, '-'), (2, '2628');"

    sql "drop table if exists test_cast_string_to_number_with_nulls_target;"
    sql """
        create table test_cast_string_to_number_with_nulls_target (
            id int,
            v bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into test_cast_string_to_number_with_nulls_target
        select id, cast(if(s = '-', NULL, s) as bigint)
        from test_cast_string_to_number_with_nulls_bigint;
    """
    qt_insert_select_null_row_then_value """
        select id, v from test_cast_string_to_number_with_nulls_target order by id;
    """
}
