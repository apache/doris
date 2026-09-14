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

suite("test_cast_to_tinyint_from_int_nullable_null_covered_payload") {

    // Reproducer: row 1 keeps 128 as the hidden nested payload of a Nullable(INT) produced by
    // IF/NULLIF, while the null map marks it as NULL. Strict cast must skip such NULL rows.
    sql "drop table if exists test_cast_null_covered_payload;"
    sql """
        create table test_cast_null_covered_payload (
            id int,
            p boolean not null,
            x int not null,
            y int not null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """
    sql """
        insert into test_cast_null_covered_payload values
            (1, false, 128, 128),
            (2, true, 1, 2);
    """

    // Used to verify that a genuinely non NULL out-of-range value is still rejected.
    sql "drop table if exists test_cast_null_covered_payload_overflow;"
    sql """
        create table test_cast_null_covered_payload_overflow (
            id int,
            x int not null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """
    sql "insert into test_cast_null_covered_payload_overflow values (1, 128);"

    // ---------- short_circuit_evaluation=false ----------
    sql "set short_circuit_evaluation=false;"

    sql "set enable_strict_cast=true;"
    order_qt_if_strict_sc_false """
        select id, cast(if(p, x, null) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_if_non_strict_sc_false """
        select id, cast(if(p, x, null) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_nullif_strict_sc_false """
        select id, cast(nullif(x, y) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_nullif_non_strict_sc_false """
        select id, cast(nullif(x, y) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """

    // ---------- short_circuit_evaluation=true ----------
    sql "set short_circuit_evaluation=true;"

    sql "set enable_strict_cast=true;"
    order_qt_if_strict_sc_true """
        select id, cast(if(p, x, null) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_if_non_strict_sc_true """
        select id, cast(if(p, x, null) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_nullif_strict_sc_true """
        select id, cast(nullif(x, y) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_nullif_non_strict_sc_true """
        select id, cast(nullif(x, y) as tinyint) as r
        from test_cast_null_covered_payload order by id;
    """

    // Number to date/timestamp_ns casts parse the source value with a strict-mode serde batch that
    // cannot skip rows, so the hidden payload of a NULL row must not fail the query either.
    sql "drop table if exists test_cast_null_covered_payload_datetime;"
    sql """
        create table test_cast_null_covered_payload_datetime (
            id int,
            p boolean not null,
            big bigint not null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """
    sql """
        insert into test_cast_null_covered_payload_datetime values
            (1, false, 1000),
            (2, true, 20150102030405);
    """

    sql "set enable_strict_cast=true;"
    order_qt_date_strict """
        select id, cast(if(p, big, null) as date) as r
        from test_cast_null_covered_payload_datetime order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_date_non_strict """
        select id, cast(if(p, big, null) as date) as r
        from test_cast_null_covered_payload_datetime order by id;
    """
    sql "set enable_strict_cast=true;"
    order_qt_timestamp_ns_strict """
        select id, cast(if(p, big, null) as timestamp_ns) as r
        from test_cast_null_covered_payload_datetime order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_timestamp_ns_non_strict """
        select id, cast(if(p, big, null) as timestamp_ns) as r
        from test_cast_null_covered_payload_datetime order by id;
    """

    // A genuinely non NULL out-of-range value must still fail in strict mode.
    sql "set short_circuit_evaluation=false;"
    sql "set enable_strict_cast=true;"
    test {
        sql """
            select cast(x as tinyint)
            from test_cast_null_covered_payload_overflow where id = 1;
        """
        exception "Value 128 out of range for type tinyint"
    }
}
