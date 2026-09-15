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

suite("test_cast_complex_null_covered_payload") {

    // Reproducer: IF/NULLIF keep the value of the branch that was not taken as a hidden payload of
    // the rows they mark as NULL. Every child of such a row is NULL by SQL semantics, so a strict
    // cast must not validate it, and the NULL mask of the row has to be expanded to the children
    // that belong to it: ARRAY elements, nested ARRAY elements and MAP keys/values are stored
    // flattened, while STRUCT fields keep the rows of their parent.
    sql "drop table if exists test_cast_complex_null_covered_payload;"
    sql """
        create table test_cast_complex_null_covered_payload (
            id int,
            p boolean not null,
            x int not null,
            arr array<int> not null,
            nested array<array<int>> not null,
            m map<int, int> not null,
            s struct<f1:int> not null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """
    sql """
        insert into test_cast_complex_null_covered_payload values
            (1, false, 128, [128], [[128]], {1: 128}, {128}),
            (2, true, 7, [1, 2], [[1, 2]], {1: 10, 2: 20}, {7});
    """

    // Used to verify that a genuinely non NULL out of range child is still rejected.
    sql "drop table if exists test_cast_complex_null_covered_payload_overflow;"
    sql """
        create table test_cast_complex_null_covered_payload_overflow (
            id int,
            arr array<int> not null,
            m map<int, int> not null,
            s struct<f1:int> not null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """
    sql """
        insert into test_cast_complex_null_covered_payload_overflow values
            (1, [300], {1: 300}, {300});
    """

    // The hidden payload of a string element cannot be replaced with a default value, so the mask of
    // the NULL row has to reach the element cast itself.
    sql "drop table if exists test_cast_complex_null_covered_payload_str;"
    sql """
        create table test_cast_complex_null_covered_payload_str (
            id int,
            p boolean not null,
            arrs array<string> not null
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1");
    """
    sql """
        insert into test_cast_complex_null_covered_payload_str values
            (1, false, ["abc"]),
            (2, true, ["1", "2"]);
    """

    // ---------- short_circuit_evaluation=false ----------
    sql "set short_circuit_evaluation=false;"

    sql "set enable_strict_cast=true;"
    order_qt_array_strict_sc_false """
        select id, cast(if(p, arr, null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_array_non_strict_sc_false """
        select id, cast(if(p, arr, null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_nullif_array_strict_sc_false """
        select id, cast(nullif(arr, [128]) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_nullif_array_non_strict_sc_false """
        select id, cast(nullif(arr, [128]) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_array_function_strict_sc_false """
        select id, cast(if(p, array(x), null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_array_function_non_strict_sc_false """
        select id, cast(if(p, array(x), null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_nested_array_strict_sc_false """
        select id, cast(if(p, nested, null) as array<array<tinyint>>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_nested_array_non_strict_sc_false """
        select id, cast(if(p, nested, null) as array<array<tinyint>>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_map_value_strict_sc_false """
        select id, cast(if(p, m, null) as map<int, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_map_value_non_strict_sc_false """
        select id, cast(if(p, m, null) as map<int, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    // The keys of a map are stored flattened as well, so the NULL of a map row must not be applied
    // to the entries of another row.
    sql "set enable_strict_cast=true;"
    order_qt_map_key_value_strict_sc_false """
        select id, cast(if(p, m, null) as map<tinyint, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_map_key_value_non_strict_sc_false """
        select id, cast(if(p, m, null) as map<tinyint, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_map_function_strict_sc_false """
        select id, cast(if(p, map(x, x), null) as map<tinyint, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_map_function_non_strict_sc_false """
        select id, cast(if(p, map(x, x), null) as map<tinyint, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_struct_strict_sc_false """
        select id, cast(if(p, s, null) as struct<f1:tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_struct_non_strict_sc_false """
        select id, cast(if(p, s, null) as struct<f1:tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_string_array_strict_sc_false """
        select id, cast(if(p, arrs, null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload_str order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_string_array_non_strict_sc_false """
        select id, cast(if(p, arrs, null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload_str order by id;
    """
    sql "set enable_strict_cast=true;"
    order_qt_string_array_nullif_strict_sc_false """
        select id, cast(nullif(arrs, ["abc"]) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload_str order by id;
    """

    // ---------- short_circuit_evaluation=true ----------
    sql "set short_circuit_evaluation=true;"

    sql "set enable_strict_cast=true;"
    order_qt_array_strict_sc_true """
        select id, cast(if(p, arr, null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """
    sql "set enable_strict_cast=false;"
    order_qt_array_non_strict_sc_true """
        select id, cast(if(p, arr, null) as array<tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_nested_array_strict_sc_true """
        select id, cast(if(p, nested, null) as array<array<tinyint>>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_map_key_value_strict_sc_true """
        select id, cast(if(p, m, null) as map<tinyint, tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    sql "set enable_strict_cast=true;"
    order_qt_struct_strict_sc_true """
        select id, cast(if(p, s, null) as struct<f1:tinyint>) as r
        from test_cast_complex_null_covered_payload order by id;
    """

    // A genuinely non NULL out of range child must still fail in strict mode.
    sql "set short_circuit_evaluation=false;"
    sql "set enable_strict_cast=true;"
    test {
        sql """
            select cast(arr as array<tinyint>) as r
            from test_cast_complex_null_covered_payload_overflow where id = 1;
        """
        exception "Value 300 out of range for type tinyint"
    }
    test {
        sql """
            select cast(m as map<int, tinyint>) as r
            from test_cast_complex_null_covered_payload_overflow where id = 1;
        """
        exception "Value 300 out of range for type tinyint"
    }
    test {
        sql """
            select cast(s as struct<f1:tinyint>) as r
            from test_cast_complex_null_covered_payload_overflow where id = 1;
        """
        exception "Value 300 out of range for type tinyint"
    }
}
