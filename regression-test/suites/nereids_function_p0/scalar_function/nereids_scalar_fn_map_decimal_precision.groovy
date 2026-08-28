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

// The key and the value of a MAP are independent decimal slots. The default
// decimal v3 precision promotion must keep their precision/scale independent
// instead of merging them into one type, otherwise widening the scale of a big
// integral key silently converts it to NULL (e.g. in map_keys) and UNNEST(MAP)/
// explode_map may fail the type check.
suite("nereids_scalar_fn_map_decimal_precision") {
    sql "set enable_decimal256 = true;"

    // 1. map_keys must keep the big integral key instead of turning it into NULL
    order_qt_map_keys_decimal256 """
        SELECT
            CAST(MAP(
                CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
                CAST('0.125000000000000000' AS DECIMAL(76,18))
            ) AS STRING) AS source_map,
            CAST(MAP_KEYS(MAP(
                CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
                CAST('0.125000000000000000' AS DECIMAL(76,18))
            )) AS STRING) AS actual_keys,
            CAST(ARRAY(CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0))) AS STRING) AS expected_keys;
    """

    // 2. UNNEST(MAP) must pass the type check and return independent key/value
    order_qt_unnest_map_decimal256 """
        SELECT key_out, value_out
        FROM UNNEST(MAP(
            CAST('1.2500' AS DECIMAL(16,4)),
            CAST('2.125000000000000000' AS DECIMAL(76,18))
        )) AS expanded(key_out, value_out);
    """

    // 3. explode_map keeps independent key/value decimals
    order_qt_explode_map_decimal256 """
        SELECT k, v
        FROM (SELECT 1) x
        LATERAL VIEW EXPLODE_MAP(MAP(
            CAST('1.2500' AS DECIMAL(16,4)),
            CAST('2.125000000000000000' AS DECIMAL(76,18))
        )) t AS k, v;
    """

    // 4. map_contains_key can find the big integral key
    order_qt_map_contains_key_decimal256 """
        SELECT MAP_CONTAINS_KEY(MAP(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            CAST('0.125000000000000000' AS DECIMAL(76,18))
        ), CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0))) AS ck;
    """

    // 5. map_values keeps the value
    order_qt_map_values_decimal256 """
        SELECT CAST(MAP_VALUES(MAP(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            CAST('0.125000000000000000' AS DECIMAL(76,18))
        )) AS STRING) AS actual_values;
    """

    // 6. map_entries keeps independent key/value decimals
    order_qt_map_entries_decimal256 """
        SELECT CAST(MAP_ENTRIES(MAP(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            CAST('0.125000000000000000' AS DECIMAL(76,18))
        )) AS STRING) AS entries;
    """

    // 7. element_at over a map with independent key/value decimals
    order_qt_element_at_decimal256 """
        SELECT ELEMENT_AT(MAP(
            CAST('1.2500' AS DECIMAL(16,4)),
            CAST('2.125000000000000000' AS DECIMAL(76,18))
        ), CAST('1.2500' AS DECIMAL(16,4))) AS value;
    """

    // 8. element_at / map_contains_key with a wider lookup on a Decimal32 key column.
    // The lookup slot follows the MAP key type, so both must be promoted to one type
    // (across the DECIMAL32 -> DECIMAL64 storage-width boundary) instead of being
    // widened independently, otherwise the BE compares columns of different concrete
    // decimal classes.
    sql "drop table if exists fn_test_map_decimal_precision"
    sql """
        create table fn_test_map_decimal_precision (
            id int null,
            m map<decimal(9,2), decimal(5,2)> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_map_decimal_precision values
        (1, map(cast('1234567.89' as decimal(9,2)), cast('12.34' as decimal(5,2)))),
        (2, map(cast('1.23' as decimal(9,2)), cast('0.01' as decimal(5,2))));
    """
    order_qt_element_at_wider_lookup """
        select id, element_at(m, cast('1234567.890' as decimal(10,3)))
        from fn_test_map_decimal_precision order by id
    """
    order_qt_map_contains_key_wider_lookup """
        select id, map_contains_key(m, cast('1234567.890' as decimal(10,3)))
        from fn_test_map_decimal_precision order by id
    """
    // a NULL lookup must fall back to the MAP key group instead of the wider type of
    // an unrelated MAP leaf
    order_qt_element_at_null_lookup """
        select id, element_at(m, null)
        from fn_test_map_decimal_precision order by id
    """

    // 9. field declares varArgs(DECIMALV3, DECIMALV3): its fixed first operand and the
    // repeated tail are one comparison type and must keep one promoted type
    order_qt_field_decimal """
        select field(cast('1.20' as decimal(3,2)), cast('1.200' as decimal(4,3)),
                     cast('2.000' as decimal(4,3)))
    """

    // 10. nonconstant nested containers: the item of an ARRAY nested in a MAP value
    // keeps its own precision/scale instead of being merged with the key into the wider
    // type, otherwise map_values() discards the low-order fractional digits
    sql "drop table if exists fn_test_map_nested_decimal"
    sql """
        create table fn_test_map_nested_decimal (
            id int null,
            m map<decimal(38,0), array<decimal(38,18)>> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_map_nested_decimal values
        (1, map(cast('12345678901234567890123456789012345678' as decimal(38,0)),
                array(cast('0.123456789012345678' as decimal(38,18)))));
    """
    order_qt_map_values_nested_array """
        select cast(map_values(m) as string) as v from fn_test_map_nested_decimal order by id
    """
    order_qt_map_keys_nested_array """
        select cast(map_keys(m) as string) as k from fn_test_map_nested_decimal order by id
    """

    // 11. nonconstant nested MAP: the key/value of a MAP nested in a MAP value keep
    // their own precision/scale instead of being merged with the outer MAP leaves
    sql "drop table if exists fn_test_nested_map_decimal"
    sql """
        create table fn_test_nested_map_decimal (
            id int null,
            m map<decimal(38,0), map<decimal(9,2), decimal(5,2)>> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_nested_map_decimal values
        (1, map(cast('12345678901234567890123456789012345678' as decimal(38,0)),
                map(cast('1234567.89' as decimal(9,2)), cast('12.34' as decimal(5,2)))));
    """
    order_qt_map_values_nested_map """
        select cast(map_values(m) as string) as v from fn_test_nested_map_decimal order by id
    """
    order_qt_map_keys_nested_map """
        select cast(map_keys(m) as string) as k from fn_test_nested_map_decimal order by id
    """

    // 12. basic decimal v3 precision promotion (single slot) is not affected
    order_qt_basic_decimal256 """
        SELECT ABS(CAST('123.456' AS DECIMAL(10,3))) AS abs_v,
               ROUND(CAST('123.456' AS DECIMAL(10,3)), 2) AS round_v;
    """

    // 13. struct(...) fields are independent type variables: the default decimal v3
    // precision promotion must not merge them (e.g. widening the scale of a DECIMAL(76,0)
    // field would truncate the decimals of an ARRAY<DECIMAL(76,18)> field)
    order_qt_struct_independent_fields """
        SELECT CAST(STRUCT(
            CAST('9999999999999999999999999999999999999999999999999999999999999999999999999999' AS DECIMAL(76,0)),
            ARRAY(CAST('0.125000000000000000' AS DECIMAL(76,18)))
        ) AS STRING) AS s;
    """

    // 14. nonconstant map_contains_value / map_contains_entry: the probe (Follow(0)) and
    // the MAP value (Any(0)) are one logical group. When the probe and the independent key
    // resolve to the same type (DECIMAL(10,3)), the probe must still be linked to the value
    // group by the original Any/Follow index, otherwise the value regresses to DECIMAL(9,2)
    // and the BE compares a Decimal32 value array with a Decimal64 probe.
    sql "drop table if exists fn_test_map_contains_collision_decimal"
    sql """
        create table fn_test_map_contains_collision_decimal (
            id int null,
            m map<decimal(10,3), decimal(9,2)> null,
            x decimal(10,3) null,
            k decimal(10,3) null,
            v decimal(9,2) null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_map_contains_collision_decimal values
        (1, map(cast('1234567.890' as decimal(10,3)), cast('12.34' as decimal(9,2))),
            cast('1234567.890' as decimal(10,3)),
            cast('1234567.890' as decimal(10,3)), cast('12.34' as decimal(9,2))),
        (2, map(cast('1.230' as decimal(10,3)), cast('0.01' as decimal(9,2))),
            cast('9.999' as decimal(10,3)),
            cast('9.999' as decimal(10,3)), cast('0.02' as decimal(9,2)));
    """
    order_qt_map_contains_value_collision """
        select id, map_contains_value(m, x) as r from fn_test_map_contains_collision_decimal order by id
    """
    order_qt_map_contains_entry_collision """
        select id, map_contains_entry(m, k, v) as r from fn_test_map_contains_collision_decimal order by id
    """

    // 15. array_pushfront(ARRAY<Any(0)>, Any(0)) with ARRAY-of-MAP and MAP inputs: the
    // container Any identity must be propagated into the descendant-relative MAP keys so
    // both slots stay one compatible MAP type instead of regressing to incompatible types.
    sql "drop table if exists fn_test_arr_push_map_decimal"
    sql """
        create table fn_test_arr_push_map_decimal (
            id int null,
            arr array<map<decimal(10,3), decimal(5,2)>> null,
            m map<decimal(9,2), decimal(9,2)> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_arr_push_map_decimal values
        (1, array(map(cast('1234567.890' as decimal(10,3)), cast('12.345' as decimal(5,2)))),
            map(cast('12.34' as decimal(9,2)), cast('1.23' as decimal(9,2))));
    """
    order_qt_array_pushfront_map_container """
        select id, array_pushfront(arr, m) as r from fn_test_arr_push_map_decimal order by id
    """

    // 16. array_contains(ARRAY<Any(0)>, Any(0)): the ARRAY item and the probe are one
    // logical group, so a wider ARRAY<DECIMAL(27,9)> with a DECIMAL(9,3) probe must keep
    // both at DECIMAL(27,9) instead of regressing only the probe to Decimal32.
    sql "drop table if exists fn_test_arr_contains_decimal"
    sql """
        create table fn_test_arr_contains_decimal (
            id int null,
            arr array<decimal(27,9)> null,
            probe decimal(9,3) null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_arr_contains_decimal values
        (1, array(cast('123456789012345678.123456789' as decimal(27,9))), cast('123456.789' as decimal(9,3))),
        (2, array(cast('123456789012345678.123456789' as decimal(27,9))), cast('123456789012345678.123' as decimal(27,3)));
    """
    order_qt_array_contains_array_group """
        select id, array_contains(arr, probe) as r from fn_test_arr_contains_decimal order by id
    """
}
