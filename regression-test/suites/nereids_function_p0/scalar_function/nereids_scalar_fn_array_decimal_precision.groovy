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

// The default decimal v3 precision promotion must keep one Decimal group per occurrence
// for the slots a signature declares as individual type variables (array_sortby's
// src/sort_keys, array_zip's arrays and the expanded arguments of array_enumerate_uniq),
// otherwise a cross-range/cross-scale argument truncates the other one: the sort keys
// dropping the low order digits of the sorted source, array_zip returning values that do
// not match its Struct type, and array_enumerate_uniq collapsing distinct composite keys.
//
// The fixture uses the widest Decimal256 types so that merging the slots is lossy by
// construction: DECIMAL(76,76) and DECIMAL(76,70) have no common type that holds both the
// 76 digits of the source and the range of the sort keys, so the merge has to drop the six
// digits that discriminate the two source values (the decimal overflow scale fixes the
// merged type at DECIMAL(76,70)).
//
// The opposite case must keep sharing: if() resolves both branches and its return type to
// one common type, so neither branch may be narrowed back to the type of its own argument.
suite("nereids_scalar_fn_array_decimal_precision") {
    sql "set enable_decimal256 = true;"

    sql "drop table if exists fn_test_array_decimal_precision"
    sql """
        create table fn_test_array_decimal_precision (
            id int null,
            src array<decimalv3(76, 76)> null,
            sort_keys array<decimalv3(76, 70)> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_array_decimal_precision values
        (1,
         array(cast('0.0000000000000000000000000000000000000000000000000000000000000000000000111111' as decimalv3(76, 76))),
         array(cast('1.1234567890123456789012345678901234567890123456789012345678901234567890' as decimalv3(76, 70)))),
        (2,
         array(cast('0.0000000000000000000000000000000000000000000000000000000000000000000000111111' as decimalv3(76, 76)),
               cast('0.0000000000000000000000000000000000000000000000000000000000000000000000222222' as decimalv3(76, 76))),
         array(cast('1.1234567890123456789012345678901234567890123456789012345678901234567890' as decimalv3(76, 70)),
               cast('1.1234567890123456789012345678901234567890123456789012345678901234567890' as decimalv3(76, 70))));
    """

    // 1. array_sortby keeps the sorted source at its own DECIMAL(76,76) instead of
    // truncating it to the DECIMAL(76,70) of the sort keys
    order_qt_array_sortby """
        select id, array_sortby(src, sort_keys) as sorted
        from fn_test_array_decimal_precision where id = 1 order by id
    """

    // 2. array_zip keeps every array independent, so its Struct return type stays
    // consistent with the returned values
    order_qt_array_zip """
        select id, array_zip(src, sort_keys) as zipped
        from fn_test_array_decimal_precision order by id
    """

    // 3. array_enumerate_uniq expands its varargs; each argument keeps its own item type,
    // so the two source values that differ only beyond DECIMAL(76,70) stay distinct
    // (a truncation would report both as the first occurrence)
    order_qt_array_enumerate_uniq """
        select id, array_enumerate_uniq(src, sort_keys) as enumerated
        from fn_test_array_decimal_precision order by id
    """

    sql "drop table if exists fn_test_if_array_decimal_precision"
    sql """
        create table fn_test_if_array_decimal_precision (
            id int null,
            flag boolean null,
            a array<decimalv3(9, 2)> null,
            b array<decimalv3(10, 3)> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_if_array_decimal_precision values
        (1, true, array(cast('1234567.12' as decimalv3(9, 2))), array(cast('1234567.123' as decimalv3(10, 3)))),
        (2, false, array(cast('1234567.12' as decimalv3(9, 2))), array(cast('1234567.123' as decimalv3(10, 3)))),
        (3, true, null, array(cast('1234567.123' as decimalv3(10, 3))));
    """

    // 4. if() resolves both branches and the return to ARRAY<DECIMAL(10,3)> across the
    // Decimal32/Decimal64 boundary: the expected input type of the DECIMAL(9,2) branch
    // must stay common with the other branch and with the return type, otherwise the
    // analyzer omits the cast and the BE has to insert a Decimal32 nested column into
    // the Decimal64 return column
    order_qt_if_decimal32_branch """
        select id, if(flag, a, b) as chosen
        from fn_test_if_array_decimal_precision order by id
    """
}
