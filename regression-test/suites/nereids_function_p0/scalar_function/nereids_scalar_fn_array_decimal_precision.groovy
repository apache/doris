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

// Independent non-MAP ARRAY slots are separate logical type variables: array_sortby's
// src/sort_keys, array_zip's arrays and the expanded arguments of array_enumerate_uniq.
// The default decimal v3 precision promotion must keep one Decimal group per occurrence
// instead of merging them into the wider type of another slot, otherwise a
// cross-range/cross-scale argument truncates the other one (the sort keys widening the
// sorted source and dropping its low order digits, array_zip returning values that do
// not match its Struct type, and array_enumerate_uniq collapsing distinct composite
// keys).
suite("nereids_scalar_fn_array_decimal_precision") {
    sql "set enable_decimal256 = true;"

    sql "drop table if exists fn_test_array_decimal_precision"
    sql """
        create table fn_test_array_decimal_precision (
            id int null,
            src array<decimalv3(38, 38)> null,
            sort_keys array<decimalv3(38, 32)> null
        ) engine=olap
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into fn_test_array_decimal_precision values
        (1,
         array(cast('0.12345678901234567890123456789012345678' as decimalv3(38, 38))),
         array(cast('123456.12345678901234567890123456789012' as decimalv3(38, 32)))),
        (2,
         array(cast('0.12345678901234567890123456789012345678' as decimalv3(38, 38)),
               cast('0.12345678901234567890123456789012345679' as decimalv3(38, 38))),
         array(cast('1.12000000000000000000000000000000' as decimalv3(38, 32)),
               cast('1.12000000000000000000000000000000' as decimalv3(38, 32))));
    """

    // 1. array_sortby keeps the sorted source at its own DECIMAL(38,38) instead of
    // truncating it to the DECIMAL(38,32) of the sort keys
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
    // so two composite keys that differ only beyond DECIMAL(38,32) stay distinct
    // (they would both be reported as the first occurrence after a truncation)
    order_qt_array_enumerate_uniq """
        select id, array_enumerate_uniq(src, sort_keys) as enumerated
        from fn_test_array_decimal_precision order by id
    """
}
