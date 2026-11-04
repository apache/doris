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

suite("test_collect_list_native_serialization") {
    sql "set agg_phase = 2"
    sql "drop table if exists test_collect_list_native_serialization"
    sql """
        create table test_collect_list_native_serialization (
            id int not null,
            a array<string> not null,
            nullable_a array<string>,
            m map<string, string>,
            s struct<v:string, n:string>,
            decimals array<decimal(38, 18)>,
            times array<datetimev2(6)>
        ) duplicate key(id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1")
    """
    // The original single string a","b must not become two array elements after exchange.
    // Hex input also avoids dependence on SQL/Groovy escaping for quotes, backslashes and NUL.
    sql """
        insert into test_collect_list_native_serialization
        select number,
               array(unhex('61222C2262'), unhex('615C62'), unhex('610062'), null, ''),
               if(number % 2 = 0, null, array(unhex('61222C2262'))),
               map('k', unhex('61222C2262')),
               named_struct('v', unhex('61222C2262'), 'n', null),
               array(cast('12345678901234567890.123456789012345678' as decimal(38, 18)), null),
               array(cast('2026-11-04 12:34:56.123456' as datetimev2(6)), null)
        from numbers("number" = "8")
    """

    explain {
        sql "select collect_list(a) from test_collect_list_native_serialization"
        contains "update serialize"
        contains "merge finalize"
        contains "EXCHANGE"
    }

    // Sorting retains duplicate multiplicity while removing partial aggregation ordering.
    for (def function : ["collect_list", "group_array", "array_agg"]) {
        "qt_nested_${function}" """
            select size(xs), array_sort(array_map(x -> size(x), xs)),
                   array_sort(array_map(x -> hex(x[1]), xs)),
                   array_sort(array_map(x -> hex(x[2]), xs)),
                   array_sort(array_map(x -> hex(x[3]), xs)),
                   array_sort(array_map(x -> x[4] is null, xs)),
                   array_sort(array_map(x -> hex(x[5]), xs))
            from (select ${function}(a) xs from test_collect_list_native_serialization) t
        """
    }

    qt_nullable """
        select size(xs), array_sort(array_map(x -> hex(x[1]), xs)),
               size(ys), array_sort(array_map(x -> x is null, ys))
        from (select collect_list(nullable_a) xs, array_agg(nullable_a) ys
              from test_collect_list_native_serialization) t
    """
    qt_map_struct """
        select size(ms), array_sort(array_map(x -> hex(element_at(x, 'k')), ms)),
               size(ss), array_sort(array_map(x -> hex(struct_element(x, 'v')), ss)),
               array_sort(array_map(x -> struct_element(x, 'n') is null, ss))
        from (select collect_list(m) ms, collect_list(s) ss
              from test_collect_list_native_serialization) t
    """
    qt_precision """
        select array_sort(array_map(x -> cast(x[1] as string), ds)),
               array_sort(array_map(x -> cast(x[1] as string), ts)),
               array_sort(array_map(x -> x[2] is null, ds)),
               array_sort(array_map(x -> x[2] is null, ts))
        from (select collect_list(decimals) ds, collect_list(times) ts
              from test_collect_list_native_serialization) t
    """
    // Each selected value is identical, so limit does not depend on exchange arrival order.
    qt_limits """
        select size(z), size(o), size(p), array_sort(array_map(x -> hex(x[1]), p))
        from (select collect_list(nullable_a, 0) z, collect_list(nullable_a, 1) o,
                     collect_list(nullable_a, 3) p
              from test_collect_list_native_serialization) t
    """
    qt_empty """
        select collect_list(a), array_agg(a)
        from test_collect_list_native_serialization where id < 0
    """
    qt_distinct """
        select array_sort(array_map(x -> hex(x[1]), xs))
        from (select collect_list(distinct a) xs
              from test_collect_list_native_serialization) t
    """
    qt_query_local_state """
        select size(xs), array_sort(array_map(x -> hex(x[1]), xs))
        from (select collect_list_merge(collect_list_state(a)) xs
              from test_collect_list_native_serialization) t
    """
}
