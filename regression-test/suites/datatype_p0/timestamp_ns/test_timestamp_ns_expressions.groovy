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

suite("test_timestamp_ns_expressions") {
    sql "drop table if exists timestamp_ns_expressions"
    sql """
        create table timestamp_ns_expressions (
            id int,
            value timestamp_ns,
            fallback timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_expressions values
        (1, '1677-09-21 00:12:43.145224192', '1970-01-01 00:00:00.000000000'),
        (2, '1969-12-31 23:59:59.999999999', '1970-01-01 00:00:00.000000001'),
        (3, '1970-01-01 00:00:00.000000000', '2024-02-29 12:34:56.123456789'),
        (4, '1970-01-01 00:00:00.000000001', null),
        (5, '2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854775807'),
        (6, null, '1970-01-01 00:00:00.000000000'),
        (7, null, null)
    """

    order_qt_comparisons """
        select id,
               value = fallback,
               value != fallback,
               value > fallback,
               value >= fallback,
               value < fallback,
               value <= fallback,
               value <=> fallback
        from timestamp_ns_expressions
        order by id
    """
    order_qt_null_predicates """
        select id, value is null, value is not null
        from timestamp_ns_expressions
        order by id
    """
    order_qt_in_not_in """
        select id,
               value in (
                   cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               value not in (
                   cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               value in (
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns), null),
               value not in (
                   cast('1970-01-01 00:00:00.000000000' as timestamp_ns), null)
        from timestamp_ns_expressions
        order by id
    """
    order_qt_between """
        select id,
               value between cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                         and cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
               value not between cast('1969-12-31 23:59:59.999999999' as timestamp_ns)
                             and cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
        from timestamp_ns_expressions
        order by id
    """
    order_qt_boolean_composition """
        select id,
               (value is null or value >= cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               not (value <=> fallback),
               (value is not null and fallback is not null)
        from timestamp_ns_expressions
        order by id
    """

    order_qt_condition_result """
        select id,
               if(id % 2 = 0, value, fallback),
               ifnull(value, fallback),
               coalesce(value, fallback,
                        cast('1970-01-01 00:00:00.000000000' as timestamp_ns)),
               nullif(value, fallback),
               case
                   when id = 1 then value
                   when id = 2 then fallback
                   when id = 3 then cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                   else null
               end
        from timestamp_ns_expressions
        order by id
    """

    // DATETIMEV2 has only microsecond precision, so a mixed conditional expression must widen
    // it to TIMESTAMP_NS without truncating the nanosecond branch.
    order_qt_mixed_condition_result """
        select id,
               if(id % 2 = 0, value,
                  cast('2024-02-29 12:34:56.123456' as datetimev2(6))),
               case when id % 2 = 0 then value
                    else cast('2024-02-29 12:34:56.123456' as datetimev2(6)) end
        from timestamp_ns_expressions
        order by id
    """

    sql "set debug_skip_fold_constant = false"
    qt_folded_conditions """
        select
            if(true,
               cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
               cast('1970-01-01 00:00:00.000000002' as timestamp_ns)),
            coalesce(null, cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                in (cast('1970-01-01 00:00:00.000000001' as timestamp_ns))
    """
    sql "set debug_skip_fold_constant = true"
    qt_runtime_conditions """
        select
            if(true,
               cast('1970-01-01 00:00:00.000000001' as timestamp_ns),
               cast('1970-01-01 00:00:00.000000002' as timestamp_ns)),
            coalesce(null, cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
                in (cast('1970-01-01 00:00:00.000000001' as timestamp_ns))
    """
    sql "set debug_skip_fold_constant = false"
}
