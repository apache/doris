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

suite("test_timestamp_ns_mixed_datetime_coercion", "nonConcurrent") {
    sql "set enable_strict_cast = false"
    sql "drop table if exists timestamp_ns_mixed_datetime_coercion"
    sql """
        create table timestamp_ns_mixed_datetime_coercion (
            id int,
            ts timestamp_ns,
            dt datetimev2(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_datetime_coercion values
        (1, '1677-09-21 00:12:43.145224192', '1700-01-01 00:00:00.000000'),
        (2, '2262-04-11 23:47:16.854775807', '2200-01-01 00:00:00.000000'),
        (3, '1700-01-01 00:00:00.000000000', '1700-01-01 00:00:00.000000'),
        (4, '2200-01-01 00:00:00.000000000', '2200-01-01 00:00:00.000000'),
        (5, null, null)
    """

    sql "drop table if exists timestamp_ns_mixed_date_arrays"
    sql """
        create table timestamp_ns_mixed_date_arrays (
            id int,
            dates array<datev2>,
            nested_dates array<array<datev2>>,
            stamps array<timestamp_ns>,
            nested_stamps array<array<timestamp_ns>>
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_mixed_date_arrays values
            (1, ['2024-01-01'], [['2024-01-01']],
                ['2024-01-01 00:00:00.000000001'], [['2024-01-01 00:00:00.000000001']]),
            (2, ['2500-01-01'], [['2500-01-01']],
                ['2024-01-01 00:00:00.000000001'], [['2024-01-01 00:00:00.000000001']]),
            (3, ['1600-01-01'], [['1600-01-01']],
                ['2024-01-01 00:00:00.000000001'], [['2024-01-01 00:00:00.000000001']]),
            (4, null, null, null, null),
            (5, [], [[]], [], [[]]),
            (6, [null], [[null]], [null], [[null]])
    """

    def checkCoercionMode = { mode ->
        sql "set global enable_new_type_coercion_behavior = ${mode}"

        "qt_mixed_temporal_container_literals_${mode}" """
            select
                array(cast('2024-01-01' as datev2))
                    < array(cast('2024-01-01 00:00:00.000000001' as timestamp_ns)),
                array(array(cast('2024-01-01' as datev2)))
                    = array(array(cast('2024-01-01 00:00:00' as timestamp_ns))),
                coalesce({1: datev2 '2024-01-01'},
                         map(1, cast('2024-01-01 00:00:00' as timestamp_ns))),
                coalesce({1, datev2 '2024-01-01'},
                         struct(1, cast('2024-01-01 00:00:00' as timestamp_ns))),
                cast(array(cast('2500-01-01' as datev2)) as array<timestamp_ns>),
                cast(array(array(cast('2500-01-01' as datev2))) as array<array<timestamp_ns>>)
        """
        // Constant folding must propagate the implicit cast's forced strictness to every leaf.
        for (def date : ['1600-01-01', '2500-01-01']) {
            def dt = "cast('${date}' as datev2)"
            def ns = "cast('2024-01-01 00:00:00' as timestamp_ns)"
            for (def containers : [["array(${dt})", "array(${ns})"],
                                   ["array(array(${dt}))", "array(array(${ns}))"]]) {
                for (def operands : [containers, containers.reverse()]) {
                    for (def op : ['=', '<', '<=>']) {
                        test {
                            sql "select ${operands[0]} ${op} ${operands[1]}"
                            exception "outside Int64 epoch nanosecond range"
                        }
                    }
                }
            }
            // A leading integer avoids JDBC interpreting {datev2 ...} as a date escape.
            for (def containers : [["{1: datev2 '${date}'}", "map(1, ${ns})"],
                                   ["{1, datev2 '${date}'}", "struct(1, ${ns})"],
                                   ["[{{1: [datev2 '${date}']}}]",
                                    "array(struct(map(1, array(${ns}))))"]]) {
                test {
                    sql "select coalesce(${containers[0]}, ${containers[1]})"
                    exception "outside Int64 epoch nanosecond range"
                }
            }
        }

        "qt_mixed_date_array_comparisons_${mode}" """
            select id, dates = stamps, stamps = dates, dates < stamps, dates <=> stamps,
                nested_dates = nested_stamps, nested_stamps = nested_dates,
                nested_dates < nested_stamps, nested_dates <=> nested_stamps
            from timestamp_ns_mixed_date_arrays
            where id not in (2, 3)
            order by id
        """
        // Both operand orders and both nesting levels must fail instead of replacing a bad
        // temporal element with NULL when the session disables strict explicit casts.
        for (def row : [2, 3]) {
            for (def columns : [['dates', 'stamps'], ['nested_dates', 'nested_stamps']]) {
                for (def operands : [columns, columns.reverse()]) {
                    for (def op : ['=', '<', '<=>']) {
                        test {
                            sql """
                                select ${operands[0]} ${op} ${operands[1]}
                                from timestamp_ns_mixed_date_arrays where id = ${row}
                            """
                            exception "TIMESTAMP_NS overflow"
                        }
                    }
                }
            }
        }

        "order_qt_mixed_column_comparison_${mode}" """
            select id, ts > dt, dt < ts, ts = dt
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """

        "qt_signed_nanos_boundaries_${mode}" """
            select
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns)
                    < cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns)
                    > cast('2200-01-01 00:00:00.000000' as datetimev2(6))
        """

        "order_qt_exact_literal_in_${mode}" """
            select id
            from timestamp_ns_mixed_datetime_coercion
            where ts in (
                cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                cast('2200-01-01 00:00:00.000000' as datetimev2(6)))
            order by id
        """

        "order_qt_exact_literal_case_coalesce_${mode}" """
            select id,
                   case when id = 1 then ts
                        else cast('2024-01-02 03:04:05.123456' as datetimev2(6)) end,
                   coalesce(ts, cast('2024-01-02 03:04:05.123456' as datetimev2(6)))
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """

        "order_qt_mixed_column_hash_join_${mode}" """
            select l.id, r.id
            from (select * from timestamp_ns_mixed_datetime_coercion where id >= 3) l
            join (select * from timestamp_ns_mixed_datetime_coercion where id >= 3) r
              on l.ts = r.dt
            order by l.id, r.id
        """

        "qt_mixed_literal_in_${mode}" """
            select
                cast('2024-01-02 03:04:05.123456' as timestamp_ns) in (
                    cast('2024-01-02 03:04:05.123456' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6))),
                cast('2024-01-02 03:04:05.123456' as timestamp_ns) not in (
                    cast('2024-01-02 03:04:05.123456' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6)))
        """

        "order_qt_mixed_in_three_valued_logic_${mode}" """
            select id,
                ts in (
                    cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6))),
                ts not in (
                    cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6))),
                ts in (
                    cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6))),
                ts not in (
                    cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6))),
                ts in (
                    cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6)), null),
                ts not in (
                    cast('1700-01-01 00:00:00.000000' as datetimev2(6)),
                    cast('2200-01-01 00:00:00.000000' as datetimev2(6)), null)
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """
        "order_qt_mixed_column_case_coalesce_${mode}" """
            select id,
                   case when id = 1 then ts else dt end,
                   coalesce(ts, dt)
            from timestamp_ns_mixed_datetime_coercion
            order by id
        """
        "order_qt_mixed_column_union_${mode}" """
            select value from (
                select ts as value from timestamp_ns_mixed_datetime_coercion
                union all
                select dt from timestamp_ns_mixed_datetime_coercion
            ) mixed_union
            order by value
        """

        for (def badValue : [
                "1677-09-21 00:12:43.145224",
                "2262-04-11 23:47:16.854776",
                "2500-01-01 00:00:00.000000"]) {
            test {
                sql """
                    select cast('1970-01-01 00:00:00' as timestamp_ns)
                        = cast('${badValue}' as datetimev2(6))
                """
                exception "outside Int64 epoch nanosecond range"
            }
        }
    }

    try {
        [false, true].each { mode -> checkCoercionMode(mode) }
    } finally {
        sql "set global enable_new_type_coercion_behavior = true"
    }
}
