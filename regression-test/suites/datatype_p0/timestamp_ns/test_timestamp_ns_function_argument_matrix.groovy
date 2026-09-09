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

suite("test_timestamp_ns_function_argument_matrix") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists timestamp_ns_function_argument_matrix"
    sql """
        create table timestamp_ns_function_argument_matrix (
            id int,
            case_name varchar(32),
            lhs timestamp_ns,
            rhs timestamp_ns,
            origin_value timestamp_ns,
            range_start timestamp_ns,
            range_end timestamp_ns,
            delta bigint,
            period_value int,
            time_value varchar(32),
            format_value varchar(64),
            from_zone varchar(32),
            to_zone varchar(32),
            weekday_value varchar(8)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_function_argument_matrix values
        (1, 'boundary_min',
            '1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.145224193',
            '1677-09-21 00:12:43.145224192',
            '1677-09-21 00:12:43.145224192', '1677-09-21 00:12:45.145224192',
            0, 1, '00:00:00.000000', '%Y-%m-%d %H:%i:%s.%n', '+08:00', '+08:00', 'MON'),
        (2, 'boundary_max',
            '2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854775806',
            '2262-04-11 23:47:16.854775807',
            '2262-04-11 23:47:14.854775807', '2262-04-11 23:47:16.854775807',
            0, 1, '00:00:00.000000', '%Y-%m-%d %H:%i:%s.%n', '+08:00', '+08:00', 'MON'),
        (3, 'epoch',
            '1970-01-01 00:00:00.000000000', '1969-12-31 23:59:59.999999999',
            '1970-01-01 00:00:00.000000000',
            '1969-12-31 23:59:59.000000000', '1970-01-01 00:00:01.000000000',
            1, 1, '00:00:00.000001', '%Y-%m-%d %H:%i:%s.%n', '+08:00', '+00:00', 'THU'),
        (4, 'normal',
            '2024-02-29 12:34:56.123456789', '2024-02-29 12:34:55.123456789',
            '2024-02-29 12:34:56.123456789',
            '2024-02-29 12:34:56.123456789', '2024-02-29 12:34:58.123456789',
            1, 1, '01:02:03.123456', '%Y-%m-%d %H:%i:%s.%n', '+08:00', '+00:00', 'MON')
    """

    def constantCases = [
        [id: 1, name: "boundary_min",
            lhs: "cast('1677-09-21 00:12:43.145224192' as timestamp_ns)",
            rhs: "cast('1677-09-21 00:12:43.145224193' as timestamp_ns)",
            origin: "cast('1677-09-21 00:12:43.145224192' as timestamp_ns)",
            rangeStart: "cast('1677-09-21 00:12:43.145224192' as timestamp_ns)",
            rangeEnd: "cast('1677-09-21 00:12:45.145224192' as timestamp_ns)",
            delta: "0", period: "1", time: "cast('00:00:00.000000' as time(6))",
            format: "'%Y-%m-%d %H:%i:%s.%n'", fromZone: "'+08:00'", toZone: "'+08:00'",
            weekday: "'MON'"],
        [id: 2, name: "boundary_max",
            lhs: "cast('2262-04-11 23:47:16.854775807' as timestamp_ns)",
            rhs: "cast('2262-04-11 23:47:16.854775806' as timestamp_ns)",
            origin: "cast('2262-04-11 23:47:16.854775807' as timestamp_ns)",
            rangeStart: "cast('2262-04-11 23:47:14.854775807' as timestamp_ns)",
            rangeEnd: "cast('2262-04-11 23:47:16.854775807' as timestamp_ns)",
            delta: "0", period: "1", time: "cast('00:00:00.000000' as time(6))",
            format: "'%Y-%m-%d %H:%i:%s.%n'", fromZone: "'+08:00'", toZone: "'+08:00'",
            weekday: "'MON'"],
        [id: 3, name: "epoch",
            lhs: "cast('1970-01-01 00:00:00.000000000' as timestamp_ns)",
            rhs: "cast('1969-12-31 23:59:59.999999999' as timestamp_ns)",
            origin: "cast('1970-01-01 00:00:00.000000000' as timestamp_ns)",
            rangeStart: "cast('1969-12-31 23:59:59.000000000' as timestamp_ns)",
            rangeEnd: "cast('1970-01-01 00:00:01.000000000' as timestamp_ns)",
            delta: "1", period: "1", time: "cast('00:00:00.000001' as time(6))",
            format: "'%Y-%m-%d %H:%i:%s.%n'", fromZone: "'+08:00'", toZone: "'+00:00'",
            weekday: "'THU'"],
        [id: 4, name: "normal",
            lhs: "cast('2024-02-29 12:34:56.123456789' as timestamp_ns)",
            rhs: "cast('2024-02-29 12:34:55.123456789' as timestamp_ns)",
            origin: "cast('2024-02-29 12:34:56.123456789' as timestamp_ns)",
            rangeStart: "cast('2024-02-29 12:34:56.123456789' as timestamp_ns)",
            rangeEnd: "cast('2024-02-29 12:34:58.123456789' as timestamp_ns)",
            delta: "1", period: "1", time: "cast('01:02:03.123456' as time(6))",
            format: "'%Y-%m-%d %H:%i:%s.%n'", fromZone: "'+08:00'", toZone: "'+00:00'",
            weekday: "'MON'"]
    ]

    def unaryFunctions = { value ->
        [
            "year(${value})", "century(${value})", "quarter(${value})", "month(${value})",
            "day(${value})", "dayofmonth(${value})", "dayofweek(${value})",
            "dayofyear(${value})", "weekday(${value})", "week(${value})",
            "weekofyear(${value})", "yearweek(${value})", "dayname(${value})",
            "monthname(${value})", "hour(${value})", "minute(${value})", "second(${value})",
            "microsecond(${value})", "nanosecond(${value})", "year_month(${value})",
            "day_hour(${value})", "day_minute(${value})", "day_second(${value})",
            "day_microsecond(${value})", "hour_minute(${value})", "hour_second(${value})",
            "hour_microsecond(${value})", "minute_second(${value})",
            "minute_microsecond(${value})", "second_microsecond(${value})",
            "date(${value})", "datev2(${value})", "to_date(${value})", "to_datev2(${value})",
            "timestamp(${value})", "time(${value})", "to_days(${value})", "to_seconds(${value})",
            "unix_timestamp(${value})", "second_timestamp(${value})",
            "millisecond_timestamp(${value})", "microsecond_timestamp(${value})",
            "last_day(${value})", "to_monday(${value})", "year_of_week(${value})",
            "yow(${value})", "time_to_sec(${value})", "to_iso8601(${value})",
            "cast(to_json(${value}) as string)"
        ]
    }

    def auxiliaryFunctions = { value, format, fromZone, toZone, weekday ->
        [
            "date_format(${value}, ${format})", "time_format(${value}, ${format})",
            "convert_tz(${value}, ${fromZone}, ${toZone})",
            "next_day(${value}, ${weekday})", "previous_day(${value}, ${weekday})",
            "week(${value}, 3)", "yearweek(${value}, 3)"
        ]
    }

    // DATE_FORMAT and TIME_FORMAT require a literal format. The other auxiliary arguments accept
    // columns and therefore also participate in the all-column matrix.
    def columnAuxiliaryFunctions = { value, fromZone, toZone, weekday, weekMode ->
        [
            "convert_tz(${value}, ${fromZone}, ${toZone})",
            "next_day(${value}, ${weekday})", "previous_day(${value}, ${weekday})",
            "week(${value}, ${weekMode})", "yearweek(${value}, ${weekMode})"
        ]
    }

    def arithmeticFunctions = { value, delta, timeValue ->
        def expressions = []
        ["nanoseconds", "microseconds", "milliseconds", "seconds", "minutes", "hours",
         "days", "weeks", "months", "quarters", "years"].each { unit ->
            expressions.add("${unit}_add(${value}, ${delta})")
            expressions.add("${unit}_sub(${value}, ${delta})")
        }
        expressions.add("add_time(${value}, ${timeValue})")
        expressions.add("sub_time(${value}, ${timeValue})")
        return expressions
    }

    // INTERVAL is SQL syntax rather than a column expression, so DATE_ADD/DATE_SUB have an
    // all-constant form and a temporal-column/literal-interval form only.
    def intervalFunctions = { value, delta ->
        [
            "date_add(${value}, interval ${delta} second)",
            "date_sub(${value}, interval ${delta} second)"
        ]
    }

    def binaryFunctions = { lhs, rhs ->
        def expressions = ["timediff(${lhs}, ${rhs})", "datediff(${lhs}, ${rhs})"]
        ["nanoseconds", "microseconds", "milliseconds", "seconds", "minutes", "hours",
         "days", "weeks", "months", "quarters", "years"].each { unit ->
            expressions.add("${unit}_diff(${lhs}, ${rhs})")
        }
        expressions.addAll([
            "months_between(${lhs}, ${rhs})", "months_between(${lhs}, ${rhs}, false)",
            "least(${lhs}, ${rhs})", "greatest(${lhs}, ${rhs})",
            "concat(${lhs}, '|', ${rhs})", "concat_ws('|', ${lhs}, ${rhs})"
        ])
        return expressions
    }

    def originFunctions = { value, period, origin ->
        def expressions = []
        ["year", "quarter", "month", "week", "day", "hour", "minute", "second"].each { unit ->
            expressions.add("${unit}_floor(${value}, ${period}, ${origin})")
            expressions.add("${unit}_ceil(${value}, ${period}, ${origin})")
        }
        return expressions
    }

    def rangeFunctions = { rangeStart, rangeEnd ->
        [
            "sequence(${rangeStart}, ${rangeEnd}, interval 1 second)",
            "array_range(${rangeStart}, ${rangeEnd}, interval 1 second)"
        ]
    }

    def collectionFunctions = { lhs, rhs ->
        [
            "array_min(array(${lhs}, ${rhs}))", "array_max(array(${lhs}, ${rhs}))",
            "array_contains(array(${lhs}, ${rhs}), ${rhs})",
            "array_position(array(${lhs}, ${rhs}), ${rhs})",
            "array_remove(array(${lhs}, ${rhs}), ${rhs})",
            "array_pushback(array(${lhs}), ${rhs})", "array_pushfront(array(${lhs}), ${rhs})",
            "countequal(array(${lhs}, ${rhs}), ${rhs})",
            "map_contains_key(map(${lhs}, 1), ${rhs})",
            "map_contains_value(map(1, ${lhs}), ${rhs})",
            "map_contains_entry(map(1, ${lhs}), 1, ${rhs})"
        ]
    }

    def joinExpressions = { expressions -> expressions.join(",\n                   ") }
    def constantSql = constantCases.collect { testCase ->
        def expressions = []
        expressions.addAll(unaryFunctions(testCase.lhs))
        expressions.addAll(auxiliaryFunctions(testCase.lhs, testCase.format, testCase.fromZone,
                testCase.toZone, testCase.weekday))
        expressions.addAll(arithmeticFunctions(testCase.lhs, testCase.delta, testCase.time))
        expressions.addAll(intervalFunctions(testCase.lhs, testCase.delta))
        expressions.addAll(binaryFunctions(testCase.lhs, testCase.rhs))
        expressions.add("field(${testCase.lhs}, ${testCase.rhs}, ${testCase.lhs})")
        expressions.addAll(originFunctions(testCase.lhs, testCase.period, testCase.origin))
        expressions.addAll(rangeFunctions(testCase.rangeStart, testCase.rangeEnd))
        expressions.addAll(collectionFunctions(testCase.lhs, testCase.rhs))
        return """
            select ${testCase.id} as id, '${testCase.name}' as case_name,
                   ${joinExpressions(expressions)}
        """
    }.join("\n        union all\n") + "\n        order by id"

    sql "set debug_skip_fold_constant = false"
    order_qt_function_all_constants_fold constantSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_all_constants_no_fold constantSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(constantSql)

    def mixedSql = constantCases.collect { testCase ->
        def expressions = []
        expressions.addAll(auxiliaryFunctions("lhs", testCase.format, testCase.fromZone,
                testCase.toZone, testCase.weekday))
        expressions.addAll(columnAuxiliaryFunctions(testCase.lhs, "from_zone", "to_zone",
                "weekday_value", "period_value"))
        expressions.addAll(arithmeticFunctions("lhs", testCase.delta, testCase.time))
        expressions.addAll(arithmeticFunctions(testCase.lhs, "delta",
                "cast(time_value as time(6))"))
        expressions.addAll(intervalFunctions("lhs", testCase.delta))
        expressions.addAll(binaryFunctions("lhs", testCase.rhs))
        expressions.addAll(binaryFunctions(testCase.lhs, "rhs"))
        // FIELD requires every search option after its first argument to be constant.
        expressions.add("field(lhs, ${testCase.rhs}, ${testCase.lhs})")
        expressions.addAll(originFunctions("lhs", "period_value", testCase.origin))
        expressions.addAll(originFunctions(testCase.lhs, "period_value", "origin_value"))
        expressions.addAll(rangeFunctions("range_start", testCase.rangeEnd))
        expressions.addAll(rangeFunctions(testCase.rangeStart, "range_end"))
        expressions.addAll(collectionFunctions("lhs", testCase.rhs))
        expressions.addAll(collectionFunctions(testCase.lhs, "rhs"))
        return """
            select id, case_name,
                   ${joinExpressions(expressions)}
            from timestamp_ns_function_argument_matrix
            where id = ${testCase.id}
        """
    }.join("\n        union all\n") + "\n        order by id"

    sql "set debug_skip_fold_constant = false"
    order_qt_function_mixed_arguments_fold mixedSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_mixed_arguments_no_fold mixedSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(mixedSql)

    def dateFloorCeilConstantsSql = """
        select
            date_ceil(cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                      interval 1 second),
            date_floor(cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
                       interval 1 second),
            date_floor(cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                       interval 1 second),
            date_ceil(cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                      interval 1 second),
            date_floor(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                       interval 1 second),
            date_ceil(cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                      interval 1 second),
            date_trunc(cast('2262-04-11 23:47:16.854775807' as timestamp_ns), 'second'),
            date_trunc(cast('1970-01-01 00:00:00.000000000' as timestamp_ns), 'second'),
            date_trunc(cast('2024-02-29 12:34:56.123456789' as timestamp_ns), 'second')
    """
    sql "set debug_skip_fold_constant = false"
    qt_function_date_floor_ceil_constants_fold dateFloorCeilConstantsSql
    sql "set debug_skip_fold_constant = true"
    qt_function_date_floor_ceil_constants_no_fold dateFloorCeilConstantsSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(dateFloorCeilConstantsSql)

    def dateFloorCeilMixedSql = """
        select id, case_name, 1 as variant,
               date_floor(lhs, interval 1 second),
               date_trunc(lhs, 'second')
        from timestamp_ns_function_argument_matrix
        where id in (2, 3, 4)
        union all
        select id, case_name, 2 as variant,
               date_ceil(lhs, interval 1 second),
               date_trunc(range_end, 'second')
        from timestamp_ns_function_argument_matrix
        where id in (1, 3, 4)
        order by id, variant
    """
    sql "set debug_skip_fold_constant = false"
    order_qt_function_date_floor_ceil_mixed_fold dateFloorCeilMixedSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_date_floor_ceil_mixed_no_fold dateFloorCeilMixedSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(dateFloorCeilMixedSql)

    def aggregateConstantSql = constantCases.collect { testCase ->
        """
            select ${testCase.id} as id, '${testCase.name}' as case_name,
                   min(${testCase.lhs}), max(${testCase.rhs}),
                   min_by(${testCase.lhs}, 1), max_by(${testCase.rhs}, 1),
                   array_sort(array_agg(${testCase.lhs})),
                   array_sort(collect_list(${testCase.lhs})),
                   array_sort(collect_set(${testCase.lhs})),
                   histogram(${testCase.lhs}), topn_array(${testCase.lhs}, 1),
                   array_sort(map_keys(map_agg(${testCase.lhs}, 1))),
                   array_sort(map_values(map_agg(1, ${testCase.lhs}))),
                   window_funnel(86400, 'default', ${testCase.lhs}, true),
                   window_funnel_v2(86400, 'default', ${testCase.lhs}, true),
                   topn_weighted(${testCase.lhs}, cast(1 as bigint), 1),
                   topn_weighted(${testCase.lhs}, cast(1 as bigint), 1, 100)[1]
            from timestamp_ns_function_argument_matrix
            where id = ${testCase.id}
        """
    }.join("\n        union all\n") + "\n        order by id"
    sql "set debug_skip_fold_constant = false"
    order_qt_function_aggregate_constants_fold aggregateConstantSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_aggregate_constants_no_fold aggregateConstantSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(aggregateConstantSql)

    def aggregateMixedSql = constantCases.collect { testCase ->
        """
            select ${testCase.id} as id, '${testCase.name}' as case_name,
                   min(lhs), max(${testCase.rhs}),
                   min_by(lhs, 1), max_by(${testCase.rhs}, id),
                   array_sort(array_agg(if(id % 2 = 0, lhs, ${testCase.lhs}))),
                   array_sort(collect_list(if(id % 2 = 0, lhs, ${testCase.lhs}))),
                   array_sort(collect_set(if(id % 2 = 0, lhs, ${testCase.lhs}))),
                   histogram(if(id % 2 = 0, lhs, ${testCase.lhs})),
                   topn_array(if(id % 2 = 0, lhs, ${testCase.lhs}), 1),
                   array_sort(map_keys(map_agg(lhs, 1))),
                   array_sort(map_keys(map_agg(${testCase.lhs}, id))),
                   array_sort(map_values(map_agg(1, lhs))),
                   array_sort(map_values(map_agg(id, ${testCase.lhs}))),
                   window_funnel(86400, 'default', lhs, true),
                   window_funnel(86400, 'default', ${testCase.lhs}, id = ${testCase.id}),
                   window_funnel_v2(86400, 'default', lhs, true),
                   window_funnel_v2(86400, 'default', ${testCase.lhs}, id = ${testCase.id}),
                   topn_weighted(lhs, cast(1 as bigint), 1),
                   topn_weighted(${testCase.lhs}, cast(id as bigint), 1),
                   topn_weighted(lhs, cast(1 as bigint), 1, 100)[1],
                   topn_weighted(${testCase.lhs}, cast(id as bigint), 1, 100)[1]
            from timestamp_ns_function_argument_matrix
            where id = ${testCase.id}
        """
    }.join("\n        union all\n") + "\n        order by id"
    sql "set debug_skip_fold_constant = false"
    order_qt_function_aggregate_mixed_fold aggregateMixedSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_aggregate_mixed_no_fold aggregateMixedSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(aggregateMixedSql)

    def windowConstantSql = """
        select id,
               lag(cast('1677-09-21 00:12:43.145224192' as timestamp_ns)) over(order by id),
               lead(cast('2262-04-11 23:47:16.854775807' as timestamp_ns)) over(order by id),
               first_value(cast('1970-01-01 00:00:00.000000000' as timestamp_ns))
                   over(order by id rows between unbounded preceding and current row),
               last_value(cast('2024-02-29 12:34:56.123456789' as timestamp_ns))
                   over(order by id rows between current row and unbounded following)
        from timestamp_ns_function_argument_matrix
        order by id
    """
    sql "set debug_skip_fold_constant = false"
    order_qt_function_window_constants_fold windowConstantSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_window_constants_no_fold windowConstantSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(windowConstantSql)

    def windowMixedSql = """
        select id,
               lag(if(id = 1, lhs,
                      cast('1677-09-21 00:12:43.145224192' as timestamp_ns))) over(order by id),
               lead(if(id = 2, lhs,
                       cast('2262-04-11 23:47:16.854775807' as timestamp_ns))) over(order by id),
               first_value(if(id = 3, lhs,
                              cast('1970-01-01 00:00:00.000000000' as timestamp_ns)))
                   over(order by id rows between unbounded preceding and current row),
               last_value(if(id = 4, lhs,
                             cast('2024-02-29 12:34:56.123456789' as timestamp_ns)))
                   over(order by id rows between current row and unbounded following)
        from timestamp_ns_function_argument_matrix
        order by id
    """
    sql "set debug_skip_fold_constant = false"
    order_qt_function_window_mixed_fold windowMixedSql
    sql "set debug_skip_fold_constant = true"
    order_qt_function_window_mixed_no_fold windowMixedSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(windowMixedSql)

    def columnExpressions = []
    columnExpressions.addAll(unaryFunctions("lhs"))
    columnExpressions.addAll(columnAuxiliaryFunctions("lhs", "from_zone", "to_zone",
            "weekday_value", "period_value"))
    columnExpressions.addAll(arithmeticFunctions("lhs", "delta", "cast(time_value as time(6))"))
    columnExpressions.addAll(binaryFunctions("lhs", "rhs"))
    columnExpressions.addAll(originFunctions("lhs", "period_value", "origin_value"))
    columnExpressions.addAll(rangeFunctions("range_start", "range_end"))
    columnExpressions.addAll(collectionFunctions("lhs", "rhs"))

    sql "set debug_skip_fold_constant = true"
    order_qt_function_all_columns """
        select id, case_name,
               ${joinExpressions(columnExpressions)}
        from timestamp_ns_function_argument_matrix
        order by id
    """

    // SEQUENCE_COUNT and SEQUENCE_MATCH require the event timestamp and conditions to be columns,
    // so their only valid argument shape is covered here.
    order_qt_function_aggregate_columns """
        select min(lhs), max(lhs), min_by(lhs, id), max_by(lhs, id),
               array_sort(array_agg(lhs)), array_sort(collect_list(lhs)),
               array_sort(collect_set(lhs)), histogram(lhs), topn_array(lhs, 4),
               array_sort(map_keys(map_agg(lhs, id))),
               array_sort(map_values(map_agg(id, lhs))),
               sequence_count('(?1)(?2)', lhs, id = 1, id = 3),
               sequence_match('(?1)(?2)', lhs, id = 1, id = 3),
               window_funnel(86400, 'default', lhs, id = 1, id = 3),
               window_funnel_v2(86400, 'default', lhs, id = 1, id = 3),
               topn_weighted(lhs, cast(id as bigint), 4),
               topn_weighted(lhs, cast(id as bigint), 4, 100)[1]
        from timestamp_ns_function_argument_matrix
    """

    order_qt_function_window_columns """
        select id, lag(lhs) over(order by id), lead(lhs) over(order by id),
               first_value(lhs) over(order by id rows between unbounded preceding and current row),
               last_value(lhs) over(order by id rows between current row and unbounded following)
        from timestamp_ns_function_argument_matrix
        order by id
    """
    sql "set debug_skip_fold_constant = false"

    // Truncating/flooring the minimum or ceiling the maximum cannot be represented by TIMESTAMP_NS.
    // Check both planner folding and BE column execution at the exact signed-nanosecond limits.
    for (def skipFoldConstant : [false, true]) {
        sql "set debug_skip_fold_constant = ${skipFoldConstant}"
        test {
            sql """
                select date_trunc(
                    cast('1677-09-21 00:12:43.145224192' as timestamp_ns), 'second')
            """
            exception "out of range"
        }
        test {
            sql """
                select second_ceil(
                    cast('2262-04-11 23:47:16.854775807' as timestamp_ns))
            """
            exception "out of range"
        }
        test {
            sql """
                select date_floor(
                    cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                    interval 1 second)
            """
            exception "out of range"
        }
        test {
            sql """
                select date_ceil(
                    cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
                    interval 1 second)
            """
            exception "out of range"
        }
    }
    sql "set debug_skip_fold_constant = true"
    for (def boundaryColumnSql : [
            "select date_trunc(lhs, 'second') "
                    + "from timestamp_ns_function_argument_matrix where id = 1",
            "select second_ceil(lhs) "
                    + "from timestamp_ns_function_argument_matrix where id = 2",
            "select date_floor(lhs, interval 1 second) "
                    + "from timestamp_ns_function_argument_matrix where id = 1",
            "select date_ceil(lhs, interval 1 second) "
                    + "from timestamp_ns_function_argument_matrix where id = 2"]) {
        test {
            sql boundaryColumnSql
            exception "out of range"
        }
    }
    sql "set debug_skip_fold_constant = false"
}
