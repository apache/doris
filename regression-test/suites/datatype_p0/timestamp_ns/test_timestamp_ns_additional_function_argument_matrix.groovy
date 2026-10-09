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

suite("test_timestamp_ns_additional_function_argument_matrix") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists timestamp_ns_additional_function_argument_matrix"
    sql """
        create table timestamp_ns_additional_function_argument_matrix (
            id int,
            case_name varchar(32),
            lhs timestamp_ns,
            rhs timestamp_ns,
            delta int,
            lhs_array array<timestamp_ns>,
            rhs_array array<timestamp_ns>,
            default_min timestamp_ns default '1677-09-21 00:12:43.145224192',
            default_max timestamp_ns default '2262-04-11 23:47:16.854775807',
            default_epoch timestamp_ns default '1970-01-01 00:00:00.000000000',
            default_normal timestamp_ns default '2024-02-29 12:34:56.123456789'
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_additional_function_argument_matrix
            (id, case_name, lhs, rhs, delta, lhs_array, rhs_array) values
        (1, 'boundary_min',
            '1677-09-21 00:12:43.145224192', '1677-09-21 00:12:43.145225192', 0,
            array(
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                cast('1677-09-21 00:12:43.145224193' as timestamp_ns),
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns)),
            array(
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                cast('1677-09-21 00:12:43.145224192' as timestamp_ns),
                cast('1677-09-21 00:12:43.145224194' as timestamp_ns))),
        (2, 'boundary_max',
            '2262-04-11 23:47:16.854775807', '2262-04-11 23:47:16.854774807', 0,
            array(
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
                cast('2262-04-11 23:47:16.854775806' as timestamp_ns),
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns)),
            array(
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
                cast('2262-04-11 23:47:16.854775807' as timestamp_ns),
                cast('2262-04-11 23:47:16.854775805' as timestamp_ns))),
        (3, 'epoch',
            '1970-01-01 00:00:00.000000000', '1969-12-31 23:59:59.999999000', 1,
            array(
                cast('1969-12-31 23:59:59.999999999' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000001' as timestamp_ns)),
            array(
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns),
                cast('1970-01-01 00:00:00.000000000' as timestamp_ns))),
        (4, 'normal',
            '2024-02-29 12:34:56.123456789', '2024-02-29 12:34:56.123455789', -1,
            array(
                cast('2024-02-29 12:34:56.123456788' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456790' as timestamp_ns)),
            array(
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns),
                cast('2024-02-29 12:34:56.123456789' as timestamp_ns)))
    """

    def constantCases = [
        [id: 1, name: "boundary_min",
            lhs: "cast('1677-09-21 00:12:43.145224192' as timestamp_ns)",
            rhs: "cast('1677-09-21 00:12:43.145225192' as timestamp_ns)", delta: "0"],
        [id: 2, name: "boundary_max",
            lhs: "cast('2262-04-11 23:47:16.854775807' as timestamp_ns)",
            rhs: "cast('2262-04-11 23:47:16.854774807' as timestamp_ns)", delta: "0"],
        [id: 3, name: "epoch",
            lhs: "cast('1970-01-01 00:00:00.000000000' as timestamp_ns)",
            rhs: "cast('1969-12-31 23:59:59.999999000' as timestamp_ns)", delta: "1"],
        [id: 4, name: "normal",
            lhs: "cast('2024-02-29 12:34:56.123456789' as timestamp_ns)",
            rhs: "cast('2024-02-29 12:34:56.123455789' as timestamp_ns)", delta: "-1"]
    ]

    def joinExpressions = { expressions -> expressions.join(",\n                   ") }

    def scalarFunctions = [
        [name: "extract",
            constant: { testCase, lhsArray, rhsArray ->
                ["extract(year from ${testCase.lhs})",
                 "extract(microsecond from ${testCase.lhs})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                def value = "if(delta >= 0, lhs, ${testCase.lhs})"
                ["extract(year from ${value})", "extract(microsecond from ${value})"]
            },
            columns: ["extract(year from lhs)", "extract(microsecond from lhs)"]],
        [name: "timestampadd",
            constant: { testCase, lhsArray, rhsArray ->
                ["timestampadd(microsecond, ${testCase.delta}, ${testCase.lhs})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["timestampadd(microsecond, ${testCase.delta}, lhs)",
                 "timestampadd(microsecond, delta, ${testCase.lhs})"]
            },
            columns: ["timestampadd(microsecond, delta, lhs)"]],
        [name: "timestampdiff",
            constant: { testCase, lhsArray, rhsArray ->
                ["timestampdiff(microsecond, ${testCase.rhs}, ${testCase.lhs})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["timestampdiff(microsecond, ${testCase.rhs}, lhs)",
                 "timestampdiff(microsecond, rhs, ${testCase.lhs})"]
            },
            columns: ["timestampdiff(microsecond, rhs, lhs)"]],
        [name: "array_count",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_count((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_count((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_count((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_count((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_enumerate",
            constant: { testCase, lhsArray, rhsArray -> ["array_enumerate(${lhsArray})"] },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_enumerate(${mixedLhsArray})", "array_enumerate(${mixedRhsArray})"]
            },
            columns: ["array_enumerate(lhs_array)"]],
        [name: "array_enumerate_uniq",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_enumerate_uniq(${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_enumerate_uniq(${mixedLhsArray}, ${constantRhsArray})",
                 "array_enumerate_uniq(${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_enumerate_uniq(lhs_array, rhs_array)"]],
        [name: "array_exists",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_exists((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_exists((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_exists((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_exists((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_filter",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_filter((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_filter((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_filter((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_filter((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_first",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_first((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_first((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_first((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_first((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_first_index",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_first_index((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_first_index((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_first_index((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_first_index((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_last",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_last((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_last((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_last((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_last((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_last_index",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_last_index((x, y) -> x >= y, ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_last_index((x, y) -> x >= y, ${mixedLhsArray}, ${constantRhsArray})",
                 "array_last_index((x, y) -> x >= y, ${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_last_index((x, y) -> x >= y, lhs_array, rhs_array)"]],
        [name: "array_map",
            constant: { testCase, lhsArray, rhsArray ->
                ["array_map((x, y) -> if(x >= y, x, y), ${lhsArray}, ${rhsArray})"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["array_map((x, y) -> if(x >= y, x, y), "
                         + "${mixedLhsArray}, ${constantRhsArray})",
                 "array_map((x, y) -> if(x >= y, x, y), "
                         + "${constantLhsArray}, ${mixedRhsArray})"]
            },
            columns: ["array_map((x, y) -> if(x >= y, x, y), lhs_array, rhs_array)"]],
        [name: "json_array",
            constant: { testCase, lhsArray, rhsArray ->
                ["cast(json_array(${testCase.lhs}, ${testCase.rhs}) as string)"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["cast(json_array(lhs, ${testCase.rhs}) as string)",
                 "cast(json_array(${testCase.lhs}, rhs) as string)"]
            },
            columns: ["cast(json_array(lhs, rhs) as string)"]],
        [name: "json_array_ignore_null",
            constant: { testCase, lhsArray, rhsArray ->
                ["cast(json_array_ignore_null(${testCase.lhs}, ${testCase.rhs}, null) as string)"]
            },
            mixed: { testCase, mixedLhsArray, mixedRhsArray, constantLhsArray, constantRhsArray ->
                ["cast(json_array_ignore_null(lhs, ${testCase.rhs}, null) as string)",
                 "cast(json_array_ignore_null(${testCase.lhs}, rhs, null) as string)"]
            },
            columns: ["cast(json_array_ignore_null(lhs, rhs) as string)"]]
    ]

    def buildConstantSql = { function ->
        constantCases.collect { testCase ->
            def lhsArray = "array(${testCase.lhs}, ${testCase.rhs}, ${testCase.lhs})"
            def rhsArray = "array(${testCase.rhs}, ${testCase.lhs}, ${testCase.rhs})"
            def expressions = function.constant(testCase, lhsArray, rhsArray)
            return """
                select ${testCase.id} as id, '${testCase.name}' as case_name,
                       ${joinExpressions(expressions)}
            """
        }.join("\n            union all\n") + "\n            order by id"
    }

    def buildMixedSql = { function ->
        constantCases.collect { testCase ->
            def constantLhsArray = "array(${testCase.lhs}, ${testCase.rhs}, ${testCase.lhs})"
            def constantRhsArray = "array(${testCase.rhs}, ${testCase.lhs}, ${testCase.rhs})"
            def mixedLhsArray = "array_concat(array_slice(lhs_array, 1, 2), " +
                    "array_slice(${constantLhsArray}, 3, 1))"
            def mixedRhsArray = "array_concat(array_slice(rhs_array, 1, 2), " +
                    "array_slice(${constantRhsArray}, 3, 1))"
            def expressions = function.mixed(testCase, mixedLhsArray, mixedRhsArray,
                    constantLhsArray, constantRhsArray)
            return """
                select id, case_name,
                       ${joinExpressions(expressions)}
                from timestamp_ns_additional_function_argument_matrix
                where id = ${testCase.id}
            """
        }.join("\n            union all\n") + "\n            order by id"
    }

    scalarFunctions.each { function ->
        def constantSql = buildConstantSql(function)
        sql "set debug_skip_fold_constant = false"
        "order_qt_${function.name}_constants_fold" constantSql
        sql "set debug_skip_fold_constant = true"
        "order_qt_${function.name}_constants_no_fold" constantSql
        sql "set debug_skip_fold_constant = false"
        testFoldConst(constantSql)

        def mixedSql = buildMixedSql(function)
        sql "set debug_skip_fold_constant = false"
        "order_qt_${function.name}_mixed_fold" mixedSql
        sql "set debug_skip_fold_constant = true"
        "order_qt_${function.name}_mixed_no_fold" mixedSql
        sql "set debug_skip_fold_constant = false"
        testFoldConst(mixedSql)

        sql "set debug_skip_fold_constant = true"
        "order_qt_${function.name}_columns" """
            select id, case_name,
                   ${joinExpressions(function.columns)}
            from timestamp_ns_additional_function_argument_matrix
            order by id
        """
        sql "set debug_skip_fold_constant = false"
    }

    def aggregateFunctions = [
        [name: "any_value",
            constant: { testCase -> ["any_value(${testCase.lhs})"] },
            mixed: { testCase ->
                ["any_value(if(delta >= 0, lhs, ${testCase.lhs}))"]
            },
            columns: ["any_value(lhs)"]],
        [name: "topn",
            constant: { testCase -> ["topn(${testCase.lhs}, 1)"] },
            mixed: { testCase -> ["topn(if(delta >= 0, lhs, ${testCase.lhs}), 1)"] },
            columns: ["topn(lhs, 1)"]],
        [name: "min_by",
            constant: { testCase -> ["min_by('${testCase.name}', ${testCase.lhs})"] },
            mixed: { testCase ->
                ["min_by(case_name, ${testCase.lhs})", "min_by('${testCase.name}', rhs)"]
            },
            columns: ["min_by(case_name, lhs)", "min_by(case_name, rhs)"]],
        [name: "max_by",
            constant: { testCase -> ["max_by('${testCase.name}', ${testCase.rhs})"] },
            mixed: { testCase ->
                ["max_by(case_name, ${testCase.lhs})", "max_by('${testCase.name}', rhs)"]
            },
            columns: ["max_by(case_name, lhs)", "max_by(case_name, rhs)"]]
    ]

    aggregateFunctions.each { function ->
        def constantSql = constantCases.collect { testCase ->
            """
                select ${testCase.id} as id, '${testCase.name}' as case_name,
                       ${joinExpressions(function.constant(testCase))}
            """
        }.join("\n            union all\n") + "\n            order by id"
        sql "set debug_skip_fold_constant = false"
        "order_qt_${function.name}_constants_fold" constantSql
        sql "set debug_skip_fold_constant = true"
        "order_qt_${function.name}_constants_no_fold" constantSql
        sql "set debug_skip_fold_constant = false"
        testFoldConst(constantSql)

        def mixedSql = constantCases.collect { testCase ->
            """
                select ${testCase.id} as id, '${testCase.name}' as case_name,
                       ${joinExpressions(function.mixed(testCase))}
                from timestamp_ns_additional_function_argument_matrix
                where id = ${testCase.id}
            """
        }.join("\n            union all\n") + "\n            order by id"
        sql "set debug_skip_fold_constant = false"
        "order_qt_${function.name}_mixed_fold" mixedSql
        sql "set debug_skip_fold_constant = true"
        "order_qt_${function.name}_mixed_no_fold" mixedSql
        sql "set debug_skip_fold_constant = false"
        testFoldConst(mixedSql)

        sql "set debug_skip_fold_constant = true"
        "order_qt_${function.name}_columns" """
            select id, ${joinExpressions(function.columns)}
            from timestamp_ns_additional_function_argument_matrix
            group by id
            order by id
        """
        sql "set debug_skip_fold_constant = false"
    }

    sql "set debug_skip_fold_constant = true"
    qt_min_by_order_key_columns """
        select min_by(case_name, lhs), min_by(case_name, rhs)
        from timestamp_ns_additional_function_argument_matrix
    """
    qt_max_by_order_key_columns """
        select max_by(case_name, lhs), max_by(case_name, rhs)
        from timestamp_ns_additional_function_argument_matrix
    """
    sql "set debug_skip_fold_constant = false"

    // DEFAULT only accepts a column reference. Its constants and mixed-argument shapes are
    // intentionally invalid SQL, so exercise every TIMESTAMP_NS value class through column
    // defaults under both constant-folding modes.
    def defaultSql = """
        select default(default_min), default(default_max),
               default(default_epoch), default(default_normal)
        from timestamp_ns_additional_function_argument_matrix
        order by id
        limit 1
    """
    sql "set debug_skip_fold_constant = false"
    qt_default_fold defaultSql
    sql "set debug_skip_fold_constant = true"
    qt_default_no_fold defaultSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(defaultSql)
}
