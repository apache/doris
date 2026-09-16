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

suite("test_timestamp_ns_expression_argument_matrix") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists timestamp_ns_expression_argument_matrix"
    sql """
        create table timestamp_ns_expression_argument_matrix (
            id int,
            case_name varchar(32),
            lhs timestamp_ns,
            rhs timestamp_ns,
            upper_value timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_expression_argument_matrix values
        (1, 'boundary_min',
            '1677-09-21 00:12:43.145224192',
            '1677-09-21 00:12:43.145224193',
            '1677-09-21 00:12:43.145224194'),
        (2, 'boundary_max',
            '2262-04-11 23:47:16.854775807',
            '2262-04-11 23:47:16.854775806',
            '2262-04-11 23:47:16.854775807'),
        (3, 'epoch',
            '1970-01-01 00:00:00.000000000',
            '1969-12-31 23:59:59.999999999',
            '1970-01-01 00:00:00.000000001'),
        (4, 'normal',
            '2024-02-29 12:34:56.123456789',
            '2024-02-29 12:34:56.123456000',
            '2024-02-29 12:34:57.123456789')
    """

    def constantCases = [
        [id: 1, name: "boundary_min",
            lhs: "cast('1677-09-21 00:12:43.145224192' as timestamp_ns)",
            rhs: "cast('1677-09-21 00:12:43.145224193' as timestamp_ns)",
            upper: "cast('1677-09-21 00:12:43.145224194' as timestamp_ns)"],
        [id: 2, name: "boundary_max",
            lhs: "cast('2262-04-11 23:47:16.854775807' as timestamp_ns)",
            rhs: "cast('2262-04-11 23:47:16.854775806' as timestamp_ns)",
            upper: "cast('2262-04-11 23:47:16.854775807' as timestamp_ns)"],
        [id: 3, name: "epoch",
            lhs: "cast('1970-01-01 00:00:00.000000000' as timestamp_ns)",
            rhs: "cast('1969-12-31 23:59:59.999999999' as timestamp_ns)",
            upper: "cast('1970-01-01 00:00:00.000000001' as timestamp_ns)"],
        [id: 4, name: "normal",
            lhs: "cast('2024-02-29 12:34:56.123456789' as timestamp_ns)",
            rhs: "cast('2024-02-29 12:34:56.123456000' as timestamp_ns)",
            upper: "cast('2024-02-29 12:34:57.123456789' as timestamp_ns)"]
    ]

    def allConstantExpressions = { lhs, rhs, upper ->
        [
            "${lhs} = ${rhs}", "${lhs} != ${rhs}",
            "${lhs} > ${rhs}", "${lhs} >= ${rhs}",
            "${lhs} < ${rhs}", "${lhs} <= ${rhs}", "${lhs} <=> ${rhs}",
            "${lhs} + ${rhs}", "${lhs} - ${rhs}", "${lhs} * ${rhs}",
            "${lhs} / ${rhs}", "${lhs} % ${rhs}",
            "${lhs} between ${rhs} and ${upper}",
            "${lhs} not between ${rhs} and ${upper}",
            "${lhs} in (${rhs}, ${upper})", "${lhs} not in (${rhs}, ${upper})",
            "(${lhs} > ${rhs} or ${lhs} = ${rhs})",
            "(${lhs} >= ${rhs} and ${lhs} <= ${upper})", "not (${lhs} <=> ${rhs})",
            "if(true, ${lhs}, ${rhs})", "ifnull(${lhs}, ${rhs})",
            "coalesce(${lhs}, ${rhs}, ${upper})", "nvl(${lhs}, ${rhs})",
            "nullif(${lhs}, ${rhs})", "greatest(${lhs}, ${rhs})", "least(${lhs}, ${rhs})",
            "case when ${lhs} >= ${rhs} then ${lhs} else ${rhs} end"
        ].join(",\n                   ")
    }

    def constantSql = constantCases.collect { testCase ->
        """
            select ${testCase.id} as id, '${testCase.name}' as case_name,
                   ${allConstantExpressions(testCase.lhs, testCase.rhs, testCase.upper)}
        """
    }.join("\n        union all\n") + "\n        order by id"

    sql "set debug_skip_fold_constant = false"
    order_qt_expression_all_constants_fold constantSql
    sql "set debug_skip_fold_constant = true"
    order_qt_expression_all_constants_no_fold constantSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(constantSql)

    def mixedSql = constantCases.collect { testCase ->
        """
            select id, case_name,
                   lhs = ${testCase.rhs}, ${testCase.lhs} = rhs,
                   lhs != ${testCase.rhs}, ${testCase.lhs} != rhs,
                   lhs > ${testCase.rhs}, ${testCase.lhs} > rhs,
                   lhs >= ${testCase.rhs}, ${testCase.lhs} >= rhs,
                   lhs < ${testCase.rhs}, ${testCase.lhs} < rhs,
                   lhs <= ${testCase.rhs}, ${testCase.lhs} <= rhs,
                   lhs <=> ${testCase.rhs}, ${testCase.lhs} <=> rhs,
                   lhs + ${testCase.rhs}, ${testCase.lhs} + rhs,
                   lhs - ${testCase.rhs}, ${testCase.lhs} - rhs,
                   lhs * ${testCase.rhs}, ${testCase.lhs} * rhs,
                   lhs / ${testCase.rhs}, ${testCase.lhs} / rhs,
                   lhs % ${testCase.rhs}, ${testCase.lhs} % rhs,
                   lhs between ${testCase.rhs} and ${testCase.upper},
                   ${testCase.lhs} between rhs and upper_value,
                   lhs not between ${testCase.rhs} and ${testCase.upper},
                   ${testCase.lhs} not between rhs and upper_value,
                   lhs in (${testCase.rhs}, ${testCase.upper}),
                   ${testCase.lhs} in (rhs, upper_value),
                   lhs not in (${testCase.rhs}, ${testCase.upper}),
                   ${testCase.lhs} not in (rhs, upper_value),
                   (lhs > ${testCase.rhs} or lhs = ${testCase.rhs}),
                   (${testCase.lhs} >= rhs and ${testCase.lhs} <= upper_value),
                   not (lhs <=> ${testCase.rhs}), not (${testCase.lhs} <=> rhs),
                   if(id % 2 = 0, lhs, ${testCase.rhs}),
                   if(id % 2 = 0, ${testCase.lhs}, rhs),
                   ifnull(lhs, ${testCase.rhs}), ifnull(${testCase.lhs}, rhs),
                   coalesce(lhs, ${testCase.rhs}, upper_value),
                   coalesce(${testCase.lhs}, rhs, ${testCase.upper}),
                   nvl(lhs, ${testCase.rhs}), nvl(${testCase.lhs}, rhs),
                   nullif(lhs, ${testCase.rhs}), nullif(${testCase.lhs}, rhs),
                   greatest(lhs, ${testCase.rhs}), greatest(${testCase.lhs}, rhs),
                   least(lhs, ${testCase.rhs}), least(${testCase.lhs}, rhs),
                   case when id % 2 = 0 then lhs else ${testCase.rhs} end,
                   case when id % 2 = 0 then ${testCase.lhs} else rhs end
            from timestamp_ns_expression_argument_matrix
            where id = ${testCase.id}
        """
    }.join("\n        union all\n") + "\n        order by id"

    sql "set debug_skip_fold_constant = false"
    order_qt_expression_mixed_arguments_fold mixedSql
    sql "set debug_skip_fold_constant = true"
    order_qt_expression_mixed_arguments_no_fold mixedSql
    sql "set debug_skip_fold_constant = false"
    testFoldConst(mixedSql)

    sql "set debug_skip_fold_constant = true"
    order_qt_expression_all_columns """
        select id, case_name,
               lhs = rhs, lhs != rhs, lhs > rhs, lhs >= rhs,
               lhs < rhs, lhs <= rhs, lhs <=> rhs,
               lhs + rhs, lhs - rhs, lhs * rhs, lhs / rhs, lhs % rhs,
               lhs between rhs and upper_value,
               lhs not between rhs and upper_value,
               lhs in (rhs, upper_value), lhs not in (rhs, upper_value),
               (lhs > rhs or lhs = rhs),
               (lhs >= rhs and lhs <= upper_value), not (lhs <=> rhs),
               if(id % 2 = 0, lhs, rhs), ifnull(lhs, rhs),
               coalesce(lhs, rhs, upper_value), nvl(lhs, rhs), nullif(lhs, rhs),
               greatest(lhs, rhs), least(lhs, rhs),
               case when id % 2 = 0 then lhs else rhs end
        from timestamp_ns_expression_argument_matrix
        order by id
    """
    sql "set debug_skip_fold_constant = false"
}
