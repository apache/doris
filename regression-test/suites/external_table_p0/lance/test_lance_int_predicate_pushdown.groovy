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

suite("test_lance_int_predicate_pushdown", "p0,external") {
    /*
     * Lance/Arrow types currently supported by predicate pushdown. Non-integer
     * types are exercised by test_lance_scalar_predicate_pushdown:
     *
     * | Lance / Arrow type | Doris type | Pushdown boundary |
     * |---|---|---|
     * | bool | boolean | Supported |
     * | int8 | tinyint | Supported |
     * | uint8 | smallint | Unsigned Substrait type variation |
     * | int16 | smallint | Supported |
     * | uint16 | int | Unsigned Substrait type variation |
     * | int32 | int | Supported |
     * | uint32 | bigint | Unsigned Substrait type variation |
     * | int64 | bigint | Supported |
     * | uint64 | largeint | Unsigned Substrait type variation |
     * | float32 | float | Supported |
     * | float64 | double | Supported |
     * | decimal128 | decimal | Precision 1-38 and scale 0-precision |
     * | utf8 / large_utf8 | text | LargeUtf8 uses Substrait large-container variation |
     * | date32(day) | date | Arrow Date with DAY unit only |
     * | timestamp(s/ms/us), no timezone | datetime(0/3/6) | Substrait PrecisionTimestamp |
     *
     * Other readable types, including float16, decimal256, binary, date64,
     * time, timestamp(ns), timezone-aware timestamp and nested types, currently
     * remain as BE residual predicates.
     *
     * Operators currently supported by predicate pushdown:
     *
     * | SQL predicate | Pushdown boundary |
     * |---|---|
     * | =, !=, <>, <, <=, >, >= | Direct column-to-literal comparison |
     * | <=> | Direct column-to-literal null-safe equality |
     * | IN, NOT IN | Non-empty literal list without NULL |
     * | IS NULL, IS NOT NULL | Direct column reference |
     * | AND | Top-level conjuncts can be pushed independently |
     * | OR | Pushed only when both branches are fully convertible |
     * | NOT | Pushed only when its operand is fully convertible |
     *
     * Every integer case below has two independent checks:
     *   1. EXPLAIN verifies whether the predicate is in lancePushdownPredicate
     *      and whether a regular BE predicate remains.
     *   2. qt_select verifies the end-to-end SQL result.
     *
     * Only row_id is projected. The filtered integer column is deliberately
     * non-projected so a missing native filter cannot be hidden by the result.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    sql """DROP CATALOG IF EXISTS test_lance_predicate_pushdown"""
    try {
        sql """
            CREATE CATALOG test_lance_predicate_pushdown PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        sql """ use test_lance_predicate_pushdown.doris;"""

        Closure verifyFullyPushedDown = { String query, String columnName ->
            explain {
                sql(query)
                contains "lancePushdownPredicate="
                contains columnName
                notContains "predicates:"
            }
        }

        Closure verifyIntegerPushdown = { String tableName, String columnName, Map values ->
            String typeName = columnName.substring(0, columnName.lastIndexOf("_"))

            // =
            String intEqQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} = 10 ORDER BY row_id; """
            verifyFullyPushedDown(intEqQuery, columnName)
            quickTest("select_${typeName}_eq", intEqQuery)

            // !=
            String intNeQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} != 10 ORDER BY row_id; """
            verifyFullyPushedDown(intNeQuery, columnName)
            quickTest("select_${typeName}_ne", intNeQuery)

            // <> is the SQL alias of !=.
            String intNeAliasQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <> 10 ORDER BY row_id; """
            verifyFullyPushedDown(intNeAliasQuery, columnName)
            quickTest("select_${typeName}_ne_alias", intNeAliasQuery)

            // <
            String intLtQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} < ${values.orderThreshold} ORDER BY row_id; """
            verifyFullyPushedDown(intLtQuery, columnName)
            quickTest("select_${typeName}_lt", intLtQuery)

            // <=
            String intLeQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <= ${values.orderThreshold} ORDER BY row_id; """
            verifyFullyPushedDown(intLeQuery, columnName)
            quickTest("select_${typeName}_le", intLeQuery)

            // >
            String intGtQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} > 10 ORDER BY row_id; """
            verifyFullyPushedDown(intGtQuery, columnName)
            quickTest("select_${typeName}_gt", intGtQuery)

            // >=
            String intGeQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} >= 10 ORDER BY row_id; """
            verifyFullyPushedDown(intGeQuery, columnName)
            quickTest("select_${typeName}_ge", intGeQuery)

            // Null-safe equality with a non-NULL literal.
            String intNullSafeEqQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <=> 10 ORDER BY row_id; """
            verifyFullyPushedDown(intNullSafeEqQuery, columnName)
            quickTest("select_${typeName}_null_safe_eq", intNullSafeEqQuery)

            // Null-safe equality with NULL.
            String intNullSafeEqNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <=> NULL ORDER BY row_id; """
            verifyFullyPushedDown(intNullSafeEqNullQuery, columnName)
            quickTest("select_${typeName}_null_safe_eq_null", intNullSafeEqNullQuery)

            // IN
            String intInQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IN (${values.inValues}) ORDER BY row_id; """
            verifyFullyPushedDown(intInQuery, columnName)
            quickTest("select_${typeName}_in", intInQuery)

            // NOT IN
            String intNotInQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} NOT IN (${values.inValues}) ORDER BY row_id; """
            verifyFullyPushedDown(intNotInQuery, columnName)
            quickTest("select_${typeName}_not_in", intNotInQuery)

            // IS NULL
            String intIsNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IS NULL ORDER BY row_id; """
            verifyFullyPushedDown(intIsNullQuery, columnName)
            quickTest("select_${typeName}_is_null", intIsNullQuery)

            // IS NOT NULL
            String intIsNotNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IS NOT NULL ORDER BY row_id; """
            verifyFullyPushedDown(intIsNotNullQuery, columnName)
            quickTest("select_${typeName}_is_not_null", intIsNotNullQuery)

            // AND: both top-level conjuncts are pushed.
            String intAndQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} >= ${values.andLower} AND ${columnName} <= 10 ORDER BY row_id; """
            verifyFullyPushedDown(intAndQuery, columnName)
            quickTest("select_${typeName}_and", intAndQuery)

            // OR: both branches are convertible, so the complete OR is pushed.
            String intOrQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} = ${values.orLeft} OR ${columnName} = 100 ORDER BY row_id; """
            verifyFullyPushedDown(intOrQuery, columnName)
            quickTest("select_${typeName}_or", intOrQuery)

            // NOT: the optimizer may normalize this to the inverse comparison.
            String intNotQuery = """ SELECT row_id FROM ${tableName} WHERE NOT (${columnName} < ${values.orderThreshold}) ORDER BY row_id; """
            verifyFullyPushedDown(intNotQuery, columnName)
            quickTest("select_${typeName}_not", intNotQuery)

            // Reversed operands are normalized before Substrait conversion.
            String intReversedQuery = """ SELECT row_id FROM ${tableName} WHERE 10 < ${columnName} ORDER BY row_id; """
            verifyFullyPushedDown(intReversedQuery, columnName)
            quickTest("select_${typeName}_reversed", intReversedQuery)

            // IN lists containing NULL remain as BE residual predicates to
            // preserve SQL three-valued logic.
            String intInWithNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IN (10, NULL) ORDER BY row_id; """
            explain {
                sql(intInWithNullQuery)
                notContains "lancePushdownPredicate="
                contains "predicates:"
                contains columnName
            }
            quickTest("select_${typeName}_in_with_null", intInWithNullQuery)

            // A function of a column is not a direct column-to-literal comparison.
            String intFunctionResidualQuery = """ SELECT row_id FROM ${tableName} WHERE coalesce(${columnName}, 0) = 10 ORDER BY row_id; """
            explain {
                sql(intFunctionResidualQuery)
                notContains "lancePushdownPredicate="
                contains "predicates:"
                contains "coalesce"
            }
            quickTest("select_${typeName}_function_residual", intFunctionResidualQuery)

            // A supported top-level conjunct is pushed independently while the
            // function expression remains as a BE residual predicate.
            String intPartialAndQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} >= 0 AND coalesce(${columnName}, 0) = 10 ORDER BY row_id; """
            explain {
                sql(intPartialAndQuery)
                check { explainString ->
                    String pushdown = explainString.readLines()
                            .find { line -> line.contains("lancePushdownPredicate=") }
                    String residual = explainString.readLines()
                            .find { line -> line.trim().startsWith("predicates:") }
                    return pushdown != null
                            && pushdown.contains(columnName)
                            && !pushdown.toLowerCase().contains("coalesce")
                            && residual != null
                            && residual.toLowerCase().contains("coalesce")
                            && !residual.contains(">=")
                }
            }
            quickTest("select_${typeName}_partial_and", intPartialAndQuery)

            // An OR is pushed only when both branches are convertible. This
            // complete expression must remain in BE.
            String intPartialOrQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} = 10 OR coalesce(${columnName}, 0) = 100 ORDER BY row_id; """
            explain {
                sql(intPartialOrQuery)
                notContains "lancePushdownPredicate="
                contains "predicates:"
                contains columnName
                contains "coalesce"
            }
            quickTest("select_${typeName}_partial_or", intPartialOrQuery)

            if (values.maximum != null) {
                // Values above the corresponding signed maximum verify unsigned ordering,
                // not only equality on the shared signed range.
                String intAboveSignedMaxQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} > ${values.signedMaximum} ORDER BY row_id; """
                verifyFullyPushedDown(intAboveSignedMaxQuery, columnName)
                quickTest("select_${typeName}_above_signed_max", intAboveSignedMaxQuery)

                String intMaximumQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} = ${values.maximum} ORDER BY row_id; """
                verifyFullyPushedDown(intMaximumQuery, columnName)
                quickTest("select_${typeName}_maximum", intMaximumQuery)
            }
        }

        Map signedValues = [
                orderThreshold: "0",
                inValues: "-100, 0, 100",
                andLower: "-1",
                orLeft: "-100"
        ]
        verifyIntegerPushdown("predicate_pushdown", "int8_value", signedValues)
        verifyIntegerPushdown("predicate_pushdown", "int16_value", signedValues)
        verifyIntegerPushdown("predicate_pushdown", "int32_value", signedValues)
        verifyIntegerPushdown("predicate_pushdown", "int64_value", signedValues)

        sql """ use test_lance_predicate_pushdown.`default`;"""
        verifyIntegerPushdown("all_types", "uint8_col", [
                orderThreshold: "10",
                inValues: "0, 10, 100",
                andLower: "1",
                orLeft: "0",
                signedMaximum: "127",
                maximum: "255"
        ])
        verifyIntegerPushdown("all_types", "uint16_col", [
                orderThreshold: "10",
                inValues: "0, 10, 100",
                andLower: "1",
                orLeft: "0",
                signedMaximum: "32767",
                maximum: "65535"
        ])
        verifyIntegerPushdown("all_types", "uint32_col", [
                orderThreshold: "10",
                inValues: "0, 10, 100",
                andLower: "1",
                orLeft: "0",
                signedMaximum: "2147483647",
                maximum: "4294967295"
        ])
        verifyIntegerPushdown("all_types", "uint64_col", [
                orderThreshold: "10",
                inValues: "0, 10, 100",
                andLower: "1",
                orLeft: "0",
                signedMaximum: "9223372036854775807",
                maximum: "18446744073709551615"
        ])
    } finally {
        // sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
