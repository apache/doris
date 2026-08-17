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

suite("test_lance_scalar_predicate_pushdown", "p0,external") {
    /*
     * This suite covers the non-integer scalar types accepted by
     * LancePredicateConverter:
     *
     * | Lance / Arrow type | Doris type | Operators exercised |
     * |---|---|---|
     * | bool | boolean | =, !=, <>, <=>, IN, NOT IN, IS NULL, IS NOT NULL, OR, NOT |
     * | float32 | float | All operators below |
     * | float64 | double | All operators below |
     * | decimal128 | decimal(18,2) | All operators below |
     * | utf8 | text | All operators below |
     * | large_utf8 | text | All operators below; Substrait large-container variation |
     * | date32(day) | date | All operators below |
     * | timestamp(s), no timezone | datetime | All operators below |
     * | timestamp(ms), no timezone | datetime(3) | All operators below |
     * | timestamp(us), no timezone | datetime(6) | All operators below |
     *
     * Ordered scalar types exercise =, !=, <>, <, <=, >, >=, <=>, IN,
     * NOT IN, IS NULL, IS NOT NULL, AND, OR, NOT and reversed operands.
     * Boolean deliberately omits ordering comparisons because Doris does not
     * define boolean ordering as part of this pushdown contract.
     *
     * Function expressions, partially convertible AND/OR expressions and IN
     * lists containing NULL remain covered by the integer suite. Those rules
     * are expression-shape restrictions and do not vary by scalar data type.
     *
     * Every query verifies both EXPLAIN and the end-to-end result. Only row_id
     * is projected, so the filtered Lance column remains non-projected.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_scalar_predicate_pushdown"

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
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

        sql """ USE `${catalogName}`.`doris`; """
        // Arrow Timestamp without timezone maps to Doris DATETIME without applying
        // the session timezone. Use +08:00 here to verify that pushdown is invariant.
        sql """ SET time_zone = '+08:00'; """

        Closure verifyFullyPushedDown = { String query, String columnName ->
            explain {
                sql(query)
                contains "lancePushdownPredicate="
                contains columnName
                notContains "predicates:"
            }
        }

        Closure verifyOrderedScalarPushdown = { String tableName, String typeName, String columnName, Map values ->
            String eqQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} = ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(eqQuery, columnName)
            quickTest("select_${typeName}_eq", eqQuery)

            String neQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} != ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(neQuery, columnName)
            quickTest("select_${typeName}_ne", neQuery)

            String neAliasQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <> ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(neAliasQuery, columnName)
            quickTest("select_${typeName}_ne_alias", neAliasQuery)

            String ltQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} < ${values.threshold} ORDER BY row_id; """
            verifyFullyPushedDown(ltQuery, columnName)
            quickTest("select_${typeName}_lt", ltQuery)

            String leQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <= ${values.threshold} ORDER BY row_id; """
            verifyFullyPushedDown(leQuery, columnName)
            quickTest("select_${typeName}_le", leQuery)

            String gtQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} > ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(gtQuery, columnName)
            quickTest("select_${typeName}_gt", gtQuery)

            String geQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} >= ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(geQuery, columnName)
            quickTest("select_${typeName}_ge", geQuery)

            String nullSafeEqQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <=> ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(nullSafeEqQuery, columnName)
            quickTest("select_${typeName}_null_safe_eq", nullSafeEqQuery)

            String nullSafeEqNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} <=> NULL ORDER BY row_id; """
            verifyFullyPushedDown(nullSafeEqNullQuery, columnName)
            quickTest("select_${typeName}_null_safe_eq_null", nullSafeEqNullQuery)

            String inQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IN (${values.inValues}) ORDER BY row_id; """
            verifyFullyPushedDown(inQuery, columnName)
            quickTest("select_${typeName}_in", inQuery)

            String notInQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} NOT IN (${values.inValues}) ORDER BY row_id; """
            verifyFullyPushedDown(notInQuery, columnName)
            quickTest("select_${typeName}_not_in", notInQuery)

            String isNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IS NULL ORDER BY row_id; """
            verifyFullyPushedDown(isNullQuery, columnName)
            quickTest("select_${typeName}_is_null", isNullQuery)

            String isNotNullQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} IS NOT NULL ORDER BY row_id; """
            verifyFullyPushedDown(isNotNullQuery, columnName)
            quickTest("select_${typeName}_is_not_null", isNotNullQuery)

            String andQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} >= ${values.lower} AND ${columnName} <= ${values.equal} ORDER BY row_id; """
            verifyFullyPushedDown(andQuery, columnName)
            quickTest("select_${typeName}_and", andQuery)

            String orQuery = """ SELECT row_id FROM ${tableName} WHERE ${columnName} = ${values.orLeft} OR ${columnName} = ${values.orRight} ORDER BY row_id; """
            verifyFullyPushedDown(orQuery, columnName)
            quickTest("select_${typeName}_or", orQuery)

            String notQuery = """ SELECT row_id FROM ${tableName} WHERE NOT (${columnName} < ${values.threshold}) ORDER BY row_id; """
            verifyFullyPushedDown(notQuery, columnName)
            quickTest("select_${typeName}_not", notQuery)

            String reversedQuery = """ SELECT row_id FROM ${tableName} WHERE ${values.equal} < ${columnName} ORDER BY row_id; """
            verifyFullyPushedDown(reversedQuery, columnName)
            quickTest("select_${typeName}_reversed", reversedQuery)
        }

        Closure verifyBooleanPushdown = {
            String boolEqQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value = true ORDER BY row_id; """
            verifyFullyPushedDown(boolEqQuery, "bool_value")
            quickTest("select_bool_eq", boolEqQuery)

            String boolNeQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value != true ORDER BY row_id; """
            verifyFullyPushedDown(boolNeQuery, "bool_value")
            quickTest("select_bool_ne", boolNeQuery)

            String boolNeAliasQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value <> true ORDER BY row_id; """
            verifyFullyPushedDown(boolNeAliasQuery, "bool_value")
            quickTest("select_bool_ne_alias", boolNeAliasQuery)

            String boolNullSafeEqQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value <=> true ORDER BY row_id; """
            verifyFullyPushedDown(boolNullSafeEqQuery, "bool_value")
            quickTest("select_bool_null_safe_eq", boolNullSafeEqQuery)

            String boolNullSafeEqNullQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value <=> NULL ORDER BY row_id; """
            verifyFullyPushedDown(boolNullSafeEqNullQuery, "bool_value")
            quickTest("select_bool_null_safe_eq_null", boolNullSafeEqNullQuery)

            String boolInQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value IN (true, false) ORDER BY row_id; """
            verifyFullyPushedDown(boolInQuery, "bool_value")
            quickTest("select_bool_in", boolInQuery)

            String boolNotInQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value NOT IN (true, false) ORDER BY row_id; """
            verifyFullyPushedDown(boolNotInQuery, "bool_value")
            quickTest("select_bool_not_in", boolNotInQuery)

            String boolIsNullQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value IS NULL ORDER BY row_id; """
            verifyFullyPushedDown(boolIsNullQuery, "bool_value")
            quickTest("select_bool_is_null", boolIsNullQuery)

            String boolIsNotNullQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value IS NOT NULL ORDER BY row_id; """
            verifyFullyPushedDown(boolIsNotNullQuery, "bool_value")
            quickTest("select_bool_is_not_null", boolIsNotNullQuery)

            String boolOrQuery = """ SELECT row_id FROM predicate_pushdown WHERE bool_value = false OR bool_value <=> NULL ORDER BY row_id; """
            verifyFullyPushedDown(boolOrQuery, "bool_value")
            quickTest("select_bool_or", boolOrQuery)

            String boolNotQuery = """ SELECT row_id FROM predicate_pushdown WHERE NOT (bool_value = true) ORDER BY row_id; """
            verifyFullyPushedDown(boolNotQuery, "bool_value")
            quickTest("select_bool_not", boolNotQuery)

            String boolReversedQuery = """ SELECT row_id FROM predicate_pushdown WHERE true = bool_value ORDER BY row_id; """
            verifyFullyPushedDown(boolReversedQuery, "bool_value")
            quickTest("select_bool_reversed", boolReversedQuery)
        }

        verifyBooleanPushdown()

        verifyOrderedScalarPushdown("predicate_pushdown", "float32", "float32_value", [
                equal: "10",
                threshold: "0",
                inValues: "-100, 0, 100",
                lower: "-1",
                orLeft: "-100",
                orRight: "100"
        ])
        verifyOrderedScalarPushdown("predicate_pushdown", "float64", "float64_value", [
                equal: "10",
                threshold: "0",
                inValues: "-100, 0, 100",
                lower: "-1",
                orLeft: "-100",
                orRight: "100"
        ])
        verifyOrderedScalarPushdown("predicate_pushdown", "decimal128", "decimal128_value", [
                equal: "10.00",
                threshold: "0.00",
                inValues: "-100.00, 0.00, 100.00",
                lower: "-1.00",
                orLeft: "-100.00",
                orRight: "100.00"
        ])
        verifyOrderedScalarPushdown("predicate_pushdown", "utf8", "utf8_value", [
                equal: "'ten-a'",
                threshold: "'one'",
                inValues: "'negative', 'one', 'ten-a'",
                lower: "'minimum'",
                orLeft: "''",
                orRight: "'maximum'"
        ])
        verifyOrderedScalarPushdown("`default`.`all_types`", "large_utf8", "large_utf8_col", [
                equal: "'ten-a'",
                threshold: "'one'",
                inValues: "'negative', 'one', 'ten-a'",
                lower: "'minimum'",
                orLeft: "''",
                orRight: "'maximum'"
        ])
        verifyOrderedScalarPushdown("predicate_pushdown", "date32", "date32_value", [
                equal: "DATE '2024-01-10'",
                threshold: "DATE '2024-01-01'",
                inValues: "DATE '1970-01-01', DATE '2024-01-01', DATE '2024-04-10'",
                lower: "DATE '2023-12-31'",
                orLeft: "DATE '1970-01-01'",
                orRight: "DATE '2024-04-10'"
        ])
        verifyOrderedScalarPushdown("`default`.`all_types`", "timestamp_s", "timestamp_s_col", [
                equal: "TIMESTAMP '2024-01-10 00:00:00'",
                threshold: "TIMESTAMP '2024-01-01 00:00:00'",
                inValues: "TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-04-10 00:00:00'",
                lower: "TIMESTAMP '2023-12-31 23:59:59'",
                orLeft: "TIMESTAMP '1970-01-01 00:00:00'",
                orRight: "TIMESTAMP '2024-04-10 00:00:00'"
        ])
        verifyOrderedScalarPushdown("`default`.`all_types`", "timestamp_ms", "timestamp_ms_col", [
                equal: "TIMESTAMP '2024-01-10 00:00:00.000'",
                threshold: "TIMESTAMP '2024-01-01 00:00:00.000'",
                inValues: "TIMESTAMP '1970-01-01 00:00:00.000', TIMESTAMP '2024-01-01 00:00:00.000', TIMESTAMP '2024-04-10 00:00:00.000'",
                lower: "TIMESTAMP '2023-12-31 23:59:59.000'",
                orLeft: "TIMESTAMP '1970-01-01 00:00:00.000'",
                orRight: "TIMESTAMP '2024-04-10 00:00:00.000'"
        ])
        verifyOrderedScalarPushdown("`default`.`all_types`", "timestamp_us", "timestamp_us_col", [
                equal: "TIMESTAMP '2024-01-10 00:00:00.000000'",
                threshold: "TIMESTAMP '2024-01-01 00:00:00.000000'",
                inValues: "TIMESTAMP '1970-01-01 00:00:00.000000', TIMESTAMP '2024-01-01 00:00:00.000000', TIMESTAMP '2024-04-10 00:00:00.000000'",
                lower: "TIMESTAMP '2023-12-31 23:59:59.000000'",
                orLeft: "TIMESTAMP '1970-01-01 00:00:00.000000'",
                orRight: "TIMESTAMP '2024-04-10 00:00:00.000000'"
        ])

        String timestampMsFractionalQuery = """ SELECT row_id FROM `default`.`all_types` WHERE timestamp_ms_col = TIMESTAMP '2026-07-28 12:34:56.123' ORDER BY row_id; """
        verifyFullyPushedDown(timestampMsFractionalQuery, "timestamp_ms_col")
        quickTest("select_timestamp_ms_fractional", timestampMsFractionalQuery)

        String timestampUsFractionalQuery = """ SELECT row_id FROM `default`.`all_types` WHERE timestamp_us_col = TIMESTAMP '2026-07-28 12:34:56.123456' ORDER BY row_id; """
        verifyFullyPushedDown(timestampUsFractionalQuery, "timestamp_us_col")
        quickTest("select_timestamp_us_fractional", timestampUsFractionalQuery)
    } finally {
        // Keep the catalog available for debugging failed external-environment runs.
    }
}
