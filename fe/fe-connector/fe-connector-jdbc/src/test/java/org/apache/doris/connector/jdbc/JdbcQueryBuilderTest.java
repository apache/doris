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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for {@link JdbcQueryBuilder}, focusing on LIMIT pushdown
 * correctness when filters are partially pushable.
 */
class JdbcQueryBuilderTest {

    private static final ConnectorType INT_TYPE = ConnectorType.of("INT");
    private static final ConnectorType STRING_TYPE = ConnectorType.of("VARCHAR");

    private static final String DB = "testdb";
    private static final String TABLE = "testtbl";

    private JdbcQueryBuilder mysqlBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.MYSQL);
    }

    private JdbcQueryBuilder oracleBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.ORACLE);
    }

    private JdbcQueryBuilder sqlserverBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.SQLSERVER);
    }

    private List<ConnectorColumnHandle> columns(String... names) {
        ConnectorColumnHandle[] cols = new ConnectorColumnHandle[names.length];
        for (int i = 0; i < names.length; i++) {
            cols[i] = new JdbcColumnHandle(names[i], names[i]);
        }
        return Arrays.asList(cols);
    }

    private ConnectorExpression simpleComparison(String col, int value) {
        return new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef(col, INT_TYPE),
                ConnectorLiteral.ofInt(value));
    }

    // A function call expression that cannot be pushed down (no functionConfig)
    private ConnectorExpression unpushableFunction(String col) {
        return new ConnectorComparison(
                ConnectorComparison.Operator.GT,
                new ConnectorFunctionCall("some_unknown_func",
                        INT_TYPE,
                        Collections.singletonList(new ConnectorColumnRef(col, INT_TYPE))),
                ConnectorLiteral.ofInt(0));
    }

    // --- LIMIT pushdown tests ---

    @Test
    void testLimitPushedWhenNoFilter() {
        JdbcQueryBuilder builder = mysqlBuilder();
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.empty(), 10);
        Assertions.assertTrue(sql.contains("LIMIT 10"),
                "LIMIT should be pushed when there is no filter. SQL: " + sql);
    }

    @Test
    void testLimitPushedWhenAllFiltersPushable() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE id = 1 AND name = 'test'
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("name", STRING_TYPE),
                        ConnectorLiteral.ofString("test"))));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 10);
        Assertions.assertTrue(sql.contains("LIMIT 10"),
                "LIMIT should be pushed when all filters are pushable. SQL: " + sql);
        Assertions.assertTrue(sql.contains("WHERE"),
                "WHERE clause should be present. SQL: " + sql);
    }

    @Test
    void testLimitNotPushedWhenPartialFiltersFail() {
        // No functionConfig → function calls cannot be pushed down
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE id = 1 AND some_unknown_func(name) > 0
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                unpushableFunction("name")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 10);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "LIMIT must NOT be pushed when some filters cannot be pushed. SQL: " + sql);
        // The pushable part should still be in WHERE
        Assertions.assertTrue(sql.contains("WHERE"),
                "Pushable filter should still appear in WHERE. SQL: " + sql);
    }

    @Test
    void testLimitNotPushedWhenAllFiltersUnpushable() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE some_unknown_func(id) > 0 (single non-pushable expression)
        ConnectorExpression filter = unpushableFunction("id");
        String sql = builder.buildQuery(DB, TABLE, columns("id"),
                Optional.of(filter), 10);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "LIMIT must NOT be pushed when no filters can be pushed. SQL: " + sql);
        Assertions.assertFalse(sql.contains("WHERE"),
                "WHERE should not appear since no filter was pushed. SQL: " + sql);
    }

    @Test
    void testNoLimitWhenLimitNegative() {
        JdbcQueryBuilder builder = mysqlBuilder();
        String sql = builder.buildQuery(DB, TABLE, columns("id"),
                Optional.empty(), -1);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "No LIMIT clause when limit is -1. SQL: " + sql);
    }

    @Test
    void testOracleRownumNotAddedWhenPartialFiltersFail() {
        JdbcQueryBuilder builder = oracleBuilder();
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                unpushableFunction("name")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 5);
        Assertions.assertFalse(sql.contains("ROWNUM"),
                "Oracle ROWNUM must NOT be added when filters are partial. SQL: " + sql);
    }

    @Test
    void testOracleRownumAddedWhenAllFiltersPushable() {
        JdbcQueryBuilder builder = oracleBuilder();
        ConnectorExpression filter = simpleComparison("id", 1);
        String sql = builder.buildQuery(DB, TABLE, columns("id"),
                Optional.of(filter), 5);
        Assertions.assertTrue(sql.contains("ROWNUM <= 5"),
                "Oracle ROWNUM should be added when all filters are pushable. SQL: " + sql);
    }

    @Test
    void testSqlServerTopNotAddedWhenPartialFiltersFail() {
        JdbcQueryBuilder builder = sqlserverBuilder();
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                unpushableFunction("name")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 5);
        Assertions.assertFalse(sql.contains("TOP"),
                "SQL Server TOP must NOT be added when filters are partial. SQL: " + sql);
    }

    // --- Basic query generation tests ---

    @Test
    void testSelectAllNoFilterNoLimit() {
        JdbcQueryBuilder builder = mysqlBuilder();
        String sql = builder.buildQuery(DB, TABLE, Collections.emptyList(),
                Optional.empty(), -1);
        Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl`", sql);
    }

    @Test
    void testSelectColumnsWithFilter() {
        JdbcQueryBuilder builder = mysqlBuilder();
        ConnectorExpression filter = simpleComparison("id", 42);
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("WHERE"),
                "Simple comparison should be pushed. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`id`") && sql.contains("`name`"),
                "Column names should be quoted. SQL: " + sql);
    }

    @Test
    void testNestedAndWithPartialFailure() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE (id = 1 AND name = 'x') AND unknownFunc(id) > 0
        // The first two are pushable, the third is not
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("name", STRING_TYPE),
                        ConnectorLiteral.ofString("x")),
                unpushableFunction("id")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 10);
        // Pushable filters should be present
        Assertions.assertTrue(sql.contains("WHERE"),
                "Pushable filters should still be in WHERE. SQL: " + sql);
        // LIMIT must NOT be pushed
        Assertions.assertFalse(sql.contains("LIMIT"),
                "LIMIT must NOT be pushed with partial filter failure. SQL: " + sql);
    }

    // -----------------------------------------------------------------------
    // OceanBase Oracle mode — verifies OCEANBASE_ORACLE uses ROWNUM syntax
    // -----------------------------------------------------------------------

    private JdbcQueryBuilder oceanBaseOracleBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.OCEANBASE_ORACLE);
    }

    @Test
    void testOceanBaseOracleModeUsesRownum() {
        JdbcQueryBuilder builder = oceanBaseOracleBuilder();
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.empty(), 10);
        // OCEANBASE_ORACLE should use ROWNUM for LIMIT, like Oracle
        Assertions.assertTrue(sql.contains("ROWNUM"),
                "OCEANBASE_ORACLE should use ROWNUM for LIMIT. SQL: " + sql);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "OCEANBASE_ORACLE must not use LIMIT keyword. SQL: " + sql);
    }

    @Test
    void testOceanBaseOracleModeLimitWithFilter() {
        JdbcQueryBuilder builder = oceanBaseOracleBuilder();
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(simpleComparison("id", 1)), 50);
        // Should have both ROWNUM and WHERE
        Assertions.assertTrue(sql.contains("ROWNUM"),
                "OCEANBASE_ORACLE LIMIT should use ROWNUM. SQL: " + sql);
        Assertions.assertTrue(sql.contains("WHERE"),
                "Filter should be in WHERE clause. SQL: " + sql);
    }

    // -----------------------------------------------------------------------
    // Column mapping isolation — verifies no state leaks between buildQuery calls
    // -----------------------------------------------------------------------

    @Test
    void testColumnMappingDoesNotLeakBetweenCalls() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // First call with columns that have different local→remote names
        JdbcColumnHandle remapped = new JdbcColumnHandle("local_col", "REMOTE_COL");
        String sql1 = builder.buildQuery(DB, TABLE,
                Collections.singletonList(remapped),
                Optional.of(new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("local_col", INT_TYPE),
                        ConnectorLiteral.ofInt(1))),
                -1);
        Assertions.assertTrue(sql1.contains("REMOTE_COL"),
                "First call should use remapped column name. SQL: " + sql1);

        // Second call with a different column set — must NOT see first call's mapping
        String sql2 = builder.buildQuery(DB, TABLE,
                columns("other_col"),
                Optional.of(new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("local_col", INT_TYPE),
                        ConnectorLiteral.ofInt(2))),
                -1);
        // "local_col" is not in the second call's column handles, so it falls back
        // to using the column name as-is (no remapping)
        Assertions.assertFalse(sql2.contains("REMOTE_COL"),
                "Second call must not see first call's column mapping. SQL: " + sql2);
    }

    // --- Oracle/OceanBase timestamp microsecond formatting tests ---

    private static final ConnectorType DATETIME_TYPE = ConnectorType.of("DATETIMEV2");

    @Test
    void testOracleTimestampLeadingZeroMicroseconds() {
        JdbcQueryBuilder builder = oracleBuilder();
        // 00:00:00.000001 → nanos=1000, micros=1 → must be "000001" not "1"
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 0, 1000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(".000001"),
                "Leading-zero microseconds must be preserved. SQL: " + sql);
        Assertions.assertTrue(sql.contains("FF6"),
                "Oracle timestamp must use FF6 format. SQL: " + sql);
    }

    @Test
    void testOracleTimestampNormalMicroseconds() {
        JdbcQueryBuilder builder = oracleBuilder();
        // 10:30:45.123456 → nanos=123456000, micros=123456
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(".123456"),
                "Normal microseconds must be preserved. SQL: " + sql);
    }

    @Test
    void testOracleTimestampTrailingZeroMicroseconds() {
        JdbcQueryBuilder builder = oracleBuilder();
        // 10:30:45.100000 → nanos=100000000, micros=100000
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 100000000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(".100000"),
                "Trailing-zero microseconds must be 6-digit padded. SQL: " + sql);
    }

    @Test
    void testOracleTimestampNoFractionalPart() {
        JdbcQueryBuilder builder = oracleBuilder();
        // 10:30:45.000000 → nanos=0 → should use to_date, no FF6
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 0);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("to_date("),
                "Zero fractional part should use to_date, not to_timestamp. SQL: " + sql);
        Assertions.assertFalse(sql.contains("FF6"),
                "Zero fractional part should not use FF6. SQL: " + sql);
    }

    @Test
    void testOceanBaseOracleTimestampLeadingZeroMicroseconds() {
        JdbcQueryBuilder builder = oceanBaseOracleBuilder();
        // Same bug affects OceanBase Oracle mode
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 0, 0, 0, 1000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(".000001"),
                "OceanBase Oracle mode must also pad microseconds. SQL: " + sql);
    }

    // --- Non-Oracle datetime fractional-seconds preservation tests ---

    @Test
    void testMysqlDatetimeFractionalSeconds() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.MYSQL);
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("10:30:45.123456"),
                "MySQL must preserve fractional seconds. SQL: " + sql);
    }

    @Test
    void testMysqlDatetimeNoFraction() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.MYSQL);
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 0);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("10:30:45'"),
                "MySQL datetime without fraction must not have trailing dot. SQL: " + sql);
    }

    @Test
    void testTrinoDatetimeFractionalSeconds() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.TRINO);
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("timestamp '2024-01-15 10:30:45.123456'"),
                "Trino must preserve fractional seconds. SQL: " + sql);
    }

    @Test
    void testSqlServerDatetimeFractionalSeconds() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.SQLSERVER);
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("10:30:45.123456"),
                "SQL Server must preserve fractional seconds. SQL: " + sql);
    }

    @Test
    void testPostgresqlDatetimeFractionalSeconds() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL);
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("10:30:45.123456"),
                "PostgreSQL must preserve fractional seconds. SQL: " + sql);
    }

    @Test
    void testClickhouseDatetimeFractionalSeconds() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.CLICKHOUSE);
        LocalDateTime dt = LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123456000);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", DATETIME_TYPE),
                ConnectorLiteral.ofDatetime(dt));
        String sql = builder.buildQuery(DB, TABLE, columns("ts"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("10:30:45.123456"),
                "ClickHouse must preserve fractional seconds. SQL: " + sql);
    }

    // ---- Boolean literal tests ----

    @Test
    void testMysqlBooleanLiteralTrue() {
        JdbcQueryBuilder builder = mysqlBuilder();
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(true));
        String sql = builder.buildQuery(DB, TABLE, columns("flag"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("TRUE"),
                "MySQL booleans should use TRUE/FALSE keywords. SQL: " + sql);
    }

    @Test
    void testPostgresqlBooleanLiteral() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(true));
        String sql = builder.buildQuery(DB, TABLE, columns("flag"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("TRUE"),
                "PostgreSQL booleans must use TRUE/FALSE keywords. SQL: " + sql);
    }

    @Test
    void testTrinoBooleanLiteralFalse() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.TRINO);
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(false));
        String sql = builder.buildQuery(DB, TABLE, columns("flag"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("FALSE"),
                "Trino booleans must use TRUE/FALSE keywords. SQL: " + sql);
    }

    @Test
    void testOracleBooleanLiteral() {
        JdbcQueryBuilder builder = oracleBuilder();
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(true));
        String sql = builder.buildQuery(DB, TABLE, columns("flag"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(" 1"),
                "Oracle booleans must use 1/0 integers. SQL: " + sql);
        Assertions.assertFalse(sql.contains("TRUE"),
                "Oracle must not render TRUE keyword. SQL: " + sql);
    }

    @Test
    void testSqlserverBooleanLiteral() {
        JdbcQueryBuilder builder = sqlserverBuilder();
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(false));
        String sql = builder.buildQuery(DB, TABLE, columns("flag"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(" 0"),
                "SQL Server booleans must use 1/0 integers. SQL: " + sql);
        Assertions.assertFalse(sql.contains("FALSE"),
                "SQL Server must not render FALSE keyword. SQL: " + sql);
    }

    @Test
    void testOceanbaseOracleBooleanLiteral() {
        JdbcQueryBuilder builder = oceanBaseOracleBuilder();
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("flag", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(true));
        String sql = builder.buildQuery(DB, TABLE, columns("flag"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains(" 1"),
                "OceanBase Oracle booleans must use 1/0 integers. SQL: " + sql);
    }
}
