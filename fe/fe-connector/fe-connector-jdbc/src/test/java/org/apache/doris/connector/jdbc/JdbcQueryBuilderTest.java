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

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorBetween;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLike;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

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
    private static final ConnectorType BOOLEAN_TYPE = ConnectorType.of("BOOLEAN");

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

    private void assertMysqlPredicate(ConnectorExpression filter, String expected) {
        Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl` WHERE (" + expected + ")",
                mysqlBuilder().buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(filter), -1));
    }

    @Test
    void testComparisonGroupsCompoundOperands() {
        ConnectorColumnRef a = new ConnectorColumnRef("a", BOOLEAN_TYPE);
        ConnectorColumnRef b = new ConnectorColumnRef("b", BOOLEAN_TYPE);
        ConnectorColumnRef c = new ConnectorColumnRef("c", BOOLEAN_TYPE);
        ConnectorExpression and = new ConnectorAnd(Arrays.asList(a, b));
        ConnectorExpression or = new ConnectorOr(Arrays.asList(a, b));
        for (ConnectorComparison.Operator op : ConnectorComparison.Operator.values()) {
            // For a=false, b=true, c=false, (a AND b)=c must not become a AND (b=c).
            assertMysqlPredicate(new ConnectorComparison(op, and, c),
                    "((`a`) AND (`b`)) " + op.getSymbol() + " `c`");
            assertMysqlPredicate(new ConnectorComparison(op, c, or),
                    "`c` " + op.getSymbol() + " ((`a`) OR (`b`))");
        }
        JdbcQueryBuilder postgres = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL);
        Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\""
                        + " WHERE (((\"a\") AND (\"b\")) = \"c\")",
                postgres.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                        new ConnectorComparison(ConnectorComparison.Operator.EQ, and, c)), -1));
    }

    @Test
    void testComparisonGroupsNotAndNestedComparison() {
        ConnectorColumnRef a = new ConnectorColumnRef("a", BOOLEAN_TYPE);
        ConnectorColumnRef b = new ConnectorColumnRef("b", BOOLEAN_TYPE);
        ConnectorExpression equal = new ConnectorComparison(ConnectorComparison.Operator.EQ, a, b);
        assertMysqlPredicate(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorNot(a), equal),
                "(NOT (`a`)) = (`a` = `b`)");
        assertMysqlPredicate(new ConnectorNot(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        new ConnectorOr(Arrays.asList(a, b)), ConnectorLiteral.ofBoolean(false))),
                "NOT (((`a`) OR (`b`)) = FALSE)");
    }

    @Test
    void testInGroupsValueAndListExpressions() {
        ConnectorColumnRef a = new ConnectorColumnRef("a", BOOLEAN_TYPE);
        ConnectorColumnRef b = new ConnectorColumnRef("b", BOOLEAN_TYPE);
        ConnectorExpression or = new ConnectorOr(Arrays.asList(a, b));
        for (boolean negated : Arrays.asList(false, true)) {
            assertMysqlPredicate(new ConnectorIn(or, Arrays.asList(new ConnectorNot(a),
                            ConnectorLiteral.ofNull(BOOLEAN_TYPE)), negated),
                    "((`a`) OR (`b`))" + (negated ? " NOT IN " : " IN ") + "((NOT (`a`)), NULL)");
        }
    }

    @Test
    void testIsNullGroupsNullableCompoundOperand() {
        ConnectorExpression and = new ConnectorAnd(Arrays.asList(
                new ConnectorColumnRef("a", BOOLEAN_TYPE), new ConnectorColumnRef("b", BOOLEAN_TYPE)));
        // With a=NULL and b=NULL, the whole AND is NULL and must satisfy IS NULL.
        assertMysqlPredicate(new ConnectorIsNull(and, false), "((`a`) AND (`b`)) IS NULL");
        assertMysqlPredicate(new ConnectorIsNull(and, true), "((`a`) AND (`b`)) IS NOT NULL");
    }

    @Test
    void testBetweenGroupsAllOperands() {
        ConnectorColumnRef a = new ConnectorColumnRef("a", BOOLEAN_TYPE);
        ConnectorColumnRef b = new ConnectorColumnRef("b", BOOLEAN_TYPE);
        ConnectorExpression between = new ConnectorBetween(new ConnectorOr(Arrays.asList(a, b)),
                new ConnectorAnd(Arrays.asList(a, b)), new ConnectorNot(a));
        assertMysqlPredicate(between, "((`a`) OR (`b`)) BETWEEN ((`a`) AND (`b`)) AND (NOT (`a`))");
    }

    @Test
    void testLikeGroupsBothOperands() {
        ConnectorColumnRef a = new ConnectorColumnRef("a", BOOLEAN_TYPE);
        ConnectorColumnRef b = new ConnectorColumnRef("b", BOOLEAN_TYPE);
        for (ConnectorLike.Operator op : ConnectorLike.Operator.values()) {
            assertMysqlPredicate(new ConnectorLike(op, new ConnectorOr(Arrays.asList(a, b)), new ConnectorNot(a)),
                    "((`a`) OR (`b`)) " + op.name() + " (NOT (`a`))");
        }
    }

    @Test
    void testArithmeticGroupsPredicateOperands() {
        ConnectorColumnRef a = new ConnectorColumnRef("a", BOOLEAN_TYPE);
        ConnectorColumnRef b = new ConnectorColumnRef("b", BOOLEAN_TYPE);
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(JdbcDbType.MYSQL, null, true);
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.MYSQL, config, Collections.emptyMap());
        ConnectorExpression sum = new ConnectorFunctionCall("+", INT_TYPE,
                Arrays.asList(new ConnectorAnd(Arrays.asList(a, b)), new ConnectorNot(a)));
        Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl`"
                        + " WHERE (((((`a`) AND (`b`)) + (NOT (`a`)))) = 1)",
                builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                        new ConnectorComparison(ConnectorComparison.Operator.EQ, sum, ConnectorLiteral.ofInt(1))), -1));
        ConnectorExpression bitNot = new ConnectorFunctionCall("~", INT_TYPE,
                Collections.singletonList(new ConnectorAnd(Arrays.asList(a, b))));
        Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl` WHERE (((~((`a`) AND (`b`)))) = 1)",
                builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                        new ConnectorComparison(ConnectorComparison.Operator.EQ, bitNot, ConnectorLiteral.ofInt(1))), -1));
    }

    @Test
    void testPreRenderedOperandIsGrouped() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(JdbcDbType.MYSQL, null, true);
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.MYSQL, config, Collections.emptyMap());
        ConnectorExpression fragment = new ConnectorFunctionCall("`a` OR `b`", BOOLEAN_TYPE, Collections.emptyList());
        Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl` WHERE ((`a` OR `b`) = FALSE)",
                builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(new ConnectorComparison(
                        ConnectorComparison.Operator.EQ, fragment, ConnectorLiteral.ofBoolean(false))), -1));
    }

    @Test
    void testEmptyInRemainsLocalWithoutLimit() {
        ConnectorColumnRef column = new ConnectorColumnRef("id", INT_TYPE);
        for (boolean negated : Arrays.asList(false, true)) {
            ConnectorExpression emptyIn = new ConnectorIn(column, Collections.emptyList(), negated);
            for (ConnectorExpression filter : Arrays.asList(emptyIn, new ConnectorNot(emptyIn),
                    new ConnectorOr(Arrays.asList(emptyIn, simpleComparison("id", 1))))) {
                Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl`",
                        mysqlBuilder().buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(filter), 1));
            }
            Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl` WHERE (`id` = 1)",
                    mysqlBuilder().buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                            new ConnectorAnd(Arrays.asList(emptyIn, simpleComparison("id", 1)))), 1));
        }
    }

    @Test
    void testFloatingPointLiteralsPreserveValue() {
        double[] values = {1e20, -1e20, 0x1.0p63, -0x1.0p63,
                Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -0.0, 0.0, 1.5};
        String[] expected = {"1.0E20", "-1.0E20", "9.223372036854776E18", "-9.223372036854776E18",
                "1.7976931348623157E308", "-1.7976931348623157E308", "4.9E-324", "-0.0", "0.0", "1.5"};
        ConnectorColumnRef column = new ConnectorColumnRef("d", ConnectorType.of("DOUBLE"));
        for (int i = 0; i < values.length; i++) {
            for (ConnectorComparison.Operator op : Arrays.asList(ConnectorComparison.Operator.EQ,
                    ConnectorComparison.Operator.LT, ConnectorComparison.Operator.GT)) {
                assertMysqlPredicate(new ConnectorComparison(op, column, ConnectorLiteral.ofDouble(values[i])),
                        "`d` " + op.getSymbol() + " " + expected[i]);
                assertMysqlPredicate(new ConnectorComparison(op, ConnectorLiteral.ofDouble(values[i]), column),
                        expected[i] + " " + op.getSymbol() + " `d`");
            }
            assertMysqlPredicate(new ConnectorIn(column,
                    Collections.singletonList(ConnectorLiteral.ofDouble(values[i])), false),
                    "`d` IN (" + expected[i] + ")");
        }
        assertMysqlPredicate(new ConnectorBetween(column, ConnectorLiteral.ofDouble(-1e20),
                ConnectorLiteral.ofDouble(1e20)), "`d` BETWEEN -1.0E20 AND 1.0E20");
        assertMysqlPredicate(new ConnectorComparison(ConnectorComparison.Operator.EQ, column,
                new ConnectorLiteral(ConnectorType.of("FLOAT"), 1e20)), "`d` = 1.0E20");
    }

    @Test
    void testFloatingPointNeighborsOfLongBoundsRoundTrip() {
        ConnectorColumnRef column = new ConnectorColumnRef("d", ConnectorType.of("DOUBLE"));
        for (double value : new double[] {Math.nextDown(0x1.0p63), Math.nextUp(0x1.0p63),
                Math.nextDown(-0x1.0p63), Math.nextUp(-0x1.0p63)}) {
            String sql = mysqlBuilder().buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                    new ConnectorComparison(ConnectorComparison.Operator.EQ, column,
                            ConnectorLiteral.ofDouble(value))), -1);
            String literal = sql.substring(sql.indexOf("`d` = ") + "`d` = ".length(), sql.length() - 1);
            Assertions.assertEquals(Double.doubleToRawLongBits(value),
                    Double.doubleToRawLongBits(Double.parseDouble(literal)), sql);
        }
    }

    @Test
    void testPostgresqlBigintComparisonUsesFloatingPointSemantics() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL);
        ConnectorColumnRef id = new ConnectorColumnRef("id", ConnectorType.of("BIGINT"));
        // A stripped CAST(id AS DOUBLE) must not turn this into a BIGINT/NUMERIC comparison.
        // The exponent text as NUMERIC is 9223372036854774800, not the original 9223372036854774784.
        ConnectorLiteral literal = ConnectorLiteral.ofDouble(0x1.fffffffffffffp62);
        String typed = "CAST('9.2233720368547748E18' AS DOUBLE PRECISION)";
        for (ConnectorComparison.Operator op : Arrays.asList(ConnectorComparison.Operator.EQ,
                ConnectorComparison.Operator.LT, ConnectorComparison.Operator.GT)) {
            Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (\"id\" "
                            + op.getSymbol() + " " + typed + ")",
                    builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                            new ConnectorComparison(op, id, literal)), -1));
            Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (" + typed
                            + " " + op.getSymbol() + " \"id\")",
                    builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                            new ConnectorComparison(op, literal, id)), -1));
        }
        for (boolean negated : Arrays.asList(false, true)) {
            Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (\"id\""
                            + (negated ? " NOT IN (" : " IN (") + typed + "))",
                    builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                            new ConnectorIn(id, Collections.singletonList(literal), negated)), -1));
        }
        Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (\"id\" BETWEEN "
                        + typed + " AND " + typed + ")",
                builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                        new ConnectorBetween(id, literal, literal)), -1));
    }

    @Test
    void testPostgresqlFloatingPointRangeAndSignedZero() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.POSTGRESQL);
        ConnectorColumnRef column = new ConnectorColumnRef("d", ConnectorType.of("DOUBLE"));
        double[] values = {1e20, -1e20, 0x1.0p63, -0x1.0p63, Double.MAX_VALUE,
                -Double.MAX_VALUE, Double.MIN_VALUE, -0.0, 0.0, 1.5};
        String[] expected = {"1.0E20", "-1.0E20", "9.223372036854776E18", "-9.223372036854776E18",
                "1.7976931348623157E308", "-1.7976931348623157E308", "4.9E-324", "-0.0", "0.0", "1.5"};
        for (int i = 0; i < values.length; i++) {
            Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (\"d\" = CAST('"
                            + expected[i] + "' AS DOUBLE PRECISION))",
                    builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(new ConnectorComparison(
                            ConnectorComparison.Operator.EQ, column, ConnectorLiteral.ofDouble(values[i]))), -1));
        }
    }

    @Test
    void testOracleFloatingPointRangeUsesBinaryDoubleLiterals() {
        double[] values = {1e200, -1e200, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MIN_VALUE, -0.0, 0.0, 1.5};
        String[] expected = {"1.0E200D", "-1.0E200D", "1.7976931348623157E308D",
                "-1.7976931348623157E308D", "4.9E-324D", "-0.0D", "0.0D", "1.5D"};
        ConnectorColumnRef column = new ConnectorColumnRef("d", ConnectorType.of("DOUBLE"));
        for (JdbcDbType db : Arrays.asList(JdbcDbType.ORACLE, JdbcDbType.OCEANBASE_ORACLE)) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(db);
            for (int i = 0; i < values.length; i++) {
                ConnectorLiteral literal = ConnectorLiteral.ofDouble(values[i]);
                for (ConnectorComparison.Operator op : Arrays.asList(ConnectorComparison.Operator.EQ,
                        ConnectorComparison.Operator.LT, ConnectorComparison.Operator.GT)) {
                    Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (\"d\" "
                                    + op.getSymbol() + " " + expected[i] + ")",
                            builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                                    new ConnectorComparison(op, column, literal)), -1));
                }
                Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE (\"d\" IN ("
                                + expected[i] + "))",
                        builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                                new ConnectorIn(column, Collections.singletonList(literal), false)), -1));
            }
            Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\""
                            + " WHERE (\"d\" BETWEEN -1.0E200D AND 1.0E200D)",
                    builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(new ConnectorBetween(
                            column, ConnectorLiteral.ofDouble(-1e200), ConnectorLiteral.ofDouble(1e200))), -1));
        }
    }

    @Test
    void testOracleNvlUsesTypedFloatingPointArgument() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(JdbcDbType.ORACLE, null, true);
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.ORACLE, config, Collections.emptyMap());
        ConnectorColumnRef score = new ConnectorColumnRef("SCORE", ConnectorType.of("DOUBLE"));
        ConnectorExpression nvl = new ConnectorFunctionCall("ifnull", score.getType(),
                Arrays.asList(score, ConnectorLiteral.ofDouble(0.0)));
        Assertions.assertEquals("SELECT * FROM \"testdb\".\"testtbl\" WHERE ((nvl(\"SCORE\", 0.0D)) < \"SCORE\")",
                builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                        new ConnectorComparison(ConnectorComparison.Operator.LT, nvl, score)), -1));
    }

    @Test
    void testNonFiniteLiteralsRemainLocalWithoutLimit() {
        ConnectorColumnRef column = new ConnectorColumnRef("d", ConnectorType.of("DOUBLE"));
        for (JdbcDbType db : Arrays.asList(JdbcDbType.MYSQL, JdbcDbType.POSTGRESQL,
                JdbcDbType.ORACLE, JdbcDbType.OCEANBASE_ORACLE, JdbcDbType.SQLSERVER)) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(db);
            for (double value : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                ConnectorLiteral literal = ConnectorLiteral.ofDouble(value);
                ConnectorExpression comparison = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        column, literal);
                ConnectorExpression in = new ConnectorIn(column, Collections.singletonList(literal), false);
                ConnectorExpression between = new ConnectorBetween(column, literal, ConnectorLiteral.ofDouble(1.0));
                for (ConnectorExpression filter : Arrays.asList(comparison, in, between,
                        new ConnectorNot(comparison), new ConnectorOr(Arrays.asList(comparison,
                                simpleComparison("id", 1))))) {
                    String sql = builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(filter), 1);
                    Assertions.assertFalse(sql.contains(" WHERE "), sql);
                    Assertions.assertFalse(sql.contains("LIMIT") || sql.contains("TOP") || sql.contains("ROWNUM"), sql);
                }
                String sql = builder.buildQuery(DB, TABLE, Collections.emptyList(), Optional.of(
                        new ConnectorAnd(Arrays.asList(comparison, simpleComparison("id", 1)))), 1);
                Assertions.assertTrue(sql.contains(" WHERE "), sql);
                Assertions.assertFalse(sql.contains("NaN") || sql.contains("Infinity"), sql);
                Assertions.assertFalse(sql.contains("LIMIT") || sql.contains("TOP") || sql.contains("ROWNUM"), sql);
            }
        }
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
    void testSqlserverBooleanTrueLiteral() {
        // SQL Server has no boolean literal and reads TRUE as a column name (issue #64464).
        JdbcQueryBuilder builder = sqlserverBuilder();
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("bit_value", ConnectorType.of("BOOLEAN")),
                ConnectorLiteral.ofBoolean(true));
        String sql = builder.buildQuery(DB, TABLE, columns("bit_value"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("[bit_value] = 1"),
                "SQL Server booleans must use 1/0 integers. SQL: " + sql);
        Assertions.assertFalse(sql.contains("TRUE"),
                "SQL Server must not render TRUE keyword. SQL: " + sql);
    }

    @Test
    void testSqlserverBooleanInList() {
        // `bit_value IN ('1', '0')` reaches the connector as an IN list of boolean literals; every
        // item goes through the same 1/0 rendering as a comparison.
        JdbcQueryBuilder builder = sqlserverBuilder();
        ConnectorExpression filter = new ConnectorIn(
                new ConnectorColumnRef("bit_value", ConnectorType.of("BOOLEAN")),
                Arrays.asList(ConnectorLiteral.ofBoolean(true), ConnectorLiteral.ofBoolean(false)),
                false);
        String sql = builder.buildQuery(DB, TABLE, columns("bit_value"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("[bit_value] IN (1, 0)"),
                "SQL Server boolean IN lists must use 1/0 integers. SQL: " + sql);
        Assertions.assertFalse(sql.contains("TRUE") || sql.contains("FALSE"),
                "SQL Server must not render TRUE/FALSE keywords. SQL: " + sql);
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

    @Test
    void testClickHouseUuidRangesRemainLocalWithoutRemoteLimit() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.CLICKHOUSE);
        ConnectorType uuidType = ConnectorType.of("UUID");
        ConnectorColumnRef column = new ConnectorColumnRef("u", uuidType);
        ConnectorLiteral literal = new ConnectorLiteral(uuidType, "80000000-0000-0000-0000-000000000000");
        for (ConnectorComparison.Operator op : Arrays.asList(ConnectorComparison.Operator.LT,
                ConnectorComparison.Operator.LE, ConnectorComparison.Operator.GT, ConnectorComparison.Operator.GE)) {
            for (ConnectorExpression right : Arrays.asList(literal, new ConnectorColumnRef("v", uuidType))) {
                ConnectorExpression range = new ConnectorComparison(op, column, right);
                for (ConnectorExpression filter : Arrays.asList(range, new ConnectorNot(range),
                        new ConnectorOr(Arrays.asList(range, simpleComparison("id", 1))),
                        new ConnectorComparison(op, right, column))) {
                    String sql = builder.buildQuery(DB, TABLE, columns("id", "u", "v"), Optional.of(filter), 1);
                    Assertions.assertFalse(sql.contains(" WHERE "), sql);
                    Assertions.assertFalse(sql.contains(" LIMIT "), sql);
                }
                String sql = builder.buildQuery(DB, TABLE, columns("id", "u", "v"),
                        Optional.of(new ConnectorAnd(Arrays.asList(range, simpleComparison("id", 1)))), 1);
                Assertions.assertTrue(sql.contains(" WHERE "), sql);
                Assertions.assertFalse(sql.contains(op.getSymbol()), sql);
                Assertions.assertFalse(sql.contains(" LIMIT "), sql);
            }
        }
        ConnectorExpression between = new ConnectorBetween(column, literal, literal);
        String sql = builder.buildQuery(DB, TABLE, columns("u"), Optional.of(between), 1);
        Assertions.assertFalse(sql.contains(" WHERE "), sql);
        Assertions.assertFalse(sql.contains(" LIMIT "), sql);
        for (ConnectorComparison.Operator op : Arrays.asList(ConnectorComparison.Operator.EQ,
                ConnectorComparison.Operator.NE)) {
            sql = builder.buildQuery(DB, TABLE, columns("u"),
                    Optional.of(new ConnectorComparison(op, column, literal)), 1);
            Assertions.assertTrue(sql.contains(" WHERE "), sql);
            Assertions.assertTrue(sql.contains(" LIMIT 1"), sql);
        }
    }

    @Test
    void trinoAndPrestoUuidPredicatesUseTypedLiterals() {
        ConnectorType type = ConnectorType.of("UUID");
        ConnectorColumnRef column = new ConnectorColumnRef("u", type);
        String value = "00112233-4455-6677-8899-aabbccddeeff";
        ConnectorLiteral literal = new ConnectorLiteral(type, value);
        String typed = "CAST('" + value + "' AS UUID)";
        for (JdbcDbType db : Arrays.asList(JdbcDbType.TRINO, JdbcDbType.PRESTO)) {
            JdbcQueryBuilder builder = new JdbcQueryBuilder(db);
            for (ConnectorComparison.Operator op : Arrays.asList(ConnectorComparison.Operator.EQ,
                    ConnectorComparison.Operator.NE, ConnectorComparison.Operator.LT, ConnectorComparison.Operator.LE,
                    ConnectorComparison.Operator.GT, ConnectorComparison.Operator.GE)) {
                String sql = builder.buildQuery(DB, TABLE, columns("u"), Optional.of(
                        new ConnectorComparison(op, column, literal)), 1);
                Assertions.assertTrue(sql.contains("\"u\" " + op.getSymbol() + " " + typed), sql);
                sql = builder.buildQuery(DB, TABLE, columns("u"), Optional.of(
                        new ConnectorComparison(op, literal, column)), 1);
                Assertions.assertTrue(sql.contains(typed + " " + op.getSymbol() + " \"u\""), sql);
            }
            String sql = builder.buildQuery(DB, TABLE, columns("u"), Optional.of(
                    new ConnectorIn(column, Arrays.asList(literal, literal), false)), 1);
            Assertions.assertTrue(sql.contains("IN (" + typed + ", " + typed + ")"), sql);
            sql = builder.buildQuery(DB, TABLE, columns("u"), Optional.of(
                    new ConnectorBetween(column, literal, literal)), 1);
            Assertions.assertTrue(sql.contains("BETWEEN " + typed + " AND " + typed), sql);
        }
    }

    @Test
    void sqlServerUuidRangesUseDorisOrdering() {
        JdbcQueryBuilder builder = new JdbcQueryBuilder(JdbcDbType.SQLSERVER);
        ConnectorType type = ConnectorType.of("UUID");
        ConnectorColumnRef column = new ConnectorColumnRef("u", type);
        ConnectorLiteral literal = new ConnectorLiteral(type, "80000000-0000-0000-0000-000000000000");
        String sql = builder.buildQuery(DB, TABLE, columns("u"), Optional.of(
                new ConnectorComparison(ConnectorComparison.Operator.GE, column, literal)), 1);
        Assertions.assertFalse(sql.contains(" WHERE "), sql);
        Assertions.assertFalse(sql.contains("TOP"), sql);
    }

}
