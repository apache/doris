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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * Builds the one SQL statement a scan range asks its remote source to run.
 *
 * <p>The shape is fixed -- {@code SELECT cols FROM table [WHERE ...] [LIMIT n]} -- and every difference
 * between sources goes through {@link AdbcDialect}. Adding a source must not require touching this class;
 * a test drives it with a dialect defined inside the test to keep that honest.
 *
 * <p>Two rules here are not style, they decide whether the query returns the right rows:
 *
 * <ul>
 *   <li><b>The projection is always explicit.</b> BE matches the columns the source returns against the
 *       query's slots by name and rejects any column it did not ask for, so {@code SELECT *} fails the scan
 *       outright ({@code ADBC source returned unknown column}). This is the opposite of the JDBC connector,
 *       which selects {@code *} when it has no columns.</li>
 *   <li><b>A row limit is pushed only when every predicate was.</b> BE re-evaluates the predicates on
 *       whatever comes back, so a source that truncated to {@code n} rows BEFORE the predicates Doris still
 *       has to apply returns fewer rows than the query asked for. Pushing the predicates is otherwise pure
 *       speed-up, which is exactly why a predicate that cannot be translated is simply left behind.</li>
 * </ul>
 */
public final class AdbcQueryBuilder {

    private AdbcQueryBuilder() {
    }

    /** A generated statement, and whether the source will do all of the filtering. */
    public static final class BuiltQuery {

        private final String sql;
        private final boolean allFiltersPushed;

        BuiltQuery(String sql, boolean allFiltersPushed) {
            this.sql = sql;
            this.allFiltersPushed = allFiltersPushed;
        }

        public String getSql() {
            return sql;
        }

        /** Whether every conjunct became part of the {@code WHERE} clause. */
        public boolean isAllFiltersPushed() {
            return allFiltersPushed;
        }
    }

    /**
     * @param limit the row limit, or a non-positive value for none
     */
    public static BuiltQuery build(AdbcDialect dialect, AdbcTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter, long limit) {
        List<String> predicates = new ArrayList<>();
        boolean allFiltersPushed = true;
        if (filter.isPresent()) {
            allFiltersPushed = collectPredicates(dialect, filter.get(), predicates);
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(renderProjection(dialect, columns));
        sql.append(" FROM ").append(dialect.qualifiedTableName(handle));
        if (!predicates.isEmpty()) {
            sql.append(" WHERE (").append(String.join(") AND (", predicates)).append(')');
        }
        if (limit > 0 && allFiltersPushed && dialect.supportsLimitClause()) {
            sql.append(" LIMIT ").append(limit);
        }
        return new BuiltQuery(sql.toString(), allFiltersPushed);
    }

    /**
     * Renders the select list.
     *
     * <p>An empty column list is not "select everything": it is Doris pushing a {@code COUNT(*)} down, where
     * the scan needs row count and no values at all. {@code SELECT 1} asks for exactly that, one narrow
     * column per row, instead of dragging the full table width across the wire to be discarded. BE's
     * ADBC reader recognizes the no-columns-requested case and counts rows without materializing any.
     */
    private static String renderProjection(AdbcDialect dialect, List<ConnectorColumnHandle> columns) {
        StringJoiner joiner = new StringJoiner(", ");
        for (ConnectorColumnHandle column : columns) {
            joiner.add(dialect.quoteIdentifier(columnName(column)));
        }
        return joiner.length() == 0 ? "1" : joiner.toString();
    }

    private static String columnName(ConnectorColumnHandle column) {
        if (column instanceof NamedColumnHandle) {
            return ((NamedColumnHandle) column).getName();
        }
        throw new IllegalArgumentException(
                "An adbc scan received a column handle it did not create: " + column.getClass().getName());
    }

    /**
     * Translates the conjuncts it can and reports whether it got all of them.
     *
     * <p>Splitting only the top-level {@code AND} is deliberate: each conjunct is translated whole or
     * dropped whole, so a partly translated conjunct -- which would be a DIFFERENT predicate, not a weaker
     * one -- can never be emitted.
     */
    private static boolean collectPredicates(AdbcDialect dialect, ConnectorExpression filter,
            List<String> out) {
        List<ConnectorExpression> conjuncts = filter instanceof ConnectorAnd
                ? ((ConnectorAnd) filter).getConjuncts()
                : List.of(filter);
        boolean all = true;
        for (ConnectorExpression conjunct : conjuncts) {
            String rendered = render(dialect, conjunct);
            if (rendered == null) {
                all = false;
            } else {
                out.add(rendered);
            }
        }
        return all;
    }

    /**
     * Renders one expression, or returns {@code null} when this connector will not push it.
     *
     * <p>The accepted set is the conservative one from the design: comparisons, null tests, {@code IN}, and
     * the boolean connectives over them. Everything else -- functions, arithmetic, {@code LIKE},
     * {@code BETWEEN} -- is refused, because each is a claim that some unknown source spells and evaluates
     * it as Doris does, and a wrong claim here changes the result set rather than the speed.
     */
    private static String render(AdbcDialect dialect, ConnectorExpression expression) {
        if (expression instanceof ConnectorColumnRef) {
            return dialect.quoteIdentifier(((ConnectorColumnRef) expression).getColumnName());
        }
        if (expression instanceof ConnectorLiteral) {
            return dialect.renderLiteral((ConnectorLiteral) expression);
        }
        if (expression instanceof ConnectorComparison) {
            return renderComparison(dialect, (ConnectorComparison) expression);
        }
        if (expression instanceof ConnectorIsNull) {
            ConnectorIsNull isNull = (ConnectorIsNull) expression;
            String operand = render(dialect, isNull.getOperand());
            return operand == null ? null
                    : operand + (isNull.isNegated() ? " IS NOT NULL" : " IS NULL");
        }
        if (expression instanceof ConnectorIn) {
            return renderIn(dialect, (ConnectorIn) expression);
        }
        if (expression instanceof ConnectorAnd) {
            return renderConnective(dialect, ((ConnectorAnd) expression).getConjuncts(), " AND ");
        }
        if (expression instanceof ConnectorOr) {
            return renderConnective(dialect, ((ConnectorOr) expression).getDisjuncts(), " OR ");
        }
        if (expression instanceof ConnectorNot) {
            String operand = render(dialect, ((ConnectorNot) expression).getOperand());
            return operand == null ? null : "NOT (" + operand + ")";
        }
        return null;
    }

    private static String renderComparison(AdbcDialect dialect, ConnectorComparison comparison) {
        if (comparison.getOperator() == ConnectorComparison.Operator.EQ_FOR_NULL) {
            // Doris's null-safe equality has no portable spelling: standard SQL's IS NOT DISTINCT FROM is
            // not universally implemented, and every substitute (=, IS NULL OR =) differs from it on nulls.
            // Getting it wrong changes which rows match rather than failing, so it is not pushed at all.
            return null;
        }
        String left = render(dialect, comparison.getLeft());
        String right = render(dialect, comparison.getRight());
        if (left == null || right == null) {
            return null;
        }
        return left + " " + comparison.getOperator().getSymbol() + " " + right;
    }

    private static String renderIn(AdbcDialect dialect, ConnectorIn in) {
        String value = render(dialect, in.getValue());
        if (value == null || in.getInList().isEmpty()) {
            // An empty list has no SQL spelling; Doris folds it away long before a scan, so refusing it
            // costs nothing.
            return null;
        }
        StringJoiner items = new StringJoiner(", ");
        for (ConnectorExpression item : in.getInList()) {
            String rendered = render(dialect, item);
            if (rendered == null) {
                return null;
            }
            items.add(rendered);
        }
        return value + (in.isNegated() ? " NOT IN (" : " IN (") + items + ")";
    }

    /**
     * Renders {@code AND}/{@code OR} over its children, all or nothing.
     *
     * <p>{@code OR} obviously cannot drop a branch -- the remaining branches accept fewer rows than the
     * whole. Nested {@code AND} is treated the same way, because it is reached only from inside another
     * expression (under a {@code NOT}, or as an {@code OR} branch) where dropping a branch is not a
     * weakening either.
     */
    private static String renderConnective(AdbcDialect dialect, List<ConnectorExpression> operands,
            String separator) {
        if (operands.isEmpty()) {
            return null;
        }
        StringJoiner joiner = new StringJoiner(separator);
        for (ConnectorExpression operand : operands) {
            String rendered = render(dialect, operand);
            if (rendered == null) {
                return null;
            }
            joiner.add("(" + rendered + ")");
        }
        return joiner.toString();
    }
}
