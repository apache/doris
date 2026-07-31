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
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLike;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Converts {@link ConnectorExpression} trees into SQL WHERE clauses
 * with database-specific formatting.
 *
 * <p>Handles column quoting, date literal formatting, LIMIT syntax
 * differences, function pushdown validation, function name rewriting,
 * and time arithmetic conversion across supported JDBC database types.</p>
 */
public final class JdbcQueryBuilder {

    private static final Logger LOG = LogManager.getLogger(JdbcQueryBuilder.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Formatter that preserves fractional seconds (up to microsecond precision).
     * Used for non-Oracle paths where the previous JDBC scan path emitted
     * {@code expr.getStringValue()}, keeping DATETIMEV2 literals intact.
     */
    private static final DateTimeFormatter DATETIME_FRAC_FMT;

    static {
        DATETIME_FRAC_FMT = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .toFormatter();
    }

    private final JdbcDbType dbType;
    private final JdbcFunctionPushdownConfig functionConfig;
    private final boolean clickhouseQueryFinal;
    private final boolean oracleNullPredicatePushDown;

    public JdbcQueryBuilder(JdbcDbType dbType) {
        this(dbType, null, Collections.emptyMap());
    }

    /**
     * Creates a query builder with full pushdown configuration.
     *
     * @param dbType            the JDBC database type
     * @param functionConfig    function pushdown rules (null disables function pushdown)
     * @param sessionProperties session variables map for query modifiers
     */
    public JdbcQueryBuilder(JdbcDbType dbType,
            JdbcFunctionPushdownConfig functionConfig,
            Map<String, String> sessionProperties) {
        this.dbType = dbType;
        this.functionConfig = functionConfig;
        this.clickhouseQueryFinal = Boolean.parseBoolean(
                sessionProperties.getOrDefault("jdbc_clickhouse_query_final", "false"));
        this.oracleNullPredicatePushDown = Boolean.parseBoolean(
                sessionProperties.getOrDefault("enable_jdbc_oracle_null_predicate_push_down", "false"));
    }

    /**
     * Builds a complete SELECT query from table coordinates, columns, filter, and limit.
     *
     * @param remoteDbName    the remote database name
     * @param remoteTableName the remote table name
     * @param columns         the columns to select (empty = SELECT *)
     * @param filter          the optional filter expression
     * @param limit           the row limit, or -1 for no limit
     * @return the complete SQL query string
     */
    public String buildQuery(String remoteDbName, String remoteTableName,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter, long limit) {
        // Build local→remote column name mapping from column handles
        Map<String, String> colMapping = new HashMap<>();
        for (ConnectorColumnHandle col : columns) {
            if (col instanceof JdbcColumnHandle) {
                JdbcColumnHandle jch = (JdbcColumnHandle) col;
                colMapping.put(jch.getLocalName(), jch.getRemoteName());
            }
        }

        List<String> filterClauses = new ArrayList<>();
        boolean allFiltersCollected = true;
        if (filter.isPresent()) {
            allFiltersCollected = collectFilters(filter.get(), filterClauses, colMapping);
        }

        StringBuilder sql = new StringBuilder("SELECT ");

        // Oracle uses ROWNUM in WHERE clause for TOP-N
        if (shouldPushDownLimit(limit, allFiltersCollected, filter)
                && (dbType == JdbcDbType.ORACLE || dbType == JdbcDbType.OCEANBASE_ORACLE)) {
            filterClauses.add("ROWNUM <= " + limit);
        }

        // SQL Server uses SELECT TOP N
        if (shouldPushDownLimit(limit, allFiltersCollected, filter) && dbType == JdbcDbType.SQLSERVER) {
            sql.append("TOP ").append(limit).append(" ");
        }

        // Column list
        if (columns.isEmpty()) {
            sql.append("*");
        } else {
            StringJoiner colJoiner = new StringJoiner(", ");
            for (ConnectorColumnHandle col : columns) {
                if (col instanceof JdbcColumnHandle) {
                    colJoiner.add(JdbcIdentifierQuoter.quoteRemoteIdentifier(
                            dbType, ((JdbcColumnHandle) col).getRemoteName()));
                }
            }
            String colStr = colJoiner.toString();
            sql.append(colStr.isEmpty() ? "*" : colStr);
        }

        // FROM clause
        sql.append(" FROM ");
        sql.append(JdbcIdentifierQuoter.quoteFullTableName(dbType, remoteDbName, remoteTableName));

        // WHERE clause
        if (!filterClauses.isEmpty()) {
            sql.append(" WHERE (");
            sql.append(String.join(") AND (", filterClauses));
            sql.append(")");
        }

        // LIMIT clause (MySQL, PostgreSQL, ClickHouse, etc.)
        if (shouldPushDownLimit(limit, allFiltersCollected, filter) && supportsLimitClause()) {
            sql.append(" LIMIT ").append(limit);
        }

        // ClickHouse FINAL: ensures reads from ReplacingMergeTree see deduplicated rows
        if (clickhouseQueryFinal && dbType == JdbcDbType.CLICKHOUSE) {
            sql.append(" SETTINGS final = 1");
        }

        return sql.toString();
    }

    private boolean shouldPushDownLimit(long limit, boolean allFiltersCollected,
            Optional<ConnectorExpression> filter) {
        if (limit <= 0) {
            return false;
        }
        // Only push down limit when all filters were successfully converted.
        // If any filter was dropped (evaluated locally), pushing LIMIT to remote
        // would cause remote to truncate rows before local filtering, producing
        // fewer results than expected.
        if (!filter.isPresent()) {
            return true;
        }
        return allFiltersCollected;
    }

    private boolean supportsLimitClause() {
        switch (dbType) {
            case MYSQL:
            case POSTGRESQL:
            case CLICKHOUSE:
            case SAP_HANA:
            case TRINO:
            case PRESTO:
            case OCEANBASE:
            case GBASE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Recursively collects individual filter clauses from a ConnectorExpression.
     * Top-level AND nodes are flattened into separate clauses.
     * Applies conjunct-level guards (e.g. Oracle NULL literal exclusion).
     *
     * @return true if all expressions were successfully converted to SQL,
     *         false if any sub-expression was dropped (not pushed down)
     */
    private boolean collectFilters(ConnectorExpression expr, List<String> clauses,
            Map<String, String> colMapping) {
        if (expr instanceof ConnectorAnd) {
            ConnectorAnd and = (ConnectorAnd) expr;
            boolean allCollected = true;
            for (ConnectorExpression child : and.getChildren()) {
                if (!collectFilters(child, clauses, colMapping)) {
                    allCollected = false;
                }
            }
            return allCollected;
        } else {
            if (!shouldPushDownExpression(expr)) {
                return false;
            }
            String sql = expressionToSql(expr, colMapping);
            if (sql != null && !sql.isEmpty()) {
                clauses.add(sql);
                return true;
            }
            return false;
        }
    }

    /**
     * Determines whether a single conjunct should be pushed down.
     * Mirrors the old JdbcScanNode.shouldPushDownConjunct() guards.
     */
    private boolean shouldPushDownExpression(ConnectorExpression expr) {
        // Guard: Oracle NULL literal exclusion
        if (!oracleNullPredicatePushDown
                && (dbType == JdbcDbType.ORACLE || dbType == JdbcDbType.OCEANBASE_ORACLE)
                && containsNullLiteral(expr)) {
            return false;
        }

        // Guard: function-containing expressions need DB-type support
        if (containsFunctionCall(expr)) {
            if (functionConfig == null) {
                return false;
            }
            // Only MySQL, ClickHouse, Oracle have function pushdown support
            switch (dbType) {
                case MYSQL:
                case CLICKHOUSE:
                case ORACLE:
                case OCEANBASE_ORACLE:
                    return functionConfig != null;
                default:
                    return false;
            }
        }

        return true;
    }

    private static boolean containsNullLiteral(ConnectorExpression expr) {
        if (expr instanceof ConnectorLiteral && ((ConnectorLiteral) expr).isNull()) {
            return true;
        }
        for (ConnectorExpression child : expr.getChildren()) {
            if (containsNullLiteral(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsFunctionCall(ConnectorExpression expr) {
        if (expr instanceof ConnectorFunctionCall) {
            return true;
        }
        for (ConnectorExpression child : expr.getChildren()) {
            if (containsFunctionCall(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts a single ConnectorExpression into a SQL string.
     * Returns null if the expression cannot be converted.
     */
    private String expressionToSql(ConnectorExpression expr, Map<String, String> colMapping) {
        if (expr instanceof ConnectorComparison) {
            return comparisonToSql((ConnectorComparison) expr, colMapping);
        } else if (expr instanceof ConnectorAnd) {
            return andToSql((ConnectorAnd) expr, colMapping);
        } else if (expr instanceof ConnectorOr) {
            return orToSql((ConnectorOr) expr, colMapping);
        } else if (expr instanceof ConnectorNot) {
            return notToSql((ConnectorNot) expr, colMapping);
        } else if (expr instanceof ConnectorIn) {
            return inToSql((ConnectorIn) expr, colMapping);
        } else if (expr instanceof ConnectorIsNull) {
            return isNullToSql((ConnectorIsNull) expr, colMapping);
        } else if (expr instanceof ConnectorLike) {
            return likeToSql((ConnectorLike) expr, colMapping);
        } else if (expr instanceof ConnectorBetween) {
            return betweenToSql((ConnectorBetween) expr, colMapping);
        } else if (expr instanceof ConnectorFunctionCall) {
            return functionCallToSql((ConnectorFunctionCall) expr, colMapping);
        } else if (expr instanceof ConnectorColumnRef) {
            return columnToSql((ConnectorColumnRef) expr, colMapping);
        } else if (expr instanceof ConnectorLiteral) {
            return literalToSql((ConnectorLiteral) expr);
        }
        return null;
    }

    private String comparisonToSql(ConnectorComparison comp, Map<String, String> colMapping) {
        String left = expressionToSql(comp.getLeft(), colMapping);
        String right = expressionToSql(comp.getRight(), colMapping);
        if (left == null || right == null) {
            return null;
        }

        // Handle date literal formatting for the right side
        if (comp.getRight() instanceof ConnectorLiteral) {
            ConnectorLiteral lit = (ConnectorLiteral) comp.getRight();
            String dateSql = formatDateLiteral(lit);
            if (dateSql != null) {
                right = dateSql;
            }
        }

        return left + " " + comp.getOperator().getSymbol() + " " + right;
    }

    private String andToSql(ConnectorAnd and, Map<String, String> colMapping) {
        List<String> parts = new ArrayList<>();
        for (ConnectorExpression child : and.getChildren()) {
            String childSql = expressionToSql(child, colMapping);
            if (childSql == null) {
                return null;
            }
            parts.add("(" + childSql + ")");
        }
        return String.join(" AND ", parts);
    }

    private String orToSql(ConnectorOr or, Map<String, String> colMapping) {
        List<String> parts = new ArrayList<>();
        for (ConnectorExpression child : or.getChildren()) {
            String childSql = expressionToSql(child, colMapping);
            if (childSql == null) {
                return null;
            }
            parts.add("(" + childSql + ")");
        }
        return String.join(" OR ", parts);
    }

    private String notToSql(ConnectorNot not, Map<String, String> colMapping) {
        String childSql = expressionToSql(not.getOperand(), colMapping);
        if (childSql == null) {
            return null;
        }
        return "NOT (" + childSql + ")";
    }

    private String inToSql(ConnectorIn in, Map<String, String> colMapping) {
        String valueSql = expressionToSql(in.getValue(), colMapping);
        if (valueSql == null) {
            return null;
        }
        StringJoiner joiner = new StringJoiner(", ");
        for (ConnectorExpression item : in.getInList()) {
            String itemSql;
            if (item instanceof ConnectorLiteral) {
                String dateSql = formatDateLiteral((ConnectorLiteral) item);
                itemSql = dateSql != null ? dateSql : expressionToSql(item, colMapping);
            } else {
                itemSql = expressionToSql(item, colMapping);
            }
            if (itemSql == null) {
                return null;
            }
            joiner.add(itemSql);
        }
        return valueSql + (in.isNegated() ? " NOT IN (" : " IN (") + joiner + ")";
    }

    private String isNullToSql(ConnectorIsNull isNull, Map<String, String> colMapping) {
        String operandSql = expressionToSql(isNull.getOperand(), colMapping);
        if (operandSql == null) {
            return null;
        }
        return operandSql + (isNull.isNegated() ? " IS NOT NULL" : " IS NULL");
    }

    private String likeToSql(ConnectorLike like, Map<String, String> colMapping) {
        String valueSql = expressionToSql(like.getValue(), colMapping);
        String patternSql = expressionToSql(like.getPattern(), colMapping);
        if (valueSql == null || patternSql == null) {
            return null;
        }
        String keyword = like.getOperator() == ConnectorLike.Operator.REGEXP ? "REGEXP" : "LIKE";
        return valueSql + " " + keyword + " " + patternSql;
    }

    private String betweenToSql(ConnectorBetween between, Map<String, String> colMapping) {
        String valueSql = expressionToSql(between.getValue(), colMapping);
        String lowerSql = expressionToSql(between.getLower(), colMapping);
        String upperSql = expressionToSql(between.getUpper(), colMapping);
        if (valueSql == null || lowerSql == null || upperSql == null) {
            return null;
        }
        // Apply date formatting for lower/upper if they are date literals
        if (between.getLower() instanceof ConnectorLiteral) {
            String dateSql = formatDateLiteral((ConnectorLiteral) between.getLower());
            if (dateSql != null) {
                lowerSql = dateSql;
            }
        }
        if (between.getUpper() instanceof ConnectorLiteral) {
            String dateSql = formatDateLiteral((ConnectorLiteral) between.getUpper());
            if (dateSql != null) {
                upperSql = dateSql;
            }
        }
        return valueSql + " BETWEEN " + lowerSql + " AND " + upperSql;
    }

    /**
     * Converts a function call expression to SQL.
     *
     * <p>Handles three categories:</p>
     * <ol>
     *   <li>Time arithmetic functions (years_add, months_sub, etc.)
     *       → DATE_ADD/DATE_SUB with INTERVAL syntax</li>
     *   <li>Regular functions with name rewriting and pushdown validation</li>
     *   <li>Unsupported functions → returns null (filter evaluated locally)</li>
     * </ol>
     */
    private static final Set<String> ARITHMETIC_OPERATORS = new HashSet<>(
            Arrays.asList("+", "-", "*", "/", "%", "DIV", "&", "|", "^"));
    private static final Set<String> UNARY_OPERATORS = Collections.singleton("~");

    private String functionCallToSql(ConnectorFunctionCall func, Map<String, String> colMapping) {
        String funcName = func.getFunctionName();

        // 0. Handle arithmetic operators rendered as infix: (arg1 + arg2)
        if (ARITHMETIC_OPERATORS.contains(funcName) && func.getArguments().size() == 2) {
            String left = expressionToSql(func.getArguments().get(0), colMapping);
            String right = expressionToSql(func.getArguments().get(1), colMapping);
            if (left == null || right == null) {
                return null;
            }
            return "(" + left + " " + funcName + " " + right + ")";
        }
        if (UNARY_OPERATORS.contains(funcName) && func.getArguments().size() == 1) {
            String operand = expressionToSql(func.getArguments().get(0), colMapping);
            if (operand == null) {
                return null;
            }
            return "(" + funcName + operand + ")";
        }

        // 0b. Fallback expressions with no arguments are pre-rendered SQL fragments
        if (func.getArguments().isEmpty() && !funcName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            // The function name is actually a SQL snippet from Expr.toSql() fallback
            return funcName;
        }

        // 1. Check for time arithmetic patterns (years_add → DATE_ADD)
        JdbcFunctionPushdownConfig.TimeArithmeticInfo timeInfo =
                JdbcFunctionPushdownConfig.parseTimeArithmetic(funcName);
        if (timeInfo != null && func.getArguments().size() == 2) {
            String arg1 = expressionToSql(func.getArguments().get(0), colMapping);
            String arg2 = expressionToSql(func.getArguments().get(1), colMapping);
            if (arg1 == null || arg2 == null) {
                return null;
            }
            return timeInfo.getBaseFunction() + "(" + arg1
                    + ", INTERVAL " + arg2 + " " + timeInfo.getTimeUnit() + ")";
        }

        // 2. Check pushdown rules
        if (functionConfig == null || !functionConfig.canPushDown(funcName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Function '{}' cannot be pushed down for DB type {}", funcName, dbType);
            }
            return null;
        }

        // 3. Apply name rewriting
        String rewrittenName = functionConfig.rewriteFunctionName(funcName);

        // 4. Render function call SQL
        StringJoiner argJoiner = new StringJoiner(", ");
        for (ConnectorExpression arg : func.getArguments()) {
            String argSql = expressionToSql(arg, colMapping);
            if (argSql == null) {
                return null;
            }
            argJoiner.add(argSql);
        }
        return rewrittenName + "(" + argJoiner + ")";
    }

    private String columnToSql(ConnectorColumnRef col, Map<String, String> colMapping) {
        String remoteName = colMapping.getOrDefault(
                col.getColumnName(), col.getColumnName());
        return JdbcIdentifierQuoter.quoteRemoteIdentifier(dbType, remoteName);
    }

    private String literalToSql(ConnectorLiteral lit) {
        if (lit.isNull()) {
            return "NULL";
        }
        Object val = lit.getValue();
        if (val instanceof String) {
            return "'" + escapeSql((String) val) + "'";
        } else if (val instanceof Boolean) {
            return formatBooleanLiteral((Boolean) val);
        } else if (val instanceof Double) {
            double d = (Double) val;
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                return Long.toString((long) d);
            }
            return val.toString();
        } else if (val instanceof BigDecimal || val instanceof Integer
                || val instanceof Long) {
            return val.toString();
        } else if (val instanceof LocalDate || val instanceof LocalDateTime) {
            // Date/datetime formatting is handled by formatDateLiteral when in comparison context
            return formatDateLiteralDirect(lit);
        }
        return val.toString();
    }

    /**
     * Formats a date/datetime literal with database-specific syntax.
     * Returns null if the literal is not a date/datetime type.
     */
    private String formatDateLiteral(ConnectorLiteral lit) {
        if (lit.isNull() || lit.getValue() == null) {
            return null;
        }
        ConnectorType type = lit.getType();
        String typeName = type.getTypeName().toUpperCase();
        if (!typeName.contains("DATE") && !typeName.contains("TIME")) {
            return null;
        }
        return formatDateLiteralDirect(lit);
    }

    /**
     * Formats a boolean literal with database-specific syntax.
     * Databases with strict BOOLEAN types (PostgreSQL, Trino) require TRUE/FALSE keywords.
     * Databases that represent booleans as integers (Oracle, SQL Server, DB2) need 1/0.
     * MySQL-compatible databases accept either; we use TRUE/FALSE for consistency
     * with the old ExprToSqlVisitor.visitBoolLiteral() behavior.
     */
    private String formatBooleanLiteral(boolean val) {
        switch (dbType) {
            case ORACLE:
            case OCEANBASE_ORACLE:
            case SQLSERVER:
            case DB2:
                return val ? "1" : "0";
            default:
                return val ? "TRUE" : "FALSE";
        }
    }

    private String formatDateLiteralDirect(ConnectorLiteral lit) {
        Object val = lit.getValue();
        ConnectorType type = lit.getType();
        String typeName = type.getTypeName().toUpperCase();
        boolean isDatetime = typeName.contains("TIME") || val instanceof LocalDateTime;

        switch (dbType) {
            case ORACLE:
            case OCEANBASE_ORACLE:
                return formatOracleDate(val, isDatetime);
            case TRINO:
            case PRESTO:
                return formatTrinoDate(val, isDatetime);
            case SQLSERVER:
                return formatSqlServerDate(val, isDatetime);
            default:
                return formatGenericDate(val, isDatetime);
        }
    }

    private String formatOracleDate(Object val, boolean isDatetime) {
        if (isDatetime && val instanceof LocalDateTime) {
            String dateStr = ((LocalDateTime) val).format(DATETIME_FMT);
            int nanos = ((LocalDateTime) val).getNano();
            if (nanos > 0) {
                long micros = nanos / 1000;
                String fracStr = String.format("%06d", micros);
                dateStr = dateStr + "." + fracStr;
                return "to_timestamp('" + dateStr + "', 'yyyy-mm-dd hh24:mi:ss.FF6')";
            }
            return "to_date('" + dateStr + "', 'yyyy-mm-dd hh24:mi:ss')";
        }
        if (val instanceof LocalDate) {
            return "to_date('" + ((LocalDate) val).format(DATE_FMT) + "', 'yyyy-mm-dd')";
        }
        return "'" + val + "'";
    }

    private String formatTrinoDate(Object val, boolean isDatetime) {
        if (isDatetime && val instanceof LocalDateTime) {
            return "timestamp '" + ((LocalDateTime) val).format(DATETIME_FRAC_FMT) + "'";
        }
        if (val instanceof LocalDate) {
            return "date '" + ((LocalDate) val).format(DATE_FMT) + "'";
        }
        return "'" + val + "'";
    }

    private String formatSqlServerDate(Object val, boolean isDatetime) {
        if (isDatetime && val instanceof LocalDateTime) {
            return "CONVERT(DATETIME, '" + ((LocalDateTime) val).format(DATETIME_FRAC_FMT) + "', 121)";
        }
        if (val instanceof LocalDate) {
            return "CONVERT(DATE, '" + ((LocalDate) val).format(DATE_FMT) + "', 23)";
        }
        return "'" + val + "'";
    }

    private String formatGenericDate(Object val, boolean isDatetime) {
        if (isDatetime && val instanceof LocalDateTime) {
            return "'" + ((LocalDateTime) val).format(DATETIME_FRAC_FMT) + "'";
        }
        if (val instanceof LocalDate) {
            return "'" + ((LocalDate) val).format(DATE_FMT) + "'";
        }
        return "'" + val + "'";
    }

    private static String escapeSql(String str) {
        return str.replace("'", "''");
    }
}
