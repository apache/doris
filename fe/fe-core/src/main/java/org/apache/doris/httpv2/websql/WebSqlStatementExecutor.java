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

package org.apache.doris.httpv2.websql;

import org.apache.doris.common.Config;

import com.google.common.collect.Lists;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/** Executes one validated statement on an existing Web SQL connection and builds a bounded JSON result. */
public class WebSqlStatementExecutor {
    private final LongSupplier maxResultBytesSupplier;

    public WebSqlStatementExecutor() {
        this(() -> Config.web_sql_max_result_bytes);
    }

    WebSqlStatementExecutor(LongSupplier maxResultBytesSupplier) {
        this.maxResultBytesSupplier = maxResultBytesSupplier;
    }

    public WebSqlExecutionResult execute(WebSqlSession session, String sql, WebSqlLimits limits) {
        String validatedSql = SingleStatementValidator.requireSingleStatement(sql);
        long maxResultBytes = currentMaxResultBytes();
        Connection connection = session.getConnection();
        long startTime = System.currentTimeMillis();
        QueryResult queryResult;

        try (Statement statement = connection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            statement.setFetchSize(1000);
            session.setActiveStatement(statement);
            boolean hasResultSet = statement.execute(validatedSql);
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    queryResult = readResultSet(resultSet, limits.maxResultRows, maxResultBytes);
                }
            } else {
                queryResult = new QueryResult(Collections.emptyList(), Collections.emptyList(),
                        Math.max(statement.getUpdateCount(), 0), false);
            }
            queryResult.warnings.addAll(readWarnings(statement));
        } catch (SQLTimeoutException exception) {
            throw new WebSqlException(WebSqlError.QUERY_TIMEOUT, sqlDetails(exception), exception);
        } catch (SQLException exception) {
            throw new WebSqlException(WebSqlError.QUERY_ERROR, sqlDetails(exception), exception);
        } finally {
            session.setActiveStatement(null);
        }

        SessionMetadata metadata = readSessionMetadata(connection);
        return new WebSqlExecutionResult(queryResult.columns, queryResult.rows, queryResult.affectedRows,
                System.currentTimeMillis() - startTime, metadata.queryId, queryResult.warnings,
                metadata.catalog, metadata.database, queryResult.truncated);
    }

    private QueryResult readResultSet(ResultSet resultSet, int maxResultRows, long maxResultBytes)
            throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        List<WebSqlColumn> columns = Lists.newArrayListWithCapacity(columnCount);
        for (int column = 1; column <= columnCount; column++) {
            columns.add(new WebSqlColumn(metadata.getColumnName(column), metadata.getColumnTypeName(column)));
        }

        List<List<Object>> rows = Lists.newArrayList();
        long resultBytes = 0;
        boolean truncated = false;
        while (resultSet.next()) {
            if (rows.size() >= maxResultRows) {
                truncated = true;
                break;
            }
            List<Object> row = Lists.newArrayListWithCapacity(columnCount);
            long rowBytes = 0;
            for (int column = 1; column <= columnCount; column++) {
                String type = metadata.getColumnTypeName(column);
                Object value = isDateType(type) ? resultSet.getString(column) : resultSet.getObject(column);
                row.add(value);
                rowBytes += valueSize(value);
            }
            if (resultBytes + rowBytes > maxResultBytes) {
                truncated = true;
                break;
            }
            rows.add(row);
            resultBytes += rowBytes;
        }
        return new QueryResult(columns, rows, 0, truncated);
    }

    long currentMaxResultBytes() {
        long value = maxResultBytesSupplier.getAsLong();
        if (value <= 0 || value > Config.WEB_SQL_MAX_RESULT_BYTES_UPPER_BOUND) {
            throw new IllegalStateException("Invalid web_sql_max_result_bytes: " + value);
        }
        return value;
    }

    private boolean isDateType(String type) {
        return "DATE".equalsIgnoreCase(type) || "DATETIME".equalsIgnoreCase(type)
                || "DATEV2".equalsIgnoreCase(type) || "DATETIMEV2".equalsIgnoreCase(type);
    }

    private long valueSize(Object value) {
        return value == null ? 4 : String.valueOf(value).getBytes(StandardCharsets.UTF_8).length;
    }

    private List<String> readWarnings(Statement statement) throws SQLException {
        List<String> warnings = Lists.newArrayList();
        SQLWarning warning = statement.getWarnings();
        while (warning != null) {
            warnings.add(warning.getMessage());
            warning = warning.getNextWarning();
        }
        return warnings;
    }

    private SessionMetadata readSessionMetadata(Connection connection) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "SELECT CURRENT_CATALOG(), DATABASE(), LAST_QUERY_ID()")) {
            if (resultSet.next()) {
                return new SessionMetadata(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3));
            }
        } catch (SQLException ignored) {
            // Metadata is supplementary and must not turn a successful user statement into a failure.
        }
        try {
            return new SessionMetadata(null, connection.getCatalog(), null);
        } catch (SQLException ignored) {
            return new SessionMetadata(null, null, null);
        }
    }

    private Map<String, Object> sqlDetails(SQLException exception) {
        Map<String, Object> details = new HashMap<>();
        details.put("message", sqlMessage(exception));
        details.put("sqlState", String.valueOf(exception.getSQLState()));
        details.put("vendorCode", exception.getErrorCode());
        return details;
    }

    /**
     * JDBC drivers may put the server's diagnostic text on the chained SQLException rather than
     * the exception thrown by execute(). Keep the first useful message so the Web SQL client can
     * show the same SQL error that a MySQL client receives without exposing a Java stack trace.
     */
    private String sqlMessage(SQLException exception) {
        SQLException current = exception;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && !message.trim().isEmpty()) {
                return message;
            }
            current = current.getNextException();
        }
        Throwable cause = exception.getCause();
        return cause != null && cause.getMessage() != null && !cause.getMessage().trim().isEmpty()
                ? cause.getMessage() : "SQL execution failed.";
    }

    /** Intermediate result-set data collected before supplementary session metadata is queried. */
    private static class QueryResult {
        private final List<WebSqlColumn> columns;
        private final List<List<Object>> rows;
        private final long affectedRows;
        private final boolean truncated;
        private final List<String> warnings = Lists.newArrayList();

        QueryResult(List<WebSqlColumn> columns, List<List<Object>> rows, long affectedRows, boolean truncated) {
            this.columns = columns;
            this.rows = rows;
            this.affectedRows = affectedRows;
            this.truncated = truncated;
        }
    }

    /** Best-effort catalog, database, and query ID observed after a statement finishes. */
    private static class SessionMetadata {
        private final String catalog;
        private final String database;
        private final String queryId;

        SessionMetadata(String catalog, String database, String queryId) {
            this.catalog = catalog;
            this.database = database;
            this.queryId = queryId;
        }
    }
}
