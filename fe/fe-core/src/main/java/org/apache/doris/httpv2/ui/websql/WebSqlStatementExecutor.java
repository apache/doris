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

package org.apache.doris.httpv2.ui.websql;

import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;

import com.google.common.collect.Lists;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebSqlStatementExecutor {
    private final StatementSubmitter statementSubmitter;

    public WebSqlStatementExecutor() {
        this(new StatementSubmitter());
    }

    WebSqlStatementExecutor(StatementSubmitter statementSubmitter) {
        this.statementSubmitter = statementSubmitter;
    }

    @SuppressWarnings("unchecked")
    public WebSqlExecutionResult execute(WebSqlSession session, String sql, WebSqlLimits limits) {
        String statement = SingleStatementValidator.requireSingleStatement(sql);
        StatementSubmitter.StmtContext context = new StatementSubmitter.StmtContext(
                statement, "", "", limits.maxResultRows, false, null, "")
                .withQueryTimeoutSeconds(limits.statementTimeoutSeconds)
                .withMaxResultBytes(limits.maxResultBytes)
                .withStatementObserver(session::setActiveStatement);
        try {
            ExecutionResultSet execution = statementSubmitter.execute(session.getConnection(), context);
            Map<String, Object> result = execution.getResult();
            List<WebSqlColumn> columns = Lists.newArrayList();
            for (Map<String, String> field : (List<Map<String, String>>) result.getOrDefault(
                    "meta", Collections.emptyList())) {
                columns.add(new WebSqlColumn(field.get("name"), field.get("type")));
            }

            List<List<Object>> boundedRows = Lists.newArrayList();
            long bytes = estimateColumnsBytes(columns);
            boolean truncated = Boolean.TRUE.equals(result.get("truncated"));
            for (List<Object> row : (List<List<Object>>) result.getOrDefault("data", Collections.emptyList())) {
                long rowBytes = estimateRowBytes(row);
                if (bytes + rowBytes > limits.maxResultBytes) {
                    truncated = true;
                    break;
                }
                boundedRows.add(row);
                bytes += rowBytes;
            }

            SessionMetadata metadata = readSessionMetadata(session.getConnection());
            long affectedRows = ((Number) result.getOrDefault("affectedRows", 0)).longValue();
            long elapsedTime = ((Number) result.getOrDefault("time", 0)).longValue();
            List<String> warnings = (List<String>) result.getOrDefault("warnings", Collections.emptyList());
            return new WebSqlExecutionResult(columns, boundedRows, affectedRows, elapsedTime,
                    metadata.queryId, warnings, metadata.catalog, metadata.database, truncated);
        } catch (SQLTimeoutException exception) {
            throw new WebSqlException(WebSqlError.QUERY_TIMEOUT, sqlDetails(exception), exception);
        } catch (SQLException exception) {
            throw new WebSqlException(WebSqlError.QUERY_ERROR, sqlDetails(exception), exception);
        } catch (WebSqlException exception) {
            throw exception;
        } catch (Exception exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof SQLTimeoutException) {
                SQLTimeoutException timeout = (SQLTimeoutException) cause;
                throw new WebSqlException(WebSqlError.QUERY_TIMEOUT, sqlDetails(timeout), timeout);
            }
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;
                throw new WebSqlException(WebSqlError.QUERY_ERROR, sqlDetails(sqlException), sqlException);
            }
            throw new WebSqlException(WebSqlError.QUERY_ERROR, exception);
        }
    }

    private SessionMetadata readSessionMetadata(java.sql.Connection connection) {
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
        details.put("sqlState", String.valueOf(exception.getSQLState()));
        details.put("vendorCode", exception.getErrorCode());
        return details;
    }

    private long estimateColumnsBytes(List<WebSqlColumn> columns) {
        long bytes = 0;
        for (WebSqlColumn column : columns) {
            bytes += utf8Length(column.getName()) + utf8Length(column.getType());
        }
        return bytes;
    }

    private long estimateRowBytes(List<Object> row) {
        long bytes = 0;
        for (Object value : row) {
            bytes += utf8Length(value == null ? null : String.valueOf(value));
        }
        return bytes;
    }

    private int utf8Length(String value) {
        return value == null ? 4 : value.getBytes(StandardCharsets.UTF_8).length;
    }

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
