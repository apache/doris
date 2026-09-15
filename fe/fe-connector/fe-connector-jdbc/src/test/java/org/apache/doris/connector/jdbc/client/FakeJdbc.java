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


package org.apache.doris.connector.jdbc.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Reflection-proxy stand-ins for the handful of {@code java.sql} interfaces the metadata clients touch,
 * so a client's row handling can be exercised without a database or a mocking framework (the connector
 * poms carry none, deliberately).
 *
 * <p>A fake {@link ResultSet} is a list of rows, each a column-name-to-value map; {@code getXxx(label)}
 * reads the current row, {@code getXxx(index)} reads by 1-based position in the row's insertion order.
 * Anything not modelled throws, so a client reaching for more than the fake offers fails loud instead of
 * silently reading {@code null}.</p>
 */
final class FakeJdbc {

    private FakeJdbc() {
    }

    /** A row of a fake result set, in insertion order. */
    static Map<String, Object> row(Object... labelValuePairs) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < labelValuePairs.length; i += 2) {
            row.put((String) labelValuePairs[i], labelValuePairs[i + 1]);
        }
        return row;
    }

    static ResultSet resultSet(List<Map<String, Object>> rows) {
        return (ResultSet) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, new ResultSetHandler(rows));
    }

    /**
     * A connection whose metadata answers {@code getColumns} and {@code getPrimaryKeys} with the given
     * result sets, and whose statements run {@code queries} (sql -> result set); a prepared statement
     * records every bound parameter into {@code boundParams} in bind order.
     */
    static Connection connection(ResultSet columns, ResultSet primaryKeys,
            Function<String, ResultSet> queries, List<Object> boundParams) {
        DatabaseMetaData meta = (DatabaseMetaData) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(),
                new Class<?>[] {DatabaseMetaData.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getColumns":
                            return columns;
                        case "getPrimaryKeys":
                            return primaryKeys;
                        case "getSearchStringEscape":
                            return "\\";
                        default:
                            throw new UnsupportedOperationException("DatabaseMetaData." + method.getName());
                    }
                });
        return (Connection) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getMetaData":
                            return meta;
                        case "getCatalog":
                        case "getSchema":
                            return null;
                        case "prepareStatement":
                            return preparedStatement(queries.apply((String) args[0]), boundParams);
                        case "createStatement":
                            return statement(queries);
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException("Connection." + method.getName());
                    }
                });
    }

    private static Statement statement(Function<String, ResultSet> queries) {
        return (Statement) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(),
                new Class<?>[] {Statement.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "executeQuery": {
                            ResultSet result = queries.apply((String) args[0]);
                            if (result == null) {
                                throw new SQLException("no result set configured for: " + args[0]);
                            }
                            return result;
                        }
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException("Statement." + method.getName());
                    }
                });
    }

    private static PreparedStatement preparedStatement(ResultSet result, List<Object> boundParams) {
        return (PreparedStatement) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "setObject":
                        case "setString":
                            boundParams.add(args[1]);
                            return null;
                        case "executeQuery":
                            if (result == null) {
                                throw new SQLException("no result set configured for this statement");
                            }
                            return result;
                        case "close":
                            return null;
                        default:
                            throw new UnsupportedOperationException("PreparedStatement." + method.getName());
                    }
                });
    }

    private static final class ResultSetHandler implements InvocationHandler {
        private final List<Map<String, Object>> rows;
        private int cursor = -1;
        private boolean lastWasNull;

        private ResultSetHandler(List<Map<String, Object>> rows) {
            this.rows = rows;
        }

        private Object value(Object[] args) throws SQLException {
            if (cursor < 0 || cursor >= rows.size()) {
                throw new SQLException("cursor is not on a row");
            }
            Map<String, Object> row = rows.get(cursor);
            Object v;
            if (args[0] instanceof Integer) {
                int index = (Integer) args[0];
                v = new ArrayList<>(row.values()).get(index - 1);
            } else {
                String label = (String) args[0];
                if (!row.containsKey(label)) {
                    throw new SQLException("no such column: " + label);
                }
                v = row.get(label);
            }
            lastWasNull = v == null;
            return v;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "next":
                    cursor++;
                    return cursor < rows.size();
                case "getString": {
                    Object v = value(args);
                    return v == null ? null : v.toString();
                }
                case "getObject":
                    return value(args);
                case "getInt": {
                    Object v = value(args);
                    return v == null ? 0 : ((Number) v).intValue();
                }
                case "getShort": {
                    Object v = value(args);
                    return v == null ? (short) 0 : ((Number) v).shortValue();
                }
                case "getLong": {
                    Object v = value(args);
                    return v == null ? 0L : ((Number) v).longValue();
                }
                case "getBoolean": {
                    Object v = value(args);
                    return v != null && (Boolean) v;
                }
                case "wasNull":
                    return lastWasNull;
                case "getMetaData":
                    return metaData();
                case "close":
                    return null;
                default:
                    throw new UnsupportedOperationException("ResultSet." + method.getName());
            }
        }

        private ResultSetMetaData metaData() {
            List<String> labels = rows.isEmpty() ? new ArrayList<>() : new ArrayList<>(rows.get(0).keySet());
            return (ResultSetMetaData) Proxy.newProxyInstance(FakeJdbc.class.getClassLoader(),
                    new Class<?>[] {ResultSetMetaData.class}, (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "getColumnCount":
                                return labels.size();
                            case "getColumnLabel":
                            case "getColumnName":
                                return labels.get((Integer) args[0] - 1);
                            default:
                                throw new UnsupportedOperationException("ResultSetMetaData." + method.getName());
                        }
                    });
        }
    }
}
