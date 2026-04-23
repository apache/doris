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

package org.apache.doris.jdbc;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL-specific type handler.
 * Key specializations:
 * - Streaming result set via Integer.MIN_VALUE fetchSize
 * - TIME type uses getString() for >24h values
 * - TINYINT/LARGEINT need type conversion
 * - byte[] → hex string conversion
 * - Connection abort for incomplete result sets
 */
public class MySQLTypeHandler extends DefaultTypeHandler {

    // Store the table type to differentiate MySQL vs OceanBase behavior.
    // OceanBase TIME columns lose fractional second precision with rs.getString(),
    // so we only use getString() for MySQL TIME (to handle >24h values).
    private final String tableType;

    public MySQLTypeHandler() {
        this("MYSQL");
    }

    public MySQLTypeHandler(String tableType) {
        this.tableType = tableType != null ? tableType.toUpperCase() : "MYSQL";
    }

    /**
     * MySQL Connector/J 5.1.x's getObject(col, Type.class) may return the primitive
     * default (0, 0.0, false) instead of null for SQL NULL columns.  Always follow
     * up with wasNull() so we propagate NULLs correctly regardless of driver version.
     */
    private static <T> T getTypedObject(ResultSet rs, int col, Class<T> type) throws SQLException {
        T val = rs.getObject(col, type);
        return rs.wasNull() ? null : val;
    }

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        switch (type.getType()) {
            case BOOLEAN:
                return getTypedObject(rs, columnIndex, Boolean.class);
            case TINYINT:
            case SMALLINT:
                // Must use typed getObject() to avoid MySQL YEAR returning java.sql.Date
                // when yearIsDateType=true (the default in Connector/J 8.x)
                return getTypedObject(rs, columnIndex, Integer.class);
            case LARGEINT:
                return rs.getObject(columnIndex);
            case INT:
                return getTypedObject(rs, columnIndex, Integer.class);
            case BIGINT:
                return getTypedObject(rs, columnIndex, Long.class);
            case FLOAT:
                return getTypedObject(rs, columnIndex, Float.class);
            case DOUBLE:
                return getTypedObject(rs, columnIndex, Double.class);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return rs.getObject(columnIndex, BigDecimal.class);
            case DATE:
            case DATEV2:
                return rs.getObject(columnIndex, LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return rs.getObject(columnIndex, LocalDateTime.class);
            case CHAR:
            case VARCHAR:
            case ARRAY:
                return rs.getObject(columnIndex, String.class);
            case STRING: {
                int jdbcType = metadata.getColumnType(columnIndex);
                // MySQL TIME type needs getString() to handle >24h values.
                // But for OceanBase, getString() loses fractional second precision,
                // so we use getObject() instead (matching old MySQLJdbcExecutor behavior).
                if (jdbcType == Types.TIME && "MYSQL".equals(tableType)) {
                    return rs.getString(columnIndex);
                }
                return rs.getObject(columnIndex);
            }
            case BYTE:
            case VARBINARY: {
                byte[] data = rs.getBytes(columnIndex);
                return rs.wasNull() ? null : data;
            }
            case TIMESTAMPTZ:
                return rs.getObject(columnIndex, LocalDateTime.class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case TINYINT:
                return createConverter(input -> {
                    if (input instanceof Integer) {
                        return ((Integer) input).byteValue();
                    } else if (input instanceof Number) {
                        return ((Number) input).byteValue();
                    } else if (input instanceof String) {
                        return Byte.parseByte((String) input);
                    }
                    return input;
                }, Byte.class);
            case SMALLINT:
                return createConverter(input -> {
                    if (input instanceof Integer) {
                        return ((Integer) input).shortValue();
                    } else if (input instanceof Number) {
                        return ((Number) input).shortValue();
                    } else if (input instanceof String) {
                        return Short.parseShort((String) input);
                    }
                    return input;
                }, Short.class);
            case LARGEINT:
                return createConverter(input -> {
                    if (input instanceof BigInteger) {
                        return input;
                    } else if (input instanceof String) {
                        return new BigInteger((String) input);
                    } else if (input instanceof Number) {
                        // Use toString() to avoid signed long overflow for BIGINT UNSIGNED
                        return new BigDecimal(input.toString()).toBigInteger();
                    }
                    return input;
                }, BigInteger.class);
            case STRING:
                if ("bitmap".equals(replaceString) || "hll".equals(replaceString)) {
                    return null;
                }
                return createConverter(input -> {
                    if (input instanceof byte[]) {
                        return defaultByteArrayToHexString((byte[]) input);
                    } else if (input instanceof java.sql.Time) {
                        return timeToString((java.sql.Time) input);
                    }
                    return input.toString();
                }, String.class);
            case ARRAY:
                return createConverter(
                        input -> convertArray(input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    @Override
    public PreparedStatement initializeStatement(Connection conn, String sql,
                                                 int fetchSize) throws SQLException {
        conn.setAutoCommit(false);
        PreparedStatement stmt = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        // MySQL: Integer.MIN_VALUE signals streaming results to avoid OOM
        stmt.setFetchSize(Integer.MIN_VALUE);
        return stmt;
    }

    @Override
    public void abortReadConnection(Connection conn, ResultSet rs) throws SQLException {
        if (rs != null && !rs.isAfterLast()) {
            // Abort connection to avoid draining all remaining results
            conn.abort(MoreExecutors.directExecutor());
        }
    }

    @Override
    public void setSystemProperties() {
        super.setSystemProperties();
        System.setProperty("com.mysql.cj.disableAbandonedConnectionCleanup", "true");
    }

    private static final Gson gson = new Gson();

    /**
     * Parse MySQL JSON array string into a List, applying type-specific conversions.
     * MySQL returns ARRAY columns as JSON strings (e.g. "[1,2,3]"); this method
     * deserializes them into proper Java List objects matching the expected child type.
     */
    private Object convertArray(Object input, ColumnType columnType) {
        if (input == null) {
            return null;
        }
        String jsonStr = input.toString();
        ColumnType.Type childTypeEnum = columnType.getType();

        if (childTypeEnum == ColumnType.Type.BOOLEAN) {
            List<?> list = gson.fromJson(jsonStr, List.class);
            return list.stream().map(item -> {
                if (item instanceof Boolean) {
                    return item;
                } else if (item instanceof Number) {
                    return ((Number) item).intValue() != 0;
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to Boolean.");
                }
            }).collect(Collectors.toList());
        } else if (childTypeEnum == ColumnType.Type.DATE || childTypeEnum == ColumnType.Type.DATEV2) {
            List<?> list = gson.fromJson(jsonStr, List.class);
            return list.stream().map(item -> {
                if (item instanceof String) {
                    return LocalDate.parse((String) item);
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDate.");
                }
            }).collect(Collectors.toList());
        } else if (childTypeEnum == ColumnType.Type.DATETIME || childTypeEnum == ColumnType.Type.DATETIMEV2) {
            List<?> list = gson.fromJson(jsonStr, List.class);
            return list.stream().map(item -> {
                if (item instanceof String) {
                    return LocalDateTime.parse(
                            (String) item,
                            new DateTimeFormatterBuilder()
                                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                                    .appendFraction(ChronoField.MILLI_OF_SECOND,
                                            columnType.getPrecision(),
                                            columnType.getPrecision(), true)
                                    .toFormatter());
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDateTime.");
                }
            }).collect(Collectors.toList());
        } else if (childTypeEnum == ColumnType.Type.LARGEINT) {
            List<?> list = gson.fromJson(jsonStr, List.class);
            return list.stream().map(item -> {
                if (item instanceof Number) {
                    return new BigDecimal(item.toString()).toBigInteger();
                } else if (item instanceof String) {
                    return new BigDecimal((String) item).toBigInteger();
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to BigInteger.");
                }
            }).collect(Collectors.toList());
        } else if (childTypeEnum == ColumnType.Type.ARRAY) {
            ColumnType nestedChildType = columnType.getChildTypes().get(0);
            List<?> rawList = gson.fromJson(jsonStr, List.class);
            return rawList.stream()
                    .map(element -> {
                        String elementJson = gson.toJson(element);
                        return convertArray(elementJson, nestedChildType);
                    })
                    .collect(Collectors.toList());
        } else {
            java.lang.reflect.Type listType = getListTypeForArray(columnType);
            return gson.fromJson(jsonStr, listType);
        }
    }

    /**
     * Map a Doris child ColumnType to the corresponding Gson TypeToken for List deserialization.
     */
    private java.lang.reflect.Type getListTypeForArray(ColumnType type) {
        switch (type.getType()) {
            case BOOLEAN:
                return new TypeToken<List<Boolean>>() { }.getType();
            case TINYINT:
                return new TypeToken<List<Byte>>() { }.getType();
            case SMALLINT:
                return new TypeToken<List<Short>>() { }.getType();
            case INT:
                return new TypeToken<List<Integer>>() { }.getType();
            case BIGINT:
                return new TypeToken<List<Long>>() { }.getType();
            case LARGEINT:
                return new TypeToken<List<BigInteger>>() { }.getType();
            case FLOAT:
                return new TypeToken<List<Float>>() { }.getType();
            case DOUBLE:
                return new TypeToken<List<Double>>() { }.getType();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new TypeToken<List<BigDecimal>>() { }.getType();
            case DATE:
            case DATEV2:
                return new TypeToken<List<LocalDate>>() { }.getType();
            case DATETIME:
            case DATETIMEV2:
                return new TypeToken<List<LocalDateTime>>() { }.getType();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new TypeToken<List<String>>() { }.getType();
            case ARRAY:
                java.lang.reflect.Type childType = getListTypeForArray(type.getChildTypes().get(0));
                TypeToken<?> token = TypeToken.getParameterized(List.class, childType);
                return token.getType();
            default:
                throw new IllegalArgumentException("Unsupported array child type: " + type.getType());
        }
    }
}
