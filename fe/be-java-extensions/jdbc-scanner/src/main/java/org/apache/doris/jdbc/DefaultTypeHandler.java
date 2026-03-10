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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Default type handler for databases without specific specializations.
 * Provides basic JDBC type reading that works for most databases.
 * Database-specific handlers extend this and override methods as needed.
 */
public class DefaultTypeHandler implements JdbcTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        Object value;
        switch (type.getType()) {
            case BOOLEAN:
                value = rs.getBoolean(columnIndex);
                break;
            case TINYINT:
                value = rs.getByte(columnIndex);
                break;
            case SMALLINT:
                value = rs.getShort(columnIndex);
                break;
            case INT:
                value = rs.getInt(columnIndex);
                break;
            case BIGINT:
                value = rs.getLong(columnIndex);
                break;
            case LARGEINT:
                value = rs.getObject(columnIndex);
                if (value instanceof BigDecimal) {
                    value = ((BigDecimal) value).toBigInteger();
                } else if (value != null && !(value instanceof BigInteger)) {
                    value = new BigInteger(value.toString());
                }
                break;
            case FLOAT:
                value = rs.getFloat(columnIndex);
                break;
            case DOUBLE:
                value = rs.getDouble(columnIndex);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                value = rs.getBigDecimal(columnIndex);
                break;
            case DATE:
            case DATEV2: {
                Date sqlDate = rs.getDate(columnIndex);
                value = sqlDate != null ? sqlDate.toLocalDate() : null;
                break;
            }
            case DATETIME:
            case DATETIMEV2: {
                Timestamp ts = rs.getTimestamp(columnIndex);
                value = ts != null ? ts.toLocalDateTime() : null;
                break;
            }
            case CHAR:
            case VARCHAR:
            case STRING:
                value = rs.getString(columnIndex);
                break;
            case VARBINARY:
            case BYTE: {
                byte[] data = rs.getBytes(columnIndex);
                return rs.wasNull() ? null : data;
            }
            default:
                value = rs.getString(columnIndex);
                break;
        }
        return rs.wasNull() ? null : value;
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        return null;
    }

    // =====================================================================
    // Shared utility methods available to all type handlers
    // =====================================================================

    /**
     * Trim trailing spaces from a string. Used by Oracle, PostgreSQL, Gbase
     * for CHAR type columns that have right-padding.
     */
    protected static String trimSpaces(String str) {
        if (str == null) {
            return null;
        }
        int end = str.length();
        while (end > 0 && str.charAt(end - 1) == ' ') {
            end--;
        }
        return end < str.length() ? str.substring(0, end) : str;
    }

    /**
     * Convert java.sql.Time to a properly formatted String.
     * Preserves millisecond precision when present (e.g., "16:49:05.123").
     */
    protected static String timeToString(java.sql.Time time) {
        if (time == null) {
            return null;
        }
        long milliseconds = Math.abs(time.getTime() % 1000L);
        if (milliseconds != 0) {
            return String.format("%s.%03d", time, milliseconds);
        } else {
            return time.toString();
        }
    }

    /**
     * Convert byte array to hex string with "0x" prefix.
     * Default implementation used by most databases.
     */
    protected static String defaultByteArrayToHexString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        StringBuilder hexString = new StringBuilder("0x");
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex.toUpperCase());
        }
        return hexString.toString();
    }

    /**
     * Helper to create a ColumnValueConverter from a converter function.
     */
    protected static ColumnValueConverter createConverter(
            java.util.function.Function<Object, ?> converterFunction, Class<?> type) {
        return column -> {
            Object[] result = (Object[]) java.lang.reflect.Array.newInstance(type, column.length);
            for (int i = 0; i < column.length; i++) {
                result[i] = column[i] != null ? converterFunction.apply(column[i]) : null;
            }
            return result;
        };
    }
}
