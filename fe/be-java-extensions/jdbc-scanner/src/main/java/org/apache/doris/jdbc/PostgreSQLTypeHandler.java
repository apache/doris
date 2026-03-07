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
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL-specific type handler.
 * Key specializations:
 * - TIMESTAMPTZ: OffsetDateTime → LocalDateTime conversion
 * - ARRAY: PgArray handling
 * - CHAR trimming
 * - STRING: Time and byte[] conversion
 */
public class PostgreSQLTypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        switch (type.getType()) {
            case BOOLEAN:
                return rs.getObject(columnIndex, Boolean.class);
            case SMALLINT:
                return rs.getObject(columnIndex, Short.class);
            case INT:
                return rs.getObject(columnIndex, Integer.class);
            case BIGINT:
                return rs.getObject(columnIndex, Long.class);
            case FLOAT:
                return rs.getObject(columnIndex, Float.class);
            case DOUBLE:
                return rs.getObject(columnIndex, Double.class);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return rs.getObject(columnIndex, BigDecimal.class);
            case DATE:
            case DATEV2: {
                java.sql.Date sqlDate = rs.getDate(columnIndex);
                return sqlDate != null ? sqlDate.toLocalDate() : null;
            }
            case DATETIME:
            case DATETIMEV2:
                return rs.getObject(columnIndex);
            case CHAR:
            case VARCHAR:
            case STRING:
                return rs.getObject(columnIndex);
            case VARBINARY:
                return rs.getBytes(columnIndex);
            case TIMESTAMPTZ: {
                OffsetDateTime odt = rs.getObject(columnIndex, OffsetDateTime.class);
                return odt == null ? null : Timestamp.from(odt.toInstant());
            }
            case ARRAY: {
                Array array = rs.getArray(columnIndex);
                return array == null ? null : convertArrayToList(array.getArray());
            }
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case DATE:
            case DATEV2:
                return createConverter(input -> {
                    if (input instanceof java.sql.Date) {
                        return ((java.sql.Date) input).toLocalDate();
                    }
                    return input;
                }, LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return createConverter(input -> {
                    if (input instanceof Timestamp) {
                        return ((Timestamp) input).toLocalDateTime();
                    } else if (input instanceof OffsetDateTime) {
                        return ((OffsetDateTime) input).toLocalDateTime();
                    } else if (input instanceof java.sql.Date) {
                        return ((java.sql.Date) input).toLocalDate().atStartOfDay();
                    }
                    return input;
                }, LocalDateTime.class);
            case TIMESTAMPTZ:
                return createConverter(input -> {
                    if (input instanceof Timestamp) {
                        return LocalDateTime.ofInstant(
                                ((Timestamp) input).toInstant(), java.time.ZoneOffset.UTC);
                    }
                    return input;
                }, LocalDateTime.class);
            case CHAR:
                return createConverter(input -> trimSpaces(input.toString()), String.class);
            case STRING:
                return createConverter(input -> {
                    if (input instanceof java.sql.Time) {
                        return timeToString((java.sql.Time) input);
                    } else if (input instanceof byte[]) {
                        return pgByteArrayToHexString((byte[]) input);
                    }
                    return input.toString();
                }, String.class);
            case ARRAY:
                return createConverter(
                        input -> convertArray((List<?>) input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    /**
     * Convert a JDBC array object to a List.
     */
    private static List<?> convertArrayToList(Object array) {
        if (array == null) {
            return null;
        }
        List<Object> list = new ArrayList<>();
        if (array instanceof Object[]) {
            for (Object element : (Object[]) array) {
                list.add(element);
            }
        }
        return list;
    }

    /**
     * PostgreSQL byte[] → hex string (uses \\x prefix format with lowercase).
     * PostgreSQL native bytea format: \\xdeadbeef
     */
    private static String pgByteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder("\\x");
        for (byte b : bytes) {
            hexString.append(String.format("%02x", b & 0xff));
        }
        return hexString.toString();
    }

    /**
     * Recursively convert array elements for nested ARRAY types.
     */
    private Object convertArray(List<?> input, ColumnType childType) {
        if (input == null) {
            return null;
        }
        return input;
    }
}
