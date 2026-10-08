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

import org.apache.doris.jni.spi.vec.ColumnType;
import org.apache.doris.jni.spi.vec.ColumnValueConverter;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Trino/Presto-specific type handler.
 * Key specializations:
 * - DATETIME: uses Timestamp.class then converts in output converter
 * - ARRAY: getArray() → Object[] → List
 */
public class TrinoTypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
        if (type.getType() == ColumnType.Type.TIMESTAMPTZ) {
            // The remote projection and driver preserve the instant; JNI receives UTC fields.
            ZonedDateTime value = rs.getObject(columnIndex, ZonedDateTime.class);
            return value == null ? null : checkedUtcTimestamp(value.toInstant());
        }
        switch (type.getType()) {
            case BOOLEAN:
                return rs.getObject(columnIndex, Boolean.class);
            case TINYINT:
                return rs.getObject(columnIndex, Byte.class);
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
            case DATEV2:
                return rs.getObject(columnIndex, LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return rs.getObject(columnIndex, Timestamp.class);
            case CHAR:
            case VARCHAR:
            case STRING:
                return rs.getObject(columnIndex, String.class);
            case ARRAY: {
                Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                Object[] dataArray = (Object[]) array.getArray();
                if (dataArray.length == 0) {
                    return Collections.emptyList();
                }
                return Arrays.asList(dataArray);
            }
            case VARBINARY:
                return rs.getObject(columnIndex, byte[].class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case DATETIME:
            case DATETIMEV2:
                return createConverter(
                        input -> ((Timestamp) input).toLocalDateTime(), LocalDateTime.class);
            case ARRAY:
                return createConverter(
                        input -> convertArray((List<?>) input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    private List<?> convertArray(List<?> array, ColumnType type) {
        if (array == null) {
            return null;
        }
        if (array.isEmpty()) {
            return Collections.emptyList();
        }
        switch (type.getType()) {
            case DATE:
            case DATEV2: {
                List<LocalDate> result = Lists.newArrayList();
                for (Object element : array) {
                    result.add(element != null ? ((Date) element).toLocalDate() : null);
                }
                return result;
            }
            case TIMESTAMPTZ: {
                List<LocalDateTime> result = Lists.newArrayList();
                // Trino JDBC exposes timestamp-with-zone array elements as java.sql.Timestamp.
                for (Object element : array) {
                    result.add(element == null ? null
                            : checkedUtcTimestamp(((Timestamp) element).toInstant()));
                }
                return result;
            }
            case DATETIME:
            case DATETIMEV2: {
                List<LocalDateTime> result = Lists.newArrayList();
                for (Object element : array) {
                    result.add(element != null ? ((Timestamp) element).toLocalDateTime() : null);
                }
                return result;
            }
            case ARRAY: {
                List<List<?>> resultArray = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        resultArray.add(null);
                    } else {
                        resultArray.add(
                                Lists.newArrayList(convertArray((List<?>) element, type.getChildTypes().get(0))));
                    }
                }
                return resultArray;
            }
            default:
                return array;
        }
    }

    private static final DateTimeFormatter TIMESTAMP_TZ_WRITE_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

    @Override
    public void setTimestampTz(java.sql.PreparedStatement statement, int parameterIndex, LocalDateTime value)
            throws SQLException {
        // Trino/Presto require a string for typed zoned binds; Timestamp drops the zone and sub-millisecond digits.
        statement.setObject(parameterIndex, value.format(TIMESTAMP_TZ_WRITE_FORMATTER) + " UTC",
                Types.TIMESTAMP_WITH_TIMEZONE);
    }

    @Override
    public void setTimestampTzNull(java.sql.PreparedStatement statement, int parameterIndex) throws SQLException {
        // These drivers reject TIMESTAMP_WITH_TIMEZONE in setNull; SQL NULL is coerced by the target column.
        statement.setNull(parameterIndex, Types.NULL);
    }

}
