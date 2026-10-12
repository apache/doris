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
import org.apache.doris.common.jni.vec.ColumnType.Type;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorTable;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TrinoJdbcExecutor extends BaseJdbcExecutor {
    private static final DateTimeFormatter TIMESTAMP_TZ_WRITE_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter ARRAY_TIMESTAMP_FORMATTER = new java.time.format.DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .appendFraction(java.time.temporal.ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendLiteral(' ').appendZoneOrOffsetId().toFormatter();

    public TrinoJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected void setTimestampTz(int parameterIndex, LocalDateTime value) throws SQLException {
        if (config != null && config.getTableType() == org.apache.doris.thrift.TOdbcTableType.PRESTO) {
            // PrestoDB requires VARCHAR plus a server-side cast; its driver rejects JDBC type 2014.
            preparedStatement.setString(parameterIndex, value.format(TIMESTAMP_TZ_WRITE_FORMATTER) + " UTC");
            return;
        }
        // Trino/Presto require a string for typed zoned binds; Timestamp drops the zone and sub-millisecond digits.
        preparedStatement.setObject(parameterIndex, value.format(TIMESTAMP_TZ_WRITE_FORMATTER) + " UTC",
                Types.TIMESTAMP_WITH_TIMEZONE);
    }

    @Override
    protected void setTimestampTzNull(int parameterIndex) throws SQLException {
        // These drivers reject TIMESTAMP_WITH_TIMEZONE in setNull; SQL NULL is coerced by the target column.
        preparedStatement.setNull(parameterIndex,
                config != null && config.getTableType() == org.apache.doris.thrift.TOdbcTableType.PRESTO
                        ? Types.VARCHAR : Types.NULL);
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.DATETIME
                    || outputTable.getColumnType(i).getType() == Type.DATETIMEV2) {
                block.add(new Timestamp[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.ARRAY) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        switch (type.getType()) {
            case TIMESTAMPTZ: {
                if (config != null && config.getTableType() == org.apache.doris.thrift.TOdbcTableType.PRESTO) {
                    // PrestoDB does not implement typed getObject for zoned timestamps.
                    Timestamp value = resultSet.getTimestamp(columnIndex + 1);
                    return value == null ? null : LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC);
                }
                // JdbcScanNode projects to UTC before the driver can lose a named-zone overlap offset.
                // JNI carries instants as UTC components, not the source zone's wall clock.
                ZonedDateTime value = resultSet.getObject(columnIndex + 1, ZonedDateTime.class);
                return value == null ? null : LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.UTC);
            }
            case BOOLEAN:
                return resultSet.getObject(columnIndex + 1, Boolean.class);
            case TINYINT:
                return resultSet.getObject(columnIndex + 1, Byte.class);
            case SMALLINT:
                return resultSet.getObject(columnIndex + 1, Short.class);
            case INT:
                return resultSet.getObject(columnIndex + 1, Integer.class);
            case BIGINT:
                return resultSet.getObject(columnIndex + 1, Long.class);
            case FLOAT:
                return resultSet.getObject(columnIndex + 1, Float.class);
            case DOUBLE:
                return resultSet.getObject(columnIndex + 1, Double.class);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return resultSet.getObject(columnIndex + 1, BigDecimal.class);
            case DATE:
            case DATEV2:
                return resultSet.getObject(columnIndex + 1, LocalDate.class);
            case DATETIME:
            case DATETIMEV2:
                return resultSet.getObject(columnIndex + 1, Timestamp.class);
            case CHAR:
            case VARCHAR:
            case STRING:
                return resultSet.getObject(columnIndex + 1, String.class);
            case ARRAY: {
                Array array = resultSet.getArray(columnIndex + 1);
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
                return resultSet.getObject(columnIndex + 1, byte[].class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case DATETIME:
            case DATETIMEV2:
                return createConverter(
                        input -> ((Timestamp) input).toLocalDateTime(), java.time.LocalDateTime.class);
            case ARRAY:
                return createConverter(
                        input -> convertArray((List<?>) input, columnType.getChildTypes().get(0)), List.class);
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
                            : convertTimestampTzArrayElement(element));
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

    private static LocalDateTime convertTimestampTzArrayElement(Object element) {
        // PrestoDB returns zoned text for arrays, whereas Trino returns Timestamp objects.
        java.time.Instant instant;
        if (element instanceof String) {
            instant = ZonedDateTime.parse((String) element, ARRAY_TIMESTAMP_FORMATTER).toInstant();
        } else {
            instant = ((Timestamp) element).toInstant();
        }
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

}
