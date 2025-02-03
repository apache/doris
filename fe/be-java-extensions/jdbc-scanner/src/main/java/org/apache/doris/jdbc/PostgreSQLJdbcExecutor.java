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
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class PostgreSQLJdbcExecutor extends BaseJdbcExecutor {
    private static final Logger LOG = Logger.getLogger(PostgreSQLJdbcExecutor.class);

    public PostgreSQLJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.DATETIME
                    || outputTable.getColumnType(i).getType() == Type.DATETIMEV2) {
                block.add(new Object[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.STRING
                    || outputTable.getColumnType(i).getType() == Type.ARRAY) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        switch (type.getType()) {
            case BOOLEAN:
                return resultSet.getObject(columnIndex + 1, Boolean.class);
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
            case CHAR:
            case VARCHAR:
            case STRING:
                return resultSet.getObject(columnIndex + 1);
            case ARRAY:
                java.sql.Array array = resultSet.getArray(columnIndex + 1);
                return array == null ? null : convertArrayToList(array.getArray());
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case DATETIME:
            case DATETIMEV2:
                return createConverter(input -> {
                    if (input instanceof Timestamp) {
                        return ((Timestamp) input).toLocalDateTime();
                    } else if (input instanceof OffsetDateTime) {
                        return ((OffsetDateTime) input).toLocalDateTime();
                    } else {
                        return input;
                    }
                }, LocalDateTime.class);
            case CHAR:
                return createConverter(
                        input -> trimSpaces(input.toString()), String.class);
            case STRING:
                return createConverter(input -> {
                    if (input instanceof java.sql.Time) {
                        return timeToString((java.sql.Time) input);
                    } else if (input instanceof byte[]) {
                        return pgByteArrayToHexString((byte[]) input);
                    } else {
                        return input.toString();
                    }
                }, String.class);
            case ARRAY:
                return createConverter(
                        (Object input) -> convertArray((List<?>) input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    private static String pgByteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder("\\x");
        for (byte b : bytes) {
            hexString.append(String.format("%02x", b & 0xff));
        }
        return hexString.toString();
    }

    private List<Object> convertArrayToList(Object array) {
        if (array == null) {
            return null;
        }

        int length = java.lang.reflect.Array.getLength(array);
        List<Object> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            Object element = java.lang.reflect.Array.get(array, i);
            list.add(element);
        }

        return list;
    }

    private List<?> convertArray(List<?> array, ColumnType type) {
        if (array == null) {
            return null;
        }
        switch (type.getType()) {
            case DATE:
            case DATEV2: {
                List<LocalDate> result = new ArrayList<>();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else {
                        result.add(((Date) element).toLocalDate());
                    }
                }
                return result;
            }
            case DATETIME:
            case DATETIMEV2: {
                List<LocalDateTime> result = new ArrayList<>();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else {
                        if (element instanceof Timestamp) {
                            result.add(((Timestamp) element).toLocalDateTime());
                        } else if (element instanceof OffsetDateTime) {
                            result.add(((OffsetDateTime) element).toLocalDateTime());
                        } else {
                            result.add((LocalDateTime) element);
                        }
                    }
                }
                return result;
            }
            case ARRAY:
                List<List<?>> resultArray = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        resultArray.add(null);
                    } else {
                        List<?> nestedList = convertArrayToList(element);
                        resultArray.add(convertArray(nestedList, type.getChildTypes().get(0)));
                    }
                }
                return resultArray;
            default:
                return array;
        }
    }
}
