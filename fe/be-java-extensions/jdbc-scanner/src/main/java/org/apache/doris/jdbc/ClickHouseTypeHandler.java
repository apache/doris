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

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * ClickHouse-specific type handler.
 * Key specializations:
 * - ARRAY: direct getArray() call with element type conversion
 * - All numeric types use getObject(Class) for proper null handling
 */
public class ClickHouseTypeHandler extends DefaultTypeHandler {

    @Override
    public Object getColumnValue(ResultSet rs, int columnIndex, ColumnType type,
                                 ResultSetMetaData metadata) throws SQLException {
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
            case LARGEINT:
                return rs.getObject(columnIndex, BigInteger.class);
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
                return rs.getObject(columnIndex, LocalDateTime.class);
            case CHAR:
            case VARCHAR:
            case STRING:
                return rs.getObject(columnIndex, String.class);
            case ARRAY: {
                Array array = rs.getArray(columnIndex);
                if (array == null) {
                    return null;
                }
                return convertArrayToList(array.getArray());
            }
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    public ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        if (columnType.getType() == ColumnType.Type.ARRAY) {
            return createConverter(
                    input -> convertArray((List<?>) input, columnType.getChildTypes().get(0)),
                    List.class);
        }
        return null;
    }

    private static List<?> convertArrayToList(Object array) {
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

    /**
     * Convert array elements to the expected Doris column type.
     * ClickHouse JDBC driver may return unsigned integer array elements as wider
     * Java types (e.g., UInt32 elements as BigInteger instead of Long).
     */
    private List<?> convertArray(List<?> array, ColumnType type) {
        if (array == null) {
            return null;
        }
        switch (type.getType()) {
            case SMALLINT: {
                List<Short> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else if (element instanceof Number) {
                        result.add(((Number) element).shortValue());
                    } else {
                        throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                    }
                }
                return result;
            }
            case INT: {
                List<Integer> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else if (element instanceof Number) {
                        result.add(((Number) element).intValue());
                    } else {
                        throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                    }
                }
                return result;
            }
            case BIGINT: {
                List<Long> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else if (element instanceof Number) {
                        result.add(((Number) element).longValue());
                    } else {
                        throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                    }
                }
                return result;
            }
            case LARGEINT: {
                List<BigInteger> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else if (element instanceof BigInteger) {
                        result.add((BigInteger) element);
                    } else if (element instanceof BigDecimal) {
                        result.add(((BigDecimal) element).toBigInteger());
                    } else if (element instanceof Number) {
                        result.add(BigInteger.valueOf(((Number) element).longValue()));
                    } else {
                        throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                    }
                }
                return result;
            }
            case STRING: {
                List<String> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else if (element instanceof InetAddress) {
                        result.add(((InetAddress) element).getHostAddress());
                    } else {
                        result.add(element.toString());
                    }
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
}

