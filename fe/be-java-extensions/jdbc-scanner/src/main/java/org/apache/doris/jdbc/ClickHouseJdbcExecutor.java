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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseJdbcExecutor extends BaseJdbcExecutor {

    public ClickHouseJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (outputTable.getColumnType(i).getType() == Type.ARRAY) {
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
            case TINYINT:
                return resultSet.getObject(columnIndex + 1, Byte.class);
            case SMALLINT:
                return resultSet.getObject(columnIndex + 1, Short.class);
            case INT:
                return resultSet.getObject(columnIndex + 1, Integer.class);
            case BIGINT:
                return resultSet.getObject(columnIndex + 1, Long.class);
            case LARGEINT:
                return resultSet.getObject(columnIndex + 1, BigInteger.class);
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
                return resultSet.getObject(columnIndex + 1, LocalDateTime.class);
            case CHAR:
            case VARCHAR:
            case STRING:
                return resultSet.getObject(columnIndex + 1, String.class);
            case ARRAY:
                return convertArrayToList(resultSet.getArray(columnIndex + 1).getArray());
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        if (columnType.getType() == Type.ARRAY) {
            return createConverter(
                    (Object input) -> convertArray((List<?>) input, columnType.getChildTypes().get(0)),
                    List.class);
        } else {
            return null;
        }
    }

    private List<Object> convertArrayToList(Object array) {
        if (array == null) {
            return null;
        }

        int length = Array.getLength(array);
        List<Object> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            Object element = Array.get(array, i);
            list.add(element);
        }

        return list;
    }

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
                    } else {
                        if (element instanceof Byte) {
                            result.add(((Byte) element).shortValue());
                        } else if (element instanceof Number) {
                            result.add(((Number) element).shortValue());
                        } else {
                            throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                        }
                    }
                }
                return result;
            }
            case INT: {
                List<Integer> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else {
                        if (element instanceof Short) {
                            result.add(((Short) element).intValue());
                        } else if (element instanceof Number) {
                            result.add(((Number) element).intValue());
                        } else {
                            throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                        }
                    }
                }
                return result;
            }
            case BIGINT: {
                List<Long> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else {
                        if (element instanceof Integer) {
                            result.add(((Integer) element).longValue());
                        } else if (element instanceof Number) {
                            result.add(((Number) element).longValue());
                        } else {
                            throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                        }
                    }
                }
                return result;
            }
            case LARGEINT: {
                List<BigInteger> result = Lists.newArrayList();
                for (Object element : array) {
                    if (element == null) {
                        result.add(null);
                    } else {
                        if (element instanceof BigDecimal) {
                            result.add(((BigDecimal) element).toBigInteger());
                        } else if (element instanceof Number) {
                            result.add(BigInteger.valueOf(((Number) element).longValue()));
                        } else {
                            throw new IllegalArgumentException("Unsupported element type: " + element.getClass());
                        }
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
            case ARRAY:
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
            default:
                return array;
        }
    }
}
