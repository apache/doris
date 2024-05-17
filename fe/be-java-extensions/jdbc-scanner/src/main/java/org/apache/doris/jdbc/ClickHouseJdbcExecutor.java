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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
                return resultSet.getObject(columnIndex + 1);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        if (columnType.getType() == Type.ARRAY) {
            return createConverter(
                    (Object input) -> convertArray(input, columnType.getChildTypes().get(0)),
                    List.class);
        } else {
            return null;
        }
    }

    private <T, U> List<U> convertArray(T[] input, Function<T, U> converter) {
        if (input == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(input)
                .map(converter)
                .collect(Collectors.toList());
    }

    private List<?> convertArray(Object input, ColumnType childType) {
        if (input == null) {
            return Collections.emptyList();
        }
        if (childType.isArray()) {
            ColumnType subType = childType.getChildTypes().get(0);
            Object[] array = (Object[]) input;
            List<Object> convertedList = new ArrayList<>();
            for (Object subArray : array) {
                convertedList.add(convertArray(subArray, subType));
            }
            return convertedList;
        }
        if (input instanceof Object[]) {
            Object[] arrayInput = (Object[]) input;
            switch (childType.getType()) {
                case SMALLINT:
                    return input instanceof Byte[]
                            ? convertArray((Byte[]) input,
                                byteValue -> byteValue != null ? (short) (byte) byteValue : null)
                            : convertArray((Number[]) arrayInput,
                                    number -> number != null ? number.shortValue() : null);
                case INT:
                    return input instanceof Short[]
                            ? convertArray((Short[]) input,
                                shortValue -> shortValue != null ? (int) (short) shortValue : null)
                            : convertArray((Number[]) arrayInput, number -> number != null ? number.intValue() : null);
                case BIGINT:
                    return input instanceof Integer[]
                            ? convertArray((Integer[]) input,
                                intValue -> intValue != null ? (long) (int) intValue : null)
                            : convertArray((Number[]) arrayInput, number -> number != null ? number.longValue() : null);
                case LARGEINT:
                    return input instanceof Long[]
                            ? convertArray((Long[]) input,
                                longValue -> longValue != null ? BigInteger.valueOf(longValue) : null)
                            : convertArray((Number[]) arrayInput,
                                    number -> number != null ? BigInteger.valueOf(number.longValue()) : null);
                case STRING:
                    if (input instanceof InetAddress[]) {
                        return convertArray((InetAddress[]) input,
                                inetAddress -> inetAddress != null ? inetAddress.getHostAddress() : null);
                    } else {
                        return convertArray(arrayInput, element -> element != null ? element.toString() : null);
                    }
                default:
                    return Arrays.asList(arrayInput);
            }
        } else {
            return convertPrimitiveArray(input, childType);
        }
    }

    private List<?> convertPrimitiveArray(Object input, ColumnType childType) {
        int length = Array.getLength(input);
        List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            Object element = Array.get(input, i);
            switch (childType.getType()) {
                case SMALLINT:
                    if (input instanceof byte[]) {
                        list.add((short) (byte) element);
                    } else {
                        list.add(element);
                    }
                    break;
                case INT:
                    if (input instanceof short[]) {
                        list.add((int) (short) element);
                    } else {
                        list.add(element);
                    }
                    break;
                case BIGINT:
                    if (input instanceof int[]) {
                        list.add((long) (int) element);
                    } else {
                        list.add(element);
                    }
                    break;
                case LARGEINT:
                    if (input instanceof long[]) {
                        list.add(BigInteger.valueOf((long) element));
                    } else {
                        list.add(element);
                    }
                    break;
                default:
                    list.add(element);
                    break;
            }
        }
        return list;
    }
}
