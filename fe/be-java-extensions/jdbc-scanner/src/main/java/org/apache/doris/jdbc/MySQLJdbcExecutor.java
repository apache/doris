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
import org.apache.doris.thrift.TJdbcOperation;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLJdbcExecutor extends BaseJdbcExecutor {
    private static final Logger LOG = Logger.getLogger(MySQLJdbcExecutor.class);

    private static final Gson gson = new Gson();

    public MySQLJdbcExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
        System.setProperty("com.mysql.cj.disableAbandonedConnectionCleanup", "true");
    }

    @Override
    protected void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the MySQL driver
            // attempts to drain the connection by reading all the results.
            connection.abort(MoreExecutors.directExecutor());
        }
    }

    @Override
    protected void initializeStatement(Connection conn, JdbcDataSourceConfig config, String sql) throws SQLException {
        if (config.getOp() == TJdbcOperation.READ) {
            conn.setAutoCommit(false);
            Preconditions.checkArgument(sql != null, "SQL statement cannot be null for READ operation.");
            stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);  // MySQL: signal streaming results with Integer.MIN_VALUE
            batchSizeNum = config.getBatchSize();
        } else {
            LOG.info("Insert SQL: " + sql);
            preparedStatement = conn.prepareStatement(sql);
        }
    }

    @Override
    protected void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
            VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (replaceStringList[i].equals("bitmap") || replaceStringList[i].equals("hll")) {
                block.add(new byte[batchSizeNum][]);
            } else if (outputTable.getColumnType(i).getType() == Type.ARRAY) {
                block.add(new String[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == Type.TINYINT
                    || outputTable.getColumnType(i).getType() == Type.SMALLINT
                    || outputTable.getColumnType(i).getType() == Type.LARGEINT
                    || outputTable.getColumnType(i).getType() == Type.STRING) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    @Override
    protected Object getColumnValue(int columnIndex, ColumnType type, String[] replaceStringList) throws SQLException {
        if (replaceStringList[columnIndex].equals("bitmap") || replaceStringList[columnIndex].equals("hll")) {
            byte[] data = resultSet.getBytes(columnIndex + 1);
            if (resultSet.wasNull()) {
                return null;
            }
            return data;
        } else {
            switch (type.getType()) {
                case BOOLEAN:
                    return resultSet.getObject(columnIndex + 1, Boolean.class);
                case TINYINT:
                case SMALLINT:
                case LARGEINT:
                    return resultSet.getObject(columnIndex + 1);
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
                    return resultSet.getObject(columnIndex + 1, LocalDateTime.class);
                case CHAR:
                case VARCHAR:
                case ARRAY:
                    return resultSet.getObject(columnIndex + 1, String.class);
                case STRING:
                    return resultSet.getObject(columnIndex + 1);
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + type.getType());
            }
        }
    }

    @Override
    protected ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
        switch (columnType.getType()) {
            case TINYINT:
                return createConverter(input -> {
                    if (input instanceof Integer) {
                        return ((Integer) input).byteValue();
                    } else {
                        return input;
                    }
                }, Byte.class);
            case SMALLINT:
                return createConverter(input -> {
                    if (input instanceof Integer) {
                        return ((Integer) input).shortValue();
                    } else {
                        return input;
                    }
                }, Short.class);
            case LARGEINT:
                return createConverter(input -> {
                    if (input instanceof String) {
                        return new BigInteger((String) input);
                    } else {
                        return input;
                    }
                }, BigInteger.class);
            case STRING:
                if (replaceString.equals("bitmap") || replaceString.equals("hll")) {
                    return null;
                } else {
                    return createConverter(input -> {
                        if (input instanceof byte[]) {
                            return mysqlByteArrayToHexString((byte[]) input);
                        } else if (input instanceof java.sql.Time) {
                            return timeToString((java.sql.Time) input);
                        } else {
                            return input.toString();
                        }
                    }, String.class);
                }
            case ARRAY:
                return createConverter(
                        (Object input) -> convertArray(input, columnType.getChildTypes().get(0)),
                        List.class);
            default:
                return null;
        }
    }

    private Object convertArray(Object input, ColumnType columnType) {
        java.lang.reflect.Type listType = getListTypeForArray(columnType);
        if (columnType.getType() == Type.BOOLEAN) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item instanceof Boolean) {
                    return item;
                } else if (item instanceof Number) {
                    return ((Number) item).intValue() != 0;
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to Boolean.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == Type.DATE || columnType.getType() == Type.DATEV2) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item instanceof String) {
                    return LocalDate.parse((String) item);
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDate.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == Type.DATETIME || columnType.getType() == Type.DATETIMEV2) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item instanceof String) {
                    return LocalDateTime.parse(
                            (String) item,
                            new DateTimeFormatterBuilder()
                                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                                    .appendFraction(ChronoField.MILLI_OF_SECOND, columnType.getPrecision(),
                                            columnType.getPrecision(), true)
                                    .toFormatter());
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDateTime.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == Type.ARRAY) {
            List<?> list = gson.fromJson((String) input, listType);
            return list.stream()
                    .map(item -> convertArray(gson.toJson(item), columnType.getChildTypes().get(0)))
                    .collect(Collectors.toList());
        } else {
            return gson.fromJson((String) input, listType);
        }
    }

    private java.lang.reflect.Type getListTypeForArray(ColumnType type) {
        switch (type.getType()) {
            case BOOLEAN:
                return new TypeToken<List<Boolean>>() {
                }.getType();
            case TINYINT:
                return new TypeToken<List<Byte>>() {
                }.getType();
            case SMALLINT:
                return new TypeToken<List<Short>>() {
                }.getType();
            case INT:
                return new TypeToken<List<Integer>>() {
                }.getType();
            case BIGINT:
                return new TypeToken<List<Long>>() {
                }.getType();
            case LARGEINT:
                return new TypeToken<List<BigInteger>>() {
                }.getType();
            case FLOAT:
                return new TypeToken<List<Float>>() {
                }.getType();
            case DOUBLE:
                return new TypeToken<List<Double>>() {
                }.getType();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new TypeToken<List<BigDecimal>>() {
                }.getType();
            case DATE:
            case DATEV2:
                return new TypeToken<List<LocalDate>>() {
                }.getType();
            case DATETIME:
            case DATETIMEV2:
                return new TypeToken<List<LocalDateTime>>() {
                }.getType();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new TypeToken<List<String>>() {
                }.getType();
            case ARRAY:
                java.lang.reflect.Type childType = getListTypeForArray(type.getChildTypes().get(0));
                TypeToken<?> token = TypeToken.getParameterized(List.class, childType);
                return token.getType();
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type.getType());
        }
    }

    private String mysqlByteArrayToHexString(byte[] bytes) {
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
}
