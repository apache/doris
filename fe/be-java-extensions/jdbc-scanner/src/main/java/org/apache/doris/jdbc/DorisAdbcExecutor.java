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

import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TJdbcExecutorCtorParams;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DorisAdbcExecutor implements JdbcExecutor {
    private static final Logger LOG = Logger.getLogger(DorisAdbcExecutor.class);
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();

    private static final Gson gson = new Gson();

    private final String ticket;
    private final String locationUri;
    private final String ip;
    private final int arrowPort;
    private final String user;
    private final String password;

    private int curBlockRows = 0;
    private VectorTable outputTable = null;
    private List<Object[]> block = null;

    private FlightStream stream;
    private FlightClient clientBE;
    private FlightSqlClient sqlClientBE;
    private FlightClient clientFE;
    private int columnCount = 0;
    private int streamIndex = -1;
    private boolean hasStreamNext = true;

    public DorisAdbcExecutor(byte[] thriftParams) throws Exception {
        TJdbcExecutorCtorParams request = new TJdbcExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        String arrowHost = request.arrow_host_port;
        String[] arrowHostSplit = arrowHost.trim().split(":");
        this.ticket = request.ticket;
        this.locationUri = request.location_uri;
        this.ip = arrowHostSplit[0].trim();
        this.arrowPort = Integer.parseInt(arrowHostSplit[1].trim());
        this.user = request.jdbc_user;
        this.password = request.jdbc_password;
    }

    @Override
    public int read() throws JdbcExecutorException {
        try {
            Location location = new Location(new URI(locationUri));
            Ticket ticket = new Ticket(this.ticket.getBytes());

            BufferAllocator allocatorFE = new RootAllocator(Integer.MAX_VALUE);
            final Location clientLocationFE = new Location(new URI("grpc", null, ip, arrowPort, null, null, null));
            this.clientFE = FlightClient.builder(allocatorFE, clientLocationFE).build();
            CredentialCallOption credentialCallOption = clientFE.authenticateBasicToken(user, password).get();
            this.clientBE = FlightClient.builder(allocatorFE, location).build();
            this.sqlClientBE = new org.apache.arrow.flight.sql.FlightSqlClient(clientBE);
            this.stream = sqlClientBE.getStream(ticket, credentialCallOption);

            this.columnCount = stream.getSchema().getFields().size();
            this.block = new ArrayList<>(columnCount);
            return columnCount;
        } catch (Exception e) {
            throw new JdbcExecutorException("Arrow flight sql has error: ", e);
        }
    }

    @Override
    public int write(Map<String, String> params) throws JdbcExecutorException {
        return 0;
    }

    @Override
    public long getBlockAddress(int batchSize, Map<String, String> outputParams) throws JdbcExecutorException {
        try {
            if (outputTable != null) {
                outputTable.close();
            }

            outputTable = VectorTable.createWritableTable(outputParams, 0);

            String isNullableString = outputParams.get("is_nullable");
            String replaceString = outputParams.get("replace_string");

            if (isNullableString == null || replaceString == null) {
                throw new IllegalArgumentException(
                    "Output parameters 'is_nullable' and 'replace_string' are required.");
            }

            String[] nullableList = isNullableString.split(",");
            String[] replaceStringList = replaceString.split(",");
            curBlockRows = 0;

            initializeBlock(columnCount, replaceStringList, batchSize, outputTable);

            while (curBlockRows < batchSize && streamNext()) {
                VectorSchemaRoot streamRoot = stream.getRoot();
                for (int i = 0; i < columnCount; ++i) {
                    block.get(i)[curBlockRows] = streamRoot.getVector(i).getObject(streamIndex);
                }
                curBlockRows++;
            }

            for (int i = 0; i < columnCount; ++i) {
                ColumnType type = outputTable.getColumnType(i);
                Object[] columnData = block.get(i);
                Class<?> componentType = columnData.getClass().getComponentType();
                Object[] newColumn = (Object[]) Array.newInstance(componentType, curBlockRows);
                System.arraycopy(columnData, 0, newColumn, 0, curBlockRows);
                boolean isNullable = Boolean.parseBoolean(nullableList[i]);
                outputTable.appendData(i, newColumn, getOutputConverter(type, replaceStringList[i]), isNullable);
            }
        } catch (Exception e) {
            LOG.warn("adbc get block address exception: ", e);
            throw new JdbcExecutorException("adbc get block address: ", e);
        } finally {
            block.clear();
        }
        return outputTable.getMetaAddress();
    }

    @Override
    public void close() throws JdbcExecutorException, Exception {
        sqlClientBE.close();
        clientFE.close();
        clientBE.close();
    }

    @Override
    public void openTrans() throws JdbcExecutorException {
        // nothing
    }

    @Override
    public void commitTrans() throws JdbcExecutorException {
        // nothing
    }

    @Override
    public void rollbackTrans() throws JdbcExecutorException {
        // nothing
    }

    @Override
    public int getCurBlockRows() {
        return curBlockRows;
    }

    @Override
    public boolean hasNext() throws JdbcExecutorException {
        return hasStreamNext;
    }

    private void initializeBlock(int columnCount, String[] replaceStringList, int batchSizeNum,
                                   VectorTable outputTable) {
        for (int i = 0; i < columnCount; ++i) {
            if (replaceStringList[i].equals("bitmap") || replaceStringList[i].equals("hll")) {
                block.add(new byte[batchSizeNum][]);
            } else if (replaceStringList[i].equals("variant")) {
                block.add(new String[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == ColumnType.Type.ARRAY) {
                block.add(new String[batchSizeNum]);
            } else if (outputTable.getColumnType(i).getType() == ColumnType.Type.TINYINT
                    || outputTable.getColumnType(i).getType() == ColumnType.Type.SMALLINT
                    || outputTable.getColumnType(i).getType() == ColumnType.Type.LARGEINT
                    || outputTable.getColumnType(i).getType() == ColumnType.Type.STRING) {
                block.add(new Object[batchSizeNum]);
            } else {
                block.add(outputTable.getColumn(i).newObjectContainerArray(batchSizeNum));
            }
        }
    }

    private ColumnValueConverter getOutputConverter(ColumnType columnType, String replaceString) {
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

    private ColumnValueConverter createConverter(
            Function<Object, ?> converterFunction, Class<?> type) {
        return (Object[] columnData) -> {
            Object[] result = (Object[]) Array.newInstance(type, columnData.length);
            for (int i = 0; i < columnData.length; i++) {
                result[i] = columnData[i] != null ? converterFunction.apply(columnData[i]) : null;
            }
            return result;
        };
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

    protected String timeToString(java.sql.Time time) {
        if (time == null) {
            return null;
        } else {
            long milliseconds = time.getTime() % 1000L;
            if (milliseconds > 0) {
                return String.format("%s.%03d", time, milliseconds);
            } else {
                return time.toString();
            }
        }
    }

    private Object convertArray(Object input, ColumnType columnType) {
        java.lang.reflect.Type listType = getListTypeForArray(columnType);
        if (columnType.getType() == ColumnType.Type.BOOLEAN) {
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
        } else if (columnType.getType() == ColumnType.Type.DATE || columnType.getType() == ColumnType.Type.DATEV2) {
            List<?> list = gson.fromJson((String) input, List.class);
            return list.stream().map(item -> {
                if (item instanceof String) {
                    return LocalDate.parse((String) item);
                } else {
                    throw new IllegalArgumentException("Cannot convert " + item + " to LocalDate.");
                }
            }).collect(Collectors.toList());
        } else if (columnType.getType() == ColumnType.Type.DATETIME
                || columnType.getType() == ColumnType.Type.DATETIMEV2) {
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
        } else if (columnType.getType() == ColumnType.Type.ARRAY) {
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

    private boolean streamNext() {
        if (streamIndex == -1) {
            this.streamIndex = 0;
            this.hasStreamNext = stream.next();
            return hasStreamNext;
        }

        VectorSchemaRoot streamRoot = stream.getRoot();
        if (streamRoot.getFieldVectors().isEmpty()) {
            return false;
        }

        int streamCount = streamRoot.getFieldVectors().get(0).getValueCount();
        if (streamCount >= this.streamIndex + 1) {
            this.streamIndex = 0;
            this.hasStreamNext = stream.next();
            return hasStreamNext;
        }

        this.streamIndex++;
        this.hasStreamNext = true;
        return hasStreamNext;
    }

    public void cleanDataSource() {
    }

    public void testConnection() throws JdbcExecutorException {
    }
}
