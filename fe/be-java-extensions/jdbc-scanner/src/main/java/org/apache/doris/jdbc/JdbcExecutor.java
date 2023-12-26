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
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TJdbcExecutorCtorParams;
import org.apache.doris.thrift.TJdbcOperation;
import org.apache.doris.thrift.TOdbcTableType;

import com.alibaba.druid.pool.DruidDataSource;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;
import com.google.common.base.Preconditions;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcExecutor {
    private static final Logger LOG = Logger.getLogger(JdbcExecutor.class);
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();
    private Connection conn = null;
    private PreparedStatement preparedStatement = null;
    private Statement stmt = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData resultSetMetaData = null;
    private List<String> resultColumnTypeNames = null;
    private List<Object[]> block = null;
    private VectorTable outputTable = null;
    private int batchSizeNum = 0;
    private int curBlockRows = 0;
    private static final byte[] emptyBytes = new byte[0];
    private DruidDataSource druidDataSource = null;
    private byte[] druidDataSourceLock = new byte[0];
    private int minPoolSize;
    private int maxPoolSize;
    private int minIdleSize;
    private int maxIdleTime;
    private int maxWaitTime;
    private TOdbcTableType tableType;

    public JdbcExecutor(byte[] thriftParams) throws Exception {
        TJdbcExecutorCtorParams request = new TJdbcExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        tableType = request.table_type;
        minPoolSize = Integer.valueOf(System.getProperty("JDBC_MIN_POOL", "1"));
        maxPoolSize = Integer.valueOf(System.getProperty("JDBC_MAX_POOL", "100"));
        maxIdleTime = Integer.valueOf(System.getProperty("JDBC_MAX_IDLE_TIME", "300000"));
        maxWaitTime = Integer.valueOf(System.getProperty("JDBC_MAX_WAIT_TIME", "5000"));
        minIdleSize = minPoolSize > 0 ? 1 : 0;
        LOG.info("JdbcExecutor set minPoolSize = " + minPoolSize
                + ", maxPoolSize = " + maxPoolSize
                + ", maxIdleTime = " + maxIdleTime
                + ", maxWaitTime = " + maxWaitTime
                + ", minIdleSize = " + minIdleSize);
        init(request.driver_path, request.statement, request.batch_size, request.jdbc_driver_class,
                request.jdbc_url, request.jdbc_user, request.jdbc_password, request.op, request.table_type);
    }

    public void close() throws Exception {
        if (resultSet != null) {
            resultSet.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
        if (minIdleSize == 0) {
            // it can be immediately closed if there is no need to maintain the cache of datasource
            druidDataSource.close();
            JdbcDataSource.getDataSource().getSourcesMap().clear();
            druidDataSource = null;
        }
        resultSet = null;
        stmt = null;
        conn = null;
    }

    public int read() throws UdfRuntimeException {
        try {
            resultSet = ((PreparedStatement) stmt).executeQuery();
            resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            resultColumnTypeNames = new ArrayList<>(columnCount);
            block = new ArrayList<>(columnCount);
            if (isNebula()) {
                for (int i = 0; i < columnCount; ++i) {
                    block.add((Object[]) Array.newInstance(Object.class, batchSizeNum));
                }
            } else {
                for (int i = 0; i < columnCount; ++i) {
                    resultColumnTypeNames.add(resultSetMetaData.getColumnClassName(i + 1));
                    block.add((Object[]) Array.newInstance(Object.class, batchSizeNum));
                }
            }
            return columnCount;
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        }
    }

    public long getBlockAddress(int batchSize, Map<String, String> outputParams)
            throws UdfRuntimeException {
        try {
            if (outputTable != null) {
                outputTable.close();
            }

            String isNullableString = outputParams.get("is_nullable");
            String replaceString = outputParams.get("replace_string");

            if (isNullableString == null || replaceString == null) {
                throw new IllegalArgumentException(
                        "Output parameters 'is_nullable' and 'replace_string' are required.");
            }

            String[] nullableList = isNullableString.split(",");
            String[] replaceStringList = replaceString.split(",");
            curBlockRows = 0;
            int columnCount = resultSetMetaData.getColumnCount();

            do {
                for (int i = 0; i < columnCount; ++i) {
                    boolean isBitmapOrHll =
                            replaceStringList[i].equals("bitmap")
                                    || replaceStringList[i].equals("hll");
                    block.get(i)[curBlockRows] = getColumnValue(tableType, i, isBitmapOrHll);
                }
                curBlockRows++;
            } while (curBlockRows < batchSize && resultSet.next());

            outputTable = VectorTable.createWritableTable(outputParams, curBlockRows);

            for (int i = 0; i < columnCount; ++i) {
                Object[] columnData = block.get(i);
                ColumnType type = outputTable.getColumnType(i);
                Class<?> clz = findNonNullClass(columnData, type);
                Object[] newColumn = (Object[]) Array.newInstance(clz, curBlockRows);
                System.arraycopy(columnData, 0, newColumn, 0, curBlockRows);
                boolean isNullable = Boolean.parseBoolean(nullableList[i]);
                outputTable.appendData(
                        i,
                        newColumn,
                        getOutputConverter(type, clz, replaceStringList[i]),
                        isNullable);
            }
        } catch (Exception e) {
            LOG.warn("jdbc get block address exception: ", e);
            throw new UdfRuntimeException("jdbc get block address: ", e);
        }
        return outputTable.getMetaAddress();
    }

    public int write(Map<String, String> params) throws UdfRuntimeException {
        VectorTable batchTable = VectorTable.createReadableTable(params);
        // Can't release or close batchTable, it's released by c++
        try {
            insert(batchTable);
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        }
        return batchTable.getNumRows();
    }

    public void openTrans() throws UdfRuntimeException {
        try {
            if (conn != null) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor open transaction has error: ", e);
        }
    }

    public void commitTrans() throws UdfRuntimeException {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor commit transaction has error: ", e);
        }
    }

    public void rollbackTrans() throws UdfRuntimeException {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor rollback transaction has error: ", e);
        }
    }

    public List<String> getResultColumnTypeNames() {
        return resultColumnTypeNames;
    }

    public int getCurBlockRows() {
        return curBlockRows;
    }

    public boolean hasNext() throws UdfRuntimeException {
        try {
            if (resultSet == null) {
                return false;
            }
            return resultSet.next();
        } catch (SQLException e) {
            throw new UdfRuntimeException("resultSet to get next error: ", e);
        }
    }

    private void init(String driverUrl, String sql, int batchSize, String driverClass, String jdbcUrl, String jdbcUser,
            String jdbcPassword, TJdbcOperation op, TOdbcTableType tableType) throws UdfRuntimeException {
        String druidDataSourceKey = JdbcDataSource.getDataSource().createCacheKey(jdbcUrl, jdbcUser, jdbcPassword,
                driverUrl, driverClass);
        try {
            if (isNebula()) {
                batchSizeNum = batchSize;
                Class.forName(driverClass);
                conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                stmt = conn.prepareStatement(sql);
            } else {
                ClassLoader parent = getClass().getClassLoader();
                ClassLoader classLoader = UdfUtils.getClassLoader(driverUrl, parent);
                druidDataSource = JdbcDataSource.getDataSource().getSource(druidDataSourceKey);
                if (druidDataSource == null) {
                    synchronized (druidDataSourceLock) {
                        druidDataSource = JdbcDataSource.getDataSource().getSource(druidDataSourceKey);
                        if (druidDataSource == null) {
                            long start = System.currentTimeMillis();
                            DruidDataSource ds = new DruidDataSource();
                            ds.setDriverClassLoader(classLoader);
                            ds.setDriverClassName(driverClass);
                            ds.setUrl(jdbcUrl);
                            ds.setUsername(jdbcUser);
                            ds.setPassword(jdbcPassword);
                            ds.setMinIdle(minIdleSize);
                            ds.setInitialSize(minPoolSize);
                            ds.setMaxActive(maxPoolSize);
                            ds.setMaxWait(maxWaitTime);
                            ds.setTestWhileIdle(true);
                            ds.setTestOnBorrow(false);
                            setValidationQuery(ds, tableType);
                            ds.setTimeBetweenEvictionRunsMillis(maxIdleTime / 5);
                            ds.setMinEvictableIdleTimeMillis(maxIdleTime);
                            druidDataSource = ds;
                            // and the default datasource init = 1, min = 1, max = 100, if one of connection idle
                            // time greater than 10 minutes. then connection will be retrieved.
                            JdbcDataSource.getDataSource().putSource(druidDataSourceKey, ds);
                            LOG.info("init datasource [" + (jdbcUrl + jdbcUser) + "] cost: " + (
                                    System.currentTimeMillis() - start) + " ms");
                        }
                    }
                }

                long start = System.currentTimeMillis();
                conn = druidDataSource.getConnection();
                LOG.info("get connection [" + (jdbcUrl + jdbcUser) + "] cost: " + (System.currentTimeMillis() - start)
                        + " ms");
                if (op == TJdbcOperation.READ) {
                    conn.setAutoCommit(false);
                    Preconditions.checkArgument(sql != null);
                    stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    if (tableType == TOdbcTableType.MYSQL) {
                        stmt.setFetchSize(Integer.MIN_VALUE);
                    } else {
                        stmt.setFetchSize(batchSize);
                    }
                    batchSizeNum = batchSize;
                } else {
                    LOG.info("insert sql: " + sql);
                    preparedStatement = conn.prepareStatement(sql);
                }
            }
        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("MalformedURLException to load class about " + driverUrl, e);
        } catch (SQLException e) {
            throw new UdfRuntimeException("Initialize datasource failed: ", e);
        } catch (FileNotFoundException e) {
            throw new UdfRuntimeException("FileNotFoundException failed: ", e);
        } catch (Exception e) {
            throw new UdfRuntimeException("Initialize datasource failed: ", e);
        }
    }

    private void setValidationQuery(DruidDataSource ds, TOdbcTableType tableType) {
        if (tableType == TOdbcTableType.ORACLE || tableType == TOdbcTableType.OCEANBASE_ORACLE) {
            ds.setValidationQuery("SELECT 1 FROM dual");
        } else if (tableType == TOdbcTableType.SAP_HANA) {
            ds.setValidationQuery("SELECT 1 FROM DUMMY");
        } else {
            ds.setValidationQuery("SELECT 1");
        }
    }

    public boolean isNebula() {
        return tableType == TOdbcTableType.NEBULA;
    }

    private Class<?> findNonNullClass(Object[] columnData, ColumnType type) {
        for (Object data : columnData) {
            if (data != null) {
                return data.getClass();
            }
        }
        switch (type.getType()) {
            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INT:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case LARGEINT:
                return BigInteger.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return BigDecimal.class;
            case DATE:
            case DATEV2:
                return LocalDate.class;
            case DATETIME:
            case DATETIMEV2:
                return LocalDateTime.class;
            case CHAR:
            case VARCHAR:
            case STRING:
                return String.class;
            case ARRAY:
                return List.class;
            default:
                throw new IllegalArgumentException(
                        "Unsupported column type: " + type.getType());
        }
    }

    public Object getColumnValue(TOdbcTableType tableType, int columnIndex, boolean isBitmapOrHll)
            throws SQLException {
        Object result;
        if (tableType == TOdbcTableType.NEBULA) {
            result = UdfUtils.convertObject((ValueWrapper) resultSet.getObject(columnIndex + 1));
        } else {
            result =
                    isBitmapOrHll
                            ? resultSet.getBytes(columnIndex + 1)
                            : resultSet.getObject(columnIndex + 1);
        }
        return result;
    }

    /*
    | Type                                        | Java Array Type            |
    |---------------------------------------------|----------------------------|
    | BOOLEAN                                     | Boolean[]                  |
    | TINYINT                                     | Byte[]                     |
    | SMALLINT                                    | Short[]                    |
    | INT                                         | Integer[]                  |
    | BIGINT                                      | Long[]                     |
    | LARGEINT                                    | BigInteger[]               |
    | FLOAT                                       | Float[]                    |
    | DOUBLE                                      | Double[]                   |
    | DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128 | BigDecimal[]               |
    | DATE, DATEV2                                | LocalDate[]                |
    | DATETIME, DATETIMEV2                        | LocalDateTime[]            |
    | CHAR, VARCHAR, STRING                       | String[]                   |
    | ARRAY                                       | List<Object>[]             |
    | MAP                                         | Map<Object, Object>[]      |
    | STRUCT                                      | Map<String, Object>[]      |
    */

    private ColumnValueConverter getOutputConverter(
            ColumnType columnType, Class clz, String replaceString) {
        switch (columnType.getType()) {
            case BOOLEAN:
                if (Integer.class.equals(clz)) {
                    return createConverter(input -> ((Integer) input) != 0, Boolean.class);
                }
                if (Byte.class.equals(clz)) {
                    return createConverter(input -> ((Byte) input) != 0, Boolean.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input ->
                                    Boolean.parseBoolean(
                                            String.valueOf(input).equals("1") ? "true" : "false"),
                            Boolean.class);
                }
                break;
            case TINYINT:
                if (Integer.class.equals(clz)) {
                    return createConverter(input -> ((Integer) input).byteValue(), Byte.class);
                }
                if (Short.class.equals(clz)) {
                    return createConverter(input -> ((Short) input).byteValue(), Byte.class);
                }
                if (Object.class.equals(clz)) {
                    return createConverter(
                            input -> (byte) Integer.parseInt(String.valueOf(input)), Byte.class);
                }
                if (BigDecimal.class.equals(clz)) {
                    return createConverter(input -> ((BigDecimal) input).byteValue(), Byte.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> Byte.parseByte(String.valueOf(input)), Byte.class);
                }
                break;
            case SMALLINT:
                if (Integer.class.equals(clz)) {
                    return createConverter(input -> ((Integer) input).shortValue(), Short.class);
                }
                if (BigDecimal.class.equals(clz)) {
                    return createConverter(input -> ((BigDecimal) input).shortValue(), Short.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> Short.parseShort(String.valueOf(input)), Short.class);
                }
                if (Byte.class.equals(clz)) {
                    return createConverter(input -> ((Byte) input).shortValue(), Short.class);
                }
                if (com.clickhouse.data.value.UnsignedByte.class.equals(clz)) {
                    return createConverter(
                            input -> ((UnsignedByte) input).shortValue(), Short.class);
                }
                break;
            case INT:
                if (Long.class.equals(clz)) {
                    return createConverter(input -> ((Long) input).intValue(), Integer.class);
                }
                if (BigDecimal.class.equals(clz)) {
                    return createConverter(input -> ((BigDecimal) input).intValue(), Integer.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> Integer.parseInt(String.valueOf(input)), Integer.class);
                }
                if (Short.class.equals(clz)) {
                    return createConverter(input -> ((Short) input).intValue(), Integer.class);
                }
                if (com.clickhouse.data.value.UnsignedShort.class.equals(clz)) {
                    return createConverter(
                            input -> ((UnsignedShort) input).intValue(), Integer.class);
                }
                break;
            case BIGINT:
                if (BigDecimal.class.equals(clz)) {
                    return createConverter(input -> ((BigDecimal) input).longValue(), Long.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> Long.parseLong(String.valueOf(input)), Long.class);
                }
                if (Integer.class.equals(clz)) {
                    return createConverter(input -> ((Integer) input).longValue(), Long.class);
                }
                if (com.clickhouse.data.value.UnsignedInteger.class.equals(clz)) {
                    return createConverter(
                            input -> ((UnsignedInteger) input).longValue(), Long.class);
                }
                break;
            case LARGEINT:
                if (BigDecimal.class.equals(clz)) {
                    return createConverter(
                            input -> ((BigDecimal) input).toBigInteger(), BigInteger.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> new BigInteger(String.valueOf(input)), BigInteger.class);
                }
                if (Long.class.equals(clz)) {
                    return createConverter(
                            input -> BigInteger.valueOf((Long) input), BigInteger.class);
                }
                if (com.clickhouse.data.value.UnsignedLong.class.equals(clz)) {
                    return createConverter(
                            input -> ((UnsignedLong) input).bigIntegerValue(), BigInteger.class);
                }
                break;
            case DOUBLE:
                if (BigDecimal.class.equals(clz)) {
                    return createConverter(
                            input -> ((BigDecimal) input).doubleValue(), Double.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> Double.parseDouble(String.valueOf(input)), Double.class);
                }
                break;
            case FLOAT:
                return createConverter(
                        input -> Float.parseFloat(String.valueOf(input)), Float.class);
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return createConverter(
                        input -> new BigDecimal(String.valueOf(input)), BigDecimal.class);
            case DATE:
            case DATEV2:
                if (Date.class.equals(clz)) {
                    return createConverter(input -> ((Date) input).toLocalDate(), LocalDate.class);
                }
                if (Timestamp.class.equals(clz)) {
                    return createConverter(
                            input -> ((Timestamp) input).toLocalDateTime().toLocalDate(),
                            LocalDate.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> LocalDate.parse(String.valueOf(input)), LocalDate.class);
                }
                break;
            case DATETIME:
            case DATETIMEV2:
                if (Timestamp.class.equals(clz)) {
                    return createConverter(
                            input -> ((Timestamp) input).toLocalDateTime(), LocalDateTime.class);
                }
                if (OffsetDateTime.class.equals(clz)) {
                    return createConverter(
                            input -> ((OffsetDateTime) input).toLocalDateTime(),
                            LocalDateTime.class);
                }
                if (oracle.sql.TIMESTAMP.class.equals(clz)) {
                    return createConverter(
                            input -> {
                                try {
                                    return ((oracle.sql.TIMESTAMP) input)
                                            .timestampValue()
                                            .toLocalDateTime();
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            LocalDateTime.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input ->
                                    LocalDateTime.parse(
                                            String.valueOf(input),
                                            getDateTimeFormatter(String.valueOf(input))),
                            LocalDateTime.class);
                }
                break;
            case CHAR:
                return createConverter(
                        input -> trimSpaces(tableType, input.toString()), String.class);
            case VARCHAR:
            case STRING:
                if (byte[].class.equals(clz)) {
                    if (replaceString.equals("bitmap") || replaceString.equals("hll")) {
                        break;
                    } else {
                        return createConverter(
                                input -> byteArrayToHexString(tableType, (byte[]) input),
                                String.class);
                    }
                }
                if (Time.class.equals(clz)) {
                    return createConverter(
                            input -> timeToString((java.sql.Time) input), String.class);
                }
                if (oracle.sql.CLOB.class.equals(clz)) {
                    return createConverter(
                            input -> {
                                try {
                                    oracle.sql.CLOB clob = (oracle.sql.CLOB) input;
                                    return clob.getSubString(1, (int) clob.length());
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            String.class);
                }
                if (java.net.Inet4Address.class.equals(clz)) {
                    return createConverter(
                            input -> ((InetAddress) input).getHostAddress(), String.class);
                }
                if (java.net.Inet6Address.class.equals(clz)) {
                    return createConverter(
                            input -> {
                                String inetAddress = ((InetAddress) input).getHostAddress();
                                return simplifyIPv6Address(inetAddress);
                            },
                            String.class);
                } else {
                    return createConverter(Object::toString, String.class);
                }
            case ARRAY:
                if (java.sql.Array.class.equals(clz)) {
                    return createConverter(
                            input -> {
                                try {
                                    return Arrays.asList(
                                            (Object[]) ((java.sql.Array) input).getArray());
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            List.class);
                }
                if (String.class.equals(clz)) {
                    return createConverter(
                            input -> {
                                List<Object> list = parseArray(String.valueOf(input));
                                return convertArray(list, columnType.getChildTypes().get(0));
                            },
                            List.class);
                }
                if (tableType == TOdbcTableType.CLICKHOUSE) {
                    return createConverter(
                            input -> {
                                List<Object> list = convertClickHouseArray(input);
                                return convertArray(list, columnType.getChildTypes().get(0));
                            },
                            List.class);
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported column type: " + columnType.getType());
        }
        return null;
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

    private String byteArrayToHexString(TOdbcTableType tableType, byte[] columnData) {
        if (tableType == TOdbcTableType.MYSQL || tableType == TOdbcTableType.OCEANBASE) {
            return mysqlByteArrayToHexString(columnData);
        } else if (tableType == TOdbcTableType.POSTGRESQL) {
            return pgByteArrayToHexString(columnData);
        } else {
            return defaultByteArrayToHexString(columnData);
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

    private static String pgByteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder("\\x");
        for (byte b : bytes) {
            hexString.append(String.format("%02x", b & 0xff));
        }
        return hexString.toString();
    }

    private String defaultByteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex.toUpperCase());
        }
        return hexString.toString();
    }

    private String trimSpaces(TOdbcTableType tableType, String str) {
        if (tableType == TOdbcTableType.POSTGRESQL || tableType == TOdbcTableType.ORACLE) {
            int end = str.length() - 1;
            while (end >= 0 && str.charAt(end) == ' ') {
                end--;
            }
            return str.substring(0, end + 1);
        } else {
            return str;
        }
    }

    public String timeToString(java.sql.Time time) {
        long milliseconds = time.getTime() % 1000L;
        if (milliseconds > 0) {
            return String.format("%s.%03d", time, milliseconds);
        } else {
            return time.toString();
        }
    }

    private List<Object> convertArray(List<Object> list, ColumnType childType) {
        Class<?> clz = Object.class;
        for (Object data : list) {
            if (data != null) {
                clz = data.getClass();
                break;
            }
        }
        List<Object> convertedList = new ArrayList<>(list.size());
        ColumnValueConverter converter = getOutputConverter(childType, clz, "not_replace");
        for (Object element : list) {
            if (childType.isComplexType()) {
                convertedList.add(convertArray((List<Object>) element, childType));
            } else {
                if (converter != null) {
                    convertedList.add(converter.convert(new Object[] {element})[0]);
                } else {
                    convertedList.add(element);
                }
            }
        }
        return convertedList;
    }

    private static String simplifyIPv6Address(String address) {
        // Replace longest sequence of zeros with "::"
        String[] parts = address.split(":");
        int longestSeqStart = -1;
        int longestSeqLen = 0;
        int curSeqStart = -1;
        int curSeqLen = 0;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equals("0")) {
                if (curSeqStart == -1) {
                    curSeqStart = i;
                }
                curSeqLen++;
                if (curSeqLen > longestSeqLen) {
                    longestSeqStart = curSeqStart;
                    longestSeqLen = curSeqLen;
                }
            } else {
                curSeqStart = -1;
                curSeqLen = 0;
            }
        }
        if (longestSeqLen <= 1) {
            return address; // No sequences of zeros to replace
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < longestSeqStart; i++) {
            sb.append(parts[i]).append(':');
        }
        sb.append(':');
        for (int i = longestSeqStart + longestSeqLen; i < parts.length; i++) {
            sb.append(parts[i]);
            if (i < parts.length - 1) {
                sb.append(':');
            }
        }
        return sb.toString();
    }

    private static final Pattern MILLIS_PATTERN = Pattern.compile("(\\.\\d+)");

    public static DateTimeFormatter getDateTimeFormatter(String dateTimeString) {
        Matcher matcher = MILLIS_PATTERN.matcher(dateTimeString);
        int fractionDigits = 0;
        if (matcher.find()) {
            fractionDigits = matcher.group(1).length() - 1; // Subtract 1 to exclude the dot
        }
        fractionDigits = Math.min(fractionDigits, 6); // Limit the fraction digits to 6

        return new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.MILLI_OF_SECOND, fractionDigits, fractionDigits, true)
                .toFormatter();
    }

    private static final Map<Class<?>, Function<Object, List<Object>>> CK_ARRAY_CONVERTERS =
            new HashMap<>();

    static {
        CK_ARRAY_CONVERTERS.put(String[].class, res -> Arrays.asList((String[]) res));
        CK_ARRAY_CONVERTERS.put(boolean[].class, res -> toList((boolean[]) res));
        CK_ARRAY_CONVERTERS.put(Boolean[].class, res -> Arrays.asList((Boolean[]) res));
        CK_ARRAY_CONVERTERS.put(byte[].class, res -> toList((byte[]) res));
        CK_ARRAY_CONVERTERS.put(Byte[].class, res -> Arrays.asList((Byte[]) res));
        CK_ARRAY_CONVERTERS.put(LocalDate[].class, res -> Arrays.asList((LocalDate[]) res));
        CK_ARRAY_CONVERTERS.put(LocalDateTime[].class, res -> Arrays.asList((LocalDateTime[]) res));
        CK_ARRAY_CONVERTERS.put(float[].class, res -> toList((float[]) res));
        CK_ARRAY_CONVERTERS.put(Float[].class, res -> Arrays.asList((Float[]) res));
        CK_ARRAY_CONVERTERS.put(double[].class, res -> toList((double[]) res));
        CK_ARRAY_CONVERTERS.put(Double[].class, res -> Arrays.asList((Double[]) res));
        CK_ARRAY_CONVERTERS.put(short[].class, res -> toList((short[]) res));
        CK_ARRAY_CONVERTERS.put(Short[].class, res -> Arrays.asList((Short[]) res));
        CK_ARRAY_CONVERTERS.put(int[].class, res -> toList((int[]) res));
        CK_ARRAY_CONVERTERS.put(Integer[].class, res -> Arrays.asList((Integer[]) res));
        CK_ARRAY_CONVERTERS.put(long[].class, res -> toList((long[]) res));
        CK_ARRAY_CONVERTERS.put(Long[].class, res -> Arrays.asList((Long[]) res));
        CK_ARRAY_CONVERTERS.put(BigInteger[].class, res -> Arrays.asList((BigInteger[]) res));
        CK_ARRAY_CONVERTERS.put(BigDecimal[].class, res -> Arrays.asList((BigDecimal[]) res));
        CK_ARRAY_CONVERTERS.put(
                Inet4Address[].class,
                res ->
                        Arrays.stream((Inet4Address[]) res)
                                .map(addr -> addr == null ? null : addr.getHostAddress())
                                .collect(Collectors.toList()));
        CK_ARRAY_CONVERTERS.put(
                Inet6Address[].class,
                res ->
                        Arrays.stream((Inet6Address[]) res)
                                .map(addr -> addr == null ? null : simplifyIPv6Address(addr.getHostAddress()))
                                .collect(Collectors.toList()));
        CK_ARRAY_CONVERTERS.put(UUID[].class, res -> Arrays.asList((UUID[]) res));
        CK_ARRAY_CONVERTERS.put(com.clickhouse.data.value.UnsignedByte[].class,
                res -> Arrays.asList((com.clickhouse.data.value.UnsignedByte[]) res));
        CK_ARRAY_CONVERTERS.put(com.clickhouse.data.value.UnsignedShort[].class,
                res -> Arrays.asList((com.clickhouse.data.value.UnsignedShort[]) res));
        CK_ARRAY_CONVERTERS.put(com.clickhouse.data.value.UnsignedInteger[].class,
                res -> Arrays.asList((com.clickhouse.data.value.UnsignedInteger[]) res));
        CK_ARRAY_CONVERTERS.put(com.clickhouse.data.value.UnsignedLong[].class,
                res -> Arrays.asList((com.clickhouse.data.value.UnsignedLong[]) res));
    }

    public static List<Object> convertClickHouseArray(Object obj) {
        Function<Object, List<Object>> converter = CK_ARRAY_CONVERTERS.get(obj.getClass());
        return converter != null ? converter.apply(obj) : Collections.singletonList(obj);
    }

    private static <T> List<Object> toList(T array) {
        if (array instanceof Object[]) {
            return Arrays.asList((Object[]) array);
        }
        int length = Array.getLength(array);
        List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(Array.get(array, i));
        }
        return list;
    }

    private static final Pattern ARRAY_PATTERN = Pattern.compile("\"([^\"]*)\"|([^,]+)");

    private static List<Object> parseArray(String input) {
        String trimmedInput = input.substring(1, input.length() - 1);
        List<Object> list = new ArrayList<>();
        Matcher matcher = ARRAY_PATTERN.matcher(trimmedInput);
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                list.add(matcher.group(1));
            } else {
                list.add(matcher.group(2));
            }
        }
        return list;
    }

    private int insert(VectorTable data) throws SQLException {
        for (int i = 0; i < data.getNumRows(); ++i) {
            for (int j = 0; j < data.getColumns().length; ++j) {
                insertColumn(i, j, data.getColumns()[j]);
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.clearBatch();
        return data.getNumRows();
    }

    private void insertColumn(int rowIdx, int colIdx, VectorColumn column) throws SQLException {
        int parameterIndex = colIdx + 1;
        ColumnType.Type dorisType = column.getColumnTyp();
        if (column.isNullAt(rowIdx)) {
            insertNullColumn(parameterIndex, dorisType);
            return;
        }
        switch (dorisType) {
            case BOOLEAN:
                preparedStatement.setBoolean(parameterIndex, column.getBoolean(rowIdx));
                break;
            case TINYINT:
                preparedStatement.setByte(parameterIndex, column.getByte(rowIdx));
                break;
            case SMALLINT:
                preparedStatement.setShort(parameterIndex, column.getShort(rowIdx));
                break;
            case INT:
                preparedStatement.setInt(parameterIndex, column.getInt(rowIdx));
                break;
            case BIGINT:
                preparedStatement.setLong(parameterIndex, column.getLong(rowIdx));
                break;
            case LARGEINT:
                preparedStatement.setObject(parameterIndex, column.getBigInteger(rowIdx));
                break;
            case FLOAT:
                preparedStatement.setFloat(parameterIndex, column.getFloat(rowIdx));
                break;
            case DOUBLE:
                preparedStatement.setDouble(parameterIndex, column.getDouble(rowIdx));
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                preparedStatement.setBigDecimal(parameterIndex, column.getDecimal(rowIdx));
                break;
            case DATEV2:
                preparedStatement.setDate(parameterIndex, Date.valueOf(column.getDate(rowIdx)));
                break;
            case DATETIMEV2:
                preparedStatement.setTimestamp(
                        parameterIndex, Timestamp.valueOf(column.getDateTime(rowIdx)));
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                preparedStatement.setString(parameterIndex, column.getStringWithOffset(rowIdx));
                break;
            default:
                throw new RuntimeException("Unknown type value: " + dorisType);
        }
    }

    private void insertNullColumn(int parameterIndex, ColumnType.Type dorisType)
            throws SQLException {
        switch (dorisType) {
            case BOOLEAN:
                preparedStatement.setNull(parameterIndex, Types.BOOLEAN);
                break;
            case TINYINT:
                preparedStatement.setNull(parameterIndex, Types.TINYINT);
                break;
            case SMALLINT:
                preparedStatement.setNull(parameterIndex, Types.SMALLINT);
                break;
            case INT:
                preparedStatement.setNull(parameterIndex, Types.INTEGER);
                break;
            case BIGINT:
                preparedStatement.setNull(parameterIndex, Types.BIGINT);
                break;
            case LARGEINT:
                preparedStatement.setNull(parameterIndex, Types.JAVA_OBJECT);
                break;
            case FLOAT:
                preparedStatement.setNull(parameterIndex, Types.FLOAT);
                break;
            case DOUBLE:
                preparedStatement.setNull(parameterIndex, Types.DOUBLE);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                preparedStatement.setNull(parameterIndex, Types.DECIMAL);
                break;
            case DATEV2:
                preparedStatement.setNull(parameterIndex, Types.DATE);
                break;
            case DATETIMEV2:
                preparedStatement.setNull(parameterIndex, Types.TIMESTAMP);
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
                preparedStatement.setNull(parameterIndex, Types.VARCHAR);
                break;
            default:
                throw new RuntimeException("Unknown type value: " + dorisType);
        }
    }
}
