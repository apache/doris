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
import org.apache.doris.common.jni.utils.JNINativeMethod;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.ColumnType;
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
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

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

    public boolean isNebula() {
        return tableType == TOdbcTableType.NEBULA;
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

    public int write(String sql) throws UdfRuntimeException {
        try {
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        }
    }

    public int write(Map<String, String> params) throws UdfRuntimeException {
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split("#");
        long metaAddress = Long.parseLong(params.get("meta_address"));
        // Get sql string from configuration map
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        VectorTable batchTable = new VectorTable(columnTypes, requiredFields, metaAddress);
        // todo: insert the batch table by PreparedStatement
        // Can't release or close batchTable, it's released by c++
        try {
            insert(batchTable);
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        }
        return batchTable.getNumRows();
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
                preparedStatement.setTimestamp(parameterIndex, Timestamp.valueOf(column.getDateTime(rowIdx)));
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

    private void insertNullColumn(int parameterIndex, ColumnType.Type dorisType) throws SQLException {
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

    public List<String> getResultColumnTypeNames() {
        return resultColumnTypeNames;
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

    public List<Object[]> getBlock(int batchSize, Object colsArray) throws UdfRuntimeException {
        try {
            ArrayList<Integer> colsTypes = (ArrayList<Integer>) colsArray;
            Integer[] colArray = new Integer[colsTypes.size()];
            colArray = colsTypes.toArray(colArray);
            int columnCount = resultSetMetaData.getColumnCount();
            curBlockRows = 0;
            do {
                for (int i = 0; i < columnCount; ++i) {
                    // colArray[i] > 0, means the type is Hll/Bitmap, we should read it with getBytes
                    // instead of getObject, as Hll/Bitmap in JDBC will map to String by default.
                    if (colArray[i] > 0) {
                        block.get(i)[curBlockRows] = resultSet.getBytes(i + 1);
                    } else {
                        block.get(i)[curBlockRows] = resultSet.getObject(i + 1);
                    }
                }
                curBlockRows++;
            } while (curBlockRows < batchSize && resultSet.next());
        } catch (SQLException e) {
            throw new UdfRuntimeException("get next block failed: ", e);
        }
        return block;
    }

    public List<Object[]> getBlock(int batchSize) throws UdfRuntimeException {
        try {
            int columnCount = resultSetMetaData.getColumnCount();
            curBlockRows = 0;

            if (isNebula()) {
                do {
                    for (int i = 0; i < columnCount; ++i) {
                        block.get(i)[curBlockRows] = UdfUtils.convertObject((ValueWrapper) resultSet.getObject(i + 1));
                    }
                    curBlockRows++;
                } while (curBlockRows < batchSize && resultSet.next());
            } else {
                do {
                    for (int i = 0; i < columnCount; ++i) {
                        block.get(i)[curBlockRows] = resultSet.getObject(i + 1);
                    }
                    curBlockRows++;
                } while (curBlockRows < batchSize && resultSet.next());
            }
        } catch (SQLException e) {
            throw new UdfRuntimeException("get next block failed: ", e);
        }
        return block;
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
        try {
            if (isNebula()) {
                batchSizeNum = batchSize;
                Class.forName(driverClass);
                conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                stmt = conn.prepareStatement(sql);
            } else {
                ClassLoader parent = getClass().getClassLoader();
                ClassLoader classLoader = UdfUtils.getClassLoader(driverUrl, parent);
                druidDataSource = JdbcDataSource.getDataSource().getSource(jdbcUrl + jdbcUser + jdbcPassword);
                if (druidDataSource == null) {
                    synchronized (druidDataSourceLock) {
                        druidDataSource = JdbcDataSource.getDataSource().getSource(jdbcUrl + jdbcUser + jdbcPassword);
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
                            // here is a cache of datasource, which using the string(jdbcUrl + jdbcUser +
                            // jdbcPassword) as key.
                            // and the default datasource init = 1, min = 1, max = 100, if one of connection idle
                            // time greater than 10 minutes. then connection will be retrieved.
                            JdbcDataSource.getDataSource().putSource(jdbcUrl + jdbcUser + jdbcPassword, ds);
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

    public void booleanPutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr, long columnAddr,
            int startRow) {
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(columnAddr + i, (Boolean) column[i] ? (byte) 1 : 0);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putByte(columnAddr + i, (Boolean) column[i] ? (byte) 1 : 0);
            }
        }
    }

    public void copyBatchBooleanResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof Boolean) {
            booleanPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Integer) {
            integerPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Byte) {
            bytePutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void bigDecimalPutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    Short res = ((BigDecimal) column[i]).shortValueExact();
                    UdfUtils.UNSAFE.putByte(columnAddr + i, res.byteValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                Short res = ((BigDecimal) column[i]).shortValueExact();
                UdfUtils.UNSAFE.putByte(columnAddr + i, res.byteValue());
            }
        }
    }

    private void integerPutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(columnAddr + i, ((Integer) column[i]).byteValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putByte(columnAddr + i, ((Integer) column[i]).byteValue());
            }
        }
    }

    private void shortPutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(columnAddr + i, ((Short) column[i]).byteValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putByte(columnAddr + i, ((Short) column[i]).byteValue());
            }
        }
    }

    private void bytePutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(columnAddr + i, (Byte) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putByte(columnAddr + i, (Byte) column[i]);
            }
        }
    }

    private void objectPutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
                                 long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    String columnStr = String.valueOf(column[i]);
                    int columnInt = Integer.parseInt(columnStr);
                    UdfUtils.UNSAFE.putByte(columnAddr + i, (byte) columnInt);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                String columnStr = String.valueOf(column[i]);
                int columnInt = Integer.parseInt(columnStr);
                UdfUtils.UNSAFE.putByte(columnAddr + i, (byte) columnInt);
            }
        }
    }

    public void copyBatchTinyIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof BigDecimal) {
            bigDecimalPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Integer) {
            integerPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Short) {
            shortPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Byte) {
            bytePutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }  else if (column[firstNotNullIndex] instanceof java.lang.Object) {
            objectPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void bigDecimalPutToShort(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), ((BigDecimal) column[i]).shortValueExact());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), ((BigDecimal) column[i]).shortValueExact());
            }
        }
    }

    private void integerPutToShort(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), ((Integer) column[i]).shortValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), ((Integer) column[i]).shortValue());
            }
        }
    }

    private void shortPutToShort(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), (Short) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), (Short) column[i]);
            }
        }
    }

    public void clickHouseUInt8ToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), (short) ((UnsignedByte) column[i]).intValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putShort(columnAddr + (i * 2L), (short) ((UnsignedByte) column[i]).intValue());
            }
        }
    }

    public void copyBatchSmallIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof BigDecimal) {
            bigDecimalPutToShort(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Integer) {
            integerPutToShort(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Short) {
            shortPutToShort(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof com.clickhouse.data.value.UnsignedByte) {
            clickHouseUInt8ToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void bigDecimalPutToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), ((BigDecimal) column[i]).intValueExact());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), ((BigDecimal) column[i]).intValueExact());
            }
        }
    }

    private void integerPutToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), (Integer) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), (Integer) column[i]);
            }
        }
    }

    private void longPutToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
                                 long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), ((Long) column[i]).intValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), ((Long) column[i]).intValue());
            }
        }
    }

    public void clickHouseUInt16ToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), ((UnsignedShort) column[i]).intValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L), ((UnsignedShort) column[i]).intValue());
            }
        }
    }

    public void copyBatchIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof BigDecimal) {
            bigDecimalPutToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Integer) {
            integerPutToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof java.lang.Long) {
            // For mysql view. But don't worry about overflow
            longPutToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof com.clickhouse.data.value.UnsignedShort) {
            clickHouseUInt16ToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void bigDecimalPutToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L), ((BigDecimal) column[i]).longValueExact());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L), ((BigDecimal) column[i]).longValueExact());
            }
        }
    }

    private void longPutToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L), (Long) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L), (Long) column[i]);
            }
        }
    }

    private void clickHouseUInt32ToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L), ((UnsignedInteger) column[i]).longValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L), ((UnsignedInteger) column[i]).longValue());
            }
        }
    }

    public void copyBatchBigIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof BigDecimal) {
            bigDecimalPutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Long) {
            longPutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof com.clickhouse.data.value.UnsignedInteger) {
            clickHouseUInt32ToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void bigDecimalPutToBigInteger(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = ((BigDecimal) column[i]).toBigInteger();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 16, startRowForNullable);
    }

    private void bigIntegerPutToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    BigInteger columnValue = (BigInteger) column[i];
                    byte[] bytes = UdfUtils.convertByteOrder(columnValue.toByteArray());
                    byte[] value = new byte[16];
                    if (columnValue.signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                        value[index] = bytes[index];
                    }
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * 16L), 16);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                BigInteger columnValue = (BigInteger) column[i];
                byte[] bytes = UdfUtils.convertByteOrder(columnValue.toByteArray());
                byte[] value = new byte[16];
                if (columnValue.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * 16L), 16);
            }
        }
    }

    private void stringPutToBigInteger(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = new BigInteger((String) column[i]);
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 16, startRowForNullable);
    }

    private void clickHouseUInt64ToByte(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UnsignedLong columnValue = (UnsignedLong) column[i];
                    BigInteger bigIntValue = columnValue.bigIntegerValue();
                    byte[] bytes = UdfUtils.convertByteOrder(bigIntValue.toByteArray());
                    byte[] value = new byte[16];
                    if (bigIntValue.signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * 16L), 16);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UnsignedLong columnValue = (UnsignedLong) column[i];
                BigInteger bigIntValue = columnValue.bigIntegerValue();
                byte[] bytes = UdfUtils.convertByteOrder(bigIntValue.toByteArray());
                byte[] value = new byte[16];
                if (bigIntValue.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                System.arraycopy(bytes, 0, value, 0, Math.min(bytes.length, value.length));
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * 16L), 16);
            }
        }
    }

    public void copyBatchLargeIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof BigDecimal) {
            bigDecimalPutToBigInteger(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof BigInteger) {
            bigIntegerPutToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof String) {
            stringPutToBigInteger(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof com.clickhouse.data.value.UnsignedLong) {
            clickHouseUInt64ToByte(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    public void copyBatchFloatResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putFloat(columnAddr + (i * 4L), (Float) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putFloat(columnAddr + (i * 4L), (Float) column[i]);
            }
        }
    }

    private void bigDecimalPutToDouble(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putDouble(columnAddr + (i * 8L), ((BigDecimal) column[i]).doubleValue());
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putDouble(columnAddr + (i * 8L), ((BigDecimal) column[i]).doubleValue());
            }
        }
    }

    private void doublePutToDouble(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putDouble(columnAddr + (i * 8L), (Double) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putDouble(columnAddr + (i * 8L), (Double) column[i]);
            }
        }
    }

    public void copyBatchDoubleResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof BigDecimal) {
            bigDecimalPutToDouble(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Double) {
            doublePutToDouble(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void localDatePutToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDate date = (LocalDate) column[i];
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), 0, 0, 0, true));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDate date = (LocalDate) column[i];
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth(), 0, 0, 0, true));
            }
        }
    }

    private void datePutToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDate date = ((Date) column[i]).toLocalDate();
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), 0, 0, 0, true));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDate date = ((Date) column[i]).toLocalDate();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth(), 0, 0, 0, true));
            }
        }
    }

    public void copyBatchDateResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof LocalDate) {
            localDatePutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Date) {
            datePutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void localDatePutToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDate date = (LocalDate) column[i];
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L),
                            UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth()));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDate date = (LocalDate) column[i];
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L),
                        UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth()));
            }
        }
    }

    private void datePutToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDate date = ((Date) column[i]).toLocalDate();
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L),
                            UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDate date = ((Date) column[i]).toLocalDate();
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L),
                        UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
            }
        }
    }

    private void timestampPutToInt(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4L),
                            UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth()));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 4L),
                        UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth()));
            }
        }
    }

    public void copyBatchDateV2Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof LocalDate) {
            localDatePutToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Date) {
            datePutToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof Timestamp) {
            timestampPutToInt(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void localDateTimePutToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = (LocalDateTime) column[i];
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                    date.getSecond(), false));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = (LocalDateTime) column[i];
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                date.getSecond(), false));
            }
        }
    }

    private void timestampPutToLong(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(), date.getSecond(), false));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                                date.getHour(), date.getMinute(), date.getSecond(), false));
            }
        }
    }

    private void oracleTimetampPutToLong(Object[] column, boolean isNullable, int numRows,
            long nullMapAddr,
            long columnAddr, int startRowForNullable) throws SQLException {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = ((oracle.sql.TIMESTAMP) column[i]).timestampValue().toLocalDateTime();
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(), date.getSecond(), false));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((oracle.sql.TIMESTAMP) column[i]).timestampValue().toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                                date.getHour(), date.getMinute(), date.getSecond(), false));
            }
        }
    }

    public void copyBatchDateTimeResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) throws SQLException {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof LocalDateTime) {
            localDateTimePutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof java.sql.Timestamp) {
            timestampPutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof oracle.sql.TIMESTAMP) {
            oracleTimetampPutToLong(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    private void localDateTimePutToLongV2(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = (LocalDateTime) column[i];
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                    date.getSecond(), date.getNano() / 1000));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = (LocalDateTime) column[i];
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                date.getSecond(), date.getNano() / 1000));
            }
        }
    }

    private void timestampPutToLongV2(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                    date.getSecond(), date.getNano() / 1000));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                                date.getHour(), date.getMinute(), date.getSecond(), date.getNano() / 1000));
            }
        }
    }

    private void oracleTimetampPutToLongV2(Object[] column, boolean isNullable, int numRows,
            long nullMapAddr, long columnAddr, int startRowForNullable) throws SQLException {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = ((oracle.sql.TIMESTAMP) column[i]).timestampValue().toLocalDateTime();
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                    date.getSecond(), date.getNano() / 1000));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((oracle.sql.TIMESTAMP) column[i]).timestampValue().toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                                date.getHour(), date.getMinute(), date.getSecond(), date.getNano() / 1000));
            }
        }
    }

    private void offsetDateTimePutToLongV2(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
                                           long columnAddr, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    LocalDateTime date = ((OffsetDateTime) column[i]).toLocalDateTime();
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                            UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                            date.getDayOfMonth(), date.getHour(), date.getMinute(),
                            date.getSecond(), date.getNano() / 1000));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((OffsetDateTime) column[i]).toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                        date.getDayOfMonth(), date.getHour(), date.getMinute(),
                        date.getSecond(), date.getNano() / 1000));
            }
        }
    }

    public void copyBatchDateTimeV2Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) throws SQLException {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof LocalDateTime) {
            localDateTimePutToLongV2(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof java.sql.Timestamp) {
            timestampPutToLongV2(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof OffsetDateTime) {
            offsetDateTimePutToLongV2(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        } else if (column[firstNotNullIndex] instanceof oracle.sql.TIMESTAMP) {
            oracleTimetampPutToLongV2(column, isNullable, numRows, nullMapAddr, columnAddr, firstNotNullIndex);
        }
    }

    public String trimSpaces(String str) {
        int end = str.length() - 1;
        while (end >= 0 && str.charAt(end) == ' ') {
            end--;
        }
        return str.substring(0, end + 1);
    }

    public void copyBatchCharResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr, boolean needTrimSpaces) {
        if (needTrimSpaces == true) {
            Object[] column = (Object[]) columnObj;
            for (int i = 0; i < numRows; i++) {
                if (column[i] != null) {
                    column[i] = trimSpaces((String) column[i]);
                }
            }
            copyBatchStringResult(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else {
            copyBatchStringResult(columnObj, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        }
    }

    private void hllPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable == true) {
            // Here can not loop from startRowForNullable,
            // because byteRes will be used later
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = (byte[]) column[i];
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = (byte[]) column[i];
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private void bitMapPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable == true) {
            // Here can not loop from startRowForNullable,
            // because byteRes will be used later
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = (byte[]) column[i];
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = (byte[]) column[i];
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    public void copyBatchHllResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
                                   long offsetsAddr, long charsAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        hllPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
    }

    public void copyBatchBitMapResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
                                      long offsetsAddr, long charsAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        bitMapPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
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
            return address;  // No sequences of zeros to replace
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

    private void ipPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
                               long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    String ip = ((java.net.InetAddress) column[i]).getHostAddress();
                    if (column[i] instanceof java.net.Inet6Address) {
                        ip = simplifyIPv6Address(ip);
                    }
                    byteRes[i] = ip.getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                String ip = ((java.net.InetAddress) column[i]).getHostAddress();
                if (column[i] instanceof java.net.Inet6Address) {
                    ip = simplifyIPv6Address(ip);
                }
                byteRes[i] = ip.getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private void oracleClobToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
                                    long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    try {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) column[i];
                        String result = clob.getSubString(1, (int) clob.length());
                        byteRes[i] = result.getBytes(StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        LOG.info("clobToString have error when convert " + e.getMessage());
                    }
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                try {
                    oracle.sql.CLOB clob = (oracle.sql.CLOB) column[i];
                    String result = clob.getSubString(1, (int) clob.length());
                    byteRes[i] = result.getBytes(StandardCharsets.UTF_8);
                } catch (Exception e) {
                    LOG.info("clobToString have error when convert " + e.getMessage());
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private void objectPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            // Here can not loop from startRowForNullable,
            // because byteRes will be used later
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    String result = column[i].toString();
                    if (column[i] instanceof java.sql.Time) {
                        // the default toString() method doesn't format the milliseconds in Time.
                        long milliseconds = ((java.sql.Time) column[i]).getTime() % 1000L;
                        if (milliseconds > 0) {
                            result = String.format("%s.%03d", column[i].toString(), milliseconds);
                        }
                    }
                    byteRes[i] = result.getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            boolean isTime = numRows > 0 && column[0] instanceof java.sql.Time;
            for (int i = 0; i < numRows; i++) {
                String result = column[i].toString();
                if (isTime) {
                    // Doc https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
                    // shows that jdbc API use java.sql.Time to hold the TIME type,
                    // but java.sql.Time can only have millisecond precision.
                    // the default toString() method doesn't format the milliseconds in Time.
                    // Doc https://dev.mysql.com/doc/refman/8.0/en/time.html shows that MySQL supports time[0~6],
                    // so time[4~6] will lose precision
                    long milliseconds = ((java.sql.Time) column[i]).getTime() % 1000L;
                    if (milliseconds > 0) {
                        result = String.format("%s.%03d", column[i].toString(), milliseconds);
                    }
                }
                byteRes[i] = result.getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private void stringPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = ((String) column[i]).getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = ((String) column[i]).getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private void byteaPutToHexString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = byteArrayToHexString((byte[]) column[i]).getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = byteArrayToHexString((byte[]) column[i]).getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private static String byteArrayToHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder("\\x");
        for (byte b : bytes) {
            hexString.append(String.format("%02x", b & 0xff));
        }
        return hexString.toString();
    }

    private void byteaPutToMySQLString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
                                       long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = mysqlByteArrayToHexString((byte[]) column[i]).getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = mysqlByteArrayToHexString((byte[]) column[i]).getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private static String mysqlByteArrayToHexString(byte[] bytes) {
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

    public void copyBatchStringResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof String) {
            stringPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else if (column[firstNotNullIndex] instanceof byte[] && tableType == TOdbcTableType.POSTGRESQL) {
            // for postgresql bytea type
            byteaPutToHexString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else if ((column[firstNotNullIndex] instanceof java.net.Inet4Address
                || column[firstNotNullIndex] instanceof java.net.Inet6Address)
                && tableType == TOdbcTableType.CLICKHOUSE) {
            // for clickhouse ipv4 and ipv6 type
            ipPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else if (column[firstNotNullIndex] instanceof byte[] && (tableType == TOdbcTableType.MYSQL
                || tableType == TOdbcTableType.OCEANBASE)) {
            // for mysql bytea type
            byteaPutToMySQLString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else if (column[firstNotNullIndex] instanceof oracle.sql.CLOB && tableType == TOdbcTableType.ORACLE) {
            // for oracle clob type
            oracleClobToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else {
            // object like in pg type point, polygon, jsonb..... get object is
            // org.postgresql.util.PGobject.....
            // here object put to string, so the object must have impl toString() function
            objectPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        }
    }

    public void copyBatchDecimalV2Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = ((BigDecimal) column[i]).setScale(9, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 16, 0);
    }

    public void copyBatchDecimal32Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int scale) {
        Object[] column = (Object[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = ((BigDecimal) column[i]).setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 4, 0);
    }

    public void copyBatchDecimal64Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int scale) {
        Object[] column = (Object[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = ((BigDecimal) column[i]).setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 8, 0);
    }

    public void copyBatchDecimal128Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int scale) {
        Object[] column = (Object[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = ((BigDecimal) column[i]).setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 16, 0);
    }

    private void copyBatchDecimalResult(BigInteger[] column, boolean isNullable, int numRows,
            long columnAddr, int typeLen, int startRowForNullable) {
        if (isNullable) {
            for (int i = startRowForNullable; i < numRows; i++) {
                if (column[i] != null) {
                    byte[] bytes = UdfUtils.convertByteOrder(column[i].toByteArray());
                    byte[] value = new byte[typeLen];
                    if (column[i].signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                        value[index] = bytes[index];
                    }
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + ((long) i * typeLen),
                            typeLen);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byte[] bytes = UdfUtils.convertByteOrder(column[i].toByteArray());
                byte[] value = new byte[typeLen];
                if (column[i].signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + ((long) i * typeLen),
                        typeLen);
            }
        }
    }

    private static final Map<Class<?>, Function<Object, String>> CK_ARRAY_CONVERTERS = new HashMap<>();

    static {
        CK_ARRAY_CONVERTERS.put(String[].class, res -> Arrays.toString((String[]) res));
        CK_ARRAY_CONVERTERS.put(boolean[].class, res -> Arrays.toString((boolean[]) res));
        CK_ARRAY_CONVERTERS.put(byte[].class, res -> Arrays.toString((byte[]) res));
        CK_ARRAY_CONVERTERS.put(Byte[].class, res -> Arrays.toString((Byte[]) res));
        CK_ARRAY_CONVERTERS.put(LocalDate[].class, res -> Arrays.toString((LocalDate[]) res));
        CK_ARRAY_CONVERTERS.put(LocalDateTime[].class, res -> Arrays.toString((LocalDateTime[]) res));
        CK_ARRAY_CONVERTERS.put(float[].class, res -> Arrays.toString((float[]) res));
        CK_ARRAY_CONVERTERS.put(double[].class, res -> Arrays.toString((double[]) res));
        CK_ARRAY_CONVERTERS.put(short[].class, res -> Arrays.toString((short[]) res));
        CK_ARRAY_CONVERTERS.put(int[].class, res -> Arrays.toString((int[]) res));
        CK_ARRAY_CONVERTERS.put(long[].class, res -> Arrays.toString((long[]) res));
        CK_ARRAY_CONVERTERS.put(BigInteger[].class, res -> Arrays.toString((BigInteger[]) res));
        CK_ARRAY_CONVERTERS.put(BigDecimal[].class, res -> Arrays.toString((BigDecimal[]) res));
        CK_ARRAY_CONVERTERS.put(Inet4Address[].class, res -> Arrays.toString(Arrays.stream((Inet4Address[]) res)
                .map(InetAddress::getHostAddress).toArray(String[]::new)));
        CK_ARRAY_CONVERTERS.put(Inet6Address[].class, res -> Arrays.toString(Arrays.stream((Inet6Address[]) res)
                .map(addr -> simplifyIPv6Address(addr.getHostAddress())).toArray(String[]::new)));
        CK_ARRAY_CONVERTERS.put(UUID[].class, res -> Arrays.toString((UUID[]) res));
    }

    public static Object convertClickHouseArray(Object obj) {
        Function<Object, String> converter = CK_ARRAY_CONVERTERS.get(obj.getClass());
        return converter != null ? converter.apply(obj) : obj;
    }

    private void ckArrayPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            // Here can not loop from startRowForNullable,
            // because byteRes will be used later
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = ((String) convertClickHouseArray(column[i])).getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = ((String) convertClickHouseArray(column[i])).getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    private void arrayPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable) {
            // Here can not loop from startRowForNullable,
            // because byteRes will be used later
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    try {
                        byteRes[i] = Arrays.toString((Object[]) ((java.sql.Array) column[i]).getArray())
                                .getBytes(StandardCharsets.UTF_8);
                    } catch (SQLException e) {
                        LOG.info("arrayPutToString have error when convert " + e.getMessage());
                    }
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                try {
                    byteRes[i] = Arrays.toString((Object[]) ((java.sql.Array) column[i]).getArray())
                            .getBytes(StandardCharsets.UTF_8);
                } catch (SQLException e) {
                    LOG.info("arrayPutToString have error when convert " + e.getMessage());
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeStringColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
    }

    public void copyBatchArrayResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        // for doris array
        if (column[firstNotNullIndex] instanceof String) {
            stringPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else if (column[firstNotNullIndex] instanceof java.sql.Array) {
            // for PG array
            arrayPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else {
            // For the ClickHouse array type
            ckArrayPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        }
    }

    public void copyBatchJsonResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        Object[] column = (Object[]) columnObj;
        int firstNotNullIndex = 0;
        if (isNullable) {
            firstNotNullIndex = getFirstNotNullObject(column, numRows, nullMapAddr);
        }
        if (firstNotNullIndex == numRows) {
            return;
        }
        if (column[firstNotNullIndex] instanceof String) {
            stringPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        } else {
            objectPutToString(column, isNullable, numRows, nullMapAddr, offsetsAddr, charsAddr);
        }
    }

    private int getFirstNotNullObject(Object[] column, int numRows, long nullMapAddr) {
        int i = 0;
        for (; i < numRows; ++i) {
            if (null == column[i]) {
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                break;
            }
        }
        return i;
    }
}

