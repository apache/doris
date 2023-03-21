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

package org.apache.doris.udf;

import org.apache.doris.thrift.TJdbcExecutorCtorParams;
import org.apache.doris.thrift.TJdbcOperation;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JdbcExecutor {
    private static final Logger LOG = Logger.getLogger(JdbcExecutor.class);
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData resultSetMetaData = null;
    private List<String> resultColumnTypeNames = null;
    private int baseTypeInt = 0;
    private List<Object[]> block = null;
    private int batchSizeNum = 0;
    private int curBlockRows = 0;
    private static final byte[] emptyBytes = new byte[0];
    private DruidDataSource druidDataSource = null;

    public JdbcExecutor(byte[] thriftParams) throws Exception {
        TJdbcExecutorCtorParams request = new TJdbcExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        init(request.driver_path, request.statement, request.batch_size, request.jdbc_driver_class,
                request.jdbc_url, request.jdbc_user, request.jdbc_password, request.op);
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
            for (int i = 0; i < columnCount; ++i) {
                resultColumnTypeNames.add(resultSetMetaData.getColumnClassName(i + 1));
                block.add((Object[]) Array.newInstance(Object.class, batchSizeNum));
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

    public List<String> getResultColumnTypeNames() {
        return resultColumnTypeNames;
    }

    public List<Object> getArrayColumnData(Object object) throws UdfRuntimeException {
        try {
            java.sql.Array obj = (java.sql.Array) object;
            baseTypeInt = obj.getBaseType();
            return Arrays.asList((Object[]) obj.getArray());
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor getArrayColumnData has error: ", e);
        }
    }

    public int getBaseTypeInt() {
        return baseTypeInt;
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

    public List<Object[]> getBlock(int batchSize) throws UdfRuntimeException {
        try {
            int columnCount = resultSetMetaData.getColumnCount();
            curBlockRows = 0;
            do {
                for (int i = 0; i < columnCount; ++i) {
                    block.get(i)[curBlockRows] = resultSet.getObject(i + 1);
                }
                curBlockRows++;
            } while (curBlockRows < batchSize && resultSet.next());
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

    public Object convertArrayToObject(Object obj, int idx) {
        Object[] columnData = (Object[]) obj;
        if (columnData[idx] instanceof String) {
            return (String) columnData[idx];
        } else {
            return (java.sql.Array) columnData[idx];
        }
    }

    private void init(String driverUrl, String sql, int batchSize, String driverClass, String jdbcUrl, String jdbcUser,
            String jdbcPassword, TJdbcOperation op) throws UdfRuntimeException {
        try {
            ClassLoader parent = getClass().getClassLoader();
            ClassLoader classLoader = UdfUtils.getClassLoader(driverUrl, parent);
            druidDataSource = JdbcDataSource.getDataSource().getSource(jdbcUrl);
            if (druidDataSource == null) {
                DruidDataSource ds = new DruidDataSource();
                ds.setDriverClassLoader(classLoader);
                ds.setDriverClassName(driverClass);
                ds.setUrl(jdbcUrl);
                ds.setUsername(jdbcUser);
                ds.setPassword(jdbcPassword);
                ds.setMinIdle(1);
                ds.setInitialSize(2);
                ds.setMaxActive(5);
                druidDataSource = ds;
                JdbcDataSource.getDataSource().putSource(jdbcUrl, ds);
            }
            conn = druidDataSource.getConnection();
            if (op == TJdbcOperation.READ) {
                conn.setAutoCommit(false);
                Preconditions.checkArgument(sql != null);
                stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(batchSize);
                batchSizeNum = batchSize;
            } else {
                stmt = conn.createStatement();
            }
        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("MalformedURLException to load class about " + driverUrl, e);
        } catch (SQLException e) {
            throw new UdfRuntimeException("Initialize datasource failed: ", e);
        } catch (FileNotFoundException e) {
            throw new UdfRuntimeException("FileNotFoundException failed: ", e);
        }
    }

    public void copyBatchBooleanResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
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
        if (isNullable == true) {
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

    //TODO: now array type need this function, can remove after refactor read array type
    public long convertDateToLong(Object obj, boolean isDateV2) {
        LocalDate date;
        if (obj instanceof LocalDate) {
            date = (LocalDate) obj;
        } else {
            date = ((Date) obj).toLocalDate();
        }
        if (isDateV2) {
            return UdfUtils.convertToDateV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        }
        return UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                0, 0, 0, true);
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
        }
    }

    //TODO: now array type need this function, can remove after refactor read array type
    public long convertDateTimeToLong(Object obj, boolean isDateTimeV2) throws UdfRuntimeException {
        LocalDateTime date = null;
        // TODO: not for sure: https://bugs.mysql.com/bug.php?id=101413
        if (obj instanceof LocalDateTime) {
            date = (LocalDateTime) obj;
        } else if (obj instanceof java.sql.Timestamp) {
            date = ((java.sql.Timestamp) obj).toLocalDateTime();
        } else if (obj instanceof oracle.sql.TIMESTAMP) {
            try {
                date = ((oracle.sql.TIMESTAMP) obj).timestampValue().toLocalDateTime();
            } catch (SQLException e) {
                throw new UdfRuntimeException("Convert oracle.sql.TIMESTAMP"
                        + " to LocalDateTime failed: ", e);
            }
        }
        if (isDateTimeV2) {
            return UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                    date.getHour(), date.getMinute(), date.getSecond());
        }
        return UdfUtils.convertToDateTime(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                date.getHour(), date.getMinute(), date.getSecond(), false);
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
                                    date.getSecond()));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = (LocalDateTime) column[i];
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(),
                                date.getDayOfMonth(), date.getHour(), date.getMinute(),
                                date.getSecond()));
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
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(), date.getSecond()));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((java.sql.Timestamp) column[i]).toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                                date.getHour(), date.getMinute(), date.getSecond()));
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
                                    date.getDayOfMonth(), date.getHour(), date.getMinute(), date.getSecond()));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                LocalDateTime date = ((oracle.sql.TIMESTAMP) column[i]).timestampValue().toLocalDateTime();
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8L),
                        UdfUtils.convertToDateTimeV2(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                                date.getHour(), date.getMinute(), date.getSecond()));
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

    private void objectPutToString(Object[] column, boolean isNullable, int numRows, long nullMapAddr,
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
                    byteRes[i] = column[i].toString().getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = column[i].toString().getBytes(StandardCharsets.UTF_8);
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        }
        byte[] bytes = new byte[offsets[numRows - 1]];
        long bytesAddr = JNINativeMethod.resizeColumn(charsAddr, offsets[numRows - 1]);
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
        if (isNullable == true) {
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
        long bytesAddr = JNINativeMethod.resizeColumn(charsAddr, offsets[numRows - 1]);
        int dst = 0;
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < byteRes[i].length; j++) {
                bytes[dst++] = byteRes[i][j];
            }
        }
        UdfUtils.copyMemory(offsets, UdfUtils.INT_ARRAY_OFFSET, null, offsetsAddr, numRows * 4L);
        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, bytesAddr, offsets[numRows - 1]);
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
        } else {
            //object like in pg type point, polygon, jsonb..... get object is org.postgresql.util.PGobject.....
            //here object put to string, so the object must have impl toString() function
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
        if (isNullable == true) {
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
