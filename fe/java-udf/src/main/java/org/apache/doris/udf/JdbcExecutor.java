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
    private int bacthSizeNum = 0;
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
                Class<?> clazz = Class.forName(resultSetMetaData.getColumnClassName(i + 1));
                block.add((Object[]) Array.newInstance(clazz, bacthSizeNum));
            }
            return columnCount;
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        } catch (ClassNotFoundException e) {
            throw new UdfRuntimeException("JDBC executor sql ClassNotFoundException: ", e);
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
        } catch (Exception e) {
            throw new UdfRuntimeException("unable to get next : ", e);
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

    public byte convertTinyIntToByte(Object obj) {
        byte res = 0;
        if (obj instanceof Integer) {
            res = ((Integer) obj).byteValue();
        } else if (obj instanceof Short) {
            res = ((Short) obj).byteValue();
        }
        return res;
    }

    public short convertSmallIntToShort(Object obj) {
        short res = 0;
        if (obj instanceof Integer) {
            res = ((Integer) obj).shortValue();
        } else if (obj instanceof Short) {
            res = (short) obj;
        }
        return res;
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
                stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                        ResultSet.FETCH_FORWARD);
                stmt.setFetchSize(batchSize);
                bacthSizeNum = batchSize;
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
        Boolean[] column = (Boolean[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(columnAddr + i, column[i] ? (byte) 1 : 0);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putByte(columnAddr + i, column[i] ? (byte) 1 : 0);
            }
        }
    }

    public void copyBatchTinyIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putByte(columnAddr + i, convertTinyIntToByte(column[i]));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putByte(columnAddr + i, convertTinyIntToByte(column[i]));
            }
        }
    }

    public void copyBatchSmallIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putShort(columnAddr + (i * 2), convertSmallIntToShort(column[i]));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putShort(columnAddr + (i * 2), convertSmallIntToShort(column[i]));
            }
        }
    }

    public void copyBatchIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Integer[] column = (Integer[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4), (int) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4), (int) column[i]);
            }
        }
    }

    public void copyBatchBigIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Long[] column = (Long[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), (long) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), (long) column[i]);
            }
        }
    }

    public void copyBatchLargeIntResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        BigInteger[] column = (BigInteger[]) columnObj;
        if (isNullable == true) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byte[] bytes = UdfUtils.convertByteOrder(column[i].toByteArray());
                    byte[] value = new byte[16];
                    if (column[i].signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                        value[index] = bytes[index];
                    }
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * 16), 16);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byte[] bytes = UdfUtils.convertByteOrder(column[i].toByteArray());
                byte[] value = new byte[16];
                if (column[i].signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * 16), 16);
            }
        }
    }

    public void copyBatchFloatResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Float[] column = (Float[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putFloat(columnAddr + (i * 4), (float) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putFloat(columnAddr + (i * 4), (float) column[i]);
            }
        }
    }

    public void copyBatchDoubleResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Double[] column = (Double[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putDouble(columnAddr + (i * 8), (double) column[i]);
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putDouble(columnAddr + (i * 8), (double) column[i]);
            }
        }
    }

    public void copyBatchDateResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), convertDateToLong(column[i], false));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), convertDateToLong(column[i], false));
            }
        }
    }

    public void copyBatchDateV2Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putInt(columnAddr + (i * 4), (int) convertDateToLong(column[i], true));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putInt(columnAddr + (i * 4), (int) convertDateToLong(column[i], true));
            }
        }
    }

    public void copyBatchDateTimeResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) throws UdfRuntimeException {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), convertDateTimeToLong(column[i], false));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), convertDateTimeToLong(column[i], false));
            }
        }
    }

    public void copyBatchDateTimeV2Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) throws UdfRuntimeException {
        Object[] column = (Object[]) columnObj;
        if (isNullable) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), convertDateTimeToLong(column[i], true));
                }
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                UdfUtils.UNSAFE.putLong(columnAddr + (i * 8), convertDateTimeToLong(column[i], true));
            }
        }
    }

    public void copyBatchStringResult(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long charsAddr) {
        String[] column = (String[]) columnObj;
        int[] offsets = new int[numRows];
        byte[][] byteRes = new byte[numRows][];
        int offset = 0;
        if (isNullable == true) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] == null) {
                    byteRes[i] = emptyBytes;
                    UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
                } else {
                    byteRes[i] = column[i].getBytes(StandardCharsets.UTF_8);
                }
                offset += byteRes[i].length;
                offsets[i] = offset;
            }
        } else {
            for (int i = 0; i < numRows; i++) {
                byteRes[i] = column[i].getBytes(StandardCharsets.UTF_8);
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

    public void copyBatchDecimalV2Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr) {
        BigDecimal[] column = (BigDecimal[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = column[i].setScale(9, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 16);
    }

    public void copyBatchDecimal32Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int scale) {
        BigDecimal[] column = (BigDecimal[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = column[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 4);
    }

    public void copyBatchDecimal64Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int scale) {
        BigDecimal[] column = (BigDecimal[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = column[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 8);
    }

    public void copyBatchDecimal128Result(Object columnObj, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, int scale) {
        BigDecimal[] column = (BigDecimal[]) columnObj;
        BigInteger[] data = new BigInteger[numRows];
        for (int i = 0; i < numRows; i++) {
            if (column[i] == null) {
                data[i] = null;
                UdfUtils.UNSAFE.putByte(nullMapAddr + i, (byte) 1);
            } else {
                data[i] = column[i].setScale(scale, RoundingMode.HALF_EVEN).unscaledValue();
            }
        }
        copyBatchDecimalResult(data, isNullable, numRows, columnAddr, 16);
    }

    public void copyBatchDecimalResult(Object columnObj, boolean isNullable, int numRows,
            long columnAddr, int typeLen) {
        BigInteger[] column = (BigInteger[]) columnObj;
        if (isNullable == true) {
            for (int i = 0; i < numRows; i++) {
                if (column[i] != null) {
                    byte[] bytes = UdfUtils.convertByteOrder(column[i].toByteArray());
                    byte[] value = new byte[typeLen];
                    if (column[i].signum() == -1) {
                        Arrays.fill(value, (byte) -1);
                    }
                    for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                        value[index] = bytes[index];
                    }
                    UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * typeLen), typeLen);
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
                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null, columnAddr + (i * typeLen), typeLen);
            }
        }
    }
}
