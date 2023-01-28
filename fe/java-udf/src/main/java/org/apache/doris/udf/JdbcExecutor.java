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

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
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
    private URLClassLoader classLoader = null;
    private List<List<Object>> block = null;
    private int bacthSizeNum = 0;
    private int curBlockRows = 0;

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
            conn.clearWarnings();
            conn.close();
        }
        if (classLoader != null) {
            classLoader.clearAssertionStatus();
            classLoader.close();
        }
        resultSet = null;
        stmt = null;
        conn = null;
        classLoader = null;
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
                block.add(Arrays.asList(new Object[bacthSizeNum]));
            }
            return columnCount;
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        }
    }

    public int write(String sql) throws UdfRuntimeException {
        try {
            boolean res = stmt.execute(sql);
            if (res) { // sql query
                resultSet = stmt.getResultSet();
                resultSetMetaData = resultSet.getMetaData();
                return resultSetMetaData.getColumnCount();
            } else {
                return stmt.getUpdateCount();
            }
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

    public List<List<Object>> getBlock(int batchSize) throws UdfRuntimeException {
        try {
            int columnCount = resultSetMetaData.getColumnCount();
            curBlockRows = 0;
            do {
                for (int i = 0; i < columnCount; ++i) {
                    block.get(i).set(curBlockRows, resultSet.getObject(i + 1));
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

    private void init(String driverUrl, String sql, int batchSize, String driverClass, String jdbcUrl, String jdbcUser,
            String jdbcPassword, TJdbcOperation op) throws UdfRuntimeException {
        try {
            File file = new File(driverUrl);
            URL url = file.toURI().toURL();
            classLoader = new URLClassLoader(new URL[] {url});
            Driver driver = (Driver) Class.forName(driverClass, true, classLoader).getDeclaredConstructor()
                    .newInstance();
            // in jdk11 cann't call addURL function by reflect to load class. so use this way
            // But DriverManager can't find the driverClass correctly, so add a faker driver
            // https://www.kfu.com/~nsayer/Java/dyn-jdbc.html
            DriverManager.registerDriver(new FakeDriver(driver));
            conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);

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
        } catch (ClassNotFoundException e) {
            throw new UdfRuntimeException("ClassNotFoundException:  " + driverClass, e);
        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("MalformedURLException to load class about " + driverUrl, e);
        } catch (SQLException e) {
            throw new UdfRuntimeException("Initialize datasource failed: ", e);
        } catch (InstantiationException e) {
            throw new UdfRuntimeException("InstantiationException failed: ", e);
        } catch (IllegalAccessException e) {
            throw new UdfRuntimeException("IllegalAccessException failed: ", e);
        } catch (InvocationTargetException e) {
            throw new UdfRuntimeException("InvocationTargetException new instance failed: ", e);
        } catch (NoSuchMethodException e) {
            throw new UdfRuntimeException("NoSuchMethodException Load class failed: ", e);
        }
    }
}
