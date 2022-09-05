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

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class JdbcExecutor {
    private static final Logger LOG = Logger.getLogger(JdbcExecutor.class);
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();
    private URLClassLoader classLoader = null;
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet resultSet = null;
    private ResultSetMetaData resultSetMetaData = null;

    public JdbcExecutor(byte[] thriftParams) throws Exception {
        TJdbcExecutorCtorParams request = new TJdbcExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        init(request.jar_location_path, request.jdbc_driver_class, request.jdbc_url, request.jdbc_user,
                request.jdbc_password);
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
        if (classLoader != null) {
            classLoader.close();
        }
    }


    public int querySQL(String sql) throws UdfRuntimeException {
        try {
            boolean res = stmt.execute(sql);
            if (res) { // sql query
                resultSet = stmt.getResultSet();
                resultSetMetaData = resultSet.getMetaData();
                return resultSetMetaData.getColumnCount();
            } else { //TODO: update query
                return 0;
            }
        } catch (SQLException e) {
            throw new UdfRuntimeException("JDBC executor sql has error: ", e);
        }
    }

    public List<List<Object>> getBlock(int batchSize) throws UdfRuntimeException {
        List<List<Object>> block = null;
        try {
            int columnCount = resultSetMetaData.getColumnCount();
            block = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; ++i) {
                block.add(new ArrayList<>(batchSize));
            }
            int numRows = 0;
            do {
                for (int i = 0; i < columnCount; ++i) {
                    block.get(i).add(resultSet.getObject(i + 1));
                }
                numRows++;
            } while (numRows < batchSize && resultSet.next());
        } catch (SQLException e) {
            throw new UdfRuntimeException("get next block failed: ", e);
        } catch (Exception e) {
            throw new UdfRuntimeException("unable to get next : ", e);
        }
        return block;
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

    public long convertDateToLong(Object obj) {
        LocalDate date = ((Date) obj).toLocalDate();
        long time = UdfUtils.convertDateTimeToLong(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                0, 0, 0, true);
        return time;
    }

    public long convertDateTimeToLong(Object obj) {
        LocalDateTime date = ((Timestamp) obj).toLocalDateTime();
        long time = UdfUtils.convertDateTimeToLong(date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                date.getHour(), date.getMinute(), date.getSecond(), false);
        return time;
    }

    private void init(String driverPath, String driverClass, String jdbcUrl, String jdbcUser, String jdbcPassword)
            throws UdfRuntimeException {
        try {
            ClassLoader loader;
            if (driverPath != null) {
                ClassLoader parent = getClass().getClassLoader();
                classLoader = UdfUtils.getClassLoader(driverPath, parent);
                loader = classLoader;
            } else {
                loader = ClassLoader.getSystemClassLoader();
            }
            Class.forName(driverClass, true, loader);
            conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            stmt = conn.createStatement();
        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("MalformedURLException to load class about " + driverPath, e);
        } catch (ClassNotFoundException e) {
            throw new UdfRuntimeException("Loading JDBC class error ClassNotFoundException about " + driverClass, e);
        } catch (SQLException e) {
            throw new UdfRuntimeException("Connection JDBC class error about " + jdbcUrl, e);
        } catch (Exception e) {
            throw new UdfRuntimeException("unable to init jdbc executor Exception ", e);
        }
    }
}
