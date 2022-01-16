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

package org.apache.doris.stack.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.exceptions.ServerException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * jdbc util
 **/
@Slf4j
public class JdbcUtil {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ServerException("fail to load JDBC driver.");
        }
    }

    public static Connection getConnection(String host, Integer port) throws SQLException {
        String url = "jdbc:mysql://" + host + ":" + port + "?user=" + Constants.DORIS_DEFAULT_QUERY_USER + "&password=" + Constants.DORIS_DEFAULT_QUERY_PASSWORD;
        return DriverManager.getConnection(url);
    }

    public static Connection getConnection(String host, Integer port, String user, String password, String database) throws SQLException {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + user + "&password=" + password;
        return DriverManager.getConnection(url);
    }

    public static boolean execute(Connection conn, String sql) throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            return stmt.execute();
        } finally {
            closeStmt(stmt);
        }
    }

    public static void closeConn(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            log.warn("close connection error:", e);
        }
    }

    public static void closeStmt(PreparedStatement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            log.warn("close statement error:", e);
        }
    }

    public static void closeRs(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            log.warn("close resultset error:", e);
        }
    }
}
