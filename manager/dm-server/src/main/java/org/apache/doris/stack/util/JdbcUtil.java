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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * jdbc util
 **/
@Slf4j
public class JdbcUtil {

    public static Connection getConn(String host, String port, String user, String password, String database) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + user + "&password=" + password;
        return DriverManager.getConnection(url);
    }

    public static boolean execute(Connection conn, String sql) {
        boolean flag = false;
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            flag = stmt.execute();
        } catch (SQLException e) {
            log.error("sql execute error:{}", sql, e);
        } finally {
            closeConn(conn);
            closeStmt(stmt);
        }
        return flag;
    }

    public static void closeConn(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("close connection error:", e);
        }
    }

    public static void closeStmt(PreparedStatement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            log.error("close statement error:", e);
        }
    }
}
