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

package org.apache.doris.common.util;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlUtil {
    private static final Logger LOG = LogManager.getLogger(MysqlUtil.class);

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL_PREFIX = "jdbc:mysql://";
    private static final int CONNECT_TIMEOUT_MS = 1000; // 1S
    private static final int SOCKET_TIMEOUT_MS = 10000; // 10S

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            Preconditions.checkState(false, "fail to load JDBC driver.");
        }
    }

    private static String buildMysqlUrl(String host, int port, String db) {
        StringBuilder builder = new StringBuilder();
        builder.append(MYSQL_URL_PREFIX)
                .append(host).append(":")
                .append(port).append("/")
                .append(db).append("?")
                .append("connectTimeout=").append(CONNECT_TIMEOUT_MS)
                .append("&").append("socketTimeout=").append(SOCKET_TIMEOUT_MS);

        return builder.toString();
    }

    public static Connection getConnection(String host, int port, String db,
                                           String user, String passwd) {
        String url = buildMysqlUrl(host, port, db);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, passwd);
        } catch (SQLException e) {
            LOG.warn("fail to get connection to mysql. url={}, user={}, exception={}",
                    url, user, e.getMessage());
            return null;
        }
        return conn;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlEx) {
                LOG.warn("fail to close connection to mysql.");
            }
        }
    }
}
