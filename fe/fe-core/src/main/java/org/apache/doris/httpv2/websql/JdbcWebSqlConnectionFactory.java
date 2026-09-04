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

package org.apache.doris.httpv2.websql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Opens Web SQL connections to the current FE's MySQL query port with the authenticated Doris user. */
public class JdbcWebSqlConnectionFactory implements WebSqlConnectionFactory {
    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final int CONNECT_TIMEOUT_MILLIS = 10000;
    private static final int SOCKET_TIMEOUT_MILLIS = 30 * 60 * 1000;
    private static final String CURRENT_USER_SQL = "SELECT CURRENT_USER()";
    private static final String DB_URL_PATTERN = "jdbc:mariadb://127.0.0.1:%d/"
            + "?connectTimeout=" + CONNECT_TIMEOUT_MILLIS + "&socketTimeout=" + SOCKET_TIMEOUT_MILLIS;

    @Override
    public Connection open(String user, String password) throws SQLException {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException exception) {
            throw new SQLException("MariaDB JDBC driver is unavailable", exception);
        }
        return DriverManager.getConnection(connectionUrl(Config.query_port), user, password);
    }

    @Override
    public Connection open(UserIdentity userIdentity, String password) throws SQLException {
        Connection connection = open(userIdentity.getQualifiedUser(), password);
        try {
            String actualIdentity = currentUser(connection);
            String expectedIdentity = userIdentity.toString();
            if (!expectedIdentity.equals(actualIdentity)) {
                throw new WebSqlIdentityMismatchException(expectedIdentity, actualIdentity);
            }
            return connection;
        } catch (SQLException exception) {
            try {
                connection.close();
            } catch (SQLException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    static String connectionUrl(int queryPort) {
        return String.format(DB_URL_PATTERN, queryPort);
    }

    String currentUser(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(CURRENT_USER_SQL)) {
            if (!resultSet.next()) {
                throw new SQLException("CURRENT_USER() returned no row");
            }
            return resultSet.getString(1);
        }
    }
}
