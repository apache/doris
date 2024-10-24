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
// This file is copied from

package org.apache.doris.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FlightSqlJDBC {
    // JDBC driver name and database URL
    static final String DB_URL = "jdbc:arrow-flight-sql://0.0.0.0:10478?useServerPrepStmts=false"
            + "&cachePrepStmts=true&useSSL=false&useEncryption=false";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "";

    public static void main(String[] args) throws ClassNotFoundException {
        Connection conn = null;
        Statement stmt = null;
        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
        try {

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.createStatement();

            stmt.executeQuery("use information_schema;");
            String sql = "show tables;";

            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String col1 = resultSet.getString(1);
                System.out.println(col1);
            }

            stmt.execute(sql);
            try (final ResultSet resultSet2 = stmt.getResultSet()) {
                final int columnCount = resultSet2.getMetaData().getColumnCount();
                System.out.println(columnCount);
                while (resultSet2.next()) {
                    String col1 = resultSet2.getString(1);
                    System.out.println(col1);
                }
            }

            resultSet.close();
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
