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

package doris.arrowflight.demo;

import doris.arrowflight.demo.JdbcResultSetReader.LoadJdbcResultSetFunc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;

/**
 * Use the Java JDBC DriverManager to connect to the Doris Arrow Flight server and execute query.
 * Usually, DriverManager is used to connect to the database using the Mysql protocol in Java. only need to replace
 * `jdbc:mysql` in the URI with `jdbc:arrow-flight-sql` to connect to the database using the Arrow Flight SQL protocol
 * (provided that the database implements the Arrow Flight server).
 */
public class JdbcDriverManager {
    private static void connectAndExecute(Configuration configuration, String urlPrefix, String port,
            LoadJdbcResultSetFunc loadJdbcResultSetFunc) {
        String DB_URL = urlPrefix + "://" + configuration.ip + ":" + port + "?useServerPrepStmts=false"
                + "&cachePrepStmts=true&useSSL=false&useEncryption=false";
        try {
            long start = System.currentTimeMillis();
            Connection conn = DriverManager.getConnection(DB_URL, configuration.user, configuration.password);
            Statement stmt = conn.createStatement();
            stmt.execute(configuration.sql);

            final ResultSet resultSet = stmt.getResultSet();
            loadJdbcResultSetFunc.load(resultSet);
            System.out.printf("> cost: %d ms.\n\n", (System.currentTimeMillis() - start));

            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void run(Configuration configuration) {
        System.out.println("*************************************");
        System.out.println("|          JdbcDriverManager        |");
        System.out.println("*************************************");

        try {
            if (!Objects.equals(configuration.mysqlPort, "")) {
                Class.forName("com.mysql.cj.jdbc.Driver");
                System.out.println("JdbcDriverManager > jdbc:mysql > loadJdbcResult");
                connectAndExecute(configuration, "jdbc:mysql", configuration.mysqlPort,
                        JdbcResultSetReader.loadJdbcResult);
                System.out.println("JdbcDriverManager > jdbc:mysql > loadJdbcResultToString");
                connectAndExecute(configuration, "jdbc:mysql", configuration.mysqlPort,
                        JdbcResultSetReader.loadJdbcResultToString);
            }

            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
            System.out.println("JdbcDriverManager > jdbc:arrow-flight-sql > loadJdbcResultToString");
            connectAndExecute(configuration, "jdbc:arrow-flight-sql", configuration.arrowFlightPort,
                    JdbcResultSetReader.loadJdbcResult);
            System.out.println("JdbcDriverManager > jdbc:arrow-flight-sql > loadJdbcResultToString");
            connectAndExecute(configuration, "jdbc:arrow-flight-sql", configuration.arrowFlightPort,
                    JdbcResultSetReader.loadJdbcResultToString);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        Configuration configuration = new Configuration(args);
        for (int i = 0; i < configuration.retryTimes; i++) {
            run(configuration);
        }
    }
}
