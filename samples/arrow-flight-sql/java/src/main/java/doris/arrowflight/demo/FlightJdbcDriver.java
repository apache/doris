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

import doris.arrowflight.demo.ArrowBatchReader.LoadArrowBatchFunc;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Use the Arrow Flight JDBC driver to connect to the Doris Arrow Flight server and execute query.
 * Unlike the Java JDBC DriverManager, this is a JDBC Driver provided by Arrow Flight, which may contain
 * some optimizations (although no performance advantage was observed).
 */
public class FlightJdbcDriver {
    private static void connectAndExecute(Configuration configuration, LoadArrowBatchFunc loadArrowReader) {
        String DB_URL = "jdbc:arrow-flight-sql://" + configuration.ip + ":" + configuration.arrowFlightPort
                + "?useServerPrepStmts=false" + "&cachePrepStmts=true&useSSL=false&useEncryption=false";
        final Map<String, Object> parameters = new HashMap<>();
        AdbcDriver.PARAM_URI.set(parameters, DB_URL);
        AdbcDriver.PARAM_USERNAME.set(parameters, configuration.user);
        AdbcDriver.PARAM_PASSWORD.set(parameters, configuration.password);

        try {
            BufferAllocator allocator = new RootAllocator();
            AdbcDatabase db = new JdbcDriver(allocator).open(parameters);
            AdbcConnection connection = db.connect();
            AdbcStatement stmt = connection.createStatement();

            long start = System.currentTimeMillis();
            stmt.setSqlQuery(configuration.sql);
            AdbcStatement.QueryResult queryResult = stmt.executeQuery();
            ArrowReader reader = queryResult.getReader();
            loadArrowReader.load(reader);
            System.out.printf("> cost: %d ms.\n\n", (System.currentTimeMillis() - start));

            reader.close();
            queryResult.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void run(Configuration configuration) {
        System.out.println("*************************************");
        System.out.println("|          FlightJdbcDriver         |");
        System.out.println("*************************************");

        System.out.println("FlightJdbcDriver > loadArrowBatch");
        connectAndExecute(configuration, ArrowBatchReader.loadArrowBatch);
        System.out.println("FlightJdbcDriver > loadArrowBatchToString");
        connectAndExecute(configuration, ArrowBatchReader.loadArrowBatchToString);
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration(args);
        for (int i = 0; i < configuration.retryTimes; i++) {
            run(configuration);
        }
    }
}
