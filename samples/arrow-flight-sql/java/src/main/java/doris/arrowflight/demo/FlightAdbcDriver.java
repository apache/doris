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
import org.apache.arrow.adbc.core.AdbcStatement.QueryResult;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Use the Arrow Flight ADBC driver to connect to the Doris Arrow Flight server and execute query.
 */
public class FlightAdbcDriver {
    private static void connectAndExecute(Configuration configuration, LoadArrowBatchFunc loadArrowReader) {
        final BufferAllocator allocator = new RootAllocator();
        FlightSqlDriver driver = new FlightSqlDriver(allocator);
        Map<String, Object> parameters = new HashMap<>();
        AdbcDriver.PARAM_URI.set(parameters,
                Location.forGrpcInsecure(configuration.ip, Integer.parseInt(configuration.arrowFlightPort)).getUri()
                        .toString());
        AdbcDriver.PARAM_USERNAME.set(parameters, configuration.user);
        AdbcDriver.PARAM_PASSWORD.set(parameters, configuration.password);

        try {
            AdbcDatabase adbcDatabase = driver.open(parameters);
            AdbcConnection connection = adbcDatabase.connect();
            AdbcStatement stmt = connection.createStatement();
            long start = System.currentTimeMillis();
            stmt.setSqlQuery(configuration.sql);

            // executeQuery, two steps:
            // 1. Execute Query and get returned FlightInfo;
            // 2. Create FlightInfoReader to sequentially traverse each Endpoint;
            QueryResult queryResult = stmt.executeQuery();
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
        System.out.println("|          FlightAdbcDriver         |");
        System.out.println("*************************************");

        System.out.println("FlightAdbcDriver > loadArrowBatch");
        connectAndExecute(configuration, ArrowBatchReader.loadArrowBatch);
        System.out.println("FlightAdbcDriver > loadArrowBatchToString");
        connectAndExecute(configuration, ArrowBatchReader.loadArrowBatchToString);
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(args);
        for (int i = 0; i < configuration.retryTimes; i++) {
            run(configuration);
        }
    }
}
