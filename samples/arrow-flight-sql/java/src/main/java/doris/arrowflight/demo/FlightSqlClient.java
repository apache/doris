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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Manually execute Arrow Flight SQL Rpc process, usually used for debug.
 */
public class FlightSqlClient {
    public record FlightInfoResult<T, T2, T3>(T first, T2 second, T3 third) {
    }

    public record DummyFlightInfoResult<T, T2>(T first, T2 second) {
    }

    /**
     * Connect to FE Arrow Flight Server to obtain Bearertoken and execute Query to get Ticket.
     */
    public static FlightInfoResult<FlightInfo, CredentialCallOption, org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement> getFlightInfoFromDorisFe(
            Configuration configuration) throws URISyntaxException {
        BufferAllocator allocatorFE = new RootAllocator(Integer.MAX_VALUE);
        final Location clientLocationFE = new Location(
                new URI("grpc", null, configuration.ip, Integer.parseInt(configuration.arrowFlightPort),
                        null, null, null));
        FlightClient clientFE = FlightClient.builder(allocatorFE, clientLocationFE).build();
        org.apache.arrow.flight.sql.FlightSqlClient sqlClinetFE = new org.apache.arrow.flight.sql.FlightSqlClient(
                clientFE);

        // Use username and password authentication to obtain a Bearertoken for subsequent access to the Doris Arrow Flight Server.
        CredentialCallOption credentialCallOption = clientFE.authenticateBasicToken(configuration.user,
                configuration.password).get();
        final org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement preparedStatement = sqlClinetFE.prepare(
                configuration.sql,
                credentialCallOption);
        final FlightInfo info = preparedStatement.execute(credentialCallOption);
        return new FlightInfoResult<>(info, credentialCallOption, preparedStatement);
    }

    /**
     * Use the correct Bearertoken and the correct Ticket, and the expected return result is normal.
     */
    public static void getResultFromDorisBe(FlightInfo info, Ticket ticket, CredentialCallOption credentialCallOption)
            throws Exception {
        final Location locationBE = info.getEndpoints().get(0).getLocations().get(0);
        // 连接 BE Arrow Flight Server
        BufferAllocator allocatorBE = new RootAllocator(Integer.MAX_VALUE);
        FlightClient clientBE = FlightClient.builder(allocatorBE, locationBE).build();
        org.apache.arrow.flight.sql.FlightSqlClient sqlClinetBE = new org.apache.arrow.flight.sql.FlightSqlClient(
                clientBE);

        FlightStream stream = sqlClinetBE.getStream(ticket, credentialCallOption);
        int rowCount = 0;
        int batchCount = 0;
        while (stream.next()) {
            VectorSchemaRoot root = stream.getRoot();
            if (batchCount == 0) {
                System.out.println("> " + root.getSchema().toString());
                ArrowBatchReader.printRow(root, 1);  // only print first line
            }
            rowCount += root.getRowCount();
            batchCount += 1;
        }
        System.out.println("> batchCount: " + batchCount + ", rowCount: " + rowCount);
        stream.close();
    }

    /**
     * Construct a dummy Ticket and CredentialCallOption to simulate the BE Arrow Flight Server being hacked,
     * to analyze data security.
     *
     * @return get error `INVALID_ARGUMENT: Malformed ticket`
     */
    public static DummyFlightInfoResult<Ticket, CredentialCallOption> constructDummyFlightInfo() {
        String Bearertoken = "ojatddjr72k1ss20sqkatkhtd7";
        String queryId = "18c64b4e15094922-af5fea3da80fb89f";
        String query = "select * from clickbench.hits limit 10;";
        CredentialCallOption dummyCredentialCallOption = new CredentialCallOption(
                new BearerCredentialWriter(Bearertoken));
        final ByteString handle = ByteString.copyFromUtf8(queryId + ":" + query);
        TicketStatementQuery ticketStatement = TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
        final Ticket dummyTicket = new Ticket(Any.pack(ticketStatement).toByteArray());
        return new DummyFlightInfoResult<>(dummyTicket, dummyCredentialCallOption);
    }

    public static void run(Configuration configuration) {
        System.out.println("*************************************");
        System.out.println("|           FlightSqlClient         |");
        System.out.println("*************************************");

        try {
            System.out.println("FlightSqlClient > getFlightInfoFromDorisFe");
            var flightInfo = getFlightInfoFromDorisFe(configuration);
            getResultFromDorisBe(flightInfo.first(), flightInfo.first().getEndpoints().get(0).getTicket(),
                    flightInfo.second());
            System.out.println();

            System.out.println(
                    "FlightSqlClient > constructDummyFlightInfo, don't be afraid! expected to get error `INVALID_ARGUMENT: Malformed ticket`");
            var dummyFlightInfo = constructDummyFlightInfo();
            getResultFromDorisBe(flightInfo.first(), dummyFlightInfo.first(), dummyFlightInfo.second());
            System.out.println();

            flightInfo.third().close(flightInfo.second());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration(args);
        run(configuration);
    }
}
