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

package org.apache.doris.arrowflight;

import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FlightSqlFeResultLocationTest {
    // The defect this setting fixes, end to end through a real Flight SQL server: the FE advertises an
    // address its client cannot reach, as an FE behind a load balancer or a proxy does. A metadata
    // result's endpoint names that address by default, so a client that follows endpoint locations
    // cannot fetch it; with arrow_flight_fe_result_empty_location the endpoint carries no location and
    // the same client reads it over the connection that issued the ticket.
    @Test
    public void locationFollowingClientReadsAnFeHeldResultOnlyWithoutALocation() throws Exception {
        boolean previousRunningUnitTest = FeConstants.runningUnitTest;
        boolean previousEmptyLocation = Config.arrow_flight_fe_result_empty_location;
        FeConstants.runningUnitTest = true;
        ConnectContext ctx = ConnectContext.forFlight("metadata-token");
        try {
            FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
            Mockito.when(sessions.getConnectContext(Mockito.anyString())).thenReturn(ctx);
            // A loopback port nothing listens on: unreachable on every host, refused at once, no DNS.
            int closedPort;
            try (ServerSocket probe = new ServerSocket(0)) {
                closedPort = probe.getLocalPort();
            }
            Location advertised = Location.forGrpcInsecure("127.0.0.1", closedPort);
            try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(advertised, sessions);
                    RootAllocator allocator = new RootAllocator();
                    FlightServer server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0),
                            producer).build().start();
                    FlightClient client = FlightClient.builder(allocator,
                            Location.forGrpcInsecure("127.0.0.1", server.getPort())).build()) {
                FlightSqlClient sql = new FlightSqlClient(client);

                Config.arrow_flight_fe_result_empty_location = false;
                FlightEndpoint byAddress = sql.getTableTypes().getEndpoints().get(0);
                Assertions.assertEquals(Collections.singletonList(advertised), byAddress.getLocations());
                FlightRuntimeException unreachable = Assertions.assertThrows(FlightRuntimeException.class,
                        () -> readTableTypes(allocator, client, byAddress));
                Assertions.assertEquals(FlightStatusCode.UNAVAILABLE, unreachable.status().code(),
                        unreachable.getMessage());

                Config.arrow_flight_fe_result_empty_location = true;
                FlightEndpoint noLocation = sql.getTableTypes().getEndpoints().get(0);
                Assertions.assertTrue(noLocation.getLocations().isEmpty(), "locations " + noLocation.getLocations());
                Assertions.assertEquals(Arrays.asList("BASE TABLE", "SYSTEM VIEW", "VIEW"),
                        readTableTypes(allocator, client, noLocation));
            }
        } finally {
            ctx.getFlightSqlChannel().close();
            Config.arrow_flight_fe_result_empty_location = previousEmptyLocation;
            FeConstants.runningUnitTest = previousRunningUnitTest;
        }
    }

    // Reads an endpoint the way a client that follows locations does (the Flight SQL JDBC driver): at its
    // first location, or, when it has none, on the client that issued the ticket.
    private static List<String> readTableTypes(BufferAllocator allocator, FlightClient issuer,
            FlightEndpoint endpoint) throws Exception {
        if (endpoint.getLocations().isEmpty()) {
            return readTableTypes(issuer, endpoint);
        }
        try (FlightClient atLocation = FlightClient.builder(allocator, endpoint.getLocations().get(0)).build()) {
            return readTableTypes(atLocation, endpoint);
        }
    }

    private static List<String> readTableTypes(FlightClient reader, FlightEndpoint endpoint) throws Exception {
        List<String> types = new ArrayList<>();
        try (FlightStream stream = reader.getStream(endpoint.getTicket(), CallOptions.timeout(10, TimeUnit.SECONDS))) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                for (int row = 0; row < root.getRowCount(); ++row) {
                    types.add(root.getVector("table_type").getObject(row).toString());
                }
            }
        }
        return types;
    }
}
