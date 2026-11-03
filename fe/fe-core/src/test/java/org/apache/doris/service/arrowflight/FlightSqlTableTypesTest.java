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

package org.apache.doris.service.arrowflight;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class FlightSqlTableTypesTest {
    @Test
    void tableTypesAreSortedAndStableThroughFlightRpc() throws Exception {
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(sessions.getConnectContext(Mockito.anyString())).thenReturn(context);
        Location location = Location.forGrpcInsecure("127.0.0.1", 0);
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(location, sessions);
                RootAllocator allocator = new RootAllocator();
                FlightServer server = FlightServer.builder(allocator, location, producer).build().start();
                FlightClient client = FlightClient.builder(allocator,
                        Location.forGrpcInsecure("127.0.0.1", server.getPort())).build()) {
            FlightSqlClient sql = new FlightSqlClient(client);
            for (int iteration = 0; iteration < 3; ++iteration) {
                FlightInfo info = sql.getTableTypes();
                Assertions.assertEquals(Schemas.GET_TABLE_TYPES_SCHEMA, info.getSchema());
                Assertions.assertEquals(1, info.getEndpoints().size());
                List<String> types = new ArrayList<>();
                try (FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket())) {
                    Assertions.assertEquals(info.getSchema(), stream.getSchema());
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        for (int row = 0; row < root.getRowCount(); ++row) {
                            types.add(root.getVector("table_type").getObject(row).toString());
                        }
                    }
                }
                Assertions.assertEquals(Arrays.asList("BASE TABLE", "SYSTEM VIEW", "VIEW"), types);
            }
            // Metadata must not plan queries or enumerate catalogs, including on empty sessions.
            Mockito.verify(sessions, Mockito.times(3)).getConnectContext(Mockito.anyString());
            Mockito.verifyNoInteractions(context);
        }
    }

    @Test
    void streamFailureReleasesAllocatedVectors() throws Exception {
        FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
        CallContext context = Mockito.mock(CallContext.class);
        ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
        Mockito.doThrow(new IllegalStateException("stream failed")).when(listener).putNext();
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 0), sessions)) {
            Assertions.assertThrows(FlightRuntimeException.class, () -> producer.getStreamTableTypes(context, listener));
            Mockito.verify(listener).error(Mockito.any(FlightRuntimeException.class));
            Mockito.verify(listener, Mockito.never()).completed();
        }
    }
}
