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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;

import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FlightSqlTableTypesTest {
    @Test
    public void streamsSortedDorisTypesAndReleasesBuffers() throws Exception {
        checkStream(false);
    }

    @Test
    public void releasesBuffersWhenSendingFails() throws Exception {
        checkStream(true);
    }

    private void checkStream(boolean failSend) throws Exception {
        boolean previous = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            ConnectContext ctx = ConnectContext.forFlight("metadata-token");
            FlightSessionsManager sessions = Mockito.mock(FlightSessionsManager.class);
            Mockito.when(sessions.getConnectContext("metadata-token")).thenReturn(ctx);
            CallContext context = Mockito.mock(CallContext.class);
            Mockito.when(context.peerIdentity()).thenReturn("metadata-token");
            try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                    Location.forGrpcInsecure("127.0.0.1", 9090), sessions)) {
                BufferAllocator allocator = Deencapsulation.getField(producer, "rootAllocator");
                for (int i = 0; i < 10; i++) {
                    ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
                    AtomicReference<VectorSchemaRoot> batch = new AtomicReference<>();
                    List<String> values = new ArrayList<>();
                    Mockito.doAnswer(invocation -> {
                        batch.set(invocation.getArgument(0));
                        Assertions.assertEquals(Schemas.GET_TABLE_TYPES_SCHEMA, batch.get().getSchema());
                        return null;
                    }).when(listener).start(Mockito.any(VectorSchemaRoot.class));
                    Mockito.doAnswer(invocation -> {
                        VectorSchemaRoot root = batch.get();
                        for (int row = 0; row < root.getRowCount(); row++) {
                            values.add(root.getVector("table_type").getObject(row).toString());
                        }
                        if (failSend) {
                            throw new IllegalStateException("simulated send failure");
                        }
                        return null;
                    }).when(listener).putNext();
                    if (failSend) {
                        Assertions.assertThrows(RuntimeException.class,
                                () -> producer.getStreamTableTypes(context, listener));
                        Mockito.verify(listener).error(Mockito.any(Throwable.class));
                        Mockito.verify(listener, Mockito.never()).completed();
                    } else {
                        producer.getStreamTableTypes(context, listener);
                        Mockito.verify(listener).completed();
                        Mockito.verify(listener, Mockito.never()).error(Mockito.any(Throwable.class));
                    }
                    Assertions.assertEquals(Arrays.asList("BASE TABLE", "SYSTEM VIEW", "VIEW"), values);
                    Assertions.assertEquals(0, allocator.getAllocatedMemory());
                }
                Mockito.verify(sessions, Mockito.times(10)).getConnectContext("metadata-token");
            } finally {
                ctx.getFlightSqlChannel().close();
            }
        } finally {
            FeConstants.runningUnitTest = previous;
        }
    }
}
