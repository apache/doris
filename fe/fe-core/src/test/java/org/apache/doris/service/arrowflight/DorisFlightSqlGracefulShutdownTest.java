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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryAdmissionController;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

import com.google.protobuf.ByteString;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DorisFlightSqlGracefulShutdownTest {
    private DorisFlightSqlProducer producer;
    private FlightSessionsManager sessions;
    private CallContext call;
    private final QueryAdmissionController admission = QueryAdmissionController.INSTANCE;

    @BeforeEach
    public void setUp() {
        sessions = Mockito.mock(FlightSessionsManager.class);
        call = Mockito.mock(CallContext.class);
        Mockito.when(call.peerIdentity()).thenReturn("token");
        producer = new DorisFlightSqlProducer(Location.forGrpcInsecure("127.0.0.1", 9090), sessions);
    }

    @AfterEach
    public void tearDown() throws Exception {
        Deencapsulation.setField(admission, "accepting", true);
        producer.close();
        Assertions.assertEquals(0, admission.getActiveRequests());
    }

    @Test
    public void testShutdownRejectsStatementsBeforeExecution() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(sessions.getConnectContext("token")).thenReturn(context);
        admission.stopAccepting();
        FlightDescriptor descriptor = FlightDescriptor.command(new byte[0]);
        Assertions.assertThrows(FlightRuntimeException.class, () -> producer.getFlightInfoStatement(
                CommandStatementQuery.newBuilder().setQuery("select 1").build(), call, descriptor));
        Assertions.assertThrows(FlightRuntimeException.class, () -> producer.getFlightInfoPreparedStatement(
                CommandPreparedStatementQuery.newBuilder().build(), call, descriptor));
        Mockito.verifyNoInteractions(context);
    }

    @Test
    public void testShutdownStillDrainsIssuedResult() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        try (FlightSqlChannelResource resource = new FlightSqlChannelResource()) {
            FlightSqlChannel channel = resource.channel;
            channel.addOKResult("result", "insert into t values (1)");
            Assertions.assertTrue(channel.hasPendingResults());
            Mockito.when(context.getFlightSqlChannel()).thenReturn(channel);
            Mockito.when(sessions.getConnectContext("token")).thenReturn(context);
            ServerStreamListener listener = Mockito.mock(ServerStreamListener.class);
            Mockito.doAnswer(invocation -> {
                Assertions.assertEquals(1, admission.getActiveRequests());
                Assertions.assertTrue(channel.hasPendingResults());
                return null;
            }).when(listener).putNext();
            admission.stopAccepting();
            producer.getStreamStatement(TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFromUtf8("token:result")).build(), call, listener);
            Mockito.verify(listener).completed();
            Assertions.assertFalse(channel.hasPendingResults());
        }
    }

    @Test
    public void testCloseSessionReservationCoversRollbackAndReply() {
        Mockito.doAnswer(invocation -> {
            Assertions.assertEquals(1, admission.getActiveRequests());
            return null;
        }).when(sessions).closeConnectContext("token");
        StreamListener<CloseSessionResult> listener = Mockito.mock(StreamListener.class);
        Mockito.doAnswer(invocation -> {
            Assertions.assertEquals(1, admission.getActiveRequests());
            return null;
        }).when(listener).onCompleted();
        admission.stopAccepting();
        producer.closeSession(Mockito.mock(CloseSessionRequest.class), call, listener);
        Mockito.verify(sessions).closeConnectContext("token");
        Mockito.verify(listener).onCompleted();
    }

    private static class FlightSqlChannelResource implements AutoCloseable {
        private final FlightSqlChannel channel = new FlightSqlChannel();

        @Override
        public void close() {
            channel.close();
        }
    }
}
