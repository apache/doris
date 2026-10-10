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

package org.apache.doris.mysql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.auth2.FlightAuthResult;
import org.apache.doris.arrowflight.sessions.FlightSessionsInConnectPool;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryState;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.xnio.StreamConnection;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.util.concurrent.RejectedExecutionException;

public class ConnectionExceedTest {
    private Auth mockAuth = Mockito.mock(Auth.class);
    private Env mockEnv = Mockito.mock(Env.class);
    private InternalCatalog mockCatalog = Mockito.mock(InternalCatalog.class);
    private StreamConnection mockConnection = Mockito.mock(StreamConnection.class);

    @Test
    public void testHandleConnectionExceed() throws Exception {
        try (MockedStatic<MysqlProto> mockedProto = Mockito.mockStatic(MysqlProto.class)) {
            // Create a scheduler with small max connections
            ConnectScheduler scheduler = new ConnectScheduler(2);

            // Setup expectations
            Mockito.when(mockEnv.getInternalCatalog()).thenReturn(mockCatalog);
            Mockito.when(mockCatalog.getName()).thenReturn("internal");
            Mockito.when(mockEnv.getAuth()).thenReturn(mockAuth);
            Mockito.when(mockAuth.getMaxConn("test_user")).thenReturn(2L);
            // Mock MysqlProto.negotiate to return true to simulate successful authentication
            mockedProto.when(() -> MysqlProto.negotiate(Mockito.nullable(ConnectContext.class))).thenReturn(true);

            // Create first context and register
            ConnectContext context1 = new ConnectContext();
            context1.setEnv(mockEnv);
            context1.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            Assertions.assertTrue(scheduler.submit(context1));
            Assertions.assertEquals(-1, scheduler.getConnectPoolMgr().registerConnection(context1));

            // Create second context and register
            ConnectContext context2 = new ConnectContext();
            context2.setEnv(mockEnv);
            context2.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            Assertions.assertTrue(scheduler.submit(context2));
            Assertions.assertEquals(-1, scheduler.getConnectPoolMgr().registerConnection(context2));

            // Create third context and try to register - should fail
            ConnectContext context3 = new ConnectContext();
            context3.setEnv(mockEnv);
            context3.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            Assertions.assertTrue(scheduler.submit(context3));

            // Create AcceptListener and handle the connection
            AcceptListener listener = new AcceptListener(scheduler);
            listener.handleConnection(context3, mockConnection);
            String expectedMsg = String.format(
                    "Reach limit of connections. Total: %d, User: %d, Current: %d",
                    scheduler.getConnectPoolMgr().getMaxConnections(),
                    2, // Mocked user connection limit
                    scheduler.getConnectionNum());
            Assertions.assertEquals(expectedMsg, context3.getState().getErrorMessage());
            Assertions.assertEquals(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, context3.getState().getErrorCode());
        }
    }

    @Test
    public void testHandleReadEventRejectedExecution() throws Exception {
        try (MockedStatic<XnioIoThread> mockedIoThread = Mockito.mockStatic(XnioIoThread.class)) {
            ConnectContext context = Mockito.mock(ConnectContext.class);
            MysqlProtocolAdapter protocol = Mockito.mock(MysqlProtocolAdapter.class);
            QueryState queryState = Mockito.mock(QueryState.class);
            ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
            ConduitStreamSourceChannel channel = Mockito.mock(ConduitStreamSourceChannel.class);
            XnioWorker worker = Mockito.mock(XnioWorker.class);

            Mockito.when(context.getProtocolAdapter()).thenReturn(protocol);
            Mockito.when(context.getState()).thenReturn(queryState);
            Mockito.when(channel.getWorker()).thenReturn(worker);
            Mockito.doThrow(new RejectedExecutionException("queue full"))
                    .when(worker).execute(Mockito.any(Runnable.class));

            ReadListener listener = new ReadListener(context, processor);
            listener.handleEvent(channel);

            InOrder contextInOrder = Mockito.inOrder(protocol, context);
            contextInOrder.verify(protocol).suspendAcceptQuery();
            contextInOrder.verify(context).setThreadLocalInfo();
            contextInOrder.verify(context).setKilled();
            contextInOrder.verify(context).cleanup();
            Mockito.verifyNoInteractions(queryState);
            Mockito.verifyNoInteractions(processor);
        }
    }

    // An Arrow Flight SQL session is a connection of the one pool: refused at the pool's limit, the
    // user's limit or the Flight sub-quota, in the words a MySQL client is refused in, as the
    // RESOURCE_EXHAUSTED status of the handshake that would have opened it - and no bearer token
    // is issued for it, since there is no session it could name.
    @Test
    public void testFlightSessionConnectionExceed() throws Exception {
        try (MockedStatic<Env> mockedEnvStatic = Mockito.mockStatic(Env.class)) {
            // A pool of 1000 with a Flight sub-quota of 2
            ConnectScheduler scheduler = new ConnectScheduler(1000, 2);

            // Setup expectations
            Mockito.when(mockEnv.getInternalCatalog()).thenReturn(mockCatalog);
            Mockito.when(mockCatalog.getName()).thenReturn("internal");
            Mockito.when(mockAuth.getMaxConn(Mockito.anyString())).thenReturn(100L);
            Mockito.when(mockEnv.getAuth()).thenReturn(mockAuth);
            // The session the sessions manager builds takes its Env from Env.getCurrentEnv().
            mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);

            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");
            FlightAuthResult authResult = FlightAuthResult.of("test_user", userIdentity, "127.0.0.1");

            // Two Flight sessions fill the sub-quota, next to a MySQL connection of the same user: the
            // refusal has to tell the Flight usage from the pool's count.
            ConnectContext mysql = new ConnectContext();
            mysql.setEnv(mockEnv);
            mysql.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            Assertions.assertTrue(scheduler.submit(mysql));
            Assertions.assertEquals(-1, scheduler.getConnectPoolMgr().registerConnection(mysql));
            FlightSessionsInConnectPool manager = new FlightSessionsInConnectPool(scheduler);
            for (int i = 0; i < 2; i++) {
                String token = manager.openSession(authResult);
                Assertions.assertSame(scheduler.getContextWithPeerIdentity(token), manager.getConnectContext(token));
            }
            Assertions.assertEquals(2, scheduler.getConnectPoolMgr().getFlightConnectionNum());

            FlightRuntimeException refused = Assertions.assertThrows(FlightRuntimeException.class,
                    () -> manager.openSession(authResult));
            Assertions.assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, refused.status().code());
            Assertions.assertEquals(
                    "Reach limit of connections. Total: 1000, User: 100, Current: 3, Arrow Flight SQL: 2 (current: 2)",
                    refused.status().description());
            Assertions.assertEquals(3, scheduler.getConnectionNum());
            Assertions.assertEquals(2, scheduler.getConnectPoolMgr().getFlightConnectionNum());
        }
    }
}
