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

package org.apache.doris.datasource.doris.source;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The Flight SQL session a remote Doris scan opens on the remote frontend lives exactly as long as
 * the scan: opened for the query in getSplits, ended with a CloseSession when the coordinator stops
 * the scan node, and never left behind - not by a failed query, not by a stop() that came first.
 * The remote frontend is an in-process Flight SQL server that counts what the scan does to it.
 */
public class RemoteDorisScanNodeTest {
    private static final String USER = "catalog_user";
    private static final String PASSWORD = "catalog_password";

    /** A remote frontend that records the queries it ran and the sessions it was asked to close. */
    private static class RecordingRemoteFrontend extends NoOpFlightSqlProducer {
        final List<String> queries = new CopyOnWriteArrayList<>();
        final List<String> closedSessions = new CopyOnWriteArrayList<>();

        @Override
        public FlightInfo getFlightInfoStatement(CommandStatementQuery command, CallContext context,
                FlightDescriptor descriptor) {
            String query = command.getQuery();
            queries.add(query);
            if (query.contains("boom")) {
                throw CallStatus.INTERNAL.withDescription("query failed on the remote frontend").toRuntimeException();
            }
            FlightEndpoint endpoint = new FlightEndpoint(new Ticket(query.getBytes(StandardCharsets.UTF_8)),
                    Location.forGrpcInsecure("127.0.0.1", 9999));
            return new FlightInfo(new Schema(Collections.emptyList()), descriptor,
                    Collections.singletonList(endpoint), -1, -1);
        }

        @Override
        public void closeSession(CloseSessionRequest request, CallContext context,
                StreamListener<CloseSessionResult> listener) {
            closedSessions.add(context.peerIdentity());
            listener.onNext(new CloseSessionResult(CloseSessionResult.Status.CLOSED));
            listener.onCompleted();
        }
    }

    private BufferAllocator serverAllocator;
    private RecordingRemoteFrontend remote;
    private FlightServer server;
    private Pair<String, Integer> hostAndPort;

    @BeforeEach
    public void startRemoteFrontend() throws Exception {
        serverAllocator = new RootAllocator();
        remote = new RecordingRemoteFrontend();
        // The handshake the scan performs (authenticateBasicToken) opens a session and issues a bearer
        // token for it, and the peer identity a later call carries is the one authenticated then.
        server = FlightServer.builder(serverAllocator, Location.forGrpcInsecure("127.0.0.1", 0), remote)
                .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                        new BasicCallHeaderAuthenticator((user, password) -> {
                            if (USER.equals(user) && PASSWORD.equals(password)) {
                                return () -> user;
                            }
                            throw CallStatus.UNAUTHENTICATED.withDescription("bad credentials").toRuntimeException();
                        })))
                .build()
                .start();
        hostAndPort = Pair.of("127.0.0.1", server.getPort());
    }

    @AfterEach
    public void stopRemoteFrontend() throws Exception {
        server.close();
        serverAllocator.close();
    }

    private static RemoteDorisScanNode scanNode() {
        // The node under test is only its session bookkeeping; the planner state a real node carries
        // (descriptors, the source, the catalog) plays no part in it.
        return Mockito.mock(RemoteDorisScanNode.class, Mockito.CALLS_REAL_METHODS);
    }

    private static Coordinator coordinator(ScanNode... scanNodes) {
        return new Coordinator(1L, new TUniqueId(1L, 2L), new DescriptorTable(), Lists.<PlanFragment>newArrayList(),
                Lists.newArrayList(scanNodes), "UTC", false, false);
    }

    @Test
    public void testSessionLivesFromTheQueryUntilTheScanStops() throws Exception {
        RemoteDorisScanNode node = scanNode();
        List<Pair<String, ByteBuffer>> endpoints = node.executeFlightSqlQuery(hostAndPort, USER, PASSWORD,
                "select 1", 10);

        Assertions.assertEquals(1, endpoints.size());
        Assertions.assertEquals(Collections.singletonList("select 1"), remote.queries);
        // Open on the remote frontend, so the local coordinator has to outlive dispatch: its close is
        // what ends the session, and the BE may still be reading the remote query until then.
        Assertions.assertTrue(remote.closedSessions.isEmpty());
        Assertions.assertTrue(node.coordinatorMustOutliveDispatch());
        Assertions.assertTrue(coordinator(node).mustOutliveDispatch());

        node.stop();

        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
        Assertions.assertFalse(node.coordinatorMustOutliveDispatch());
        Assertions.assertFalse(coordinator(node).mustOutliveDispatch());

        // cancel() then close() both stop the scan node; the session is closed once.
        node.stop();
        Assertions.assertEquals(1, remote.closedSessions.size());
    }

    @Test
    public void testFailedQueryClosesItsSessionAtOnce() {
        RemoteDorisScanNode node = scanNode();

        Assertions.assertThrows(Exception.class,
                () -> node.executeFlightSqlQuery(hostAndPort, USER, PASSWORD, "select boom", 10));

        // The retry on the next node must not leave this node's session behind.
        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
        Assertions.assertFalse(node.coordinatorMustOutliveDispatch());
    }

    @Test
    public void testRefusedHandshakeOpensNoSession() {
        Assertions.assertThrows(Exception.class,
                () -> RemoteDorisFlightSession.open(hostAndPort, USER, "wrong password"));

        Assertions.assertTrue(remote.queries.isEmpty());
        Assertions.assertTrue(remote.closedSessions.isEmpty());
    }

    @Test
    public void testSessionHandedOverAfterStopIsClosedAtOnce() throws Exception {
        RemoteDorisScanNode node = scanNode();
        node.stop();

        RemoteDorisFlightSession late = RemoteDorisFlightSession.open(hostAndPort, USER, PASSWORD);
        node.keepFlightSession(late);

        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
        Assertions.assertFalse(node.coordinatorMustOutliveDispatch());
    }

    @Test
    public void testSessionCloseIsIdempotent() throws Exception {
        RemoteDorisFlightSession session = RemoteDorisFlightSession.open(hostAndPort, USER, PASSWORD);
        session.execute("select 1", 10);

        session.close();
        session.close();

        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
    }
}
