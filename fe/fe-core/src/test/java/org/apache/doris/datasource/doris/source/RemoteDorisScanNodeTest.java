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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.datasource.split.SplitSource;
import org.apache.doris.datasource.split.SplitSourceManager;
import org.apache.doris.datasource.split.SplitToScanRange;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TRemoteDorisFileDesc;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A remote Doris scan reaches the remote frontend only when the coordinator dispatches its plan. Planning builds a
 * scan range per backend pointing at a split source and contacts nothing; the split assignment runs the query once
 * the coordinator starts it, and holds the Flight SQL session the query runs in until the coordinator stops the
 * scan. Nothing is left behind - not by a failed query, not by a stop that came while the query was running. The
 * remote frontend is an in-process Flight SQL server that counts what the scan does to it.
 */
public class RemoteDorisScanNodeTest {
    private static final String USER = "catalog_user";
    private static final String PASSWORD = "catalog_password";
    private static final String QUERY = "SELECT `id` FROM `db`.`t`";
    private static final Location ENDPOINT_LOCATION = Location.forGrpcInsecure("127.0.0.1", 9999);

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
                    ENDPOINT_LOCATION);
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
    private final Backend backend = new Backend(10001L, "127.0.0.1", 9050);

    @BeforeEach
    public void startRemoteFrontend() throws Exception {
        new ConnectContext().setThreadLocalInfo();
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
        ConnectContext.remove();
        server.close();
        serverAllocator.close();
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
            try {
                Field field = c.getDeclaredField(name);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                // declared further up
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static Object getField(Object target, String name) throws Exception {
        for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
            try {
                Field field = c.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException e) {
                // declared further up
            }
        }
        throw new NoSuchFieldException(name);
    }

    // A remote Doris scan of a catalog pointing at the in-process remote frontend, its query built (what
    // convertPredicate does from the finalized slots and conjuncts plays no part here).
    private RemoteDorisScanNode scanNode(String query) throws Exception {
        RemoteDorisScanNode node = Mockito.mock(RemoteDorisScanNode.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(query).when(node).getQueryStr();
        RemoteDorisExternalCatalog catalog = Mockito.mock(RemoteDorisExternalCatalog.class);
        Mockito.when(catalog.getUsername()).thenReturn(USER);
        Mockito.when(catalog.getPassword()).thenReturn(PASSWORD);
        // The catalog lists two remote frontends, both this one.
        Mockito.when(catalog.getQueryRetryCount()).thenReturn(2);
        Mockito.when(catalog.getQueryTimeoutSec()).thenReturn(10);
        Mockito.when(catalog.getProperties()).thenReturn(new HashMap<>());
        RemoteDorisSource source = Mockito.mock(RemoteDorisSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.nextHostAndArrowPort()).thenReturn(hostAndPort);
        Mockito.when(source.getHostAndArrowPort()).thenReturn(hostAndPort);
        setField(node, "source", source);
        return node;
    }

    private FederationBackendPolicy backendPolicy() throws Exception {
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.numBackends()).thenReturn(1);
        Mockito.when(policy.getBackends()).thenReturn(Lists.newArrayList(backend));
        Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(invocation -> {
            Multimap<Backend, Split> assignment = ArrayListMultimap.create();
            assignment.putAll(backend, invocation.<List<Split>>getArgument(0));
            return assignment;
        });
        return policy;
    }

    // What planning leaves a scan node with, before FileQueryScanNode.createScanRangeLocations runs.
    private void initForPlanning(RemoteDorisScanNode node) throws Exception {
        setField(node, "params", new TFileScanRangeParams());
        setField(node, "sessionVariable", new SessionVariable());
        setField(node, "backendPolicy", backendPolicy());
        setField(node, "scanRangeLocations", new ArrayList<TScanRangeLocations>());
        setField(node, "scanBackendIds", new LinkedHashSet<Long>());
    }

    // The split assignment planning gives the scan, built directly: the scan ranges play no part.
    private SplitAssignment splitAssignment(RemoteDorisScanNode node) throws Exception {
        SplitToScanRange toScanRange = Mockito.mock(SplitToScanRange.class);
        Mockito.when(toScanRange.getScanRange(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(new TScanRangeLocations());
        SplitAssignment assignment = new SplitAssignment(backendPolicy(), node, toScanRange, new HashMap<>(),
                new ArrayList<>(), false, new SplitSourceManager());
        setField(node, "splitAssignment", assignment);
        return assignment;
    }

    private static Coordinator coordinator(ScanNode... scanNodes) {
        return new Coordinator(1L, new TUniqueId(1L, 2L), new DescriptorTable(), Lists.<PlanFragment>newArrayList(),
                Lists.newArrayList(scanNodes), "UTC", false, false);
    }

    private static long splitSourceId(TScanRangeLocations range) {
        return range.getScanRange().getExtScanRange().getFileScanRange().getSplitSource().getSplitSourceId();
    }

    @Test
    public void testPlanningContactsNoRemoteFrontend() throws Exception {
        RemoteDorisScanNode node = scanNode(QUERY);
        initForPlanning(node);

        node.createScanRangeLocations();

        // A scan range per backend, pointing at a split source no backend can reach yet ...
        List<TScanRangeLocations> ranges = node.getScanRangeLocations(0);
        Assertions.assertEquals(1, ranges.size());
        SplitSourceManager manager = Env.getCurrentEnv().getSplitSourceManager();
        Assertions.assertNull(manager.getSplitSource(splitSourceId(ranges.get(0))));
        // ... and nothing ran on the remote frontend: an EXPLAIN, a plan built only to be inspected, a statement
        // that fails before dispatch leave nothing there. Stopping such a plan releases nothing either.
        Assertions.assertTrue(remote.queries.isEmpty());
        node.stop();
        Assertions.assertTrue(remote.queries.isEmpty());
        Assertions.assertTrue(remote.closedSessions.isEmpty());
    }

    @Test
    public void testDispatchRunsTheQueryOnceAndTheCoordinatorEndsItsSession() throws Exception {
        RemoteDorisScanNode node = scanNode(QUERY);
        initForPlanning(node);
        node.createScanRangeLocations();
        long sourceId = splitSourceId(node.getScanRangeLocations(0).get(0));
        Coordinator coordinator = coordinator(node);

        // The coordinator dispatching the plan starts the scan: the query runs once, in a session kept open ...
        ScanNode.startAll(Lists.newArrayList(node));
        Assertions.assertEquals(Collections.singletonList(QUERY), remote.queries);
        Assertions.assertTrue(remote.closedSessions.isEmpty());
        // ... and the endpoint of its result is the split the backend fetches from the split source.
        SplitSource splitSource = Env.getCurrentEnv().getSplitSourceManager().getSplitSource(sourceId);
        List<TScanRangeLocations> splits = splitSource.getNextBatch(10);
        Assertions.assertEquals(1, splits.size());
        TRemoteDorisFileDesc fileDesc = splits.get(0).getScanRange().getExtScanRange().getFileScanRange()
                .getRanges().get(0).getTableFormatParams().getRemoteDorisParams();
        Assertions.assertEquals(ENDPOINT_LOCATION.getUri().toString(), fileDesc.getLocationUri());
        // The backend reads the remote query until the coordinator closes, so the coordinator outlives dispatch.
        Assertions.assertTrue(node.coordinatorMustOutliveDispatch());
        Assertions.assertTrue(coordinator.mustOutliveDispatch());

        // A cancel - a KILL, the timeout checker - stops the scan: the session ends, the split source is gone ...
        coordinator.cancel(new Status(TStatusCode.CANCELLED, "killed"));

        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
        Assertions.assertNull(Env.getCurrentEnv().getSplitSourceManager().getSplitSource(sourceId));
        // ... and the close() that follows stops it again: the session is closed once.
        coordinator.close();
        Assertions.assertEquals(1, remote.closedSessions.size());
        // The query and the split sources of the scan ranges are gone: the same plan cannot run again.
        Assertions.assertTrue(node.cannotBeRedispatched());
    }

    @Test
    public void testFailedQueryClosesItsSessionsAndFailsTheDispatch() throws Exception {
        RemoteDorisScanNode node = scanNode("select boom");
        SplitAssignment assignment = splitAssignment(node);

        UserException e = Assertions.assertThrows(UserException.class, assignment::start);

        Assertions.assertTrue(e.getMessage().contains("Failed to execute query"), e.getMessage());
        // Tried on each remote frontend in turn, each session closed as its query failed: none left behind.
        Assertions.assertEquals(2, remote.queries.size());
        Assertions.assertEquals(Lists.newArrayList(USER, USER), remote.closedSessions);
    }

    @Test
    public void testSessionOpenedAfterTheScanWasStoppedIsClosedAtOnce() throws Exception {
        // The coordinator was cancelled - a KILL, the timeout checker - while the scan was running its query.
        RemoteDorisScanNode node = scanNode(QUERY);
        SplitAssignment assignment = splitAssignment(node);
        assignment.stop();

        node.executeFlightSqlQuery(hostAndPort, USER, PASSWORD, QUERY, 10);

        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
    }

    @Test
    public void testStoppedScanTriesNoRemoteFrontend() throws Exception {
        // Stopped between two attempts on the remote frontends; the coordinator reports why.
        RemoteDorisScanNode node = scanNode(QUERY);
        SplitAssignment assignment = splitAssignment(node);
        assignment.stop();

        node.startSplit(1);

        Assertions.assertTrue(remote.queries.isEmpty());
        Assertions.assertTrue(remote.closedSessions.isEmpty());
    }

    @Test
    public void testRefusedHandshakeOpensNoSession() {
        Assertions.assertThrows(Exception.class,
                () -> RemoteDorisFlightSession.open(hostAndPort, USER, "wrong password", 10));

        Assertions.assertTrue(remote.queries.isEmpty());
        Assertions.assertTrue(remote.closedSessions.isEmpty());
    }

    @Test
    public void testHandshakeTheRemoteFrontendNeverAnswersGivesUpAtTheTimeout() throws Exception {
        // A remote frontend that accepts the connection and never answers the handshake: its authenticator waits until
        // the test lets it go.
        CountDownLatch answer = new CountDownLatch(1);
        FlightServer hanging = FlightServer.builder(serverAllocator, Location.forGrpcInsecure("127.0.0.1", 0), remote)
                .headerAuthenticator(headers -> {
                    try {
                        answer.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw CallStatus.UNAUTHENTICATED.toRuntimeException();
                })
                .build()
                .start();
        ExecutorService coordinator = Executors.newSingleThreadExecutor();
        try {
            // The coordinator dispatching the plan opens the session while it holds the query's admission slot, which
            // neither KILL nor the query's timeout takes back from it: the handshake has to give up by itself.
            Future<RemoteDorisFlightSession> opening = coordinator.submit(() -> RemoteDorisFlightSession.open(
                    Pair.of("127.0.0.1", hanging.getPort()), USER, PASSWORD, 1));
            ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                    () -> opening.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(FlightStatusCode.TIMED_OUT,
                    ((FlightRuntimeException) e.getCause()).status().code(), String.valueOf(e.getCause()));
        } finally {
            answer.countDown();
            coordinator.shutdownNow();
            hanging.close();
        }
        Assertions.assertTrue(remote.queries.isEmpty());
        Assertions.assertTrue(remote.closedSessions.isEmpty());
    }

    @Test
    public void testSessionCloseIsIdempotent() throws Exception {
        RemoteDorisFlightSession session = RemoteDorisFlightSession.open(hostAndPort, USER, PASSWORD, 10);
        session.execute("select 1", 10);

        session.close();
        session.close();

        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
    }

    // A batch scan that plans with its first split - a hive or an iceberg one, whose split generators need a live
    // connector - starts generating its splits while it is planned. A remote Doris scan made to plan that way stands
    // in for one here, its generator queueing an endpoint of a query that ran already.
    private RemoteDorisScanNode scanStartingWhilePlanned() throws Exception {
        RemoteDorisScanNode node = scanNode(QUERY);
        initForPlanning(node);
        Mockito.doReturn(true).when(node).needsSampleSplit();
        Mockito.doAnswer(invocation -> {
            SplitAssignment assignment = (SplitAssignment) getField(node, "splitAssignment");
            assignment.addToQueue(Collections.singletonList(new RemoteDorisSplit(
                    ENDPOINT_LOCATION.getUri().toString(), ByteBuffer.wrap(QUERY.getBytes(StandardCharsets.UTF_8)))));
            assignment.finishSchedule();
            return null;
        }).when(node).startSplit(Mockito.anyInt());
        return node;
    }

    @Test
    public void testScanStartedWhilePlannedIsStoppedWhenItsStatementEndsUndispatched() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext.get().setStatementContext(statementContext);
        RemoteDorisScanNode node = scanStartingWhilePlanned();

        node.createScanRangeLocations();

        // Started while planned: its split source is reachable ...
        SplitSourceManager manager = Env.getCurrentEnv().getSplitSourceManager();
        long sourceId = splitSourceId(node.getScanRangeLocations(0).get(0));
        Assertions.assertNotNull(manager.getSplitSource(sourceId));
        // ... but no coordinator dispatches the plan - an EXPLAIN, the plan CREATE JOB validates, a statement refused
        // before dispatch - and the statement stops the scan when it ends.
        statementContext.close();
        Assertions.assertNull(manager.getSplitSource(sourceId));
        Assertions.assertTrue(node.cannotBeRedispatched());
    }

    @Test
    public void testScanStartedWhilePlannedIsLeftToTheCoordinatorThatDispatchedIt() throws Exception {
        StatementContext statementContext = new StatementContext();
        ConnectContext.get().setStatementContext(statementContext);
        RemoteDorisScanNode node = scanStartingWhilePlanned();
        node.createScanRangeLocations();
        SplitSourceManager manager = Env.getCurrentEnv().getSplitSourceManager();
        long sourceId = splitSourceId(node.getScanRangeLocations(0).get(0));
        Coordinator coordinator = coordinator(node);

        // The coordinator dispatching the plan takes the scan over, so the end of the statement leaves it running -
        // an Arrow Flight SQL query's coordinator outlives its statement until the client has fetched the result ...
        ScanNode.startAll(Lists.newArrayList(node));
        statementContext.close();
        Assertions.assertNotNull(manager.getSplitSource(sourceId));
        // ... and stops it when it closes.
        coordinator.close();
        Assertions.assertNull(manager.getSplitSource(sourceId));
    }

    @Test
    public void testCoordinatorStopsTheRemoteScanAfterOneThatFailsToStop() throws Exception {
        // A batch-mode external scan planned before the remote Doris scan, whose split assignment
        // rethrows the failure of its asynchronous split generation from stop().
        ScanNode failing = Mockito.mock(ScanNode.class);
        Mockito.doThrow(new RuntimeException("split generation failed")).when(failing).stop();
        RemoteDorisScanNode node = scanNode(QUERY);
        splitAssignment(node).start();
        Assertions.assertTrue(remote.closedSessions.isEmpty());

        coordinator(failing, node).close();

        Mockito.verify(failing).stop();
        Assertions.assertEquals(Collections.singletonList(USER), remote.closedSessions);
    }
}
