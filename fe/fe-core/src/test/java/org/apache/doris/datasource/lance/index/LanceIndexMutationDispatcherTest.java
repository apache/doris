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

package org.apache.doris.datasource.lance.index;

import org.apache.doris.common.GenericPool;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TLanceIndexMutationRequest;
import org.apache.doris.thrift.TLanceIndexMutationResult;
import org.apache.doris.thrift.TLanceIndexMutationType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The real thrift dispatcher against real sockets and the real shared backend pool: the
 * judgment seam between provable pre-send failures and possible-send failures. A connection
 * refused at the borrow is typed as the pre-dispatch marker (zero payload bytes were sent);
 * a failure after the borrow succeeded stays raw (the send may have started) and invalidates
 * the connection; a completed answer is returned even when the pool cannot take the
 * connection back. No live backend is needed - a loopback stub server answers with the same
 * clean NOT_IMPLEMENTED rejection the shipped backend stub gives.
 */
public class LanceIndexMutationDispatcherTest {

    @Test
    public void connectionRefusalIsTypedAsPreDispatchFailure() throws Exception {
        // A port with no listener: the pool factory's transport.open() fails inside the
        // borrow, so the failure provably predates any invocation byte being written.
        int unusedPort = findUnusedLocalPort();
        LanceIndexMutationDispatcher.PreDispatchTransportException exception =
                Assertions.assertThrows(
                        LanceIndexMutationDispatcher.PreDispatchTransportException.class,
                        () -> ThriftLanceIndexMutationDispatcher.INSTANCE.dispatch(
                                request(), backendAt("127.0.0.1", unusedPort), 5_000L));
        Assertions.assertNotNull(exception.getCause(), "the borrow failure must ride as cause");
    }

    @Test
    public void dispatchAgainstARealBackendReturnsTheTrustedAnswer() throws Exception {
        try (StubBackendServer server = new StubBackendServer()) {
            Backend backend = backendAt("127.0.0.1", server.port());
            TLanceIndexMutationResult result = ThriftLanceIndexMutationDispatcher.INSTANCE
                    .dispatch(request(), backend, 5_000L);
            Assertions.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR,
                    result.getStatus().getStatusCode());
            // The completed connection was returned to the pool and answers again over the
            // same address: the happy borrow/call/return cycle leaves no residue.
            TLanceIndexMutationResult again = ThriftLanceIndexMutationDispatcher.INSTANCE
                    .dispatch(request(), backend, 5_000L);
            Assertions.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR,
                    again.getStatus().getStatusCode());
        }
    }

    @Test
    public void callPhaseFailureStaysRawAndInvalidatesTheConnection() throws Exception {
        try (AcceptAndCloseServer server = new AcceptAndCloseServer()) {
            GenericPool<BackendService.Client> pool = mockedPoolReturning(
                    openedClient("127.0.0.1", server.port()));
            ThriftLanceIndexMutationDispatcher dispatcher =
                    new ThriftLanceIndexMutationDispatcher(pool);
            // The borrow succeeded, so whatever the call threw may already have written the
            // request: the failure stays raw (the indeterminate family), never the
            // pre-dispatch marker - which is exactly why assertThrows pins the raw type.
            Assertions.assertThrows(TTransportException.class,
                    () -> dispatcher.dispatch(request(), backendAt("127.0.0.1", server.port()),
                            5_000L));
            Mockito.verify(pool).invalidateObject(Mockito.any(TNetworkAddress.class),
                    Mockito.any(BackendService.Client.class));
            Mockito.verify(pool, Mockito.never()).returnObject(Mockito.any(TNetworkAddress.class),
                    Mockito.any(BackendService.Client.class));
        }
    }

    @Test
    public void poolReturnFailureDoesNotDiscardTheTrustedResult() throws Exception {
        try (StubBackendServer server = new StubBackendServer()) {
            GenericPool<BackendService.Client> pool = mockedPoolReturning(
                    openedClient("127.0.0.1", server.port()));
            Mockito.doThrow(new IllegalStateException("pool closed"))
                    .when(pool).returnObject(Mockito.any(TNetworkAddress.class),
                            Mockito.any(BackendService.Client.class));
            ThriftLanceIndexMutationDispatcher dispatcher =
                    new ThriftLanceIndexMutationDispatcher(pool);
            TLanceIndexMutationResult result = dispatcher.dispatch(
                    request(), backendAt("127.0.0.1", server.port()), 5_000L);
            // The trusted answer survives the return failure; the connection is degraded to
            // invalidated instead of the failure replacing the result.
            Assertions.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR,
                    result.getStatus().getStatusCode());
            Mockito.verify(pool).returnObject(Mockito.any(TNetworkAddress.class),
                    Mockito.any(BackendService.Client.class));
            Mockito.verify(pool).invalidateObject(Mockito.any(TNetworkAddress.class),
                    Mockito.any(BackendService.Client.class));
        }
    }

    // ---------------------------------------------------------------- helpers

    private static TLanceIndexMutationRequest request() {
        // Every required field is set so the server-side thrift read cannot fail on
        // requiredness; the content mirrors the executor's wire request shape.
        TLanceIndexMutationRequest request = new TLanceIndexMutationRequest();
        request.setInvocationId("invocation-1");
        request.setBeProcessEpoch(42L);
        request.setDeadlineMs(System.currentTimeMillis() + 60_000L);
        request.setMutationType(TLanceIndexMutationType.CREATE);
        request.setIndexName("idx");
        request.setColumnName("v");
        request.setIndexType("IVF_PQ");
        request.setDatasetUri("s3://bucket/dataset");
        request.setAdmittedDatasetVersion(7L);
        return request;
    }

    private static Backend backendAt(String host, int port) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn(host);
        Mockito.when(backend.getBePort()).thenReturn(port);
        return backend;
    }

    private static int findUnusedLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }

    @SuppressWarnings("unchecked")
    private static GenericPool<BackendService.Client> mockedPoolReturning(
            BackendService.Client client) throws Exception {
        GenericPool<BackendService.Client> pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(Mockito.any(TNetworkAddress.class), Mockito.anyInt()))
                .thenReturn(client);
        return pool;
    }

    /** Opens the client the way the pool factory would: a plain blocking TSocket, already open. */
    private static BackendService.Client openedClient(String host, int port) throws Exception {
        TSocket transport = new TSocket(host, port, 10_000);
        transport.open();
        return new BackendService.Client(new TBinaryProtocol(transport));
    }

    /**
     * A loopback thrift backend whose lance worker answers with the same clean
     * NOT_IMPLEMENTED rejection the shipped backend stub gives, so the dispatch completes
     * and the return-to-pool path runs for real. The worker threads are daemons on purpose:
     * a connection returned to the pool stays idle-open, and its server-side worker blocks
     * reading it - a non-daemon default executor would outlive the test JVM.
     */
    private static final class StubBackendServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final TServer server;
        private final ExecutorService serverWorkers;
        private final Thread serverThread;

        StubBackendServer() throws Exception {
            this.serverSocket = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
            BackendService.Iface stub = Mockito.mock(BackendService.Iface.class);
            Mockito.when(stub.lanceIndexMutate(Mockito.any(TLanceIndexMutationRequest.class)))
                    .thenAnswer(invocation -> {
                        TLanceIndexMutationResult answer = new TLanceIndexMutationResult();
                        answer.setStatus(new TStatus(TStatusCode.NOT_IMPLEMENTED_ERROR));
                        return answer;
                    });
            this.serverWorkers = Executors.newCachedThreadPool(runnable -> {
                Thread worker = new Thread(runnable, "stub-backend-worker-" + port());
                worker.setDaemon(true);
                return worker;
            });
            this.server = new TThreadPoolServer(new TThreadPoolServer.Args(
                    new TServerSocket(serverSocket)).processor(new BackendService.Processor<>(stub))
                    .executorService(serverWorkers));
            this.serverThread = new Thread(server::serve, "stub-backend-" + port());
            serverThread.setDaemon(true);
            serverThread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() {
            server.stop();
            serverWorkers.shutdownNow();
            try {
                serverThread.join(5_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Already closed by the server transport.
            }
        }
    }

    /**
     * A loopback listener that accepts every connection and closes it at once: the borrow
     * (the TCP connect) succeeds, and the failure surfaces inside the call - the exact
     * possible-send phase.
     */
    private static final class AcceptAndCloseServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final Thread reaper;
        private volatile boolean closed = false;

        AcceptAndCloseServer() throws Exception {
            this.serverSocket = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
            this.reaper = new Thread(() -> {
                while (!closed) {
                    try (Socket accepted = serverSocket.accept()) {
                        // Closed by the try-with-resources immediately.
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "accept-and-close-" + port());
            reaper.setDaemon(true);
            reaper.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            serverSocket.close();
        }
    }
}
