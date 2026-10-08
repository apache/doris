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

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The Flight SQL session a {@link RemoteDorisScanNode} opens on a remote Doris frontend for one
 * scan, and ends with a CloseSession once the scan is over.
 *
 * <p>The handshake ({@code authenticateBasicToken}) opens a session on the remote frontend: a
 * connection in its pool, counted against {@code qe_max_connection}, the Arrow Flight SQL sub-quota
 * and the catalog user's {@code max_user_connections}, that only a CloseSession, a KILL or
 * {@code wait_timeout} ends. Closing the gRPC channel does not. A session opened per scan and never
 * closed therefore stays for hours and, one scan at a time, exhausts the catalog user's connection
 * quota on the remote frontend - refusing that user's MySQL connections there as well.
 *
 * <p>The session outlives GetFlightInfo on purpose: the query it ran serves the BE's DoGet of the
 * endpoints, and the remote frontend cancels whatever a closed session was still running - the
 * query itself, when the remote table is an external table scanned in batch mode and the query is
 * therefore deferred there. So {@link #close()} is called from {@link RemoteDorisScanNode#stop()},
 * when the coordinator of the local query closes, and that coordinator is kept alive until the BE
 * has finished scanning ({@link RemoteDorisScanNode#coordinatorMustOutliveDispatch()}).
 */
class RemoteDorisFlightSession implements Closeable {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisFlightSession.class);

    // A bound on the CloseSession round trip. close() runs on the local query's teardown path,
    // which must not hang on a remote frontend that has stopped answering; the session is then left
    // to the remote frontend's wait_timeout, as every session was before this class existed.
    @VisibleForTesting
    static final int CLOSE_SESSION_TIMEOUT_SECONDS = 5;

    private final Pair<String, Integer> hostAndPort;
    private final BufferAllocator allocator;
    private final FlightSqlClient client;
    private final CredentialCallOption credential;
    private boolean closed = false;

    private RemoteDorisFlightSession(Pair<String, Integer> hostAndPort, BufferAllocator allocator,
            FlightSqlClient client, CredentialCallOption credential) {
        this.hostAndPort = hostAndPort;
        this.allocator = allocator;
        this.client = client;
        this.credential = credential;
    }

    /**
     * Opens a session on the remote frontend at {@code hostAndPort} with the catalog's credentials.
     * Nothing is left behind when this fails: a handshake that was refused opened no session, and
     * the channel and allocator are released before the exception propagates.
     */
    static RemoteDorisFlightSession open(Pair<String, Integer> hostAndPort, String user, String password)
            throws Exception {
        BufferAllocator allocator = new RootAllocator();
        FlightClient flightClient = null;
        try {
            URI uri = new URI("grpc", null, hostAndPort.first, hostAndPort.second, null, null, null);
            flightClient = FlightClient.builder(allocator, new Location(uri)).build();
            Optional<CredentialCallOption> credential = flightClient.authenticateBasicToken(user, password);
            if (!credential.isPresent()) {
                throw new UserException("Authenticates with a username and password failure");
            }
            return new RemoteDorisFlightSession(hostAndPort, allocator, new FlightSqlClient(flightClient),
                    credential.get());
        } catch (Throwable t) {
            closeQuietly(flightClient, allocator, hostAndPort);
            throw t;
        }
    }

    /** Runs {@code sql} on the remote frontend; the endpoints of the result are where the BE reads it. */
    FlightInfo execute(String sql, int timeoutSec) {
        return client.execute(sql, credential, CallOptions.timeout(timeoutSec, TimeUnit.SECONDS));
    }

    Pair<String, Integer> getHostAndPort() {
        return hostAndPort;
    }

    /**
     * Ends the session on the remote frontend (CloseSession), then releases the channel and the
     * allocator. Never throws: this runs on the local query's teardown path, and a session the
     * remote frontend could not be told to close is only left to its wait_timeout. Idempotent.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            client.closeSession(new CloseSessionRequest(), credential,
                    CallOptions.timeout(CLOSE_SESSION_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } catch (Throwable t) {
            LOG.warn("failed to close the Arrow Flight SQL session on remote Doris frontend {}:{}, it stays open there"
                    + " until its wait_timeout", hostAndPort.first, hostAndPort.second, t);
        }
        closeQuietly(client, allocator, hostAndPort);
    }

    private static void closeQuietly(AutoCloseable client, BufferAllocator allocator,
            Pair<String, Integer> hostAndPort) {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Throwable t) {
            LOG.warn("failed to close the Arrow Flight client to remote Doris frontend {}:{}",
                    hostAndPort.first, hostAndPort.second, t);
        }
        try {
            allocator.close();
        } catch (Throwable t) {
            LOG.warn("failed to close the Arrow allocator of the Flight client to remote Doris frontend {}:{}",
                    hostAndPort.first, hostAndPort.second, t);
        }
    }
}
