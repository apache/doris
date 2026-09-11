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

package org.apache.doris.arrowflight.protocol;

import org.apache.doris.arrowflight.auth2.FlightRemoteIpServerStreamTracer;
import org.apache.doris.arrowflight.results.FlightSqlChannel;
import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TResultSinkType;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Arrow Flight SQL side of a session: the bearer token it is known by, the cache of results
 * the frontend materialized itself, the backend endpoints of the last query, and the executors
 * kept alive until the client has pulled their results.
 *
 * <p>Arrow Flight SQL has no connection in the MySQL sense: every gRPC call of a session may
 * arrive on its own thread, and nothing in the transport serializes them. {@link #runCommand}
 * does, so that a session's {@link ConnectContext}, which is not thread-safe, is only ever used
 * by one command at a time.
 */
public class FlightProtocolAdapter implements ProtocolAdapter {
    private static final Logger LOG = LogManager.getLogger(FlightProtocolAdapter.class);

    private final String peerIdentity;
    private final FlightSqlChannel channel = new FlightSqlChannel();
    private final Map<String, String> preparedQuerys = new HashMap<>();
    private String runningQuery;
    private final List<FlightSqlEndpointsLocation> endpointsLocations = Lists.newArrayList();
    private boolean returnResultFromLocal = true;
    // Executors of already-planned queries whose results are produced on the BE and pulled later
    // during the DoGet phase. Their coordinators must stay alive until the BE finishes scanning:
    // an external-table scan in batch mode lazily fetches splits from the FE (a batch SplitSource
    // held by the coordinator's scan nodes), so closing the coordinator at the end of
    // GetFlightInfo would release the SplitSource too early and make the BE's fetchSplitBatch fail
    // with "Split source X is released". These executors are finalized when the next query starts
    // on this connection, when the connection is torn down, or by the idle reaper in
    // ConnectContext.checkTimeout once the connection has been sleeping for
    // arrow_flight_deferred_query_idle_timeout_second. See #62259 and #67503.
    private final List<StmtExecutor> deferredExecutors = new ArrayList<>();
    // Serializes the commands of this session, see runCommand.
    private final ReentrantLock commandLock = new ReentrantLock();

    public FlightProtocolAdapter(String peerIdentity) {
        this.peerIdentity = peerIdentity;
    }

    /** The Arrow Flight SQL side of {@code ctx}; throws if the connection speaks another protocol. */
    public static FlightProtocolAdapter of(ConnectContext ctx) {
        ProtocolAdapter adapter = ctx.getProtocolAdapter();
        if (adapter instanceof FlightProtocolAdapter) {
            return (FlightProtocolAdapter) adapter;
        }
        throw new IllegalStateException("not an Arrow Flight SQL connection: " + adapter.type());
    }

    @Override
    public ConnectType type() {
        return ConnectType.ARROW_FLIGHT_SQL;
    }

    @Override
    public String remoteHostPortString(ConnectContext ctx) {
        // An Arrow Flight SQL session has no MysqlChannel. The client address is captured when the
        // bearer token is issued (FlightRemoteIpServerStreamTracer) and kept on the context. There is
        // no stable peer port to report: every gRPC call of a session may arrive on its own connection.
        return Strings.isNullOrEmpty(ctx.getRemoteIP())
                ? FlightRemoteIpServerStreamTracer.UNKNOWN_REMOTE_IP : ctx.getRemoteIP();
    }

    @Override
    public TResultSinkType resultSinkType() {
        return TResultSinkType.ARROW_FLIGHT_PROTOCOL;
    }

    @Override
    public ConnectPoolMgr connectPool(ConnectScheduler scheduler) {
        return scheduler.getFlightSqlConnectPoolMgr();
    }

    @Override
    public void afterStatement(ConnectContext ctx) {
        // The channel keeps the statement's result until the client pulls it with DoGet, and the
        // next statement resets it; nothing to drop here.
    }

    @Override
    public void closeConnection(ConnectContext ctx) {
        // Releases the channel, the deferred executors and the transaction of the session.
        connectPool(ctx.getConnectScheduler()).unregisterConnection(ctx);
    }

    public String getPeerIdentity() {
        return peerIdentity;
    }

    public FlightSqlChannel getChannel() {
        return channel;
    }

    public void addPreparedQuery(String preparedStatementId, String preparedQuery) {
        preparedQuerys.put(preparedStatementId, preparedQuery);
    }

    public String getPreparedQuery(String preparedStatementId) {
        return preparedQuerys.get(preparedStatementId);
    }

    public void removePreparedQuery(String preparedStatementId) {
        preparedQuerys.remove(preparedStatementId);
    }

    public void setRunningQuery(String runningQuery) {
        this.runningQuery = runningQuery;
    }

    public String getRunningQuery() {
        return runningQuery;
    }

    public void addEndpointsLocation(FlightSqlEndpointsLocation endpointsLocation) {
        endpointsLocations.add(endpointsLocation);
    }

    public List<FlightSqlEndpointsLocation> getEndpointsLocations() {
        return endpointsLocations;
    }

    public void clearEndpointsLocations() {
        endpointsLocations.clear();
    }

    public void setReturnResultFromLocal(boolean returnResultFromLocal) {
        this.returnResultFromLocal = returnResultFromLocal;
    }

    public boolean isReturnResultFromLocal() {
        return returnResultFromLocal;
    }

    public void addDeferredExecutor(StmtExecutor executor) {
        synchronized (deferredExecutors) {
            deferredExecutors.add(executor);
        }
    }

    public void closeDeferredExecutors() {
        List<StmtExecutor> toClose;
        synchronized (deferredExecutors) {
            if (deferredExecutors.isEmpty()) {
                return;
            }
            toClose = new ArrayList<>(deferredExecutors);
            deferredExecutors.clear();
        }
        for (StmtExecutor deferredExecutor : toClose) {
            try {
                deferredExecutor.finalizeArrowFlightQuery();
            } catch (Throwable t) {
                LOG.warn("failed to finalize deferred arrow flight executor", t);
            }
        }
    }

    /**
     * How long, in seconds, a sleeping connection may keep its deferred executors before the
     * timeout checker finalizes them without killing the connection
     * (Config.arrow_flight_deferred_query_idle_timeout_second). A Flight client that opens a
     * session per query and never closes it would otherwise pin each deferred query's query queue
     * slot and query registration until wait_timeout (8h by default). The bound is never shorter
     * than the execution timeout the deferred query was run with: the client may still be pulling
     * that query's results from the BE, which still needs the batch split source the coordinator
     * holds. Returns -1 when the bound is disabled or nothing is deferred.
     */
    public long getDeferredExecutorsIdleTimeoutS() {
        int configTimeoutS = Config.arrow_flight_deferred_query_idle_timeout_second;
        if (configTimeoutS <= 0) {
            return -1;
        }
        long execTimeoutS = -1;
        synchronized (deferredExecutors) {
            if (deferredExecutors.isEmpty()) {
                return -1;
            }
            for (StmtExecutor deferredExecutor : deferredExecutors) {
                execTimeoutS = Math.max(execTimeoutS, deferredExecutor.getDeferredExecTimeoutS());
            }
        }
        return Math.max(configTimeoutS, execTimeoutS);
    }

    /** The body of a command run by {@link #runCommand}. */
    @FunctionalInterface
    public interface SessionAction<E extends Exception> {
        void run() throws E;
    }

    /** The body of a command run by {@link #callCommand}. */
    @FunctionalInterface
    public interface SessionCommand<T, E extends Exception> {
        T call() throws E;
    }

    /**
     * Runs one command of the session, and no other one at the same time: a statement, a prepared
     * statement action, a DoGet of a frontend-side result, a metadata request. The session's
     * {@link ConnectContext} is the thread's current context while the command runs. A command
     * that finds another one still running waits for it up to the session's query timeout and
     * then fails with {@code UNAVAILABLE} instead of running concurrently on the same context.
     *
     * <p>Session teardown (bearer token expiry, CloseSession, KILL) does not go through here.
     */
    public <E extends Exception> void runCommand(ConnectContext ctx, SessionAction<E> action) throws E {
        this.<Void, E>callCommand(ctx, () -> {
            action.run();
            return null;
        });
    }

    /** {@link #runCommand} for a command that returns a value. */
    public <T, E extends Exception> T callCommand(ConnectContext ctx, SessionCommand<T, E> command) throws E {
        acquireCommandLock(ctx);
        ConnectContext previous = ConnectContext.get();
        ctx.setThreadLocalInfo();
        try {
            return command.call();
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
            commandLock.unlock();
        }
    }

    private void acquireCommandLock(ConnectContext ctx) {
        long waitS = ctx.getQueryTimeoutS();
        boolean locked;
        try {
            locked = commandLock.tryLock(waitS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CallStatus.CANCELLED.withDescription("interrupted while waiting for the previous command of "
                    + "this Arrow Flight SQL session to finish").withCause(e).toRuntimeException();
        }
        if (!locked) {
            throw CallStatus.UNAVAILABLE.withDescription(String.format("another command of this Arrow Flight SQL "
                    + "session is still running after %d seconds, connection id: %d", waitS, ctx.getConnectionId()))
                    .toRuntimeException();
        }
    }
}
