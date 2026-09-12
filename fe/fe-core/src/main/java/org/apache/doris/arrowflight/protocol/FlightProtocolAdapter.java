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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TResultSinkType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
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
    // Whether the result of the statement being executed is on this frontend (a SHOW, a SET, an
    // EXPLAIN: cached on the channel for the client's DoGet) or on the backends the coordinator
    // ran the query on, registered in endpointsLocations for the client to pull from. Set by the
    // statement lifecycle hooks below.
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
        throw new IllegalStateException("not an Arrow Flight SQL connection: "
                + (adapter == null ? "no protocol adapter" : adapter.type()));
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
    public FlightResultSender resultSender(ConnectContext ctx) {
        return new FlightResultSender(ctx, this);
    }

    /**
     * The SQL cache keeps the result rows in MySQL wire format, which cannot be turned into the
     * Arrow batches a Flight client needs; the cached rows would be wrong for it anyway (object
     * types such as HLL / BITMAP / QUANTILE_STATE were serialized as NULL under
     * return_object_data_as_binary=false). A Flight session always re-executes the query.
     */
    @Override
    public boolean supportsSqlCacheReplay() {
        return false;
    }

    /**
     * The master returns a query result as MySQL wire packets, which cannot be turned into the
     * Arrow batches a Flight client needs. The executor refuses to forward a query rather than
     * let the master build a result set this frontend would discard and answer the client with a
     * synthesized empty success.
     */
    @Override
    public boolean canReplayForwardedQueryResult() {
        return false;
    }

    /**
     * A result this frontend materializes is cached with every column as a Utf8 vector, whatever
     * its type ({@link FlightResultSender}). That is acceptable for the text a SHOW or an EXPLAIN
     * produces, not for a SELECT a client expects typed Arrow data from, so a query the planner
     * could answer here is run on a backend until the sender types its vectors.
     */
    @Override
    public boolean supportsFeSideResult() {
        return false;
    }

    /**
     * The short circuit produces no Arrow result at either end. PointQueryExecutor is not a
     * Coordinator, and Coordinator/NereidsCoordinator are the only places that register a
     * FlightSqlEndpointsLocation, so GetFlightInfo found none and failed the query with
     * "no FlightSqlEndpointsLocations"; the backend side cannot be pointed at either, since the
     * lookup rpc serializes with VMysqlResultWriter into PTabletKeyLookupResponse.row_batch and
     * never creates the ArrowFlightResultBlockBuffer that fetch_arrow_flight_schema looks up.
     * Arrow Flight SQL stays on the normal execution path. See #67368.
     */
    @Override
    public boolean supportsShortCircuitPointQuery() {
        return false;
    }

    /**
     * A Flight session does not retry a failed query: the backend endpoints the failed attempt
     * registered would have to be withdrawn first, and nothing does that yet.
     */
    @Override
    public boolean canRetryQuery(ConnectContext ctx) {
        return false;
    }

    /** A statement's result is on this frontend until {@link #beforeQuery} says otherwise. */
    @Override
    public void beforeStatement(ConnectContext ctx) {
        returnResultFromLocal = true;
    }

    /**
     * The query's result stays on the backends for the client to pull with DoGet; the
     * coordinator registers where ({@link #addEndpointsLocation}) instead of fetching the rows.
     */
    @Override
    public void beforeQuery(ConnectContext ctx) {
        returnResultFromLocal = false;
    }

    @Override
    public boolean returnsResultFromLocal(ConnectContext ctx) {
        return returnResultFromLocal;
    }

    /**
     * The master's response is consumed here as a status and, for a SHOW, a result set (see
     * {@link #carryForwardedOutcome}); it is never replayed to the client as packets, so the
     * master needs to know nothing about the client.
     */
    @Override
    public void fillForwardRequest(ConnectContext ctx, TMasterOpRequest request) {
    }

    @Override
    public ConnectPoolMgr connectPool(ConnectScheduler scheduler) {
        return scheduler.getFlightSqlConnectPoolMgr();
    }

    /**
     * A statement forwarded to the master has its outcome carried into this session here, the
     * way {@code MysqlProtocolAdapter.finishCommand} replays it to a MySQL client. And of the
     * statements of one request only the last may produce a result: the FlightInfo returned for
     * the request describes exactly one.
     */
    @Override
    public boolean finishStatement(ConnectContext ctx, StmtExecutor executor, int stmtIndex, int stmtCount)
            throws IOException {
        if (executor.hasForwardedToMaster()) {
            carryForwardedOutcome(ctx, executor);
        }
        Preconditions.checkState(channel.resultNum() <= 1);
        if (channel.resultNum() == 1 && stmtIndex != stmtCount - 1) {
            String errMsg = "Only be one stmt that returns the result and it is at the end. "
                    + "stmts.size(): " + stmtCount;
            LOG.warn(errMsg);
            ctx.getState().setError(ErrorCode.ERR_ARROW_FLIGHT_SQL_MUST_ONLY_RESULT_STMT, errMsg);
            ctx.getState().setErrType(QueryState.ErrType.OTHER_ERR);
            return false;
        }
        return true;
    }

    // The master answers a forwarded statement with its status and, for a SHOW, its rows. Without
    // this a forwarded statement leaves ctx.getState() at the OK that executeQuery() set with
    // reset() and leaves the FlightSqlChannel empty, so DorisFlightSqlProducer answers with
    // addOKResult()'s synthesized StatusResult=0 -- reporting success for a statement that failed
    // on the master, and an empty status row instead of the rows a forwarded SHOW produced.
    @VisibleForTesting
    void carryForwardedOutcome(ConnectContext ctx, StmtExecutor executor) throws IOException {
        if (executor.getProxyStatusCode() != 0) {
            // The master rejected the statement, e.g. CREATE TABLE on a table that already exists.
            // TMasterOpResult carries the master's error code as a plain int and ErrorCode has no
            // reverse lookup, so the master's code travels in the message instead.
            String errMsg = "forwarded statement failed on master FE, error code: "
                    + executor.getProxyStatusCode() + ", error message: " + executor.getProxyErrMsg();
            LOG.warn(errMsg);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, errMsg);
            return;
        }
        // Set exactly when the forwarded statement produced rows: proxyExecute() fills
        // TMasterOpResult.resultSet from getProxyShowResultSet(). A forwarded DDL produces none,
        // and the synthesized StatusResult=0 is the right answer for it.
        ShowResultSet resultSet = executor.getShowResultSet();
        if (resultSet != null) {
            executor.sendResultSet(resultSet);
        }
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

    /**
     * Starts a request of the session: whatever the previous request left behind is dropped.
     * Its query's coordinator, if its close was deferred, is finalized now -- the previous DoGet
     * is done by the time the next request arrives (#62259); the result it may have cached and
     * never pulled with DoGet is released; its endpoints are forgotten; and the new request's
     * result is on this frontend until a query is run for it.
     */
    public void beginRequest() {
        closeDeferredExecutors();
        channel.reset();
        endpointsLocations.clear();
        returnResultFromLocal = true;
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
     * that finds another one still running waits for it up to the session's execution timeout and
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
        // Wait as long as the running command is allowed to run: for a synchronous load statement
        // that is max(insert_timeout, query_timeout), the bound the timeout checker applies to it.
        long waitS = ctx.getExecTimeoutS();
        boolean locked;
        try {
            locked = commandLock.tryLock(waitS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("interrupted while waiting for the running command of Arrow Flight SQL connection {}",
                    ctx.getConnectionId());
            throw CallStatus.CANCELLED.withDescription("interrupted while waiting for the previous command of "
                    + "this Arrow Flight SQL session to finish").withCause(e).toRuntimeException();
        }
        if (!locked) {
            LOG.warn("a command of Arrow Flight SQL connection {} gave up after waiting {}s for the running one",
                    ctx.getConnectionId(), waitS);
            throw CallStatus.UNAVAILABLE.withDescription(String.format("another command of this Arrow Flight SQL "
                    + "session is still running after %d seconds, connection id: %d", waitS, ctx.getConnectionId()))
                    .toRuntimeException();
        }
    }
}
