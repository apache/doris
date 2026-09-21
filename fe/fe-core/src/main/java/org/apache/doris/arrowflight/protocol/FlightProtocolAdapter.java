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
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TokenMasker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.arrow.flight.CallStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
 *
 * <p>Nor does a query end where the frontend's command does. The client pulls the results from the
 * backends by itself (DoGet), and the frontend hears nothing of it -- not when it starts, not when
 * it is done -- so a query may still be running on the backends while the session runs its next
 * command here. Two things of a request therefore outlive the command that made them, and the
 * contract for both is that the command stream owns them and nothing else does: the result a
 * statement materialized on this frontend, cached on the channel until the client's DoGet takes it
 * or the next request drops it; and the executors of queries whose coordinator must stay alive
 * until the backends are done with it, the {@link #deferredExecutors}. A deferred executor is a
 * closed object from the moment it is deferred: it carries what finalizing it needs and reads
 * nothing of the session's live state afterwards, since the session moves on without it (a session
 * option's SET, a metadata request, the next request) and since it may be finalized from a thread
 * that runs no command of the session (the timeout checker, a token expiry). It is finalized
 * exactly once, by whoever takes it out of the list under the list's lock -- the next request
 * ({@link #beginRequest}), the timeout checker ({@link #takeExpiredDeferredExecutors}, each
 * executor by its own deadline) or teardown ({@link #tearDown}) -- deciding and taking in one
 * critical section, finalizing outside it. Teardown does not wait for a running command, so a
 * query may be deferred after teardown took everything the list held; it is then finalized on the
 * spot by the command that deferred it ({@link #addDeferredExecutor}), since nothing would take it
 * later. The frontend has no signal for the moment a query is done on the backends; the next
 * request stands in for it, as it has since #62259, and the deadline bounds the wait when no
 * request comes.
 */
public class FlightProtocolAdapter implements ProtocolAdapter {
    private static final Logger LOG = LogManager.getLogger(FlightProtocolAdapter.class);

    private final String peerIdentity;
    private final FlightSqlChannel channel = new FlightSqlChannel();
    private final Map<String, String> preparedQuerys = new HashMap<>();
    private String runningQuery;
    private final List<FlightSqlEndpointsLocation> endpointsLocations = Lists.newArrayList();
    // How many of endpointsLocations were registered before the statement being executed
    // started: what an attempt of that statement registers comes after them, and only that is
    // withdrawn when the statement is attempted again (beforeAttempt).
    private int endpointsBeforeStatement = 0;
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
    // ConnectContext.checkTimeout once each one's own bound has passed since its query started.
    // See #62259 and #67503, and the class comment for who owns them. Guarded by its own monitor:
    // the commands of the session add and take under the command lock, the timeout checker and
    // teardown take without it.
    private final List<StmtExecutor> deferredExecutors = new ArrayList<>();
    // Whether the session has been torn down (tearDown): set under the monitor of
    // deferredExecutors, so that a publication sees either the list or the tombstone; read at the
    // entry of every command as well.
    private volatile boolean closed = false;
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
     * A Flight session does not retry a failed query under a new query id within
     * {@code StmtExecutor.handleQueryWithRetry}: the client is not told which of the attempts
     * its endpoints belong to. (The replan retry of {@code StmtExecutor.queryRetry} is not asked;
     * it starts every attempt through {@link #beforeAttempt}.)
     */
    @Override
    public boolean canRetryQuery(ConnectContext ctx) {
        return false;
    }

    /** A statement's result is on this frontend until {@link #beforeQuery} says otherwise. */
    @Override
    public void beforeStatement(ConnectContext ctx) {
        returnResultFromLocal = true;
        endpointsBeforeStatement = endpointsLocations.size();
    }

    /**
     * An attempt starts where the statement did: its result is on this frontend, and it has
     * registered no endpoint yet. The attempt that failed before it may have moved the result to
     * the backends ({@link #beforeQuery}) and registered where; nothing will be pulled from
     * there, and a stale "on the backends" state would keep the statement's cleanup (its query
     * registration, its connector statement scope) waiting for a DoGet that never comes. Only
     * what that attempt registered is withdrawn: what an earlier statement of the request
     * registered is left as it was.
     */
    @Override
    public void beforeAttempt(ConnectContext ctx) {
        returnResultFromLocal = true;
        if (endpointsLocations.size() > endpointsBeforeStatement) {
            endpointsLocations.subList(endpointsBeforeStatement, endpointsLocations.size()).clear();
        }
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

    /**
     * Every Flight SQL session teardown path - the idle timeout (wait_timeout), bearer token expiry
     * or eviction, CloseSession, a KILL CONNECTION from another connection - reaches here through
     * the pool's unregisterConnection. The
     * channel-cached Arrow results go first, then the session is closed for good: teardown does not
     * wait for a command that may still be running, and what that command defers afterwards is
     * finalized on the spot ({@link #tearDown}).
     */
    @Override
    public void releaseSession(ConnectContext ctx) {
        try {
            channel.close();
        } catch (Throwable t) {
            // RootAllocator.close() marks the allocator closed before it reports outstanding
            // bytes. The error is actionable, but session teardown must still release the
            // coordinator, transaction and pool/token bookkeeping. The peer identity IS the bearer
            // token, so it is logged as a masked id, the same one FlightTokenManagerImpl uses.
            LOG.warn("failed to close Flight SQL channel while unregistering connection {}, peer identity {}",
                    ctx.getConnectionId(), TokenMasker.tokenId(peerIdentity), t);
        }
        tearDown();
    }

    /**
     * A statement forwarded to the master has its outcome carried into this session here, the
     * way {@code MysqlProtocolAdapter.finishCommand} replays it to a MySQL client. And of the
     * statements of one request only the last may produce a result: the FlightInfo returned for
     * the request describes exactly one, wherever it is. A result this frontend cached and one
     * left on the backends count the same -- the endpoints of two queries in one FlightInfo would
     * be read as the partitions of one result, and a query before a SET would have its endpoints
     * dropped for the SET's status.
     */
    @Override
    public boolean finishStatement(ConnectContext ctx, StmtExecutor executor, int stmtIndex, int stmtCount)
            throws IOException {
        if (executor.hasForwardedToMaster()) {
            carryForwardedOutcome(ctx, executor);
        }
        Preconditions.checkState(channel.resultNum() <= 1);
        // A statement that succeeded produced a result when it cached one on the channel or left
        // one on the backends for the client to pull (beforeQuery); one that failed produced none,
        // whatever it registered before failing, and the request stops with the statement's own
        // error.
        boolean producedResult = ctx.getState().getStateType() != QueryState.MysqlStateType.ERR
                && (channel.resultNum() == 1 || !returnResultFromLocal);
        if (producedResult && stmtIndex != stmtCount - 1) {
            String errMsg = "Only be one stmt that returns the result and it is at the end. "
                    + "stmts.size(): " + stmtCount;
            LOG.warn(errMsg);
            if (!returnResultFromLocal) {
                // Nobody will pull this result: stop the query on the backends now rather than
                // leave it to the BE's result buffer timer (execution_timeout + 5s). A result this
                // frontend cached (a forwarded SHOW's) has nothing to cancel.
                executor.cancel(new Status(TStatusCode.CANCELLED, errMsg));
            }
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
        ctx.getConnectScheduler().getConnectPoolMgr().unregisterConnection(ctx);
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
        endpointsBeforeStatement = 0;
        returnResultFromLocal = true;
    }

    /**
     * Keeps a deferred executor for the session to finalize later -- unless the session has been
     * torn down meanwhile ({@link #tearDown}). Teardown does not wait for a running command, so
     * its query may be deferred after teardown took everything the list held, and nothing would
     * take it then: the session runs no next request, and the timeout checker no longer sees it.
     * Such an executor is finalized here and now, by the command that deferred it.
     */
    public void addDeferredExecutor(StmtExecutor executor) {
        synchronized (deferredExecutors) {
            if (!closed) {
                deferredExecutors.add(executor);
                return;
            }
        }
        finalizeDeferredExecutors(Collections.singletonList(executor));
    }

    /**
     * Takes every deferred executor out of the list and finalizes it: the next request of the
     * session ({@link #beginRequest}), which stands in for the end of the previous query's DoGet, and
     * a request that failed after deferring its query, which no DoGet will ever pull.
     */
    public void closeDeferredExecutors() {
        finalizeDeferredExecutors(takeDeferredExecutors());
    }

    /**
     * Like {@link #closeDeferredExecutors()}, for a request that failed after deferring its query:
     * no DoGet will ever pull the result, so the query is cancelled on the backends first -- a query
     * still producing would otherwise run on until the BE's result buffer timer
     * (execution_timeout + 5s) -- and the executor is then finalized.
     */
    public void cancelDeferredExecutors(Status cancelReason) {
        List<StmtExecutor> taken = takeDeferredExecutors();
        for (StmtExecutor deferredExecutor : taken) {
            try {
                deferredExecutor.cancel(cancelReason);
            } catch (Throwable t) {
                LOG.warn("failed to cancel deferred arrow flight query {}",
                        DebugUtil.printId(deferredExecutor.getDeferredQueryId()), t);
            }
        }
        finalizeDeferredExecutors(taken);
    }

    private List<StmtExecutor> takeDeferredExecutors() {
        synchronized (deferredExecutors) {
            if (deferredExecutors.isEmpty()) {
                return Collections.emptyList();
            }
            List<StmtExecutor> taken = new ArrayList<>(deferredExecutors);
            deferredExecutors.clear();
            return taken;
        }
    }

    /**
     * Tears the session down: takes every deferred executor out of the list and finalizes it, and
     * closes the list for good. Teardown -- CloseSession, the bearer token's expiry, KILL, the
     * timeout checker -- does not wait for the command that may be running, so that command may
     * still defer its query afterwards, which is then finalized on the spot
     * ({@link #addDeferredExecutor}); and a command that was waiting for the session does not run
     * on it ({@link #callCommand}). Reached through the pool's unregisterConnection.
     */
    public void tearDown() {
        List<StmtExecutor> taken;
        synchronized (deferredExecutors) {
            closed = true;
            taken = new ArrayList<>(deferredExecutors);
            deferredExecutors.clear();
        }
        finalizeDeferredExecutors(taken);
    }

    /**
     * Takes out of the list the deferred executors whose own bound has passed by {@code now} and
     * returns them for the caller to finalize: the timeout checker's half of the exactly-once rule
     * in the class comment. Each executor is judged and removed in the same critical section, by
     * its own deadline -- when its query started plus {@link #deferredBoundMs} -- so that of the
     * executors of one multi-statement request, each with a start and an execution timeout of its
     * own, only the overdue ones go, and an executor added while the checker runs, which it never
     * judged, stays. Takes nothing when the bound is disabled
     * (Config.arrow_flight_deferred_query_idle_timeout_second is 0).
     */
    public List<StmtExecutor> takeExpiredDeferredExecutors(long now) {
        int configTimeoutS = Config.arrow_flight_deferred_query_idle_timeout_second;
        if (configTimeoutS <= 0) {
            return Collections.emptyList();
        }
        List<StmtExecutor> expired = new ArrayList<>();
        synchronized (deferredExecutors) {
            Iterator<StmtExecutor> iterator = deferredExecutors.iterator();
            while (iterator.hasNext()) {
                StmtExecutor deferredExecutor = iterator.next();
                long deferredMs = now - deferredExecutor.getDeferredStartTimeMs();
                if (deferredMs > deferredBoundMs(deferredExecutor, configTimeoutS)) {
                    expired.add(deferredExecutor);
                    iterator.remove();
                }
            }
        }
        return expired;
    }

    /**
     * How long, in milliseconds, a deferred executor may be kept after its query started before the
     * timeout checker finalizes it without killing the session: {@code configTimeoutS}
     * (Config.arrow_flight_deferred_query_idle_timeout_second) floored at the execution timeout the
     * query ran with -- the client may still be pulling its results from the BE, which still needs
     * the batch split source the coordinator holds. It counts from the query's own start, not from
     * the session's last command: a session option or a metadata request that came since neither
     * finished the query nor may keep its coordinator alive for another bound. A Flight client that
     * opens a session per query and never closes it would otherwise pin each deferred query's query
     * queue slot and query registration until wait_timeout (8h by default).
     */
    public static long deferredBoundMs(StmtExecutor deferredExecutor, int configTimeoutS) {
        return Math.max(configTimeoutS, deferredExecutor.getDeferredExecTimeoutS()) * 1000L;
    }

    /**
     * Finalizes executors taken out of the list (see {@link StmtExecutor#finalizeArrowFlightQuery}).
     * One failing does not keep the next from being finalized.
     */
    public static void finalizeDeferredExecutors(List<StmtExecutor> takenExecutors) {
        for (StmtExecutor deferredExecutor : takenExecutors) {
            try {
                deferredExecutor.finalizeArrowFlightQuery();
            } catch (Throwable t) {
                LOG.warn("failed to finalize deferred arrow flight executor", t);
            }
        }
    }

    /** A snapshot of the deferred executors, in the order they were deferred. */
    @VisibleForTesting
    public List<StmtExecutor> getDeferredExecutors() {
        synchronized (deferredExecutors) {
            return ImmutableList.copyOf(deferredExecutors);
        }
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
     * statement action, a DoGet of a frontend-side result, a metadata request, a session option
     * action. The session's {@link ConnectContext} is the thread's current context while the
     * command runs, and the command counts as activity of the session's client: wait_timeout starts
     * over, for a command that runs no statement as much as for one that does. A command that
     * finds another one still running waits for it up to the session's execution timeout and then
     * fails with {@code UNAVAILABLE} instead of running concurrently on the same context.
     *
     * <p>Session teardown (bearer token expiry, CloseSession, KILL) does not go through here and
     * does not wait for the running command ({@link #tearDown}); a command that gets its turn
     * after teardown fails with {@code UNAUTHENTICATED}, as any later call of the session would.
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
        try {
            if (closed) {
                throw CallStatus.UNAUTHENTICATED.withDescription(String.format("this Arrow Flight SQL session "
                        + "was closed, connection id: %d; reconnect to run further commands", ctx.getConnectionId()))
                        .toRuntimeException();
            }
            ctx.refreshStartTime();
            ctx.setThreadLocalInfo();
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
