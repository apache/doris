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

package org.apache.doris.qe.protocol;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TResultSinkType;

import java.io.IOException;

/**
 * The wire-protocol half of a connection.
 *
 * <p>A {@link ConnectContext} is the session: user, catalog and database, session variables,
 * transaction, prepared statements, the statement being executed. Everything only one wire
 * protocol knows about lives behind this interface instead: the MySQL channel and the negotiated
 * capabilities, or the Arrow Flight SQL result cache and endpoints. A context is bound to exactly
 * one adapter when it is created and keeps it for its whole life.
 *
 * <p>Implementations: {@code MysqlProtocolAdapter} (a MySQL client, a proxy context replaying a
 * forwarded statement on the master, and an internal context whose channel discards everything)
 * and {@code FlightProtocolAdapter} (an Arrow Flight SQL session).
 */
public interface ProtocolAdapter {

    ConnectType type();

    /**
     * The client address, as shown in the Host column of SHOW PROCESSLIST and as client_ip in the
     * audit log.
     */
    String remoteHostPortString(ConnectContext ctx);

    /** The result sink a backend must use for a query on this connection. */
    TResultSinkType resultSinkType();

    /** How a statement's result reaches the client of {@code ctx}'s connection. */
    ResultSender resultSender(ConnectContext ctx);

    /**
     * Whether a hit in the SQL cache can be answered by replaying the cached rows. The cache keeps
     * them in MySQL wire format, so only a MySQL connection can; a connection of any other protocol
     * re-executes the query and never populates the cache either.
     */
    boolean supportsSqlCacheReplay();

    /**
     * Whether a query forwarded to the master can be answered with the result the master sends
     * back. The master produces it as MySQL wire packets ({@code TMasterOpResult.queryResultBufList}),
     * so only a MySQL connection can replay it; a connection of any other protocol refuses to
     * forward a query instead of answering it with a synthesized empty success. Forwarded
     * statements whose result is a {@code ShowResultSet} or just a status are not affected.
     */
    boolean canReplayForwardedQueryResult();

    /**
     * Whether a query the planner can answer without a backend (a literal, a session variable)
     * may be answered by this frontend, through {@link ResultSender#sendResultSet}, instead of
     * being run on a backend.
     */
    boolean supportsFeSideResult();

    /**
     * Whether a point query on a merge-on-write unique table may take the short circuit:
     * {@code PointQueryExecutor} looks the row up on the backend over a plain rpc and encodes it
     * on this frontend, instead of running a query whose result the backend keeps in a result
     * sink of this connection's {@link #resultSinkType}.
     */
    boolean supportsShortCircuitPointQuery();

    /**
     * Whether the query being executed, which just failed, may be run again under a new query id
     * without the client noticing. Asked after each failed attempt of
     * {@code StmtExecutor.handleQueryWithRetry}.
     */
    boolean canRetryQuery(ConnectContext ctx);

    /**
     * Called by the connect processor before each statement of a request is executed. The
     * protocol drops what the previous statement of the same request left behind, so that a
     * request delivers only the outcome of its last statement.
     */
    void beforeStatement(ConnectContext ctx);

    /**
     * Called by the executor when the statement's plan is about to be run on the backends as a
     * query, before the coordinator is built. The protocol decides here where the query's result
     * goes: relayed by this frontend row by row, or left on the backends for the client to pull;
     * see {@link #returnsResultFromLocal}.
     */
    void beforeQuery(ConnectContext ctx);

    /**
     * Whether the result of the statement being executed comes from this frontend: materialized
     * by it, or relayed by it from the backends, and delivered through the {@link ResultSender}.
     * It does not when the backends keep the result for the client to pull; such a statement is
     * not over when its executor returns, and its query registration and coordinator are released
     * only once the client has pulled the result.
     */
    boolean returnsResultFromLocal(ConnectContext ctx);

    /**
     * Adds to the request that forwards a statement to the master what the master needs to
     * know about this connection's client to produce the response the client expects.
     */
    void fillForwardRequest(ConnectContext ctx, TMasterOpRequest request);

    /**
     * Called by {@code ConnectProcessor.executeQuery} after the {@code stmtIndex}-th of the
     * {@code stmtCount} statements of one request has been executed, before it is audited. The
     * protocol hands the statement's outcome to its client where the transport needs it (an
     * intermediate MySQL response, the outcome of a statement forwarded to the master) and
     * decides whether the request goes on to the next statement.
     *
     * @return false to stop the request here, with {@code ctx.getState()} set to the reason
     */
    boolean finishStatement(ConnectContext ctx, StmtExecutor executor, int stmtIndex, int stmtCount)
            throws IOException;

    /**
     * The pool this connection is registered in. Each protocol still keeps its own pool; this
     * goes away when they are merged.
     */
    ConnectPoolMgr connectPool(ConnectScheduler scheduler);

    /**
     * Called from {@link ConnectContext#clear()} once the response of a statement has been sent,
     * to drop the protocol state that only belonged to that statement.
     */
    void afterStatement(ConnectContext ctx);

    /** Tears down the transport side of the connection. Must be idempotent. */
    void closeConnection(ConnectContext ctx);
}
