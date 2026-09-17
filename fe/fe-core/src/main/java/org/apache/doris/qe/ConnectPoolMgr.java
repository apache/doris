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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectContext.ThreadInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The one pool of every connection this frontend serves, whatever protocol it speaks: MySQL
 * connections and Arrow Flight SQL sessions share {@code qe_max_connection}, the per-user
 * {@code max_user_connections}, the processlist, KILL, the timeout checker and the connection
 * metrics. Arrow Flight SQL sessions additionally count against their own sub-quota
 * ({@code arrow_flight_max_connections}, which follows the pool's limit unless set) and are indexed
 * by their peer identity, the bearer token, since that is how Flight requests name their session.
 *
 * <p>{@link #unregisterConnection} is where every teardown path of a connection meets - a MySQL
 * channel closing, a Flight bearer token expiring or being evicted, CloseSession, KILL, the
 * timeout checker - so it is where the protocol releases what it still holds for the session.
 */
public class ConnectPoolMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectPoolMgr.class);
    protected final int maxConnections;
    private final int flightMaxConnections;
    protected final AtomicInteger numberConnection;
    private final AtomicInteger numberFlightConnection = new AtomicInteger(0);
    protected final Map<Integer, ConnectContext> connectionMap = Maps.newConcurrentMap();
    protected final Map<String, AtomicInteger> connByUser = Maps.newConcurrentMap();
    // Arrow Flight SQL: peer identity (bearer token) -> connection id
    private final Map<String, Integer> peerIdentity2ConnectionId = Maps.newConcurrentMap();

    // valid trace id -> query id
    protected final Map<String, TUniqueId> traceId2QueryId = Maps.newConcurrentMap();

    public ConnectPoolMgr(int maxConnections) {
        this(maxConnections, -1);
    }

    /**
     * @param maxConnections       the pool's limit, {@code qe_max_connection}
     * @param flightMaxConnections the Arrow Flight SQL sub-quota, {@code arrow_flight_max_connections};
     *                             negative follows {@code maxConnections}
     */
    public ConnectPoolMgr(int maxConnections, int flightMaxConnections) {
        this.maxConnections = maxConnections;
        this.flightMaxConnections = effectiveFlightMaxConnections(maxConnections, flightMaxConnections);
        numberConnection = new AtomicInteger(0);
    }

    /**
     * The Arrow Flight SQL sub-quota as enforced: a negative setting follows the pool's limit, and an
     * explicit one can never exceed it, since every Flight session is a connection of the pool too.
     */
    public static int effectiveFlightMaxConnections(int maxConnections, int flightMaxConnections) {
        return flightMaxConnections < 0 ? maxConnections : Math.min(maxConnections, flightMaxConnections);
    }

    private static boolean isFlight(ConnectContext ctx) {
        return ctx.getConnectType() == ConnectType.ARROW_FLIGHT_SQL;
    }

    public void timeoutChecker(long now) {
        for (ConnectContext connectContext : connectionMap.values()) {
            try {
                connectContext.checkTimeout(now);
            } catch (Throwable t) {
                LOG.warn("failed to check timeout for connection, connectionId: {}, user: {}",
                        connectContext.getConnectionId(), connectContext.getQualifiedUser(), t);
            }
        }
    }

    // Register one connection with its connection id.
    // Return -1 means register OK
    // Return >=0 means register failed, and return value is current connection num.
    public int registerConnection(ConnectContext ctx) {
        boolean flight = isFlight(ctx);
        if (numberConnection.incrementAndGet() > maxConnections) {
            numberConnection.decrementAndGet();
            return numberConnection.get();
        }
        if (flight && numberFlightConnection.incrementAndGet() > flightMaxConnections) {
            numberFlightConnection.decrementAndGet();
            numberConnection.decrementAndGet();
            return numberConnection.get();
        }
        // Check user
        connByUser.putIfAbsent(ctx.getQualifiedUser(), new AtomicInteger(0));
        AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
        if (conns.incrementAndGet() > ctx.getEnv().getAuth().getMaxConn(ctx.getQualifiedUser())) {
            conns.decrementAndGet();
            if (flight) {
                numberFlightConnection.decrementAndGet();
            }
            numberConnection.decrementAndGet();
            return numberConnection.get();
        }
        connectionMap.put(ctx.getConnectionId(), ctx);
        if (flight) {
            peerIdentity2ConnectionId.put(ctx.getPeerIdentity(), ctx.getConnectionId());
        }
        return -1;
    }

    /**
     * The refusal a client is told when {@link #registerConnection} returned a count instead of -1:
     * the same sentence for every protocol, naming the limits a connection is held to. The Flight
     * sub-quota is named only when it is tighter than the pool's limit, since only then can it be
     * the one that was reached.
     */
    public String limitReachedMessage(ConnectContext ctx, int current) {
        long userLimit = ctx.getEnv().getAuth().getMaxConn(ctx.getQualifiedUser());
        String message = String.format("Reach limit of connections. Total: %d, User: %d, Current: %d",
                maxConnections, userLimit, current);
        if (isFlight(ctx) && flightMaxConnections < maxConnections) {
            message += String.format(", Arrow Flight SQL: %d", flightMaxConnections);
        }
        return message;
    }

    public void unregisterConnection(ConnectContext ctx) {
        // First, before any bookkeeping that could fail: what the protocol holds for the session -
        // for Arrow Flight SQL the channel-cached results and the deferred query coordinators
        // (see FlightProtocolAdapter.tearDown) - is released whether or not the connection is
        // still in the pool, so an abandoned session is cleaned up rather than leaked.
        ctx.releaseProtocolSession();
        ctx.closeTxn();
        if (connectionMap.remove(ctx.getConnectionId()) != null) {
            AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
            if (ctx.traceId() != null) {
                traceId2QueryId.remove(ctx.traceId());
            }
            if (isFlight(ctx)) {
                peerIdentity2ConnectionId.remove(ctx.getPeerIdentity());
                numberFlightConnection.decrementAndGet();
            }
            numberConnection.decrementAndGet();
        }
    }

    public ConnectContext getContext(int connectionId) {
        return connectionMap.get(connectionId);
    }

    /** The Arrow Flight SQL session registered under this peer identity (bearer token), or null. */
    public ConnectContext getContextWithPeerIdentity(String peerIdentity) {
        Integer connectionId = peerIdentity2ConnectionId.get(peerIdentity);
        return connectionId == null ? null : getContext(connectionId);
    }

    public ConnectContext getContextWithQueryId(String queryId) {
        for (ConnectContext context : connectionMap.values()) {
            if (queryId.equals(DebugUtil.printId(context.queryId)) || queryId.equals(context.traceId())) {
                return context;
            }
        }
        return null;
    }

    public boolean cancelQuery(String queryId, Status cancelReason) {
        for (ConnectContext ctx : connectionMap.values()) {
            TUniqueId qid = ctx.queryId();
            if (qid != null && DebugUtil.printId(qid).equals(queryId)) {
                ctx.cancelQuery(cancelReason);
                return true;
            }
        }
        return false;
    }

    public int getConnectionNum() {
        return numberConnection.get();
    }

    /** How many of the pool's connections are Arrow Flight SQL sessions. */
    public int getFlightConnectionNum() {
        return numberFlightConnection.get();
    }

    public List<ThreadInfo> listConnection(String user, boolean isFull) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();
        for (ConnectContext ctx : connectionMap.values()) {
            // Check auth
            if (!ctx.getQualifiedUser().equals(user) && !Env.getCurrentEnv().getAccessManager()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                continue;
            }

            infos.add(ctx.toThreadInfo(isFull));
        }
        return infos;
    }

    // used for thrift
    public List<List<String>> listConnectionForRpc(UserIdentity userIdentity, boolean isShowFullSql,
            Optional<String> timeZone) {
        List<List<String>> list = new ArrayList<>();
        long nowMs = System.currentTimeMillis();
        for (ConnectContext ctx : connectionMap.values()) {
            // Check auth
            if (!ctx.getCurrentUserIdentity().equals(userIdentity) && !Env.getCurrentEnv().getAccessManager()
                    .checkGlobalPriv(userIdentity, PrivPredicate.ADMIN)) {
                continue;
            }
            list.add(ctx.toThreadInfo(isShowFullSql).toRow(-1, nowMs, timeZone));
        }
        return list;
    }

    public void putTraceId2QueryId(String traceId, TUniqueId queryId) {
        traceId2QueryId.put(traceId, queryId);
    }

    public String getQueryIdByTraceId(String traceId) {
        TUniqueId queryId = traceId2QueryId.get(traceId);
        return queryId == null ? "" : DebugUtil.printId(queryId);
    }

    public void removeTraceId(String traceId) {
        if (traceId != null) {
            traceId2QueryId.remove(traceId);
        }
    }

    public Map<Integer, ConnectContext> getConnectionMap() {
        return connectionMap;
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        return connByUser;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    /** The Arrow Flight SQL sub-quota as enforced (see {@link #effectiveFlightMaxConnections}). */
    public int getFlightMaxConnections() {
        return flightMaxConnections;
    }
}
