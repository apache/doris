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

public class ConnectPoolMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectPoolMgr.class);
    protected final int maxConnections;
    protected final AtomicInteger numberConnection;
    protected final Map<Integer, ConnectContext> connectionMap = Maps.newConcurrentMap();
    protected final Map<String, AtomicInteger> connByUser = Maps.newConcurrentMap();

    // valid trace id -> query id
    protected final Map<String, TUniqueId> traceId2QueryId = Maps.newConcurrentMap();

    public ConnectPoolMgr(int maxConnections) {
        this.maxConnections = maxConnections;
        numberConnection = new AtomicInteger(0);
    }

    public void timeoutChecker(long now) {
        for (ConnectContext connectContext : connectionMap.values()) {
            connectContext.checkTimeout(now);
        }
    }

    // Register one connection with its connection id.
    // Return -1 means register OK
    // Return >=0 means register failed, and return value is current connection num.
    public int registerConnection(ConnectContext ctx) {
        if (numberConnection.incrementAndGet() > maxConnections) {
            numberConnection.decrementAndGet();
            return numberConnection.get();
        }
        // Check user
        connByUser.putIfAbsent(ctx.getQualifiedUser(), new AtomicInteger(0));
        AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
        if (conns.incrementAndGet() > ctx.getEnv().getAuth().getMaxConn(ctx.getQualifiedUser())) {
            conns.decrementAndGet();
            numberConnection.decrementAndGet();
            return numberConnection.get();
        }
        connectionMap.put(ctx.getConnectionId(), ctx);
        return -1;
    }

    public void unregisterConnection(ConnectContext ctx) {
        ctx.closeTxn();
        if (connectionMap.remove(ctx.getConnectionId()) != null) {
            AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
            if (ctx.traceId() != null) {
                traceId2QueryId.remove(ctx.traceId());
            }
            numberConnection.decrementAndGet();
        }
    }

    public ConnectContext getContext(int connectionId) {
        return connectionMap.get(connectionId);
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
                    .checkGlobalPriv(userIdentity, PrivPredicate.GRANT)) {
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
}
