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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// The scheduler of query requests
// Now the strategy is simple, we allocate a thread for it when a request comes.
// TODO(zhaochun): We should consider if the number of local file connection can >= maximum connections later.
public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private final int maxConnections;
    private final AtomicInteger numberConnection;
    private final AtomicInteger nextConnectionId;
    private final Map<Integer, ConnectContext> connectionMap = Maps.newConcurrentMap();
    private final Map<String, AtomicInteger> connByUser = Maps.newConcurrentMap();
    private final Map<String, Integer> flightToken2ConnectionId = Maps.newConcurrentMap();

    // valid trace id -> query id
    private final Map<String, TUniqueId> traceId2QueryId = Maps.newConcurrentMap();

    // Use a thread to check whether connection is timeout. Because
    // 1. If use a scheduler, the task maybe a huge number when query is messy.
    //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
    // 2. Use a thread to poll maybe lose some accurate, but is enough to us.
    private final ScheduledExecutorService checkTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "connect-scheduler-check-timer", true);

    public ConnectScheduler(int maxConnections) {
        this.maxConnections = maxConnections;
        numberConnection = new AtomicInteger(0);
        nextConnectionId = new AtomicInteger(0);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    private class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            for (ConnectContext connectContext : connectionMap.values()) {
                connectContext.checkTimeout(now);
            }
        }
    }

    // submit one MysqlContext to this scheduler.
    // return true, if this connection has been successfully submitted, otherwise return false.
    // Caller should close ConnectContext if return false.
    public boolean submit(ConnectContext context) {
        if (context == null) {
            return false;
        }
        context.setConnectionId(nextConnectionId.getAndAdd(1));
        context.resetLoginTime();
        return true;
    }

    // Register one connection with its connection id.
    public boolean registerConnection(ConnectContext ctx) {
        if (numberConnection.incrementAndGet() > maxConnections) {
            numberConnection.decrementAndGet();
            return false;
        }
        // Check user
        connByUser.putIfAbsent(ctx.getQualifiedUser(), new AtomicInteger(0));
        AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
        if (conns.incrementAndGet() > ctx.getEnv().getAuth().getMaxConn(ctx.getQualifiedUser())) {
            conns.decrementAndGet();
            numberConnection.decrementAndGet();
            return false;
        }
        connectionMap.put(ctx.getConnectionId(), ctx);
        if (ctx.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
            flightToken2ConnectionId.put(ctx.getPeerIdentity(), ctx.getConnectionId());
        }
        return true;
    }

    public void unregisterConnection(ConnectContext ctx) {
        ctx.closeTxn();
        if (connectionMap.remove(ctx.getConnectionId()) != null) {
            AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
            numberConnection.decrementAndGet();
            if (ctx.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
                flightToken2ConnectionId.remove(ctx.getPeerIdentity());
            }
        }
    }

    public ConnectContext getContext(int connectionId) {
        return connectionMap.get(connectionId);
    }

    public ConnectContext getContextWithQueryId(String queryId) {
        for (ConnectContext context : connectionMap.values()) {
            if (queryId.equals(DebugUtil.printId(context.queryId))) {
                return context;
            }
        }
        return null;
    }

    public ConnectContext getContext(String flightToken) {
        if (flightToken2ConnectionId.containsKey(flightToken)) {
            int connectionId = flightToken2ConnectionId.get(flightToken);
            return getContext(connectionId);
        }
        return null;
    }

    public void cancelQuery(String queryId) {
        for (ConnectContext ctx : connectionMap.values()) {
            TUniqueId qid = ctx.queryId();
            if (qid != null && DebugUtil.printId(qid).equals(queryId)) {
                ctx.cancelQuery();
                break;
            }
        }
    }

    public int getConnectionNum() {
        return numberConnection.get();
    }

    public List<ConnectContext.ThreadInfo> listConnection(String user, boolean isFull) {
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
    public List<List<String>> listConnectionForRpc(UserIdentity userIdentity, boolean isShowFullSql) {
        List<List<String>> list = new ArrayList<>();
        long nowMs = System.currentTimeMillis();
        for (ConnectContext ctx : connectionMap.values()) {
            // Check auth
            if (!ctx.getCurrentUserIdentity().equals(userIdentity) && !Env.getCurrentEnv()
                    .getAccessManager()
                    .checkGlobalPriv(userIdentity, PrivPredicate.GRANT)) {
                continue;
            }
            list.add(ctx.toThreadInfo(isShowFullSql).toRow(-1, nowMs));
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

    public Map<Integer, ConnectContext> getConnectionMap() {
        return connectionMap;
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        return connByUser;
    }
}
