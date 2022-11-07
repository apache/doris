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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.nio.NConnectContext;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
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
    private final ExecutorService executor = ThreadPoolManager.newDaemonCacheThreadPool(
            Config.max_connection_scheduler_threads_num, "connect-scheduler-pool", true);

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
        // no necessary for nio.
        if (context instanceof NConnectContext) {
            return true;
        }
        executor.submit(new LoopHandler(context));
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
        }
    }

    public ConnectContext getContext(int connectionId) {
        return connectionMap.get(connectionId);
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
            if (!ctx.getQualifiedUser().equals(user) && !Env.getCurrentEnv().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                continue;
            }

            infos.add(ctx.toThreadInfo(isFull));
        }
        return infos;
    }

    public void putTraceId2QueryId(String traceId, TUniqueId queryId) {
        traceId2QueryId.put(traceId, queryId);
    }

    public String getQueryIdByTraceId(String traceId) {
        TUniqueId queryId = traceId2QueryId.get(traceId);
        return queryId == null ? "" : DebugUtil.printId(queryId);
    }

    private class LoopHandler implements Runnable {
        ConnectContext context;

        LoopHandler(ConnectContext context) {
            this.context = context;
        }

        @Override
        public void run() {
            try {
                // Set thread local info
                context.setThreadLocalInfo();
                context.setConnectScheduler(ConnectScheduler.this);
                // authenticate check failed.
                if (!MysqlProto.negotiate(context)) {
                    return;
                }

                if (registerConnection(context)) {
                    MysqlProto.sendResponsePacket(context);
                } else {
                    context.getState().setError(ErrorCode.ERR_USER_LIMIT_REACHED, "Reach limit of connections");
                    MysqlProto.sendResponsePacket(context);
                    return;
                }

                context.setUserQueryTimeout(context.getEnv().getAuth().getQueryTimeout(context.getQualifiedUser()));
                context.setStartTime();
                ConnectProcessor processor = new ConnectProcessor(context);
                processor.loop();
            } catch (Exception e) {
                // for unauthorized access such lvs probe request, may cause exception, just log it in debug level
                if (context.getCurrentUserIdentity() != null) {
                    LOG.warn("connect processor exception because ", e);
                } else {
                    LOG.debug("connect processor exception because ", e);
                }
            } finally {
                unregisterConnection(context);
                context.cleanup();
            }
        }
    }
}
