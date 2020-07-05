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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.nio.NConnectContext;
import org.apache.doris.mysql.privilege.PrivPredicate;

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

// 查询请求的调度器
// 当前的策略比较简单，有请求过来，就为其单独申请一个线程进行服务。
// TODO(zhaochun): 应当后面考虑本地文件的连接是否可以超过最大连接数
public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private int maxConnections;
    private int numberConnection;
    private AtomicInteger nextConnectionId;
    private Map<Long, ConnectContext> connectionMap = Maps.newHashMap();
    private Map<String, AtomicInteger> connByUser = Maps.newHashMap();
    private ExecutorService executor = ThreadPoolManager.newDaemonCacheThreadPool(Config.max_connection_scheduler_threads_num, "connect-scheduler-pool");

    // Use a thread to check whether connection is timeout. Because
    // 1. If use a scheduler, the task maybe a huge number when query is messy.
    //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
    // 2. Use a thread to poll maybe lose some accurate, but is enough to us.
    private ScheduledExecutorService checkTimer = ThreadPoolManager.newScheduledThreadPool(1,
            "Connect-Scheduler-Check-Timer");

    public ConnectScheduler(int maxConnections) {
        this.maxConnections = maxConnections;
        numberConnection = 0;
        nextConnectionId = new AtomicInteger(0);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    private class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            synchronized (ConnectScheduler.this) {
                for (ConnectContext connectContext : connectionMap.values()) {
                    connectContext.checkTimeout(now);
                }
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
        if(context instanceof NConnectContext){
            return true;
        }
        if (executor.submit(new LoopHandler(context)) == null) {
            LOG.warn("Submit one thread failed.");
            return false;
        }
        return true;
    }

    // Register one connection with its connection id.
    public synchronized boolean registerConnection(ConnectContext ctx) {
        if (numberConnection >= maxConnections) {
            return false;
        }
        // Check user
        if (connByUser.get(ctx.getQualifiedUser()) == null) {
            connByUser.put(ctx.getQualifiedUser(), new AtomicInteger(0));
        }
        int conns = connByUser.get(ctx.getQualifiedUser()).get();
        if (conns >= ctx.getCatalog().getAuth().getMaxConn(ctx.getQualifiedUser())) {
            return false;
        }
        numberConnection++;
        connByUser.get(ctx.getQualifiedUser()).incrementAndGet();
        connectionMap.put((long) ctx.getConnectionId(), ctx);
        return true;
    }

    public synchronized void unregisterConnection(ConnectContext ctx) {
        if (connectionMap.remove((long) ctx.getConnectionId()) != null) {
            numberConnection--;
            AtomicInteger conns = connByUser.get(ctx.getQualifiedUser());
            if (conns != null) {
                conns.decrementAndGet();
            }
        }
    }

    public synchronized ConnectContext getContext(long connectionId) {
        return connectionMap.get(connectionId);
    }

    public synchronized int getConnectionNum() {
        return numberConnection;
    }

    public synchronized List<ConnectContext.ThreadInfo> listConnection(String user) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();

        for (ConnectContext ctx : connectionMap.values()) {
            // Check auth
            if (!ctx.getQualifiedUser().equals(user) &&
                    !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                                                                           PrivPredicate.GRANT)) {
                continue;
            }
            
            infos.add(ctx.toThreadInfo());
        }
        return infos;
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
                    context.getState().setError("Reach limit of connections");
                    MysqlProto.sendResponsePacket(context);
                    return;
                }

                context.setStartTime();
                ConnectProcessor processor = new ConnectProcessor(context);
                processor.loop();
            } catch (Exception e) {
                // for unauthrorized access such lvs probe request, may cause exception, just log it in debug level
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
