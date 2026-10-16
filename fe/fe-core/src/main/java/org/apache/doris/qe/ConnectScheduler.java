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
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.qe.ConnectContext.ThreadInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// The scheduler of query requests
// Now the strategy is simple, we allocate a thread for it when a request comes.
// TODO(zhaochun): We should consider if the number of local file connection can >= maximum connections later.
public class ConnectScheduler {
    private static final Logger LOG = LogManager.getLogger(ConnectScheduler.class);
    private final AtomicInteger nextConnectionId;
    // Every connection of every protocol: see ConnectPoolMgr.
    private final ConnectPoolMgr connectPoolMgr;

    // Use a thread to check whether connection is timeout. Because
    // 1. If use a scheduler, the task maybe a huge number when query is messy.
    //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
    // 2. Use a thread to poll maybe lose some accurate, but is enough to us.
    private final ScheduledExecutorService checkTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "connect-scheduler-check-timer", true);

    /**
     * @param maxConnections       the pool's limit, {@code qe_max_connection}
     * @param flightMaxConnections the Arrow Flight SQL sub-quota, {@code arrow_flight_max_connections};
     *                             negative is half of {@code maxConnections}
     */
    public ConnectScheduler(int maxConnections, int flightMaxConnections) {
        nextConnectionId = new AtomicInteger(0);
        this.connectPoolMgr = new ConnectPoolMgr(maxConnections, flightMaxConnections);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    public ConnectScheduler(int maxConnections) {
        this(maxConnections, Config.arrow_flight_max_connections);
    }

    public ConnectPoolMgr getConnectPoolMgr() {
        return connectPoolMgr;
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

    public ConnectContext getContext(int connectionId) {
        return connectPoolMgr.getContext(connectionId);
    }

    /** The Arrow Flight SQL session registered under this peer identity (bearer token), or null. */
    public ConnectContext getContextWithPeerIdentity(String peerIdentity) {
        return connectPoolMgr.getContextWithPeerIdentity(peerIdentity);
    }

    public ConnectContext getContextWithQueryId(String queryId) {
        return connectPoolMgr.getContextWithQueryId(queryId);
    }

    public boolean cancelQuery(String queryId, Status cancelReason) {
        return connectPoolMgr.cancelQuery(queryId, cancelReason);
    }

    public int getConnectionNum() {
        return connectPoolMgr.getConnectionNum();
    }

    public List<ThreadInfo> listConnection(String user, boolean isFull) {
        return connectPoolMgr.listConnection(user, isFull);
    }

    // used for thrift
    public List<List<String>> listConnectionForRpc(UserIdentity userIdentity, boolean isShowFullSql,
            Optional<String> timeZone) {
        return connectPoolMgr.listConnectionForRpc(userIdentity, isShowFullSql, timeZone);
    }

    public String getQueryIdByTraceId(String traceId) {
        return connectPoolMgr.getQueryIdByTraceId(traceId);
    }

    public void removeOldTraceId(String traceId) {
        connectPoolMgr.removeTraceId(traceId);
    }

    public Map<Integer, ConnectContext> getConnectionMap() {
        return connectPoolMgr.getConnectionMap();
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        return connectPoolMgr.getUserConnectionMap();
    }

    private class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            try {
                connectPoolMgr.timeoutChecker(System.currentTimeMillis());
            } catch (Throwable t) {
                LOG.warn("failed to check connection timeout", t);
            }
        }
    }
}
