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
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectSchedulerImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final ConnectSchedulerImpl commonConnectScheduler;
    private final FlightSqlConnectSchedulerImpl flightSqlConnectScheduler;

    // Use a thread to check whether connection is timeout. Because
    // 1. If use a scheduler, the task maybe a huge number when query is messy.
    //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
    // 2. Use a thread to poll maybe lose some accurate, but is enough to us.
    private final ScheduledExecutorService checkTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "connect-scheduler-check-timer", true);

    public ConnectScheduler(int commonMaxConnections, int flightSqlMaxConnections) {
        nextConnectionId = new AtomicInteger(0);
        this.commonConnectScheduler = new ConnectSchedulerImpl(commonMaxConnections);
        this.flightSqlConnectScheduler = new FlightSqlConnectSchedulerImpl(flightSqlMaxConnections);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    public ConnectScheduler(int commonMaxConnections) {
        this(commonMaxConnections, Config.arrow_flight_max_connections);
    }

    public ConnectSchedulerImpl getCommonConnectScheduler() {
        return commonConnectScheduler;
    }

    public FlightSqlConnectSchedulerImpl getFlightSqlConnectScheduler() {
        return flightSqlConnectScheduler;
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
        ConnectContext ctx = commonConnectScheduler.getContext(connectionId);
        if (ctx == null) {
            ctx = flightSqlConnectScheduler.getContext(connectionId);
        }
        return ctx;
    }

    public ConnectContext getContextWithQueryId(String queryId) {
        ConnectContext ctx = commonConnectScheduler.getContextWithQueryId(queryId);
        if (ctx == null) {
            ctx = flightSqlConnectScheduler.getContextWithQueryId(queryId);
        }
        return ctx;
    }

    public boolean cancelQuery(String queryId, Status cancelReason) {
        boolean ret = commonConnectScheduler.cancelQuery(queryId, cancelReason);
        if (!ret) {
            ret = flightSqlConnectScheduler.cancelQuery(queryId, cancelReason);
        }
        return ret;
    }

    public int getConnectionNum() {
        return commonConnectScheduler.getConnectionNum() + flightSqlConnectScheduler.getConnectionNum();
    }

    public List<ThreadInfo> listConnection(String user, boolean isFull) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();
        infos.addAll(commonConnectScheduler.listConnection(user, isFull));
        infos.addAll(flightSqlConnectScheduler.listConnection(user, isFull));
        return infos;
    }

    // used for thrift
    public List<List<String>> listConnectionForRpc(UserIdentity userIdentity, boolean isShowFullSql,
            Optional<String> timeZone) {
        List<List<String>> list = new ArrayList<>();
        list.addAll(commonConnectScheduler.listConnectionForRpc(userIdentity, isShowFullSql, timeZone));
        list.addAll(flightSqlConnectScheduler.listConnectionForRpc(userIdentity, isShowFullSql, timeZone));
        return list;
    }

    public String getQueryIdByTraceId(String traceId) {
        String queryId = commonConnectScheduler.getQueryIdByTraceId(traceId);
        if (Objects.equals(queryId, "")) {
            queryId = flightSqlConnectScheduler.getQueryIdByTraceId(traceId);
        }
        return queryId;
    }

    public Map<Integer, ConnectContext> getConnectionMap() {
        Map<Integer, ConnectContext> map = Maps.newConcurrentMap();
        map.putAll(commonConnectScheduler.getConnectionMap());
        map.putAll(flightSqlConnectScheduler.getConnectionMap());
        return map;
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        Map<String, AtomicInteger> map = Maps.newConcurrentMap();
        map.putAll(commonConnectScheduler.getUserConnectionMap());
        map.putAll(flightSqlConnectScheduler.getUserConnectionMap());
        return map;
    }

    private class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            commonConnectScheduler.timeoutChecker(now);
            flightSqlConnectScheduler.timeoutChecker(now);
        }
    }
}
