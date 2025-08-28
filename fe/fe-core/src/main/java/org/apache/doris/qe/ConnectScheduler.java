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
import org.apache.doris.service.arrowflight.sessions.FlightSqlConnectPoolMgr;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
    private final ConnectPoolMgr connectPoolMgr;
    private final FlightSqlConnectPoolMgr flightSqlConnectPoolMgr;

    // Use a thread to check whether connection is timeout. Because
    // 1. If use a scheduler, the task maybe a huge number when query is messy.
    //    Let timeout is 10m, and 5000 qps, then there are up to 3000000 tasks in scheduler.
    // 2. Use a thread to poll maybe lose some accurate, but is enough to us.
    private final ScheduledExecutorService checkTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "connect-scheduler-check-timer", true);

    public ConnectScheduler(int commonMaxConnections, int flightSqlMaxConnections) {
        nextConnectionId = new AtomicInteger(0);
        this.connectPoolMgr = new ConnectPoolMgr(commonMaxConnections);
        this.flightSqlConnectPoolMgr = new FlightSqlConnectPoolMgr(flightSqlMaxConnections);
        checkTimer.scheduleAtFixedRate(new TimeoutChecker(), 0, 1000L, TimeUnit.MILLISECONDS);
    }

    public ConnectScheduler(int commonMaxConnections) {
        this(commonMaxConnections, Config.arrow_flight_max_connections);
    }

    public ConnectPoolMgr getConnectPoolMgr() {
        return connectPoolMgr;
    }

    public FlightSqlConnectPoolMgr getFlightSqlConnectPoolMgr() {
        return flightSqlConnectPoolMgr;
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
        ConnectContext ctx = connectPoolMgr.getContext(connectionId);
        if (ctx == null) {
            ctx = flightSqlConnectPoolMgr.getContext(connectionId);
        }
        return ctx;
    }

    public ConnectContext getContextWithQueryId(String queryId) {
        ConnectContext ctx = connectPoolMgr.getContextWithQueryId(queryId);
        if (ctx == null) {
            ctx = flightSqlConnectPoolMgr.getContextWithQueryId(queryId);
        }
        return ctx;
    }

    public boolean cancelQuery(String queryId, Status cancelReason) {
        boolean ret = connectPoolMgr.cancelQuery(queryId, cancelReason);
        if (!ret) {
            ret = flightSqlConnectPoolMgr.cancelQuery(queryId, cancelReason);
        }
        return ret;
    }

    public int getConnectionNum() {
        return connectPoolMgr.getConnectionNum() + flightSqlConnectPoolMgr.getConnectionNum();
    }

    public List<ThreadInfo> listConnection(String user, boolean isFull) {
        List<ConnectContext.ThreadInfo> infos = Lists.newArrayList();
        infos.addAll(connectPoolMgr.listConnection(user, isFull));
        infos.addAll(flightSqlConnectPoolMgr.listConnection(user, isFull));
        return infos;
    }

    // used for thrift
    public List<List<String>> listConnectionForRpc(UserIdentity userIdentity, boolean isShowFullSql,
            Optional<String> timeZone) {
        List<List<String>> list = new ArrayList<>();
        list.addAll(connectPoolMgr.listConnectionForRpc(userIdentity, isShowFullSql, timeZone));
        list.addAll(flightSqlConnectPoolMgr.listConnectionForRpc(userIdentity, isShowFullSql, timeZone));
        return list;
    }

    public String getQueryIdByTraceId(String traceId) {
        String queryId = connectPoolMgr.getQueryIdByTraceId(traceId);
        if (Strings.isNullOrEmpty(queryId)) {
            queryId = flightSqlConnectPoolMgr.getQueryIdByTraceId(traceId);
        }
        return queryId;
    }

    public void removeOldTraceId(String traceId) {
        connectPoolMgr.removeTraceId(traceId);
        flightSqlConnectPoolMgr.removeTraceId(traceId);
    }

    public Map<Integer, ConnectContext> getConnectionMap() {
        Map<Integer, ConnectContext> map = Maps.newConcurrentMap();
        map.putAll(connectPoolMgr.getConnectionMap());
        map.putAll(flightSqlConnectPoolMgr.getConnectionMap());
        return map;
    }

    public Map<String, AtomicInteger> getUserConnectionMap() {
        Map<String, AtomicInteger> map = Maps.newConcurrentMap();
        map.putAll(connectPoolMgr.getUserConnectionMap());
        map.putAll(flightSqlConnectPoolMgr.getUserConnectionMap());
        return map;
    }

    private class TimeoutChecker extends TimerTask {
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            connectPoolMgr.timeoutChecker(now);
            flightSqlConnectPoolMgr.timeoutChecker(now);
        }
    }
}
