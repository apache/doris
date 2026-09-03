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

package org.apache.doris.service.arrowflight.sessions;

import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class FlightSqlConnectContext extends ConnectContext {
    private static final Logger LOG = LogManager.getLogger(FlightSqlConnectContext.class);
    protected volatile FlightSqlChannel flightSqlChannel;

    public FlightSqlConnectContext(String peerIdentity) {
        this.connectType = ConnectType.ARROW_FLIGHT_SQL;
        this.peerIdentity = peerIdentity;
        mysqlChannel = null; // Use of MysqlChannel is not expected
        flightSqlChannel = new FlightSqlChannel();
        setResultSinkType(TResultSinkType.ARROW_FLIGHT_PROTOCOL);
        init();
    }

    @Override
    public FlightSqlChannel getFlightSqlChannel() {
        return flightSqlChannel;
    }

    /**
     * Flight sessions get their own idle bound (Config.arrow_flight_session_idle_timeout_second),
     * tighter than the MySQL wait_timeout: an abandoned Flight session keeps its last query's
     * coordinator — and that query's workload-group queue slot — alive until it is closed, so a
     * client that opens a session per query and never closes it would otherwise pin a slot for
     * the whole wait_timeout (8h by default).
     *
     * A Flight session is COM_SLEEP while the client drains the result from the BE (DoGet), and
     * its idle clock runs from the query's START — so the bound is floored at the query's own
     * execution timeout: a long result drain is never killed before query_timeout would kill the
     * query itself. Effective bound = min(wait_timeout, max(config, exec timeout)); 0 disables it.
     */
    @Override
    public long getIdleTimeoutS() {
        long waitTimeoutS = super.getIdleTimeoutS();
        int flightIdleS = Config.arrow_flight_session_idle_timeout_second;
        if (flightIdleS <= 0) {
            return waitTimeoutS;
        }
        return Math.min(waitTimeoutS, Math.max((long) flightIdleS, (long) getExecTimeoutS()));
    }

    @Override
    public MysqlChannel getMysqlChannel() {
        throw new RuntimeException("getMysqlChannel not in mysql connection");
    }

    @Override
    public String getClientIP() {
        return flightSqlChannel.getRemoteHostPortString();
    }

    @Override
    protected void closeChannel() {
        connectScheduler.getFlightSqlConnectPoolMgr().unregisterConnection(this);
    }

    // kill operation with no protect.
    @Override
    public void kill(boolean killConnection) {
        LOG.warn("kill query from {}, kill flight sql connection: {}", getRemoteHostPortString(), killConnection);

        if (killConnection) {
            killConnection();
        }
        // Now, cancel running query.
        cancelQuery(new Status(TStatusCode.CANCELLED, "arrow flight query killed by user"));
    }

    @Override
    public void setQueryId(TUniqueId queryId) {
        if (this.queryId != null) {
            this.lastQueryId = this.queryId.deepCopy();
        }
        this.queryId = queryId;
        if (connectScheduler != null && !Strings.isNullOrEmpty(traceId)) {
            connectScheduler.getFlightSqlConnectPoolMgr().putTraceId2QueryId(traceId, queryId);
        }
    }

    @Override
    public String getRemoteHostPortString() {
        return getFlightSqlChannel().getRemoteHostPortString();
    }

    @Override
    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        throw new RuntimeException("Flight Sql Not impl startAcceptQuery");
    }

    @Override
    public void suspendAcceptQuery() {
        throw new RuntimeException("Flight Sql Not impl suspendAcceptQuery");
    }

    @Override
    public void resumeAcceptQuery() {
        throw new RuntimeException("Flight Sql Not impl resumeAcceptQuery");
    }

    @Override
    public void stopAcceptQuery() throws IOException {
        throw new RuntimeException("Flight Sql Not impl stopAcceptQuery");
    }
}
