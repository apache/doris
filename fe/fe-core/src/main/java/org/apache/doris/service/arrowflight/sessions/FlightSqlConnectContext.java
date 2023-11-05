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

import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;
import org.apache.doris.thrift.TResultSinkType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class FlightSqlConnectContext extends ConnectContext {
    private static final Logger LOG = LogManager.getLogger(FlightSqlConnectContext.class);

    public FlightSqlConnectContext(String peerIdentity) {
        this.connectType = ConnectType.ARROW_FLIGHT_SQL;
        this.peerIdentity = peerIdentity;
        mysqlChannel = null; // Use of MysqlChannel is not expected
        flightSqlChannel = new FlightSqlChannel();
        setResultSinkType(TResultSinkType.ARROW_FLIGHT_PROTOCAL);
        init();
    }

    @Override
    public FlightSqlChannel getFlightSqlChannel() {
        return flightSqlChannel;
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
        if (flightSqlChannel != null) {
            flightSqlChannel.close();
        }
    }

    // kill operation with no protect.
    @Override
    public void kill(boolean killConnection) {
        LOG.warn("kill query from {}, kill flight sql connection: {}", getRemoteHostPortString(), killConnection);

        if (killConnection) {
            isKilled = true;
            closeChannel();
            connectScheduler.unregisterConnection(this);
        }
        // Now, cancel running query.
        cancelQuery();
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
