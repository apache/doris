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

import org.apache.doris.common.util.TokenMasker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.service.arrowflight.results.FlightSqlChannel;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class FlightSqlConnectPoolMgr extends ConnectPoolMgr {
    private static final Logger LOG = LogManager.getLogger(
            FlightSqlConnectPoolMgr.class);
    private final Map<String, Integer> flightToken2ConnectionId = Maps.newConcurrentMap();

    public FlightSqlConnectPoolMgr(int maxConnections) {
        super(maxConnections);
    }

    // Register one connection with its connection id.
    // Return -1 means register OK
    // Return >=0 means register failed, and return value is current connection num.
    @Override
    public int registerConnection(ConnectContext ctx) {
        if (numberConnection.incrementAndGet() > maxConnections) {
            numberConnection.decrementAndGet();
            return numberConnection.get();
        }
        // not check user
        connectionMap.put(ctx.getConnectionId(), ctx);
        if (ctx.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
            flightToken2ConnectionId.put(ctx.getPeerIdentity(), ctx.getConnectionId());
        }
        return -1;
    }

    @Override
    public void unregisterConnection(ConnectContext ctx) {
        // All Flight SQL session teardown paths (idle/query timeout, bearer token expiry, and
        // explicit CloseSession) reach here. Release channel-cached Arrow results before removing
        // the context from the pool.
        FlightSqlChannel flightSqlChannel = ctx.getFlightSqlChannel();
        if (flightSqlChannel != null) {
            try {
                flightSqlChannel.close();
            } catch (Throwable t) {
                // RootAllocator.close() marks the allocator closed before it reports outstanding
                // bytes. The error is actionable, but session teardown must still release the
                // coordinator, transaction and pool/token bookkeeping below.
                // For an Arrow Flight SQL connection the peer identity IS the bearer token, so it is
                // logged as a masked id, the same one FlightTokenManagerImpl uses.
                LOG.warn("failed to close Flight SQL channel while unregistering connection {}, peer identity {}",
                        ctx.getConnectionId(), TokenMasker.tokenId(ctx.getPeerIdentity()), t);
            }
        }
        // Finalize any Arrow Flight query whose coordinator was kept alive across the
        // GetFlightInfo -> DoGet phases (see #62259), releasing its resources (e.g. external-table
        // batch SplitSources and the query queue slot).
        ctx.sealAndCloseFlightSqlDeferredExecutors();
        ctx.closeTxn();
        if (connectionMap.remove(ctx.getConnectionId()) != null) {
            numberConnection.decrementAndGet();
            if (ctx.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL)) {
                flightToken2ConnectionId.remove(ctx.getPeerIdentity());
            }
        }
    }

    public ConnectContext getContextWithFlightToken(String flightToken) {
        if (flightToken2ConnectionId.containsKey(flightToken)) {
            int connectionId = flightToken2ConnectionId.get(flightToken);
            return getContext(connectionId);
        }
        return null;
    }
}
