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

package org.apache.doris.arrowflight;

import org.apache.doris.arrowflight.auth2.FlightBearerTokenAuthenticator;
import org.apache.doris.arrowflight.auth2.FlightRemoteIpServerStreamTracer;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.arrowflight.sessions.FlightSessionsWithTokenManager;
import org.apache.doris.arrowflight.tokens.FlightTokenManager;
import org.apache.doris.arrowflight.tokens.FlightTokenManagerImpl;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.service.FrontendOptions;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * flight sql protocol implementation based on nio.
 */
public class DorisFlightSqlService {
    private static final Logger LOG = LogManager.getLogger(DorisFlightSqlService.class);
    private static final String GRPC_BUILDER_CONSUMER = "grpc.builderConsumer";
    private final FlightServer flightServer;
    private final FlightTokenManager flightTokenManager;
    private final FlightSessionsManager flightSessionsManager;
    private volatile boolean running;

    /**
     * The bearer token cache size: the effective Flight sub-quota (a session opens on a token, so the
     * sub-quota is what bounds live sessions), capped by {@code arrow_flight_token_cache_size}. The
     * sub-quota is floored at 1 -- a legal sub-quota of 0 ({@code qe_max_connection = 1}) still needs
     * one token so the first request reaches the pool and is refused with RESOURCE_EXHAUSTED, instead
     * of a {@code maximumSize(0)} cache evicting the freshly issued token and answering UNAUTHENTICATED.
     * The floor is NOT applied to {@code arrow_flight_token_cache_size}: an illegal value there
     * ({@literal <= 0}) stays the loud failure it is on the base (Guava rejects a negative maximumSize;
     * 0 evicts every token) rather than the FE silently running on a one-token cache.
     */
    @VisibleForTesting
    static int effectiveTokenCacheSize(int flightMaxConnections, int tokenCacheConfig) {
        return Math.min(Math.max(1, flightMaxConnections), tokenCacheConfig);
    }

    public DorisFlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
        // arrow flight sql is a stateless protocol, connection is usually not actively disconnected.
        // bearer token is evict from the cache will unregister ConnectContext.
        int flightMaxConnections = ConnectPoolMgr.effectiveFlightMaxConnections(
                Config.qe_max_connection, Config.arrow_flight_max_connections);
        if (Config.arrow_flight_max_connections > Config.qe_max_connection) {
            // A fe.conf from before the pools were merged may still carry the old default, 4096: capped
            // to the whole pool, that is exactly the sharing the default of half is there to prevent.
            LOG.warn("arrow_flight_max_connections={} exceeds qe_max_connection={}: Arrow Flight SQL sessions are"
                            + " connections of the one pool, so the sub-quota is capped at {}, the whole pool."
                            + " Flight sessions, which their clients mostly never close, can then hold every"
                            + " connection until wait_timeout and refuse MySQL logins. On an FE that serves both"
                            + " protocols, remove the setting (the default is half of qe_max_connection; 4096 was"
                            + " the default before the pools were merged) or set it below qe_max_connection",
                    Config.arrow_flight_max_connections, Config.qe_max_connection, flightMaxConnections);
        }
        int tokenCacheSize = effectiveTokenCacheSize(flightMaxConnections, Config.arrow_flight_token_cache_size);
        this.flightTokenManager = new FlightTokenManagerImpl(tokenCacheSize,
                Config.arrow_flight_token_alive_time_second);
        this.flightSessionsManager = new FlightSessionsWithTokenManager(flightTokenManager);

        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port), flightSessionsManager);
        flightServer = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", port), producer)
                .transportHint(GRPC_BUILDER_CONSUMER, (Consumer<ServerBuilder<?>>) builder ->
                        builder.addStreamTracerFactory(new FlightRemoteIpServerStreamTracer.Factory()))
                .headerAuthenticator(new FlightBearerTokenAuthenticator(flightTokenManager)).build();
        LOG.info("Arrow Flight SQL service is created, port: {}, arrow_flight_max_connections: {} (effective: {},"
                        + " within qe_max_connection: {}), token cache size: {} (arrow_flight_token_cache_size: {}),"
                        + " arrow_flight_token_alive_time_second: {}", port,
                Config.arrow_flight_max_connections, flightMaxConnections, Config.qe_max_connection,
                tokenCacheSize, Config.arrow_flight_token_cache_size, Config.arrow_flight_token_alive_time_second);
    }

    // start Arrow Flight SQL service, return true if success, otherwise false
    public boolean start() {
        try {
            flightServer.start();
            running = true;
            LOG.info("Arrow Flight SQL service is started.");
        } catch (IOException e) {
            LOG.error("Start Arrow Flight SQL service failed.", e);
            return false;
        }
        return true;
    }

    public void stop() {
        if (running) {
            running = false;
            try {
                flightServer.close();
            } catch (InterruptedException e) {
                LOG.warn("close Arrow Flight SQL server failed.", e);
            }
        }
    }
}
