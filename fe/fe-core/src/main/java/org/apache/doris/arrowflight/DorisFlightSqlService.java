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
import org.apache.doris.arrowflight.sessions.FlightSessionsInConnectPool;
import org.apache.doris.arrowflight.sessions.FlightSessionsManager;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.grpc.ServerBuilder;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * flight sql protocol implementation based on nio.
 */
public class DorisFlightSqlService {
    private static final Logger LOG = LogManager.getLogger(DorisFlightSqlService.class);
    private static final String GRPC_BUILDER_CONSUMER = "grpc.builderConsumer";
    // The defaults of the settings that stopped being read when a bearer token became the credential
    // of exactly one session: what a fe.conf that still sets them is compared against at startup.
    @VisibleForTesting
    static final int DEFAULT_TOKEN_CACHE_SIZE = 4096;
    @VisibleForTesting
    static final int DEFAULT_TOKEN_ALIVE_TIME_SECOND = 86400;

    private final FlightServer flightServer;
    private final FlightSessionsManager flightSessionsManager;
    private volatile boolean running;

    /**
     * The settings of the former bearer token cache that fe.conf still sets to something other than
     * their default, as "name=value": a token now lives exactly as long as its session, so neither
     * bounds anything any more, and an operator who tuned them should learn that at startup rather
     * than from a limit that is not where it was left.
     */
    @VisibleForTesting
    static List<String> ignoredTokenSettings(int tokenCacheSize, int tokenAliveTimeSecond) {
        List<String> ignored = Lists.newArrayList();
        if (tokenCacheSize != DEFAULT_TOKEN_CACHE_SIZE) {
            ignored.add("arrow_flight_token_cache_size=" + tokenCacheSize);
        }
        if (tokenAliveTimeSecond != DEFAULT_TOKEN_ALIVE_TIME_SECOND) {
            ignored.add("arrow_flight_token_alive_time_second=" + tokenAliveTimeSecond);
        }
        return ignored;
    }

    public DorisFlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
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
        List<String> ignoredTokenSettings = ignoredTokenSettings(Config.arrow_flight_token_cache_size,
                Config.arrow_flight_token_alive_time_second);
        if (!ignoredTokenSettings.isEmpty()) {
            LOG.warn("{} is set in fe.conf but no longer read and will be removed in a later release: a bearer"
                            + " token of the Arrow Flight SQL server is the credential of exactly one session and"
                            + " lives as long as it, so the sessions are bounded by the connection pool"
                            + " (qe_max_connection, arrow_flight_max_connections, max_user_connections) and end"
                            + " with CloseSession, KILL CONNECTION or wait_timeout; remove the setting",
                    String.join(", ", ignoredTokenSettings));
        }
        // A session is a connection of the one pool, known by its bearer token; the sessions manager is
        // both what issues and validates tokens (the header authenticator) and what the producer
        // resolves a call's session with.
        this.flightSessionsManager = new FlightSessionsInConnectPool(ExecuteEnv.getInstance().getScheduler());

        DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port), flightSessionsManager);
        flightServer = FlightServer.builder(allocator, Location.forGrpcInsecure("0.0.0.0", port), producer)
                .transportHint(GRPC_BUILDER_CONSUMER, (Consumer<ServerBuilder<?>>) builder ->
                        builder.addStreamTracerFactory(new FlightRemoteIpServerStreamTracer.Factory()))
                .headerAuthenticator(new FlightBearerTokenAuthenticator(flightSessionsManager)).build();
        LOG.info("Arrow Flight SQL service is created, port: {}, arrow_flight_max_connections: {} (effective: {},"
                        + " within qe_max_connection: {})", port,
                Config.arrow_flight_max_connections, flightMaxConnections, Config.qe_max_connection);
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
