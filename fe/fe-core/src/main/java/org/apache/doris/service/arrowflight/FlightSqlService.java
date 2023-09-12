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

package org.apache.doris.service.arrowflight;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * flight sql protocol implementation based on nio.
 */
public class FlightSqlService {
    private static final Logger LOG = LogManager.getLogger(FlightSqlService.class);
    private final FlightServer flightServer;
    private volatile boolean running;
    public static final String FLIGHT_CLIENT_PROPERTIES_MIDDLEWARE = "client-properties-middleware";
    public static final FlightServerMiddleware.Key<ServerCookieMiddleware> FLIGHT_CLIENT_PROPERTIES_MIDDLEWARE_KEY
            = FlightServerMiddleware.Key.of(FLIGHT_CLIENT_PROPERTIES_MIDDLEWARE);

    public FlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
        Location location = Location.forGrpcInsecure("0.0.0.0", port);
        FlightSqlServiceImpl producer = new FlightSqlServiceImpl(location);
        flightServer = FlightServer.builder(allocator, location, producer)
            .middleware(FLIGHT_CLIENT_PROPERTIES_MIDDLEWARE_KEY,
                new ServerCookieMiddleware.Factory())
            .authHandler(new BasicServerAuthHandler(new FlightServerBasicAuthValidator())).build();
    }

    // start Flightsql protocol service
    // return true if success, otherwise false
    public boolean start() {
        try {
            flightServer.start();
            LOG.info("Flightsql network service is started.");
        } catch (IOException e) {
            LOG.warn("Open Flightsql network service failed.", e);
            return false;
        }
        return true;
    }

    public void stop() {
        if (running) {
            running = false;
            // close server channel, make accept throw exception
            try {
                flightServer.close();
            } catch (InterruptedException e) {
                LOG.warn("close server channel failed.", e);
            }
        }
    }
}
