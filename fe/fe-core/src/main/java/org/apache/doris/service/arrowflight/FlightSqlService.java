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
import org.apache.arrow.flight.Location;
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

    public FlightSqlService(int port) {
        BufferAllocator allocator = new RootAllocator();
        Location location = Location.forGrpcInsecure("0.0.0.0", port);
        FlightSqlServiceImpl producer = new FlightSqlServiceImpl(location);
        flightServer = FlightServer.builder(allocator, location, producer).build();
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
