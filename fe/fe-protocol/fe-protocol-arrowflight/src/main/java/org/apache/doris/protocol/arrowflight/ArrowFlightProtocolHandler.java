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

package org.apache.doris.protocol.arrowflight;

import org.apache.doris.protocol.ProtocolConfig;
import org.apache.doris.protocol.ProtocolException;
import org.apache.doris.protocol.ProtocolHandler;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Arrow Flight SQL Protocol Handler - SPI implementation for Arrow Flight SQL.
 *
 * <p>This handler implements the Arrow Flight SQL protocol, providing high-performance
 * data transfer using Apache Arrow's columnar format. It's designed for:
 * <ul>
 *   <li>High-throughput analytical queries</li>
 *   <li>Efficient data transfer to Python/Pandas/Spark clients</li>
 *   <li>Zero-copy data exchange where possible</li>
 * </ul>
 *
 * <h3>Protocol Features:</h3>
 * <ul>
 *   <li>Arrow Flight SQL standard compliance</li>
 *   <li>gRPC-based communication</li>
 *   <li>TLS/SSL support</li>
 *   <li>Token-based authentication</li>
 *   <li>Prepared statements</li>
 *   <li>Metadata queries</li>
 * </ul>
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * // Python client example
 * import pyarrow.flight as flight
 *
 * client = flight.connect("grpc://localhost:9090")
 * info = client.get_flight_info(
 *     flight.FlightDescriptor.for_command("SELECT * FROM table")
 * )
 * reader = client.do_get(info.endpoints[0].ticket)
 * df = reader.read_pandas()
 * }</pre>
 */
public class ArrowFlightProtocolHandler implements ProtocolHandler {

    private static final Logger LOG = LogManager.getLogger(ArrowFlightProtocolHandler.class);

    /**
     * Protocol name
     */
    public static final String PROTOCOL_NAME = "arrowflight";

    /**
     * Protocol version (Arrow Flight SQL version)
     */
    public static final String PROTOCOL_VERSION = "17.0";

    private int port = -1;
    private ProtocolConfig config;
    private Consumer<Object> acceptor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private String host = "::0";
    private FlightSqlProducer producer;
    private CallHeaderAuthenticator authenticator;
    private DorisFlightSqlService flightService;

    @Override
    public String getProtocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public String getProtocolVersion() {
        return PROTOCOL_VERSION;
    }

    @Override
    public void initialize(ProtocolConfig config) throws ProtocolException {
        this.config = config;
        this.port = config.getArrowFlightPort();

        if (port <= 0) {
            LOG.info("Arrow Flight SQL protocol is disabled (port not configured)");
            return;
        }

        LOG.info("Initializing Arrow Flight SQL protocol handler on port {}", port);

        // Validate configuration
        validateConfig(config);
        host = config.getArrowFlightHost();
        Object producerObj = config.getArrowFlightProducer();
        Object authenticatorObj = config.getArrowFlightAuthenticator();
        if (!(producerObj instanceof FlightSqlProducer)) {
            throw ProtocolException.configError("Arrow Flight producer is not configured");
        }
        if (!(authenticatorObj instanceof CallHeaderAuthenticator)) {
            throw ProtocolException.configError("Arrow Flight authenticator is not configured");
        }
        producer = (FlightSqlProducer) producerObj;
        authenticator = (CallHeaderAuthenticator) authenticatorObj;
    }

    /**
     * Validates Arrow Flight configuration.
     */
    private void validateConfig(ProtocolConfig config) throws ProtocolException {
        // Check for required configuration
        int maxStreams = config.getInt("arrowflight.max.streams", 100);
        if (maxStreams <= 0) {
            throw ProtocolException.configError("Invalid max streams value: " + maxStreams);
        }

        // Check buffer sizes
        int bufferSize = config.getInt("arrowflight.buffer.size", 64 * 1024);
        if (bufferSize < 1024) {
            throw ProtocolException.configError("Buffer size too small: " + bufferSize);
        }
    }

    @Override
    public void setAcceptor(Consumer<Object> acceptor) {
        this.acceptor = acceptor;
    }

    @Override
    public boolean start() {
        if (port <= 0) {
            LOG.info("Arrow Flight SQL protocol is disabled, skipping start");
            return true;
        }

        try {
            // Initialize and start the Flight server
            // This would integrate with the existing DorisFlightSqlService
            LOG.info("Starting Arrow Flight SQL server on port {}", port);

            // The actual Flight server creation is delegated to the existing
            // DorisFlightSqlService implementation to maintain compatibility
            startFlightServer();

            running.set(true);
            LOG.info("Arrow Flight SQL protocol handler started on port {}", port);
            return true;
        } catch (Exception e) {
            LOG.error("Failed to start Arrow Flight SQL protocol handler on port {}", port, e);
            return false;
        }
    }

    /**
     * Starts the Flight server.
     *
     * <p>This method integrates with the existing DorisFlightSqlService.
     */
    private void startFlightServer() throws Exception {
        if (flightService == null) {
            flightService = new DorisFlightSqlService(host, port, producer, authenticator);
        }
        if (!flightService.start()) {
            throw new ProtocolException("Arrow Flight SQL service failed to start");
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping Arrow Flight SQL protocol handler");
        running.set(false);

        if (flightService != null) {
            flightService.stop();
            flightService = null;
        }

        LOG.info("Arrow Flight SQL protocol handler stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public boolean isEnabled(ProtocolConfig config) {
        return config.getArrowFlightPort() > 0;
    }

    @Override
    public int getPriority() {
        // Lower priority than MySQL
        return 50;
    }

    /**
     * Gets the maximum number of concurrent streams.
     *
     * @return max streams
     */
    public int getMaxStreams() {
        return config.getInt("arrowflight.max.streams", 100);
    }

    /**
     * Checks if TLS is enabled.
     *
     * @return true if TLS enabled
     */
    public boolean isTlsEnabled() {
        return config.getBoolean("arrowflight.tls.enabled", false);
    }
}
