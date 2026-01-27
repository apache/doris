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

import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.protocol.ProtocolConfig;
import org.apache.doris.protocol.ProtocolHandler;
import org.apache.doris.protocol.ProtocolLoader;
import org.apache.doris.protocol.mysql.MysqlProto;
import org.apache.doris.protocol.mysql.channel.MysqlChannel;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.service.arrowflight.DorisFlightSqlService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Query Execution Service - manages protocol handlers and query execution lifecycle.
 * 
 * <p>MySQL protocol is loaded via SPI plugin architecture.
 * ArrowFlight is created directly (to be refactored in future PR).
 */
public class QeService {
    private static final Logger LOG = LogManager.getLogger(QeService.class);

    private final List<ProtocolHandler> protocolHandlers;
    private final ConnectScheduler scheduler;
    private final int arrowFlightPort;

    /**
     * Creates a QeService.
     * 
     * @param mysqlPort MySQL server port (-1 to disable)
     * @param arrowFlightPort Arrow Flight SQL server port (-1 to disable)
     * @param scheduler connection scheduler for MySQL protocol
     */
    public QeService(int mysqlPort, int arrowFlightPort, ConnectScheduler scheduler) {
        this.scheduler = scheduler;
        this.arrowFlightPort = arrowFlightPort;
        this.protocolHandlers = new ArrayList<>();
        
        // Load MySQL protocol via SPI
        ProtocolConfig config = new ProtocolConfig(mysqlPort, arrowFlightPort, scheduler);
        List<ProtocolHandler> spiHandlers = ProtocolLoader.loadConfiguredProtocols(config);
        
        // Inject acceptor for MySQL protocol
        for (ProtocolHandler handler : spiHandlers) {
            if ("mysql".equals(handler.getProtocolName())) {
                handler.setAcceptor(this::handleMysqlConnection);
            }
            protocolHandlers.add(handler);
        }
    }
    
    /**
     * Handles new MySQL connection - this is the acceptor callback.
     */
    private void handleMysqlConnection(Object connection) {
        StreamConnection streamConnection = (StreamConnection) connection;
        ConnectContext context = new ConnectContext(streamConnection);
        
        if (!scheduler.submit(context)) {
            LOG.warn("Reject connection from {}", context.getRemoteIP());
            ErrorReport.report(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS);
            try {
                context.getState().setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, 
                        "Reach limit of connections");
                MysqlProto.sendResponsePacket(context);
            } catch (IOException e) {
                LOG.warn("Failed to send error packet", e);
            } finally {
                context.cleanup();
            }
            return;
        }
        
        ConnectProcessor processor = new MysqlConnectProcessor(context);
        
        streamConnection.getWorker().execute(() -> {
            context.setThreadLocalInfo();
            try {
                if (!MysqlProto.negotiate(context)) {
                    LOG.warn("Failed to negotiate with client from {}", context.getRemoteIP());
                    context.cleanup();
                    return;
                }
                
                MysqlChannel channel = context.getMysqlChannel();
                channel.startAcceptQuery(context, processor);
            } catch (Exception e) {
                LOG.warn("Exception happened in one session(" + context + ").", e);
                context.setKilled();
                context.cleanup();
            } finally {
                ConnectContext.remove();
            }
        });
    }

    /**
     * Starts all configured protocol handlers.
     * 
     * @throws Exception if any protocol fails to start
     */
    public void start() throws Exception {
        // Set up help module
        try {
            HelpModule.getInstance().setUpModule(HelpModule.HELP_ZIP_FILE_NAME);
        } catch (Exception e) {
            LOG.warn("Help module failed. ignore it.", e);
        }

        // Start all protocol handlers
        for (ProtocolHandler handler : protocolHandlers) {
            LOG.info("Starting protocol: {}", handler.getProtocolName());
            if (!handler.start()) {
                LOG.error("Failed to start protocol: {}", handler.getProtocolName());
                System.exit(-1);
            }
            LOG.info("Protocol '{}' started successfully", handler.getProtocolName());
        }
        
        LOG.info("QE service started with {} protocol(s)", protocolHandlers.size());
    }
    
    /**
     * Stops all protocol handlers.
     */
    public void stop() {
        LOG.info("Stopping QE service...");
        for (ProtocolHandler handler : protocolHandlers) {
            try {
                LOG.info("Stopping protocol: {}", handler.getProtocolName());
                handler.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping protocol: " + handler.getProtocolName(), e);
            }
        }
        LOG.info("QE service stopped");
    }
    
    /**
     * Returns list of active protocol handlers.
     * 
     * @return unmodifiable list of protocol handlers
     */
    public List<ProtocolHandler> getProtocolHandlers() {
        return new ArrayList<>(protocolHandlers);
    }
}
