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

import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.ProxyProtocolHandler;
import org.apache.doris.protocol.ProtocolConfig;
import org.apache.doris.protocol.ProtocolHandler;
import org.apache.doris.protocol.ProtocolLoader;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.service.arrowflight.DorisFlightSqlService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the encapsulation of the entire front-end service,
 * including the creation of services that support the MySQL protocol
 * and Arrow Flight SQL protocol.
 * 
 * <p>QeService loads protocol handlers via SPI (Service Provider Interface) mechanism,
 * allowing the kernel to support multiple protocols (MySQL, Arrow Flight SQL) without
 * hard-coding protocol-specific logic.
 * 
 * <h3>Initialization Flow:</h3>
 * <ol>
 *   <li>Build ProtocolConfig from Config and FrontendOptions</li>
 *   <li>Load protocol handlers via ProtocolLoader.loadConfiguredProtocols()</li>
 *   <li>Set acceptor callbacks for each protocol</li>
 *   <li>Start each protocol handler</li>
 * </ol>
 */
public class QeService {
    private static final Logger LOG = LogManager.getLogger(QeService.class);

    private int port;
    private int arrowFlightSQLPort;
    private ConnectScheduler scheduler;
    
    // Protocol handlers loaded via SPI
    private final List<ProtocolHandler> protocolHandlers = new ArrayList<>();
    
    // Task executor for handling connections asynchronously
    private java.util.concurrent.ExecutorService taskExecutor;
    
    // Legacy: Arrow Flight service (to be migrated to fe-protocol-arrowflight)
    private DorisFlightSqlService dorisFlightSqlService;

    @Deprecated
    public QeService(int port, int arrowFlightSQLPort) {
        this.port = port;
        this.arrowFlightSQLPort = arrowFlightSQLPort;
    }

    public QeService(int port, int arrowFlightSQLPort, ConnectScheduler scheduler) {
        this.port = port;
        this.arrowFlightSQLPort = arrowFlightSQLPort;
        this.scheduler = scheduler;
        
        // Build protocol configuration
        ProtocolConfig protocolConfig = buildProtocolConfig();
        
        // Load and initialize protocol handlers via SPI
        List<ProtocolHandler> handlers = ProtocolLoader.loadConfiguredProtocols(protocolConfig);
        
        // Set up acceptor callbacks for each protocol
        for (ProtocolHandler handler : handlers) {
            String protocolName = handler.getProtocolName();
            
            if ("mysql".equalsIgnoreCase(protocolName)) {
                // Set MySQL connection acceptor
                handler.setAcceptor(this::handleMysqlConnection);
                LOG.info("Registered MySQL connection acceptor");
            } else if ("arrowflight".equalsIgnoreCase(protocolName)) {
                // Set Arrow Flight connection acceptor
                handler.setAcceptor(this::handleArrowFlightConnection);
                LOG.info("Registered Arrow Flight connection acceptor");
            }
            
            protocolHandlers.add(handler);
        }
    }

    /**
     * Builds ProtocolConfig by collecting parameters from Config and FrontendOptions.
     * 
     * @return configured ProtocolConfig
     */
    private ProtocolConfig buildProtocolConfig() {
        // Create base config with ports and scheduler
        ProtocolConfig config = new ProtocolConfig(port, arrowFlightSQLPort, scheduler);
        
        // ==================== MySQL Protocol Configuration ====================
        
        // MySQL IO threads (default: 4)
        config.set(ProtocolConfig.KEY_MYSQL_IO_THREADS, Config.mysql_service_io_threads_num);
        
        // MySQL backlog size (default: 1024)
        config.set(ProtocolConfig.KEY_MYSQL_BACKLOG, Config.mysql_nio_backlog_num);
        
        // MySQL TCP keep-alive (default: false)
        config.set(ProtocolConfig.KEY_MYSQL_KEEP_ALIVE, Config.mysql_nio_enable_keep_alive);
        
        // MySQL bind IPv6 (from FrontendOptions)
        config.set(ProtocolConfig.KEY_MYSQL_BIND_IPV6, FrontendOptions.isBindIPV6());
        
        // MySQL max task threads (default: 4096)
        config.set(ProtocolConfig.KEY_MYSQL_MAX_TASK_THREADS, Config.max_mysql_service_task_threads_num);
        
        // MySQL worker name
        config.set(ProtocolConfig.KEY_MYSQL_WORKER_NAME, "doris-mysql-nio");
        
        // MySQL task executor (external thread pool from ThreadPoolManager)
        this.taskExecutor = ThreadPoolManager.newDaemonCacheThreadPool(
                Config.max_mysql_service_task_threads_num,
                "doris-mysql-nio-pool",
                true
        );
        config.set(ProtocolConfig.KEY_MYSQL_TASK_EXECUTOR, taskExecutor);
        
        // ==================== Arrow Flight Protocol Configuration ====================
        
        // Arrow Flight host to bind (default: "::0")
        config.set(ProtocolConfig.KEY_ARROWFLIGHT_HOST, "::0");
        
        // Arrow Flight token cache size
        config.set(ProtocolConfig.KEY_ARROWFLIGHT_TOKEN_CACHE_SIZE, Config.arrow_flight_token_cache_size);
        
        // Arrow Flight token TTL (convert seconds to minutes)
        config.set(ProtocolConfig.KEY_ARROWFLIGHT_TOKEN_TTL_MINUTES, 
                Config.arrow_flight_token_alive_time_second / 60);
        
        // Arrow Flight max connections
        config.set(ProtocolConfig.KEY_MAX_CONNECTIONS, Config.arrow_flight_max_connections);
        
        LOG.info("Built ProtocolConfig: mysqlPort={}, arrowFlightPort={}, mysqlIoThreads={}, "
                + "mysqlBacklog={}, mysqlKeepAlive={}, bindIPv6={}, maxTaskThreads={}, "
                + "arrowFlightTokenCacheSize={}, arrowFlightTokenTtlMin={}, maxConnections={}",
                port, arrowFlightSQLPort, Config.mysql_service_io_threads_num,
                Config.mysql_nio_backlog_num, Config.mysql_nio_enable_keep_alive,
                FrontendOptions.isBindIPV6(), Config.max_mysql_service_task_threads_num,
                Config.arrow_flight_token_cache_size, 
                Config.arrow_flight_token_alive_time_second / 60,
                Config.arrow_flight_max_connections);
        
        return config;
    }

    /**
     * Handles new MySQL protocol connections.
     * Called by MysqlProtocolHandler when a new client connects.
     * 
     * <p>This method submits connection handling to a worker thread pool to avoid
     * blocking the acceptor thread. The actual negotiation and authentication
     * happens asynchronously in the worker thread.
     * 
     * @param connection the StreamConnection from XNIO
     */
    private void handleMysqlConnection(Object connection) {
        if (!(connection instanceof StreamConnection)) {
            LOG.error("Invalid connection object type: {}", connection.getClass().getName());
            return;
        }
        
        StreamConnection streamConnection = (StreamConnection) connection;
        
        // Submit connection handling to worker thread pool to avoid blocking acceptor thread
        // negotiate() contains blocking calls that wait for client responses
        taskExecutor.submit(() -> {
            ConnectContext context = null;
            try {
                // Create ConnectContext for this MySQL connection
                context = new ConnectContext(streamConnection);
                
                // Set thread local info
                context.setThreadLocalInfo();
                context.setConnectScheduler(scheduler);
                context.setEnv(org.apache.doris.catalog.Env.getCurrentEnv());
                
                // Submit connection to scheduler (assigns connection ID)
                if (!scheduler.submit(context)) {
                    LOG.warn("Failed to submit MySQL connection to scheduler");
                    context.cleanup();
                    return;
                }
                
                // Handle proxy protocol if enabled
                if (Config.enable_proxy_protocol) {
                    org.apache.doris.mysql.ProxyProtocolHandler.ProxyProtocolResult result = 
                            org.apache.doris.mysql.ProxyProtocolHandler.handle(context.getMysqlChannel());
                    if (result != null) {
                        org.apache.doris.mysql.ProxyProtocolHandler.ProtocolType pType = result.pType;
                        if (pType == org.apache.doris.mysql.ProxyProtocolHandler.ProtocolType.PROTOCOL_WITH_IP) {
                            context.getMysqlChannel().setRemoteAddr(result.sourceIP, result.sourcePort);
                        }
                        // For PROTOCOL_WITHOUT_IP, use IP from MySQL protocol (already set when creating MysqlChannel)
                        // For NOT_PROXY_PROTOCOL, ignore to let connection with no proxy protocol in
                    }
                }
                
                // Perform MySQL protocol handshake and authentication
                // This sends handshake packet and waits for auth response (BLOCKING)
                if (!org.apache.doris.mysql.MysqlProto.negotiate(context)) {
                    // negotiate failed, cleanup and return
                    context.cleanup();
                    return;
                }
                
                // Register connection (checks max connection limits)
                // Return -1 means register OK
                // Return >=0 means register failed, and return value is current connection num
                int res = scheduler.getConnectPoolMgr().registerConnection(context);
                if (res == -1) {
                    // Registration successful
                    // Send response packet and set up close listener
                    org.apache.doris.mysql.MysqlProto.sendResponsePacket(context);
                    ConnectContext finalContext = context;
                    streamConnection.setCloseListener(
                            conn -> scheduler.getConnectPoolMgr().unregisterConnection(finalContext));
                } else {
                    // Registration failed - connection limit reached
                    long userConnLimit = context.getEnv().getAuth().getMaxConn(context.getQualifiedUser());
                    String errMsg = String.format(
                            "Reach limit of connections. Total: %d, User: %d, Current: %d",
                            scheduler.getConnectPoolMgr().getMaxConnections(), userConnLimit, res);
                    context.getState().setError(ErrorCode.ERR_TOO_MANY_USER_CONNECTIONS, errMsg);
                    org.apache.doris.mysql.MysqlProto.sendResponsePacket(context);
                    context.cleanup();
                    return;
                }
                
                // Set start time and query timeout
                context.setStartTime();
                int userQueryTimeout = context.getEnv().getAuth().getQueryTimeout(context.getQualifiedUser());
                if (userQueryTimeout <= 0 && LOG.isDebugEnabled()) {
                    LOG.debug("Connection set query timeout to {}",
                            context.getSessionVariable().getQueryTimeoutS());
                }
                context.setUserQueryTimeout(userQueryTimeout);
                context.setUserInsertTimeout(
                        context.getEnv().getAuth().getInsertTimeout(context.getQualifiedUser()));
                
                // Create MySQL protocol processor
                MysqlConnectProcessor processor = new MysqlConnectProcessor(context);
                
                // Start accepting queries
                context.startAcceptQuery(processor);
                
                if (LOG.isDebugEnabled()) {
                    LOG.debug("MySQL connection accepted: connectionId={}, user={}, remoteAddr={}", 
                            context.getConnectionId(), 
                            context.getQualifiedUser(),
                            streamConnection.getPeerAddress());
                }
                
            } catch (Throwable e) {
                // should be unexpected exception, so print warn log
                if (context != null && context.getCurrentUserIdentity() != null) {
                    LOG.warn("connect processor exception because ", e);
                } else if (e instanceof Error) {
                    LOG.error("connect processor exception because ", e);
                } else {
                    // for unauthorized access such lvs probe request,
                    // may cause exception, just log it in debug level
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("connect processor exception because ", e);
                    }
                }
                if (context != null) {
                    context.cleanup();
                }
            } finally {
                ConnectContext.remove();
            }
        });
    }

    /**
     * Handles new Arrow Flight SQL connections.
     * Called by ArrowFlightProtocolHandler when a new client connects.
     * 
     * @param connection the connection context from Arrow Flight
     */
    private void handleArrowFlightConnection(Object connection) {
        // TODO: Implement Arrow Flight connection handling
        // This will be fully implemented when Arrow Flight protocol is migrated
        // to fe-protocol-arrowflight module
        LOG.debug("Arrow Flight connection accepted: {}", connection);
    }

    /**
     * Starts the QE service and all protocol handlers.
     * 
     * @throws Exception if startup fails
     */
    public void start() throws Exception {
        // Set up help module
        try {
            HelpModule.getInstance().setUpModule(HelpModule.HELP_ZIP_FILE_NAME);
        } catch (Exception e) {
            LOG.warn("Help module failed. ignore it.", e);
            // TODO: ignore the help module failure temporarily.
            // We should fix it in the future.
        }

        
        // Start all protocol handlers
        for (ProtocolHandler handler : protocolHandlers) {
            LOG.info("Starting protocol: {} (version: {}) on port {}", 
                    handler.getProtocolName(), 
                    handler.getProtocolVersion(),
                    handler.getPort());
            
            if (!handler.start()) {
                LOG.error("Failed to start protocol: {}", handler.getProtocolName());
                throw new RuntimeException("Failed to start protocol: " + handler.getProtocolName());
            }
            
            LOG.info("Protocol '{}' started successfully on port {}", 
                    handler.getProtocolName(), handler.getPort());
        }
        
        // Legacy: Start Arrow Flight service if not using SPI handler
        // TODO: Remove this after Arrow Flight is fully migrated to SPI
        if (arrowFlightSQLPort > 0 && !hasArrowFlightHandler()) {
            LOG.info("Starting legacy Arrow Flight SQL service on port {}", arrowFlightSQLPort);
            dorisFlightSqlService = new DorisFlightSqlService(arrowFlightSQLPort);
            dorisFlightSqlService.start();
        }
        
        LOG.info("QE service started. Active protocols: {}", getActiveProtocolNames());
    }

    /**
     * Checks if Arrow Flight handler is loaded via SPI.
     * 
     * @return true if Arrow Flight handler is present
     */
    private boolean hasArrowFlightHandler() {
        return protocolHandlers.stream()
                .anyMatch(h -> "arrowflight".equalsIgnoreCase(h.getProtocolName()));
    }

    /**
     * Gets the names of all active protocol handlers.
     * 
     * @return comma-separated list of protocol names
     */
    private String getActiveProtocolNames() {
        return protocolHandlers.stream()
                .map(ProtocolHandler::getProtocolName)
                .reduce((a, b) -> a + ", " + b)
                .orElse("none");
    }

    /**
     * Stops the QE service and all protocol handlers.
     */
    public void stop() {
        LOG.info("Stopping QE service...");
        
        // Stop all protocol handlers
        for (ProtocolHandler handler : protocolHandlers) {
            try {
                LOG.info("Stopping protocol: {}", handler.getProtocolName());
                handler.stop();
            } catch (Exception e) {
                LOG.error("Failed to stop protocol: {}", handler.getProtocolName(), e);
            }
        }
        
        // Stop legacy Arrow Flight service
        if (dorisFlightSqlService != null) {
            try {
                dorisFlightSqlService.stop();
            } catch (Exception e) {
                LOG.error("Failed to stop Arrow Flight SQL service", e);
            }
        }
        
        // Shutdown task executor
        if (taskExecutor != null) {
            try {
                LOG.info("Shutting down MySQL task executor...");
                taskExecutor.shutdown();
                if (!taskExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.warn("Task executor did not terminate in time, forcing shutdown");
                    taskExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for task executor shutdown", e);
                taskExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        LOG.info("QE service stopped");
    }
}
