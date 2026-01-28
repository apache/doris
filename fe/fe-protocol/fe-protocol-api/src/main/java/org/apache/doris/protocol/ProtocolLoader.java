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

package org.apache.doris.protocol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Protocol Loader - Loads protocol implementations via SPI.
 * 
 * <p>This class uses Java ServiceLoader mechanism to discover and load
 * protocol handler implementations. Protocol handlers must be registered
 * in {@code META-INF/services/org.apache.doris.protocol.ProtocolHandler}.
 * 
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * ProtocolConfig config = new ProtocolConfig(mysqlPort, arrowFlightPort, scheduler);
 * List<ProtocolHandler> handlers = ProtocolLoader.loadConfiguredProtocols(config);
 * 
 * for (ProtocolHandler handler : handlers) {
 *     handler.start();
 * }
 * }</pre>
 * 
 * <h3>Adding New Protocols:</h3>
 * <ol>
 *   <li>Implement {@link ProtocolHandler}</li>
 *   <li>Create META-INF/services/org.apache.doris.protocol.ProtocolHandler</li>
 *   <li>Add the implementation class name to the service file</li>
 * </ol>
 * 
 * @since 2.0.0
 */
public final class ProtocolLoader {
    
    private static final Logger LOG = LogManager.getLogger(ProtocolLoader.class);
    
    private ProtocolLoader() {
        // Utility class
    }
    
    /**
     * Loads all available protocol handlers.
     * 
     * @return list of protocol handlers
     */
    public static List<ProtocolHandler> loadAllProtocols() {
        List<ProtocolHandler> handlers = new ArrayList<>();
        ServiceLoader<ProtocolHandler> serviceLoader = ServiceLoader.load(ProtocolHandler.class);
        
        for (ProtocolHandler handler : serviceLoader) {
            LOG.info("Discovered protocol handler: {} (version: {})", 
                    handler.getProtocolName(), handler.getProtocolVersion());
            handlers.add(handler);
        }
        
        // Sort by priority (higher first)
        handlers.sort(Comparator.comparingInt(ProtocolHandler::getPriority).reversed());
        
        return handlers;
    }
    
    /**
     * Loads all available protocol handlers using the specified class loader.
     * 
     * @param classLoader class loader to use
     * @return list of protocol handlers
     */
    public static List<ProtocolHandler> loadAllProtocols(ClassLoader classLoader) {
        List<ProtocolHandler> handlers = new ArrayList<>();
        ServiceLoader<ProtocolHandler> serviceLoader = ServiceLoader.load(ProtocolHandler.class, classLoader);
        
        for (ProtocolHandler handler : serviceLoader) {
            LOG.info("Discovered protocol handler: {} (version: {})", 
                    handler.getProtocolName(), handler.getProtocolVersion());
            handlers.add(handler);
        }
        
        // Sort by priority (higher first)
        handlers.sort(Comparator.comparingInt(ProtocolHandler::getPriority).reversed());
        
        return handlers;
    }
    
    /**
     * Loads and initializes protocol handlers based on configuration.
     * 
     * <p>Only protocols that are enabled in the configuration will be loaded.
     * 
     * @param config protocol configuration
     * @return list of initialized protocol handlers
     */
    public static List<ProtocolHandler> loadConfiguredProtocols(ProtocolConfig config) {
        List<ProtocolHandler> allHandlers = loadAllProtocols();
        List<ProtocolHandler> enabledHandlers = new ArrayList<>();
        
        for (ProtocolHandler handler : allHandlers) {
            try {
                // Initialize handler
                handler.initialize(config);
                
                // Check if enabled
                if (handler.isEnabled(config)) {
                    enabledHandlers.add(handler);
                    LOG.info("Protocol '{}' enabled on port {}", 
                            handler.getProtocolName(), handler.getPort());
                } else {
                    LOG.info("Protocol '{}' is disabled", handler.getProtocolName());
                }
            } catch (ProtocolException e) {
                LOG.warn("Failed to initialize protocol '{}': {}", 
                        handler.getProtocolName(), e.getMessage());
            }
        }
        
        return enabledHandlers;
    }
    
    /**
     * Loads a specific protocol handler by name.
     * 
     * @param protocolName protocol name (case-insensitive)
     * @return protocol handler or null if not found
     */
    public static ProtocolHandler loadProtocol(String protocolName) {
        List<ProtocolHandler> handlers = loadAllProtocols();
        
        for (ProtocolHandler handler : handlers) {
            if (handler.getProtocolName().equalsIgnoreCase(protocolName)) {
                return handler;
            }
        }
        
        LOG.warn("Protocol '{}' not found", protocolName);
        return null;
    }
    
    /**
     * Loads and initializes a specific protocol handler by name.
     * 
     * @param protocolName protocol name (case-insensitive)
     * @param config protocol configuration
     * @return initialized protocol handler or null if not found
     */
    public static ProtocolHandler loadProtocol(String protocolName, ProtocolConfig config) {
        ProtocolHandler handler = loadProtocol(protocolName);
        
        if (handler != null) {
            try {
                handler.initialize(config);
            } catch (ProtocolException e) {
                LOG.warn("Failed to initialize protocol '{}': {}", protocolName, e.getMessage());
                return null;
            }
        }
        
        return handler;
    }
    
    /**
     * Gets a list of all registered protocol names.
     * 
     * @return list of protocol names
     */
    public static List<String> getRegisteredProtocolNames() {
        List<String> names = new ArrayList<>();
        List<ProtocolHandler> handlers = loadAllProtocols();
        
        for (ProtocolHandler handler : handlers) {
            names.add(handler.getProtocolName());
        }
        
        return names;
    }
}
