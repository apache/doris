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

import java.util.function.Consumer;

/**
 * Protocol Handler SPI Interface.
 * 
 * <p>This is the main entry point for protocol implementations. Each protocol
 * (MySQL, Arrow Flight SQL, PostgreSQL, etc.) should implement this interface.
 * 
 * <p>The kernel loads protocol handlers via Java ServiceLoader mechanism,
 * without knowing the specific protocol implementation details.
 * 
 * <h3>Lifecycle:</h3>
 * <ol>
 *   <li>{@link #initialize(ProtocolConfig)} - Initialize with configuration</li>
 *   <li>{@link #setAcceptor(Consumer)} - Set connection acceptor callback</li>
 *   <li>{@link #start()} - Start accepting connections</li>
 *   <li>{@link #stop()} - Stop and cleanup</li>
 * </ol>
 * 
 * <h3>Implementation Requirements:</h3>
 * <ul>
 *   <li>Must have a public no-arg constructor</li>
 *   <li>Must be registered in META-INF/services/org.apache.doris.protocol.ProtocolHandler</li>
 *   <li>Should be thread-safe</li>
 * </ul>
 * 
 * @since 2.0.0
 */
public interface ProtocolHandler {
    
    /**
     * Returns the unique protocol name.
     * 
     * <p>This name is used to identify the protocol and should be lowercase.
     * Examples: "mysql", "arrowflight", "postgresql"
     * 
     * @return protocol name, never null
     */
    String getProtocolName();
    
    /**
     * Returns the protocol version.
     * 
     * @return version string (e.g., "5.7", "10.0")
     */
    default String getProtocolVersion() {
        return "1.0";
    }
    
    /**
     * Initializes the protocol handler with configuration.
     * 
     * <p>Called once before {@link #start()}. Implementations should
     * validate configuration and prepare resources.
     * 
     * @param config protocol configuration
     * @throws ProtocolException if initialization fails
     */
    void initialize(ProtocolConfig config) throws ProtocolException;
    
    /**
     * Sets the connection acceptor callback.
     * 
     * <p>When a new connection is established, the protocol handler should
     * invoke this callback with the connection object. The kernel will then
     * handle the connection lifecycle.
     * 
     * @param acceptor connection acceptor callback
     */
    void setAcceptor(Consumer<Object> acceptor);
    
    /**
     * Starts the protocol handler.
     * 
     * <p>After this call, the handler should be ready to accept connections.
     * This method should not block; use a separate thread for accepting connections.
     * 
     * @return true if started successfully, false otherwise
     */
    boolean start();
    
    /**
     * Stops the protocol handler.
     * 
     * <p>Should gracefully close all connections and release resources.
     */
    void stop();
    
    /**
     * Checks if the handler is running.
     * 
     * @return true if running and accepting connections
     */
    boolean isRunning();
    
    /**
     * Returns the port this handler is listening on.
     * 
     * @return port number, or -1 if not listening
     */
    int getPort();
    
    /**
     * Checks if this protocol is enabled based on configuration.
     * 
     * @param config protocol configuration
     * @return true if the protocol should be loaded
     */
    default boolean isEnabled(ProtocolConfig config) {
        return getPort() > 0;
    }
    
    /**
     * Returns handler priority for loading order.
     * 
     * <p>Higher priority handlers are loaded first. Default is 0.
     * 
     * @return priority value
     */
    default int getPriority() {
        return 0;
    }
}
