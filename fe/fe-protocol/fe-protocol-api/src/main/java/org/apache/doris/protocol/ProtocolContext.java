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

/**
 * Protocol Context Interface.
 *
 * <p>This interface defines the contract between the kernel and protocol
 * implementations for connection context management. It provides protocol-agnostic
 * methods that the kernel can use to interact with any protocol.
 *
 * <p>Each protocol implementation should have its own context class that
 * implements this interface, wrapping protocol-specific details.
 *
 * <h3>Implementation Requirements:</h3>
 * <ul>
 *   <li>Thread-safe for concurrent access</li>
 *   <li>Proper resource cleanup in {@link #cleanup()}</li>
 *   <li>Session state preservation across requests</li>
 * </ul>
 *
 * @since 2.0.0
 */
public interface ProtocolContext {

    /**
     * Gets the protocol name for this context.
     *
     * @return protocol name (e.g., "mysql", "arrowflight")
     */
    String getProtocolName();

    /**
     * Gets the unique connection ID.
     *
     * @return connection ID
     */
    long getConnectionId();

    /**
     * Gets the remote client address.
     *
     * @return client IP address
     */
    String getRemoteIP();

    /**
     * Gets the remote host and port string.
     *
     * @return host:port string
     */
    String getRemoteHostPortString();

    /**
     * Gets the current user name.
     *
     * @return user name, or null if not authenticated
     */
    String getUser();

    /**
     * Gets the current database.
     *
     * @return database name, or null if not set
     */
    String getDatabase();

    /**
     * Sets the current database.
     *
     * @param database database name
     */
    void setDatabase(String database);

    /**
     * Checks if the connection is authenticated.
     *
     * @return true if authenticated
     */
    boolean isAuthenticated();

    /**
     * Checks if the connection is killed/closed.
     *
     * @return true if killed
     */
    boolean isKilled();

    /**
     * Marks the connection as killed.
     */
    void setKilled();

    /**
     * Checks if SSL/TLS is enabled for this connection.
     *
     * @return true if using SSL
     */
    boolean isSslEnabled();

    /**
     * Cleans up resources associated with this context.
     *
     * <p>Called when the connection is closed. Implementations should
     * release all resources (buffers, connections, etc.).
     */
    void cleanup();

    /**
     * Gets the start time of current command/query.
     *
     * @return start time in milliseconds
     */
    long getStartTime();

    /**
     * Sets the start time of current command/query.
     *
     * @param startTime start time in milliseconds
     */
    void setStartTime(long startTime);

    /**
     * Gets the protocol-specific channel/connection object.
     *
     * <p>This returns the underlying protocol connection, which can be
     * cast to the specific type (e.g., MysqlChannel).
     *
     * @param <T> channel type
     * @return protocol channel
     */
    <T> T getChannel();
}
