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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Configuration for protocol handlers.
 *
 * <p>This class provides a protocol-agnostic way to pass configuration
 * from the kernel to protocol implementations. Each protocol can define
 * its own configuration keys.
 *
 * <h3>Standard Configuration Keys:</h3>
 * <ul>
 *   <li>{@code mysql.port} - MySQL protocol port</li>
 *   <li>{@code arrowflight.port} - Arrow Flight SQL port</li>
 *   <li>{@code ssl.enabled} - Enable SSL/TLS</li>
 *   <li>{@code ssl.keystore.path} - SSL keystore path</li>
 * </ul>
 *
 * <h3>MySQL-Specific Configuration Keys:</h3>
 * <ul>
 *   <li>{@code mysql.io.threads} - IO thread count</li>
 *   <li>{@code mysql.backlog} - TCP backlog size</li>
 *   <li>{@code mysql.keep.alive} - Enable TCP keep-alive</li>
 *   <li>{@code mysql.bind.ipv6} - Bind to IPv6 address</li>
 *   <li>{@code mysql.task.executor} - External executor service</li>
 * </ul>
 */
public class ProtocolConfig {

    /**
     * Standard key for MySQL port
     */
    public static final String KEY_MYSQL_PORT = "mysql.port";

    /**
     * Standard key for Arrow Flight port
     */
    public static final String KEY_ARROWFLIGHT_PORT = "arrowflight.port";

    /**
     * Arrow Flight host to bind
     */
    public static final String KEY_ARROWFLIGHT_HOST = "arrowflight.host";

    /**
     * Arrow Flight token cache size
     */
    public static final String KEY_ARROWFLIGHT_TOKEN_CACHE_SIZE = "arrowflight.token.cache.size";

    /**
     * Arrow Flight token TTL in minutes
     */
    public static final String KEY_ARROWFLIGHT_TOKEN_TTL_MINUTES = "arrowflight.token.ttl.minutes";

    /**
     * Arrow Flight FlightSqlProducer instance
     */
    public static final String KEY_ARROWFLIGHT_PRODUCER = "arrowflight.producer";

    /**
     * Arrow Flight CallHeaderAuthenticator instance
     */
    public static final String KEY_ARROWFLIGHT_AUTHENTICATOR = "arrowflight.authenticator";

    /**
     * Standard key for SSL enabled flag
     */
    public static final String KEY_SSL_ENABLED = "ssl.enabled";

    /**
     * Standard key for max connections
     */
    public static final String KEY_MAX_CONNECTIONS = "max.connections";

    /**
     * Standard key for connection scheduler
     */
    public static final String KEY_CONNECT_SCHEDULER = "connect.scheduler";

    // ==================== MySQL-Specific Configuration Keys ====================

    /**
     * MySQL IO threads count (default: 4)
     */
    public static final String KEY_MYSQL_IO_THREADS = "mysql.io.threads";

    /**
     * MySQL backlog size for accept queue (default: 1024)
     */
    public static final String KEY_MYSQL_BACKLOG = "mysql.backlog";

    /**
     * Enable TCP keep-alive for MySQL connections (default: false)
     */
    public static final String KEY_MYSQL_KEEP_ALIVE = "mysql.keep.alive";

    /**
     * Bind to IPv6 address (default: false)
     */
    public static final String KEY_MYSQL_BIND_IPV6 = "mysql.bind.ipv6";

    /**
     * External executor service for MySQL task threads
     */
    public static final String KEY_MYSQL_TASK_EXECUTOR = "mysql.task.executor";

    /**
     * Max task threads for MySQL service
     */
    public static final String KEY_MYSQL_MAX_TASK_THREADS = "mysql.max.task.threads";

    /**
     * MySQL worker name prefix
     */
    public static final String KEY_MYSQL_WORKER_NAME = "mysql.worker.name";

    private final Map<String, Object> properties;

    /**
     * Creates a new protocol configuration.
     */
    public ProtocolConfig() {
        this.properties = new HashMap<>();
    }

    /**
     * Creates a protocol configuration with initial properties.
     *
     * @param properties initial properties
     */
    public ProtocolConfig(Map<String, Object> properties) {
        this.properties = new HashMap<>(properties);
    }

    /**
     * Convenience constructor for common parameters.
     *
     * @param mysqlPort        MySQL port (-1 to disable)
     * @param arrowFlightPort  Arrow Flight port (-1 to disable)
     * @param connectScheduler connection scheduler instance
     */
    public ProtocolConfig(int mysqlPort, int arrowFlightPort, Object connectScheduler) {
        this.properties = new HashMap<>();
        this.properties.put(KEY_MYSQL_PORT, mysqlPort);
        this.properties.put(KEY_ARROWFLIGHT_PORT, arrowFlightPort);
        this.properties.put(KEY_CONNECT_SCHEDULER, connectScheduler);
    }

    /**
     * Sets a configuration property.
     *
     * @param key   property key
     * @param value property value
     * @return this config for chaining
     */
    public ProtocolConfig set(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    /**
     * Gets a configuration property.
     *
     * @param key property key
     * @param <T> value type
     * @return optional value
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(String key) {
        return Optional.ofNullable((T) properties.get(key));
    }

    /**
     * Gets a configuration property with default value.
     *
     * @param key          property key
     * @param defaultValue default value if not found
     * @param <T>          value type
     * @return property value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, T defaultValue) {
        Object value = properties.get(key);
        return value != null ? (T) value : defaultValue;
    }

    /**
     * Gets an integer property.
     *
     * @param key          property key
     * @param defaultValue default value
     * @return integer value
     */
    public int getInt(String key, int defaultValue) {
        Object value = properties.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }

    /**
     * Gets a boolean property.
     *
     * @param key          property key
     * @param defaultValue default value
     * @return boolean value
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Object value = properties.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return defaultValue;
    }

    /**
     * Gets a string property.
     *
     * @param key          property key
     * @param defaultValue default value
     * @return string value
     */
    public String getString(String key, String defaultValue) {
        Object value = properties.get(key);
        if (value != null) {
            return value.toString();
        }
        return defaultValue;
    }

    /**
     * Checks if a property exists.
     *
     * @param key property key
     * @return true if property exists
     */
    public boolean contains(String key) {
        return properties.containsKey(key);
    }

    /**
     * Returns all properties as unmodifiable map.
     *
     * @return properties map
     */
    public Map<String, Object> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Gets the MySQL port.
     *
     * @return MySQL port, or -1 if disabled
     */
    public int getMysqlPort() {
        return getInt(KEY_MYSQL_PORT, -1);
    }

    /**
     * Gets the Arrow Flight port.
     *
     * @return Arrow Flight port, or -1 if disabled
     */
    public int getArrowFlightPort() {
        return getInt(KEY_ARROWFLIGHT_PORT, -1);
    }

    /**
     * Gets the Arrow Flight host.
     *
     * @return host to bind (default "::0")
     */
    public String getArrowFlightHost() {
        return getString(KEY_ARROWFLIGHT_HOST, "::0");
    }

    /**
     * Gets the Arrow Flight token cache size.
     *
     * @return cache size (default 1024)
     */
    public int getArrowFlightTokenCacheSize() {
        return getInt(KEY_ARROWFLIGHT_TOKEN_CACHE_SIZE, 1024);
    }

    /**
     * Gets the Arrow Flight token TTL in minutes.
     *
     * @return TTL (default 60 minutes)
     */
    public int getArrowFlightTokenTtlMinutes() {
        return getInt(KEY_ARROWFLIGHT_TOKEN_TTL_MINUTES, 60);
    }

    /**
     * Gets the Arrow Flight FlightSqlProducer instance.
     *
     * @return producer instance, or null if not set
     */
    public Object getArrowFlightProducer() {
        return properties.get(KEY_ARROWFLIGHT_PRODUCER);
    }

    /**
     * Gets the Arrow Flight CallHeaderAuthenticator instance.
     *
     * @return authenticator instance, or null if not set
     */
    public Object getArrowFlightAuthenticator() {
        return properties.get(KEY_ARROWFLIGHT_AUTHENTICATOR);
    }

    /**
     * Gets the connection scheduler.
     *
     * @param <T> scheduler type
     * @return connection scheduler
     */
    @SuppressWarnings("unchecked")
    public <T> T getConnectScheduler() {
        return (T) properties.get(KEY_CONNECT_SCHEDULER);
    }

    // ==================== MySQL-Specific Convenience Methods ====================

    /**
     * Gets the MySQL IO threads count.
     *
     * @return IO threads count (default: 4)
     */
    public int getMysqlIoThreads() {
        return getInt(KEY_MYSQL_IO_THREADS, 4);
    }

    /**
     * Gets the MySQL backlog size.
     *
     * @return backlog size (default: 1024)
     */
    public int getMysqlBacklog() {
        return getInt(KEY_MYSQL_BACKLOG, 1024);
    }

    /**
     * Checks if TCP keep-alive is enabled for MySQL.
     *
     * @return true if keep-alive is enabled
     */
    public boolean isMysqlKeepAlive() {
        return getBoolean(KEY_MYSQL_KEEP_ALIVE, false);
    }

    /**
     * Checks if MySQL should bind to IPv6 address.
     *
     * @return true if bind to IPv6
     */
    public boolean isMysqlBindIPv6() {
        return getBoolean(KEY_MYSQL_BIND_IPV6, false);
    }

    /**
     * Gets the external executor service for MySQL.
     *
     * @return executor service, or null if not set
     */
    public ExecutorService getMysqlTaskExecutor() {
        Object value = properties.get(KEY_MYSQL_TASK_EXECUTOR);
        return value instanceof ExecutorService ? (ExecutorService) value : null;
    }

    /**
     * Gets the max task threads for MySQL service.
     *
     * @return max task threads (default: 4096)
     */
    public int getMysqlMaxTaskThreads() {
        return getInt(KEY_MYSQL_MAX_TASK_THREADS, 4096);
    }

    /**
     * Gets the MySQL worker name prefix.
     *
     * @return worker name prefix
     */
    public String getMysqlWorkerName() {
        return getString(KEY_MYSQL_WORKER_NAME, "doris-mysql-nio");
    }
}
