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
 * @since 2.0.0
 */
public class ProtocolConfig {
    
    /** Standard key for MySQL port */
    public static final String KEY_MYSQL_PORT = "mysql.port";
    
    /** Standard key for Arrow Flight port */
    public static final String KEY_ARROWFLIGHT_PORT = "arrowflight.port";
    
    /** Standard key for SSL enabled flag */
    public static final String KEY_SSL_ENABLED = "ssl.enabled";
    
    /** Standard key for max connections */
    public static final String KEY_MAX_CONNECTIONS = "max.connections";
    
    /** Standard key for connection scheduler */
    public static final String KEY_CONNECT_SCHEDULER = "connect.scheduler";
    
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
     * @param mysqlPort MySQL port (-1 to disable)
     * @param arrowFlightPort Arrow Flight port (-1 to disable)
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
     * @param key property key
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
     * @param key property key
     * @param defaultValue default value if not found
     * @param <T> value type
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
     * @param key property key
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
     * @param key property key
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
     * @param key property key
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
     * Gets the connection scheduler.
     * 
     * @param <T> scheduler type
     * @return connection scheduler
     */
    @SuppressWarnings("unchecked")
    public <T> T getConnectScheduler() {
        return (T) properties.get(KEY_CONNECT_SCHEDULER);
    }
}
