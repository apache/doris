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

package org.apache.doris.paimon;

import org.apache.doris.jni.toolkit.jdbc.JdbcDriverUtils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Makes a catalog's JDBC driver usable by Paimon, whose JDBC catalog asks
 * {@link DriverManager} for connections.
 *
 * <p>The driver jar is named by the catalog and downloaded at query time, so it gets a classloader
 * of its own - a child of this plugin's, built by the toolkit. Paimon therefore cannot see the
 * driver class, and that matters: {@code DriverManager} only hands a caller a driver whose class
 * the <em>caller's</em> classloader can resolve to the same class object. Registering the driver
 * wrapped in {@link DriverShim}, a class of this plugin, is what satisfies that check - the shim is
 * exactly as visible to Paimon as Paimon is to itself.
 */
final class PaimonJdbcDriverUtils {
    static final String PAIMON_JDBC_DRIVER_URL = "paimon.jdbc.driver_url";
    static final String PAIMON_JDBC_DRIVER_CLASS = "paimon.jdbc.driver_class";
    static final String JDBC_DRIVER_URL = "jdbc.driver_url";
    static final String JDBC_DRIVER_CLASS = "jdbc.driver_class";

    /** One registration per driver: DriverManager keeps every driver ever registered, forever. */
    private static final Set<String> REGISTERED = ConcurrentHashMap.newKeySet();

    /**
     * One lock per driver, so that registering two different drivers does not serialize. Same shape
     * as {@code JdbcDriverUtils.driverClassLoader}, and for the same reason: loading the driver
     * class reads a jar, which must not happen inside a {@code ConcurrentHashMap} bin lock.
     */
    private static final ConcurrentHashMap<String, Object> REGISTRATION_LOCKS = new ConcurrentHashMap<>();

    private PaimonJdbcDriverUtils() {
    }

    static void registerDriverIfNeeded(Map<String, String> params, ClassLoader parentClassLoader) {
        String driverUrl = firstNonBlank(params.get(PAIMON_JDBC_DRIVER_URL), params.get(JDBC_DRIVER_URL));
        if (driverUrl == null) {
            return;
        }
        String driverClassName = firstNonBlank(params.get(PAIMON_JDBC_DRIVER_CLASS), params.get(JDBC_DRIVER_CLASS));
        if (driverClassName == null) {
            throw new IllegalArgumentException("paimon.jdbc.driver_class or jdbc.driver_class is required when "
                    + "paimon.jdbc.driver_url or jdbc.driver_url is specified");
        }
        registerDriver(driverUrl, driverClassName, parentClassLoader);
    }

    static void registerDriver(String driverUrl, String driverClassName, ClassLoader parentClassLoader) {
        String key = driverUrl + "#" + driverClassName;
        if (REGISTERED.contains(key)) {
            return;
        }
        // Published only once the driver really is registered. The set used to be claimed up front,
        // which made "somebody is registering this" and "this is registered" the same state:
        // PaimonJniScanner calls per scanner, the scanners of one query's splits run concurrently,
        // and the second thread returned here immediately and asked DriverManager for a connection
        // while the first was still loading the driver class - "No suitable driver".
        synchronized (REGISTRATION_LOCKS.computeIfAbsent(key, entry -> new Object())) {
            if (REGISTERED.contains(key)) {
                return;
            }
            try {
                ClassLoader driverClassLoader = JdbcDriverUtils.driverClassLoader(driverUrl, parentClassLoader);
                Class<?> driverClass = Class.forName(driverClassName, true, driverClassLoader);
                Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(new DriverShim(driver));
            } catch (Exception e) {
                // Nothing to undo: a failed registration was never published, so the catalog can be
                // fixed and retried without restarting BE.
                throw new RuntimeException("Failed to register JDBC driver: " + driverClassName, e);
            }
            REGISTERED.add(key);
        }
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first;
        }
        if (second != null && !second.trim().isEmpty()) {
            return second;
        }
        return null;
    }

    /** A driver of this plugin's, delegating to one this plugin cannot name. */
    private static final class DriverShim implements Driver {
        private final Driver delegate;

        private DriverShim(Driver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection connect(String url, Properties info) throws SQLException {
            return delegate.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return delegate.acceptsURL(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return delegate.getPropertyInfo(url, info);
        }

        @Override
        public int getMajorVersion() {
            return delegate.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return delegate.getMinorVersion();
        }

        @Override
        public boolean jdbcCompliant() {
            return delegate.jdbcCompliant();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }
    }
}
