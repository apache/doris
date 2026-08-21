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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
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

public class JdbcDriverLoader {
    private static final Logger LOG = LogManager.getLogger(JdbcDriverLoader.class);
    private static final Map<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_DRIVER_KEYS = ConcurrentHashMap.newKeySet();

    private JdbcDriverLoader() {
    }

    public static String validateDriverChecksum(String driverUrl, String expectedChecksum) throws DdlException {
        String actualChecksum = JdbcResource.computeObjectChecksum(driverUrl);
        if (StringUtils.isNotBlank(expectedChecksum) && !expectedChecksum.equals(actualChecksum)) {
            throw new DdlException("The provided checksum (" + expectedChecksum
                    + ") does not match the computed checksum (" + actualChecksum
                    + ") for the driver_url.");
        }
        return actualChecksum;
    }

    public static String registerDriver(String driverUrl, String driverClassName, String expectedChecksum,
            ClassLoader parentClassLoader) {
        if (StringUtils.isBlank(driverClassName)) {
            throw new IllegalArgumentException("JDBC driver class is required when driver url is specified.");
        }

        try {
            String fullDriverUrl = JdbcResource.getFullDriverUrl(driverUrl);
            try {
                validateDriverChecksum(fullDriverUrl, expectedChecksum);
            } catch (DdlException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
            URL url = new URL(fullDriverUrl);
            String driverKey = fullDriverUrl + "#" + driverClassName;
            if (!REGISTERED_DRIVER_KEYS.add(driverKey)) {
                LOG.info("JDBC driver already registered: {} from {}", driverClassName, fullDriverUrl);
                return fullDriverUrl;
            }
            try {
                ClassLoader classLoader = DRIVER_CLASS_LOADER_CACHE.computeIfAbsent(url, u ->
                        URLClassLoader.newInstance(new URL[] {u}, parentClassLoader));
                Class<?> loadedDriverClass = Class.forName(driverClassName, true, classLoader);
                Driver driver = (Driver) loadedDriverClass.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(new DriverShim(driver));
                LOG.info("Successfully registered JDBC driver: {} from {}", driverClassName, fullDriverUrl);
                return fullDriverUrl;
            } catch (ClassNotFoundException e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new IllegalArgumentException("Failed to load JDBC driver class: " + driverClassName, e);
            } catch (Exception e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new RuntimeException("Failed to register JDBC driver: " + driverClassName, e);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid driver URL: " + driverUrl, e);
        }
    }

    private static class DriverShim implements Driver {
        private final Driver delegate;

        DriverShim(Driver delegate) {
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
