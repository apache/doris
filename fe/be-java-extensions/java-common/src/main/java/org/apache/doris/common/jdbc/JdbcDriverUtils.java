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

package org.apache.doris.common.jdbc;

import org.apache.doris.common.classloader.JniScannerClassLoader;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class JdbcDriverUtils {
    private static final int HTTP_TIMEOUT_MS = 10000;
    private static final ConcurrentHashMap<URL, ClassLoader> DRIVER_CLASS_LOADER_CACHE = new ConcurrentHashMap<>();
    private static final Set<String> REGISTERED_DRIVER_KEYS = ConcurrentHashMap.newKeySet();

    private JdbcDriverUtils() {
    }

    public static void registerDriver(String driverUrl, String driverClassName, ClassLoader classLoader) {
        registerDriver(driverUrl, driverClassName, "", classLoader);
    }

    public static void registerDriver(String driverUrl, String driverClassName, String expectedChecksum,
            ClassLoader classLoader) {
        if (StringUtils.isBlank(driverClassName)) {
            throw new IllegalArgumentException("JDBC driver class is required when driver url is specified.");
        }
        if (StringUtils.isNotBlank(expectedChecksum)) {
            validateDriverChecksum(driverUrl, expectedChecksum);
        }

        try {
            URL url = new URL(driverUrl);
            String driverKey = driverUrl + "#" + driverClassName;
            if (!REGISTERED_DRIVER_KEYS.add(driverKey)) {
                return;
            }
            try {
                ClassLoader driverClassLoader = prepareDriverClassLoader(url, classLoader);
                Class<?> loadedDriverClass = Class.forName(driverClassName, true, driverClassLoader);
                Driver driver = (Driver) loadedDriverClass.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(new DriverShim(driver));
            } catch (ClassNotFoundException e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new IllegalArgumentException("Failed to load JDBC driver class: " + driverClassName, e);
            } catch (Exception e) {
                REGISTERED_DRIVER_KEYS.remove(driverKey);
                throw new RuntimeException("Failed to register JDBC driver: " + driverClassName, e);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid JDBC driver URL: " + driverUrl, e);
        }
    }

    public static String validateDriverChecksum(String driverUrl, String expectedChecksum) {
        String actualChecksum = computeObjectChecksum(driverUrl);
        if (StringUtils.isNotBlank(expectedChecksum) && !expectedChecksum.equals(actualChecksum)) {
            throw new IllegalArgumentException("Checksum mismatch for JDBC driver. expected: "
                    + expectedChecksum + ", actual: " + actualChecksum);
        }
        return actualChecksum;
    }

    public static String computeObjectChecksum(String urlStr) {
        try (InputStream inputStream = getInputStreamFromUrl(urlStr, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS)) {
            // Keep the checksum format compatible with FE JdbcResource.
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buf)) != -1) {
                digest.update(buf, 0, bytesRead);
            }
            return Hex.encodeHexString(digest.digest());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Compute driver checksum from url: " + urlStr
                    + " encountered an error: " + e.getMessage(), e);
        }
    }

    private static InputStream getInputStreamFromUrl(String urlStr, int connectTimeoutMs, int readTimeoutMs)
            throws IOException {
        try {
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            return conn.getInputStream();
        } catch (Exception e) {
            throw new IOException("Failed to open URL connection: " + urlStr, e);
        }
    }

    private static ClassLoader prepareDriverClassLoader(URL driverUrl, ClassLoader classLoader) {
        if (classLoader instanceof JniScannerClassLoader) {
            JniScannerClassLoader scannerClassLoader = (JniScannerClassLoader) classLoader;
            scannerClassLoader.addURLIfAbsent(driverUrl);
            return scannerClassLoader;
        }
        return DRIVER_CLASS_LOADER_CACHE.computeIfAbsent(driverUrl,
                url -> URLClassLoader.newInstance(new URL[] {url}, classLoader));
    }

    private static final class DriverShim implements Driver {
        private final Driver delegate;

        private DriverShim(Driver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection connect(String url, java.util.Properties info) throws java.sql.SQLException {
            return delegate.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws java.sql.SQLException {
            return delegate.acceptsURL(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info)
                throws java.sql.SQLException {
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
