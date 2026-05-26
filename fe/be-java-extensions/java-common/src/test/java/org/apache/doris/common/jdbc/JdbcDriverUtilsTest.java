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

import org.junit.Assert;
import org.junit.After;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcDriverUtilsTest {
    private static final String DRIVER_BYTES = "doris-jdbc-driver";
    private static final String DRIVER_CHECKSUM = "ef2b3218a863c98bc78fdf447331b206";
    private static final String DUMMY_JDBC_URL = "jdbc:doris-common-dummy:test";

    @After
    public void tearDown() throws Exception {
        deregisterDummyDrivers();
    }

    @Test
    public void testValidateDriverChecksumRejectsMismatch() throws Exception {
        runWithDriverUrl(driverUrl -> {
            IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                    () -> JdbcDriverUtils.validateDriverChecksum(driverUrl, "bad-checksum"));
            Assert.assertTrue(exception.getMessage().contains("Checksum mismatch for JDBC driver"));
        });
    }

    @Test
    public void testValidateDriverChecksumReturnsComputedChecksum() throws Exception {
        runWithDriverUrl(driverUrl -> Assert.assertEquals(DRIVER_CHECKSUM,
                JdbcDriverUtils.validateDriverChecksum(driverUrl, DRIVER_CHECKSUM)));
    }

    @Test
    public void testRegisterDriverWithoutChecksumKeepsOriginalLoadingBehavior() throws Exception {
        String missingDriverUrl = "file:///tmp/doris-missing-driver-" + System.nanoTime() + ".jar";

        JdbcDriverUtils.registerDriver(missingDriverUrl, DummyJdbcDriver.class.getName(), getClass().getClassLoader());

        Driver driver = DriverManager.getDriver(DUMMY_JDBC_URL);
        Assert.assertTrue(driver.acceptsURL(DUMMY_JDBC_URL));
    }

    private void runWithDriverUrl(ThrowingConsumer<String> consumer) throws Exception {
        Path driverPath = Files.createTempFile("doris-jdbc-driver", ".jar");
        try {
            Files.write(driverPath, DRIVER_BYTES.getBytes(StandardCharsets.UTF_8));
            consumer.accept(driverPath.toUri().toString());
        } finally {
            Files.deleteIfExists(driverPath);
        }
    }

    private interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }

    private void deregisterDummyDrivers() throws Exception {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.acceptsURL(DUMMY_JDBC_URL)) {
                DriverManager.deregisterDriver(driver);
            }
        }
    }

    public static class DummyJdbcDriver implements Driver {
        @Override
        public java.sql.Connection connect(String url, Properties info) {
            return null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return DUMMY_JDBC_URL.equals(url);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException("not supported");
        }
    }
}
