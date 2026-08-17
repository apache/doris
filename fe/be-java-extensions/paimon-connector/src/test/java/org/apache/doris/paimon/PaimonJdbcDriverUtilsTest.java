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

import org.apache.doris.common.classloader.JniScannerClassLoader;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.logging.Logger;

public class PaimonJdbcDriverUtilsTest {
    private final List<Driver> registeredDrivers = new ArrayList<>();
    private final List<Path> tempJars = new ArrayList<>();

    @After
    public void tearDown() throws Exception {
        for (Driver driver : registeredDrivers) {
            DriverManager.deregisterDriver(driver);
        }
        registeredDrivers.clear();
        for (Path tempJar : tempJars) {
            Files.deleteIfExists(tempJar);
        }
        tempJars.clear();
    }

    @Test
    public void testRegisterDriverIfNeeded() throws Exception {
        Path driverJar = createDriverJar();
        Map<String, String> params = new HashMap<>();
        params.put(PaimonJdbcDriverUtils.PAIMON_JDBC_DRIVER_URL, driverJar.toUri().toURL().toString());
        params.put(PaimonJdbcDriverUtils.PAIMON_JDBC_DRIVER_CLASS, DummyJdbcDriver.class.getName());

        JniScannerClassLoader scannerClassLoader =
                new JniScannerClassLoader("paimon-test", List.of(), ClassLoader.getPlatformClassLoader());
        PaimonJdbcDriverUtils.registerDriverIfNeeded(params, scannerClassLoader);

        Driver driver = DriverManager.getDriver("jdbc:dummy:test");
        registeredDrivers.add(driver);
        Assert.assertTrue(driver.acceptsURL("jdbc:dummy:test"));
    }

    @Test
    public void testRegisterDriverIfNeededRequiresDriverClass() {
        Map<String, String> params = new HashMap<>();
        params.put(PaimonJdbcDriverUtils.PAIMON_JDBC_DRIVER_URL, "file:///tmp/postgresql-42.5.0.jar");

        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonJdbcDriverUtils.registerDriverIfNeeded(params, getClass().getClassLoader()));
        Assert.assertTrue(exception.getMessage().contains("driver_class"));
    }

    private Path createDriverJar() throws IOException {
        Path jarPath = Files.createTempFile("paimon-jdbc-driver", ".jar");
        tempJars.add(jarPath);
        String resourceName = DummyJdbcDriver.class.getName().replace('.', '/') + ".class";
        try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarPath));
                InputStream inputStream = DummyJdbcDriver.class.getClassLoader().getResourceAsStream(resourceName)) {
            Assert.assertNotNull(inputStream);
            jarOutputStream.putNextEntry(new JarEntry(resourceName));
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) >= 0) {
                jarOutputStream.write(buffer, 0, bytesRead);
            }
            jarOutputStream.closeEntry();
        }
        return jarPath;
    }

    public static class DummyJdbcDriver implements Driver {
        @Override
        public java.sql.Connection connect(String url, Properties info) {
            return null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:dummy:");
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
