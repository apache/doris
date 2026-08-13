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

    /**
     * The registered driver has to be a class of this plugin, not of the driver jar. Paimon calls
     * DriverManager, which only hands back drivers whose class the calling code's classloader can
     * resolve - and the driver jar lives in a classloader below this plugin's, invisible from
     * inside it. Registering the driver directly compiles and works right up until a real Paimon
     * JDBC catalog asks for a connection and is told there is no suitable driver.
     */
    @Test
    public void registersADriverPaimonItselfCanSee() throws Exception {
        Path driverJar = createDriverJar();
        Map<String, String> params = new HashMap<>();
        params.put(PaimonJdbcDriverUtils.PAIMON_JDBC_DRIVER_URL, driverJar.toUri().toURL().toString());
        params.put(PaimonJdbcDriverUtils.PAIMON_JDBC_DRIVER_CLASS, DummyJdbcDriver.class.getName());

        // The platform loader stands in for the plugin's, because what has to hold is that the
        // driver class is NOT resolvable from the caller's side. Passing this test's own loader
        // would let the driver resolve parent-first to the copy sitting on the test classpath, and
        // then registering the driver bare would look just as correct as wrapping it.
        PaimonJdbcDriverUtils.registerDriverIfNeeded(params, ClassLoader.getPlatformClassLoader());

        Driver driver = DriverManager.getDriver("jdbc:dummy:test");
        registeredDrivers.add(driver);
        Assert.assertSame("the registered driver must belong to this plugin, not to the driver jar",
                PaimonJdbcDriverUtils.class.getClassLoader(), driver.getClass().getClassLoader());
        Assert.assertNotSame("...and the driver underneath must be the one from the jar",
                PaimonJdbcDriverUtils.class.getClassLoader(), driverFromJar(driverJar).getClassLoader());
        Assert.assertTrue("and must still be the user's driver underneath",
                driver.acceptsURL("jdbc:dummy:test"));
    }

    /** The driver jar gets its own classloader below the plugin's, so it can see Paimon but not
     * the other way round; the toolkit hands out one per jar. */
    @Test
    public void putsTheDriverJarInAClassloaderOfItsOwn() throws Exception {
        Path driverJar = createDriverJar();
        String url = driverJar.toUri().toURL().toString();
        ClassLoader parent = getClass().getClassLoader();

        ClassLoader driverClassLoader = JdbcDriverUtils.driverClassLoader(url, parent);

        Assert.assertNotSame(parent, driverClassLoader);
        Assert.assertSame(parent, driverClassLoader.getParent());
        Assert.assertSame(driverClassLoader, JdbcDriverUtils.driverClassLoader(url, parent));
    }

    @Test
    public void testRegisterDriverIfNeededRequiresDriverClass() {
        Map<String, String> params = new HashMap<>();
        params.put(PaimonJdbcDriverUtils.PAIMON_JDBC_DRIVER_URL, "file:///tmp/postgresql-42.5.0.jar");

        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class,
                () -> PaimonJdbcDriverUtils.registerDriverIfNeeded(params, getClass().getClassLoader()));
        Assert.assertTrue(exception.getMessage().contains("driver_class"));
    }

    /** The class the jar really holds, which is not the one on this test's classpath. */
    private static Class<?> driverFromJar(Path driverJar) throws Exception {
        return Class.forName(DummyJdbcDriver.class.getName(), false,
                JdbcDriverUtils.driverClassLoader(driverJar.toUri().toURL().toString(),
                        ClassLoader.getPlatformClassLoader()));
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
