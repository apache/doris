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

package org.apache.doris.regression.suite

import org.apache.doris.regression.Config
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy
import java.nio.file.Path
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLFeatureNotSupportedException
import java.util.logging.Logger

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertSame
import static org.junit.jupiter.api.Assertions.assertTrue

class SuiteContextConnectTest {
    private static RecordingDriver driver

    @TempDir
    Path tempDir

    @BeforeAll
    static void registerDriver() {
        driver = new RecordingDriver()
        DriverManager.registerDriver(driver)
    }

    @AfterAll
    static void deregisterDriver() {
        DriverManager.deregisterDriver(driver)
    }

    @Test
    void connectUsesProvidedExternalUrlWhenTlsEnabled() {
        File suiteDir = tempDir.resolve("suites/external").toFile()
        assertTrue(suiteDir.mkdirs())
        File suiteFile = new File(suiteDir, "test_external.groovy")
        assertTrue(suiteFile.createNewFile())

        Config config = new Config()
        config.suitePath = tempDir.resolve("suites").toString()
        config.dataPath = tempDir.resolve("data").toString()
        config.realDataPath = tempDir.resolve("real-data").toString()
        config.defaultDb = "regression_test"
        config.otherConfigs.put("enableTLS", "true")

        ScriptContext scriptContext = new ScriptContext(
                suiteFile, null, null, config, Collections.emptyList(), { true })
        SuiteContext context = new SuiteContext(
                suiteFile, "test_external", "nondatalake", scriptContext,
                new SuiteCluster("test_external", config), null, null, config)

        String externalUrl = "jdbc:recording:external"
        boolean closureCalled = false
        String result = context.connect("external_user", "external_password", externalUrl) {
            assertSame(driver.connection, context.threadLocalConn.get().conn)
            closureCalled = true
            return "connected"
        }

        assertEquals("connected", result)
        assertTrue(closureCalled)
        assertEquals(externalUrl, driver.url)
        assertTrue(driver.closed)
        assertNull(context.threadLocalConn.get())
    }

    private static class RecordingDriver implements Driver {
        String url
        Connection connection
        boolean closed

        @Override
        Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null
            }
            this.url = url
            this.closed = false
            this.connection = Proxy.newProxyInstance(
                    Connection.class.classLoader,
                    [Connection.class] as Class<?>[],
                    { Object proxy, java.lang.reflect.Method method, Object[] args ->
                        if (method.name == "close") {
                            closed = true
                            return null
                        }
                        if (method.name == "isClosed") {
                            return closed
                        }
                        return null
                    } as InvocationHandler) as Connection
            return connection
        }

        @Override
        boolean acceptsURL(String url) {
            return url.startsWith("jdbc:recording:")
        }

        @Override
        DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0]
        }

        @Override
        int getMajorVersion() {
            return 1
        }

        @Override
        int getMinorVersion() {
            return 0
        }

        @Override
        boolean jdbcCompliant() {
            return false
        }

        @Override
        Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return Logger.getLogger(RecordingDriver.name)
        }
    }
}
