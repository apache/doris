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

import ch.qos.logback.classic.Logger as LogbackLogger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.mysql.cj.conf.ConnectionUrl
import org.apache.doris.regression.Config
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory

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
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertNull
import static org.junit.jupiter.api.Assertions.assertSame
import static org.junit.jupiter.api.Assertions.assertThrows
import static org.junit.jupiter.api.Assertions.assertTrue

class SuiteContextConnectTest {
    private static RecordingDriver driver

    @TempDir
    Path tempDir

    @BeforeAll
    static void registerDriver() {
        driver = new RecordingDriver()
        // Record MySQL connections before Connector/J can try to open a real socket.
        List<Driver> existingDrivers = Collections.list(DriverManager.getDrivers())
        existingDrivers.each { DriverManager.deregisterDriver(it) }
        DriverManager.registerDriver(driver)
        existingDrivers.each { DriverManager.registerDriver(it) }
    }

    @AfterAll
    static void deregisterDriver() {
        DriverManager.deregisterDriver(driver)
    }

    @Test
    void connectUsesProvidedExternalUrlWhenTlsEnabled() {
        SuiteContext context = createContext()

        ["jdbc:recording:external", "jdbc:mysql://external:3306/db?useSSL=false"].each { externalUrl ->
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
    }

    @Test
    void dorisConnectionsKeepTargetAndJdbcOptionsWhileAddingMtls() {
        SuiteContext context = createContext()
        List<String> urls = [
                "jdbc:mysql://master:9030/",
                "jdbc:mysql://master:9030/information_schema?",
                "jdbc:mysql://master:9030",
                "jdbc:mysql://observer:9030/",
                "jdbc:mysql://master:9030/point_query?&useServerPrepStmts=true",
                "jdbc:mysql://master:9030/insert_test?useLocalSessionState=true&rewriteBatchedStatements=true&allowMultiQueries=false",
                "jdbc:mysql://master:9030/multi_stmt?useLocalSessionState=false&allowMultiQueries=true",
                "jdbc:mysql://master:9030/single_stmt?useLocalSessionState=false",
                "jdbc:mysql://[::1]:9030/db?connectTimeout=1234&socketTimeout=5678"
        ]
        urls.each { url ->
            context.connectToDoris("case_user", "case_password", url) {
                assertSame(driver.connection, context.threadLocalConn.get().conn)
                assertEquals("case_user", context.threadLocalConn.get().username)
                assertEquals("case_password", context.threadLocalConn.get().password)
            }
            def original = ConnectionUrl.getConnectionUrlInstance(url, null).mainHost
            def actual = ConnectionUrl.getConnectionUrlInstance(driver.url, driver.properties).mainHost
            assertEquals(original.host, actual.host)
            assertEquals(original.port, actual.port)
            assertEquals(original.database, actual.database)
            original.hostProperties.each { key, value -> assertEquals(value, actual.hostProperties.get(key)) }
            assertEquals("case_user", actual.user)
            assertEquals("case_password", actual.password)
            assertEquals("true", actual.hostProperties.get("useSSL"))
            assertEquals("true", actual.hostProperties.get("requireSSL"))
            assertEquals("true", actual.hostProperties.get("verifyServerCertificate"))
            assertEquals("file:/test/client.p12", actual.hostProperties.get("clientCertificateKeyStoreUrl"))
            assertEquals("client_store_password", actual.hostProperties.get("clientCertificateKeyStorePassword"))
            assertEquals("file:/test/trust.p12", actual.hostProperties.get("trustCertificateKeyStoreUrl"))
            assertEquals("trust_store_password", actual.hostProperties.get("trustCertificateKeyStorePassword"))
            assertTrue(driver.closed)
            assertNull(context.threadLocalConn.get())
        }
    }

    @Test
    void dorisConnectionWithoutTlsKeepsUrlUnchanged() {
        SuiteContext context = createContext()
        context.config.otherConfigs.put("enableTLS", "false")
        String url = "jdbc:mysql://master:9030/db?useServerPrepStmts=true"
        context.connectToDoris("user", "password", url) { }
        assertEquals(url, driver.url)
    }

    @Test
    void dorisConnectionOverridesLegacyOptionalSslSettings() {
        SuiteContext context = createContext()
        String url = "jdbc:mysql://master:9030/db?useSSL=true&requireSSL=false&verifyServerCertificate=false"
        context.connectToDoris("user", "password", url) { }
        def properties = ConnectionUrl.getConnectionUrlInstance(driver.url, driver.properties).mainHost.hostProperties
        assertEquals("true", properties.get("requireSSL"))
        assertEquals("true", properties.get("verifyServerCertificate"))
        assertEquals("file:/test/client.p12", properties.get("clientCertificateKeyStoreUrl"))
    }

    @Test
    void genericConnectPreservesExplicitTlsNegativeTest() {
        SuiteContext context = createContext()
        String url = "jdbc:mysql://master:9030/?sslMode=VERIFY_CA&trustCertificateKeyStoreUrl=file:/test/other-ca.p12"
        context.connect("user", "password", url) { }
        assertEquals(url, driver.url)
        assertNull(ConnectionUrl.getConnectionUrlInstance(driver.url, driver.properties)
                .mainHost.hostProperties.get("clientCertificateKeyStoreUrl"))
    }

    @Test
    void dorisConnectionRestoresOriginalContextAfterClosureFailure() {
        SuiteContext context = createContext()
        ConnectionInfo original = new ConnectionInfo(username: "original_user", password: "original_password")
        context.threadLocalConn.set(original)
        assertThrows(IllegalStateException) {
            context.connectToDoris("case_user", "case_password", "jdbc:mysql://observer:9030/") {
                throw new IllegalStateException("case failed")
            }
        }
        assertTrue(driver.closed)
        assertSame(original, context.threadLocalConn.get())
    }

    @Test
    void suiteDorisConnectionSupportsDefaultUrl() {
        SuiteContext context = createContext()
        context.config.jdbcUrl = "jdbc:mysql://master:9030/default_db?useLocalSessionState=true"
        Suite suite = new Suite("test_external", "nondatalake", context, context.cluster)
        assertEquals("connected", suite.connectToDoris("case_user", "case_password") { "connected" })
        assertEquals("default_db", ConnectionUrl.getConnectionUrlInstance(driver.url, driver.properties).mainHost.database)
        assertTrue(driver.closed)
    }

    @Test
    void connectionUrlLoggingRedactsCredentials() {
        String url = "jdbc:mysql://alice:login-secret@master:9030/db?useSSL=true" +
                "&clientCertificateKeyStorePassword=client-secret" +
                "&trustCertificateKeyStorePassword=trust-secret&authToken=auth-secret&connectTimeout=1234"
        assertEquals("jdbc:mysql://***@master:9030/db?useSSL=true" +
                "&clientCertificateKeyStorePassword=***" +
                "&trustCertificateKeyStorePassword=***&authToken=***&connectTimeout=1234",
                Config.sanitizeJdbcUrlForLogging(url))
        assertNull(Config.sanitizeJdbcUrlForLogging(null))
    }

    @Test
    void tlsConnectionLogsDoNotExposeKeyStorePasswords() {
        SuiteContext context = createContext()
        LogbackLogger logger = (LogbackLogger) LoggerFactory.getLogger(SuiteContext)
        ListAppender<ILoggingEvent> appender = new ListAppender<>()
        appender.start()
        logger.addAppender(appender)
        try {
            context.connectToDoris("case_user", "case_password", "jdbc:mysql://master:9030/db") { }
            String messages = appender.list.collect { it.formattedMessage }.join('\n')
            assertFalse(messages.contains("client_store_password"))
            assertFalse(messages.contains("trust_store_password"))
            assertTrue(messages.contains("clientCertificateKeyStorePassword=***"))
            assertTrue(messages.contains("trustCertificateKeyStorePassword=***"))
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    void defaultDatabaseResetLogDoesNotExposeKeyStorePasswords() {
        Config config = createContext().config
        config.jdbcUrl = "jdbc:mysql://master:9030/"
        config.defaultDb = "regression_test"
        config.dryRun = true
        LogbackLogger logger = (LogbackLogger) LoggerFactory.getLogger(Config)
        ListAppender<ILoggingEvent> appender = new ListAppender<>()
        appender.start()
        logger.addAppender(appender)
        try {
            config.createDefaultDb()
            String messages = appender.list.collect { it.formattedMessage }.join('\n')
            assertTrue(config.jdbcUrl.contains("client_store_password"))
            assertTrue(config.jdbcUrl.contains("trust_store_password"))
            assertFalse(messages.contains("client_store_password"))
            assertFalse(messages.contains("trust_store_password"))
            assertTrue(messages.contains("clientCertificateKeyStorePassword=***"))
            assertTrue(messages.contains("trustCertificateKeyStorePassword=***"))
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    private SuiteContext createContext() {
        File suiteDir = tempDir.resolve("suites/external").toFile()
        assertTrue(suiteDir.mkdirs())
        File suiteFile = new File(suiteDir, "test_external.groovy")
        assertTrue(suiteFile.createNewFile())

        Config config = new Config()
        config.suitePath = tempDir.resolve("suites").toString()
        config.dataPath = tempDir.resolve("data").toString()
        config.realDataPath = tempDir.resolve("real-data").toString()
        config.defaultDb = "regression_test"
        config.otherConfigs.put("tlsVerifyMode", "none")
        ScriptContext scriptContext = new ScriptContext(
                suiteFile, null, null, config, Collections.emptyList(), { true })
        SuiteContext context = new SuiteContext(
                suiteFile, "test_external", "nondatalake", scriptContext,
                new SuiteCluster("test_external", config), null, null, config)
        config.otherConfigs.put("enableTLS", "true")
        config.otherConfigs.put("keyStorePath", "/test/client.p12")
        config.otherConfigs.put("keyStorePassword", "client_store_password")
        config.otherConfigs.put("trustStorePath", "/test/trust.p12")
        config.otherConfigs.put("trustStorePassword", "trust_store_password")
        return context
    }

    private static class RecordingDriver implements Driver {
        String url
        Properties properties
        Connection connection
        boolean closed

        @Override
        Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null
            }
            this.url = url
            this.properties = new Properties()
            this.properties.putAll(info)
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
            return url.startsWith("jdbc:recording:") || url.startsWith("jdbc:mysql:")
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
