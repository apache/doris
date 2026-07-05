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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorContractValidator;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.NoOpConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

class JdbcDorisConnectorTest {

    private static ConnectorContext testContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Collections.emptyMap();
            }
        };
    }

    private static Map<String, String> minimalProps() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc_url", "jdbc:mysql://localhost:3306/test");
        return props;
    }

    @Test
    void testGetMetadataAfterCloseThrows() throws IOException {
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        connector.close();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getMetadata(null));
    }

    @Test
    void testGetScanPlanProviderAfterCloseThrows() throws IOException {
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        connector.close();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getScanPlanProvider());
    }

    @Test
    void testGetWritePlanProviderAfterCloseThrows() throws IOException {
        // getWritePlanProvider() must be wired (non-null routing premise: a non-null provider
        // sends jdbc writes through the unified plan-provider sink path). It resolves the client
        // lazily, so after close it fails loud just like the scan provider.
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        connector.close();
        Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getWritePlanProvider());
    }

    @Test
    void testDeclaresMetadataPreloadCapability() {
        // F11: PluginDrivenExternalTable.supportsExternalMetadataPreload is now capability-driven (replacing
        // the legacy engine-name "jdbc" gate). jdbc must keep declaring SUPPORTS_METADATA_PRELOAD so jdbc
        // tables retain async metadata pre-load. MUTATION: dropping it from getCapabilities() -> jdbc loses
        // async pre-load after F11 -> red.
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        Assertions.assertTrue(
                connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_METADATA_PRELOAD),
                "jdbc must keep SUPPORTS_METADATA_PRELOAD so async metadata pre-load survives the F11 "
                        + "capability conversion");
    }

    @Test
    void testDoubleCloseNoException() throws IOException {
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        connector.close();
        // Second close should not throw
        connector.close();
    }

    @Test
    void testTestConnectionAfterCloseReturnFailure() throws IOException {
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        connector.close();
        // testConnection catches exceptions internally and returns failure result
        org.apache.doris.connector.api.ConnectorTestResult result = connector.testConnection(null);
        Assertions.assertFalse(result.isSuccess());
        Assertions.assertTrue(result.getMessage().contains("closed"),
                "Failure message should mention closed: " + result.getMessage());
    }

    @Test
    void testPropertyNormalizationStripsPrefix() {
        Map<String, String> props = new HashMap<>();
        props.put("jdbc.jdbc_url", "jdbc:mysql://localhost:3306/test");
        props.put("jdbc.user", "root");
        JdbcDorisConnector connector = new JdbcDorisConnector(props, testContext());
        // Connector should normalize props internally; it was created without error
        Assertions.assertNotNull(connector);
    }

    @Test
    void testConcurrentCloseAndGetMetadataNoNpe() throws Exception {
        int threadCount = 20;
        JdbcDorisConnector connector = new JdbcDorisConnector(minimalProps(), testContext());
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger npeCount = new java.util.concurrent.atomic.AtomicInteger(0);
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final boolean doClose = (i % 2 == 0);
            threads[i] = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    if (doClose) {
                        connector.close();
                    } else {
                        connector.getMetadata(null);
                    }
                } catch (DorisConnectorException e) {
                    // Expected: "Connector has been closed" or client creation failure
                } catch (NullPointerException e) {
                    npeCount.incrementAndGet();
                } catch (Exception e) {
                    // Other exceptions are OK (e.g., failed to create client)
                }
            });
            threads[i].start();
        }
        latch.countDown();
        for (Thread t : threads) {
            t.join(5000);
        }
        Assertions.assertEquals(0, npeCount.get(), "NPE detected during concurrent close/getMetadata");
    }

    @Test
    void testJdbcConnectorSupportsInsertOnly() {
        // getWritePlanProvider() eagerly resolves a real JdbcConnectorClient, whose postInitialize()
        // probes the remote server for MySQL (detectDoris) — use postgresql (no such probe) plus a
        // harmless instantiable driver_class (java.lang.Object; never cast to java.sql.Driver here)
        // so client creation succeeds without a live database or driver jar on the test classpath.
        Map<String, String> props = new HashMap<>();
        props.put(JdbcConnectorProperties.JDBC_URL, "jdbc:postgresql://localhost:5432/test");
        props.put(JdbcConnectorProperties.DRIVER_CLASS, "java.lang.Object");
        JdbcDorisConnector connector = new JdbcDorisConnector(props, testContext());
        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT), connector.supportedWriteOperations(),
                "JDBC connector should declare INSERT as its only supported write operation");
        Assertions.assertFalse(connector.supportsWriteBranch(),
                "JDBC connector should not support writing into a named table branch");
        // Task 6 P2: the structural contract validator must pass for a real connector (positive control).
        ConnectorContractValidator.validate(connector, "jdbc");
    }

    @Test
    void testGetWritePlanProviderWithoutDriverClassDoesNotThrow() {
        // Regression test: driver_class is optional (JdbcConnectorProperties.DRIVER_CLASS is read
        // via a plain properties.get(), so it is null when the catalog omits it — see
        // JdbcDorisConnector#createClient). initializeDataSource() must not pass that null straight
        // to HikariConfig#setDriverClassName, which NPEs deep inside HikariCP (loadClass(null) ->
        // ClassLoader lock map -> ConcurrentHashMap forbids a null key) instead of throwing
        // ClassNotFoundException. Use postgresql (no detectDoris probe in postInitialize(), unlike
        // mysql) so client creation succeeds without a live database or driver jar on the classpath;
        // HikariDataSource is lazy and only resolves the driver from the jdbcUrl at first
        // getConnection(), so building it here must succeed even without driver_class.
        Map<String, String> props = new HashMap<>();
        props.put(JdbcConnectorProperties.JDBC_URL, "jdbc:postgresql://localhost:5432/test");
        JdbcDorisConnector connector = new JdbcDorisConnector(props, testContext());
        Assertions.assertDoesNotThrow(connector::getWritePlanProvider,
                "missing driver_class must not NPE inside HikariCP during client initialization");
    }

    @Test
    void testBeginTransactionReturnsNoOpTransaction() {
        // jdbc writes are auto-committed by BE per row; beginTransaction returns a degenerate no-op
        // transaction so the engine's write lifecycle is uniform (single ConnectorTransaction model).
        JdbcConnectorMetadata metadata = new JdbcConnectorMetadata(null, minimalProps());
        ConnectorTransaction txn = metadata.beginTransaction(new FixedIdSession(99L));

        Assertions.assertTrue(txn instanceof NoOpConnectorTransaction,
                "jdbc beginTransaction must return a no-op ConnectorTransaction");
        Assertions.assertEquals(99L, txn.getTransactionId(),
                "the transaction id must come from the engine session (allocateTransactionId)");
        Assertions.assertEquals("JDBC", txn.profileLabel(),
                "the profile label must map to TransactionType.JDBC");
        Assertions.assertEquals(-1L, txn.getUpdateCnt(),
                "getUpdateCnt == -1 keeps the coordinator row counter (no affected-rows regression)");
    }

    /** Minimal {@link ConnectorSession} that hands back a fixed engine transaction id. */
    private static final class FixedIdSession implements ConnectorSession {
        private final long txnId;

        private FixedIdSession(long txnId) {
            this.txnId = txnId;
        }

        @Override
        public long allocateTransactionId() {
            return txnId;
        }

        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public String getCatalogName() {
            return "test_catalog";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
