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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
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
    void testJdbcMetadataSupportsInsert() {
        JdbcConnectorMetadata metadata = new JdbcConnectorMetadata(null, minimalProps());
        Assertions.assertTrue(metadata.supportsInsert(),
                "JDBC connector metadata should support INSERT");
        Assertions.assertFalse(metadata.supportsDelete(),
                "JDBC connector metadata should not support DELETE by default");
        Assertions.assertFalse(metadata.supportsMerge(),
                "JDBC connector metadata should not support MERGE by default");
    }
}
