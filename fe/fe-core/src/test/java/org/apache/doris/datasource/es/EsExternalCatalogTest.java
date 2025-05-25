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

package org.apache.doris.datasource.es;

import org.apache.doris.catalog.EsResource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EsExternalCatalogTest {

    @Mock
    private EsRestClient mockEsRestClient;

    @Mock
    private EsRestClient mockEsRestClientForNode;

    private EsExternalCatalog catalog;
    private EsNodeInfo node1;
    private EsNodeInfo node2;
    private final String testHost = "http://localhost:";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        // Setup test catalog
        Map<String, String> props = new HashMap<>();
        props.put(EsResource.HOSTS, "http://localhost:9200");
        catalog = new EsExternalCatalog(1L, "test_catalog", "", props, "") {
            @Override
            protected EsRestClient createEsRestClient(String[] nodes, String username, String password, boolean enableSsl) {
                return mockEsRestClientForNode;
            }
        };

        // Setup test nodes
        String addr1 = testHost + "9200";
        String addr2 = testHost + "9201";
        node1 = new EsNodeInfo("node1", addr1);
        node2 = new EsNodeInfo("node2", addr2);

        // Use setter method to set mock client
        catalog.setEsRestClient(mockEsRestClient);
    }

    @Test
    public void testDetectAvailableNodesInfo() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health()).thenReturn(true);

        // Execute
        catalog.detectAvailableNodesInfo();

        // Verify
        assertEquals(2, catalog.getAvailableNodesInfo().size());
        assertTrue(catalog.getAvailableNodesInfo().contains(node1));
        assertTrue(catalog.getAvailableNodesInfo().contains(node2));
    }

    @Test
    public void testDetectAvailableNodesInfoWithFailures() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health())
            .thenReturn(true)  // node1 healthy
                .thenReturn(false); // node2 failed

        // Execute
        catalog.detectAvailableNodesInfo();

        // Verify
        assertEquals(1, catalog.getAvailableNodesInfo().size());
        assertTrue(catalog.getAvailableNodesInfo().contains(node1));
    }

    @Test
    public void testGetAvailableNodesInfoEmpty() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Collections.emptyList());

        // Execute
        Set<EsNodeInfo> nodes = catalog.getAvailableNodesInfo();

        // Verify
        assertTrue(nodes.isEmpty());
    }

    @Test
    public void testClearAvailableNodesInfo() throws IllegalAccessException {
        // Setup
        EsExternalCatalog spyCatalog = spy(catalog);
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health()).thenReturn(true);
        spyCatalog.detectAvailableNodesInfo();

        // Mock getAvailableNodesInfo to return actual set without detect
        // Use reflection to access the private field
        Field availableNodesInfoField = null;
        try {
            availableNodesInfoField = EsExternalCatalog.class.getDeclaredField("availableNodesInfo");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        availableNodesInfoField.setAccessible(true);

        // Verify initial state
        assertEquals(2, ((Set<?>) availableNodesInfoField.get(spyCatalog)).size());

        // Execute
        spyCatalog.clearAvailableNodesInfo();

        // Verify
        assertTrue(((Set<?>) availableNodesInfoField.get(spyCatalog)).isEmpty());
    }

    @Test
    public void testGetAvailableNodesInfoWithEmptyCache() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health()).thenReturn(true);

        // Execute - should trigger detect
        Set<EsNodeInfo> nodes = catalog.getAvailableNodesInfo();

        // Verify
        assertEquals(2, nodes.size());
        verify(mockEsRestClient, times(1)).getHttpNodesList();
        verify(mockEsRestClientForNode, times(2)).health(); // Ensure health check is called for each node
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health()).thenReturn(true);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        // Execute concurrent operations
        for (int i = 0; i < threadCount; i++) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        catalog.detectAvailableNodesInfo();
                        catalog.getAvailableNodesInfo();
                        catalog.clearAvailableNodesInfo();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        // Wait for completion
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Verify no exceptions were thrown
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    }

    @Test
    public void testGetAvailableNodesInfoWhenDetectThrowsException() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenThrow(new RuntimeException("Test exception"));

        // Execute
        Set<EsNodeInfo> nodes = catalog.getAvailableNodesInfo();

        // Verify
        assertTrue(nodes.isEmpty());
        verify(mockEsRestClient, times(1)).getHttpNodesList();
    }

    @Test
    public void testDetectAvailableNodesInfoWhenGetNodesListReturnsNull() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(null);

        // Execute
        catalog.detectAvailableNodesInfo();

        // Verify
        assertTrue(catalog.getAvailableNodesInfo().isEmpty());
    }

    @Test
    public void testDetectAvailableNodesInfoWhenHealthThrowsException() {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health()).thenThrow(new RuntimeException("Health check failed"));

        // Execute
        catalog.detectAvailableNodesInfo();

        // Verify
        assertTrue(catalog.getAvailableNodesInfo().isEmpty());
    }

    @Test
    public void testDetectAvailableNodesInfoWithInvalidNodeAddress() {
        // Setup
        EsNodeInfo invalidNode = new EsNodeInfo("invalid", "invalid:port");
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(invalidNode));
        when(mockEsRestClientForNode.health()).thenReturn(false);

        // Execute
        catalog.detectAvailableNodesInfo();

        // Verify
        assertTrue(catalog.getAvailableNodesInfo().isEmpty());
    }

    @Test
    public void testClearAvailableNodesInfoWhenAlreadyEmpty() {
        // Setup - ensure nodes are empty
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Collections.emptyList());
        catalog.detectAvailableNodesInfo();

        // Execute
        catalog.clearAvailableNodesInfo();

        // Verify
        assertTrue(catalog.getAvailableNodesInfo().isEmpty());
    }

    @Test
    public void testConcurrentModificationsDuringDetect() throws InterruptedException {
        // Setup
        when(mockEsRestClient.getHttpNodesList()).thenReturn(Arrays.asList(node1, node2));
        when(mockEsRestClientForNode.health()).thenReturn(true);

        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);
        CountDownLatch latch = new CountDownLatch(threadCount * 2);

        // Execute - half threads detect, half threads clear
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    catalog.detectAvailableNodesInfo();
                } finally {
                    latch.countDown();
                }
            });
            executor.submit(() -> {
                try {
                    catalog.clearAvailableNodesInfo();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait and verify
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));

        // Final state should be consistent
        Set<EsNodeInfo> finalNodes = catalog.getAvailableNodesInfo();
        assertTrue(finalNodes.isEmpty() || finalNodes.size() == 2);
    }
}
