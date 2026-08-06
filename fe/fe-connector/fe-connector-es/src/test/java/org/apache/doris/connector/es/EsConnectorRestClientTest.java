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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for thread-safety fixes in EsConnectorRestClient:
 * - No shared mutable Request.Builder (P0-2)
 * - AtomicInteger-based node rotation (P0-3)
 */
public class EsConnectorRestClientTest {

    @Test
    public void testConstructorWithAuth() throws Exception {
        EsConnectorRestClient client = new EsConnectorRestClient(
                new String[]{"node1:9200"}, "user", "pass", false, null);
        String authHeader = getFieldValue(client, "authHeader", String.class);
        Assertions.assertNotNull(authHeader, "authHeader should be set when user/password provided");
        Assertions.assertTrue(authHeader.startsWith("Basic "),
                "authHeader should be a Basic auth header");
    }

    @Test
    public void testConstructorWithoutAuth() throws Exception {
        EsConnectorRestClient client = new EsConnectorRestClient(
                new String[]{"node1:9200"}, null, null, false, null);
        String authHeader = getFieldValue(client, "authHeader", String.class);
        Assertions.assertNull(authHeader, "authHeader should be null when no credentials");
    }

    @Test
    public void testConstructorEmptyUserPassword() throws Exception {
        EsConnectorRestClient client = new EsConnectorRestClient(
                new String[]{"node1:9200"}, "", "", false, null);
        String authHeader = getFieldValue(client, "authHeader", String.class);
        Assertions.assertNull(authHeader, "authHeader should be null for empty credentials");
    }

    @Test
    public void testNodeRotationIsAtomic() throws Exception {
        String[] nodes = {"node0:9200", "node1:9200", "node2:9200"};
        EsConnectorRestClient client = new EsConnectorRestClient(
                nodes, null, null, false, null);

        AtomicInteger nodeIndexField = getFieldValue(client, "nodeIndex", AtomicInteger.class);
        Assertions.assertEquals(0, nodeIndexField.get());

        // Use reflection to call advanceNode and currentNode
        java.lang.reflect.Method advanceNode = EsConnectorRestClient.class.getDeclaredMethod("advanceNode");
        advanceNode.setAccessible(true);
        java.lang.reflect.Method currentNode = EsConnectorRestClient.class.getDeclaredMethod("currentNode");
        currentNode.setAccessible(true);

        Assertions.assertEquals("node0:9200", currentNode.invoke(client));
        advanceNode.invoke(client);
        Assertions.assertEquals("node1:9200", currentNode.invoke(client));
        advanceNode.invoke(client);
        Assertions.assertEquals("node2:9200", currentNode.invoke(client));
        advanceNode.invoke(client);
        // Wraps around
        Assertions.assertEquals("node0:9200", currentNode.invoke(client));
    }

    @Test
    public void testConcurrentNodeRotation() throws Exception {
        String[] nodes = {"node0:9200", "node1:9200", "node2:9200"};
        EsConnectorRestClient client = new EsConnectorRestClient(
                nodes, null, null, false, null);

        java.lang.reflect.Method advanceNode = EsConnectorRestClient.class.getDeclaredMethod("advanceNode");
        advanceNode.setAccessible(true);
        java.lang.reflect.Method currentNode = EsConnectorRestClient.class.getDeclaredMethod("currentNode");
        currentNode.setAccessible(true);

        int threadCount = 20;
        int advancesPerThread = 1000;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        Set<String> observedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());

        for (int t = 0; t < threadCount; t++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < advancesPerThread; i++) {
                        String node = (String) currentNode.invoke(client);
                        observedNodes.add(node);
                        advanceNode.invoke(client);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        doneLatch.await();

        // All three nodes should have been observed
        Assertions.assertEquals(3, observedNodes.size(),
                "All nodes should be visited during concurrent rotation");
        Assertions.assertTrue(observedNodes.contains("node0:9200"));
        Assertions.assertTrue(observedNodes.contains("node1:9200"));
        Assertions.assertTrue(observedNodes.contains("node2:9200"));

        // The final index should equal total advances (mod is applied at read time)
        AtomicInteger nodeIndexField = getFieldValue(client, "nodeIndex", AtomicInteger.class);
        int expectedTotal = threadCount * advancesPerThread;
        Assertions.assertEquals(expectedTotal, nodeIndexField.get(),
                "AtomicInteger should reflect exact total of increments");
    }

    @Test
    public void testNoSharedBuilderField() {
        // Verify the class no longer has a "builder" field (which was the shared mutable state)
        try {
            EsConnectorRestClient.class.getDeclaredField("builder");
            Assertions.fail("Class should not have a 'builder' instance field — "
                    + "Request.Builder must be method-local for thread safety");
        } catch (NoSuchFieldException e) {
            // expected
        }
    }

    @Test
    public void testNoCurrentNodeField() {
        // Verify the class no longer has a "currentNode" mutable field
        try {
            EsConnectorRestClient.class.getDeclaredField("currentNode");
            Assertions.fail("Class should not have a 'currentNode' instance field — "
                    + "node address must be computed from AtomicInteger index");
        } catch (NoSuchFieldException e) {
            // expected
        }
    }

    @Test
    public void testSingleNodeRotation() throws Exception {
        String[] nodes = {"single:9200"};
        EsConnectorRestClient client = new EsConnectorRestClient(
                nodes, null, null, false, null);

        java.lang.reflect.Method advanceNode = EsConnectorRestClient.class.getDeclaredMethod("advanceNode");
        advanceNode.setAccessible(true);
        java.lang.reflect.Method currentNode = EsConnectorRestClient.class.getDeclaredMethod("currentNode");
        currentNode.setAccessible(true);

        // With single node, rotation always returns the same node
        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals("single:9200", currentNode.invoke(client));
            advanceNode.invoke(client);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getFieldValue(Object obj, String fieldName, Class<T> type) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }
}
