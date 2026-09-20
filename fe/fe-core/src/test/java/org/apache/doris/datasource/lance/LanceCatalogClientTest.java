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

package org.apache.doris.datasource.lance;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListNamespacesResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LanceCatalogClientTest {
    @Test
    public void testCloseWaitsForOperationAndNewGenerationRemainsUsable() throws Exception {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class,
                Mockito.withSettings().extraInterfaces(AutoCloseable.class));
        Session session = Mockito.mock(Session.class);
        BufferAllocator allocator = Mockito.mock(BufferAllocator.class);
        LanceCatalogClient oldClient = client(namespace, allocator, session);
        LanceCatalogClient newClient = client(Mockito.mock(LanceNamespace.class),
                Mockito.mock(BufferAllocator.class), Mockito.mock(Session.class));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            entered.countDown();
            Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
            return null;
        }).when(namespace).tableExists(Mockito.any());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> read = executor.submit(() -> {
                try (LanceCatalogClient.Lease lease = oldClient.acquire()) {
                    return lease.client().tableExists("default", "table");
                }
            });
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            oldClient.close();
            oldClient.close();
            Mockito.verifyNoInteractions(session, allocator);
            Mockito.verify((AutoCloseable) namespace, Mockito.never()).close();
            Assertions.assertThrows(IllegalStateException.class, oldClient::acquire);
            try (LanceCatalogClient.Lease lease = newClient.acquire()) {
                Assertions.assertTrue(lease.client().tableExists("default", "table"));
            }
            finish.countDown();
            Assertions.assertTrue(read.get(10, TimeUnit.SECONDS));
            Mockito.verify((AutoCloseable) namespace).close();
            Mockito.verify(session).close();
            Mockito.verify(allocator).close();
        } finally {
            finish.countDown();
            executor.shutdownNow();
            oldClient.close();
            newClient.close();
        }
    }

    @Test
    public void testCloseFailureStillReleasesSessionAndAllocator() throws Exception {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class,
                Mockito.withSettings().extraInterfaces(AutoCloseable.class));
        Mockito.doThrow(new IllegalStateException("close failed")).when((AutoCloseable) namespace).close();
        Session session = Mockito.mock(Session.class);
        BufferAllocator allocator = Mockito.mock(BufferAllocator.class);
        LanceCatalogClient client = client(namespace, allocator, session);
        client.close();
        client.close();
        Mockito.verify((AutoCloseable) namespace).close();
        Mockito.verify(session).close();
        Mockito.verify(allocator).close();
    }

    @Test
    public void testRepeatedPageTokenFailsInsteadOfReturningPartialListing() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.listNamespaces(Mockito.any())).thenReturn(
                new ListNamespacesResponse().namespaces(Collections.emptySet()).pageToken("repeated"));
        try (LanceCatalogClient client = client(namespace,
                Mockito.mock(BufferAllocator.class), Mockito.mock(Session.class));
                LanceCatalogClient.Lease lease = client.acquire()) {
            Assertions.assertThrows(IllegalStateException.class, client::listDatabaseNames);
            Mockito.verify(namespace, Mockito.times(2)).listNamespaces(Mockito.any());
        }
    }

    @Test
    public void testProviderFailureUsesCapturedSecrets() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenThrow(new RuntimeException("old-bearer-token"));
        java.util.List<String> secrets = new java.util.ArrayList<>(Collections.singletonList("old-bearer-token"));
        try (LanceCatalogClient client = new LanceCatalogClient(namespace, Mockito.mock(BufferAllocator.class),
                Mockito.mock(Session.class), "rest", "default", Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), secrets);
                LanceCatalogClient.Lease lease = client.acquire()) {
            secrets.set(0, "new-bearer-token");
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> client.loadTableMetadata("default", "table"));
            Assertions.assertFalse(failure.getMessage().contains("old-bearer-token"));
            Assertions.assertFalse(failure.getCause().getMessage().contains("old-bearer-token"));
        }
    }

    @Test
    public void testLeaseAdmittedBeforeRetirementCanStartItsFirstOperation() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Session session = Mockito.mock(Session.class);
        BufferAllocator allocator = Mockito.mock(BufferAllocator.class);
        LanceCatalogClient client = client(namespace, allocator, session);
        try (LanceCatalogClient.Lease lease = client.acquire()) {
            client.close();
            Mockito.verifyNoInteractions(session, allocator);
            Assertions.assertTrue(lease.client().tableExists("default", "table"));
        }
        Mockito.verify(session).close();
        Mockito.verify(allocator).close();
    }

    @Test
    public void testConcurrentMetadataReadsShareArrowBudgetAndReleaseChildren() throws Exception {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        CountDownLatch childrenCreated = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(1);
        Mockito.when(namespace.describeTable(Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
            throw new IllegalStateException("stop before Dataset open");
        });
        BufferAllocator allocator = Mockito.spy(new RootAllocator(1024));
        Mockito.doAnswer(invocation -> {
            BufferAllocator child = (BufferAllocator) invocation.callRealMethod();
            childrenCreated.countDown();
            return child;
        }).when(allocator).newChildAllocator(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
        LanceCatalogClient client = new LanceCatalogClient(namespace, allocator, Mockito.mock(Session.class),
                "rest", "default", Collections.emptyList(), Collections.emptyList(),
                Collections.emptyMap(), Collections.emptyList());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Callable<Void> read = () -> {
                try (LanceCatalogClient.Lease lease = client.acquire()) {
                    Assertions.assertThrows(RuntimeException.class,
                            () -> lease.client().loadTableSchema("default", "table"));
                }
                return null;
            };
            Future<Void> first = executor.submit(read);
            Future<Void> second = executor.submit(read);
            // Namespace calls are serialized, but both reads already own their child allocators.
            Assertions.assertTrue(childrenCreated.await(10, TimeUnit.SECONDS));
            java.util.List<BufferAllocator> children = new java.util.ArrayList<>(allocator.getChildAllocators());
            Assertions.assertEquals(2, children.size());
            try (ArrowBuf buffer = children.get(0).buffer(1024)) {
                Assertions.assertEquals(1024, allocator.getAllocatedMemory());
                Assertions.assertThrows(OutOfMemoryException.class, () -> children.get(1).buffer(1));
            }
            try (ArrowBuf buffer = children.get(1).buffer(1024)) {
                Assertions.assertEquals(1024, allocator.getAllocatedMemory());
            }
            finish.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, allocator.getAllocatedMemory());
            Assertions.assertTrue(allocator.getChildAllocators().isEmpty());
        } finally {
            finish.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            client.close();
        }
    }

    @Test
    public void testCreateTableSerializesAnEmptyArrowStream() throws Exception {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        BufferAllocator allocator = new RootAllocator(1024 * 1024);
        LanceCatalogClient client = client(namespace, allocator, Mockito.mock(Session.class));
        Schema schema = new Schema(Collections.singletonList(
                Field.notNullable("id", new ArrowType.Int(32, true))));
        try {
            try (LanceCatalogClient.Lease lease = client.acquire()) {
                lease.client().createTable("default", "events", schema, Collections.emptyMap());
            }

            ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
            Mockito.verify(namespace).createTable(Mockito.any(), payload.capture());
            Assertions.assertTrue(payload.getValue().length > 0);
            try (ArrowStreamReader reader = new ArrowStreamReader(
                    new ByteArrayInputStream(payload.getValue()), allocator)) {
                Assertions.assertEquals(schema, reader.getVectorSchemaRoot().getSchema());
                Assertions.assertFalse(reader.loadNextBatch());
            }
        } finally {
            client.close();
        }
    }

    private static LanceCatalogClient client(LanceNamespace namespace, BufferAllocator allocator, Session session) {
        return new LanceCatalogClient(namespace, allocator, session, "filesystem", "default",
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), Collections.emptyList());
    }
}
