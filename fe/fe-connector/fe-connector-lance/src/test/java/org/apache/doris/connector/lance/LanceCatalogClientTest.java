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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.FakeResourceFactory.RecordingAllocator;
import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ListNamespacesResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The generations of {@link LanceCatalogClient}: a call keeps the generation it started on, a refresh or a close
 * waits for no call, and the last call on a retired generation releases its resources.
 */
class LanceCatalogClientTest {

    private final FakeResourceFactory resources = new FakeResourceFactory();

    @Test
    void closeLetsTheCallInFlightFinishThenReleasesAndRefusesNewCalls() throws Exception {
        LanceCatalogClient client = client(1024 * 1024);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        resources.nextNamespace = () -> blockingExistenceChecks(entered, release);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> inFlight = executor.submit(() -> client.tableExists("default", "t"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            client.close();
            client.close();
            Assertions.assertEquals(0, resources.namespaces.get(0).closes.get());
            Assertions.assertFalse(resources.allocators.get(0).closed);
            Assertions.assertEquals("Lance catalog resources have been closed", Assertions.assertThrows(
                    DorisConnectorException.class, () -> client.tableExists("default", "t")).getMessage());

            release.countDown();
            Assertions.assertTrue(inFlight.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, resources.namespaces.get(0).closes.get());
            Assertions.assertTrue(resources.allocators.get(0).closed);
        } finally {
            release.countDown();
            shutdown(executor);
        }
    }

    @Test
    void failingToCloseTheNamespaceStillReleasesTheAllocator() {
        LanceCatalogClient client = client(1024 * 1024);
        resources.nextNamespace = () -> {
            FakeLanceNamespace namespace = new FakeLanceNamespace();
            namespace.closeFailure = new IllegalStateException("close failed");
            return namespace;
        };
        Assertions.assertTrue(client.tableExists("default", "t"));
        client.close();
        client.close();
        Assertions.assertEquals(1, resources.namespaces.get(0).closes.get());
        Assertions.assertTrue(resources.allocators.get(0).closed);
    }

    /** A generation that cannot be opened releases what it had opened, and the next call tries again. */
    @Test
    void failingToOpenAGenerationReleasesWhatItOpenedAndTheNextCallRetries() {
        LanceCatalogClient client = client(1024 * 1024);
        resources.nextSession = () -> {
            throw new IllegalStateException("no session");
        };
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> client.tableExists("default", "t"));
        Assertions.assertTrue(failure.getMessage().startsWith("Failed to initialize Lance filesystem catalog"),
                failure.getMessage());
        Assertions.assertEquals(1, resources.namespaces.get(0).closes.get());
        Assertions.assertTrue(resources.allocators.get(0).closed);

        resources.nextSession = () -> null;
        Assertions.assertTrue(client.tableExists("default", "t"));
        Assertions.assertEquals(2, resources.namespaces.size());
        client.close();
        Assertions.assertEquals(1, resources.namespaces.get(1).closes.get());
    }

    @Test
    void refreshLetsTheCallInFlightFinishOnItsGenerationAndMovesNewCallsToANewOne() throws Exception {
        LanceCatalogClient client = client(1024 * 1024);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        resources.nextNamespace = () -> blockingExistenceChecks(entered, release);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> inFlight = executor.submit(() -> client.tableExists("default", "t"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            resources.nextNamespace = FakeLanceNamespace::new;
            // Returns without waiting for the call in flight, and closes nothing under it.
            client.refresh();
            Assertions.assertEquals(0, resources.namespaces.get(0).closes.get());

            Assertions.assertTrue(client.tableExists("default", "t"));
            Assertions.assertEquals(2, resources.namespaces.size());
            Assertions.assertEquals(1, resources.namespaces.get(1).existenceChecks.size());
            Assertions.assertEquals(0, resources.namespaces.get(0).closes.get());

            release.countDown();
            Assertions.assertTrue(inFlight.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, resources.namespaces.get(0).closes.get());
            Assertions.assertTrue(resources.allocators.get(0).closed);
            Assertions.assertEquals(0, resources.namespaces.get(1).closes.get());
            Assertions.assertFalse(resources.allocators.get(1).closed);
        } finally {
            release.countDown();
            shutdown(executor);
        }
        client.close();
        Assertions.assertEquals(1, resources.namespaces.get(1).closes.get());
        Assertions.assertTrue(resources.allocators.get(1).closed);
    }

    @Test
    void idleGenerationIsReleasedByTheRefreshItself() {
        LanceCatalogClient client = client(1024 * 1024);
        Assertions.assertTrue(client.tableExists("default", "t"));
        client.refresh();
        Assertions.assertEquals(1, resources.namespaces.get(0).closes.get());
        Assertions.assertTrue(resources.allocators.get(0).closed);
        client.close();
        Assertions.assertEquals(1, resources.namespaces.size());
    }

    /** An FE replays REFRESH and DDL edits for catalogs nobody has used: invalidating opens nothing. */
    @Test
    void invalidationOpensNothingAndCannotReviveAClosedClient() {
        LanceCatalogClient client = new LanceCatalogClient(filesystem(), () -> {
            throw new AssertionError("an invalidation must not bind storage");
        }, () -> {
            throw new AssertionError("an invalidation must not read lance.conf");
        }, resources, () -> 0L);
        client.refresh();
        client.invalidateTableAccess();
        client.close();
        client.refresh();
        client.invalidateTableAccess();
        Assertions.assertTrue(resources.namespaces.isEmpty());
        Assertions.assertTrue(resources.allocators.isEmpty());
        Assertions.assertEquals("Lance catalog resources have been closed", Assertions.assertThrows(
                DorisConnectorException.class, () -> client.listDatabaseNames()).getMessage());
    }

    /** REFRESH TABLE / DATABASE keep the generation, and with it the Session's cached metadata. */
    @Test
    void tableAccessInvalidationKeepsTheGeneration() {
        LanceCatalogClient client = client(1024 * 1024);
        Assertions.assertTrue(client.tableExists("default", "t"));
        client.invalidateTableAccess();
        Assertions.assertTrue(client.tableExists("default", "t"));
        Assertions.assertEquals(1, resources.namespaces.size());
        Assertions.assertEquals(2, resources.namespaces.get(0).existenceChecks.size());
        Assertions.assertEquals(0, resources.namespaces.get(0).closes.get());
        client.close();
    }

    @Test
    void repeatedPageTokenFailsInsteadOfReturningAPartialListing() {
        LanceCatalogClient client = client(1024 * 1024);
        resources.nextNamespace = () -> {
            FakeLanceNamespace namespace = new FakeLanceNamespace();
            namespace.listNamespaces = request -> new ListNamespacesResponse()
                    .namespaces(Collections.emptySet()).pageToken("repeated");
            return namespace;
        };
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                client::listDatabaseNames);
        Assertions.assertTrue(failure.getMessage().contains("Lance namespace repeated a pagination token"),
                failure.getMessage());
        Assertions.assertEquals(2, resources.namespaces.get(0).namespaceListings.size());
        client.close();
    }

    /**
     * Each read takes a child of its generation's allocator, so concurrent reads share the catalog's Arrow limit,
     * and a read gives its child back whether it succeeds or fails.
     */
    @Test
    void concurrentReadsShareTheArrowLimitAndReleaseTheirAllocators() throws Exception {
        LanceCatalogClient client = client(1024);
        CountDownLatch release = new CountDownLatch(1);
        resources.childAllocators = new CountDownLatch(2);
        resources.nextNamespace = () -> {
            FakeLanceNamespace namespace = new FakeLanceNamespace();
            namespace.describe = request -> {
                await(release);
                throw new IllegalStateException("stop before the dataset is opened");
            };
            return namespace;
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Callable<DorisConnectorException> read = () -> Assertions.assertThrows(DorisConnectorException.class,
                    () -> client.loadTableMetadata("default", "t", OptionalLong.empty(), new LanceMetadataMetrics()));
            Future<DorisConnectorException> first = executor.submit(read);
            Future<DorisConnectorException> second = executor.submit(read);
            Assertions.assertTrue(resources.childAllocators.await(10, TimeUnit.SECONDS));
            RecordingAllocator root = resources.allocators.get(0);
            List<BufferAllocator> children = new ArrayList<>(root.getChildAllocators());
            Assertions.assertEquals(2, children.size());
            try (ArrowBuf buffer = children.get(0).buffer(1024)) {
                Assertions.assertEquals(1024, root.getAllocatedMemory());
                Assertions.assertThrows(OutOfMemoryException.class, () -> children.get(1).buffer(1));
            }
            try (ArrowBuf buffer = children.get(1).buffer(1024)) {
                Assertions.assertEquals(1024, root.getAllocatedMemory());
            }
            release.countDown();
            Assertions.assertTrue(first.get(10, TimeUnit.SECONDS).getMessage()
                    .startsWith("Failed to load Lance table metadata for default.t"));
            second.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(0, root.getAllocatedMemory());
            Assertions.assertTrue(root.getChildAllocators().isEmpty());
        } finally {
            release.countDown();
            shutdown(executor);
            client.close();
        }
    }

    private LanceCatalogClient client(long arrowMemoryLimitBytes) {
        return new LanceCatalogClient(filesystem(), Collections::emptyList, () -> arrowMemoryLimitBytes, resources,
                () -> 0L);
    }

    private static LanceCatalogProperties filesystem() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", "/tmp/lance");
        return LanceCatalogProperties.of(properties);
    }

    private static FakeLanceNamespace blockingExistenceChecks(CountDownLatch entered, CountDownLatch release) {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        namespace.tableExists = request -> {
            entered.countDown();
            await(release);
        };
        return namespace;
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
}
