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

import org.apache.doris.datasource.lance.metadata.LanceTableAccess;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableResponse;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LanceTableAccessCacheTest {
    @Test
    public void testRepeatedFilesystemReadDescribesOnce() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                new DescribeTableResponse().tableUri("file:///warehouse/items.lance"));
        LanceNamespaceClient client = new LanceNamespaceClient(namespace, "filesystem", "default",
                Collections.emptyList(), Collections.emptyList());
        Assertions.assertEquals("file:///warehouse/items.lance",
                client.resolveTableAccess("default", "items").getDatasetUri());
        client.resolveTableAccess("default", "items");
        Mockito.verify(namespace, Mockito.times(1)).describeTable(Mockito.any());
    }

    @Test
    public void testRepeatedRestReadDescribesOnce() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                new DescribeTableResponse().tableUri("s3://example-bucket/items.lance"));
        LanceNamespaceClient client = new LanceNamespaceClient(namespace, "rest", "default",
                Collections.emptyList(), Collections.emptyList());
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("default", "items");
        Mockito.verify(namespace, Mockito.times(1)).describeTable(Mockito.argThat(
                request -> Boolean.TRUE.equals(request.getVendCredentials())));
    }

    @Test
    public void testExpiryIsNotExtendedByHits() {
        LanceNamespace namespace = namespace();
        AtomicLong millis = new AtomicLong(1_000_000);
        LanceNamespaceClient client = client(namespace, "filesystem", 60, millis);
        client.resolveTableAccess("default", "items");
        millis.addAndGet(59_000);
        client.resolveTableAccess("default", "items");
        Mockito.verify(namespace).describeTable(Mockito.any());
        millis.addAndGet(1_000);
        client.resolveTableAccess("default", "items");
        Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
    }

    @Test
    public void testZeroTtlDisablesCaching() {
        LanceNamespace namespace = namespace();
        LanceNamespaceClient client = client(namespace, "rest", 0, new AtomicLong(1_000_000));
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("default", "items");
        Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
    }

    @Test
    public void testVendedCredentialsAreResolvedForEveryRead() {
        LanceNamespace namespace = namespace();
        AtomicLong millis = new AtomicLong(1_000_000);
        Map<String, String> options = new HashMap<>();
        options.put("aws_session_token", "example-token");
        options.put("expires_at_millis", "1040000");
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                new DescribeTableResponse().tableUri("s3://example-bucket/items.lance").storageOptions(options));
        LanceNamespaceClient client = client(namespace, "rest", 60, millis);
        LanceTableAccess first = client.resolveTableAccess("default", "items");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> first.getStorageOptions().put("aws_session_token", "modified"));
        millis.addAndGet(9_000);
        // A new scan may outlive the remaining credential lifetime, regardless of the cache TTL.
        Assertions.assertNotSame(first, client.resolveTableAccess("default", "items"));
        Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
    }

    @Test
    public void testSignedUrisWithoutStorageOptionsAreNotCached() {
        for (String uri : new String[] {
                "s3://example-bucket/items.lance?X-Amz-Signature=example",
                "az://container/items.lance?sig=example&se=example",
                "s3://example:password@example-bucket/items.lance",
                "s3://example@example_bucket/items.lance",
                "s3://example-bucket/items.lance#example"}) {
            LanceNamespace namespace = namespace();
            Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                    new DescribeTableResponse().tableUri(uri));
            LanceNamespaceClient client = client(namespace, "rest", 60, new AtomicLong(1_000_000));
            client.resolveTableAccess("default", "items");
            client.resolveTableAccess("default", "items");
            Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
        }
    }

    @Test
    public void testUnsafeVendedExpiryIsNotCached() {
        for (String expiry : new String[] {null, "invalid", "-9223372036854775808", "999999", "1029999"}) {
            LanceNamespace namespace = namespace();
            Map<String, String> options = new HashMap<>();
            options.put("aws_session_token", "example-token");
            if (expiry != null) {
                options.put("expires_at_millis", expiry);
            }
            Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                    new DescribeTableResponse().tableUri("s3://example-bucket/items.lance").storageOptions(options));
            LanceNamespaceClient client = client(namespace, "rest", 60, new AtomicLong(1_000_000));
            client.resolveTableAccess("default", "items");
            client.resolveTableAccess("default", "items");
            Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
        }
    }

    @Test
    public void testCacheKeyIncludesNamespaceAndTable() {
        LanceNamespace namespace = namespace();
        LanceNamespaceClient client = new LanceNamespaceClient(namespace, "filesystem", "default",
                Collections.singletonList("parent"), Collections.emptyList());
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("sales", "items");
        client.resolveTableAccess("default", "other");
        client.resolveTableAccess("sales", "items");
        Mockito.verify(namespace).describeTable(Mockito.argThat(
                request -> request.getId().equals(Arrays.asList("parent", "items"))));
        Mockito.verify(namespace).describeTable(Mockito.argThat(
                request -> request.getId().equals(Arrays.asList("parent", "sales", "items"))));
        Mockito.verify(namespace).describeTable(Mockito.argThat(
                request -> request.getId().equals(Arrays.asList("parent", "other"))));
    }

    @Test
    public void testFailedResolutionIsRetried() {
        LanceNamespace namespace = namespace();
        Mockito.when(namespace.describeTable(Mockito.any())).thenThrow(new IllegalStateException("unavailable"))
                .thenReturn(new DescribeTableResponse().tableUri("file:///warehouse/items.lance"));
        LanceNamespaceClient client = client(namespace, "filesystem", 60, new AtomicLong(1_000_000));
        Assertions.assertThrows(IllegalStateException.class, () -> client.resolveTableAccess("default", "items"));
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("default", "items");
        Mockito.verify(namespace, Mockito.times(2)).describeTable(Mockito.any());
    }

    @Test
    public void testRefreshAndUncachedResolutionSeeChangedTarget() {
        LanceNamespace namespace = namespace();
        LanceNamespaceClient client = client(namespace, "filesystem", 60, new AtomicLong(1_000_000));
        String original = client.resolveTableAccess("default", "items").getDatasetUri();
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                new DescribeTableResponse().tableUri("file:///warehouse/replacement.lance"));
        Assertions.assertEquals(original, client.resolveTableAccess("default", "items").getDatasetUri());
        Assertions.assertEquals("file:///warehouse/replacement.lance",
                client.resolveTableAccessUncached("default", "items").getDatasetUri());
        client.invalidateTableAccessCache();
        Assertions.assertEquals("file:///warehouse/replacement.lance",
                client.resolveTableAccess("default", "items").getDatasetUri());
    }

    @Test
    public void testConcurrentMissesShareOneDescribe() throws Exception {
        LanceNamespace namespace = namespace();
        LanceNamespaceClient client = client(namespace, "filesystem", 60, new AtomicLong(1_000_000));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(namespace.describeTable(Mockito.any())).thenAnswer(invocation -> {
            entered.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            return new DescribeTableResponse().tableUri("file:///warehouse/items.lance");
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<LanceTableAccess> first = executor.submit(() -> client.resolveTableAccess("default", "items"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<LanceTableAccess> second = executor.submit(() -> client.resolveTableAccess("default", "items"));
            release.countDown();
            Assertions.assertSame(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            Mockito.verify(namespace).describeTable(Mockito.any());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRefreshDoesNotWaitForOrRetainAnInFlightDescribe() throws Exception {
        LanceNamespace namespace = namespace();
        LanceNamespaceClient client = client(namespace, "filesystem", 60, new AtomicLong(1_000_000));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(namespace.describeTable(Mockito.any())).thenAnswer(invocation -> {
            entered.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            return new DescribeTableResponse().tableUri("file:///warehouse/original.lance");
        }).thenReturn(new DescribeTableResponse().tableUri("file:///warehouse/replacement.lance"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<LanceTableAccess> first = executor.submit(() -> client.resolveTableAccess("default", "items"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            executor.submit(client::invalidateTableAccessCache).get(10, TimeUnit.SECONDS);
            release.countDown();
            Assertions.assertEquals("file:///warehouse/original.lance", first.get(10, TimeUnit.SECONDS).getDatasetUri());
            Assertions.assertEquals("file:///warehouse/replacement.lance",
                    client.resolveTableAccess("default", "items").getDatasetUri());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCacheHitDoesNotWaitForAnotherNamespaceRequest() throws Exception {
        LanceNamespace namespace = namespace();
        LanceNamespaceClient client = client(namespace, "rest", 60, new AtomicLong(1_000_000));
        LanceTableAccess cached = client.resolveTableAccess("default", "items");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            entered.countDown();
            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
            return null;
        }).when(namespace).tableExists(Mockito.any());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> exists = executor.submit(() -> client.tableExists("default", "other"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Assertions.assertSame(cached,
                    executor.submit(() -> client.resolveTableAccess("default", "items")).get(10, TimeUnit.SECONDS));
            release.countDown();
            Assertions.assertTrue(exists.get(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static LanceNamespace namespace() {
        LanceNamespace namespace = Mockito.mock(LanceNamespace.class);
        Mockito.when(namespace.describeTable(Mockito.any())).thenReturn(
                new DescribeTableResponse().tableUri("file:///warehouse/items.lance"));
        return namespace;
    }

    private static LanceNamespaceClient client(LanceNamespace namespace, String type, int ttl, AtomicLong millis) {
        return new LanceNamespaceClient(namespace, type, "default", Collections.emptyList(), Collections.emptyList(),
                ttl, () -> TimeUnit.MILLISECONDS.toNanos(millis.get()));
    }
}
