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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.DescribeTableResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The table-access cache of {@link LanceNamespaceClient}: a described dataset URI and its storage options are
 * reused until the TTL counted from the describe runs out, never when they may carry credentials. A refresh
 * retires the whole client with its generation; see {@link LanceCatalogClientTest}.
 */
class LanceTableAccessCacheTest {

    @Test
    void repeatedFilesystemReadDescribesOnce() {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        LanceNamespaceClient client = client(namespace, false, 60, new AtomicLong(1_000_000));
        Assertions.assertEquals("file:///warehouse/items.lance",
                client.resolveTableAccess("default", "items").getDatasetUri());
        Assertions.assertFalse(client.resolveTableAccess("default", "items").hasVendedOptions());
        Assertions.assertEquals(1, namespace.describes.size());
    }

    @Test
    void repeatedRestReadDescribesOnceAndAsksForVendedCredentials() {
        FakeLanceNamespace namespace = new FakeLanceNamespace().describing(
                new DescribeTableResponse().tableUri("s3://example-bucket/items.lance"));
        LanceNamespaceClient client = client(namespace, true, 60, new AtomicLong(1_000_000));
        client.resolveTableAccess("default", "items");
        // Asked for, but none vended: the access is cached and read with the catalog's Session.
        Assertions.assertFalse(client.resolveTableAccess("default", "items").hasVendedOptions());
        Assertions.assertEquals(1, namespace.describes.size());
        Assertions.assertEquals(Boolean.TRUE, namespace.describes.get(0).getVendCredentials());
    }

    @Test
    void expiryIsCountedFromTheDescribeAndNotExtendedByHits() {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        AtomicLong millis = new AtomicLong(1_000_000);
        LanceNamespaceClient client = client(namespace, false, 60, millis);
        client.resolveTableAccess("default", "items");
        millis.addAndGet(59_000);
        client.resolveTableAccess("default", "items");
        Assertions.assertEquals(1, namespace.describes.size());
        millis.addAndGet(1_000);
        client.resolveTableAccess("default", "items");
        Assertions.assertEquals(2, namespace.describes.size());
    }

    @Test
    void zeroTtlDescribesOnEveryRead() {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        LanceNamespaceClient client = client(namespace, true, 0, new AtomicLong(1_000_000));
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("default", "items");
        Assertions.assertEquals(2, namespace.describes.size());
    }

    /** A table the connector refuses is described, and refused, again on every read: no refusal is cached. */
    @Test
    void namespaceManagedTableIsRefusedOnEveryRead() {
        FakeLanceNamespace namespace = new FakeLanceNamespace().describing(new DescribeTableResponse()
                .location("s3://example-bucket/items.lance").tableUri("s3://example-bucket/items.lance")
                .managedVersioning(true));
        LanceNamespaceClient client = client(namespace, true, 60, new AtomicLong(1_000_000));
        for (int read = 0; read < 2; read++) {
            Assertions.assertEquals("Lance table default.items has its versions managed by the namespace"
                    + " (managed_versioning), which is not supported yet", Assertions.assertThrows(
                    DorisConnectorException.class, () -> client.resolveTableAccess("default", "items"))
                    .getMessage());
        }
        Assertions.assertEquals(2, namespace.describes.size());
    }

    @Test
    void vendedCredentialsAreDescribedForEveryRead() {
        AtomicLong millis = new AtomicLong(1_000_000);
        Map<String, String> vended = vendedS3Credentials();
        vended.put("expires_at_millis", "1040000");
        FakeLanceNamespace namespace = new FakeLanceNamespace().describing(new DescribeTableResponse()
                .tableUri("s3://example-bucket/items.lance").storageOptions(vended));
        LanceNamespaceClient client = client(namespace, true, 60, millis);
        LanceTableAccess first = client.resolveTableAccess("default", "items");
        Assertions.assertTrue(first.hasVendedOptions());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> first.getStorageOptions().put("aws_session_token", "modified"));
        millis.addAndGet(9_000);
        // A new scan may outlive what is left of the credentials, whatever the cache TTL.
        Assertions.assertNotSame(first, client.resolveTableAccess("default", "items"));
        Assertions.assertEquals(2, namespace.describes.size());
    }

    @Test
    void vendedCredentialsWithAnyExpiryAreNotCached() {
        for (String expiry : new String[] {null, "invalid", "-9223372036854775808", "999999", "1029999",
                "99999999999"}) {
            Map<String, String> vended = vendedS3Credentials();
            if (expiry != null) {
                vended.put("expires_at_millis", expiry);
            }
            FakeLanceNamespace namespace = new FakeLanceNamespace().describing(new DescribeTableResponse()
                    .tableUri("s3://example-bucket/items.lance").storageOptions(vended));
            LanceNamespaceClient client = client(namespace, true, 60, new AtomicLong(1_000_000));
            client.resolveTableAccess("default", "items");
            client.resolveTableAccess("default", "items");
            Assertions.assertEquals(2, namespace.describes.size(), "expires_at_millis=" + expiry);
        }
    }

    /** Presigned, SAS and user-info URIs carry credentials even without storage options. */
    @Test
    void uriThatMayCarryCredentialsIsNotCached() {
        for (String uri : new String[] {
                "s3://example-bucket/items.lance?X-Amz-Signature=example",
                "az://container/items.lance?sig=example&se=example",
                "s3://example:password@example-bucket/items.lance",
                "s3://example@example_bucket/items.lance",
                "s3://example-bucket/items.lance#example",
                "s3://example-bucket/items lance"}) {
            FakeLanceNamespace namespace = new FakeLanceNamespace().describing(
                    new DescribeTableResponse().tableUri(uri));
            LanceNamespaceClient client = client(namespace, true, 60, new AtomicLong(1_000_000));
            client.resolveTableAccess("default", "items");
            client.resolveTableAccess("default", "items");
            Assertions.assertEquals(2, namespace.describes.size(), uri);
        }
    }

    @Test
    void cacheKeyIsTheFullTableIdentifier() {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        LanceNamespaceClient client = new LanceNamespaceClient(namespace, false, "default",
                Collections.singletonList("parent"), Collections.emptyList(), 60, () -> 0L);
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("sales", "items");
        client.resolveTableAccess("default", "other");
        client.resolveTableAccess("sales", "items");
        client.resolveTableAccess("default", "items");
        Assertions.assertEquals(Arrays.asList(Arrays.asList("parent", "items"),
                Arrays.asList("parent", "sales", "items"), Arrays.asList("parent", "other")), describedIds(namespace));
    }

    @Test
    void failedDescribeIsNotCached() {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        AtomicInteger attempts = new AtomicInteger();
        namespace.describe = request -> {
            if (attempts.getAndIncrement() == 0) {
                throw new IllegalStateException("unavailable");
            }
            return new DescribeTableResponse().tableUri("file:///warehouse/items.lance");
        };
        LanceNamespaceClient client = client(namespace, false, 60, new AtomicLong(1_000_000));
        Assertions.assertThrows(IllegalStateException.class, () -> client.resolveTableAccess("default", "items"));
        client.resolveTableAccess("default", "items");
        client.resolveTableAccess("default", "items");
        Assertions.assertEquals(2, namespace.describes.size());
    }

    @Test
    void concurrentMissesShareOneDescribe() throws Exception {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        LanceNamespaceClient client = client(namespace, false, 60, new AtomicLong(1_000_000));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        namespace.describe = request -> {
            entered.countDown();
            await(release);
            return new DescribeTableResponse().tableUri("file:///warehouse/items.lance");
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<LanceTableAccess> first = executor.submit(() -> client.resolveTableAccess("default", "items"));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<LanceTableAccess> second = executor.submit(() -> client.resolveTableAccess("default", "items"));
            release.countDown();
            Assertions.assertSame(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, namespace.describes.size());
        } finally {
            release.countDown();
            shutdown(executor);
        }
    }

    /** A hit takes neither the namespace lock nor I/O, so a stalled namespace request does not hold it up. */
    @Test
    void cacheHitDoesNotWaitForAnotherNamespaceRequest() throws Exception {
        FakeLanceNamespace namespace = new FakeLanceNamespace();
        LanceNamespaceClient client = client(namespace, true, 60, new AtomicLong(1_000_000));
        LanceTableAccess cached = client.resolveTableAccess("default", "items");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        namespace.tableExists = request -> {
            entered.countDown();
            await(release);
        };
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
            shutdown(executor);
        }
    }

    private static Map<String, String> vendedS3Credentials() {
        Map<String, String> options = new HashMap<>();
        options.put("aws_access_key_id", "example-access-key");
        options.put("aws_secret_access_key", "example-secret-key");
        options.put("aws_session_token", "example-token");
        return options;
    }

    private static List<List<String>> describedIds(FakeLanceNamespace namespace) {
        List<List<String>> ids = new ArrayList<>();
        namespace.describes.forEach(request -> ids.add(request.getId()));
        return ids;
    }

    private static LanceNamespaceClient client(FakeLanceNamespace namespace, boolean rest, int ttlSeconds,
            AtomicLong millis) {
        return new LanceNamespaceClient(namespace, rest, "default", Collections.emptyList(), Collections.emptyList(),
                ttlSeconds, () -> TimeUnit.MILLISECONDS.toNanos(millis.get()));
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
