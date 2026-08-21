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

import org.apache.doris.common.AnalysisException;

import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LanceFilesystemCatalogTest {

    @Test
    public void testLoadTableIndexEntriesRejectsRestCatalogBeforeInit() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE, LanceExternalCatalog.LANCE_REST);
        properties.put(LanceExternalCatalog.REST_URI, "http://127.0.0.1:1/");
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                5, "lance_rest_entries", null, properties, "");

        Assert.assertFalse(catalog.isInitialized());
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> catalog.loadTableIndexEntries("db", "table"));
        Assert.assertEquals("Lance index inspection is not supported for Lance REST catalogs",
                exception.getDetailMessage());
        Assert.assertFalse(catalog.isInitialized());
    }

    @Test
    public void testMinioStorageOptionMapping() {
        Map<String, String> backendProperties = new HashMap<>();
        backendProperties.put("AWS_ACCESS_KEY", "ak");
        backendProperties.put("AWS_SECRET_KEY", "sk");
        backendProperties.put("AWS_ENDPOINT", "http://minio:9000");
        backendProperties.put("AWS_REGION", "us-east-1");
        backendProperties.put("use_path_style", "true");

        Map<String, String> options = LanceStorageOptions.forJavaSdk(backendProperties);
        Assert.assertEquals("ak", options.get("aws_access_key_id"));
        Assert.assertEquals("sk", options.get("aws_secret_access_key"));
        Assert.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        Assert.assertEquals("us-east-1", options.get("aws_region"));
        Assert.assertEquals("true", options.get("allow_http"));
        Assert.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
    }

    @Test
    public void testNamespaceNameRoundTrip() throws Exception {
        Assert.assertEquals(Collections.emptyList(), LanceNamespaceName.dorisDatabaseNameToNamespace(
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        Collections.emptyList(), ".", "default"),
                ".", "default"));
        Assert.assertEquals("doris",
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        Collections.singletonList("doris"), ".", "default"));
        Assert.assertEquals("company.analytics",
                LanceNamespaceName.namespaceToDorisDatabaseName(
                        java.util.Arrays.asList("company", "analytics"), ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("company", "analytics"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("company", "analytics"), ".", "default"),
                        ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("a.b", "c"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("a.b", "c"), ".", "default"),
                        ".", "default"));

        java.util.List<String> delimiterAtEnd = java.util.Arrays.asList("a.", "b");
        java.util.List<String> delimiterAtStart = java.util.Arrays.asList("a", ".b");
        String encodedAtEnd =
                LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtEnd, ".", "default");
        String encodedAtStart =
                LanceNamespaceName.namespaceToDorisDatabaseName(delimiterAtStart, ".", "default");
        Assert.assertNotEquals(encodedAtEnd, encodedAtStart);
        Assert.assertEquals(delimiterAtEnd,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtEnd, ".", "default"));
        Assert.assertEquals(delimiterAtStart,
                LanceNamespaceName.dorisDatabaseNameToNamespace(encodedAtStart, ".", "default"));
        Assert.assertEquals(java.util.Arrays.asList("a\\b", "c"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(
                        LanceNamespaceName.namespaceToDorisDatabaseName(
                                java.util.Arrays.asList("a\\b", "c"), ".", "default"),
                        ".", "default"));

        String rootCollision = LanceNamespaceName.namespaceToDorisDatabaseName(
                Collections.singletonList("default"), ".", "default");
        Assert.assertEquals("\\default", rootCollision);
        Assert.assertEquals(Collections.singletonList("default"),
                LanceNamespaceName.dorisDatabaseNameToNamespace(rootCollision, ".", "default"));
    }

    @Test
    public void testLoadTableIndexEntriesWrapsFailureWithSanitizedMessage() {
        String accessKey = "sentinel-access-key";
        String secretKey = "sentinel-secret-key";
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.LANCE_CATALOG_TYPE,
                LanceExternalCatalog.LANCE_FILESYSTEM);
        properties.put(LanceExternalCatalog.WAREHOUSE, "/nonexistent-lance-warehouse-dir");
        properties.put("AWS_ACCESS_KEY", accessKey);
        properties.put("AWS_SECRET_KEY", secretKey);
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                6, "lance_filesystem_entries", null, properties, "");

        RuntimeException exception = Assert.assertThrows(RuntimeException.class,
                () -> catalog.loadTableIndexEntries("db", "table"));

        Assert.assertTrue(exception.getMessage().contains(
                "Failed to load Lance index metadata for db.table: "));
        Assert.assertNotNull(exception.getCause());
        StringWriter stackTrace = new StringWriter();
        exception.printStackTrace(new PrintWriter(stackTrace));
        for (String sentinel : Arrays.asList(accessKey, secretKey)) {
            Assert.assertFalse(exception.getMessage().contains(sentinel));
            Assert.assertFalse(exception.getCause().getMessage().contains(sentinel));
            Assert.assertFalse(stackTrace.toString().contains(sentinel));
        }
    }

    @Test
    public void testIndexMetadataErrorSanitization() {
        String bearerToken = "sentinel-bearer-token";
        String apiKey = "sentinel-api-key";
        String accessKey = "sentinel-access-key";
        String secretKey = "sentinel-secret-key";
        String sessionToken = "sentinel-session-token";
        String datasetUri = "s3://sentinel-user:sentinel-password@bucket/private/table.lance";

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(LanceExternalCatalog.REST_BEARER_TOKEN, bearerToken);
        catalogProperties.put(LanceExternalCatalog.REST_API_KEY, apiKey);
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                1, "lance_filesystem", null, catalogProperties, "");

        Map<String, String> runtimeStorageOptions = new HashMap<>();
        runtimeStorageOptions.put("aws_access_key_id", accessKey);
        runtimeStorageOptions.put("aws_secret_access_key", secretKey);
        runtimeStorageOptions.put("aws_session_token", sessionToken);
        String providerMessage = "provider failure\nuri=" + datasetUri
                + " bearer=" + bearerToken + " api-key=" + apiKey
                + " access=" + accessKey + " secret=" + secretKey + " session=" + sessionToken;

        RuntimeException providerFailure = new RuntimeException(providerMessage);
        RuntimeException exposed = catalog.indexMetadataLoadFailure(
                "db", "table", providerFailure, datasetUri, runtimeStorageOptions);
        StringWriter stackTrace = new StringWriter();
        exposed.printStackTrace(new PrintWriter(stackTrace));

        for (String sentinel : Arrays.asList(bearerToken, apiKey, accessKey, secretKey,
                sessionToken, datasetUri)) {
            Assert.assertFalse(exposed.getMessage().contains(sentinel));
            Assert.assertFalse(exposed.getCause().getMessage().contains(sentinel));
            Assert.assertFalse(stackTrace.toString().contains(sentinel));
        }
        Assert.assertNotSame(providerFailure, exposed.getCause());
        Assert.assertTrue(exposed.getCause().getMessage().contains("***"));
        Assert.assertFalse(exposed.getCause().getMessage().contains("\n"));
        Assert.assertTrue(exposed.getCause().getMessage().getBytes(StandardCharsets.UTF_8).length <= 1024);
    }

    @Test
    public void testIndexMetadataErrorSanitizationUsesUtf8ByteLimit() {
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                2, "lance_filesystem", null, Collections.emptyMap(), "");
        char[] multibyteCharacters = new char[1024];
        Arrays.fill(multibyteCharacters, '界');

        String sanitized = catalog.sanitizedRootCauseMessage(
                new RuntimeException(new String(multibyteCharacters)), null, Collections.emptyMap());

        Assert.assertTrue(sanitized.getBytes(StandardCharsets.UTF_8).length <= 1024);
    }

    @Test
    public void testIndexMetadataErrorSanitizationReplacesOverlappingSecrets() {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put(LanceExternalCatalog.REST_BEARER_TOKEN, "overlapping-secret");
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                3, "lance_filesystem", null, catalogProperties, "");
        Map<String, String> runtimeStorageOptions = Collections.singletonMap(
                "aws_secret_access_key", "overlapping-secret-with-suffix");

        String sanitized = catalog.sanitizedRootCauseMessage(
                new RuntimeException("overlapping-secret-with-suffix"),
                null, runtimeStorageOptions);

        Assert.assertEquals("RuntimeException: ***", sanitized);
    }

    @Test
    public void testIndexMetadataFailurePreservesSanitizedMetadataErrorType() {
        LanceExternalCatalog catalog = new LanceExternalCatalog(
                4, "lance_filesystem", null, Collections.emptyMap(), "");
        IllegalArgumentException metadataFailure = new IllegalArgumentException("invalid metadata");

        RuntimeException exposed = catalog.indexMetadataLoadFailure(
                "db", "table", metadataFailure, null, null);

        Assert.assertTrue(exposed.getCause() instanceof IllegalArgumentException);
        Assert.assertNotSame(metadataFailure, exposed.getCause());
        Assert.assertEquals("IllegalArgumentException: invalid metadata",
                exposed.getCause().getMessage());
    }

    @Test
    public void testIndexMetadataReadTimeoutKeepsWorkerOwnershipUntilReturn() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicBoolean ownerOpen = new AtomicBoolean(false);
        AtomicReference<Throwable> callerFailure = new AtomicReference<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                return new FutureTask<T>(callable) {
                    @Override
                    public T get(long timeout, TimeUnit unit)
                            throws InterruptedException, ExecutionException, TimeoutException {
                        if (!taskStarted.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("Metadata read task did not start");
                        }
                        throw new TimeoutException("deterministic test deadline");
                    }
                };
            }
        };
        Thread caller = new Thread(() -> {
            try {
                LanceMetadataReadExecutor.execute(() -> {
                    ownerOpen.set(true);
                    taskStarted.countDown();
                    try {
                        releaseTask.await();
                        return Collections.emptyList();
                    } finally {
                        ownerOpen.set(false);
                        taskFinished.countDown();
                    }
                }, executor, 5, TimeUnit.SECONDS);
            } catch (Throwable throwable) {
                callerFailure.set(throwable);
            }
        }, "lance-metadata-read-timeout-caller-test");
        try {
            caller.start();
            Assert.assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
            caller.join(TimeUnit.SECONDS.toMillis(5));

            Assert.assertFalse(caller.isAlive());
            Assert.assertTrue(callerFailure.get()
                    instanceof LanceMetadataReadExecutor.MetadataReadTimeoutException);
            Assert.assertEquals("Lance metadata read timed out after 5 seconds",
                    callerFailure.get().getMessage());
            Assert.assertTrue(ownerOpen.get());
            Assert.assertEquals(1, taskFinished.getCount());

            releaseTask.countDown();
            Assert.assertTrue(taskFinished.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(ownerOpen.get());
        } finally {
            releaseTask.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(5));
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testInterruptedIndexMetadataWaitKeepsWorkerOwnershipUntilReturn() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicBoolean ownerOpen = new AtomicBoolean(false);
        AtomicReference<Throwable> callerFailure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                LanceMetadataReadExecutor.execute(() -> {
                    ownerOpen.set(true);
                    taskStarted.countDown();
                    try {
                        releaseTask.await();
                        return Collections.emptyList();
                    } finally {
                        ownerOpen.set(false);
                        taskFinished.countDown();
                    }
                }, executor, 5, TimeUnit.SECONDS);
            } catch (Throwable throwable) {
                callerFailure.set(throwable);
            }
        }, "lance-metadata-read-interrupted-caller-test");
        try {
            caller.start();
            Assert.assertTrue(taskStarted.await(5, TimeUnit.SECONDS));
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(5));

            Assert.assertFalse(caller.isAlive());
            Assert.assertTrue(callerFailure.get()
                    instanceof LanceMetadataReadExecutor.MetadataReadInterruptedException);
            Assert.assertTrue(ownerOpen.get());
            Assert.assertEquals(1, taskFinished.getCount());

            releaseTask.countDown();
            Assert.assertTrue(taskFinished.await(5, TimeUnit.SECONDS));
            Assert.assertFalse(ownerOpen.get());
        } finally {
            releaseTask.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(5));
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testExpiredQueuedIndexMetadataReadDoesNotEnterProvider() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicBoolean providerEntered = new AtomicBoolean(false);
        try {
            executor.submit(() -> {
                blockerStarted.countDown();
                releaseBlocker.await();
                return null;
            });
            Assert.assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

            try {
                LanceMetadataReadExecutor.execute(() -> {
                    providerEntered.set(true);
                    return Collections.emptyList();
                }, executor, 20, TimeUnit.MILLISECONDS);
                Assert.fail("Expected Lance metadata read timeout");
            } catch (LanceMetadataReadExecutor.MetadataReadTimeoutException expected) {
                Assert.assertTrue(expected.getMessage().contains("timed out"));
            }

            releaseBlocker.countDown();
            executor.shutdown();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            Assert.assertFalse(providerEntered.get());
        } finally {
            releaseBlocker.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testIndexMetadataReadRejectsWhenCapacityIsExhausted() throws Exception {
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new ThreadPoolExecutor.AbortPolicy());
        try {
            executor.submit(() -> {
                blockerStarted.countDown();
                releaseBlocker.await();
                return null;
            });
            Assert.assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

            try {
                LanceMetadataReadExecutor.execute(
                        Collections::emptyList, executor, 1, TimeUnit.SECONDS);
                Assert.fail("Expected Lance metadata read capacity rejection");
            } catch (LanceMetadataReadExecutor.MetadataReadCapacityException expected) {
                Assert.assertEquals(
                        "Lance metadata read capacity is exhausted", expected.getMessage());
            }
        } finally {
            releaseBlocker.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
