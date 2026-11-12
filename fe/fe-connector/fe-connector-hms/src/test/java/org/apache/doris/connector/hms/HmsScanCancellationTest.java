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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class HmsScanCancellationTest {
    @ParameterizedTest
    @CsvSource({"0, false", "0, true", "2, false", "2, true"})
    void cancellationStopsAfterTheRunningPhysicalCall(int poolSize, boolean cacheEnabled) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient remote = remote(names -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                await(release);
            }
            return partitions(names);
        });
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (ThriftHmsClient raw = client(poolSize, remote, 1);
                CachingHmsClient cache = new CachingHmsClient(CatalogMetaCache.unmanaged(), raw,
                        Collections.singletonMap("meta.cache.hive.partition.enable", String.valueOf(cacheEnabled)))) {
            Future<?> pending = worker.submit(() -> cache.getExistingPartitionsWithStats(
                    "db", "tbl", names(1024), context));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            context.cancel();
            Assertions.assertFalse(pending.isDone(), "termination must wait for the running RPC");
            release.countDown();
            ExecutionException error = Assertions.assertThrows(ExecutionException.class,
                    () -> pending.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, error.getCause());
            Assertions.assertEquals(1, calls.get(), "no second physical request after cancellation");
            Assertions.assertEquals(0, cache.inFlightPartitionLoadCountForTest());
            Assertions.assertEquals(1, cache.getExistingPartitionsWithStats(
                    "db", "tbl", names(1), new ConnectorScanContext()).getPartitions().size());
            Assertions.assertEquals(2, calls.get(), "the cancelled lookup must not publish its first chunk");
        } finally {
            release.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancelledOwnerDoesNotAffectHealthyCallerOrCompletedCache(boolean healthyScan) throws Exception {
        ConnectorScanContext cancelled = new ConnectorScanContext();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient remote = remote(names -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                await(release);
            }
            return partitions(names);
        });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (ThriftHmsClient raw = client(2, remote, 1);
                CachingHmsClient cache = new CachingHmsClient(CatalogMetaCache.unmanaged(), raw,
                        Collections.emptyMap())) {
            Future<?> stopped = workers.submit(() -> cache.getExistingPartitionsWithStats(
                    "db", "tbl", names(2), cancelled));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<HmsPartitionBatchResult> healthy = workers.submit(() -> healthyScan
                    ? cache.getExistingPartitionsWithStats("db", "tbl", names(2), new ConnectorScanContext())
                    : cache.getExistingPartitionsWithStats("db", "tbl", names(2)));
            Assertions.assertEquals(2, healthy.get(5, TimeUnit.SECONDS).getPartitions().size());
            cancelled.cancel();
            release.countDown();
            ExecutionException error = Assertions.assertThrows(ExecutionException.class,
                    () -> stopped.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, error.getCause());
            Assertions.assertEquals(3, calls.get());
            Assertions.assertEquals(2, cache.getExistingPartitionsWithStats(
                    "db", "tbl", names(2), new ConnectorScanContext()).getPartitions().size());
            Assertions.assertEquals(3, calls.get(), "completed entries remain shared");
            Assertions.assertEquals(0, cache.inFlightPartitionLoadCountForTest());
        } finally {
            release.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void cancellationPreventsSizeFallback(int poolSize) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient remote = remote(names -> {
            calls.incrementAndGet();
            context.cancel();
            throw new MetaException("frame too large");
        });
        try (ThriftHmsClient client = client(poolSize, remote, 1024)) {
            Assertions.assertThrows(CancellationException.class,
                    () -> client.getExistingPartitionsWithStats("db", "tbl", names(1024), context));
            Assertions.assertEquals(1, calls.get(), "cancel must prevent the smaller fallback request");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void cancellationDuringClientCreationPreventsPartitionRequest(int poolSize) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient remote = remote(names -> {
            calls.incrementAndGet();
            return partitions(names);
        });
        HmsClientConfig config = new HmsClientConfig(Collections.emptyMap(), poolSize);
        try (ThriftHmsClient client = new ThriftHmsClient(config, null, conf -> {
            context.cancel();
            return remote;
        }, HmsTypeMapping.Options.DEFAULT)) {
            Assertions.assertThrows(CancellationException.class,
                    () -> client.getExistingPartitionsWithStats("db", "tbl", names(1), context));
            Assertions.assertEquals(0, calls.get());
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void poolWaitStopsAfterCancellationWithoutAffectingTheHealthyHolder(boolean cacheEnabled,
            boolean listNames) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        CountDownLatch holderEntered = new CountDownLatch(1);
        CountDownLatch releaseHolder = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger created = new AtomicInteger();
        Callable<List<String>> namesCall = () -> {
            if (calls.incrementAndGet() == 1) {
                holderEntered.countDown();
                await(releaseHolder);
            }
            return names(1);
        };
        IMetaStoreClient remote = remote(names -> {
            namesCall.call();
            return partitions(names);
        }, namesCall);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        HmsClientConfig config = new HmsClientConfig(Collections.emptyMap(), 1);
        try (ThriftHmsClient raw = new ThriftHmsClient(config, null, conf -> {
            created.incrementAndGet();
            return remote;
        }, HmsTypeMapping.Options.DEFAULT);
                CachingHmsClient cache = new CachingHmsClient(CatalogMetaCache.unmanaged(), raw,
                        cacheProperties(cacheEnabled))) {
            Future<?> holder = workers.submit(() -> listNames
                    ? cache.listPartitionNames("db", "tbl", -1)
                    : raw.getExistingPartitionsWithStats("db", "tbl", names(1)));
            Assertions.assertTrue(holderEntered.await(5, TimeUnit.SECONDS));
            Future<?> stopped = workers.submit(() -> listNames
                    ? cache.listPartitionNames("db", "tbl", -1, context)
                    : cache.getExistingPartitionsWithStats("db", "tbl", names(1), context));
            GenericObjectPool<?> pool = pool(raw);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            int waiters = pool.getNumWaiters();
            while (waiters == 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
                waiters = pool.getNumWaiters();
            }
            Assertions.assertEquals(1, waiters, "the scan must actually be waiting for the pool");
            context.cancel();
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> stopped.get(2, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, failure.getCause());
            Assertions.assertFalse(holder.isDone(), "cancelling a waiter must not stop the healthy holder");
            Assertions.assertEquals(1, calls.get(), "a cancelled pool waiter must not dispatch a request");
            releaseHolder.countDown();
            holder.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(1, raw.getExistingPartitionsWithStats(
                    "db", "tbl", names(1)).getPartitions().size());
            Assertions.assertEquals(2, calls.get());
            Assertions.assertEquals(1, created.get(), "the healthy pooled connection must remain reusable");
            if (listNames && cacheEnabled) {
                Assertions.assertEquals(names(1), cache.listPartitionNames(
                        "db", "tbl", -1, new ConnectorScanContext()));
                Assertions.assertEquals(2, calls.get(), "completed names remain shared with scan requests");
            }
        } finally {
            releaseHolder.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void scanPoolPollingKeepsTheOriginalTotalWaitBudget(boolean listNames) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient remote = remote(names -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                await(release);
            }
            return partitions(names);
        });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (ThriftHmsClient raw = client(1, remote, 1)) {
            Future<?> holder = workers.submit(() -> raw.getExistingPartitionsWithStats("db", "tbl", names(1)));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            pool(raw).setMaxWaitMillis(250);
            long start = System.nanoTime();
            Future<?> pending = workers.submit(() -> listNames
                    ? raw.listPartitionNames("db", "tbl", -1, context)
                    : raw.getExistingPartitionsWithStats("db", "tbl", names(1), context));
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> pending.get(3, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(HmsClientException.class, failure.getCause());
            Assertions.assertInstanceOf(NoSuchElementException.class, failure.getCause().getCause());
            Assertions.assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(150),
                    "the first 100ms poll must not replace the total borrow budget");
            Assertions.assertFalse(holder.isDone());
            Assertions.assertEquals(1, calls.get());
            release.countDown();
            holder.get(5, TimeUnit.SECONDS);
        } finally {
            context.cancel();
            release.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void clientCreationFailureIsNotRetriedAsAPoolWaitTimeout(boolean listNames) throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HmsClientConfig config = new HmsClientConfig(Collections.emptyMap(), 1);
        try (ThriftHmsClient raw = new ThriftHmsClient(config, null, conf -> {
            attempts.incrementAndGet();
            throw new NoSuchElementException("client creation failed");
        }, HmsTypeMapping.Options.DEFAULT)) {
            ConnectorScanContext context = new ConnectorScanContext();
            HmsClientException failure = Assertions.assertThrows(HmsClientException.class, () -> {
                if (listNames) {
                    raw.listPartitionNames("db", "tbl", -1, context);
                } else {
                    raw.getExistingPartitionsWithStats("db", "tbl", names(1), context);
                }
            });
            Assertions.assertTrue(failure.getMessage().contains("client creation failed"));
            Assertions.assertEquals(1, attempts.get());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void cancellationDuringAuthenticationPreventsNamesRpcWithoutTaintingTheClient(int poolSize) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger authCalls = new AtomicInteger();
        AtomicInteger cancelAt = new AtomicInteger(-1);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger created = new AtomicInteger();
        IMetaStoreClient remote = remote(HmsScanCancellationTest::partitions, () -> {
            calls.incrementAndGet();
            return names(1);
        });
        HmsClientConfig config = new HmsClientConfig(Collections.emptyMap(), poolSize);
        ThriftHmsClient.AuthAction auth = new ThriftHmsClient.AuthAction() {
            @Override
            public <T> T execute(Callable<T> callable) throws Exception {
                if (authCalls.incrementAndGet() == cancelAt.get()) {
                    context.cancel();
                }
                return callable.call();
            }
        };
        try (ThriftHmsClient raw = new ThriftHmsClient(config, auth, conf -> {
            created.incrementAndGet();
            return remote;
        }, HmsTypeMapping.Options.DEFAULT)) {
            Assertions.assertEquals(names(1), raw.listPartitionNames("db", "tbl", -1));
            cancelAt.set(authCalls.get() + (poolSize == 0 ? 2 : 1));
            Assertions.assertThrows(CancellationException.class,
                    () -> raw.listPartitionNames("db", "tbl", -1, context));
            Assertions.assertEquals(1, calls.get(), "cancellation during auth must prevent RPC dispatch");
            Assertions.assertEquals(names(1), raw.listPartitionNames("db", "tbl", -1));
            Assertions.assertEquals(2, calls.get());
            Assertions.assertEquals(poolSize == 0 ? 3 : 1, created.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancelledNamesLoadDoesNotPublishItsResult(boolean cacheEnabled) throws Exception {
        ConnectorScanContext context = new ConnectorScanContext();
        AtomicInteger calls = new AtomicInteger();
        IMetaStoreClient remote = remote(HmsScanCancellationTest::partitions, () -> {
            if (calls.incrementAndGet() == 1) {
                context.cancel();
            }
            return names(1);
        });
        try (ThriftHmsClient raw = client(1, remote, 1);
                CachingHmsClient cache = new CachingHmsClient(CatalogMetaCache.unmanaged(), raw,
                        cacheProperties(cacheEnabled))) {
            Assertions.assertThrows(CancellationException.class,
                    () -> cache.listPartitionNames("db", "tbl", -1, context));
            Assertions.assertEquals(names(1), cache.listPartitionNames(
                    "db", "tbl", -1, new ConnectorScanContext()));
            Assertions.assertEquals(2, calls.get());
        }
    }

    private static Map<String, String> cacheProperties(boolean enabled) {
        Map<String, String> properties = new HashMap<>();
        properties.put("meta.cache.hive.partition.enable", String.valueOf(enabled));
        properties.put("meta.cache.hive.partition_names.enable", String.valueOf(enabled));
        return properties;
    }

    private static GenericObjectPool<?> pool(ThriftHmsClient raw) throws Exception {
        Field field = ThriftHmsClient.class.getDeclaredField("clientPool");
        field.setAccessible(true);
        return (GenericObjectPool<?>) field.get(raw);
    }

    private static ThriftHmsClient client(int poolSize, IMetaStoreClient remote, int batchSize) {
        HmsClientConfig config = new HmsClientConfig(
                Collections.singletonMap(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, String.valueOf(batchSize)),
                poolSize);
        return new ThriftHmsClient(config, null, conf -> remote, HmsTypeMapping.Options.DEFAULT);
    }

    @FunctionalInterface
    private interface PartitionCall {
        List<Partition> call(List<String> names) throws Exception;
    }

    private static IMetaStoreClient remote(PartitionCall call) {
        return remote(call, Collections::emptyList);
    }

    @SuppressWarnings("unchecked")
    private static IMetaStoreClient remote(PartitionCall call, Callable<List<String>> namesCall) {
        return (IMetaStoreClient) Proxy.newProxyInstance(IMetaStoreClient.class.getClassLoader(),
                new Class<?>[] {IMetaStoreClient.class}, (proxy, method, arguments) -> {
                    if ("getPartitionsByNames".equals(method.getName())) {
                        return call.call((List<String>) arguments[2]);
                    }
                    if ("listPartitionNames".equals(method.getName())) {
                        return namesCall.call();
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
    }

    private static List<String> names(int count) {
        return IntStream.range(0, count).mapToObj(i -> "p=" + i).collect(Collectors.toList());
    }

    private static List<Partition> partitions(List<String> names) {
        List<Partition> result = new ArrayList<>();
        for (String name : names) {
            Partition partition = new Partition();
            partition.setValues(Collections.singletonList(name.substring(2)));
            partition.setParameters(Collections.emptyMap());
            result.add(partition);
        }
        return result;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
