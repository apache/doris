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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    private static ThriftHmsClient client(int poolSize, IMetaStoreClient remote, int batchSize) {
        HmsClientConfig config = new HmsClientConfig(
                Collections.singletonMap(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, String.valueOf(batchSize)), poolSize);
        return new ThriftHmsClient(config, null, conf -> remote, HmsTypeMapping.Options.DEFAULT);
    }

    @FunctionalInterface
    private interface PartitionCall {
        List<Partition> call(List<String> names) throws Exception;
    }

    @SuppressWarnings("unchecked")
    private static IMetaStoreClient remote(PartitionCall call) {
        return (IMetaStoreClient) Proxy.newProxyInstance(IMetaStoreClient.class.getClassLoader(),
                new Class<?>[] {IMetaStoreClient.class}, (proxy, method, arguments) -> {
                    if ("getPartitionsByNames".equals(method.getName())) {
                        return call.call((List<String>) arguments[2]);
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
