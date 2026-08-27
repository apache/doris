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

import org.apache.doris.connector.spi.ConnectorMetadataAccessEvent;
import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.connector.spi.ConnectorOperationAbortedException;
import org.apache.doris.connector.spi.ConnectorOperationControl;
import org.apache.doris.connector.spi.ConnectorSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HmsPartitionBatchLoaderTest {

    @Test
    public void emptyRequestDoesNotFetch() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchLoader loader = loader(5000, (db, table, names, control) -> {
            batchSizes.add(names.size());
            return infos(names);
        });

        Assertions.assertEquals(Collections.emptyList(), loader.load(request(Collections.emptyList())));
        Assertions.assertEquals(Collections.emptyList(), batchSizes);
    }

    @Test
    public void chunksLargeRequestAndRestoresRequestOrder() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchLoader loader = loader(5000, (db, table, names, control) -> {
            batchSizes.add(names.size());
            List<HmsPartitionInfo> result = infos(names);
            Collections.reverse(result);
            return result;
        });

        List<String> names = names(12001);
        List<HmsPartitionInfo> result = loader.load(request(names));

        Assertions.assertEquals(Arrays.asList(5000, 5000, 2001), batchSizes);
        Assertions.assertEquals(names.stream().map(HmsPartitionIdentity::fromName).collect(Collectors.toList()),
                result.stream().map(HmsPartitionInfo::getValues).collect(Collectors.toList()));
    }

    @Test
    public void boundsOneHundredTwentyThousandPartitionRequest() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchLoader loader = loader(5000, (db, table, names, control) -> {
            batchSizes.add(names.size());
            return infos(names);
        });

        List<String> names = names(120_000);
        List<HmsPartitionInfo> result = loader.load(request(names));

        Assertions.assertEquals(120_000, result.size());
        Assertions.assertEquals(24, batchSizes.size());
        Assertions.assertTrue(batchSizes.stream().allMatch(size -> size == 5000));
        Assertions.assertEquals(Collections.singletonList("119999"), result.get(result.size() - 1).getValues());
    }

    @Test
    public void halvesDegradableBatchAndReusesTheSafeSize() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchLoader loader = loader(8, (db, table, names, control) -> {
            batchSizes.add(names.size());
            if (names.size() > 2) {
                throw new HmsRemoteCallException("remote", new IOException("frame too large"));
            }
            return infos(names);
        });

        Assertions.assertEquals(10, loader.load(request(names(10))).size());
        Assertions.assertEquals(Arrays.asList(8, 4, 2, 2, 2, 2, 2), batchSizes);
    }

    @Test
    public void doesNotReplayAnEarlierSuccessfulBatch() {
        List<List<String>> attempts = new ArrayList<>();
        HmsPartitionBatchLoader loader = loader(4, (db, table, names, control) -> {
            attempts.add(new ArrayList<>(names));
            if (names.contains("p=4") && names.size() > 1) {
                throw new HmsRemoteCallException("remote", new IOException("message size limit"));
            }
            return infos(names);
        });

        loader.load(request(names(8)));
        Assertions.assertEquals(names(4), attempts.get(0));
        Assertions.assertEquals(1, attempts.stream().filter(names(4)::equals).count());
    }

    @Test
    public void minimumBatchFailurePropagates() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchLoader loader = loader(2, (db, table, names, control) -> {
            batchSizes.add(names.size());
            throw new HmsRemoteCallException("remote", new IOException("frame too large"));
        });

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> loader.load(request(names(2))));
        Assertions.assertEquals(Arrays.asList(2, 1), batchSizes);
        Assertions.assertInstanceOf(HmsRemoteCallException.class, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains("failedBatchSize=1"));
        Assertions.assertTrue(failure.getMessage().contains("attempts=2"));
    }

    @Test
    public void genericFailureIsNotSplit() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsClientException expected = new HmsClientException("authorization failed");
        HmsPartitionBatchLoader loader = loader(8, (db, table, names, control) -> {
            batchSizes.add(names.size());
            throw expected;
        });

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> loader.load(request(names(8))));
        Assertions.assertEquals(Collections.singletonList(8), batchSizes);
        Assertions.assertSame(expected, failure);
    }

    @Test
    public void onlyClassifiedRemoteThriftFailuresCanDegrade() {
        Assertions.assertTrue(HmsPartitionBatchLoader.isDegradableRemoteFailure(
                new HmsRemoteCallException("remote",
                        new shade.doris.hive.org.apache.thrift.transport.TTransportException("closed"))));
        Assertions.assertTrue(HmsPartitionBatchLoader.isDegradableRemoteFailure(
                new HmsRemoteCallException("remote",
                        new shade.doris.hive.org.apache.thrift.TException("too many partitions in request"))));
        Assertions.assertTrue(HmsPartitionBatchLoader.isDegradableRemoteFailure(
                new HmsRemoteCallException("remote",
                        new shade.doris.hive.org.apache.thrift.protocol.TProtocolException(
                                "max message size reached"))));
        Assertions.assertFalse(HmsPartitionBatchLoader.isDegradableRemoteFailure(
                new HmsRemoteCallException("remote",
                        new shade.doris.hive.org.apache.thrift.protocol.TProtocolException(
                                "Unexpected field type"))));
        Assertions.assertFalse(HmsPartitionBatchLoader.isDegradableRemoteFailure(
                new HmsRemoteCallException("remote",
                        new shade.doris.hive.org.apache.thrift.TException("server failure"))));
        Assertions.assertFalse(HmsPartitionBatchLoader.isDegradableRemoteFailure(
                new HmsClientException("local pool failure")));
    }

    @Test
    public void reportsOverlappingMismatchTypesPrecisely() {
        HmsPartitionBatchLoader loader = loader(10, (db, table, names, control) -> Arrays.asList(
                info("a"), info("c"), info("c")));

        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> loader.load(request(Arrays.asList("p=a", "p=b"))));
        Assertions.assertEquals(
                java.util.EnumSet.of(
                        HmsPartitionResultException.MismatchType.MISSING_RESULT,
                        HmsPartitionResultException.MismatchType.DUPLICATE_RESULT,
                        HmsPartitionResultException.MismatchType.UNEXPECTED_RESULT),
                failure.getMismatchTypes());
        Assertions.assertEquals(1, failure.getMissingCount());
        Assertions.assertEquals(1, failure.getUnexpectedCount());
        Assertions.assertEquals(1, failure.getDuplicateCount());
        Assertions.assertEquals(Collections.singletonList("p=b"), failure.getMissingSamples());
        Assertions.assertEquals(Collections.singletonList("[c]"), failure.getUnexpectedSamples());
        Assertions.assertEquals(Collections.singletonList("[c]"), failure.getDuplicateSamples());
    }

    @Test
    public void reportsInvalidResultIdentity() {
        HmsPartitionBatchLoader loader = loader(10, (db, table, names, control) -> Collections.singletonList(
                new HmsPartitionInfo(Arrays.asList("a", "extra"), null, null, null, null, null)));

        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> loader.load(request(Collections.singletonList("p=a"))));
        Assertions.assertEquals(
                java.util.EnumSet.of(
                        HmsPartitionResultException.MismatchType.MISSING_RESULT,
                        HmsPartitionResultException.MismatchType.INVALID_RESULT),
                failure.getMismatchTypes());
        Assertions.assertEquals(1, failure.getMissingCount());
        Assertions.assertEquals(1, failure.getInvalidCount());
    }

    @Test
    public void timeoutAndCancellationStopBeforeAnRpc() {
        AtomicLong time = new AtomicLong();
        List<Integer> calls = new ArrayList<>();
        HmsPartitionBatchLoader timeoutLoader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(10)
                .fallbackTimeoutMillis(30)
                .fetcher((db, table, names, control) -> {
                    calls.add(names.size());
                    throw new HmsRemoteCallException("remote", new IOException("frame too large"));
                })
                .nanoTime(() -> time.getAndAdd(31_000_000L))
                .build();
        ConnectorOperationAbortedException timeout = Assertions.assertThrows(
                ConnectorOperationAbortedException.class,
                () -> timeoutLoader.load(request(names(2))));
        Assertions.assertEquals(ConnectorOperationAbortedException.Reason.DEADLINE_EXCEEDED,
                timeout.getReason());
        Assertions.assertEquals(Collections.singletonList(2), calls);

        ConnectorOperationControl cancelled = new ConnectorOperationControl() {
            @Override
            public void checkActive() {
                throw new ConnectorOperationAbortedException(
                        ConnectorOperationAbortedException.Reason.CANCELLED, "cancelled");
            }

            @Override
            public long remainingTimeMillis() {
                return 1000;
            }
        };
        ConnectorOperationAbortedException cancellation = Assertions.assertThrows(
                ConnectorOperationAbortedException.class, () -> loader(10,
                        (db, table, names, control) -> infos(names)).load(HmsPartitionRequest.builder()
                                .database("db")
                                .table("table")
                                .partitionNames(names(1))
                                .operationControl(cancelled)
                                .build()));
        Assertions.assertEquals(ConnectorOperationAbortedException.Reason.CANCELLED,
                cancellation.getReason());
    }

    @Test
    public void normalBatchingIsNotLimitedByFallbackBudget() {
        AtomicLong time = new AtomicLong();
        HmsPartitionBatchLoader loader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(2)
                .fallbackTimeoutMillis(30)
                .fetcher((db, table, names, control) -> infos(names))
                .nanoTime(() -> time.getAndAdd(31_000_000L))
                .build();

        Assertions.assertEquals(5, loader.load(request(names(5))).size());
    }

    @Test
    public void retryingClientWireAttemptsAreCountedIndividually() {
        AtomicReference<ConnectorMetadataAccessEvent> event = new AtomicReference<>();
        HmsPartitionBatchLoader loader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(10).fallbackTimeoutMillis(30_000)
                .trackedFetcher((db, table, names, control, tracker) ->
                        HmsRemoteCallTracking.withTracker(tracker, names.size(), control, () -> {
                            try {
                                HmsRemoteCallTracking.trackWireAttempt(() -> {
                                    throw new shade.doris.hive.org.apache.thrift.TException("retry");
                                });
                            } catch (shade.doris.hive.org.apache.thrift.TException expected) {
                                // Simulate RetryingMetaStoreClient swallowing one wire failure.
                            }
                            return HmsRemoteCallTracking.trackWireAttempt(() -> infos(names));
                        }))
                .observer(event::set).build();

        Assertions.assertEquals(2, loader.load(request(names(2))).size());
        Assertions.assertEquals(2, event.get().getRpcCount());
        Assertions.assertEquals(4, event.get().getRpcItems());
    }

    @Test
    public void trackingClientMatchesRetryingClientReflectionContract() throws Exception {
        Assertions.assertNotNull(TrackingHiveMetaStoreClient.class.getConstructor(
                Configuration.class, HiveMetaHookLoader.class, Boolean.class));
        Assertions.assertTrue(Arrays.asList(TrackingHiveMetaStoreClient.class.getInterfaces())
                .contains(IMetaStoreClient.class));
    }

    @Test
    public void validatesCanonicalNamesAndPartitionKeys() {
        Assertions.assertEquals(Collections.singletonList(""), HmsPartitionIdentity.fromName("p="));
        Assertions.assertEquals(Arrays.asList("a/b", "x=y"),
                HmsPartitionIdentity.fromName("p=a%2Fb/q=x%3Dy"));
        Assertions.assertDoesNotThrow(() -> request(Arrays.asList("P=a", "p=b")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Arrays.asList("p=a", "q=b")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Collections.singletonList("p=a/")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Arrays.asList("p=a", "p=a")));
    }

    @Test
    public void createsInternalRequestFromSessionAndAccessSource() {
        ConnectorOperationControl operationControl = ConnectorOperationControl.NONE;
        ConnectorMetadataAccessObserver metadataAccessObserver = event -> { };
        ConnectorSession session = (ConnectorSession) Proxy.newProxyInstance(
                ConnectorSession.class.getClassLoader(), new Class<?>[] {ConnectorSession.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getOperationControl")) {
                        return operationControl;
                    }
                    if (method.getName().equals("getMetadataAccessObserver")) {
                        return metadataAccessObserver;
                    }
                    throw new AssertionError("unexpected ConnectorSession method: " + method.getName());
                });

        HmsPartitionRequest request = HmsPartitionRequest.from(
                session, HmsPartitionAccessSource.STATISTICS, "db", "table", names(2));

        Assertions.assertEquals(HmsPartitionAccessSource.STATISTICS, request.getSource());
        Assertions.assertSame(operationControl, request.getOperationControl());
        Assertions.assertSame(metadataAccessObserver, request.getMetadataAccessObserver());
        Assertions.assertEquals(names(2), request.getPartitionNames());
    }

    @Test
    public void emitsOneLogicalRequestEventWithPhysicalAttempts() {
        AtomicReference<ConnectorMetadataAccessEvent> event = new AtomicReference<>();
        AtomicReference<ConnectorMetadataAccessEvent> requestEvent = new AtomicReference<>();
        AtomicLong time = new AtomicLong();
        HmsPartitionBatchLoader loader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(3)
                .fallbackTimeoutMillis(30_000)
                .fetcher((db, table, names, control) -> infos(names))
                .observer(event::set)
                .nanoTime(() -> time.getAndAdd(10_000_000L))
                .build();

        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names(5))
                .source(HmsPartitionAccessSource.QUERY)
                .metadataAccessObserver(requestEvent::set)
                .build();
        loader.load(request);
        Assertions.assertEquals(2, event.get().getRpcCount());
        Assertions.assertEquals(5, event.get().getRpcItems());
        Assertions.assertEquals(3, event.get().getLargestBatchSize());
        Assertions.assertEquals(2, event.get().getSmallestBatchSize());
        Assertions.assertEquals(50, event.get().getLogicalElapsedMillis());
        Assertions.assertEquals(20, event.get().getRpcElapsedMillis());
        Assertions.assertEquals(10, event.get().getMaxRpcElapsedMillis());
        Assertions.assertTrue(event.get().isSuccess());
        Assertions.assertSame(event.get(), requestEvent.get());
    }

    @Test
    public void observerFailuresDoNotFailSuccessfulRequestAndBothSinksRun() {
        AtomicInteger observerCalls = new AtomicInteger();
        HmsPartitionBatchLoader loader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(10)
                .fallbackTimeoutMillis(30_000)
                .fetcher((db, table, names, control) -> infos(names))
                .observer(event -> {
                    observerCalls.incrementAndGet();
                    throw new IllegalStateException("metrics failure");
                })
                .build();
        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names(2))
                .metadataAccessObserver(event -> {
                    observerCalls.incrementAndGet();
                    throw new IllegalStateException("profile failure");
                })
                .build();

        Assertions.assertEquals(2, loader.load(request).size());
        Assertions.assertEquals(2, observerCalls.get());
    }

    @Test
    public void observerFailuresDoNotReplaceHmsFailure() {
        AtomicInteger observerCalls = new AtomicInteger();
        HmsPartitionBatchLoader loader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(10)
                .fallbackTimeoutMillis(30_000)
                .fetcher((db, table, names, control) -> {
                    throw new HmsClientException("authorization failed");
                })
                .observer(event -> {
                    observerCalls.incrementAndGet();
                    throw new IllegalStateException("metrics failure");
                })
                .build();
        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names(1))
                .metadataAccessObserver(event -> {
                    observerCalls.incrementAndGet();
                    throw new IllegalStateException("profile failure");
                })
                .build();

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> loader.load(request));
        Assertions.assertTrue(failure.getMessage().contains("authorization failed"));
        Assertions.assertEquals(2, observerCalls.get());
    }

    @Test
    public void concurrentLogicalRequestsKeepIndependentBatchState() throws Exception {
        CountDownLatch bothRequestsEntered = new CountDownLatch(2);
        HmsPartitionBatchLoader loader = loader(100, (db, table, names, control) -> {
            bothRequestsEntered.countDown();
            Assertions.assertTrue(bothRequestsEntered.await(5, TimeUnit.SECONDS));
            return infos(names);
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> first = executor.submit(() -> loader.load(request(names(10))));
            Future<List<HmsPartitionInfo>> second = executor.submit(() -> loader.load(request(names(10))));

            Assertions.assertEquals(10, first.get(10, TimeUnit.SECONDS).size());
            Assertions.assertEquals(10, second.get(10, TimeUnit.SECONDS).size());
        } finally {
            executor.shutdownNow();
        }
    }

    private static HmsPartitionBatchLoader loader(int maxBatchSize, HmsPartitionBatchLoader.Fetcher fetcher) {
        return HmsPartitionBatchLoader.builder()
                .maxBatchSize(maxBatchSize)
                .fallbackTimeoutMillis(30_000)
                .fetcher(fetcher)
                .build();
    }

    private static HmsPartitionRequest request(List<String> names) {
        return HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names)
                .source(HmsPartitionAccessSource.QUERY)
                .build();
    }

    private static List<String> names(int count) {
        return IntStream.range(0, count).mapToObj(i -> "p=" + i).collect(Collectors.toList());
    }

    private static List<HmsPartitionInfo> infos(List<String> names) {
        return names.stream().map(name -> new HmsPartitionInfo(
                HmsPartitionIdentity.fromName(name), "loc/" + name, null, null, null, null))
                .collect(Collectors.toList());
    }

    private static HmsPartitionInfo info(String value) {
        return new HmsPartitionInfo(Collections.singletonList(value), "loc/" + value,
                null, null, null, null);
    }
}
