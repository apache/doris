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
import org.apache.doris.connector.spi.ConnectorMetadataAccessSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HmsPartitionBatchExecutorTest {

    @Test
    public void boundsOneHundredTwentyThousandPartitionRequestAndRestoresOrder() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(5000, (db, table, names) -> {
            batchSizes.add(names.size());
            List<HmsPartitionInfo> result = infos(names);
            Collections.reverse(result);
            return result;
        });
        List<String> names = names(120_000);
        List<HmsPartitionInfo> result = load(executor, request(names));
        Assertions.assertEquals(24, batchSizes.size());
        Assertions.assertTrue(batchSizes.stream().allMatch(size -> size == 5000));
        Assertions.assertEquals(names.stream().map(HmsPartitionIdentity::fromName).collect(Collectors.toList()),
                result.stream().map(HmsPartitionInfo::getValues).collect(Collectors.toList()));
    }

    @Test
    public void halvesDegradableBatchAndReusesTheSafeSize() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(8, (db, table, names) -> {
            batchSizes.add(names.size());
            if (names.size() > 2) {
                throw remoteFailure("frame too large");
            }
            return infos(names);
        });
        Assertions.assertEquals(10, load(executor, request(names(10))).size());
        Assertions.assertEquals(Arrays.asList(8, 4, 2, 2, 2, 2, 2), batchSizes);
    }

    @Test
    public void doesNotReplayAnEarlierSuccessfulBatch() {
        List<List<String>> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(4, (db, table, names) -> {
            attempts.add(new ArrayList<>(names));
            if (names.contains("p=4") && names.size() > 1) {
                throw remoteFailure("message size limit");
            }
            return infos(names);
        });
        load(executor, request(names(8)));
        Assertions.assertEquals(names(4), attempts.get(0));
        Assertions.assertEquals(1, attempts.stream().filter(names(4)::equals).count());
    }

    @Test
    public void minimumBatchFailurePropagates() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(2, (db, table, names) -> {
            batchSizes.add(names.size());
            throw remoteFailure("frame too large");
        });
        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> load(executor, request(names(2))));
        Assertions.assertEquals(Arrays.asList(2, 1), batchSizes);
        Assertions.assertInstanceOf(HmsPartitionBatchExecutor.RemoteCallException.class, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains("failedBatchSize=1"));
        Assertions.assertTrue(failure.getMessage().contains("attempts=2"));
    }

    @Test
    public void onlyClassifiedRemoteThriftFailuresCanDegrade() {
        assertDegradable(false, new shade.doris.hive.org.apache.thrift.transport.TTransportException("closed"));
        assertDegradable(true, new shade.doris.hive.org.apache.thrift.transport.TTransportException(
                "MaxMessageSize reached"));
        assertDegradable(true,
                new shade.doris.hive.org.apache.thrift.TException("too many partitions in request"));
        assertDegradable(true,
                new shade.doris.hive.org.apache.thrift.protocol.TProtocolException("max message size reached"));
        assertDegradable(true, new shade.doris.hive.org.apache.thrift.transport.TTransportException(
                "Frame size (100) larger than max length (10)!"));
        assertDegradable(false, new shade.doris.hive.org.apache.thrift.transport.TTransportException(
                "Read a negative frame size (-1)!"));
        assertDegradable(false,
                new shade.doris.hive.org.apache.thrift.protocol.TProtocolException("Unexpected field type"));
        assertDegradable(false, new shade.doris.hive.org.apache.thrift.TException("server failure"));
        assertDegradable(false, new java.io.IOException("frame too large"));
        Assertions.assertFalse(HmsPartitionBatchExecutor.isDegradableRemoteFailure(
                new HmsClientException("local pool failure")));
    }

    @Test
    public void reportsOverlappingMismatchTypesPrecisely() {
        HmsPartitionBatchExecutor executor = executor(10, (db, table, names) -> Arrays.asList(
                info("a"), info("c"), info("c")));
        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> load(executor, request(Arrays.asList("p=a", "p=b"))));
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
        HmsPartitionBatchExecutor executor = executor(10, (db, table, names) -> Collections.singletonList(
                new HmsPartitionInfo(Arrays.asList("a", "extra"), null, null, null, null, null)));
        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> load(executor, request(Collections.singletonList("p=a"))));
        Assertions.assertEquals(
                java.util.EnumSet.of(
                        HmsPartitionResultException.MismatchType.MISSING_RESULT,
                        HmsPartitionResultException.MismatchType.INVALID_RESULT),
                failure.getMismatchTypes());
        Assertions.assertEquals(1, failure.getMissingCount());
        Assertions.assertEquals(1, failure.getInvalidCount());
    }

    @Test
    public void fallbackTimeoutStopsBeforeRetry() {
        AtomicLong time = new AtomicLong();
        List<Integer> calls = new ArrayList<>();
        HmsPartitionBatchExecutor timeoutLoader = HmsPartitionBatchExecutor.builder()
                .maxBatchSize(10)
                .fallbackTimeoutMillis(30)
                .fetcher((db, table, names) -> {
                    calls.add(names.size());
                    throw remoteFailure("frame too large");
                })
                .nanoTime(() -> time.getAndAdd(31_000_000L))
                .build();
        HmsClientException timeout = Assertions.assertThrows(
                HmsClientException.class,
                () -> load(timeoutLoader, request(names(2))));
        Assertions.assertTrue(timeout.getMessage().contains("fallback timeout"));
        Assertions.assertEquals(Collections.singletonList(2), calls);
    }

    @Test
    public void retryingClientWireAttemptsAreCountedIndividually() {
        AtomicReference<ConnectorMetadataAccessEvent> event = new AtomicReference<>();
        HmsPartitionBatchExecutor executor = HmsPartitionBatchExecutor.builder()
                .maxBatchSize(10).fallbackTimeoutMillis(30_000)
                .transport((db, table, names, tracker) ->
                        HmsRemoteCallTracking.withTracker(tracker, names.size(), () -> {
                            try {
                                HmsRemoteCallTracking.trackWireAttempt(() -> {
                                    throw new shade.doris.hive.org.apache.thrift.TException("retry");
                                });
                            } catch (shade.doris.hive.org.apache.thrift.TException ignored) { // Expected retry.
                            }
                            return HmsRemoteCallTracking.trackWireAttempt(() -> infos(names));
                        }))
                .build();
        HmsPartitionBatchExecutor.Access access = new HmsPartitionBatchExecutor.Access(executor, event::set);
        Assertions.assertEquals(2, access.load(request(names(2))).size());
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
    public void emitsOneLogicalRequestEventWithPhysicalAttempts() {
        AtomicReference<ConnectorMetadataAccessEvent> event = new AtomicReference<>();
        AtomicReference<ConnectorMetadataAccessEvent> requestEvent = new AtomicReference<>();
        AtomicLong time = new AtomicLong();
        HmsPartitionBatchExecutor executor = HmsPartitionBatchExecutor.builder()
                .maxBatchSize(3)
                .fallbackTimeoutMillis(30_000)
                .fetcher((db, table, names) -> infos(names))
                .nanoTime(() -> time.getAndAdd(10_000_000L))
                .build();
        HmsPartitionBatchExecutor.Access access = new HmsPartitionBatchExecutor.Access(
                executor, event::set, () -> time.getAndAdd(10_000_000L));
        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names(5))
                .source(ConnectorMetadataAccessSource.QUERY)
                .metadataAccessObserver(requestEvent::set)
                .build();
        access.load(request);
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
        HmsPartitionBatchExecutor executor = HmsPartitionBatchExecutor.builder()
                .maxBatchSize(10)
                .fallbackTimeoutMillis(30_000)
                .fetcher((db, table, names) -> infos(names))
                .build();
        HmsPartitionBatchExecutor.Access access = new HmsPartitionBatchExecutor.Access(executor, event -> {
            observerCalls.incrementAndGet();
            throw new IllegalStateException("metrics failure");
        });
        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names(2))
                .metadataAccessObserver(event -> {
                    observerCalls.incrementAndGet();
                    throw new IllegalStateException("profile failure");
                })
                .build();
        Assertions.assertEquals(2, access.load(request).size());
        Assertions.assertEquals(2, observerCalls.get());
    }

    @Test
    public void batchConfigParsesAndValidatesProperties() {
        HmsClientConfig defaults = new HmsClientConfig(Collections.emptyMap(), 0);
        Assertions.assertEquals(5000, defaults.getPartitionBatchSize());
        Assertions.assertEquals(30_000L, defaults.getPartitionBatchFallbackTimeoutMillis());
        Map<String, String> properties = new HashMap<>();
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "321");
        properties.put(HmsClientConfig.PARTITION_BATCH_FALLBACK_TIMEOUT_MS_KEY, "4567");
        HmsClientConfig config = new HmsClientConfig(properties, 1);
        Assertions.assertEquals(321, config.getPartitionBatchSize());
        Assertions.assertEquals(4567L, config.getPartitionBatchFallbackTimeoutMillis());
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "0");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsClientConfig(properties, 1));
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "not-a-number");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsClientConfig(properties, 1));
    }

    private static HmsPartitionBatchExecutor executor(int maxBatchSize, HmsPartitionBatchExecutor.Fetcher fetcher) {
        return HmsPartitionBatchExecutor.builder()
                .maxBatchSize(maxBatchSize)
                .fallbackTimeoutMillis(30_000)
                .fetcher(fetcher)
                .build();
    }

    private static List<HmsPartitionInfo> load(
            HmsPartitionBatchExecutor executor, HmsPartitionRequest request) {
        return executor.execute(request, executor.newExecution());
    }

    private static HmsPartitionRequest request(List<String> names) {
        return HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names)
                .source(ConnectorMetadataAccessSource.QUERY)
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

    private static HmsPartitionBatchExecutor.RemoteCallException remoteFailure(String message) {
        return new HmsPartitionBatchExecutor.RemoteCallException(
                "remote", new shade.doris.hive.org.apache.thrift.TException(message));
    }

    private static void assertDegradable(boolean expected, Throwable cause) {
        Assertions.assertEquals(expected, HmsPartitionBatchExecutor.isDegradableRemoteFailure(
                new HmsPartitionBatchExecutor.RemoteCallException("remote", cause)));
    }
}
