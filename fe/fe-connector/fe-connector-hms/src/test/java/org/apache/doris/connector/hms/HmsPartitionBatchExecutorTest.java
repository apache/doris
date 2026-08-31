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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HmsPartitionBatchExecutorTest {

    @Test
    public void boundsLargeRequestAndRestoresOrder() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(5000, (db, table, names) -> {
            batchSizes.add(names.size());
            List<HmsPartitionInfo> result = infos(names);
            Collections.reverse(result);
            return result;
        });

        List<String> names = names(120_000);
        List<HmsPartitionInfo> result = executor.execute(request(names));

        Assertions.assertEquals(24, batchSizes.size());
        Assertions.assertTrue(batchSizes.stream().allMatch(size -> size == 5000));
        Assertions.assertEquals(names.stream().map(HmsPartitionIdentity::fromName).collect(Collectors.toList()),
                result.stream().map(HmsPartitionInfo::getValues).collect(Collectors.toList()));
    }

    @Test
    public void handlesEmptyExactAndTrailingBatches() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(3, (db, table, names) -> {
            batchSizes.add(names.size());
            return infos(names);
        });

        Assertions.assertTrue(executor.execute(request(Collections.emptyList())).isEmpty());
        Assertions.assertEquals(6, executor.execute(request(names(6))).size());
        Assertions.assertEquals(7, executor.execute(request(names(7))).size());
        Assertions.assertEquals(Arrays.asList(3, 3, 3, 3, 1), batchSizes);
    }

    @Test
    public void halvesOversizeBatchAndReusesSafeSize() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(8, (db, table, names) -> {
            attempts.add(names.size());
            if (names.size() > 2) {
                throw remoteFailure("frame too large");
            }
            return infos(names);
        });

        Assertions.assertEquals(10, executor.execute(request(names(10))).size());
        Assertions.assertEquals(Arrays.asList(8, 4, 2, 2, 2, 2, 2), attempts);
    }

    @Test
    public void reportsPhysicalBatchExecutionStats() {
        HmsPartitionBatchExecutor executor = executor(4, (db, table, names) -> {
            if (names.size() > 2) {
                throw remoteFailure("frame too large");
            }
            return infos(names);
        });

        HmsPartitionBatchResult result = executor.executeWithStats(request(names(5)));
        HmsPartitionBatchStats stats = result.getStats();

        Assertions.assertEquals(5, result.getPartitions().size());
        Assertions.assertEquals(5, stats.getRequestedItems());
        Assertions.assertEquals(4, stats.getRpcAttempts());
        Assertions.assertEquals(9, stats.getRpcItems());
        Assertions.assertEquals(4, stats.getLargestBatchSize());
        Assertions.assertEquals(1, stats.getSmallestBatchSize());
        Assertions.assertEquals(1, stats.getFallbackCount());
        Assertions.assertTrue(stats.getLogicalElapsedNanos() >= stats.getRpcElapsedNanos());
        Assertions.assertTrue(stats.getRpcElapsedNanos() >= stats.getMaxRpcElapsedNanos());
    }

    @Test
    public void minimumBatchFailurePropagates() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(2, (db, table, names) -> {
            attempts.add(names.size());
            throw remoteFailure("max message size reached");
        });

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> executor.execute(request(names(2))));
        Assertions.assertEquals(Arrays.asList(2, 1), attempts);
        Assertions.assertTrue(failure.getMessage().contains("failedBatchSize=1"));
        Assertions.assertTrue(failure.getMessage().contains("attempts=2"));
    }

    @Test
    public void ordinaryTransportFailureDoesNotFallback() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(8, (db, table, names) -> {
            attempts.add(names.size());
            throw remoteFailure("connection refused");
        });

        Assertions.assertThrows(HmsClientException.class, () -> executor.execute(request(names(8))));
        Assertions.assertEquals(Collections.singletonList(8), attempts);
    }

    @Test
    public void halvesForHivePartitionRequestLimitMessage() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(4, (db, table, names) -> {
            attempts.add(names.size());
            if (names.size() > 2) {
                throw remoteFailure("Number of partitions scanned (4) exceeds limit (2). "
                        + "This is controlled on the metastore server by hive.metastore.limit.partition.request");
            }
            return infos(names);
        });

        Assertions.assertEquals(4, executor.execute(request(names(4))).size());
        Assertions.assertEquals(Arrays.asList(4, 2, 2), attempts);
    }

    @Test
    public void fallbackTimeoutStopsBeforeNextRetry() {
        AtomicLong time = new AtomicLong();
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = HmsPartitionBatchExecutor.builder()
                .maxBatchSize(8)
                .fallbackTimeoutMillis(30)
                .transport((db, table, names) -> {
                    attempts.add(names.size());
                    throw remoteFailure("request too large");
                })
                .nanoTime(() -> time.getAndAdd(31_000_000L))
                .build();

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> executor.execute(request(names(8))));
        Assertions.assertTrue(failure.getMessage().contains("fallback timeout"));
        Assertions.assertEquals(Collections.singletonList(8), attempts);
    }

    @Test
    public void reportsAllResultMismatchTypesPrecisely() {
        HmsPartitionBatchExecutor executor = executor(10, (db, table, names) -> Arrays.asList(
                info("a"), info("c"), info("c")));

        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> executor.execute(request(Arrays.asList("p=a", "p=b"))));
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
    public void rejectsNullAndMalformedResults() {
        HmsPartitionBatchExecutor nullResponse = executor(10, (db, table, names) -> null);
        HmsPartitionResultException nullFailure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> nullResponse.execute(request(Collections.singletonList("p=a"))));
        Assertions.assertTrue(nullFailure.getMismatchTypes().contains(
                HmsPartitionResultException.MismatchType.INVALID_RESULT));

        HmsPartitionBatchExecutor malformed = executor(10, (db, table, names) -> Collections.singletonList(
                new HmsPartitionInfo(Arrays.asList("a", "extra"), null, null, null, null, null)));
        HmsPartitionResultException malformedFailure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> malformed.execute(request(Collections.singletonList("p=a"))));
        Assertions.assertEquals(1, malformedFailure.getInvalidCount());
        Assertions.assertEquals(1, malformedFailure.getMissingCount());
    }

    @Test
    public void validatesLogicalRequest() {
        Assertions.assertEquals(Collections.singletonList(""), HmsPartitionIdentity.fromName("p="));
        Assertions.assertEquals(Arrays.asList("a/b", "x=y"),
                HmsPartitionIdentity.fromName("p=a%2Fb/q=x%3Dy"));
        Assertions.assertDoesNotThrow(() -> request(Arrays.asList("P=a", "p=b")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Arrays.asList("p=a", "q=b")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Collections.singletonList("p=a/")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Arrays.asList("p=a", "p=a")));
    }

    @Test
    public void parsesAndValidatesConfiguration() {
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

    private static HmsPartitionBatchExecutor executor(int batchSize, HmsPartitionTransport transport) {
        return HmsPartitionBatchExecutor.builder()
                .maxBatchSize(batchSize)
                .fallbackTimeoutMillis(30_000)
                .transport(transport)
                .build();
    }

    private static HmsPartitionRequest request(List<String> names) {
        return HmsPartitionRequest.builder()
                .database("db")
                .table("table")
                .partitionNames(names)
                .build();
    }

    private static HmsPartitionBatchExecutor.RemoteCallException remoteFailure(String message) {
        return new HmsPartitionBatchExecutor.RemoteCallException(
                "remote failure", new shade.doris.hive.org.apache.thrift.TException(message));
    }

    private static List<String> names(int count) {
        return IntStream.range(0, count).mapToObj(i -> "p=" + i).collect(Collectors.toList());
    }

    private static List<HmsPartitionInfo> infos(List<String> names) {
        return names.stream().map(HmsPartitionIdentity::fromName).map(HmsPartitionBatchExecutorTest::info)
                .collect(Collectors.toList());
    }

    private static HmsPartitionInfo info(String value) {
        return info(Collections.singletonList(value));
    }

    private static HmsPartitionInfo info(List<String> values) {
        return new HmsPartitionInfo(values, null, null, null, null, Collections.emptyMap());
    }
}
