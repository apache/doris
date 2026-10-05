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
        List<HmsPartitionInfo> result = executor.executeWithStats(request(names)).getPartitions();

        Assertions.assertEquals(24, batchSizes.size());
        Assertions.assertTrue(batchSizes.stream().allMatch(size -> size == 5000));
        Assertions.assertEquals(names.stream().map(HmsPartitionBatchExecutorTest::values)
                        .collect(Collectors.toList()),
                result.stream().map(HmsPartitionInfo::getValues).collect(Collectors.toList()));
    }

    @Test
    public void handlesEmptyExactAndTrailingBatches() {
        List<Integer> batchSizes = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(3, (db, table, names) -> {
            batchSizes.add(names.size());
            return infos(names);
        });

        Assertions.assertTrue(executor.executeWithStats(request(Collections.emptyList())).getPartitions().isEmpty());
        Assertions.assertEquals(6, executor.executeWithStats(request(names(6))).getPartitions().size());
        Assertions.assertEquals(7, executor.executeWithStats(request(names(7))).getPartitions().size());
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

        Assertions.assertEquals(10, executor.executeWithStats(request(names(10))).getPartitions().size());
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
        Assertions.assertEquals(4, stats.getTransportInvocations());
        Assertions.assertEquals(9, stats.getTransportItems());
        Assertions.assertEquals(4, stats.getLargestBatchSize());
        Assertions.assertEquals(1, stats.getSmallestBatchSize());
        Assertions.assertEquals(1, stats.getFallbackCount());
        Assertions.assertTrue(stats.getLogicalElapsedNanos() >= stats.getTransportElapsedNanos());
        Assertions.assertTrue(stats.getTransportElapsedNanos() >= stats.getMaxTransportElapsedNanos());
    }

    @Test
    public void minimumBatchFailurePropagates() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(2, (db, table, names) -> {
            attempts.add(names.size());
            throw remoteFailure("max message size reached");
        });

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> executor.executeWithStats(request(names(2))));
        Assertions.assertEquals(Arrays.asList(2, 1), attempts);
        Assertions.assertTrue(failure.getMessage().contains("failedBatchSize=1"));
        Assertions.assertTrue(failure.getMessage().contains("transportInvocations=2"));
        HmsPartitionBatchStats stats = failure.getPartitionBatchStats();
        Assertions.assertNotNull(stats);
        Assertions.assertEquals(2, stats.getRequestedItems());
        Assertions.assertEquals(2, stats.getTransportInvocations());
        Assertions.assertEquals(3, stats.getTransportItems());
        Assertions.assertEquals(2, stats.getLargestBatchSize());
        Assertions.assertEquals(1, stats.getSmallestBatchSize());
        Assertions.assertEquals(1, stats.getFallbackCount());
        Assertions.assertTrue(stats.getLogicalElapsedNanos() >= stats.getTransportElapsedNanos());
    }

    @Test
    public void ordinaryTransportFailureDoesNotFallback() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchExecutor executor = executor(8, (db, table, names) -> {
            attempts.add(names.size());
            throw remoteFailure("connection refused");
        });

        HmsClientException failure = Assertions.assertThrows(
                HmsClientException.class, () -> executor.executeWithStats(request(names(8))));
        Assertions.assertEquals(Collections.singletonList(8), attempts);
        HmsPartitionBatchStats stats = failure.getPartitionBatchStats();
        Assertions.assertNotNull(stats);
        Assertions.assertEquals(8, stats.getRequestedItems());
        Assertions.assertEquals(1, stats.getTransportInvocations());
        Assertions.assertEquals(8, stats.getTransportItems());
        Assertions.assertEquals(8, stats.getLargestBatchSize());
        Assertions.assertEquals(8, stats.getSmallestBatchSize());
        Assertions.assertEquals(0, stats.getFallbackCount());
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

        Assertions.assertEquals(4, executor.executeWithStats(request(names(4))).getPartitions().size());
        Assertions.assertEquals(Arrays.asList(4, 2, 2), attempts);
    }

    @Test
    public void reportsAllResultMismatchCategoriesPrecisely() {
        HmsPartitionBatchExecutor executor = executor(10, (db, table, names) -> Arrays.asList(
                info("a"), info("c"), info("c")));

        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> executor.executeWithStats(request(Arrays.asList("p=a", "p=b"))));
        Assertions.assertTrue(failure.getMessage().contains("missing=1"));
        Assertions.assertTrue(failure.getMessage().contains("duplicate=1"));
        Assertions.assertTrue(failure.getMessage().contains("unexpected=1"));
        Assertions.assertTrue(failure.getMessage().contains("missingSamples=[p=b]"));
        Assertions.assertTrue(failure.getMessage().contains("duplicateSamples=[[c]]"));
        Assertions.assertTrue(failure.getMessage().contains("unexpectedSamples=[[c]]"));
    }

    @Test
    public void existingPartitionModeOmitsOnlyMissingResults() {
        HmsPartitionBatchExecutor executor = executor(10, (db, table, names) ->
                Arrays.asList(info("c"), info("a")));

        List<HmsPartitionInfo> existing = executor.executeExistingWithStats(
                request(Arrays.asList("p=a", "p=b", "p=c"))).getPartitions();

        Assertions.assertEquals(Arrays.asList("a", "c"), existing.stream()
                .map(partition -> partition.getValues().get(0)).collect(Collectors.toList()));
    }

    @Test
    public void existingPartitionModeStillRejectsUnexpectedAndDuplicateResults() {
        HmsPartitionBatchExecutor executor = executor(10, (db, table, names) ->
                Arrays.asList(info("a"), info("a"), info("unexpected")));

        HmsPartitionResultException failure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> executor.executeExistingWithStats(request(Arrays.asList("p=a", "p=missing"))));

        Assertions.assertTrue(failure.getMessage().contains("missing=0"));
        Assertions.assertTrue(failure.getMessage().contains("duplicate=1"));
        Assertions.assertTrue(failure.getMessage().contains("unexpected=1"));
    }

    @Test
    public void rejectsNullAndMalformedResults() {
        HmsPartitionBatchExecutor nullResponse = executor(10, (db, table, names) -> null);
        HmsPartitionResultException nullFailure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> nullResponse.executeWithStats(request(Collections.singletonList("p=a"))));
        Assertions.assertTrue(nullFailure.getMessage().contains("invalid=1"));

        HmsPartitionBatchExecutor malformed = executor(10, (db, table, names) -> Collections.singletonList(
                new HmsPartitionInfo(Arrays.asList("a", "extra"), null, null, null, null, null)));
        HmsPartitionResultException malformedFailure = Assertions.assertThrows(
                HmsPartitionResultException.class,
                () -> malformed.executeWithStats(request(Collections.singletonList("p=a"))));
        Assertions.assertTrue(malformedFailure.getMessage().contains("invalid=1"));
        Assertions.assertTrue(malformedFailure.getMessage().contains("missing=1"));
    }

    @Test
    public void validatesLogicalRequest() {
        Assertions.assertEquals(Collections.singletonList(""), values("p="));
        Assertions.assertEquals(Arrays.asList("a/b", "x=y"),
                values("p=a%2Fb/q=x%3Dy"));
        Assertions.assertDoesNotThrow(() -> request(Arrays.asList("P=a", "p=b")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Arrays.asList("p=a", "q=b")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Collections.singletonList("p=a/")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> request(Arrays.asList("p=a", "p=a")));
    }

    @Test
    public void parsesAndValidatesConfiguration() {
        HmsClientConfig defaults = new HmsClientConfig(Collections.emptyMap(), 0);
        Assertions.assertEquals(5000, defaults.getPartitionBatchSize());

        Map<String, String> properties = new HashMap<>();
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "321");
        HmsClientConfig config = new HmsClientConfig(properties, 1);
        Assertions.assertEquals(321, config.getPartitionBatchSize());

        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "0");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsClientConfig(properties, 1));
        properties.put(HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "not-a-number");
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HmsClientConfig(properties, 1));
    }

    private static HmsPartitionBatchExecutor executor(int batchSize, HmsPartitionTransport transport) {
        return new HmsPartitionBatchExecutor(batchSize, transport);
    }

    private static HmsPartitionRequest request(List<String> names) {
        return new HmsPartitionRequest("db", "table", names);
    }

    private static List<String> values(String name) {
        return request(Collections.singletonList(name)).getPartitions().get(0).getValues();
    }

    private static HmsPartitionBatchExecutor.RemoteCallException remoteFailure(String message) {
        return new HmsPartitionBatchExecutor.RemoteCallException(
                "remote failure", new shade.doris.hive.org.apache.thrift.TException(message));
    }

    private static List<String> names(int count) {
        return IntStream.range(0, count).mapToObj(i -> "p=" + i).collect(Collectors.toList());
    }

    private static List<HmsPartitionInfo> infos(List<String> names) {
        return names.stream().map(HmsPartitionBatchExecutorTest::values).map(HmsPartitionBatchExecutorTest::info)
                .collect(Collectors.toList());
    }

    private static HmsPartitionInfo info(String value) {
        return info(Collections.singletonList(value));
    }

    private static HmsPartitionInfo info(List<String> values) {
        return new HmsPartitionInfo(values, null, null, null, null, Collections.emptyMap());
    }
}
