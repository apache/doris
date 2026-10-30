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

package org.apache.doris.datasource;

import org.apache.doris.common.UserException;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SplitSourceTest {
    private Backend backend;
    private SplitAssignment assignment;
    private List<Split> splits;
    private List<TScanRangeLocations> ranges;

    @BeforeEach
    void setUp() throws Exception {
        backend = Mockito.mock(Backend.class);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.any())).thenAnswer(invocation -> {
            Multimap<Backend, Split> batch = ArrayListMultimap.create();
            for (Split split : invocation.<List<Split>>getArgument(0)) {
                batch.put(backend, split);
            }
            return batch;
        });
        splits = new ArrayList<>();
        ranges = new ArrayList<>();
        IdentityHashMap<Split, TScanRangeLocations> locations = new IdentityHashMap<>();
        for (int i = 0; i < 8; i++) {
            Split split = Mockito.mock(Split.class);
            TScanRangeLocations range = Mockito.mock(TScanRangeLocations.class);
            splits.add(split);
            ranges.add(range);
            locations.put(split, range);
        }
        assignment = new SplitAssignment(policy, Mockito.mock(SplitGenerator.class),
                (be, properties, split, keys, admission) -> locations.get(split),
                Collections.emptyMap(), Collections.emptyList(), true, 3, 3);
    }

    @Test
    void drainPendingBatchBeforeFinishedQueueBecomesEof() throws Exception {
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000);

        List<TScanRangeLocations> actual = new ArrayList<>();
        for (int requested : Arrays.asList(2, 1, 4, 1)) {
            List<TScanRangeLocations> batch = source.getNextBatch(requested);
            Assertions.assertEquals(requested, batch.size());
            actual.addAll(batch);
        }
        Assertions.assertEquals(ranges, actual);
        Assertions.assertTrue(source.getNextBatch(2).isEmpty());
        Assertions.assertTrue(source.getNextBatch(2).isEmpty());
    }

    @Test
    void producerBatchesAndConsumerCreatedQueueRespectConfiguredLimits() throws Exception {
        BlockingQueue<Collection<TScanRangeLocations>> queue = assignment.getAssignedSplits(backend);
        Assertions.assertEquals(3, queue.remainingCapacity());
        assignment.addToQueue(splits);
        Assertions.assertEquals(3, queue.size());
        Assertions.assertEquals(Arrays.asList(3, 3, 2),
                queue.stream().map(Collection::size).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    void fullQueueResumesAfterConsumerFetch() throws Exception {
        assignment.addToQueue(splits);
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> producer = executor.submit(() -> {
                started.countDown();
                assignment.addToQueue(Collections.singletonList(splits.get(0)));
                return null;
            });
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(TimeoutException.class, () -> producer.get(200, TimeUnit.MILLISECONDS));
            SplitSource source = new SplitSource(backend, assignment, 1000);
            Assertions.assertEquals(ranges.subList(0, 1), source.getNextBatch(1));
            producer.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(3, assignment.getAssignedSplits(backend).size());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void availablePartialBatchDoesNotWaitForProducer() throws Exception {
        assignment.addToQueue(splits.subList(0, 1));
        SplitSource source = new SplitSource(backend, assignment, 5000);
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                Assertions.assertEquals(ranges.subList(0, 1), source.getNextBatch(100)));
    }

    @Test
    void timeoutIsAnErrorRatherThanEof() {
        SplitSource source = new SplitSource(backend, assignment, 20);
        UserException failure = Assertions.assertThrows(UserException.class, () -> source.getNextBatch(1));
        Assertions.assertTrue(failure.getMessage().contains("Timeout"));
    }

    @Test
    void errorIsCheckedWhilePendingSplitsRemain() throws Exception {
        assignment.addToQueue(splits.subList(0, 3));
        SplitSource source = new SplitSource(backend, assignment, 1000);
        source.getNextBatch(1);
        UserException failure = new UserException("failed to generate splits");
        assignment.setException(failure);
        Assertions.assertSame(failure, Assertions.assertThrows(UserException.class, () -> source.getNextBatch(1)));
    }

    @Test
    void concurrentRequestsDoNotDuplicateOrLosePendingSplits() throws Exception {
        assignment.addToQueue(splits);
        assignment.finishSchedule();
        SplitSource source = new SplitSource(backend, assignment, 1000);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<TScanRangeLocations>> first = executor.submit(() -> source.getNextBatch(2));
            Future<List<TScanRangeLocations>> second = executor.submit(() -> source.getNextBatch(2));
            List<TScanRangeLocations> actual = new ArrayList<>(first.get(5, TimeUnit.SECONDS));
            actual.addAll(second.get(5, TimeUnit.SECONDS));
            actual.addAll(source.getNextBatch(8));
            Set<TScanRangeLocations> unique = Collections.newSetFromMap(new IdentityHashMap<>());
            unique.addAll(actual);
            Assertions.assertEquals(8, actual.size());
            Assertions.assertEquals(8, unique.size());
            Assertions.assertTrue(unique.containsAll(ranges));
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationUnblocksProducerAndClosesLateResources() throws Exception {
        assignment.addToQueue(splits);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> producer = executor.submit(() -> {
                assignment.addToQueue(Collections.singletonList(splits.get(0)));
                return null;
            });
            Assertions.assertThrows(TimeoutException.class, () -> producer.get(200, TimeUnit.MILLISECONDS));
            assignment.stop();
            producer.get(5, TimeUnit.SECONDS);
            CountDownLatch closed = new CountDownLatch(1);
            assignment.addCloseable(closed::countDown);
            Assertions.assertEquals(0, closed.getCount());
            Assertions.assertTrue(new SplitSource(backend, assignment, 1000).getNextBatch(1).isEmpty());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void invalidRequestSizeDoesNotReturnFalseEof() {
        SplitSource source = new SplitSource(backend, assignment, 1000);
        Assertions.assertThrows(IllegalArgumentException.class, () -> source.getNextBatch(0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> source.getNextBatch(-1));
    }
}
