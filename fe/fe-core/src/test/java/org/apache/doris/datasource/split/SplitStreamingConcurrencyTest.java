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

package org.apache.doris.datasource.split;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.scan.FederationBackendPolicy;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class SplitStreamingConcurrencyTest {
    @Test
    void consumerCreatedQueuesStayBoundedAcrossBackends() throws Exception {
        verifyConcurrentStreaming(true);
    }

    @Test
    void producerCreatedQueuesStayBoundedAcrossBackends() throws Exception {
        verifyConcurrentStreaming(false);
    }

    private void verifyConcurrentStreaming(boolean consumerFirst) throws Exception {
        int producerBatchSize = 7;
        int queueCapacity = 2;
        int splitCount = 10003;
        int[] requestedSizes = {1, 10, 16};
        List<Backend> backends = new ArrayList<>();
        for (int i = 0; i < requestedSizes.length; i++) {
            backends.add(new Backend(i, "127.0.0.1", 9050 + i));
        }
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.any())).thenAnswer(invocation -> {
            List<Split> batch = invocation.getArgument(0);
            Assertions.assertTrue(batch.size() <= producerBatchSize);
            Multimap<Backend, Split> assigned = ArrayListMultimap.create();
            for (Split split : batch) {
                assigned.put(backends.get((int) (split.getStart() % backends.size())), split);
            }
            return assigned;
        });
        CountDownLatch queuesFilled = new CountDownLatch(1);
        SplitAssignment assignment = new SplitAssignment(policy, Mockito.mock(SplitGenerator.class),
                (backend, properties, split, keys, admission) -> {
                    // Reaching batch three proves all queues received the first two batches.
                    if (split.getStart() >= producerBatchSize * queueCapacity) {
                        queuesFilled.countDown();
                    }
                    // Preserve a unique split identity through assignment and residual batches.
                    TFileScanRange files = new TFileScanRange().setParams(new TFileScanRangeParams())
                            .setRanges(Collections.singletonList(new TFileRangeDesc().setPath("file-" + split.getStart())));
                    return new TScanRangeLocations().setScanRange(new TScanRange().setExtScanRange(
                            new TExternalScanRange().setFileScanRange(files)))
                            .setLocations(Collections.singletonList(
                                    new TScanRangeLocation().setBackendId(split.getStart())));
                }, Collections.emptyMap(), Collections.emptyList(), true, producerBatchSize, queueCapacity);
        List<Split> splits = new ArrayList<>();
        LocationPath path = LocationPath.of("s3://bucket/file.parquet");
        for (int i = 0; i < splitCount; i++) {
            splits.add(new FileSplit(path, i, 1, splitCount, 0, null, Collections.emptyList()));
        }
        List<BlockingQueue<Collection<TScanRangeLocations>>> queues = new ArrayList<>();
        if (consumerFirst) {
            for (Backend backend : backends) {
                queues.add(assignment.getAssignedSplits(backend));
            }
        }
        ExecutorService executor = Executors.newFixedThreadPool(backends.size() + 1);
        try {
            Future<?> producer = executor.submit(() -> {
                assignment.addToQueue(splits);
                assignment.finishSchedule();
                return null;
            });
            Assertions.assertTrue(queuesFilled.await(5, TimeUnit.SECONDS));
            if (!consumerFirst) {
                for (Backend backend : backends) {
                    queues.add(assignment.getAssignedSplits(backend));
                }
            }
            for (BlockingQueue<Collection<TScanRangeLocations>> queue : queues) {
                Assertions.assertEquals(queueCapacity, queue.size());
                Assertions.assertEquals(0, queue.remainingCapacity());
                verifyQueue(queue, producerBatchSize, queueCapacity);
            }
            List<Future<Set<Long>>> consumers = new ArrayList<>();
            for (int i = 0; i < backends.size(); i++) {
                int backendIndex = i;
                SplitSource source = new SplitSource(backends.get(i), assignment, 10000);
                consumers.add(executor.submit(() -> {
                    Set<Long> identities = new HashSet<>();
                    while (true) {
                        verifyQueue(queues.get(backendIndex), producerBatchSize, queueCapacity);
                        List<TScanRangeLocations> batch = source.getNextBatch(requestedSizes[backendIndex]);
                        Assertions.assertTrue(batch.size() <= requestedSizes[backendIndex]);
                        if (batch.isEmpty()) {
                            Assertions.assertTrue(source.getNextBatch(1).isEmpty());
                            return identities;
                        }
                        for (TScanRangeLocations range : batch) {
                            long identity = range.getLocations().get(0).getBackendId();
                            Assertions.assertEquals(backendIndex, identity % backends.size());
                            Assertions.assertTrue(identities.add(identity), "Duplicate split " + identity);
                        }
                    }
                }));
            }
            producer.get(30, TimeUnit.SECONDS);
            Set<Long> actual = new HashSet<>();
            for (Future<Set<Long>> consumer : consumers) {
                actual.addAll(consumer.get(30, TimeUnit.SECONDS));
            }
            Assertions.assertEquals(splitCount, actual.size());
            for (long i = 0; i < splitCount; i++) {
                Assertions.assertTrue(actual.contains(i), "Missing split " + i);
            }
            System.out.printf("Split streaming: consumerFirst=%s, backends=%d, splits=%d, "
                            + "producerBatchLimit=%d, observedQueuePeak=%d, requests=[1,10,16]%n",
                    consumerFirst, backends.size(), splitCount, producerBatchSize, queueCapacity);
        } finally {
            assignment.stop();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private void verifyQueue(BlockingQueue<Collection<TScanRangeLocations>> queue,
            int producerBatchSize, int queueCapacity) {
        Assertions.assertTrue(queue.size() <= queueCapacity);
        for (Collection<TScanRangeLocations> batch : queue) {
            Assertions.assertTrue(batch.size() <= producerBatchSize);
        }
    }
}
