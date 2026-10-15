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

package org.apache.doris.datasource.scan;

import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.datasource.split.SplitGenerator;
import org.apache.doris.datasource.split.SplitProducer;
import org.apache.doris.datasource.split.SplitSource;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

class PluginDrivenScanNodePartitionBatchTest {
    @Test
    void fullQueueDoesNotLoadEveryPartitionBatch() throws Exception {
        Fixture fixture = new Fixture(1, Collections.emptySet());
        fixture.start(100, 1, 2);
        fixture.executor.runUntilIdle();
        Assertions.assertEquals(2, fixture.loaded.size(), "Paused producers must retain only two partition lists");
        Assertions.assertEquals(1, fixture.assignment.getAssignedSplits(fixture.backend).size());
        Assertions.assertEquals(0, fixture.finished.get());

        List<Long> actual = fixture.drain();
        List<Long> expected = new ArrayList<>();
        for (long partition = 0; partition < 100; partition++) {
            for (long range = 0; range < 3; range++) {
                expected.add(partition * 10 + range);
            }
        }
        Collections.sort(actual);
        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(100, fixture.loaded.size());
        Assertions.assertEquals(100, new HashSet<>(fixture.loaded).size());
        Assertions.assertEquals(2, fixture.finished.get());
    }

    @Test
    void emptyPartitionBatchesAndPartialLastBatchDoNotEndTheScanEarly() throws Exception {
        for (int parallelism : Arrays.asList(1, 8)) {
            Fixture fixture = new Fixture(2, new HashSet<>(Arrays.asList("0", "1", "4")));
            fixture.start(5, 2, parallelism);
            List<Long> actual = fixture.drain();
            Collections.sort(actual);
            Assertions.assertEquals(Arrays.asList(20L, 21L, 22L, 30L, 31L, 32L), actual);
            Assertions.assertEquals(Arrays.asList(Arrays.asList("0", "1"), Arrays.asList("2", "3"),
                    Collections.singletonList("4")), fixture.loaded);
            Assertions.assertEquals(Math.min(parallelism, 3), fixture.finished.get(),
                    "Do not create more producers than partition batches");
        }
    }

    @Test
    void cancellationDuringEmptyPartitionLoadingStopsFurtherCalls() throws Exception {
        Fixture fixture = new Fixture(1, Collections.emptySet());
        Mockito.doAnswer(call -> {
            fixture.loaded.add(new ArrayList<>(call.<List<String>>getArgument(2)));
            fixture.assignment.stop();
            return Collections.emptyList();
        }).when(fixture.provider).planScanForPartitionBatch(Mockito.any(), Mockito.any(), Mockito.any());
        fixture.start(100, 1, 2);
        fixture.executor.runUntilIdle();
        Assertions.assertEquals(Collections.singletonList(Collections.singletonList("0")), fixture.loaded);
        Assertions.assertEquals(2, fixture.finished.get());
    }

    private static class Fixture {
        final Backend backend = new Backend(0, "127.0.0.1", 9050);
        final ManualExecutor executor = new ManualExecutor();
        final List<List<String>> loaded = new ArrayList<>();
        final AtomicInteger finished = new AtomicInteger();
        final SplitAssignment assignment;
        final PluginDrivenScanNode node;
        final ConnectorScanPlanProvider provider;

        Fixture(int batchSize, Set<String> emptyPartitions) throws Exception {
            FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
            Mockito.when(policy.computeScanRangeAssignment(Mockito.any())).thenAnswer(call -> {
                Multimap<Backend, Split> result = ArrayListMultimap.create();
                result.putAll(backend, call.<List<Split>>getArgument(0));
                return result;
            });
            assignment = new SplitAssignment(policy, Mockito.mock(SplitGenerator.class),
                    (be, properties, split, keys, admission) -> new TScanRangeLocations().setLocations(
                            Collections.singletonList(new TScanRangeLocation().setBackendId(split.getStart()))),
                    Collections.emptyMap(), Collections.emptyList(), true, batchSize, 1);
            node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
            Deencapsulation.setField(node, "splitAssignment", assignment);
            provider = Mockito.mock(ConnectorScanPlanProvider.class);
            Mockito.when(provider.planScanForPartitionBatch(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenAnswer(call -> {
                        List<String> partitions = call.getArgument(2);
                        loaded.add(new ArrayList<>(partitions));
                        List<ConnectorScanRange> ranges = new ArrayList<>();
                        for (String partition : partitions) {
                            if (emptyPartitions.contains(partition)) {
                                continue;
                            }
                            for (int offset = 0; offset < 3; offset++) {
                                long identity = Long.parseLong(partition) * 10 + offset;
                                ranges.add(new ConnectorScanRange() {
                                    @Override
                                    public long getStart() {
                                        return identity;
                                    }

                                    @Override
                                    public Map<String, String> getProperties() {
                                        return Collections.emptyMap();
                                    }
                                });
                            }
                        }
                        return ranges;
                    });
        }

        void start(int partitionCount, int partitionsPerBatch, int parallelism) {
            List<String> partitions = new ArrayList<>();
            for (int partition = 0; partition < partitionCount; partition++) {
                partitions.add(String.valueOf(partition));
            }
            PluginDrivenScanNode.SubmittedTaskFinalizer finalizer =
                    new PluginDrivenScanNode.SubmittedTaskFinalizer(assignment::finishSchedule);
            for (SplitProducer.BatchSource source : node.createPartitionBatchSources(
                    provider, null, partitions, partitionsPerBatch, parallelism)) {
                finalizer.taskSubmitted();
                new SplitProducer(assignment, executor, source, failure -> {
                    if (failure != null) {
                        assignment.setException(new UserException(failure.getMessage(), failure));
                    }
                    finished.incrementAndGet();
                    finalizer.taskFinished();
                }).start();
            }
            finalizer.closeDispatch();
        }

        List<Long> drain() throws Exception {
            SplitSource consumer = new SplitSource(backend, assignment, 1000);
            List<Long> identities = new ArrayList<>();
            for (int request = 0; request < 1000; request++) {
                executor.runUntilIdle();
                List<TScanRangeLocations> ranges = consumer.getNextBatch(1);
                if (ranges.isEmpty()) {
                    return identities;
                }
                Assertions.assertEquals(1, ranges.size());
                identities.add(ranges.get(0).getLocations().get(0).getBackendId());
            }
            throw new AssertionError("Partition sources did not finish");
        }
    }

    private static class ManualExecutor implements Executor {
        final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        void runUntilIdle() {
            for (int task = 0; task < 1000 && !tasks.isEmpty(); task++) {
                tasks.remove().run();
            }
            Assertions.assertTrue(tasks.isEmpty(), "Producers must yield when the queue is full");
        }
    }
}
