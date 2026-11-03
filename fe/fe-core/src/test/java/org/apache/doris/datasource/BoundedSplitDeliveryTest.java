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
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
class BoundedSplitDeliveryTest {
    @Test
    void nonPositiveFetchWaitIsRejected() {
        Backend backend = new Backend(1, "localhost", 0);
        SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SplitSource(backend, assignment, 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new SplitSource(backend, assignment, -1));
    }

    @Test
    void defaultsAndValidation() throws Exception {
        SessionVariable variables = new SessionVariable();
        Assertions.assertEquals(1, variables.getNumPartitionsInBatchMode());
        Assertions.assertEquals(1, variables.getNumFilesInBatchMode());
        Assertions.assertEquals(8, variables.splitBatchSize);
        Assertions.assertEquals(1024, variables.splitQueueCapacity);
        Assertions.assertEquals(16, variables.splitPartitionBatchSize);
        Assertions.assertThrows(Exception.class, () -> variables.checkSplitQueueCapacity("0"));
        Assertions.assertThrows(Exception.class, () -> variables.checkSplitBatchSize("-1"));
        Assertions.assertThrows(Exception.class, () -> variables.checkSplitPartitionBatchSize("0"));
        variables.checkSplitQueueCapacity("1");
        Assertions.assertThrows(Exception.class, () -> variables.checkFetchSplitsMaxWaitTime("0"));
        Assertions.assertThrows(Exception.class, () -> variables.checkFetchSplitsMaxWaitTime("-1"));
        variables.checkFetchSplitsMaxWaitTime("1");
    }

    @Test
    void producerBatchLargerThanQueueStartsAndDrainsWithoutLoss() throws Exception {
        Backend backend = new Backend(1, "localhost", 0);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
            Multimap<Backend, Split> assigned = ArrayListMultimap.create();
            for (Split split : call.<List<Split>>getArgument(0)) {
                assigned.put(backend, split);
            }
            return assigned;
        });
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            splits.add(Mockito.mock(Split.class));
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<SplitAssignment> holder = new AtomicReference<>();
        CompletableFuture<Void> published = new CompletableFuture<>();
        SplitGenerator generator = new SplitGenerator() {
            @Override
            public void startSplit(int ignored) {
                executor.execute(() -> {
                    try {
                        holder.get().addToQueue(splits);
                        holder.get().finishSchedule();
                        published.complete(null);
                    } catch (Throwable e) {
                        published.completeExceptionally(e);
                    }
                });
            }
        };
        SplitAssignment assignment = new SplitAssignment(policy, generator,
                (be, properties, split, keys, admission) -> new TScanRangeLocations(),
                Collections.emptyMap(), Collections.emptyList(), true, 8, 2);
        holder.set(assignment);
        try {
            // init must return after the first published split, before the full batch fits.
            assignment.init();
            Assertions.assertNotNull(assignment.getSampleSplit());
            Assertions.assertEquals(2, assignment.getAssignedSplits(backend).size()
                    + assignment.getAssignedSplits(backend).remainingCapacity());
            SplitSource source = new SplitSource(backend, assignment, 1000);
            int count = 0;
            while (!source.isLastBatch()) {
                List<TScanRangeLocations> batch = source.getNextBatch(3);
                Assertions.assertTrue(batch.size() <= 3);
                count += batch.size();
            }
            published.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(9, count);
        } finally {
            assignment.stop();
            executor.shutdownNow();
        }
    }

    @Test
    void liveEmptySourceIsIncompleteAndFinishedSourceIsEof() throws Exception {
        SplitAssignment assignment = new SplitAssignment(Mockito.mock(FederationBackendPolicy.class),
                new SplitGenerator() { }, (be, properties, split, keys, admission) -> new TScanRangeLocations(),
                Collections.emptyMap(), Collections.emptyList(), true);
        SplitSource source = new SplitSource(new Backend(1, "localhost", 0), assignment, 1);
        Assertions.assertTrue(source.getNextBatch(1).isEmpty());
        Assertions.assertFalse(source.isLastBatch());
        assignment.finishSchedule();
        Assertions.assertTrue(source.getNextBatch(1).isEmpty());
        Assertions.assertTrue(source.isLastBatch());
    }

    @Test
    void producerFailureIsAnErrorRatherThanIncomplete() {
        SplitAssignment assignment = new SplitAssignment(Mockito.mock(FederationBackendPolicy.class),
                new SplitGenerator() { }, (be, properties, split, keys, admission) -> new TScanRangeLocations(),
                Collections.emptyMap(), Collections.emptyList(), true);
        SplitSource source = new SplitSource(new Backend(1, "localhost", 0), assignment, 1);
        assignment.setException(new UserException("listing failed"));
        Assertions.assertThrows(UserException.class, () -> source.getNextBatch(1));
    }
}
