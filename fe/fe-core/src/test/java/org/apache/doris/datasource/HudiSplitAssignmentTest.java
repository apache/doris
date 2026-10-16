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
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hudi.source.HudiScanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class HudiSplitAssignmentTest {
    @Test
    void hudiEagerFactoriesInitializeTwoScansOnOneWorkerBeforeConsumption() throws Exception {
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < 4097; i++) {
            splits.add(new FileSplit(LocationPath.of("s3://bucket/file.parquet"), i, 1,
                    4097, 0, null, Collections.emptyList()));
        }
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService planner = Executors.newSingleThreadExecutor();
        SplitAssignment first = eagerAssignment(backend, worker, splits);
        SplitAssignment second = eagerAssignment(backend, worker, splits);
        try {
            Future<?> initialized = planner.submit(() -> {
                first.init();
                second.init();
                return null;
            });
            initialized.get(5, TimeUnit.SECONDS);
            worker.submit(() -> { }).get(5, TimeUnit.SECONDS);
            for (SplitAssignment assignment : new SplitAssignment[] {first, second}) {
                Assertions.assertEquals(Integer.MAX_VALUE, assignment.getSplitBatchSize());
                Assertions.assertEquals(1, assignment.getAssignedSplits(backend).size());
                Assertions.assertEquals(9999, assignment.getAssignedSplits(backend).remainingCapacity());
                SplitSource source = new SplitSource(backend, assignment, 1000);
                Set<Long> actual = new HashSet<>();
                while (true) {
                    List<TScanRangeLocations> batch = source.getNextBatch(7);
                    Assertions.assertTrue(batch.size() <= 7);
                    if (batch.isEmpty()) {
                        break;
                    }
                    for (TScanRangeLocations range : batch) {
                        Assertions.assertTrue(actual.add(range.getLocations().get(0).getBackendId()));
                    }
                }
                Assertions.assertEquals(4097, actual.size());
                for (long i = 0; i < 4097; i++) {
                    Assertions.assertTrue(actual.contains(i));
                }
            }
        } finally {
            first.stop();
            second.stop();
            planner.shutdownNow();
            worker.shutdownNow();
            Assertions.assertTrue(planner.awaitTermination(5, TimeUnit.SECONDS));
            Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private SplitAssignment eagerAssignment(Backend backend, Executor worker, List<Split> splits) throws Exception {
        FileQueryScanNode node = Mockito.mock(HudiScanNode.class, Mockito.CALLS_REAL_METHODS);
        node.sessionVariable = new SessionVariable();
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.numBackends()).thenReturn(1);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
            Multimap<Backend, Split> batch = ArrayListMultimap.create();
            for (Split split : call.<List<Split>>getArgument(0)) {
                batch.put(backend, split);
            }
            return batch;
        });
        Field policyField = ExternalScanNode.class.getDeclaredField("backendPolicy");
        policyField.setAccessible(true);
        policyField.set(node, policy);
        Mockito.doAnswer(call -> new TScanRangeLocations().setLocations(Collections.singletonList(
                new TScanRangeLocation().setBackendId(call.<Split>getArgument(2).getStart()))))
                .when(node).splitToScanRange(Mockito.any(), Mockito.anyMap(), Mockito.any(),
                        Mockito.anyList(), Mockito.anyBoolean());
        // Exercise virtual dispatch through the actual Hudi factory; replace only remote listing
        // with the eager submission contract used by HudiScanNode's partition task.
        SplitAssignment assignment = node.createSplitAssignment(
                Collections.emptyMap(), Collections.emptyList(), true);
        Mockito.doAnswer(call -> {
            worker.execute(() -> {
                try {
                    assignment.addToQueue(splits);
                } catch (UserException e) {
                    assignment.setException(e);
                } finally {
                    assignment.finishSchedule();
                }
            });
            return null;
        }).when(node).startSplit(Mockito.anyInt());
        return assignment;
    }
}
