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

import org.apache.doris.catalog.Env;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFetchSplitBatchRequest;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Timeout(30)
class SplitConsumerCloseTest {
    @Test
    @SuppressWarnings("unchecked")
    void ownerCloseReleasesAFullBackendQueueWithoutDroppingOtherBackendSplits() throws Exception {
        Backend stopped = new Backend(1, "localhost", 0);
        Backend live = new Backend(2, "localhost", 0);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.numBackends()).thenReturn(2);
        Split first = Mockito.mock(Split.class);
        Split second = Mockito.mock(Split.class);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
            Multimap<Backend, Split> result = ArrayListMultimap.create();
            for (Split split : call.<java.util.List<Split>>getArgument(0)) {
                result.put(split == first ? stopped : live, split);
            }
            return result;
        });
        SplitAssignment assignment = new SplitAssignment(policy, new SplitGenerator() {},
                (be, props, split, keys, admission) -> new TScanRangeLocations(),
                Collections.emptyMap(), Collections.emptyList(), true, 2, 1);
        CountDownLatch offering = new CountDownLatch(1);
        CountDownLatch allowOffer = new CountDownLatch(1);
        AtomicBoolean delayNext = new AtomicBoolean(false);
        BlockingQueue<TScanRangeLocations> stoppedQueue = new LinkedBlockingQueue<TScanRangeLocations>(1) {
            @Override
            public boolean offer(TScanRangeLocations value, long timeout, TimeUnit unit) throws InterruptedException {
                if (delayNext.compareAndSet(true, false)) {
                    offering.countDown();
                    Assertions.assertTrue(allowOffer.await(5, TimeUnit.SECONDS));
                }
                return super.offer(value, timeout, unit);
            }
        };
        Field queues = SplitAssignment.class.getDeclaredField("assignment");
        queues.setAccessible(true);
        ((Map<Long, BlockingQueue<TScanRangeLocations>>) queues.get(assignment)).put(stopped.getId(), stoppedQueue);
        assignment.addToQueue(Collections.singletonList(first));
        SplitSource stoppedSource = new SplitSource(stopped, assignment, 1000);
        SplitSource liveSource = new SplitSource(live, assignment, 1000);
        SplitSourceManager manager = new SplitSourceManager();
        manager.registerSplitSource(stoppedSource);
        manager.registerSplitSource(liveSource);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getSplitSourceManager()).thenReturn(manager);
        FrontendServiceImpl service = Mockito.mock(FrontendServiceImpl.class, Mockito.CALLS_REAL_METHODS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        delayNext.set(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Future<?> produced = executor.submit(() -> {
                assignment.addToQueue(Arrays.asList(first, second));
                assignment.finishSchedule();
                return null;
            });
            Assertions.assertTrue(offering.await(5, TimeUnit.SECONDS));
            TFetchSplitBatchRequest request = new TFetchSplitBatchRequest()
                    .setSplitSourceId(stoppedSource.getUniqueId()).setMaxNumSplits(8).setCloseSource(true);
            Assertions.assertEquals(TStatusCode.OK, service.fetchSplitBatch(request).getStatus().getStatusCode());
            Assertions.assertNull(manager.getSplitSource(stoppedSource.getUniqueId()));
            Assertions.assertFalse(assignment.isStop());
            Assertions.assertEquals(TStatusCode.OK, service.fetchSplitBatch(request).getStatus().getStatusCode());
            allowOffer.countDown();
            produced.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(stoppedQueue.isEmpty(), "an offer racing close must not retain descriptors");
            Assertions.assertNull(assignment.getAssignedSplits(stopped));
            Assertions.assertEquals(1, liveSource.getNextBatch(2).size());
            Assertions.assertTrue(liveSource.isLastBatch());
            request.setSplitSourceId(liveSource.getUniqueId());
            Assertions.assertEquals(TStatusCode.OK, service.fetchSplitBatch(request).getStatus().getStatusCode());
            Assertions.assertTrue(assignment.isStop());
        } finally {
            allowOffer.countDown();
            assignment.stop();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
