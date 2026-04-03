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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleBucketJoinAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests that bucket shuffle + pooling (LocalShuffle) produces correct destinations:
 * - destinations count == bucketNum
 * - same BE's buckets all point to the same (first) instance
 *
 * Uses reflection to call DistributePlanner.sortDestinationInstancesByBuckets (private method).
 */
public class BucketShuffleDestinationTest {

    /**
     * Simulate 3 workers with 6 buckets (distribution: worker0=[0,3], worker1=[1,4], worker2=[2,5]).
     * Input = first-per-worker instances (simulating filterInstancesWhichCanReceiveDataFromRemote output).
     * Each first instance's BucketScanSource contains ALL buckets for that worker.
     * sortDestinationInstancesByBuckets should produce 6 destinations where same-worker
     * buckets point to the same instance.
     */
    @Test
    public void testBucketShufflePoolingDestinations() throws Exception {
        DistributedPlanWorker worker0 = Mockito.mock(DistributedPlanWorker.class);
        DistributedPlanWorker worker1 = Mockito.mock(DistributedPlanWorker.class);
        DistributedPlanWorker worker2 = Mockito.mock(DistributedPlanWorker.class);
        Mockito.when(worker0.id()).thenReturn(100L);
        Mockito.when(worker1.id()).thenReturn(200L);
        Mockito.when(worker2.id()).thenReturn(300L);

        ScanNode mockScanNode = Mockito.mock(ScanNode.class);
        UnassignedJob mockJob = Mockito.mock(UnassignedJob.class);

        // Worker0 first instance: has buckets 0 and 3
        BucketScanSource source0 = makeBucketScanSource(mockScanNode, 0, 3);
        LocalShuffleBucketJoinAssignedJob inst0 = new LocalShuffleBucketJoinAssignedJob(
                0, 0, new TUniqueId(0, 0), mockJob, worker0, source0, ImmutableSet.of(0, 3));

        // Worker1 first instance: has buckets 1 and 4
        BucketScanSource source1 = makeBucketScanSource(mockScanNode, 1, 4);
        LocalShuffleBucketJoinAssignedJob inst1 = new LocalShuffleBucketJoinAssignedJob(
                1, 0, new TUniqueId(0, 1), mockJob, worker1, source1, ImmutableSet.of(1, 4));

        // Worker2 first instance: has buckets 2 and 5
        BucketScanSource source2 = makeBucketScanSource(mockScanNode, 2, 5);
        LocalShuffleBucketJoinAssignedJob inst2 = new LocalShuffleBucketJoinAssignedJob(
                2, 0, new TUniqueId(0, 2), mockJob, worker2, source2, ImmutableSet.of(2, 5));

        // This simulates getFirstInstancePerWorker output
        List<AssignedJob> firstPerWorker = new ArrayList<>();
        firstPerWorker.add(inst0);
        firstPerWorker.add(inst1);
        firstPerWorker.add(inst2);

        // Invoke private sortDestinationInstancesByBuckets via reflection
        PipelineDistributedPlan mockPlan = Mockito.mock(PipelineDistributedPlan.class);
        Mockito.when(mockPlan.getFragmentJob()).thenReturn(mockJob);

        Method method = DistributePlanner.class.getDeclaredMethod(
                "sortDestinationInstancesByBuckets",
                PipelineDistributedPlan.class, List.class, int.class, boolean.class);
        method.setAccessible(true);

        // Need a DistributePlanner instance — create via Objenesis (Mockito internal) to bypass constructor
        Object planner = org.objenesis.ObjenesisStd.class.getDeclaredConstructor()
                .newInstance().newInstance(DistributePlanner.class);

        // useJoinBucketAssignment=true: non-serial mode uses getAssignedJoinBucketIndexes()
        @SuppressWarnings("unchecked")
        List<AssignedJob> destinations = (List<AssignedJob>) method.invoke(
                planner, mockPlan, firstPerWorker, 6, true);

        // Verify: destinations count == bucketNum
        Assertions.assertEquals(6, destinations.size(), "destinations count should equal bucketNum");

        // Verify: each bucket maps to the correct worker's first instance
        Assertions.assertSame(inst0, destinations.get(0), "bucket 0 → worker0 first instance");
        Assertions.assertSame(inst1, destinations.get(1), "bucket 1 → worker1 first instance");
        Assertions.assertSame(inst2, destinations.get(2), "bucket 2 → worker2 first instance");
        Assertions.assertSame(inst0, destinations.get(3), "bucket 3 → worker0 first instance (same as bucket 0)");
        Assertions.assertSame(inst1, destinations.get(4), "bucket 4 → worker1 first instance (same as bucket 1)");
        Assertions.assertSame(inst2, destinations.get(5), "bucket 5 → worker2 first instance (same as bucket 2)");

        // Verify: same worker's buckets point to the SAME instance object
        Assertions.assertSame(destinations.get(0), destinations.get(3),
                "bucket 0 and 3 should be same instance (same worker)");
        Assertions.assertSame(destinations.get(1), destinations.get(4),
                "bucket 1 and 4 should be same instance (same worker)");
        Assertions.assertSame(destinations.get(2), destinations.get(5),
                "bucket 2 and 5 should be same instance (same worker)");
    }

    private static BucketScanSource makeBucketScanSource(ScanNode scanNode, int... bucketIndices) {
        Map<Integer, Map<ScanNode, ScanRanges>> bucketMap = Maps.newLinkedHashMap();
        for (int idx : bucketIndices) {
            Map<ScanNode, ScanRanges> scanMap = ImmutableMap.of(scanNode, new ScanRanges());
            bucketMap.put(idx, scanMap);
        }
        return new BucketScanSource(bucketMap);
    }
}
