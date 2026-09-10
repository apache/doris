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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fragment must report the bucket shuffle of every join it hosts, fused or not.
 *
 * <p>GroupJoinNode does not extend HashJoinNode, so the fragment used to report no bucket shuffle
 * for a fused join. UnassignedJobBuilder then assigned the scan of the bucket side an
 * UnassignedScanSingleOlapTableJob while the exchange into the join still carried
 * TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED, and running the query failed in
 * DistributePlanner.getDestinationsByBuckets with
 * "UnassignedScanSingleOlapTableJob cannot be cast to UnassignedScanBucketOlapTableJob".
 */
public class PlanFragmentBucketShuffleTest {
    private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

    @Test
    public void testFusedGroupJoinBucketShuffleMarksTheFragment() {
        Assertions.assertTrue(fusedGroupJoinFragment(DistributionMode.BUCKET_SHUFFLE).hasBucketShuffleNode(),
                "a BUCKET_SHUFFLE fused group join must mark its fragment as bucket shuffling");
    }

    @Test
    public void testOnlyBucketShuffleMarksTheFragment() {
        Assertions.assertFalse(fusedGroupJoinFragment(DistributionMode.PARTITIONED).hasBucketShuffleNode(),
                "a partitioned fused group join must not mark its fragment as bucket shuffling");
        Assertions.assertFalse(fusedGroupJoinFragment(DistributionMode.BROADCAST).hasBucketShuffleNode(),
                "a broadcast fused group join must not mark its fragment as bucket shuffling");
    }

    private static PlanFragment fusedGroupJoinFragment(DistributionMode mode) {
        ArrayList<TupleId> tupleIds = new ArrayList<>(
                Collections.singletonList(new TupleId(NEXT_ID.getAndIncrement())));
        GroupJoinNode groupJoin = new GroupJoinNode(new PlanNodeId(NEXT_ID.getAndIncrement()),
                new EmptySetNode(new PlanNodeId(NEXT_ID.getAndIncrement()), tupleIds),
                new EmptySetNode(new PlanNodeId(NEXT_ID.getAndIncrement()), tupleIds));
        groupJoin.setDistributionMode(mode);
        return new PlanFragment(new PlanFragmentId(NEXT_ID.getAndIncrement()), groupJoin, DataPartition.RANDOM);
    }
}
