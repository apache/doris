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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/** LocalShuffleBucketJoinAssignedJob */
public class LocalShuffleBucketJoinAssignedJob extends LocalShuffleAssignedJob {
    private volatile Set<Integer> assignedJoinBucketIndexes;

    public LocalShuffleBucketJoinAssignedJob(
            int indexInUnassignedJob, int shareScanId, boolean receiveDataFromLocal,
            TUniqueId instanceId, UnassignedJob unassignedJob,
            DistributedPlanWorker worker, ScanSource scanSource,
            Set<Integer> assignedJoinBucketIndexes) {
        super(indexInUnassignedJob, shareScanId, receiveDataFromLocal, instanceId, unassignedJob, worker, scanSource);
        this.assignedJoinBucketIndexes = Utils.fastToImmutableSet(assignedJoinBucketIndexes);
    }

    public Set<Integer> getAssignedJoinBucketIndexes() {
        return assignedJoinBucketIndexes;
    }

    public void addAssignedJoinBucketIndexes(Set<Integer> joinBucketIndexes) {
        this.assignedJoinBucketIndexes = ImmutableSet.<Integer>builder()
                .addAll(assignedJoinBucketIndexes)
                .addAll(joinBucketIndexes)
                .build();
    }

    @Override
    protected String formatOtherString() {
        return ",\n  assigned join buckets: " + assignedJoinBucketIndexes;
    }
}
