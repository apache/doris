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

import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.nereids.util.Utils;

import java.util.List;
import java.util.Objects;

/** DistributedPlan */
@lombok.Getter
public abstract class DistributedPlan extends AbstractTreeNode<DistributedPlan> {
    protected final UnassignedJob fragmentJob;
    protected final List<DistributedPlan> inputs;

    public DistributedPlan(UnassignedJob fragmentJob, List<? extends DistributedPlan> inputs) {
        this.fragmentJob = Objects.requireNonNull(fragmentJob, "fragmentJob can not be null");
        this.inputs = Utils.fastToImmutableList(Objects.requireNonNull(inputs, "inputs can not be null"));
    }

    @Override
    public DistributedPlan withChildren(List<DistributedPlan> children) {
        throw new UnsupportedOperationException();
    }

    public abstract String toString(int displayFragmentId);

    /** toString */
    public static String toString(List<DistributedPlan> distributedPlansBottomToTop) {
        StringBuilder distributedPlanStringBuilder = new StringBuilder();
        int fragmentDisplayId = 0;
        for (int i = distributedPlansBottomToTop.size() - 1; i >= 0; i--) {
            DistributedPlan distributedPlan = distributedPlansBottomToTop.get(i);
            distributedPlanStringBuilder
                    .append(distributedPlan.toString(fragmentDisplayId++))
                    .append("\n");
        }
        return distributedPlanStringBuilder.toString();
    }
}
