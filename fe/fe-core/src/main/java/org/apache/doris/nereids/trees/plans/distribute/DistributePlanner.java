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

import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJobBuilder;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJobBuilder;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;

import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/** DistributePlanner */
public class DistributePlanner {
    private final List<PlanFragment> fragments;
    private final FragmentIdMapping<PlanFragment> idToFragments;

    public DistributePlanner(List<PlanFragment> fragments) {
        this.fragments = Objects.requireNonNull(fragments, "fragments can not be null");
        this.idToFragments = FragmentIdMapping.buildFragmentMapping(fragments);
    }

    public FragmentIdMapping<DistributedPlan> plan() {
        FragmentIdMapping<UnassignedJob> fragmentJobs = UnassignedJobBuilder.buildJobs(idToFragments);
        ListMultimap<PlanFragmentId, AssignedJob> instanceJobs = AssignedJobBuilder.buildJobs(fragmentJobs);
        return buildDistributePlans(fragmentJobs, instanceJobs);
    }

    private FragmentIdMapping<DistributedPlan> buildDistributePlans(
            Map<PlanFragmentId, UnassignedJob> idToUnassignedJobs,
            ListMultimap<PlanFragmentId, AssignedJob> idToAssignedJobs) {
        FragmentIdMapping<PipelineDistributedPlan> idToDistributedPlans = new FragmentIdMapping<>();
        for (Entry<PlanFragmentId, PlanFragment> kv : idToFragments.entrySet()) {
            PlanFragmentId fragmentId = kv.getKey();
            PlanFragment fragment = kv.getValue();

            UnassignedJob fragmentJob = idToUnassignedJobs.get(fragmentId);
            List<AssignedJob> instanceJobs = idToAssignedJobs.get(fragmentId);

            List<PipelineDistributedPlan> childrenPlans = idToDistributedPlans.getByChildrenFragments(fragment);
            idToDistributedPlans.put(fragmentId, new PipelineDistributedPlan(fragmentJob, instanceJobs, childrenPlans));
        }
        return (FragmentIdMapping) idToDistributedPlans;
    }
}
