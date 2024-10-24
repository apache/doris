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

import org.apache.doris.nereids.trees.plans.distribute.worker.BackendDistributedPlanWorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TExplainLevel;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** AssignedJobBuilder */
public class AssignedJobBuilder {
    /** buildJobs */
    public static ListMultimap<PlanFragmentId, AssignedJob> buildJobs(
            Map<PlanFragmentId, UnassignedJob> unassignedJobs) {
        BackendDistributedPlanWorkerManager workerManager = new BackendDistributedPlanWorkerManager();
        ListMultimap<PlanFragmentId, AssignedJob> allAssignedJobs = ArrayListMultimap.create();
        for (Entry<PlanFragmentId, UnassignedJob> kv : unassignedJobs.entrySet()) {
            PlanFragmentId fragmentId = kv.getKey();
            UnassignedJob unassignedJob = kv.getValue();
            ListMultimap<ExchangeNode, AssignedJob> inputAssignedJobs
                    = getInputAssignedJobs(unassignedJob, allAssignedJobs);
            List<AssignedJob> fragmentAssignedJobs =
                    unassignedJob.computeAssignedJobs(workerManager, inputAssignedJobs);
            if (fragmentAssignedJobs.isEmpty()) {
                throw new IllegalStateException("Fragment has no instance, unassignedJob: " + unassignedJob
                        + ", fragment: " + unassignedJob.getFragment().getExplainString(TExplainLevel.VERBOSE));
            }
            allAssignedJobs.putAll(fragmentId, fragmentAssignedJobs);
        }
        return allAssignedJobs;
    }

    private static ListMultimap<ExchangeNode, AssignedJob> getInputAssignedJobs(
            UnassignedJob unassignedJob, ListMultimap<PlanFragmentId, AssignedJob> assignedJobs) {
        ListMultimap<ExchangeNode, AssignedJob> inputJobs = ArrayListMultimap.create();
        for (Entry<ExchangeNode, Collection<UnassignedJob>> exchangeNodeToChildJobs
                : unassignedJob.getExchangeToChildJob().asMap().entrySet()) {
            ExchangeNode exchangeNode = exchangeNodeToChildJobs.getKey();
            Collection<UnassignedJob> childJobs = exchangeNodeToChildJobs.getValue();
            for (UnassignedJob childJob : childJobs) {
                inputJobs.putAll(exchangeNode, assignedJobs.get(childJob.getFragment().getFragmentId()));
            }
        }
        return inputJobs;
    }
}
