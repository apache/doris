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

package org.apache.doris.qe.runtime;

import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TRuntimeFilterTargetParams;
import org.apache.doris.thrift.TRuntimeFilterTargetParamsV2;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** RuntimeFiltersThriftBuilder */
public class RuntimeFiltersThriftBuilder {
    public final TNetworkAddress mergeAddress;

    private final List<RuntimeFilter> runtimeFilters;
    private final AssignedJob mergeInstance;
    private final Set<Integer> broadcastRuntimeFilterIds;
    private final Map<RuntimeFilterId, List<RuntimeFilterTarget>> ridToTargets;
    private final Map<RuntimeFilterId, Integer> ridToBuilderNum;

    private RuntimeFiltersThriftBuilder(
            TNetworkAddress mergeAddress, List<RuntimeFilter> runtimeFilters,
            AssignedJob mergeInstance, Set<Integer> broadcastRuntimeFilterIds,
            Map<RuntimeFilterId, List<RuntimeFilterTarget>> ridToTargets,
            Map<RuntimeFilterId, Integer> ridToBuilderNum) {
        this.mergeAddress = mergeAddress;
        this.runtimeFilters = runtimeFilters;
        this.mergeInstance = mergeInstance;
        this.broadcastRuntimeFilterIds = broadcastRuntimeFilterIds;
        this.ridToTargets = ridToTargets;
        this.ridToBuilderNum = ridToBuilderNum;
    }

    public boolean isMergeRuntimeFilterInstance(AssignedJob instance) {
        return mergeInstance == instance;
    }

    public void setRuntimeFilterThriftParams(TRuntimeFilterParams runtimeFilterParams) {
        for (RuntimeFilter rf : runtimeFilters) {
            List<RuntimeFilterTarget> targets = ridToTargets.get(rf.getFilterId());
            if (targets == null) {
                continue;
            }

            if (rf.hasRemoteTargets()) {
                Map<TNetworkAddress, TRuntimeFilterTargetParamsV2> targetToParams = new LinkedHashMap<>();
                for (RuntimeFilterTarget target : targets) {
                    TRuntimeFilterTargetParamsV2 targetParams = targetToParams.computeIfAbsent(
                            target.address, address -> {
                                TRuntimeFilterTargetParamsV2 params = new TRuntimeFilterTargetParamsV2();
                                params.target_fragment_instance_addr = address;
                                params.target_fragment_ids = new ArrayList<>();
                                // required field
                                params.target_fragment_instance_ids = new ArrayList<>();
                                return params;
                            });

                    targetParams.target_fragment_ids.add(target.fragmentId);
                }

                runtimeFilterParams.putToRidToTargetParamv2(
                        rf.getFilterId().asInt(), new ArrayList<>(targetToParams.values())
                );
            } else {
                List<TRuntimeFilterTargetParams> targetParams = Lists.newArrayList();
                for (RuntimeFilterTarget target : targets) {
                    // Instance id make no sense if this runtime filter doesn't have remote targets.
                    targetParams.add(new TRuntimeFilterTargetParams(new TUniqueId(), target.address));
                }
                runtimeFilterParams.putToRidToTargetParam(rf.getFilterId().asInt(), targetParams);
            }
        }
        for (Map.Entry<RuntimeFilterId, Integer> entry : ridToBuilderNum.entrySet()) {
            boolean isBroadcastRuntimeFilter = broadcastRuntimeFilterIds.contains(entry.getKey().asInt());
            int builderNum = isBroadcastRuntimeFilter ? 1 : entry.getValue();
            runtimeFilterParams.putToRuntimeFilterBuilderNum(entry.getKey().asInt(), builderNum);
        }
        for (RuntimeFilter rf : runtimeFilters) {
            runtimeFilterParams.putToRidToRuntimeFilter(rf.getFilterId().asInt(), rf.toThrift());
        }
    }

    public static RuntimeFiltersThriftBuilder compute(
            List<RuntimeFilter> runtimeFilters, List<PipelineDistributedPlan> distributedPlans) {
        PipelineDistributedPlan topMostPlan = distributedPlans.get(distributedPlans.size() - 1);
        AssignedJob mergeInstance = topMostPlan.getInstanceJobs().get(0);
        BackendWorker worker = (BackendWorker) mergeInstance.getAssignedWorker();
        TNetworkAddress mergeAddress = new TNetworkAddress(worker.host(), worker.brpcPort());

        Set<Integer> broadcastRuntimeFilterIds = runtimeFilters
                .stream()
                .filter(RuntimeFilter::isBroadcast)
                .map(r -> r.getFilterId().asInt())
                .collect(Collectors.toSet());

        Map<RuntimeFilterId, List<RuntimeFilterTarget>> ridToTargetParam = Maps.newLinkedHashMap();
        Map<RuntimeFilterId, Integer> ridToBuilderNum = Maps.newLinkedHashMap();
        for (PipelineDistributedPlan plan : distributedPlans) {
            PlanFragment fragment = plan.getFragmentJob().getFragment();
            // Transform <fragment, runtimeFilterId> to <runtimeFilterId, fragment>
            for (RuntimeFilterId rid : fragment.getTargetRuntimeFilterIds()) {
                List<RuntimeFilterTarget> targetFragments =
                        ridToTargetParam.computeIfAbsent(rid, k -> new ArrayList<>());
                for (AssignedJob instanceJob : plan.getInstanceJobs()) {
                    BackendWorker backendWorker = (BackendWorker) instanceJob.getAssignedWorker();
                    Backend backend = backendWorker.getBackend();
                    targetFragments.add(new RuntimeFilterTarget(
                            fragment.getFragmentId().asInt(),
                            new TNetworkAddress(backend.getHost(), backend.getBrpcPort())
                    ));
                }
            }

            for (RuntimeFilterId rid : fragment.getBuilderRuntimeFilterIds()) {
                int distinctWorkerNum = (int) plan.getInstanceJobs()
                        .stream()
                        .map(AssignedJob::getAssignedWorker)
                        .map(DistributedPlanWorker::id)
                        .distinct()
                        .count();
                ridToBuilderNum.merge(rid, distinctWorkerNum, Integer::sum);
            }
        }
        return new RuntimeFiltersThriftBuilder(
                mergeAddress, runtimeFilters, mergeInstance,
                broadcastRuntimeFilterIds, ridToTargetParam, ridToBuilderNum
        );
    }

    public static class RuntimeFilterTarget {
        public final int fragmentId;
        public final TNetworkAddress address;

        public RuntimeFilterTarget(int fragmentId, TNetworkAddress address) {
            this.fragmentId = fragmentId;
            this.address = address;
        }
    }
}
