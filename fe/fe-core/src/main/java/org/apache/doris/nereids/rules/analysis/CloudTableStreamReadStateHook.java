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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.CloudTableStreamReadStateHelper;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolves one read snapshot for every Cloud Table Stream relation after statement analysis. */
public class CloudTableStreamReadStateHook implements PlannerHook {
    public static final CloudTableStreamReadStateHook INSTANCE = new CloudTableStreamReadStateHook();

    private CloudTableStreamReadStateHook() {
    }

    @Override
    public void afterAnalyze(NereidsPlanner planner) {
        resolve(planner.getCascadesContext().getRewritePlan());
    }

    static void resolve(Plan plan) {
        List<LogicalOlapTableStreamScan> scans = new ArrayList<>();
        plan.collectToList(LogicalOlapTableStreamScan.class::isInstance).forEach(node ->
                scans.add((LogicalOlapTableStreamScan) node));
        Preconditions.checkState(!scans.isEmpty(),
                "Cloud Table Stream read-state hook requires at least one Stream scan");

        boolean readStatesInstalled = scans.get(0).getTable().hasCloudReadStates();
        for (LogicalOlapTableStreamScan scan : scans) {
            OlapTableStreamWrapper wrapper = scan.getTable();
            Preconditions.checkState(wrapper.hasCloudReadStates() == readStatesInstalled,
                    "Cloud Table Stream read state must be installed once for all scans in one statement");
            if (readStatesInstalled) {
                Preconditions.checkState(wrapper.getCloudReadStates().keySet()
                                .containsAll(scan.getSelectedPartitionIds()),
                        "Installed Cloud Table Stream read state does not cover every selected partition");
            }
        }
        if (readStatesInstalled) {
            return;
        }

        Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions = new LinkedHashMap<>();
        Map<OlapTableStreamWrapper, Set<Long>> wrapperPartitions = new IdentityHashMap<>();
        for (LogicalOlapTableStreamScan scan : scans) {
            OlapTableStreamWrapper wrapper = scan.getTable();
            Set<Long> selectedWrapperPartitions =
                    wrapperPartitions.computeIfAbsent(wrapper, ignored -> new LinkedHashSet<>());
            if (scan.getSelectedPartitionIds().isEmpty()) {
                continue;
            }
            requestedPartitions.computeIfAbsent(wrapper.getCloudIdentity(), ignored -> new LinkedHashSet<>())
                    .addAll(scan.getSelectedPartitionIds());
            selectedWrapperPartitions.addAll(scan.getSelectedPartitionIds());
        }

        if (requestedPartitions.isEmpty()) {
            scans.forEach(scan -> scan.getTable().installCloudReadStates(ImmutableMap.of()));
            return;
        }

        Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> readStates;
        try {
            readStates = CloudTableStreamReadStateHelper.getReadStates(requestedPartitions);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e);
        }

        for (Map.Entry<OlapTableStreamWrapper, Set<Long>> wrapperEntry : wrapperPartitions.entrySet()) {
            OlapTableStreamWrapper wrapper = wrapperEntry.getKey();
            Map<Long, Cloud.TableStreamPartitionReadStatePB> bindingStates =
                    readStates.get(wrapper.getCloudIdentity());
            Preconditions.checkNotNull(bindingStates,
                    "Cloud Table Stream read state is missing for a requested binding");
            ImmutableMap.Builder<Long, Cloud.TableStreamPartitionReadStatePB> wrapperStates =
                    ImmutableMap.builderWithExpectedSize(wrapperEntry.getValue().size());
            for (long partitionId : wrapperEntry.getValue()) {
                wrapperStates.put(partitionId, bindingStates.get(partitionId));
            }
            wrapper.installCloudReadStates(wrapperStates.build());
        }
    }
}
