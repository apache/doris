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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.CloudTableStreamReadStateHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolve one statement-level read snapshot for every Cloud Table Stream scan. */
public class ResolveCloudTableStreamReadState implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        if (Config.isNotCloudMode()) {
            return plan;
        }

        List<LogicalOlapTableStreamScan> scans = new ArrayList<>();
        plan.collectToList(LogicalOlapTableStreamScan.class::isInstance).forEach(node -> {
            LogicalOlapTableStreamScan scan = (LogicalOlapTableStreamScan) node;
            scans.add(scan);
        });
        if (scans.isEmpty()) {
            return plan;
        }

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
            return plan;
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
            return plan;
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
            if (bindingStates == null) {
                wrapper.installCloudReadStates(ImmutableMap.of());
                continue;
            }
            ImmutableMap.Builder<Long, Cloud.TableStreamPartitionReadStatePB> wrapperStates =
                    ImmutableMap.builderWithExpectedSize(wrapperEntry.getValue().size());
            for (long partitionId : wrapperEntry.getValue()) {
                wrapperStates.put(partitionId, bindingStates.get(partitionId));
            }
            wrapper.installCloudReadStates(wrapperStates.build());
        }
        return plan;
    }
}
