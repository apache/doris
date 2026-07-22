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
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;

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

        Cloud.GetTableStreamReadStateRequest.Builder request =
                Cloud.GetTableStreamReadStateRequest.newBuilder()
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached());
        requestedPartitions.forEach((identity, partitionIds) -> request.addBindings(
                Cloud.TableStreamPartitionSetPB.newBuilder()
                        .setIdentity(identity)
                        .addAllPartitionIds(partitionIds)));

        Cloud.GetTableStreamReadStateResponse response;
        MetaServiceProxy proxy = MetaServiceProxy.getInstance();
        try {
            response = proxy.getTableStreamReadState(request.build());
        } catch (RpcException e) {
            throw new AnalysisException("Failed to get Cloud Table Stream read state: " + e.getMessage(), e);
        }
        if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new AnalysisException("Failed to get Cloud Table Stream read state: "
                    + response.getStatus().getMsg());
        }

        Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> readStates =
                validateResponse(requestedPartitions, response);
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

    private Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> validateResponse(
            Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions,
            Cloud.GetTableStreamReadStateResponse response) {
        Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> readStates =
                new LinkedHashMap<>();
        for (Cloud.TableStreamReadBindingResultPB binding : response.getBindingsList()) {
            if (!binding.hasIdentity() || !requestedPartitions.containsKey(binding.getIdentity())) {
                throw new AnalysisException("MetaService returned an unexpected Cloud Table Stream binding");
            }
            Map<Long, Cloud.TableStreamPartitionReadStatePB> partitionStates = new LinkedHashMap<>();
            for (Cloud.TableStreamPartitionReadStatePB state : binding.getPartitionStatesList()) {
                if (!state.hasPartitionId() || partitionStates.put(state.getPartitionId(), state) != null) {
                    throw new AnalysisException("MetaService returned duplicate or invalid partition state");
                }
            }
            if (!partitionStates.keySet().equals(requestedPartitions.get(binding.getIdentity()))) {
                throw new AnalysisException("MetaService returned an incomplete Cloud Table Stream binding");
            }
            if (readStates.put(binding.getIdentity(), partitionStates) != null) {
                throw new AnalysisException("MetaService returned a duplicate Cloud Table Stream binding");
            }
        }
        if (!readStates.keySet().equals(requestedPartitions.keySet())) {
            throw new AnalysisException("MetaService did not return all Cloud Table Stream bindings");
        }
        return readStates;
    }
}
