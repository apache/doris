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

import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DummyWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJobBuilder;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.StaticAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJobBuilder;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedScanBucketOlapTableJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.MultiCastPlanFragment;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/** DistributePlanner */
public class DistributePlanner {
    private static final Logger LOG = LogManager.getLogger(DistributePlanner.class);
    private final StatementContext statementContext;
    private final FragmentIdMapping<PlanFragment> idToFragments;

    public DistributePlanner(StatementContext statementContext, List<PlanFragment> fragments) {
        this.statementContext = Objects.requireNonNull(statementContext, "statementContext can not be null");
        this.idToFragments = FragmentIdMapping.buildFragmentMapping(fragments);
    }

    /** plan */
    public FragmentIdMapping<DistributedPlan> plan() {
        try {
            FragmentIdMapping<UnassignedJob> fragmentJobs
                    = UnassignedJobBuilder.buildJobs(statementContext, idToFragments);
            ListMultimap<PlanFragmentId, AssignedJob> instanceJobs = AssignedJobBuilder.buildJobs(fragmentJobs);
            FragmentIdMapping<DistributedPlan> distributedPlans = buildDistributePlans(fragmentJobs, instanceJobs);
            FragmentIdMapping<DistributedPlan> linkedPlans = linkPlans(distributedPlans);
            updateProfileIfPresent(SummaryProfile::setAssignFragmentTime);
            return linkedPlans;
        } catch (Throwable t) {
            LOG.error("Failed to build distribute plans", t);
            throw t;
        }
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

            SetMultimap<ExchangeNode, DistributedPlan> exchangeNodeToChildren = LinkedHashMultimap.create();
            for (PlanFragment childFragment : fragment.getChildren()) {
                if (childFragment instanceof MultiCastPlanFragment) {
                    for (ExchangeNode exchangeNode : ((MultiCastPlanFragment) childFragment).getDestNodeList()) {
                        if (exchangeNode.getFragment() == fragment) {
                            exchangeNodeToChildren.put(
                                    exchangeNode, idToDistributedPlans.get(childFragment.getFragmentId())
                            );
                        }
                    }
                } else {
                    exchangeNodeToChildren.put(
                            childFragment.getDestNode(),
                            idToDistributedPlans.get(childFragment.getFragmentId())
                    );
                }
            }

            idToDistributedPlans.put(fragmentId,
                    new PipelineDistributedPlan(fragmentJob, instanceJobs, exchangeNodeToChildren)
            );
        }
        return (FragmentIdMapping) idToDistributedPlans;
    }

    private FragmentIdMapping<DistributedPlan> linkPlans(FragmentIdMapping<DistributedPlan> plans) {
        boolean enableShareHashTableForBroadcastJoin = statementContext.getConnectContext()
                .getSessionVariable()
                .enableShareHashTableForBroadcastJoin;
        for (DistributedPlan receiverPlan : plans.values()) {
            for (Entry<ExchangeNode, DistributedPlan> link : receiverPlan.getInputs().entries()) {
                linkPipelinePlan(
                        (PipelineDistributedPlan) receiverPlan,
                        (PipelineDistributedPlan) link.getValue(),
                        link.getKey(),
                        enableShareHashTableForBroadcastJoin
                );
            }
        }
        return plans;
    }

    // set shuffle destinations
    private void linkPipelinePlan(
            PipelineDistributedPlan receiverPlan,
            PipelineDistributedPlan senderPlan,
            ExchangeNode linkNode,
            boolean enableShareHashTableForBroadcastJoin) {

        List<AssignedJob> receiverInstances = filterInstancesWhichCanReceiveDataFromRemote(
                receiverPlan, enableShareHashTableForBroadcastJoin, linkNode);
        if (linkNode.getPartitionType() == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED) {
            receiverInstances = getDestinationsByBuckets(receiverPlan, receiverInstances);
        }

        DataSink sink = senderPlan.getFragmentJob().getFragment().getSink();
        if (sink instanceof MultiCastDataSink) {
            MultiCastDataSink multiCastDataSink = (MultiCastDataSink) sink;
            receiverPlan.getFragmentJob().getFragment().setOutputPartition(multiCastDataSink.getOutputPartition());
            for (DataStreamSink realSink : multiCastDataSink.getDataStreamSinks()) {
                if (realSink.getExchNodeId() == linkNode.getId()) {
                    senderPlan.addDestinations(realSink, receiverInstances);
                    break;
                }
            }
        } else {
            senderPlan.addDestinations(sink, receiverInstances);
        }
    }

    private List<AssignedJob> getDestinationsByBuckets(
            PipelineDistributedPlan joinSide,
            List<AssignedJob> receiverInstances) {
        UnassignedScanBucketOlapTableJob bucketJob = (UnassignedScanBucketOlapTableJob) joinSide.getFragmentJob();
        int bucketNum = bucketJob.getOlapScanNodes().get(0).getBucketNum();
        return sortDestinationInstancesByBuckets(joinSide, receiverInstances, bucketNum);
    }

    private List<AssignedJob> filterInstancesWhichCanReceiveDataFromRemote(
            PipelineDistributedPlan receiverPlan,
            boolean enableShareHashTableForBroadcastJoin,
            ExchangeNode linkNode) {
        boolean useLocalShuffle = receiverPlan.getInstanceJobs().stream()
                .anyMatch(LocalShuffleAssignedJob.class::isInstance);
        if (useLocalShuffle) {
            return getFirstInstancePerShareScan(receiverPlan);
        } else if (enableShareHashTableForBroadcastJoin && linkNode.isRightChildOfBroadcastHashJoin()) {
            return getFirstInstancePerWorker(receiverPlan.getInstanceJobs());
        } else {
            return receiverPlan.getInstanceJobs();
        }
    }

    private List<AssignedJob> sortDestinationInstancesByBuckets(
            PipelineDistributedPlan plan, List<AssignedJob> unsorted, int bucketNum) {
        AssignedJob[] instances = new AssignedJob[bucketNum];
        for (AssignedJob instanceJob : unsorted) {
            BucketScanSource bucketScanSource = (BucketScanSource) instanceJob.getScanSource();
            for (Integer bucketIndex : bucketScanSource.bucketIndexToScanNodeToTablets.keySet()) {
                if (instances[bucketIndex] != null) {
                    throw new IllegalStateException(
                            "Multi instances scan same buckets: " + instances[bucketIndex] + " and " + instanceJob
                    );
                }
                instances[bucketIndex] = instanceJob;
            }
        }

        for (int i = 0; i < instances.length; i++) {
            if (instances[i] == null) {
                instances[i] = new StaticAssignedJob(
                        i,
                        new TUniqueId(-1, -1),
                        plan.getFragmentJob(),
                        DummyWorker.INSTANCE,
                        new DefaultScanSource(ImmutableMap.of())
                );
            }
        }
        return Arrays.asList(instances);
    }

    private List<AssignedJob> getFirstInstancePerShareScan(PipelineDistributedPlan plan) {
        List<AssignedJob> canReceiveDataFromRemote = Lists.newArrayListWithCapacity(plan.getInstanceJobs().size());
        for (AssignedJob instanceJob : plan.getInstanceJobs()) {
            LocalShuffleAssignedJob localShuffleJob = (LocalShuffleAssignedJob) instanceJob;
            if (!localShuffleJob.receiveDataFromLocal) {
                canReceiveDataFromRemote.add(localShuffleJob);
            }
        }
        return canReceiveDataFromRemote;
    }

    private List<AssignedJob> getFirstInstancePerWorker(List<AssignedJob> instances) {
        Map<DistributedPlanWorker, AssignedJob> firstInstancePerWorker = Maps.newLinkedHashMap();
        for (AssignedJob instance : instances) {
            firstInstancePerWorker.putIfAbsent(instance.getAssignedWorker(), instance);
        }
        return Utils.fastToImmutableList(firstInstancePerWorker.values());
    }

    private void updateProfileIfPresent(Consumer<SummaryProfile> profileAction) {
        Optional.ofNullable(ConnectContext.get())
                .map(ConnectContext::getExecutor)
                .map(StmtExecutor::getSummaryProfile)
                .ifPresent(profileAction);
    }
}
