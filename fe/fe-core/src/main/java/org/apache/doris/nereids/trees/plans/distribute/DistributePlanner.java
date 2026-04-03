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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendDistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DummyWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.LoadBalanceScanWorkerSelector;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJobBuilder;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleBucketJoinAssignedJob;
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
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.ListMultimap;
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
import java.util.Set;
import java.util.function.Consumer;

/** DistributePlanner */
public class DistributePlanner {
    private static final Logger LOG = LogManager.getLogger(DistributePlanner.class);
    private final StatementContext statementContext;
    private final FragmentIdMapping<PlanFragment> idToFragments;
    private final boolean notNeedBackend;
    private final boolean isLoadJob;

    public DistributePlanner(StatementContext statementContext,
            List<PlanFragment> fragments, boolean notNeedBackend, boolean isLoadJob) {
        this.statementContext = Objects.requireNonNull(statementContext, "statementContext can not be null");
        this.idToFragments = FragmentIdMapping.buildFragmentMapping(fragments);
        this.notNeedBackend = notNeedBackend;
        this.isLoadJob = isLoadJob;
    }

    /** plan */
    public FragmentIdMapping<DistributedPlan> plan() {
        updateProfileIfPresent(profile -> profile.setQueryPlanFinishTime(TimeUtils.getStartTimeMs()));
        try {
            BackendDistributedPlanWorkerManager workerManager = new BackendDistributedPlanWorkerManager(
                            statementContext.getConnectContext(), notNeedBackend, isLoadJob);
            addExternalBackends(workerManager);
            LoadBalanceScanWorkerSelector workerSelector = new LoadBalanceScanWorkerSelector(workerManager);
            FragmentIdMapping<UnassignedJob> fragmentJobs
                    = UnassignedJobBuilder.buildJobs(workerSelector, statementContext, idToFragments);
            // assign BE and dop, to instance
            ListMultimap<PlanFragmentId, AssignedJob> instanceJobs
                    = AssignedJobBuilder.buildJobs(fragmentJobs, workerManager, isLoadJob);
            FragmentIdMapping<DistributedPlan> distributedPlans = buildDistributePlans(fragmentJobs, instanceJobs);
            // for broadcast or something impacts links' shape, they're in Node's property (like exchange's
            // partitionType). we use them to link plans. no needs of extra modification.
            FragmentIdMapping<DistributedPlan> linkedPlans = linkPlans(distributedPlans);

            if (LOG.isDebugEnabled()) {
                LOG.debug("=== LinkedPlans Debug Info ===");
                linkedPlans.forEach((fragmentId, plan) -> {
                    LOG.debug("Fragment[{}]:", fragmentId);
                    if (plan instanceof PipelineDistributedPlan) {
                        PipelineDistributedPlan pPlan = (PipelineDistributedPlan) plan;
                        LOG.debug("  Jobs: {}", pPlan.getInstanceJobs());
                        LOG.debug("  Destinations: {}", pPlan.getDestinations());
                        LOG.debug("  Inputs: {}", pPlan.getInputs());
                    }
                });
                LOG.debug("===========================");
            }

            updateProfileIfPresent(SummaryProfile::setAssignFragmentTime);
            return linkedPlans;
        } catch (Throwable t) {
            if (t instanceof NereidsException && ((NereidsException) t).isSuppressStackTrace()) {
                LOG.error("Failed to build distribute plans: {}", t);
            } else {
                LOG.error("Failed to build distribute plans", t);
            }
            Throwables.throwIfInstanceOf(t, RuntimeException.class);
            throw new IllegalStateException(t.toString(), t);
        }
    }

    private void addExternalBackends(BackendDistributedPlanWorkerManager workerManager) throws AnalysisException {
        for (PlanFragment planFragment : idToFragments.values()) {
            List<OlapScanNode> scanNodes = planFragment.getPlanRoot()
                    .collectInCurrentFragment(OlapScanNode.class::isInstance);
            for (OlapScanNode scanNode : scanNodes) {
                workerManager.addBackends(scanNode.getCatalogId(),
                        scanNode.getOlapTable().getAllBackendsByAllCluster());
            }
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
                for (Entry<DataSink, List<AssignedJob>> kv :
                        ((PipelineDistributedPlan) link.getValue()).getDestinations().entrySet()) {
                    if (kv.getValue().isEmpty()) {
                        int sourceFragmentId = link.getValue().getFragmentJob().getFragment().getFragmentId().asInt();
                        String msg = "Invalid plan which exchange not contains receiver, "
                                + "exchange id: " + kv.getKey().getExchNodeId().asInt()
                                + ", source fragmentId: " + sourceFragmentId;
                        throw new IllegalStateException(msg);
                    }
                }
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

        boolean isSerialExchange = linkNode.isSerialOperator()
                && linkNode.getFragment().useSerialSource(statementContext.getConnectContext());
        List<AssignedJob> receiverInstances = filterInstancesWhichCanReceiveDataFromRemote(
                receiverPlan, linkNode, enableShareHashTableForBroadcastJoin);
        if (linkNode.getPartitionType() == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED) {
            receiverInstances = getDestinationsByBuckets(receiverPlan, receiverInstances,
                    !isSerialExchange);
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
            List<AssignedJob> receiverInstances,
            boolean useJoinBucketAssignment) {
        UnassignedScanBucketOlapTableJob bucketJob = (UnassignedScanBucketOlapTableJob) joinSide.getFragmentJob();
        int bucketNum = bucketJob.getOlapScanNodes().get(0).getBucketNum();
        return sortDestinationInstancesByBuckets(joinSide, receiverInstances, bucketNum,
                useJoinBucketAssignment);
    }

    private List<AssignedJob> filterInstancesWhichCanReceiveDataFromRemote(
            PipelineDistributedPlan receiverPlan,
            ExchangeNode linkNode,
            boolean enableShareHashTableForBroadcastJoin) {
        // isSerialOperator(): UNPARTITIONED or use_serial_exchange (operator-level)
        // useSerialSource(): fragment is in pooling mode (fragment-level guard)
        // Both must be true: serial exchange semantics AND pooling mode active.
        // Note: cannot combine these into isSerialOperator() because useSerialSource()
        // calls planRoot.isSerialOperator() which would cause infinite recursion.
        if (linkNode.isSerialOperator()
                && linkNode.getFragment().useSerialSource(
                        statementContext.getConnectContext())) {
            return getFirstInstancePerWorker(receiverPlan.getInstanceJobs());
        } else if (enableShareHashTableForBroadcastJoin && linkNode.isRightChildOfBroadcastHashJoin()) {
            // For broadcast join with shared hash table, only 1 instance per BE receives
            // the build side data; other instances on the same BE share that hash table.
            return getFirstInstancePerWorker(receiverPlan.getInstanceJobs());
        } else {
            return receiverPlan.getInstanceJobs();
        }
    }

    private List<AssignedJob> sortDestinationInstancesByBuckets(
            PipelineDistributedPlan plan, List<AssignedJob> unsorted, int bucketNum,
            boolean useJoinBucketAssignment) {
        AssignedJob[] instances = new AssignedJob[bucketNum];
        for (AssignedJob instanceJob : unsorted) {
            // For non-serial exchanges with LocalShuffleBucketJoinAssignedJob, use join bucket
            // assignments rather than scan source buckets. In pooling mode, scan source buckets
            // are pooled to one instance per BE, but join buckets are distributed across instances
            // for parallelism. This must be consistent with
            // ThriftPlansBuilder.computeBucketIdToInstanceId().
            // For serial exchanges, scan source buckets are correct because the serial exchange
            // creates a local exchange that redistributes data to all instances.
            Set<Integer> bucketIndices;
            if (useJoinBucketAssignment
                    && instanceJob instanceof LocalShuffleBucketJoinAssignedJob) {
                bucketIndices = ((LocalShuffleBucketJoinAssignedJob) instanceJob)
                        .getAssignedJoinBucketIndexes();
            } else {
                BucketScanSource bucketScanSource = (BucketScanSource) instanceJob.getScanSource();
                bucketIndices = bucketScanSource.bucketIndexToScanNodeToTablets.keySet();
            }
            for (Integer bucketIndex : bucketIndices) {
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

    private List<AssignedJob> getFirstInstancePerWorker(List<AssignedJob> instances) {
        Map<DistributedPlanWorker, AssignedJob> firstInstancePerWorker = Maps.newLinkedHashMap();
        for (AssignedJob instance : instances) {
            firstInstancePerWorker.putIfAbsent(instance.getAssignedWorker(), instance);
        }
        return Utils.fastToImmutableList(firstInstancePerWorker.values());
    }

    private void updateProfileIfPresent(Consumer<SummaryProfile> profileAction) {
        Optional.ofNullable(statementContext.getConnectContext())
                .map(ConnectContext::getExecutor)
                .map(StmtExecutor::getSummaryProfile)
                .ifPresent(profileAction);
    }
}
