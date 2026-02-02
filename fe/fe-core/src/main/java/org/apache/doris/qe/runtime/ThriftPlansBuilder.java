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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.AIResource;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleBucketJoinAssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedScanBucketOlapTableJob;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RecursiveCteNode;
import org.apache.doris.planner.RecursiveCteScanNode;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SortNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TAIResource;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TPlanFragmentDestination;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TRecCTENode;
import org.apache.doris.thrift.TRecCTEResetInfo;
import org.apache.doris.thrift.TRecCTETarget;
import org.apache.doris.thrift.TRuntimeFilterInfo;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TTopnFilterDesc;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Suppliers;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class ThriftPlansBuilder {
    private static final Logger LOG = LogManager.getLogger(ThriftPlansBuilder.class);

    public static Map<DistributedPlanWorker, TPipelineFragmentParamsList> plansToThrift(
            CoordinatorContext coordinatorContext) {

        List<PipelineDistributedPlan> distributedPlans = coordinatorContext.distributedPlans;
        Set<Integer> fragmentToNotifyClose = setParamsForRecursiveCteNode(distributedPlans,
                coordinatorContext.runtimeFilters);

        // we should set runtime predicate first, then we can use heap sort and to thrift
        setRuntimePredicateIfNeed(coordinatorContext.scanNodes);

        RuntimeFiltersThriftBuilder runtimeFiltersThriftBuilder = RuntimeFiltersThriftBuilder.compute(
                coordinatorContext.runtimeFilters, distributedPlans);
        Supplier<List<TTopnFilterDesc>> topNFilterThriftSupplier
                = topNFilterToThrift(coordinatorContext.topnFilters);

        Multiset<DistributedPlanWorker> workerProcessInstanceNum = computeInstanceNumPerWorker(distributedPlans);
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> fragmentsGroupByWorker = Maps.newLinkedHashMap();
        int currentInstanceIndex = 0;
        Map<Integer, TFileScanRangeParams> sharedFileScanRangeParams = Maps.newLinkedHashMap();
        for (PipelineDistributedPlan currentFragmentPlan : distributedPlans) {
            sharedFileScanRangeParams.putAll(computeFileScanRangeParams(currentFragmentPlan));

            Map<Integer, Integer> exchangeSenderNum = computeExchangeSenderNum(currentFragmentPlan);
            ListMultimap<DistributedPlanWorker, AssignedJob> instancesPerWorker
                    = groupInstancePerWorker(currentFragmentPlan);
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToCurrentFragment = Maps.newLinkedHashMap();

            for (AssignedJob instanceJob : currentFragmentPlan.getInstanceJobs()) {
                TPipelineFragmentParams currentFragmentParam = fragmentToThriftIfAbsent(
                        currentFragmentPlan, instanceJob, workerToCurrentFragment,
                        instancesPerWorker, exchangeSenderNum, sharedFileScanRangeParams,
                        workerProcessInstanceNum, fragmentToNotifyClose, coordinatorContext);

                TPipelineInstanceParams instanceParam = instanceToThrift(
                        currentFragmentParam, instanceJob, currentInstanceIndex++);
                currentFragmentParam.getLocalParams().add(instanceParam);
            }

            // arrange fragments by the same worker,
            // so we can merge and send multiple fragment to a backend use one rpc
            for (Entry<DistributedPlanWorker, TPipelineFragmentParams> kv : workerToCurrentFragment.entrySet()) {
                TPipelineFragmentParamsList fragments = fragmentsGroupByWorker.computeIfAbsent(
                        kv.getKey(), w -> beToThrift(kv.getKey(), runtimeFiltersThriftBuilder,
                                topNFilterThriftSupplier));
                fragments.addToParamsList(kv.getValue());
            }
        }

        // backend should initialize fragment from target to source in backend, then
        // it can bind the receiver fragment for the sender fragment, but frontend
        // compute thrift message from source to fragment, so we need to reverse fragments.
        for (DistributedPlanWorker worker : fragmentsGroupByWorker.keySet()) {
            Collections.reverse(fragmentsGroupByWorker.get(worker).getParamsList());
        }

        setParamsForOlapTableSink(distributedPlans, fragmentsGroupByWorker, coordinatorContext);

        // remove redundant params to reduce rpc message size
        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv : fragmentsGroupByWorker.entrySet()) {
            boolean isFirstFragmentInCurrentBackend = true;
            for (TPipelineFragmentParams fragmentParams : kv.getValue().getParamsList()) {
                if (!isFirstFragmentInCurrentBackend) {
                    fragmentParams.unsetDescTbl();
                    fragmentParams.unsetFileScanParams();
                    fragmentParams.unsetCoord();
                    fragmentParams.unsetQueryGlobals();
                    fragmentParams.unsetResourceInfo();
                    fragmentParams.setIsSimplifiedParam(true);
                }
                isFirstFragmentInCurrentBackend = false;
            }
        }
        return fragmentsGroupByWorker;
    }

    private static ListMultimap<DistributedPlanWorker, AssignedJob>
            groupInstancePerWorker(PipelineDistributedPlan fragmentPlan) {
        ListMultimap<DistributedPlanWorker, AssignedJob> workerToInstances = ArrayListMultimap.create();
        for (AssignedJob instanceJob : fragmentPlan.getInstanceJobs()) {
            workerToInstances.put(instanceJob.getAssignedWorker(), instanceJob);
        }
        return workerToInstances;
    }

    private static void setRuntimePredicateIfNeed(Collection<ScanNode> scanNodes) {
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                for (SortNode topnFilterSortNode : scanNode.getTopnFilterSortNodes()) {
                    topnFilterSortNode.setHasRuntimePredicate();
                }
            }
        }
    }

    private static Supplier<List<TTopnFilterDesc>> topNFilterToThrift(List<TopnFilter> topnFilters) {
        return Suppliers.memoize(() -> {
            if (CollectionUtils.isEmpty(topnFilters)) {
                return null;
            }

            List<TTopnFilterDesc> filterDescs = new ArrayList<>(topnFilters.size());
            for (TopnFilter topnFilter : topnFilters) {
                filterDescs.add(topnFilter.toThrift());
            }
            return filterDescs;
        });
    }

    private static void setParamsForOlapTableSink(List<PipelineDistributedPlan> distributedPlans,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> fragmentsGroupByWorker,
            CoordinatorContext coordinatorContext) {
        int numBackendsWithSink = 0;
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            PlanFragment fragment = distributedPlan.getFragmentJob().getFragment();
            if (fragment.getSink() instanceof OlapTableSink) {
                numBackendsWithSink += (int) distributedPlan.getInstanceJobs()
                        .stream()
                        .map(AssignedJob::getAssignedWorker)
                        .distinct()
                        .count();
            }
        }

        ConnectContext connectContext = coordinatorContext.connectContext;
        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv : fragmentsGroupByWorker.entrySet()) {
            TPipelineFragmentParamsList fragments = kv.getValue();
            for (TPipelineFragmentParams fragmentParams : fragments.getParamsList()) {
                if (fragmentParams.getFragment().getOutputSink().getType() == TDataSinkType.OLAP_TABLE_SINK) {
                    int loadStreamPerNode = 1;
                    if (connectContext != null && connectContext.getSessionVariable() != null) {
                        loadStreamPerNode = connectContext.getSessionVariable().getLoadStreamPerNode();
                    }
                    fragmentParams.setLoadStreamPerNode(loadStreamPerNode);
                    fragmentParams.setTotalLoadStreams(numBackendsWithSink * loadStreamPerNode);
                    fragmentParams.setNumLocalSink(fragmentParams.getLocalParams().size());
                    LOG.info("num local sink for backend {} is {}", fragmentParams.getBackendId(),
                            fragmentParams.getNumLocalSink());
                }
            }
        }
    }

    private static Multiset<DistributedPlanWorker> computeInstanceNumPerWorker(
            List<PipelineDistributedPlan> distributedPlans) {
        Multiset<DistributedPlanWorker> workerCounter = LinkedHashMultiset.create();
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            for (AssignedJob instanceJob : distributedPlan.getInstanceJobs()) {
                workerCounter.add(instanceJob.getAssignedWorker());
            }
        }
        return workerCounter;
    }

    private static Map<Integer, Integer> computeExchangeSenderNum(PipelineDistributedPlan distributedPlan) {
        Map<Integer, Integer> senderNum = Maps.newLinkedHashMap();
        for (Entry<ExchangeNode, DistributedPlan> kv : distributedPlan.getInputs().entries()) {
            ExchangeNode exchangeNode = kv.getKey();
            PipelineDistributedPlan childPlan = (PipelineDistributedPlan) kv.getValue();
            senderNum.merge(exchangeNode.getId().asInt(), childPlan.getInstanceJobs().size(), Integer::sum);
        }
        return senderNum;
    }

    private static void setMultiCastDestinationThriftIfNotSet(PipelineDistributedPlan fragmentPlan) {
        MultiCastDataSink multiCastDataSink = (MultiCastDataSink) fragmentPlan.getFragmentJob().getFragment().getSink();
        List<List<TPlanFragmentDestination>> destinationList = multiCastDataSink.getDestinations();

        List<DataStreamSink> dataStreamSinks = multiCastDataSink.getDataStreamSinks();
        for (int i = 0; i < dataStreamSinks.size(); i++) {
            List<TPlanFragmentDestination> destinations = destinationList.get(i);
            if (!destinations.isEmpty()) {
                // we should only set destination only once,
                // because all backends share the same MultiCastDataSink object
                continue;
            }
            DataStreamSink realSink = dataStreamSinks.get(i);
            for (Entry<DataSink, List<AssignedJob>> kv : fragmentPlan.getDestinations().entrySet()) {
                DataSink sink = kv.getKey();
                if (sink == realSink) {
                    List<AssignedJob> destInstances = kv.getValue();
                    for (AssignedJob destInstance : destInstances) {
                        destinations.add(instanceToDestination(destInstance));
                    }
                    break;
                }
            }
        }
    }

    private static List<TPlanFragmentDestination> nonMultiCastDestinationToThrift(PipelineDistributedPlan plan) {
        Map<DataSink, List<AssignedJob>> destinationsMapping = plan.getDestinations();
        List<TPlanFragmentDestination> destinations = Lists.newArrayList();
        if (!destinationsMapping.isEmpty()) {
            List<AssignedJob> destinationJobs = destinationsMapping.entrySet().iterator().next().getValue();
            for (AssignedJob destinationJob : destinationJobs) {
                destinations.add(instanceToDestination(destinationJob));
            }
        }
        return destinations;
    }

    private static TPlanFragmentDestination instanceToDestination(AssignedJob instance) {
        DistributedPlanWorker worker = instance.getAssignedWorker();
        String host = worker.host();
        int port = worker.port();
        int brpcPort = worker.brpcPort();

        TPlanFragmentDestination destination = new TPlanFragmentDestination();
        destination.setServer(new TNetworkAddress(host, port));
        destination.setBrpcServer(new TNetworkAddress(host, brpcPort));
        destination.setFragmentInstanceId(instance.instanceId());
        return destination;
    }

    private static TPipelineFragmentParamsList beToThrift(
            DistributedPlanWorker worker,
            RuntimeFiltersThriftBuilder runtimeFiltersThriftBuilder,
            Supplier<List<TTopnFilterDesc>> topNFilterThriftSupplier) {
        TPipelineFragmentParamsList beParam = new TPipelineFragmentParamsList();
        TRuntimeFilterInfo runtimeFilterInfo = new TRuntimeFilterInfo();
        runtimeFilterInfo.setTopnFilterDescs(topNFilterThriftSupplier.get());

        TRuntimeFilterParams runtimeFilterParams = new TRuntimeFilterParams();
        runtimeFilterParams.setRuntimeFilterMergeAddr(runtimeFiltersThriftBuilder.mergeAddress);
        if (worker.host().equals(runtimeFiltersThriftBuilder.mergeAddress.getHostname())
                && worker.brpcPort() == runtimeFiltersThriftBuilder.mergeAddress.getPort()) {
            // only set following information for merge BE node
            runtimeFiltersThriftBuilder.populateRuntimeFilterParams(runtimeFilterParams);
        }
        runtimeFilterInfo.setRuntimeFilterParams(runtimeFilterParams);
        beParam.setRuntimeFilterInfo(runtimeFilterInfo);

        return beParam;
    }

    private static TPipelineFragmentParams fragmentToThriftIfAbsent(
            PipelineDistributedPlan fragmentPlan, AssignedJob assignedJob,
            Map<DistributedPlanWorker, TPipelineFragmentParams> workerToFragmentParams,
            ListMultimap<DistributedPlanWorker, AssignedJob> instancesPerWorker,
            Map<Integer, Integer> exchangeSenderNum,
            Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap,
            Multiset<DistributedPlanWorker> workerProcessInstanceNum,
            Set<Integer> fragmentToNotifyClose,
            CoordinatorContext coordinatorContext) {
        DistributedPlanWorker worker = assignedJob.getAssignedWorker();
        return workerToFragmentParams.computeIfAbsent(worker, w -> {
            PlanFragment fragment = fragmentPlan.getFragmentJob().getFragment();
            ConnectContext connectContext = coordinatorContext.connectContext;

            TPipelineFragmentParams params = new TPipelineFragmentParams();
            params.setIsNereids(true);
            params.setBackendId(worker.id());
            params.setProtocolVersion(PaloInternalServiceVersion.V1);
            params.setDescTbl(coordinatorContext.descriptorTable);
            params.setQueryId(coordinatorContext.queryId);
            params.setFragmentId(fragment.getFragmentId().asInt());
            if (fragmentToNotifyClose.contains(params.getFragmentId())) {
                params.setNeedNotifyClose(true);
            }

            // Each tParam will set the total number of Fragments that need to be executed on the same BE,
            // and the BE will determine whether all Fragments have been executed based on this information.
            // Notice. load fragment has a small probability that FragmentNumOnHost is 0, for unknown reasons.
            params.setFragmentNumOnHost(workerProcessInstanceNum.count(worker));

            params.setNeedWaitExecutionTrigger(coordinatorContext.twoPhaseExecution());
            params.setPerExchNumSenders(exchangeSenderNum);

            List<TPlanFragmentDestination> nonMultiCastDestinations;
            if (fragment.getSink() instanceof MultiCastDataSink) {
                nonMultiCastDestinations = Lists.newArrayList();
                setMultiCastDestinationThriftIfNotSet(fragmentPlan);
            } else {
                nonMultiCastDestinations = nonMultiCastDestinationToThrift(fragmentPlan);
            }
            params.setDestinations(nonMultiCastDestinations);

            int instanceNumInThisFragment = fragmentPlan.getInstanceJobs().size();
            params.setNumSenders(instanceNumInThisFragment);
            params.setTotalInstances(instanceNumInThisFragment);

            params.setCoord(coordinatorContext.coordinatorAddress);
            params.setCurrentConnectFe(coordinatorContext.directConnectFrontendAddress.get());
            params.setQueryGlobals(coordinatorContext.queryGlobals);
            params.setQueryOptions(new TQueryOptions(coordinatorContext.queryOptions));
            long memLimit = coordinatorContext.queryOptions.getMemLimit();
            // update memory limit for colocate join
            if (connectContext != null
                    && !connectContext.getSessionVariable().isDisableColocatePlan()
                    && fragment.hasColocatePlanNode()) {
                int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceNumInThisFragment);
                memLimit = coordinatorContext.queryOptions.getMemLimit() / rate;
            }
            params.getQueryOptions().setMemLimit(memLimit);

            params.setSendQueryStatisticsWithEveryBatch(fragment.isTransferQueryStatisticsWithEveryBatch());

            TPlanFragment planThrift = fragment.toThrift();
            planThrift.query_cache_param = fragment.queryCacheParam;
            params.setFragment(planThrift);
            params.setLocalParams(Lists.newArrayList());
            params.setWorkloadGroups(coordinatorContext.getWorkloadGroups());

            params.setFileScanParams(fileScanRangeParamsMap);

            if (fragmentPlan.getFragmentJob() instanceof UnassignedScanBucketOlapTableJob) {
                int bucketNum = ((UnassignedScanBucketOlapTableJob) fragmentPlan.getFragmentJob())
                        .getOlapScanNodes()
                        .get(0)
                        .getBucketNum();
                params.setNumBuckets(bucketNum);
            }

            List<AssignedJob> instances = instancesPerWorker.get(worker);
            Map<AssignedJob, Integer> instanceToIndex = instanceToIndex(instances);

            // local shuffle params: bucket_seq_to_instance_idx and shuffle_idx_to_instance_idx
            params.setBucketSeqToInstanceIdx(computeBucketIdToInstanceId(fragmentPlan, w, instanceToIndex));
            params.setShuffleIdxToInstanceIdx(computeDestIdToInstanceId(fragmentPlan, w, instanceToIndex));

            // Only used for AI Functions
            Map<String, TAIResource> aiResourceMap = Maps.newLinkedHashMap();
            for (Resource resource : Env.getCurrentEnv().getResourceMgr().getResource(Resource.ResourceType.AI)) {
                if (resource instanceof AIResource) {
                    aiResourceMap.put(resource.getName(), ((AIResource) resource).toThrift());
                }
            }
            params.setAiResources(aiResourceMap);

            return params;
        });
    }

    private static Map<AssignedJob, Integer> instanceToIndex(List<AssignedJob> instances) {
        Map<AssignedJob, Integer> instanceToIndex = Maps.newLinkedHashMap();
        for (int instanceIndex = 0; instanceIndex < instances.size(); instanceIndex++) {
            instanceToIndex.put(instances.get(instanceIndex), instanceIndex);
        }

        return instanceToIndex;
    }

    private static Map<Integer, TFileScanRangeParams> computeFileScanRangeParams(
            PipelineDistributedPlan distributedPlan) {
        // scan node id -> TFileScanRangeParams
        Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap = Maps.newLinkedHashMap();
        for (ScanNode scanNode : distributedPlan.getFragmentJob().getScanNodes()) {
            if (scanNode instanceof FileQueryScanNode) {
                TFileScanRangeParams fileScanRangeParams = ((FileQueryScanNode) scanNode).getFileScanRangeParams();
                fileScanRangeParamsMap.put(scanNode.getId().asInt(), fileScanRangeParams);
            }
        }

        return fileScanRangeParamsMap;
    }

    private static TPipelineInstanceParams instanceToThrift(
            TPipelineFragmentParams currentFragmentParam, AssignedJob instance, int currentInstanceNum) {
        TPipelineInstanceParams instanceParam = new TPipelineInstanceParams();
        instanceParam.setFragmentInstanceId(instance.instanceId());
        setScanSourceParam(currentFragmentParam, instance, instanceParam);

        instanceParam.setSenderId(instance.indexInUnassignedJob());
        instanceParam.setBackendNum(currentInstanceNum);
        boolean isLocalShuffle = instance instanceof LocalShuffleAssignedJob;
        if (isLocalShuffle) {
            // a fragment in a backend only enable local shuffle once for the first local shuffle instance,
            // because we just skip set scan params for LocalShuffleAssignedJob.receiveDataFromLocal == true
            ignoreDataDistribution(currentFragmentParam);
        }
        return instanceParam;
    }

    private static void setScanSourceParam(
            TPipelineFragmentParams currentFragmentParam, AssignedJob instance,
            TPipelineInstanceParams instanceParams) {
        ScanSource scanSource = instance.getScanSource();
        PerNodeScanParams scanParams;
        if (scanSource instanceof BucketScanSource) {
            scanParams = computeBucketScanSourceParam((BucketScanSource) scanSource);
        } else {
            scanParams = computeDefaultScanSourceParam((DefaultScanSource) scanSource);
        }
        // perNodeScanRanges is required
        instanceParams.setPerNodeScanRanges(scanParams.perNodeScanRanges);
    }

    // local shuffle has two functions:
    // 1. use 10 scan instances -> local shuffle -> 10 agg instances, this function can balance data in agg
    // 2. use 1 scan instance -> local shuffle -> 10 agg, this function is ignore_data_distribution,
    //    it can add parallel in agg
    private static void ignoreDataDistribution(TPipelineFragmentParams currentFragmentParam) {
        // `parallel_instances == 1` is the switch of ignore_data_distribution,
        // and backend will use 1 instance to scan a little data, and local shuffle to
        // # SessionVariable.parallel_pipeline_task_num instances to increment parallel
        currentFragmentParam.setParallelInstances(1);
    }

    private static PerNodeScanParams computeDefaultScanSourceParam(DefaultScanSource defaultScanSource) {
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newLinkedHashMap();
        for (Entry<ScanNode, ScanRanges> kv : defaultScanSource.scanNodeToScanRanges.entrySet()) {
            int scanNodeId = kv.getKey().getId().asInt();
            perNodeScanRanges.put(scanNodeId, kv.getValue().params);
        }

        return new PerNodeScanParams(perNodeScanRanges);
    }

    private static PerNodeScanParams computeBucketScanSourceParam(BucketScanSource bucketScanSource) {
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newLinkedHashMap();
        for (Entry<Integer, Map<ScanNode, ScanRanges>> kv :
                bucketScanSource.bucketIndexToScanNodeToTablets.entrySet()) {
            Map<ScanNode, ScanRanges> scanNodeToRanges = kv.getValue();
            for (Entry<ScanNode, ScanRanges> kv2 : scanNodeToRanges.entrySet()) {
                int scanNodeId = kv2.getKey().getId().asInt();
                List<TScanRangeParams> scanRanges = perNodeScanRanges.computeIfAbsent(scanNodeId, ArrayList::new);
                scanRanges.addAll(kv2.getValue().params);
            }
        }
        return new PerNodeScanParams(perNodeScanRanges);
    }

    private static Map<Integer, Integer> computeBucketIdToInstanceId(
            PipelineDistributedPlan receivePlan, DistributedPlanWorker worker,
            Map<AssignedJob, Integer> instanceToIndex) {
        List<AssignedJob> instanceJobs = receivePlan.getInstanceJobs();
        if (instanceJobs.isEmpty() || !(instanceJobs.get(0).getScanSource() instanceof BucketScanSource)) {
            // bucket_seq_to_instance_id is optional, so we can return null to save memory
            return null;
        }

        Map<Integer, Integer> bucketIdToInstanceId = Maps.newLinkedHashMap();
        for (AssignedJob instanceJob : instanceJobs) {
            if (instanceJob.getAssignedWorker().id() != worker.id()) {
                continue;
            }

            Integer instanceIndex = instanceToIndex.get(instanceJob);
            if (instanceJob instanceof LocalShuffleBucketJoinAssignedJob) {
                LocalShuffleBucketJoinAssignedJob assignedJob = (LocalShuffleBucketJoinAssignedJob) instanceJob;
                for (Integer bucketIndex : assignedJob.getAssignedJoinBucketIndexes()) {
                    bucketIdToInstanceId.put(bucketIndex, instanceIndex);
                }
            } else {
                BucketScanSource bucketScanSource = (BucketScanSource) instanceJob.getScanSource();
                for (Integer bucketIndex : bucketScanSource.bucketIndexToScanNodeToTablets.keySet()) {
                    bucketIdToInstanceId.put(bucketIndex, instanceIndex);
                }
            }
        }
        return bucketIdToInstanceId;
    }

    private static Map<Integer, Integer> computeDestIdToInstanceId(
            PipelineDistributedPlan receivePlan, DistributedPlanWorker worker,
            Map<AssignedJob, Integer> instanceToIndex) {
        if (receivePlan.getInputs().isEmpty()) {
            // shuffle_idx_to_index_id is required
            return Maps.newLinkedHashMap();
        }

        Map<Integer, Integer> destIdToInstanceId = Maps.newLinkedHashMap();
        filterInstancesWhichReceiveDataFromRemote(
                receivePlan, worker,
                (instanceJob, destId) -> destIdToInstanceId.put(destId, instanceToIndex.get(instanceJob))
        );
        return destIdToInstanceId;
    }

    private static void filterInstancesWhichReceiveDataFromRemote(
            PipelineDistributedPlan receivePlan, DistributedPlanWorker filterWorker,
            BiConsumer<AssignedJob, Integer> computeFn) {

        // current only support all input plans have same destination with same order,
        // so we can get first input plan to compute shuffle index to instance id
        Set<Entry<ExchangeNode, DistributedPlan>> exchangeToChildPlanSet = receivePlan.getInputs().entries();
        if (exchangeToChildPlanSet.isEmpty()) {
            return;
        }
        Entry<ExchangeNode, DistributedPlan> exchangeToChildPlan = exchangeToChildPlanSet.iterator().next();
        ExchangeNode linkNode = exchangeToChildPlan.getKey();
        PipelineDistributedPlan firstInputPlan = (PipelineDistributedPlan) exchangeToChildPlan.getValue();
        Map<DataSink, List<AssignedJob>> sinkToDestInstances = firstInputPlan.getDestinations();
        for (Entry<DataSink, List<AssignedJob>> kv : sinkToDestInstances.entrySet()) {
            DataSink senderSink = kv.getKey();
            if (senderSink.getExchNodeId().asInt() == linkNode.getId().asInt()) {
                for (int destId = 0; destId < kv.getValue().size(); destId++) {
                    AssignedJob assignedJob = kv.getValue().get(destId);
                    if (assignedJob.getAssignedWorker().id() == filterWorker.id()) {
                        computeFn.accept(assignedJob, destId);
                    }
                }
                break;
            }
        }
    }

    private static Set<Integer> setParamsForRecursiveCteNode(List<PipelineDistributedPlan> distributedPlans,
            List<RuntimeFilter> runtimeFilters) {
        /*
         * Populate and attach recursive-CTE related Thrift structures used by
         * backends (BE) to coordinate recursive Common Table Expression (CTE)
         * execution across fragments and fragment instances.
         *
         * This method performs the following responsibilities:
         * - Traverse the provided `distributedPlans` in bottom-up order (this
         *   ordering is expected by callers) and collect the set of network
         *   addresses (host + brpcPort) for every fragment. These addresses are
         *   used to reset and control recursive CTE child fragments from the
         *   producer side.
         * - Detect `RecursiveCteScanNode` within fragments and build a
         *   `TRecCTETarget` for each such scan. A `TRecCTETarget` captures the
         *   network address and a representative fragment instance id and the
         *   scan node id that the recursive producer should send data to.
         *   Exactly one `RecursiveCteScanNode` is expected per fragment that
         *   contains a scan for a recursive CTE; otherwise an
         *   IllegalStateException is thrown.
         * - For every `RecursiveCteNode` (producer/union node), collect its
         *   child fragments that implement the recursive side. For each child
         *   fragment, add the corresponding `TRecCTETarget` (if present) to
         *   the producer's target list and create `TRecCTEResetInfo` entries
         *   for all instances of that fragment. `TRecCTEResetInfo` entries
         *   carry the fragment id and addresses to be reset by the producer
         *   when a new recursion iteration begins.
         * - Populate the `TRecCTENode` object attached to the
         *   `RecursiveCteNode`, including: whether it's `UNION ALL`, result
         *   expression lists (materialized result expressions converted to
         *   Thrift `TExpr`), list of targets, fragments-to-reset, runtime
         *   filter ids that must be reset on the recursive side, and a flag
         *   indicating whether this recursive CTE node is used by other
         *   recursive CTEs.
         *
         * How runtime filters are handled:
         * - Build `runtimeFiltersToReset` by scanning provided
         *   `runtimeFilters`. A filter id is added if the filter has remote
         *   targets and if the recursive side (right child) contains the
         *   runtime-filter builder node. These filter ids are attached to the
         *   `TRecCTENode` so BE can reset the corresponding runtime filters
         *   between recursive iterations.
         *
         * Important assumptions and invariants:
         * - `distributedPlans` must be ordered bottom-up so that child
         *   fragments (containing `RecursiveCteScanNode`) are visited before
         *   their producers. The implementation relies on this to pop
         *   consumed `TRecCTETarget` entries from `fragmentIdToRecCteTargetMap`
         *   to avoid a parent producer incorrectly picking up grandchild
         *   scan nodes.
         * - Each fragment containing a `RecursiveCteScanNode` must have at
         *   least one assigned job (instance). If not, an
         *   IllegalStateException is thrown.
         * - At most one `RecursiveCteScanNode` per fragment is supported; if
         *   more than one is found an IllegalStateException is thrown.
         *
         * @param distributedPlans ordered list of PipelineDistributedPlan in
         *                         bottom-up traversal order
         * @param runtimeFilters   list of runtime filters to consider for reset
         * @return set of fragment ids (as integers) that need to be notified
         *         to close for recursive CTE handling
         */
        // fragments whose child recursive fragments need to be notified to close
        Set<Integer> fragmentToNotifyClose = new HashSet<>();
        // mapping from fragment id -> TRecCTETarget (the scan node target info)
        Map<PlanFragmentId, List<TRecCTETarget>> fragmentIdToRecCteTargetMap = new TreeMap<>();
        // mapping from fragment id -> set of network addresses for all instances
        Map<PlanFragmentId, Set<TNetworkAddress>> fragmentIdToNetworkAddressMap = new TreeMap<>();
        // distributedPlans is ordered in bottom up way, so does the fragments
        for (PipelineDistributedPlan plan : distributedPlans) {
            // collect all assigned instance network addresses for this fragment
            List<AssignedJob> fragmentAssignedJobs = plan.getInstanceJobs();
            Set<TNetworkAddress> networkAddresses = new TreeSet<>();
            Map<TNetworkAddress, TUniqueId> addressTUniqueIdMap = new TreeMap<>();
            for (AssignedJob assignedJob : fragmentAssignedJobs) {
                DistributedPlanWorker distributedPlanWorker = assignedJob.getAssignedWorker();
                // use brpc port + host as the address used by BE for control/reset
                TNetworkAddress networkAddress = new TNetworkAddress(distributedPlanWorker.host(),
                        distributedPlanWorker.brpcPort());
                if (networkAddresses.add(networkAddress)) {
                    addressTUniqueIdMap.put(networkAddress, assignedJob.instanceId());
                }
            }
            PlanFragment planFragment = plan.getFragmentJob().getFragment();
            // remember addresses for later when building reset infos
            fragmentIdToNetworkAddressMap.put(planFragment.getFragmentId(), networkAddresses);

            // find RecursiveCteScanNode in this fragment (scan side of recursive CTE)
            List<RecursiveCteScanNode> recursiveCteScanNodes = planFragment.getPlanRoot()
                    .collectInCurrentFragment(RecursiveCteScanNode.class::isInstance);
            if (!recursiveCteScanNodes.isEmpty()) {
                // validate there is exactly one scan node per fragment
                if (recursiveCteScanNodes.size() != 1) {
                    throw new IllegalStateException(
                            String.format("one fragment can only have 1 recursive cte scan node, but there is %d",
                                    recursiveCteScanNodes.size()));
                }
                // scan fragments must have at least one assigned instance
                if (fragmentAssignedJobs.isEmpty()) {
                    throw new IllegalStateException(
                            "fragmentAssignedJobs is empty for recursive cte scan node");
                }
                // Build a TRecCTETargets
                List<TRecCTETarget> recCTETargets = new ArrayList<>(addressTUniqueIdMap.size());
                for (Entry<TNetworkAddress, TUniqueId> entry : addressTUniqueIdMap.entrySet()) {
                    TRecCTETarget tRecCTETarget = new TRecCTETarget();
                    tRecCTETarget.setAddr(entry.getKey());
                    tRecCTETarget.setFragmentInstanceId(entry.getValue());
                    tRecCTETarget.setNodeId(recursiveCteScanNodes.get(0).getId().asInt());
                    recCTETargets.add(tRecCTETarget);
                }
                // store the target for producers to reference later
                fragmentIdToRecCteTargetMap.put(planFragment.getFragmentId(), recCTETargets);
            }

            List<RecursiveCteNode> recursiveCteNodes = planFragment.getPlanRoot()
                    .collectInCurrentFragment(RecursiveCteNode.class::isInstance);
            for (RecursiveCteNode recursiveCteNode : recursiveCteNodes) {
                // list of scan targets this producer should send recursive rows to
                List<TRecCTETarget> targets = new ArrayList<>();
                // reset infos for all instances of child fragments (used to reset state)
                List<TRecCTEResetInfo> fragmentsToReset = new ArrayList<>();
                // The recursive side is under the right child; collect all fragments
                List<PlanFragment> childFragments = new ArrayList<>();
                recursiveCteNode.getChild(1).getChild(0).getFragment().collectAll(PlanFragment.class::isInstance,
                        childFragments);
                for (PlanFragment child : childFragments) {
                    PlanFragmentId childFragmentId = child.getFragmentId();
                    // mark this child fragment id so it will be notified to close
                    fragmentToNotifyClose.add(childFragmentId.asInt());
                    // add target if a matching RecursiveCteScanNode was recorded
                    List<TRecCTETarget> recCTETargets = fragmentIdToRecCteTargetMap.getOrDefault(childFragmentId, null);
                    if (recCTETargets != null) {
                        // each producer can only map to one scan node target per child
                        targets.addAll(recCTETargets);
                        // remove the entry so ancestor producers won't reuse a grandchild scan node
                        fragmentIdToRecCteTargetMap.remove(childFragmentId);
                    }
                    // get all instance addresses for this child fragment and build reset infos
                    Set<TNetworkAddress> tNetworkAddresses = fragmentIdToNetworkAddressMap.get(childFragmentId);
                    if (tNetworkAddresses == null) {
                        throw new IllegalStateException(
                                String.format("can't find TNetworkAddress for fragment %d", childFragmentId));
                    }
                    for (TNetworkAddress address : tNetworkAddresses) {
                        TRecCTEResetInfo tRecCTEResetInfo = new TRecCTEResetInfo();
                        tRecCTEResetInfo.setFragmentId(childFragmentId.asInt());
                        tRecCTEResetInfo.setAddr(address);
                        fragmentsToReset.add(tRecCTEResetInfo);
                    }
                }

                // convert materialized result expression lists to Thrift TExpr lists
                List<List<Expr>> materializedResultExprLists = recursiveCteNode.getMaterializedResultExprLists();
                List<List<TExpr>> texprLists = new ArrayList<>(materializedResultExprLists.size());
                for (List<Expr> exprList : materializedResultExprLists) {
                    texprLists.add(Expr.treesToThrift(exprList));
                }
                // the recursive side's rf need to be reset
                // determine which runtime filters on the recursive side must be reset
                List<Integer> runtimeFiltersToReset = new ArrayList<>(runtimeFilters.size());
                for (RuntimeFilter rf : runtimeFilters) {
                    // only consider filters that have remote targets and whose builder
                    // node is present in the recursive side (right child)
                    if (rf.hasRemoteTargets()
                            && recursiveCteNode.getChild(1).contains(node -> node == rf.getBuilderNode())) {
                        runtimeFiltersToReset.add(rf.getFilterId().asInt());
                    }
                }
                // find recursiveCte used by other recursive cte
                // detect whether this recursive CTE node is referenced by other
                // recursive CTEs in the recursive side; needed to correctly
                // indicate sharing/usage across recursive nodes
                Set<RecursiveCteNode> recursiveCteNodesInRecursiveSide = new HashSet<>();
                PlanNode rootPlan = distributedPlans.get(distributedPlans.size() - 1)
                        .getFragmentJob().getFragment().getPlanRoot();
                collectAllRecursiveCteNodesInRecursiveSide(rootPlan, false, recursiveCteNodesInRecursiveSide);
                boolean isUsedByOtherRecCte = recursiveCteNodesInRecursiveSide.contains(recursiveCteNode);

                // build the Thrift TRecCTENode and attach it to the RecursiveCteNode
                TRecCTENode tRecCTENode = new TRecCTENode();
                tRecCTENode.setIsUnionAll(recursiveCteNode.isUnionAll());
                tRecCTENode.setTargets(targets);
                tRecCTENode.setFragmentsToReset(fragmentsToReset);
                tRecCTENode.setResultExprLists(texprLists);
                tRecCTENode.setRecSideRuntimeFilterIds(runtimeFiltersToReset);
                tRecCTENode.setIsUsedByOtherRecCte(isUsedByOtherRecCte);
                // attach Thrift node to plan node for BE consumption
                recursiveCteNode.settRecCTENode(tRecCTENode);
            }
        }
        return fragmentToNotifyClose;
    }

    private static void collectAllRecursiveCteNodesInRecursiveSide(PlanNode planNode, boolean needCollect,
            Set<RecursiveCteNode> recursiveCteNodes) {
        if (planNode instanceof RecursiveCteNode) {
            if (needCollect) {
                recursiveCteNodes.add((RecursiveCteNode) planNode);
            }
            collectAllRecursiveCteNodesInRecursiveSide(planNode.getChild(0), needCollect, recursiveCteNodes);
            collectAllRecursiveCteNodesInRecursiveSide(planNode.getChild(1), true, recursiveCteNodes);
        } else {
            for (PlanNode child : planNode.getChildren()) {
                collectAllRecursiveCteNodesInRecursiveSide(child, needCollect, recursiveCteNodes);
            }
        }
    }

    private static class PerNodeScanParams {
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges;

        public PerNodeScanParams(Map<Integer, List<TScanRangeParams>> perNodeScanRanges) {
            this.perNodeScanRanges = perNodeScanRanges;
        }
    }
}
