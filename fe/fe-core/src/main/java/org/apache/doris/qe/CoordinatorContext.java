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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanSource;
import org.apache.doris.nereids.trees.plans.physical.TopnFilter;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.runtime.LoadProcessor;
import org.apache.doris.qe.runtime.QueryProcessor;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CoordinatorContext {
    private static final Logger LOG = LogManager.getLogger(CoordinatorContext.class);

    // these are some constant parameters
    public final NereidsCoordinator coordinator;
    public final List<PlanFragment> fragments;
    public final DataSink dataSink;
    public final ExecutionProfile executionProfile;
    public final ConnectContext connectContext;
    public final PipelineDistributedPlan topDistributedPlan;
    public final List<PipelineDistributedPlan> distributedPlans;
    public final TUniqueId queryId;
    public final TQueryGlobals queryGlobals;
    public final TQueryOptions queryOptions;
    public final TDescriptorTable descriptorTable;
    public final TNetworkAddress coordinatorAddress = new TNetworkAddress(Coordinator.localIP, Config.rpc_port);
    // public final TNetworkAddress directConnectFrontendAddress;
    public final List<RuntimeFilter> runtimeFilters;
    public final List<TopnFilter> topnFilters;
    public final List<ScanNode> scanNodes;
    public final Supplier<Long> timeoutDeadline = Suppliers.memoize(this::computeTimeoutDeadline);
    public final Supplier<Integer> instanceNum = Suppliers.memoize(this::computeInstanceNum);
    public final Supplier<Set<TUniqueId>> instanceIds = Suppliers.memoize(this::getInstanceIds);
    public final Supplier<Map<TNetworkAddress, Long>> backends = Suppliers.memoize(this::getBackends);
    public final Supplier<Integer> scanRangeNum = Suppliers.memoize(this::getScanRangeNum);
    public final Supplier<TNetworkAddress> directConnectFrontendAddress
            = Suppliers.memoize(this::computeDirectConnectCoordinator);

    // these are some mutable states
    private volatile Status status = new Status();
    private volatile Optional<QueryQueue> queryQueue = Optional.empty();
    private volatile Optional<QueueToken> queueToken = Optional.empty();
    private volatile List<TPipelineWorkloadGroup> workloadGroups = ImmutableList.of();

    // query or load processor
    private volatile JobProcessor jobProcessor;

    // for sql execution
    private CoordinatorContext(
            NereidsCoordinator coordinator,
            ConnectContext connectContext,
            List<PipelineDistributedPlan> distributedPlans,
            List<PlanFragment> fragments,
            List<RuntimeFilter> runtimeFilters,
            List<TopnFilter> topnFilters,
            List<ScanNode> scanNodes,
            ExecutionProfile executionProfile,
            TQueryGlobals queryGlobals,
            TQueryOptions queryOptions,
            TDescriptorTable descriptorTable) {
        this.connectContext = connectContext;
        this.fragments = fragments;
        this.distributedPlans = distributedPlans;
        this.topDistributedPlan = distributedPlans.get(distributedPlans.size() - 1);
        this.dataSink = topDistributedPlan.getFragmentJob().getFragment().getSink();
        this.runtimeFilters = runtimeFilters == null ? Lists.newArrayList() : runtimeFilters;
        this.topnFilters = topnFilters == null ? Lists.newArrayList() : topnFilters;
        this.scanNodes = scanNodes;
        this.queryId = connectContext.queryId();
        this.executionProfile = executionProfile;
        this.queryGlobals = queryGlobals;
        this.queryOptions = queryOptions;
        this.descriptorTable = descriptorTable;

        this.coordinator = Objects.requireNonNull(coordinator, "coordinator can not be null");
    }

    // for broker load
    private CoordinatorContext(
            NereidsCoordinator coordinator,
            long jobId,
            List<PlanFragment> fragments,
            List<PipelineDistributedPlan> distributedPlans,
            List<ScanNode> scanNodes,
            TUniqueId queryId,
            TQueryOptions queryOptions,
            TQueryGlobals queryGlobals,
            TDescriptorTable descriptorTable,
            ExecutionProfile executionProfile) {
        this.coordinator = coordinator;
        this.fragments = fragments;
        this.distributedPlans = distributedPlans;
        this.topDistributedPlan = distributedPlans.get(distributedPlans.size() - 1);
        this.dataSink = topDistributedPlan.getFragmentJob().getFragment().getSink();
        this.scanNodes = scanNodes;
        this.queryId = queryId;
        this.queryOptions = queryOptions;
        this.queryGlobals = queryGlobals;
        this.descriptorTable = descriptorTable;
        this.executionProfile = executionProfile;

        this.connectContext = ConnectContext.get();
        this.runtimeFilters = ImmutableList.of();
        this.topnFilters = ImmutableList.of();
        this.jobProcessor = new LoadProcessor(this, jobId);
    }

    public void setQueueInfo(QueryQueue queryQueue, QueueToken queueToken) {
        this.queryQueue = Optional.ofNullable(queryQueue);
        this.queueToken = Optional.ofNullable(queueToken);
    }

    public Optional<QueueToken> getQueueToken() {
        return this.queueToken;
    }

    public Optional<QueryQueue> getQueryQueue() {
        return queryQueue;
    }

    public void setWorkloadGroups(List<TPipelineWorkloadGroup> workloadGroups) {
        this.workloadGroups = workloadGroups;
    }

    public List<TPipelineWorkloadGroup> getWorkloadGroups() {
        return workloadGroups;
    }

    public void updateProfileIfPresent(Consumer<SummaryProfile> profileAction) {
        Optional.ofNullable(connectContext)
                .map(ConnectContext::getExecutor)
                .map(StmtExecutor::getSummaryProfile)
                .ifPresent(profileAction);
    }

    public boolean twoPhaseExecution() {
        // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
        // else use exec_plan_fragments directly.
        // we choose #fragments > 1 because in some cases
        // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
        // For example: select * from numbers("number"="10") will generate ExchangeNode and
        // TableValuedFunctionScanNode, we should ensure TableValuedFunctionScanNode does not
        // send data until ExchangeNode is ready to receive.
        return distributedPlans.size() > 1;
    }

    public boolean isEos() {
        return jobProcessor instanceof QueryProcessor && coordinator.isEos();
    }

    public void cancelSchedule(Status cancelReason) {
        coordinator.cancelInternal(cancelReason);
    }

    public synchronized void withLock(Runnable callback) {
        callback.run();
    }

    public synchronized <T> T withLock(Callable<T> callback) throws Exception {
        return callback.call();
    }

    public synchronized Status readCloneStatus() {
        return new Status(status.getErrorCode(), status.getErrorMsg());
    }

    public synchronized Status updateStatusIfOk(Status newStatus) {
        // If query is done, we will ignore their cancelled updates, and let the remote fragments to clean up async.
        Status originStatus = readCloneStatus();
        if (coordinator.getJobProcessor() instanceof QueryProcessor && coordinator.isEos()
                && newStatus.isCancelled()) {
            return originStatus;
        }
        // nothing to update
        if (newStatus.ok()) {
            return originStatus;
        }

        // don't override an error status; also, cancellation has already started
        if (!this.status.ok()) {
            return originStatus;
        }

        status = new Status(newStatus.getErrorCode(), newStatus.getErrorMsg());
        coordinator.cancelInternal(readCloneStatus());
        return originStatus;
    }

    public void setJobProcessor(JobProcessor jobProcessor) {
        this.jobProcessor = jobProcessor;
    }

    public JobProcessor getJobProcessor() {
        return jobProcessor;
    }

    public LoadProcessor asLoadProcessor() {
        return (LoadProcessor) jobProcessor;
    }

    public QueryProcessor asQueryProcessor() {
        return (QueryProcessor) jobProcessor;
    }

    public static CoordinatorContext buildForSql(NereidsPlanner planner, NereidsCoordinator coordinator) {
        ConnectContext connectContext = planner.getCascadesContext().getConnectContext();
        TQueryOptions queryOptions = initQueryOptions(connectContext);
        TQueryGlobals queryGlobals = initQueryGlobals(connectContext);
        TDescriptorTable descriptorTable = planner.getDescTable().toThrift();

        ExecutionProfile executionProfile = new ExecutionProfile(
                connectContext.queryId,
                planner.getFragments()
                        .stream()
                        .map(fragment -> fragment.getFragmentId().asInt())
                        .collect(Collectors.toList())
        );
        return new CoordinatorContext(
                coordinator, connectContext,
                planner.getDistributedPlans().valueList(),
                planner.getFragments(), planner.getRuntimeFilters(), planner.getTopnFilters(),
                planner.getScanNodes(), executionProfile, queryGlobals, queryOptions, descriptorTable
        );
    }

    public static CoordinatorContext buildForLoad(
            NereidsCoordinator coordinator,
            long jobId, TUniqueId queryId,
            List<PlanFragment> fragments,
            List<PipelineDistributedPlan> distributedPlans,
            List<ScanNode> scanNodes,
            DescriptorTable descTable,
            String timezone, boolean loadZeroTolerance,
            boolean enableProfile) {
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setEnableProfile(enableProfile);
        queryOptions.setBeExecVersion(Config.be_exec_version);

        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.now()));
        queryGlobals.setTimestampMs(System.currentTimeMillis());
        queryGlobals.setTimeZone(timezone);
        queryGlobals.setLoadZeroTolerance(loadZeroTolerance);

        ExecutionProfile executionProfile = new ExecutionProfile(
                queryId,
                fragments.stream()
                        .map(fragment -> fragment.getFragmentId().asInt())
                        .collect(Collectors.toList())
        );

        return new CoordinatorContext(coordinator, jobId, fragments, distributedPlans,
                scanNodes, queryId, queryOptions, queryGlobals, descTable.toThrift(),
                executionProfile);
    }

    private static TQueryOptions initQueryOptions(ConnectContext context) {
        TQueryOptions queryOptions = context.getSessionVariable().toThrift();
        queryOptions.setBeExecVersion(Config.be_exec_version);
        queryOptions.setQueryTimeout(context.getExecTimeout());
        queryOptions.setExecutionTimeout(context.getExecTimeout());
        if (queryOptions.getExecutionTimeout() < 1) {
            LOG.info("try set timeout less than 1", new RuntimeException(""));
        }
        queryOptions.setFeProcessUuid(ExecuteEnv.getInstance().getProcessUUID());
        queryOptions.setMysqlRowBinaryFormat(context.getCommand() == MysqlCommand.COM_STMT_EXECUTE);

        setOptionsFromUserProperty(context, queryOptions);
        return queryOptions;
    }

    private static TQueryGlobals initQueryGlobals(ConnectContext context) {
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.now()));
        queryGlobals.setTimestampMs(System.currentTimeMillis());
        queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());
        queryGlobals.setLoadZeroTolerance(false);
        if (context.getSessionVariable().getTimeZone().equals("CST")) {
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
        }
        return queryGlobals;
    }

    private static void setOptionsFromUserProperty(ConnectContext connectContext, TQueryOptions queryOptions) {
        String qualifiedUser = connectContext.getQualifiedUser();
        // set cpu resource limit
        int cpuLimit = Env.getCurrentEnv().getAuth().getCpuResourceLimit(qualifiedUser);
        if (cpuLimit > 0) {
            // overwrite the cpu resource limit from session variable;
            TResourceLimit resourceLimit = new TResourceLimit();
            resourceLimit.setCpuLimit(cpuLimit);
            queryOptions.setResourceLimit(resourceLimit);
        }
        // set exec mem limit
        long maxExecMemByte = connectContext.getSessionVariable().getMaxExecMemByte();
        long memLimit = maxExecMemByte > 0 ? maxExecMemByte :
                Env.getCurrentEnv().getAuth().getExecMemLimit(qualifiedUser);
        if (memLimit > 0) {
            // overwrite the exec_mem_limit from session variable;
            queryOptions.setMemLimit(memLimit);
            queryOptions.setMaxReservation(memLimit);
            queryOptions.setInitialReservationTotalClaims(memLimit);
            queryOptions.setBufferPoolLimit(memLimit);
        }
    }

    private Set<TUniqueId> getInstanceIds() {
        Set<TUniqueId> instanceIds = Sets.newLinkedHashSet();
        for (DistributedPlan distributedPlan : distributedPlans) {
            PipelineDistributedPlan pipelinePlan = (PipelineDistributedPlan) distributedPlan;
            List<AssignedJob> instanceJobs = pipelinePlan.getInstanceJobs();
            for (AssignedJob instanceJob : instanceJobs) {
                instanceIds.add(instanceJob.instanceId());
            }
        }
        return instanceIds;
    }

    private Integer computeInstanceNum() {
        return distributedPlans
                .stream()
                .map(plan -> plan.getInstanceJobs().size())
                .reduce(Integer::sum)
                .get();
    }

    private long computeTimeoutDeadline() {
        return System.currentTimeMillis() + queryOptions.getExecutionTimeout() * 1000L;
    }

    private Map<TNetworkAddress, Long> getBackends() {
        Map<TNetworkAddress, Long> backends = Maps.newLinkedHashMap();
        for (DistributedPlan distributedPlan : distributedPlans) {
            PipelineDistributedPlan pipelinePlan = (PipelineDistributedPlan) distributedPlan;
            List<AssignedJob> instanceJobs = pipelinePlan.getInstanceJobs();
            for (AssignedJob instanceJob : instanceJobs) {
                DistributedPlanWorker worker = instanceJob.getAssignedWorker();
                backends.put(new TNetworkAddress(worker.address(), worker.port()), worker.id());
            }
        }
        return backends;
    }

    private TNetworkAddress computeDirectConnectCoordinator() {
        if (connectContext != null && connectContext.isProxy()
                && !StringUtils.isEmpty(connectContext.getCurrentConnectedFEIp())) {
            return new TNetworkAddress(ConnectContext.get().getCurrentConnectedFEIp(), Config.rpc_port);
        } else {
            return coordinatorAddress;
        }
    }

    private int getScanRangeNum() {
        int scanRangeNum = 0;
        for (PipelineDistributedPlan distributedPlan : distributedPlans) {
            for (AssignedJob instanceJob : distributedPlan.getInstanceJobs()) {
                ScanSource scanSource = instanceJob.getScanSource();
                if (scanSource instanceof BucketScanSource) {
                    BucketScanSource bucketScanSource = (BucketScanSource) scanSource;
                    for (Map<ScanNode, ScanRanges> kv : bucketScanSource.bucketIndexToScanNodeToTablets.values()) {
                        for (ScanRanges scanRanges : kv.values()) {
                            for (TScanRangeParams param : scanRanges.params) {
                                scanRangeNum += computeScanRangeNumByScanRange(param);
                            }
                        }
                    }
                } else {
                    DefaultScanSource defaultScanSource = (DefaultScanSource) scanSource;
                    for (ScanRanges scanRanges : defaultScanSource.scanNodeToScanRanges.values()) {
                        for (TScanRangeParams param : scanRanges.params) {
                            scanRangeNum += computeScanRangeNumByScanRange(param);
                        }
                    }
                }
            }
        }
        return scanRangeNum;
    }

    private int computeScanRangeNumByScanRange(TScanRangeParams param) {
        int scanRangeNum = 0;
        TScanRange scanRange = param.getScanRange();
        if (scanRange == null) {
            return scanRangeNum;
        }
        TBrokerScanRange brokerScanRange = scanRange.getBrokerScanRange();
        if (brokerScanRange != null) {
            scanRangeNum += brokerScanRange.getRanges().size();
        }
        TExternalScanRange externalScanRange = scanRange.getExtScanRange();
        if (externalScanRange != null) {
            TFileScanRange fileScanRange = externalScanRange.getFileScanRange();
            if (fileScanRange != null) {
                if (fileScanRange.isSetRanges()) {
                    scanRangeNum += fileScanRange.getRanges().size();
                } else if (fileScanRange.isSetSplitSource()) {
                    scanRangeNum += fileScanRange.getSplitSource().getNumSplits();
                }
            }
        }
        TPaloScanRange paloScanRange = scanRange.getPaloScanRange();
        if (paloScanRange != null) {
            scanRangeNum += 1;
        }
        // TODO: more ranges?
        return scanRangeNum;
    }
}
