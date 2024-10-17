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
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.runtime.LoadProcessor;
import org.apache.doris.qe.runtime.QueryProcessor;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
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

public class SqlCoordinatorContext {
    private static final Logger LOG = LogManager.getLogger(SqlCoordinatorContext.class);

    // these are some constant parameters
    public final NereidsSqlCoordinator coordinator;
    public final DataSink dataSink;
    public final ExecutionProfile executionProfile;
    public final ConnectContext connectContext;
    public final NereidsPlanner planner;
    public final TUniqueId queryId;
    public final TQueryGlobals queryGlobals;
    public final TQueryOptions queryOptions;
    public final TDescriptorTable descriptorTable;
    public final TNetworkAddress coordinatorAddress;
    public final TNetworkAddress directConnectFrontendAddress;
    public final long timeoutDeadline;
    public final boolean twoPhaseExecution;
    public final int instanceNum;
    public final Supplier<Set<TUniqueId>> instanceIds = Suppliers.memoize(this::getInstanceIds);
    public final Supplier<Map<TNetworkAddress, Long>> backends = Suppliers.memoize(this::getBackends);
    public final Supplier<Integer> scanRangeNum = Suppliers.memoize(this::getScanRangeNum);

    // these are some mutable states
    private volatile Status status;
    private volatile Optional<QueryQueue> queryQueue;
    private volatile Optional<QueueToken> queueToken;
    private volatile List<TPipelineWorkloadGroup> workloadGroups = ImmutableList.of();

    // query or load processor
    private volatile JobProcessor jobProcessor;

    private SqlCoordinatorContext(NereidsSqlCoordinator coordinator,
            ConnectContext connectContext,
            NereidsPlanner planner,
            ExecutionProfile executionProfile,
            TQueryGlobals queryGlobals,
            TQueryOptions queryOptions,
            TDescriptorTable descriptorTable,
            TNetworkAddress coordinatorAddress,
            TNetworkAddress directConnectFrontendAddress) {
        this.connectContext = connectContext;
        this.planner = planner;
        this.queryId = connectContext.queryId();
        this.executionProfile = executionProfile;
        this.queryGlobals = queryGlobals;
        this.queryOptions = queryOptions;
        this.descriptorTable = descriptorTable;
        this.coordinatorAddress = coordinatorAddress;
        this.directConnectFrontendAddress = directConnectFrontendAddress;

        PipelineDistributedPlan topPlan = (PipelineDistributedPlan) planner.getDistributedPlans().last();
        this.dataSink = topPlan.getFragmentJob().getFragment().getSink();

        // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
        // else use exec_plan_fragments directly.
        // we choose #fragments > 1 because in some cases
        // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
        // For example: select * from numbers("number"="10") will generate ExchangeNode and
        // TableValuedFunctionScanNode, we should ensure TableValuedFunctionScanNode does not
        // send data until ExchangeNode is ready to receive.
        this.twoPhaseExecution = planner.getDistributedPlans().size() > 1;
        this.timeoutDeadline = System.currentTimeMillis() + queryOptions.getExecutionTimeout() * 1000L;

        this.coordinator = Objects.requireNonNull(coordinator, "coordinator can not be null");
        this.status = new Status();

        this.instanceNum = planner.getDistributedPlans().valueList()
                .stream().map(plan -> ((PipelineDistributedPlan) plan).getInstanceJobs().size())
                .reduce(Integer::sum)
                .get();

        this.queryQueue = Optional.empty();
        this.queueToken = Optional.empty();
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

    public static SqlCoordinatorContext build(NereidsPlanner planner, NereidsSqlCoordinator coordinator) {
        ConnectContext connectContext = planner.getCascadesContext().getConnectContext();
        TQueryOptions queryOptions = initQueryOptions(connectContext);
        TQueryGlobals queryGlobals = initQueryGlobals(connectContext);
        TDescriptorTable descriptorTable = planner.getDescTable().toThrift();
        TNetworkAddress coordinatorAddress = new TNetworkAddress(Coordinator.localIP, Config.rpc_port);
        String currentConnectedFEIp = connectContext.getCurrentConnectedFEIp();
        TNetworkAddress directConnectFrontendAddress =
                connectContext.isProxy() && !StringUtils.isBlank(currentConnectedFEIp)
                        ? new TNetworkAddress(currentConnectedFEIp, Config.rpc_port)
                        : coordinatorAddress;

        ExecutionProfile executionProfile = new ExecutionProfile(
                connectContext.queryId,
                planner.getFragments()
                        .stream()
                        .map(fragment -> fragment.getFragmentId().asInt())
                        .collect(Collectors.toList())
        );
        return new SqlCoordinatorContext(
                coordinator, connectContext, planner, executionProfile, queryGlobals, queryOptions, descriptorTable,
                coordinatorAddress, directConnectFrontendAddress
        );
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
        for (DistributedPlan distributedPlan : planner.getDistributedPlans().valueList()) {
            PipelineDistributedPlan pipelinePlan = (PipelineDistributedPlan) distributedPlan;
            List<AssignedJob> instanceJobs = pipelinePlan.getInstanceJobs();
            for (AssignedJob instanceJob : instanceJobs) {
                instanceIds.add(instanceJob.instanceId());
            }
        }
        return instanceIds;
    }

    private Map<TNetworkAddress, Long> getBackends() {
        Map<TNetworkAddress, Long> backends = Maps.newLinkedHashMap();
        for (DistributedPlan distributedPlan : planner.getDistributedPlans().valueList()) {
            PipelineDistributedPlan pipelinePlan = (PipelineDistributedPlan) distributedPlan;
            List<AssignedJob> instanceJobs = pipelinePlan.getInstanceJobs();
            for (AssignedJob instanceJob : instanceJobs) {
                DistributedPlanWorker worker = instanceJob.getAssignedWorker();
                backends.put(new TNetworkAddress(worker.address(), worker.port()), worker.id());
            }
        }
        return backends;
    }

    private Set<ScanNode> getScanNodes() {
        Set<ScanNode> scanNodes = Sets.newLinkedHashSet();

        for (DistributedPlan distributedPlan : planner.getDistributedPlans().valueList()) {
            PipelineDistributedPlan pipelinePlan = (PipelineDistributedPlan) distributedPlan;
            List<AssignedJob> instanceJobs = pipelinePlan.getInstanceJobs();
            for (AssignedJob instanceJob : instanceJobs) {
                ScanSource scanSource = instanceJob.getScanSource();
                if (scanSource instanceof DefaultScanSource) {
                    for (ScanNode scanNode : ((DefaultScanSource) scanSource).scanNodeToScanRanges.keySet()) {
                        scanNodes.add(scanNode);
                    }
                } else {
                    BucketScanSource bucketScanSource = (BucketScanSource) scanSource;
                    for (Map<ScanNode, ScanRanges> scanNodeToRanges
                            : bucketScanSource.bucketIndexToScanNodeToTablets.values()) {
                        scanNodes.addAll(scanNodeToRanges.keySet());
                    }
                }
            }
        }
        return scanNodes;
    }

    private int getScanRangeNum() {
        int scanRangeNum = 0;

        for (DistributedPlan distributedPlan : planner.getDistributedPlans().valueList()) {
            PipelineDistributedPlan pipelinePlan = (PipelineDistributedPlan) distributedPlan;
            List<AssignedJob> instanceJobs = pipelinePlan.getInstanceJobs();
            for (AssignedJob instanceJob : instanceJobs) {
                ScanSource scanSource = instanceJob.getScanSource();
                if (scanSource instanceof DefaultScanSource) {
                    for (ScanRanges scanRanges : ((DefaultScanSource) scanSource).scanNodeToScanRanges.values()) {
                        scanRangeNum += scanRanges.params.size();
                    }
                } else {
                    BucketScanSource bucketScanSource = (BucketScanSource) scanSource;
                    for (Map<ScanNode, ScanRanges> scanNodeToRanges
                            : bucketScanSource.bucketIndexToScanNodeToTablets.values()) {
                        for (ScanRanges scanRanges : scanNodeToRanges.values()) {
                            scanRangeNum += scanRanges.params.size();
                        }
                    }
                }
            }
        }
        return scanRangeNum;
    }
}
