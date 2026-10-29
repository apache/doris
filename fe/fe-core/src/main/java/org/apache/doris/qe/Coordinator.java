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
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.qe.QueryStatisticsItem.FragmentInstanceInfo;
import org.apache.doris.qe.runtime.LoadProcessor;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.PipelineExecutionTask;
import org.apache.doris.qe.runtime.PipelineExecutionTaskBuilder;
import org.apache.doris.qe.runtime.QueryProcessor;
import org.apache.doris.qe.runtime.SingleFragmentPipelineTask;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.resource.BackendSelection;
import org.apache.doris.resource.BackendSelectionManager;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Coordinator */
public class Coordinator implements CoordInterface {
    public static final ExecutorService backendRpcCallbackExecutor = ThreadPoolManager.newDaemonProfileThreadPool(
            32, 100, "backend-rpc-callback", true);

    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    protected final CoordinatorContext coordinatorContext;

    protected volatile PipelineExecutionTask executionTask;

    private final boolean needEnqueue;

    // sql execution
    public Coordinator(ConnectContext context,
            NereidsPlanner planner, StatsErrorEstimator statsErrorEstimator) {
        this.coordinatorContext = CoordinatorContext.buildForSql(planner, this);
        this.needEnqueue = true;

        DataSink dataSink = coordinatorContext.dataSink;
        // output to mysql or to file
        if ((dataSink instanceof ResultSink || dataSink instanceof ResultFileSink)) {
            setForQuery();
        } else {
            setForInsert(-1L);
        }

        syncLocalShufflePlannerOption();
        Preconditions.checkState(!planner.getFragments().isEmpty()
                && coordinatorContext.instanceNum.get() > 0, "Fragment and Instance can not be empty˚");
    }

    // load, with JobId
    public Coordinator(ConnectContext context,
            NereidsPlanner planner, StatsErrorEstimator statsErrorEstimator, long jobId) {
        this.coordinatorContext = CoordinatorContext.buildForSql(planner, this);
        this.needEnqueue = true;

        // we don't need to check the dataSink, Because setting jobId means this must be a load operation
        setForInsert(jobId);
        syncLocalShufflePlannerOption();
        Preconditions.checkState(!planner.getFragments().isEmpty()
                && coordinatorContext.instanceNum.get() > 0, "Fragment and Instance can not be empty˚");
    }

    // broker load
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
            List<PlanFragment> fragments, List<PipelineDistributedPlan> distributedPlans,
            List<ScanNode> scanNodes, String timezone, boolean loadZeroTolerance,
            boolean enableProfile) {
        this.coordinatorContext = CoordinatorContext.buildForLoad(
                this, jobId, queryId, fragments, distributedPlans, scanNodes,
                descTable, timezone, loadZeroTolerance, enableProfile
        );
        // same reason in `setForInsert`
        this.coordinatorContext.queryOptions.setDisableFileCache(true);
        this.needEnqueue = false;
        syncLocalShufflePlannerOption();

        Preconditions.checkState(!fragments.isEmpty()
                && coordinatorContext.instanceNum.get() > 0, "Fragment and Instance can not be empty˚");
    }

    private void syncLocalShufflePlannerOption() {
        coordinatorContext.queryOptions.setEnableLocalShufflePlanner(
                coordinatorContext.distributedPlans != null
                && !coordinatorContext.distributedPlans.isEmpty()
                && coordinatorContext.connectContext != null
                && coordinatorContext.connectContext.getSessionVariable().isEnableLocalShufflePlanner());
    }

    @Override
    public void exec() throws Exception {
        Status status = getQueryStatus();
        if (!status.ok()) {
            throw new UserException(status.getErrorMsg());
        }
        enqueue(coordinatorContext.connectContext);

        processTopSink(coordinatorContext, coordinatorContext.topDistributedPlan);
        status = getQueryStatus();
        if (!status.ok()) {
            throw new UserException(status.getErrorMsg());
        }

        QeProcessorImpl.INSTANCE.registerInstances(coordinatorContext.queryId, coordinatorContext.instanceNum.get());

        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragments
                = ThriftPlansBuilder.plansToThrift(coordinatorContext);
        executionTask = PipelineExecutionTaskBuilder.build(coordinatorContext, workerToFragments);
        executionTask.execute();
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() > coordinatorContext.timeoutDeadline.get();
    }

    @Override
    public void cancel(Status cancelReason) {
        if (cancelReason.ok()) {
            throw new RuntimeException("Should use correct cancel reason, but it is " + cancelReason);
        }

        TUniqueId queryId = coordinatorContext.queryId;
        try {
            Status originQueryStatus = coordinatorContext.updateStatusIfOk(cancelReason);
            if (!originQueryStatus.ok()) {
                if (LOG.isDebugEnabled()) {
                    // Print an error stack here to know why send cancel again.
                    LOG.warn("Query {} already in abnormal status {}, but received cancel again,"
                                    + "so that send cancel to BE again",
                            DebugUtil.printId(queryId), originQueryStatus.toString(),
                            new Exception("cancel failed"));
                }
            } else {
                LOG.warn("Cancel execution of query {}, this is a outside invoke, cancelReason {}",
                        DebugUtil.printId(queryId), cancelReason);
            }
        } finally {
            // Publishing the status above can itself cancel a partially initialized processor. Start the
            // non-throwing cleanup scope before that publication so the queue token, the scan nodes, and
            // the final internal cancel are never skipped, even if the publication was only half wired.
            try {
                coordinatorContext.getQueueToken().ifPresent(QueueToken::cancel);
                // Scan cleanup is best-effort and must never escape: a throwing scan would otherwise skip
                // the remaining scans (and the caller's coordinator close), masking the retained reason. A
                // scan whose first stop() threw still removes its own sources on the close-time retry
                // because SplitAssignment.stop() is idempotent.
                for (ScanNode scanNode : coordinatorContext.scanNodes) {
                    try {
                        scanNode.stop();
                    } catch (Throwable t) {
                        LOG.error("error happens when scannode stop during cancel, query id: {}",
                                DebugUtil.printId(queryId), t);
                    }
                }
            } finally {
                cancelInternal(cancelReason);
            }
        }
    }

    public QueryProcessor asQueryProcessor() {
        return coordinatorContext.asQueryProcessor();
    }

    public JobProcessor getJobProcessor() {
        return coordinatorContext.getJobProcessor();
    }

    public LoadProcessor asLoadProcessor() {
        return coordinatorContext.asLoadProcessor();
    }

    public void setTWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        coordinatorContext.setWorkloadGroups(tWorkloadGroups);
    }

    public List<TPipelineWorkloadGroup> getTWorkloadGroups() {
        return coordinatorContext.getWorkloadGroups();
    }

    public boolean isQueryCancelled() {
        return coordinatorContext.readCloneStatus().isCancelled();
    }

    protected Status getQueryStatus() {
        return coordinatorContext.readCloneStatus();
    }

    @Override
    public RowBatch getNext() throws Exception {
        return coordinatorContext.asQueryProcessor().getNext();
    }

    public boolean mustOutliveDispatch() {
        return coordinatorContext.scanNodes.stream().anyMatch(ScanNode::coordinatorMustOutliveDispatch);
    }

    public List<Long> getScanBackendIds() {
        return coordinatorContext.scanNodes.stream()
                .flatMap(scan -> scan.getScanBackendIds().stream())
                .collect(Collectors.toList());
    }

    public Status shouldCancel(List<Backend> currentBackends) {
        PipelineExecutionTask task = executionTask;
        if (task == null) {
            return Status.OK;
        }
        Set<Long> aliveBackendIds = currentBackends.stream().filter(Backend::isAlive)
                .map(Backend::getId).collect(Collectors.toSet());
        for (Map.Entry<BackendWorker, MultiFragmentsPipelineTask> entry : task.getChildrenTasks().entrySet()) {
            // Remote Doris workers belong to another catalog and are absent from the local heartbeat inventory.
            if (entry.getKey().getCatalogId() != InternalCatalog.INTERNAL_CATALOG_ID) {
                continue;
            }
            MultiFragmentsPipelineTask backendTask = entry.getValue();
            if (!aliveBackendIds.contains(backendTask.getBackend().getId())) {
                return new Status(TStatusCode.CANCELLED, "Backend {} not exists or dead, query {} should be cancelled",
                        backendTask.getBackend(), DebugUtil.printId(getQueryId()));
            }
            for (SingleFragmentPipelineTask fragmentTask : backendTask.getChildrenTasks().values()) {
                Status status = fragmentTask.getBackendHealthStatus(getJobId());
                if (!status.ok()) {
                    return status;
                }
            }
        }
        return Status.OK;
    }

    public TPipelineFragmentParams getStreamLoadPlan() throws Exception {
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> plans = ThriftPlansBuilder.plansToThrift(
                coordinatorContext);
        Preconditions.checkState(plans.size() == 1, "HTTP stream load must use one backend");
        TPipelineFragmentParamsList fragments = plans.values().iterator().next();
        Preconditions.checkState(fragments.getParamsListSize() == 1, "HTTP stream load must use one fragment");
        return fragments.getParamsList().get(0);
    }

    public boolean isEos() {
        return coordinatorContext.asQueryProcessor().isEos();
    }

    public long getNumReceivedRows() {
        return coordinatorContext.asQueryProcessor().getNumReceivedRows();
    }

    public long getJobId() {
        JobProcessor jobProcessor = coordinatorContext.getJobProcessor();
        if (jobProcessor instanceof LoadProcessor) {
            return ((LoadProcessor) jobProcessor).jobId;
        }
        return -1L;
    }

    /*
     * Waiting the coordinator finish executing.
     * return false if waiting timeout.
     * return true otherwise.
     * NOTICE: return true does not mean that coordinator executed success,
     * the caller should check queryStatus for result.
     *
     * We divide the entire waiting process into multiple rounds,
     * with a maximum of 30 seconds per round. And after each round of waiting,
     * check the status of the BE. If the BE status is abnormal, the wait is ended
     * and the result is returned. Otherwise, continue to the next round of waiting.
     * This method mainly avoids the problem that the Coordinator waits for a long time
     * after some BE can no long return the result due to some exception, such as BE is down.
     */
    public boolean join(int timeoutS) {
        return coordinatorContext.asLoadProcessor().join(timeoutS);
    }

    public boolean isDone() {
        return coordinatorContext.asLoadProcessor().isDone();
    }

    public boolean updateFragmentExecStatus(TReportExecStatusParams params) {
        return coordinatorContext.getJobProcessor().updateFragmentExecStatus(params);
    }

    public TUniqueId getQueryId() {
        return coordinatorContext.queryId;
    }

    public TQueryOptions getQueryOptions() {
        return coordinatorContext.queryOptions;
    }

    public Status getExecStatus() {
        return coordinatorContext.readCloneStatus();
    }

    public void setQueryType(TQueryType type) {
        coordinatorContext.queryOptions.setQueryType(type);
    }

    public void setLoadZeroTolerance(boolean loadZeroTolerance) {
        coordinatorContext.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
    }

    public int getScanRangeNum() {
        return coordinatorContext.scanRangeNum.get();
    }

    public ConnectContext getConnectContext() {
        return coordinatorContext.connectContext;
    }

    public QueueToken getQueueToken() {
        return coordinatorContext.getQueueToken().orElse(null);
    }

    public Map<String, String> getLoadCounters() {
        return coordinatorContext.asLoadProcessor().loadContext.getLoadCounters();
    }

    public List<String> getDeltaUrls() {
        return coordinatorContext.asLoadProcessor().loadContext.getDeltaUrls();
    }

    public List<TTabletCommitInfo> getCommitInfos() {
        return coordinatorContext.asLoadProcessor().loadContext.getCommitInfos();
    }

    public List<String> getExportFiles() {
        return coordinatorContext.asLoadProcessor().loadContext.getExportFiles();
    }

    public long getTxnId() {
        return coordinatorContext.asLoadProcessor().loadContext.getTransactionId();
    }

    public void setTxnId(long txnId) {
        coordinatorContext.asLoadProcessor().loadContext.updateTransactionId(txnId);
    }

    public String getLabel() {
        return coordinatorContext.asLoadProcessor().loadContext.getLabel();
    }

    public String getTrackingUrl() {
        return coordinatorContext.asLoadProcessor().loadContext.getTrackingUrl();
    }

    public String getFirstErrorMsg() {
        return coordinatorContext.asLoadProcessor().loadContext.getFirstErrorMsg();
    }

    public List<TErrorTabletInfo> getErrorTabletInfos() {
        return coordinatorContext.asLoadProcessor().loadContext.getErrorTabletInfos();
    }

    @Override
    public List<TNetworkAddress> getInvolvedBackends() {
        return Utils.fastToImmutableList(coordinatorContext.backends.get().keySet());
    }

    public Set<Long> getDispatchedBackendIdsForAudit() {
        return executionTask == null
                ? ImmutableSet.of() : executionTask.getDispatchedBackendIdsForAudit();
    }

    public List<FragmentInstanceInfo> getFragmentInstanceInfos() {
        List<QueryStatisticsItem.FragmentInstanceInfo> infos = Lists.newArrayList();
        if (executionTask != null) {
            for (MultiFragmentsPipelineTask multiFragmentsPipelineTask : executionTask.getChildrenTasks().values()) {
                for (SingleFragmentPipelineTask fragmentTask : multiFragmentsPipelineTask.getChildrenTasks().values()) {
                    infos.addAll(fragmentTask.buildFragmentInstanceInfo());
                }
            }
        }
        infos.sort(Comparator.comparing(FragmentInstanceInfo::getFragmentId));
        return infos;
    }

    public List<PlanFragment> getFragments() {
        return coordinatorContext.fragments;
    }

    public CoordinatorContext getCoordinatorContext() {
        return coordinatorContext;
    }

    public ExecutionProfile getExecutionProfile() {
        return coordinatorContext.executionProfile;
    }

    public void setMemTableOnSinkNode(boolean enableMemTableOnSinkNode) {
        coordinatorContext.queryOptions.setEnableMemtableOnSinkNode(enableMemTableOnSinkNode);
    }

    public void setBatchSize(int batchSize) {
        coordinatorContext.queryOptions.setBatchSize(batchSize);
    }

    public void setTimeout(int timeout) {
        coordinatorContext.queryOptions.setQueryTimeout(timeout);
        coordinatorContext.queryOptions.setExecutionTimeout(timeout);
        if (coordinatorContext.queryOptions.getExecutionTimeout() < 1) {
            LOG.warn("try set timeout less than 1: {}", coordinatorContext.queryOptions.getExecutionTimeout());
        }
    }

    public void setLoadMemLimit(long loadMemLimit) {
        coordinatorContext.queryOptions.setLoadMemLimit(loadMemLimit);
    }

    public void setExecMemoryLimit(long execMemoryLimit) {
        coordinatorContext.queryOptions.setMemLimit(execMemoryLimit);
    }

    // this method is used to provide profile metrics: `Instances Num Per BE`
    public Map<String, Integer> getBeToInstancesNum() {
        Map<String, Integer> result = Maps.newLinkedHashMap();
        if (executionTask != null) {
            for (MultiFragmentsPipelineTask beTasks : executionTask.getChildrenTasks().values()) {
                TNetworkAddress brpcAddress = beTasks.getBackend().getBrpcAddress();
                String brpcAddrString = brpcAddress.hostname.concat(":").concat("" + brpcAddress.port);
                result.put(brpcAddrString, beTasks.getInstanceNum());
            }
        }
        return result;
    }

    public void stopSplitGeneration() {
        for (ScanNode scanNode : coordinatorContext.scanNodes) {
            scanNode.stopSplitGeneration();
        }
    }

    @Override
    public void close() {
        // NOTE: all close method should be no exception
        if (coordinatorContext.getQueryQueue().isPresent() && coordinatorContext.getQueueToken().isPresent()) {
            try {
                coordinatorContext.getQueryQueue().get().releaseAndNotify(coordinatorContext.getQueueToken().get());
            } catch (Throwable t) {
                LOG.error("error happens when coordinator close ", t);
            }
        }

        for (ScanNode scanNode : coordinatorContext.scanNodes) {
            try {
                scanNode.stop();
            } catch (Throwable t) {
                LOG.error("error happens when scannode stop ", t);
            }
        }
    }

    /**
     * A queue wait can be unblocked by {@code cancel()}, which records the real TIMEOUT/KILL reason on the
     * coordinator before cancelling the token, while {@link QueueToken#get} can only report a generic
     * "query is cancelled". Prefer the retained terminal reason when one is present.
     */
    protected UserException preferTerminalReason(UserException queueFailure) {
        Status current = getQueryStatus();
        if (!current.ok()) {
            return new UserException(current.getErrorMsg());
        }
        return queueFailure;
    }

    protected void cancelInternal(Status cancelReason) {
        coordinatorContext.withLock(() -> coordinatorContext.getJobProcessor().cancel(cancelReason));
    }

    protected void processTopSink(
            CoordinatorContext coordinatorContext, PipelineDistributedPlan topPlan) throws AnalysisException {
        recordLoadSinkCoordinator(coordinatorContext, topPlan);
        setForArrowFlight(coordinatorContext, topPlan);
        setForBroker(coordinatorContext, topPlan);
    }

    private void recordLoadSinkCoordinator(
            CoordinatorContext coordinatorContext, PipelineDistributedPlan topPlan) {
        ConnectContext connectContext = coordinatorContext.connectContext;
        if (!(coordinatorContext.dataSink instanceof OlapTableSink)
                || coordinatorContext.queryOptions.getQueryType() != TQueryType.LOAD
                || !BackendSelectionManager.isLoadSelectionEnabled(connectContext)
                || topPlan.getInstanceJobs().isEmpty()) {
            return;
        }
        BackendSelection.SelectionHint hint = BackendSelectionManager.resolveLoadSelectionHint(connectContext);
        if (!BackendSelectionManager.hasLoadSelectionPreference(hint)) {
            return;
        }
        DistributedPlanWorker worker = topPlan.getInstanceJobs().get(0).getAssignedWorker();
        if (worker instanceof BackendWorker) {
            connectContext.getBackendSelectionProfile().recordLoadCoordinator(
                    hint, ((BackendWorker) worker).getBackend());
        }
    }

    private void setForArrowFlight(CoordinatorContext coordinatorContext, PipelineDistributedPlan topPlan) {
        ConnectContext connectContext = coordinatorContext.connectContext;
        DataSink dataSink = coordinatorContext.dataSink;
        if (dataSink instanceof ResultSink || dataSink instanceof ResultFileSink) {
            // The client pulls the result from the backend (Arrow Flight SQL); register where.
            if (connectContext != null && !connectContext.isReturnResultFromLocal()) {
                Set<Long> resultBackendIds = new HashSet<>();
                for (AssignedJob instance : topPlan.getInstanceJobs()) {
                    BackendWorker worker = (BackendWorker) instance.getAssignedWorker();
                    Backend backend = worker.getBackend();
                    if (backend.getArrowFlightSqlPort() < 0) {
                        throw new IllegalStateException("be arrow_flight_sql_port cannot be empty.");
                    }
                    TUniqueId finstId;
                    if (connectContext.getSessionVariable().enableParallelResultSink()) {
                        // Parallel instances on a BE share one query-id buffer, so their tickets
                        // cannot be published as independently consumable result partitions.
                        if (!resultBackendIds.add(worker.id())) {
                            continue;
                        }
                        finstId = getQueryId();
                    } else {
                        finstId = instance.instanceId();
                    }
                    connectContext.addFlightSqlEndpointsLocation(new FlightSqlEndpointsLocation(finstId,
                            backend.getArrowFlightAddress(), backend.getBrpcAddress(),
                            topPlan.getFragmentJob().getFragment().getOutputExprs()));
                }
            }
        }
    }

    private void setForInsert(long jobId) {
        JobProcessor jobProc = new LoadProcessor(this.coordinatorContext, jobId);
        this.coordinatorContext.setJobProcessor(jobProc);
        // Set this field to true to avoid data entering the normal cache LRU queue
        this.coordinatorContext.queryOptions.setDisableFileCache(true);
        this.coordinatorContext.queryOptions.setNewVersionUnixTimestamp(true);
        this.coordinatorContext.queryOptions.setNewVersionPercentile(true);
        this.coordinatorContext.queryOptions.setNewVersionBitmapOpCount(true);
    }

    private void setForQuery() {
        JobProcessor jobProc = QueryProcessor.build(this.coordinatorContext);
        this.coordinatorContext.setJobProcessor(jobProc);
    }

    private void setForBroker(
            CoordinatorContext coordinatorContext, PipelineDistributedPlan topPlan) throws AnalysisException {
        DataSink dataSink = coordinatorContext.dataSink;
        if (dataSink instanceof ResultFileSink
                && ((ResultFileSink) dataSink).getStorageType() == StorageBackend.StorageType.BROKER) {
            // set the broker address for OUTFILE sink
            ResultFileSink topResultFileSink = (ResultFileSink) dataSink;
            DistributedPlanWorker worker = topPlan.getInstanceJobs().get(0).getAssignedWorker();
            FsBroker broker = Env.getCurrentEnv().getBrokerMgr()
                    .getBroker(topResultFileSink.getBrokerName(), worker.host());
            topResultFileSink.setBrokerAddr(broker.host, broker.port);
        }
    }

    private void enqueue(ConnectContext context) throws UserException {
        // LoadTask does not have context, not controlled by queue now
        if (context != null && needEnqueue) {
            if (Config.enable_workload_group) {
                List<WorkloadGroup> wgs = context.getEnv().getWorkloadGroupMgr().getWorkloadGroup(context);
                List<TPipelineWorkloadGroup> wgList = wgs.stream()
                        .map(e -> e.toThrift())
                        .collect(Collectors.toList());
                this.setTWorkloadGroups(wgList);
                if (shouldQueue(context)) {
                    if (wgs.size() < 1) {
                        // This logic is actually useless, because when could not find query queue, it will
                        // throw exception during workload group manager.
                        throw new UserException("could not find query queue");
                    }
                    QueryQueue queryQueue = wgs.get(0).getQueryQueue();
                    QueueToken queueToken = queryQueue.getToken(context.getSessionVariable().wgQuerySlotCount);
                    int queryTimeout = coordinatorContext.queryOptions.getExecutionTimeout() * 1000;
                    coordinatorContext.setQueueInfo(queryQueue, queueToken);
                    try {
                        queueToken.get(DebugUtil.printId(coordinatorContext.queryId), queryTimeout);
                    } catch (UserException e) {
                        throw preferTerminalReason(e);
                    }
                }
                context.setWorkloadGroupName(wgs.get(0).getName());
            } else {
                context.setWorkloadGroupName("");
            }
        }
    }

    private boolean shouldQueue(ConnectContext context) {
        boolean ret = Config.enable_query_queue && !context.getSessionVariable()
                .getBypassWorkloadGroup() && !isQueryCancelled();
        if (!ret) {
            return false;
        }
        // a query with ScanNode need not queue only when all its scan node is SchemaScanNode
        for (ScanNode scanNode : coordinatorContext.scanNodes) {
            if (!(scanNode instanceof SchemaScanNode)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setIsProfileSafeStmt(boolean isSafe) {
        coordinatorContext.queryOptions.setEnableProfile(isSafe && coordinatorContext.queryOptions.isEnableProfile());
    }
}
