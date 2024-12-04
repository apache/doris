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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SchemaScanNode;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QueryStatisticsItem.FragmentInstanceInfo;
import org.apache.doris.qe.runtime.LoadProcessor;
import org.apache.doris.qe.runtime.MultiFragmentsPipelineTask;
import org.apache.doris.qe.runtime.PipelineExecutionTask;
import org.apache.doris.qe.runtime.PipelineExecutionTaskBuilder;
import org.apache.doris.qe.runtime.QueryProcessor;
import org.apache.doris.qe.runtime.SingleFragmentPipelineTask;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.service.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** NereidsCoordinator */
public class NereidsCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(NereidsCoordinator.class);

    protected final CoordinatorContext coordinatorContext;

    protected volatile PipelineExecutionTask executionTask;

    // sql execution
    public NereidsCoordinator(ConnectContext context, Analyzer analyzer,
            NereidsPlanner planner, StatsErrorEstimator statsErrorEstimator) {
        super(context, analyzer, planner, statsErrorEstimator);

        this.coordinatorContext = CoordinatorContext.buildForSql(planner, this);
        this.coordinatorContext.setJobProcessor(buildJobProcessor(coordinatorContext));

        Preconditions.checkState(!planner.getFragments().isEmpty()
                && coordinatorContext.instanceNum.get() > 0, "Fragment and Instance can not be empty˚");
    }

    // broker load
    public NereidsCoordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
            List<PlanFragment> fragments, List<PipelineDistributedPlan> distributedPlans,
            List<ScanNode> scanNodes, String timezone, boolean loadZeroTolerance,
            boolean enableProfile) {
        super(jobId, queryId, descTable, fragments, scanNodes, timezone, loadZeroTolerance, enableProfile);
        this.coordinatorContext = CoordinatorContext.buildForLoad(
                this, jobId, queryId, fragments, distributedPlans, scanNodes,
                descTable, timezone, loadZeroTolerance, enableProfile
        );

        Preconditions.checkState(!fragments.isEmpty()
                && coordinatorContext.instanceNum.get() > 0, "Fragment and Instance can not be empty˚");
    }

    @Override
    public void exec() throws Exception {
        enqueue(coordinatorContext.connectContext);

        processTopSink(coordinatorContext, coordinatorContext.topDistributedPlan);

        QeProcessorImpl.INSTANCE.registerInstances(coordinatorContext.queryId, coordinatorContext.instanceNum.get());

        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragments
                = ThriftPlansBuilder.plansToThrift(coordinatorContext);
        executionTask = PipelineExecutionTaskBuilder.build(coordinatorContext, workerToFragments);
        executionTask.execute();
    }

    @Override
    public boolean isTimeout() {
        return System.currentTimeMillis() > coordinatorContext.timeoutDeadline.get();
    }

    @Override
    public void cancel(Status cancelReason) {
        coordinatorContext.getQueueToken().ifPresent(QueueToken::cancel);

        for (ScanNode scanNode : coordinatorContext.scanNodes) {
            scanNode.stop();
        }

        if (cancelReason.ok()) {
            throw new RuntimeException("Should use correct cancel reason, but it is " + cancelReason);
        }

        TUniqueId queryId = coordinatorContext.queryId;
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
        cancelInternal(cancelReason);
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

    @Override
    public void setTWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        coordinatorContext.setWorkloadGroups(tWorkloadGroups);
    }

    @Override
    public List<TPipelineWorkloadGroup> getTWorkloadGroups() {
        return coordinatorContext.getWorkloadGroups();
    }

    @Override
    public boolean isQueryCancelled() {
        return coordinatorContext.readCloneStatus().isCancelled();
    }

    @Override
    public RowBatch getNext() throws Exception {
        return coordinatorContext.asQueryProcessor().getNext();
    }

    public boolean isEos() {
        return coordinatorContext.asQueryProcessor().isEos();
    }

    @Override
    public long getNumReceivedRows() {
        return coordinatorContext.asQueryProcessor().getNumReceivedRows();
    }

    @Override
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
    @Override
    public boolean join(int timeoutS) {
        return coordinatorContext.asLoadProcessor().join(timeoutS);
    }

    @Override
    public boolean isDone() {
        return coordinatorContext.asLoadProcessor().isDone();
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        coordinatorContext.getJobProcessor().updateFragmentExecStatus(params);
    }

    @Override
    public TUniqueId getQueryId() {
        return coordinatorContext.queryId;
    }

    @Override
    public TQueryOptions getQueryOptions() {
        return coordinatorContext.queryOptions;
    }

    @Override
    public Status getExecStatus() {
        return coordinatorContext.readCloneStatus();
    }

    @Override
    public void setQueryType(TQueryType type) {
        coordinatorContext.queryOptions.setQueryType(type);
    }

    @Override
    public void setLoadZeroTolerance(boolean loadZeroTolerance) {
        coordinatorContext.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
    }

    @Override
    public int getScanRangeNum() {
        return coordinatorContext.scanRangeNum.get();
    }

    @Override
    public ConnectContext getConnectContext() {
        return coordinatorContext.connectContext;
    }

    @Override
    public QueueToken getQueueToken() {
        return coordinatorContext.getQueueToken().orElse(null);
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return coordinatorContext.asLoadProcessor().loadContext.getLoadCounters();
    }

    @Override
    public List<String> getDeltaUrls() {
        return coordinatorContext.asLoadProcessor().loadContext.getDeltaUrls();
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return coordinatorContext.asLoadProcessor().loadContext.getCommitInfos();
    }

    @Override
    public List<String> getExportFiles() {
        return coordinatorContext.asLoadProcessor().loadContext.getExportFiles();
    }

    @Override
    public long getTxnId() {
        return coordinatorContext.asLoadProcessor().loadContext.getTransactionId();
    }

    @Override
    public void setTxnId(long txnId) {
        coordinatorContext.asLoadProcessor().loadContext.updateTransactionId(txnId);
    }

    @Override
    public String getLabel() {
        return coordinatorContext.asLoadProcessor().loadContext.getLabel();
    }

    @Override
    public String getTrackingUrl() {
        return coordinatorContext.asLoadProcessor().loadContext.getTrackingUrl();
    }

    @Override
    public List<TErrorTabletInfo> getErrorTabletInfos() {
        return coordinatorContext.asLoadProcessor().loadContext.getErrorTabletInfos();
    }

    @Override
    public List<TNetworkAddress> getInvolvedBackends() {
        return Utils.fastToImmutableList(coordinatorContext.backends.get().keySet());
    }

    @Override
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

    @Override
    public List<PlanFragment> getFragments() {
        return coordinatorContext.fragments;
    }

    @Override
    public ExecutionProfile getExecutionProfile() {
        return coordinatorContext.executionProfile;
    }

    @Override
    public void setMemTableOnSinkNode(boolean enableMemTableOnSinkNode) {
        coordinatorContext.queryOptions.setEnableMemtableOnSinkNode(enableMemTableOnSinkNode);
    }

    @Override
    public void setBatchSize(int batchSize) {
        coordinatorContext.queryOptions.setBatchSize(batchSize);
    }

    @Override
    public void setTimeout(int timeout) {
        coordinatorContext.queryOptions.setQueryTimeout(timeout);
        coordinatorContext.queryOptions.setExecutionTimeout(timeout);
        if (coordinatorContext.queryOptions.getExecutionTimeout() < 1) {
            LOG.warn("try set timeout less than 1: {}", coordinatorContext.queryOptions.getExecutionTimeout());
        }
    }

    @Override
    public void setLoadMemLimit(long loadMemLimit) {
        coordinatorContext.queryOptions.setLoadMemLimit(loadMemLimit);
    }

    @Override
    public void setExecMemoryLimit(long execMemoryLimit) {
        coordinatorContext.queryOptions.setMemLimit(execMemoryLimit);
    }

    // this method is used to provide profile metrics: `Instances Num Per BE`
    @Override
    public Map<String, Integer> getBeToInstancesNum() {
        Map<String, Integer> result = Maps.newLinkedHashMap();
        if (executionTask != null) {
            for (MultiFragmentsPipelineTask beTasks : executionTask.getChildrenTasks().values()) {
                TNetworkAddress brpcAddress = beTasks.getBackend().getBrpcAddress();
                String brpcAddrString = brpcAddress.hostname.concat(":").concat("" + brpcAddress.port);
                result.put(brpcAddrString, beTasks.getChildrenTasks().size());
            }
        }
        return result;
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

        try {
            for (ScanNode scanNode : coordinatorContext.scanNodes) {
                scanNode.stop();
            }
        } catch (Throwable t) {
            LOG.error("error happens when scannode stop ", t);
        }
    }

    protected void cancelInternal(Status cancelReason) {
        coordinatorContext.withLock(() -> coordinatorContext.getJobProcessor().cancel(cancelReason));
    }

    protected void processTopSink(
            CoordinatorContext coordinatorContext, PipelineDistributedPlan topPlan) throws AnalysisException {
        setForArrowFlight(coordinatorContext, topPlan);
        setForBroker(coordinatorContext, topPlan);
    }

    private void setForArrowFlight(CoordinatorContext coordinatorContext, PipelineDistributedPlan topPlan) {
        ConnectContext connectContext = coordinatorContext.connectContext;
        DataSink dataSink = coordinatorContext.dataSink;
        if (dataSink instanceof ResultSink || dataSink instanceof ResultFileSink) {
            if (connectContext != null && !connectContext.isReturnResultFromLocal()) {
                Preconditions.checkState(connectContext.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL));
                for (AssignedJob instance : topPlan.getInstanceJobs()) {
                    BackendWorker worker = (BackendWorker) instance.getAssignedWorker();
                    Backend backend = worker.getBackend();
                    if (backend.getArrowFlightSqlPort() < 0) {
                        throw new IllegalStateException("be arrow_flight_sql_port cannot be empty.");
                    }
                    TUniqueId finstId;
                    if (connectContext.getSessionVariable().enableParallelResultSink()) {
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
        if (context != null) {
            if (Config.enable_workload_group) {
                coordinatorContext.setWorkloadGroups(context.getEnv().getWorkloadGroupMgr().getWorkloadGroup(context));
                if (shouldQueue(context)) {
                    QueryQueue queryQueue = context.getEnv().getWorkloadGroupMgr().getWorkloadGroupQueryQueue(context);
                    if (queryQueue == null) {
                        // This logic is actually useless, because when could not find query queue, it will
                        // throw exception during workload group manager.
                        throw new UserException("could not find query queue");
                    }
                    QueueToken queueToken = queryQueue.getToken();
                    int queryTimeout = coordinatorContext.queryOptions.getExecutionTimeout() * 1000;
                    queueToken.get(DebugUtil.printId(coordinatorContext.queryId), queryTimeout);
                    coordinatorContext.setQueueInfo(queryQueue, queueToken);
                }
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

    private JobProcessor buildJobProcessor(CoordinatorContext coordinatorContext) {
        DataSink dataSink = coordinatorContext.dataSink;
        if ((dataSink instanceof ResultSink || dataSink instanceof ResultFileSink)) {
            return QueryProcessor.build(coordinatorContext);
        } else {
            // insert statement has jobId == -1
            return new LoadProcessor(coordinatorContext, -1L);
        }
    }
}
