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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.loadv2.BrokerPendingTaskAttachment;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadLoadingTask;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class CloudBrokerLoadJob extends BrokerLoadJob {
    private static final Logger LOG = LogManager.getLogger(CloudBrokerLoadJob.class);

    protected static final String CLOUD_CLUSTER_ID = "clusterId";
    protected String cloudClusterId;

    protected int retryTimes = 3;

    public CloudBrokerLoadJob() {
    }

    public CloudBrokerLoadJob(EtlJobType type) {
        super(type);
    }

    public CloudBrokerLoadJob(EtlJobType type, long dbId, String label, BrokerDesc brokerDesc,
            OriginStatement originStmt, UserIdentity userInfo)
            throws MetaNotFoundException {
        super(type, dbId, label, brokerDesc, originStmt, userInfo);
        setCloudClusterId();
    }

    public CloudBrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, OriginStatement originStmt,
            UserIdentity userInfo) throws MetaNotFoundException {
        super(dbId, label, brokerDesc, originStmt, userInfo);
        setCloudClusterId();
    }

    private void setCloudClusterId() throws MetaNotFoundException {
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            String clusterName = context.getCloudCluster();
            if (Strings.isNullOrEmpty(clusterName)) {
                LOG.warn("cluster name is empty");
                throw new MetaNotFoundException("cluster name is empty");
            }

            this.cloudClusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                    .getCloudClusterIdByName(clusterName);
            if (!Strings.isNullOrEmpty(context.getSessionVariable().getCloudCluster())) {
                clusterName = context.getSessionVariable().getCloudCluster();
                this.cloudClusterId =
                    ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdByName(clusterName);
            }
            if (Strings.isNullOrEmpty(this.cloudClusterId)) {
                LOG.warn("cluster id is empty, cluster name {}", clusterName);
                throw new MetaNotFoundException("cluster id is empty, cluster name: " + clusterName);
            }
            sessionVariables.put(CLOUD_CLUSTER_ID, this.cloudClusterId);
        }
    }

    private AutoCloseConnectContext buildConnectContext() throws UserException {
        cloudClusterId = sessionVariables.get(CLOUD_CLUSTER_ID);
        String clusterName =  ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getClusterNameByClusterId(cloudClusterId);
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.warn("cluster name is empty, cluster id is {}", cloudClusterId);
            throw new UserException("cluster name is empty, cluster id is: " + cloudClusterId);
        }

        if (ConnectContext.get() == null) {
            ConnectContext connectContext = new ConnectContext();
            connectContext.setCloudCluster(clusterName);
            return new AutoCloseConnectContext(connectContext);
        } else {
            ConnectContext.get().setCloudCluster(clusterName);
            return null;
        }
    }

    // override BulkLoadJob.analyze
    @Override
    public void analyze() {
        cloudClusterId = sessionVariables.get(CLOUD_CLUSTER_ID);
        super.analyze();
    }

    @Override
    protected LoadLoadingTask createTask(Database db, OlapTable table, List<BrokerFileGroup> brokerFileGroups,
            boolean isEnableMemtableOnSinkNode, int batchSize, FileGroupAggKey aggKey,
            BrokerPendingTaskAttachment attachment) throws UserException {
        cloudClusterId = sessionVariables.get(CLOUD_CLUSTER_ID);
        LoadLoadingTask task = new CloudLoadLoadingTask(db, table, brokerDesc,
                brokerFileGroups, getDeadlineMs(), getExecMemLimit(),
                isStrictMode(), isPartialUpdate(), transactionId, this, getTimeZone(), getTimeout(),
                getLoadParallelism(), getSendBatchParallelism(),
                getMaxFilterRatio() <= 0, enableProfile ? jobProfile : null, isSingleTabletLoadPerSink(),
                useNewLoadScanNode(), getPriority(), isEnableMemtableOnSinkNode, batchSize, cloudClusterId);
        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

        try (AutoCloseConnectContext r = buildConnectContext()) {
            task.init(loadId, attachment.getFileStatusByTable(aggKey),
                    attachment.getFileNumByTable(aggKey), getUserInfo());
        } catch (UserException e) {
            throw e;
        }
        return task;
    }

    @Override
    protected void executeFinish() {
        super.executeFinish();
        // When replaying a load job, the state of the job can be obtained through replaying transaction
        // status information in local mode. However, in cloud mode, there is no edit log of transaction
        // in fe. So pint an edit log to save the status information of the job here.
        logFinalOperation();
    }

    @Override
    protected void afterLoadingTaskCommitTransaction(List<Table> tableList) {
        ConnectContext ctx = null;
        if (ConnectContext.get() == null) {
            ctx = new ConnectContext();
            ctx.setThreadLocalInfo();
        } else {
            ctx = ConnectContext.get();
        }

        if (ctx.getSessionVariable().enableMultiClusterSyncLoad()) {
            // get the backends of each cluster expect the load cluster
            CloudSystemInfoService infoService = (CloudSystemInfoService) Env.getCurrentSystemInfo();
            List<List<Backend>> backendsList = infoService.getCloudClusterIds()
                                                                .stream()
                                                                .filter(id -> !id.equals(cloudClusterId))
                                                                .map(id -> infoService.getBackendsByClusterId(id))
                                                                .collect(Collectors.toList());
            // for each all load table, get its tablets
            tableList.forEach(table -> {
                List<Long> allTabletIds = ((OlapTable) table).getAllTabletIds();
                StmtExecutor.syncLoadForTablets(backendsList, allTabletIds);
            });
        }
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
        if (Strings.isNullOrEmpty(this.cloudClusterId)) {
            super.onTaskFailed(taskId, failMsg);
            return;
        }

        try {
            writeLock();
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("label", label)
                        .add("transactionId", transactionId)
                        .add("state", state)
                        .add("error_msg", "this task will be ignored when job is: " + state)
                        .build());
                return;
            }
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("label", label)
                    .add("transactionId", transactionId)
                    .add("state", state)
                    .add("retryTimes", retryTimes)
                    .add("failMsg", failMsg.getMsg())
                    .build());

            this.retryTimes--;
            if (this.retryTimes <= 0) {
                boolean abortTxn = this.transactionId > 0 ? true : false;
                unprotectedExecuteCancel(failMsg, abortTxn);
                logFinalOperation();
                return;
            } else {
                unprotectedExecuteRetry(failMsg);
            }
        } finally {
            writeUnlock();
        }

        boolean allTaskDone = false;
        while (!allTaskDone) {
            try {
                writeLock();
                // check if all task has been done
                // unprotectedExecuteRetry() will cancel all running task
                allTaskDone = true;
                for (Map.Entry<Long, LoadTask> entry : idToTasks.entrySet()) {
                    if (entry.getKey() != taskId && !entry.getValue().isDone()) {
                        LOG.info("LoadTask({}) has not been done", entry.getKey());
                        allTaskDone = false;
                    }
                }
            } finally {
                writeUnlock();
            }
            if (!allTaskDone) {
                try {
                    Thread.sleep(1000);
                    continue;
                } catch (InterruptedException e) {
                    LOG.warn("", e);
                }
            }
        }

        try {
            writeLock();
            this.state = JobState.PENDING;
            this.idToTasks.clear();
            this.failMsg = null;
            this.finishedTaskIds.clear();
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
            LoadTask task = createPendingTask();
            // retry default backoff 60 seconds, because `be restart` is slow
            task.setStartTimeMs(System.currentTimeMillis() + 60 * 1000);
            idToTasks.put(task.getSignature(), task);
            Env.getCurrentEnv().getPendingLoadTaskScheduler().submit(task);
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectedExecuteRetry(FailMsg failMsg) {
        LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id).add("transaction_id", transactionId)
                .add("error_msg", "Failed to execute load with error: " + failMsg.getMsg()).build());

        // get load ids of all loading tasks, we will cancel their coordinator process later
        List<TUniqueId> loadIds = Lists.newArrayList();
        for (LoadTask loadTask : idToTasks.values()) {
            if (loadTask instanceof LoadLoadingTask) {
                loadIds.add(((LoadLoadingTask) loadTask).getLoadId());
            }
        }

        // set failMsg and state
        this.failMsg = failMsg;
        if (failMsg.getCancelType() == CancelType.TXN_UNKNOWN) {
            // for bug fix, see LoadManager's fixLoadJobMetaBugs() method
            finishTimestamp = createTimestamp;
        } else {
            finishTimestamp = System.currentTimeMillis();
        }

        // remove callback before abortTransaction(), so that the afterAborted() callback will not be called again
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        // abort txn by label, because transactionId here maybe -1
        try {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("label", label)
                    .add("msg", "begin to abort txn")
                    .build());
            Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, label, failMsg.getMsg());
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("label", label)
                    .add("error_msg", "failed to abort txn when job is cancelled. " + e.getMessage())
                    .build());
        }

        // cancel all running coordinators, so that the scheduler's worker thread will be released
        for (TUniqueId loadId : loadIds) {
            Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(loadId);
            if (coordinator != null) {
                coordinator.cancel();
            }
        }

        // change state
        state = JobState.RETRY;
    }

}
