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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    /*
     * load id is used for plan.
     * It should be changed each time we retry this load plan
     */
    private TUniqueId loadId;
    private final Database db;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final long jobDeadlineMs;
    private final long execMemLimit;
    private final boolean strictMode;
    private final long txnId;
    private final String timezone;
    // timeout of load job, in seconds
    private final long timeoutS;
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean loadZeroTolerance;
    private final boolean singleTabletLoadPerSink;
    private final boolean useNewLoadScanNode;

    private LoadingTaskPlanner planner;

    private RuntimeProfile jobProfile;
    private long beginTime;

    public LoadLoadingTask(Database db, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, int loadParallelism, int sendBatchParallelism,
            boolean loadZeroTolerance, RuntimeProfile profile, boolean singleTabletLoadPerSink,
            boolean useNewLoadScanNode) {
        super(callback, TaskType.LOADING);
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.strictMode = strictMode;
        this.txnId = txnId;
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.retryTime = 2; // 2 times is enough
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.loadZeroTolerance = loadZeroTolerance;
        this.jobProfile = profile;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.useNewLoadScanNode = useNewLoadScanNode;
    }

    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList,
            int fileNum, UserIdentity userInfo) throws UserException {
        this.loadId = loadId;
        planner = new LoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc, fileGroups,
                strictMode, timezone, this.timeoutS, this.loadParallelism, this.sendBatchParallelism,
                this.useNewLoadScanNode, userInfo);
        planner.plan(loadId, fileStatusList, fileNum);
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    @Override
    protected void executeTask() throws Exception {
        LOG.info("begin to execute loading task. load id: {} job id: {}. db: {}, tbl: {}. left retry: {}",
                DebugUtil.printId(loadId), callback.getCallbackId(), db.getFullName(), table.getName(), retryTime);
        retryTime--;
        beginTime = System.nanoTime();
        if (!((BrokerLoadJob) callback).updateState(JobState.LOADING)) {
            // job may already be cancelled
            return;
        }
        executeOnce();
    }

    private void executeOnce() throws Exception {
        // New one query id,
        Coordinator curCoordinator = new Coordinator(callback.getCallbackId(), loadId, planner.getDescTable(),
                planner.getFragments(), planner.getScanNodes(), planner.getTimezone(), loadZeroTolerance);
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.setExecMemoryLimit(execMemLimit);
        curCoordinator.setExecVecEngine(Config.enable_vectorized_load);
        /*
         * For broker load job, user only need to set mem limit by 'exec_mem_limit' property.
         * And the variable 'load_mem_limit' does not make any effect.
         * However, in order to ensure the consistency of semantics when executing on the BE side,
         * and to prevent subsequent modification from incorrectly setting the load_mem_limit,
         * here we use exec_mem_limit to directly override the load_mem_limit property.
         */
        curCoordinator.setLoadMemLimit(execMemLimit);
        curCoordinator.setTimeout((int) (getLeftTimeMs() / 1000));

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, curCoordinator);
            actualExecute(curCoordinator);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }

    private void actualExecute(Coordinator curCoordinator) throws Exception {
        int waitSecond = (int) (getLeftTimeMs() / 1000);
        if (waitSecond <= 0) {
            throw new LoadException("failed to execute plan when the left time is less than 0");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("task_id", signature)
                    .add("query_id", DebugUtil.printId(curCoordinator.getQueryId()))
                    .add("msg", "begin to execute plan")
                    .build());
        }
        curCoordinator.exec();
        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok()) {
                attachment = new BrokerLoadingTaskAttachment(signature,
                        curCoordinator.getLoadCounters(),
                        curCoordinator.getTrackingUrl(),
                        TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()),
                        ErrorTabletInfo.fromThrift(curCoordinator.getErrorTabletInfos()));
                // Create profile of this task and add to the job profile.
                createProfile(curCoordinator);
            } else {
                throw new LoadException(status.getErrorMsg());
            }
        } else {
            throw new LoadException("coordinator could not finished before job timeout");
        }
    }

    private long getLeftTimeMs() {
        return Math.max(jobDeadlineMs - System.currentTimeMillis(), 1000L);
    }

    private void createProfile(Coordinator coord) {
        if (jobProfile == null) {
            // No need to gather profile
            return;
        }
        // Summary profile
        coord.getQueryProfile().getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(beginTime));
        coord.endProfile();
        jobProfile.addChild(coord.getQueryProfile());
    }

    @Override
    public void updateRetryInfo() {
        super.updateRetryInfo();
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        planner.updateLoadId(this.loadId);
    }
}
