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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TabletCommitInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    private final Database db;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final long jobDeadlineMs;
    private final long execMemLimit;
    private final boolean strictMode;
    private final long txnId;

    private LoadingTaskPlanner planner;

    private String errMsg;

    public LoadLoadingTask(Database db, OlapTable table,
                           BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
                           long jobDeadlineMs, long execMemLimit, boolean strictMode,
                           long txnId, LoadTaskCallback callback) {
        super(callback);
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.strictMode = strictMode;
        this.txnId = txnId;
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.retryTime = 3;
    }

    public void init(List<List<TBrokerFileStatus>> fileStatusList, int fileNum) throws UserException {
        planner = new LoadingTaskPlanner(txnId, db.getId(), table, brokerDesc, fileGroups, strictMode);
        planner.plan(fileStatusList, fileNum);
    }

    @Override
    protected void executeTask() throws Exception{
        retryTime--;
        executeOnce();
    }

    private void executeOnce() throws Exception {
        // New one query id,
        UUID uuid = UUID.randomUUID();
        TUniqueId executeId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        Coordinator curCoordinator = new Coordinator(callback.getCallbackId(), executeId, planner.getDescTable(),
                                                     planner.getFragments(), planner.getScanNodes(), db.getClusterName());
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.setExecMemoryLimit(execMemLimit);
        curCoordinator.setTimeout((int) (getLeftTimeMs() / 1000));

        try {
            QeProcessorImpl.INSTANCE
                    .registerQuery(executeId, curCoordinator);
            actualExecute(curCoordinator);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(executeId);
        }
    }

    private void actualExecute(Coordinator curCoordinator) throws Exception {
        int waitSecond = (int) (getLeftTimeMs() / 1000);
        if (waitSecond <= 0) {
            throw new LoadException("failed to execute plan when the left time is less then 0");
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
                                                             TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()));
            } else {
                throw new LoadException(status.getErrorMsg());
            }
        } else {
            throw new LoadException("coordinator could not finished before job timeout");
        }
    }

    private long getLeftTimeMs() {
        return jobDeadlineMs - System.currentTimeMillis();
    }
}
