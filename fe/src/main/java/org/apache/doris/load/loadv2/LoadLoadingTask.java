/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.task.MasterTask;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TabletCommitInfo;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    private final Database db;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final long jobDeadlineMs;
    private final long execMemLimit;
    private final long txnId;

    private LoadingTaskPlanner planner;

    private String errMsg;

    public LoadLoadingTask(Database db, OlapTable table,
                           BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
                           long jobDeadlineMs, long execMemLimit, long txnId, LoadTaskCallback callback) {
        super(callback);
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.txnId = txnId;
    }

    public void init(List<List<TBrokerFileStatus>> fileStatusList, int fileNum) throws UserException {
        planner = new LoadingTaskPlanner(txnId, db.getId(), table, brokerDesc, fileGroups);
        planner.plan(fileStatusList, fileNum);
    }

    @Override
    protected void executeTask() throws UserException {
        int retryTime = 3;
        for (int i = 0; i < retryTime; ++i) {
            isFinished = executeOnce();
            if (isFinished) {
                return;
            }
        }
        throw new UserException(errMsg);
    }

    private boolean executeOnce() {
        // New one query id,
        UUID uuid = UUID.randomUUID();
        TUniqueId executeId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        Coordinator curCoordinator = new Coordinator(executeId, planner.getDescTable(),
                                                     planner.getFragments(), planner.getScanNodes(), db.getClusterName());
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.setExecMemoryLimit(execMemLimit);
        curCoordinator.setTimeout((int) (getLeftTimeMs() / 1000));

        try {
            QeProcessorImpl.INSTANCE
                    .registerQuery(executeId, curCoordinator);
            return actualExecute(curCoordinator);
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                             .add("error_msg", "failed to execute loading task")
                             .build(), e);
            errMsg = e.getMessage();
            return false;
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(executeId);
        }
    }

    private boolean actualExecute(Coordinator curCoordinator) {
        int waitSecond = (int) (getLeftTimeMs() / 1000);
        if (waitSecond <= 0) {
            errMsg = "time out";
            return false;
        }

        try {
            curCoordinator.exec();
        } catch (Exception e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                             .add("error_msg", "coordinator execute failed")
                             .build(), e);
            errMsg = "coordinator execute failed with error " + e.getMessage();
            return false;
        }
        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok()) {
                attachment = new BrokerLoadingTaskAttachment(curCoordinator.getLoadCounters(),
                                                             curCoordinator.getTrackingUrl(),
                                                             TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()));
                return true;
            } else {
                errMsg = status.getErrorMsg();
                return false;
            }
        } else {
            errMsg = "coordinator could not finished before job timeout";
            return false;
        }
    }

    private long getLeftTimeMs() {
        return jobDeadlineMs - System.currentTimeMillis();
    }
}
