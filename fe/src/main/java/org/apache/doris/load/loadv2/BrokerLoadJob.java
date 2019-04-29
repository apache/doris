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
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.PullLoadSourceInfo;
import org.apache.doris.thrift.TEtlState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class BrokerLoadJob extends LoadJob {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadJob.class);

    private BrokerDesc brokerDesc;
    // include broker desc and data desc
    private PullLoadSourceInfo dataSourceInfo = new PullLoadSourceInfo();

    public BrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc) {
        super(dbId, label);
        this.timeoutSecond = Config.pull_load_task_default_timeout_second;
        this.brokerDesc = brokerDesc;
    }

    public static BrokerLoadJob fromLoadStmt(LoadStmt stmt) throws DdlException {
        // get db id
        String dbName = stmt.getLabel().getDbName();
        Database db = Catalog.getInstance().getDb(stmt.getLabel().getDbName());
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        // create job
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob(db.getId(), stmt.getLabel().getLabelName(),
                                                        stmt.getBrokerDesc());
        brokerLoadJob.setJobProperties(stmt.getProperties());
        brokerLoadJob.setLoadInfo(db, stmt.getDataDescriptions());
        brokerLoadJob.setDataSourceInfo(db, stmt.getDataDescriptions());
        return brokerLoadJob;
    }

    private void setDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        for (DataDescription dataDescription : dataDescriptions) {
            BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription, brokerDesc);
            fileGroup.parse(db);
            dataSourceInfo.addFileGroup(fileGroup);
        }
        LOG.info("Source info is {}", dataSourceInfo);
    }

    @Override
    public void createPendingTask() {
        loadPendingTask = new BrokerLoadPendingTask(this, dataSourceInfo.getIdToFileGroups(), brokerDesc);
    }

    /**
     * divide job into loading task and init the plan of task
     * submit tasks into loadingTaskExecutor in LoadManager
     *
     * @param attachment
     */
    @Override
    public void onPendingTaskFinished(LoadPendingTaskAttachment attachment) {
        // TODO(ml): check if task has been cancelled
        BrokerPendingTaskAttachment brokerPendingTaskAttachment = (BrokerPendingTaskAttachment) attachment;
        Database db = null;
        try {
            getDb();
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("database_id", dbId)
                             .add("error_msg", "Failed to divide job into loading task when db not found.")
                             .build(), e);
            updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        // divide job into broker loading task by table
        db.readLock();
        try {
            // tableId -> BrokerFileGroups
            for (Map.Entry<Long, List<BrokerFileGroup>> entry :
                    dataSourceInfo.getIdToFileGroups().entrySet()) {
                long tableId = entry.getKey();
                OlapTable table = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                     .add("database_id", dbId)
                                     .add("table_id", tableId)
                                     .add("error_msg", "Failed to divide job into loading task when table not found")
                                     .build());
                    updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL,
                                "Unknown table(" + tableId + ") in database(" + db.getFullName() + ")");
                    return;
                }

                // Generate pull load task, one
                LoadLoadingTask task = new LoadLoadingTask(db, table, brokerDesc,
                                                           entry.getValue(), getDeadlineMs(), execMemLimit,
                                                           transactionId, this);
                task.init(brokerPendingTaskAttachment.getFileStatusByTable(tableId),
                          brokerPendingTaskAttachment.getFileNumByTable(tableId));
                loadLoadingTaskList.add(task);
                Catalog.getCurrentCatalog().getLoadManager().submitLoadingTask(task);
            }
        } catch (UserException e) {
            updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, "failed to " + e.getMessage());
        } finally {
            db.readUnlock();
        }
        loadStartTimestamp = System.currentTimeMillis();
    }

    @Override
    public void onPendingTaskFailed(String errMsg) {
        // TODO(ml): check if task has been cancelled
        updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, errMsg);
    }

    @Override
    public void onLoadingTaskFinished(LoadLoadingTaskAttachment attachment) {
        // TODO(ml): check if task has been cancelled
        boolean commitTxn = false;
        writeLock();
        try {
            // update loading status
            unprotectedUpdateLoadingStatus((BrokerLoadingTaskAttachment) attachment);

            // begin commit txn when all of loading tasks have been finished
            if (loadLoadingTaskList.size() == loadLoadingTaskList.stream()
                    .filter(entity -> entity.isFinished()).count()) {
                // check data quality
                if (!checkDataQuality()) {
                    unprotectedUpdateState(JobState.CANCELLED, FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                                           QUALITY_FAIL_MSG);
                } else {
                    commitTxn = true;
                }
            }
        } finally {
            writeUnlock();
        }

        Database db = null;
        try {
            getDb();
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("database_id", dbId)
                             .add("error_msg", "Failed to divide job into loading task when db not found.")
                             .build(), e);
            updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }
        if (commitTxn) {
            try {
                Catalog.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                        db, transactionId, commitInfos, getLeftTimeMs());
            } catch (UserException e) {
                updateState(JobState.CANCELLED, FailMsg.CancelType.LOAD_RUN_FAIL, "failed to " + e.getMessage());
            }
        }
    }

    @Override
    public void onLoadingTaskFailed(String errMsg) {
        // TODO(ml): check if task has been cancelled
        writeLock();
        try {
            // clean the loadingStatus
            loadingStatus.reset();
            loadingStatus.setState(TEtlState.CANCELLED);
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("error_msg", "Failed to execute load plan with error " + errMsg)
                             .build());
            // cancel the job
            unprotectedUpdateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, errMsg);
            return;
        } finally {
            writeUnlock();
        }
    }

    private void unprotectedUpdateLoadingStatus(BrokerLoadingTaskAttachment attachment) {
        loadingStatus.updateCounter(DPP_ABNORMAL_ALL,
                                    increaseCounter(DPP_ABNORMAL_ALL, attachment.getCounter(DPP_ABNORMAL_ALL)));
        loadingStatus.updateCounter(DPP_NORMAL_ALL,
                                    increaseCounter(DPP_NORMAL_ALL, attachment.getCounter(DPP_NORMAL_ALL)));
        if (attachment.getTrackingUrl() != null) {
            loadingStatus.setTrackingUrl(attachment.getTrackingUrl());
        }
        loadingStatus.addAllFileMap(attachment.getFileMap());
        commitInfos.addAll(attachment.getCommitInfoList());
        int finishedLoadingTaskNum = (int) loadLoadingTaskList.stream().filter(entity -> entity.isFinished()).count();
        progress = finishedLoadingTaskNum / loadLoadingTaskList.size() * 100;
        if (progress == 100) {
            progress = 99;
        }
    }

    private String increaseCounter(String key, String deltaValue) {
        long value = Long.valueOf(loadingStatus.getCounters().get(key));
        if (deltaValue != null) {
            value += Long.valueOf(deltaValue);
        }
        return String.valueOf(value);
    }
}
