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

package org.apache.doris.load.routineload;


import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

/**
 * Routine load task info is the task info include the only id (signature).
 * For the kafka type of task info, it also include partitions which will be obtained data in this task.
 * The routine load task info and routine load task are the same thing logically.
 * Differently, routine load task is a agent task include backendId which will execute this task.
 */
public abstract class RoutineLoadTaskInfo {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskInfo.class);

    private RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();

    protected UUID id;
    protected static final long INIT_TXN_ID = -1L;
    protected long txnId = INIT_TXN_ID;
    protected long jobId;

    private long createTimeMs;
    private long executeStartTimeMs = -1L;
    // the be id of previous task
    protected long previousBeId = -1L;
    // the be id of this task
    protected long beId = -1L;

    // last time this task being scheduled by RoutineLoadTaskScheduler
    protected long lastScheduledTime = -1;

    protected long timeoutMs = -1;

    protected boolean isMultiTable = false;

    protected boolean isEof = false;

    // this status will be set when corresponding transaction's status is changed.
    // so that user or other logic can know the status of the corresponding txn.
    protected TransactionStatus txnStatus = TransactionStatus.UNKNOWN;

    public RoutineLoadTaskInfo(UUID id, long jobId, long timeoutMs, boolean isMultiTable,
                    long lastScheduledTime, boolean isEof) {
        this.id = id;
        this.jobId = jobId;
        this.createTimeMs = System.currentTimeMillis();
        this.timeoutMs = timeoutMs;
        this.isMultiTable = isMultiTable;
        this.lastScheduledTime = lastScheduledTime;
        this.isEof = isEof;
    }

    public RoutineLoadTaskInfo(UUID id, long jobId, long timeoutMs, long previousBeId,
                               boolean isMultiTable, long lastScheduledTime, boolean isEof) {
        this(id, jobId, timeoutMs, isMultiTable, lastScheduledTime, isEof);
        this.previousBeId = previousBeId;
    }

    public UUID getId() {
        return id;
    }

    public long getJobId() {
        return jobId;
    }

    public void setExecuteStartTimeMs(long executeStartTimeMs) {
        this.executeStartTimeMs = executeStartTimeMs;
    }

    public long getPreviousBeId() {
        return previousBeId;
    }

    public void setBeId(long beId) {
        this.beId = beId;
    }

    public long getBeId() {
        return beId;
    }

    public long getTxnId() {
        return txnId;
    }

    public boolean isRunning() {
        return executeStartTimeMs > 0;
    }

    public long getLastScheduledTime() {
        return lastScheduledTime;
    }

    public void setLastScheduledTime(long lastScheduledTime) {
        this.lastScheduledTime = lastScheduledTime;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTxnStatus(TransactionStatus txnStatus) {
        this.txnStatus = txnStatus;
    }

    public TransactionStatus getTxnStatus() {
        return txnStatus;
    }

    public boolean getIsEof() {
        return isEof;
    }

    public boolean isTimeout() {
        if (txnStatus == TransactionStatus.COMMITTED || txnStatus == TransactionStatus.VISIBLE) {
            // the corresponding txn is already finished, this task can not be treated as timeout.
            return false;
        }

        if (isRunning() && System.currentTimeMillis() - executeStartTimeMs > timeoutMs) {
            LOG.info("task {} is timeout. start: {}, timeout: {}", DebugUtil.printId(id),
                    executeStartTimeMs, timeoutMs);
            return true;
        }
        return false;
    }

    public void handleTaskByTxnCommitAttachment(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        judgeEof(rlTaskTxnCommitAttachment);
    }

    private void judgeEof(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment) {
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        if (rlTaskTxnCommitAttachment.getTotalRows() < routineLoadJob.getMaxBatchRows()
                && rlTaskTxnCommitAttachment.getReceivedBytes() < routineLoadJob.getMaxBatchSizeBytes()
                && rlTaskTxnCommitAttachment.getTaskExecutionTimeMs() < this.timeoutMs) {
            this.isEof = true;
        }
    }

    abstract TRoutineLoadTask createRoutineLoadTask() throws UserException;

    // begin the txn of this task
    // return true if begin successfully, return false if begin failed.
    // throw exception if unrecoverable errors happen.
    public boolean beginTxn() throws UserException {
        // begin a txn for task
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        try {
            txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(routineLoadJob.getDbId(),
                    Lists.newArrayList(routineLoadJob.getTableId()), DebugUtil.printId(id), null,
                    new TxnCoordinator(TxnSourceType.FE, 0,
                            FrontendOptions.getLocalHostAddress(),
                            ExecuteEnv.getInstance().getStartupTime()),
                    TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK, routineLoadJob.getId(),
                    timeoutMs / 1000);
        } catch (DuplicatedRequestException e) {
            // should not happen, because we didn't pass request id in when begin transaction
            LOG.warn("failed to begin txn for routine load task: {}, {}", DebugUtil.printId(id), e.getMessage());
            return false;
        } catch (LabelAlreadyUsedException e) {
            // this should not happen for a routine load task, throw it out
            throw e;
        } catch (AnalysisException | BeginTransactionException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("begin txn failed for routine load task: {}, {}", DebugUtil.printId(id), e.getMessage());
            }
            return false;
        } catch (MetaNotFoundException | QuotaExceedException e) {
            LOG.warn("failed to begin txn for routine load task: {}, job id: {}",
                    DebugUtil.printId(id), jobId, e);
            throw e;
        }
        routineLoadJob.jobStatistic.runningTxnIds.add(txnId);
        return true;
    }

    public List<String> getTaskShowInfo() {
        List<String> row = Lists.newArrayList();
        row.add(DebugUtil.printId(id));
        row.add(String.valueOf(txnId));
        if (INIT_TXN_ID != txnId) {
            row.add(txnStatus.name());
        } else {
            row.add(null);
        }
        row.add(String.valueOf(jobId));
        row.add(String.valueOf(TimeUtils.longToTimeString(createTimeMs)));
        row.add(String.valueOf(TimeUtils.longToTimeString(executeStartTimeMs)));
        row.add(String.valueOf(timeoutMs / 1000));
        row.add(String.valueOf(beId));
        row.add(getTaskDataSourceProperties());
        return row;
    }

    abstract String getTaskDataSourceProperties();

    abstract boolean hasMoreDataToConsume() throws UserException;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RoutineLoadTaskInfo) {
            RoutineLoadTaskInfo routineLoadTaskInfo = (RoutineLoadTaskInfo) obj;
            return this.id.toString().equals(routineLoadTaskInfo.getId().toString());
        } else {
            return false;
        }
    }
}
