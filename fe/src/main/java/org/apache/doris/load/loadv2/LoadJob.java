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

import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.Load;
import org.apache.doris.load.Source;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class LoadJob extends AbstractTxnStateChangeCallback implements LoadTaskCallback {

    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    protected static final String QUALITY_FAIL_MSG = "quality not good enough to cancel";
    protected static final String DPP_NORMAL_ALL = "dpp.norm.ALL";
    protected static final String DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";

    protected long id = Catalog.getCurrentCatalog().getNextId();
    protected long dbId;
    protected String label;
    protected JobState state = JobState.PENDING;
    protected org.apache.doris.load.LoadJob.EtlJobType jobType;

    // optional properties
    // timeout second need to be reset in constructor of subclass
    protected int timeoutSecond = Config.pull_load_task_default_timeout_second;
    protected long execMemLimit = 2147483648L; // 2GB;
    protected double maxFilterRatio = 0;
    protected boolean deleteFlag = false;

    protected long createTimestamp = System.currentTimeMillis();
    protected long loadStartTimestamp = -1;
    protected long finishTimestamp = -1;

    protected long transactionId;
    protected FailMsg failMsg;
    protected List<LoadTask> tasks = Lists.newArrayList();
    protected Set<Long> finishedTaskIds = Sets.newHashSet();
    protected EtlStatus loadingStatus = new EtlStatus();
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all of tasks have been finished
    // 100: txn status is visible and load has been finished
    protected int progress;
    protected List<TabletCommitInfo> commitInfos = Lists.newArrayList();

    // non-persistence
    protected boolean isCommitting = false;

    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public LoadJob(long dbId, String label) {
        this.dbId = dbId;
        this.label = label;
    }

    protected void readLock() {
        lock.readLock().lock();
    }

    protected void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }

    public long getId() {
        return id;
    }

    public Database getDb() throws MetaNotFoundException {
        // get db
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("Database " + dbId + " already has been deleted");
        }
        return db;
    }

    public long getDbId() {
        return dbId;
    }

    public String getLabel() {
        return label;
    }

    public JobState getState() {
        return state;
    }

    public long getDeadlineMs() {
        return createTimestamp + timeoutSecond * 1000;
    }

    public long getLeftTimeMs() {
        return getDeadlineMs() - System.currentTimeMillis();
    }

    public long getFinishTimestamp() {
        return finishTimestamp;
    }

    protected boolean isFinished() {
        return state == JobState.FINISHED || state == JobState.CANCELLED;
    }

    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        // resource info
        if (ConnectContext.get() != null) {
            execMemLimit = ConnectContext.get().getSessionVariable().getMaxExecMemByte();
        }

        // job properties
        if (properties != null) {
            if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
                try {
                    timeoutSecond = Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
                } catch (NumberFormatException e) {
                    throw new DdlException("Timeout is not INT", e);
                }
            }

            if (properties.containsKey(LoadStmt.MAX_FILTER_RATIO_PROPERTY)) {
                try {
                    maxFilterRatio = Double.parseDouble(properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY));
                } catch (NumberFormatException e) {
                    throw new DdlException("Max filter ratio is not DOUBLE", e);
                }
            }

            if (properties.containsKey(LoadStmt.LOAD_DELETE_FLAG_PROPERTY)) {
                String flag = properties.get(LoadStmt.LOAD_DELETE_FLAG_PROPERTY);
                if (flag.equalsIgnoreCase("true") || flag.equalsIgnoreCase("false")) {
                    deleteFlag = Boolean.parseBoolean(flag);
                } else {
                    throw new DdlException("Value of delete flag is invalid");
                }
            }

            if (properties.containsKey(LoadStmt.EXEC_MEM_LIMIT)) {
                try {
                    execMemLimit = Long.parseLong(properties.get(LoadStmt.EXEC_MEM_LIMIT));
                } catch (NumberFormatException e) {
                    throw new DdlException("Execute memory limit is not Long", e);
                }
            }
        }
    }

    protected void checkDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        for (DataDescription dataDescription : dataDescriptions) {
            // loadInfo is a temporary param for the method of checkAndCreateSource.
            // <TableId,<PartitionId,<LoadInfoList>>>
            Map<Long, Map<Long, List<Source>>> loadInfo = Maps.newHashMap();
            // only support broker load now
            Load.checkAndCreateSource(db, dataDescription, loadInfo, false);
        }
    }

    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        // register txn state listener
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(this);
        transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, String.valueOf(id), -1, "FE: " + FrontendOptions.getLocalHostAddress(),
                                  TransactionState.LoadJobSourceType.FRONTEND, id,
                                  timeoutSecond - 1);
    }

    /**
     * create pending task for load job and add pending task into pool
     * if job has been cancelled, this step will be ignored
     * @throws LabelAlreadyUsedException the job is duplicated
     * @throws BeginTransactionException the limit of load job is exceeded
     * @throws AnalysisException there are error params in job
     */
    public void execute() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        writeLock();
        try {
            // check if job state is pending
            if (state != JobState.PENDING) {
                return;
            }
            // the limit of job will be restrict when begin txn
            beginTxn();
            executeJob();
            unprotectedUpdateState(JobState.LOADING);
        } finally {
            writeUnlock();
        }
    }


    public void processTimeout() {
        writeLock();
        try {
            if (isFinished() || getDeadlineMs() >= System.currentTimeMillis() || isCommitting) {
                return;
            }
            executeCancel(new FailMsg(FailMsg.CancelType.TIMEOUT, "loading timeout to cancel"));
        } finally {
            writeUnlock();
        }
    }

    abstract void executeJob();

    public void updateState(JobState jobState) {
        writeLock();
        try {
            unprotectedUpdateState(jobState);
        } finally {
            writeUnlock();
        }
        // TODO(ML): edit log
    }

    protected void unprotectedUpdateState(JobState jobState) {
        switch (jobState) {
            case LOADING:
                executeLoad();
                break;
            case FINISHED:
                executeFinish();
            default:
                break;
        }
    }

    private void executeLoad() {
        loadStartTimestamp = System.currentTimeMillis();
        state = JobState.LOADING;
    }

    public void cancelJobWithoutCheck(FailMsg failMsg) {
        writeLock();
        try {
            executeCancel(failMsg);
        } finally {
            writeUnlock();
        }
    }

    public void cancelJob(FailMsg failMsg) throws DdlException {
        writeLock();
        try {
            if (isCommitting) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                 .add("error_msg", "The txn which belongs to job is committing. "
                                         + "The job could not be cancelled in this step").build());
                throw new DdlException("Job could not be cancelled while txn is committing");
            }
            executeCancel(failMsg);
        } finally {
            writeUnlock();
        }
    }

    protected void executeCancel(FailMsg failMsg) {
        LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                         .add("error_msg", "Failed to execute load with error " + failMsg.getMsg())
                         .build());

        // reset txn id
        if (transactionId != -1) {
            transactionId = -1;
        }

        // clean the loadingStatus
        loadingStatus.setState(TEtlState.CANCELLED);

        // tasks will not be removed from task pool.
        // it will be aborted on the stage of onTaskFinished or onTaskFailed.
        tasks.clear();

        // set failMsg and state
        this.failMsg = failMsg;
        finishTimestamp = System.currentTimeMillis();
        state = JobState.CANCELLED;

        // remove callback
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
    }

    private void executeFinish() {
        progress = 100;
        finishTimestamp = System.currentTimeMillis();
        state = JobState.FINISHED;
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);

        MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
    }

    protected boolean checkDataQuality() {
        Map<String, String> counters = loadingStatus.getCounters();
        if (!counters.containsKey(DPP_NORMAL_ALL) || !counters.containsKey(DPP_ABNORMAL_ALL)) {
            return true;
        }

        long normalNum = Long.parseLong(counters.get(DPP_NORMAL_ALL));
        long abnormalNum = Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
        if (abnormalNum > (abnormalNum + normalNum) * maxFilterRatio) {
            return false;
        }

        return true;
    }

    public List<Comparable> getShowInfo() {
        readLock();
        try {
            List<Comparable> jobInfo = Lists.newArrayList();
            // jobId
            jobInfo.add(id);
            // label
            jobInfo.add(label);
            // state
            jobInfo.add(state.name());

            // progress
            switch (state) {
                case PENDING:
                    jobInfo.add("ETL:N/A; LOAD:0%");
                    break;
                case CANCELLED:
                    jobInfo.add("ETL:N/A; LOAD:N/A");
                    break;
                default:
                    jobInfo.add("ETL:N/A; LOAD:" + progress + "%");
                    break;
            }

            // type
            jobInfo.add(jobType);

            // etl info
            if (loadingStatus.getCounters().size() == 0) {
                jobInfo.add("N/A");
            } else {
                jobInfo.add(Joiner.on("; ").withKeyValueSeparator("=").join(loadingStatus.getCounters()));
            }

            // task info
            jobInfo.add("cluster:N/A" + "; timeout(s):" + timeoutSecond
                                + "; max_filter_ratio:" + maxFilterRatio);

            // error msg
            if (failMsg == null) {
                jobInfo.add("N/A");
            } else {
                jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
            }

            // create time
            jobInfo.add(TimeUtils.longToTimeString(createTimestamp));
            // etl start time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // etl end time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // load start time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // load end time
            jobInfo.add(TimeUtils.longToTimeString(finishTimestamp));
            // tracking url
            jobInfo.add(loadingStatus.getTrackingUrl());
            return jobInfo;
        } finally {
            readUnlock();
        }
    }

    @Override
    public long getCallbackId() {
        return id;
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        writeLock();
        try {
            if (transactionId == -1) {
                throw new TransactionException("txn could not be committed when job has been cancelled");
            }
            isCommitting = true;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        if (txnOperated) {
            return;
        }
        writeLock();
        try {
            isCommitting = false;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        writeLock();
        try {
            isCommitting = true;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        if (!txnOperated) {
            return;
        }
        writeLock();
        try {
            if (transactionId == -1) {
                return;
            }
            // cancel load job
            executeCancel(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnStatusChangeReason));
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, null));
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        updateState(JobState.FINISHED);
    }
}
