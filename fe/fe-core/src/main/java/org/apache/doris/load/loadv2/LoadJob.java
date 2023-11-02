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

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.QuotaExceedException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.AbstractTxnStateChangeCallback;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class LoadJob extends AbstractTxnStateChangeCallback implements LoadTaskCallback, Writable {

    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    protected static final String DPP_NORMAL_ALL = "dpp.norm.ALL";
    protected static final String DPP_ABNORMAL_ALL = "dpp.abnorm.ALL";
    public static final String UNSELECTED_ROWS = "unselected.rows";

    @SerializedName(value = "id")
    protected long id;
    // input params
    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "label")
    protected String label;
    @SerializedName(value = "state")
    protected JobState state = JobState.PENDING;
    @SerializedName(value = "jobType")
    protected EtlJobType jobType;
    // the auth info could be null when load job is created before commit named 'Persist auth info in load job'
    @SerializedName(value = "authorizationInfo")
    protected AuthorizationInfo authorizationInfo;

    @SerializedName(value = "createTimestamp")
    protected long createTimestamp = System.currentTimeMillis();
    @SerializedName(value = "loadStartTimestamp")
    protected long loadStartTimestamp = -1;
    @SerializedName(value = "finishTimestamp")
    protected long finishTimestamp = -1;

    @SerializedName(value = "transactionId")
    protected long transactionId;
    @SerializedName(value = "failMsg")
    protected FailMsg failMsg;
    @SerializedName(value = "idToTasks")
    protected Map<Long, LoadTask> idToTasks = Maps.newConcurrentMap();
    @SerializedName(value = "finishedTaskIds")
    protected Set<Long> finishedTaskIds = Sets.newHashSet();
    @SerializedName(value = "loadingStatus")
    protected EtlStatus loadingStatus = new EtlStatus();
    // 0: the job status is pending
    // n/100: n is the number of task which has been finished
    // 99: all of tasks have been finished
    // 100: txn status is visible and load has been finished
    @SerializedName(value = "progress")
    protected int progress;

    // non-persistence
    // This param is set true during txn is committing.
    // During committing, the load job could not be cancelled.
    @SerializedName(value = "isCommitting")
    protected boolean isCommitting = false;

    @SerializedName(value = "lock")
    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    // this request id is only used for checking if a load begin request is a duplicate request.
    @SerializedName(value = "requestId")
    protected TUniqueId requestId;

    @SerializedName(value = "loadStatistic")
    protected LoadStatistic loadStatistic = new LoadStatistic();

    // This map is used to save job property.
    @SerializedName(value = "jobProperties")
    private Map<String, Object> jobProperties = Maps.newHashMap();

    // only for persistence param. see readFields() for usage
    @SerializedName(value = "isJobTypeRead")
    private boolean isJobTypeRead = false;

    @SerializedName(value = "errorTabletInfos")
    protected List<ErrorTabletInfo> errorTabletInfos = Lists.newArrayList();

    @SerializedName(value = "userInfo")
    protected UserIdentity userInfo = UserIdentity.UNKNOWN;

    @SerializedName(value = "comment")
    protected String comment = "";


    public static class LoadStatistic {
        // number of rows processed on BE, this number will be updated periodically by query report.
        // A load job may has several load tasks(queries), and each task has several fragments.
        // each fragment will report independently.
        // load task id -> fragment id -> rows count
        private Table<TUniqueId, TUniqueId, Long> counterTbl = HashBasedTable.create();

        // load task id -> fragment id -> load bytes
        private Table<TUniqueId, TUniqueId, Long> loadBytes = HashBasedTable.create();

        // load task id -> unfinished backend id list
        private Map<TUniqueId, List<Long>> unfinishedBackendIds = Maps.newHashMap();
        // load task id -> all backend id list
        private Map<TUniqueId, List<Long>> allBackendIds = Maps.newHashMap();

        // number of file to be loaded
        public int fileNum = 0;
        public long totalFileSizeB = 0;

        // init the statistic of specified load task
        public synchronized void initLoad(TUniqueId loadId, Set<TUniqueId> fragmentIds, List<Long> relatedBackendIds) {
            counterTbl.rowMap().remove(loadId);
            for (TUniqueId fragId : fragmentIds) {
                counterTbl.put(loadId, fragId, 0L);
            }
            loadBytes.rowMap().remove(loadId);
            for (TUniqueId fragId : fragmentIds) {
                loadBytes.put(loadId, fragId, 0L);
            }
            allBackendIds.put(loadId, relatedBackendIds);
            // need to get a copy of relatedBackendIds, so that when we modify the "relatedBackendIds" in
            // allBackendIds, the list in unfinishedBackendIds will not be changed.
            unfinishedBackendIds.put(loadId, Lists.newArrayList(relatedBackendIds));
        }

        public synchronized void removeLoad(TUniqueId loadId) {
            counterTbl.rowMap().remove(loadId);
            loadBytes.rowMap().remove(loadId);
            unfinishedBackendIds.remove(loadId);
            allBackendIds.remove(loadId);
        }

        public synchronized void updateLoadProgress(long backendId, TUniqueId loadId, TUniqueId fragmentId,
                                                    long rows, long bytes, boolean isDone) {
            if (counterTbl.contains(loadId, fragmentId)) {
                counterTbl.put(loadId, fragmentId, rows);
            }

            if (loadBytes.contains(loadId, fragmentId)) {
                loadBytes.put(loadId, fragmentId, bytes);
            }
            if (isDone && unfinishedBackendIds.containsKey(loadId)) {
                unfinishedBackendIds.get(loadId).remove(backendId);
            }
        }

        public synchronized long getScannedRows() {
            long total = 0;
            for (long rows : counterTbl.values()) {
                total += rows;
            }
            return total;
        }

        public synchronized long getLoadBytes() {
            long total = 0;
            for (long bytes : loadBytes.values()) {
                total += bytes;
            }
            return total;
        }

        public synchronized String toJson() {
            long total = 0;
            for (long rows : counterTbl.values()) {
                total += rows;
            }
            long totalBytes = 0;
            for (long bytes : loadBytes.values()) {
                totalBytes += bytes;
            }

            Map<String, Object> details = Maps.newHashMap();
            details.put("ScannedRows", total);
            details.put("LoadBytes", totalBytes);
            details.put("FileNumber", fileNum);
            details.put("FileSize", totalFileSizeB);
            details.put("TaskNumber", counterTbl.rowMap().size());
            details.put("Unfinished backends", getPrintableMap(unfinishedBackendIds));
            details.put("All backends", getPrintableMap(allBackendIds));
            Gson gson = new Gson();
            return gson.toJson(details);
        }

        private Map<String, List<Long>> getPrintableMap(Map<TUniqueId, List<Long>> map) {
            Map<String, List<Long>> newMap = Maps.newHashMap();
            for (Map.Entry<TUniqueId, List<Long>> entry : map.entrySet()) {
                newMap.put(DebugUtil.printId(entry.getKey()), entry.getValue());
            }
            return newMap;
        }
    }

    public LoadJob(EtlJobType jobType) {
        this.jobType = jobType;
        initDefaultJobProperties();
    }

    public LoadJob(EtlJobType jobType, long dbId, String label) {
        this(jobType);
        this.id = Env.getCurrentEnv().getNextId();
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
        return Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
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

    public EtlJobType getJobType() {
        return jobType;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    protected long getDeadlineMs() {
        return createTimestamp + getTimeout() * 1000;
    }

    private boolean isTimeout() {
        return System.currentTimeMillis() > getDeadlineMs();
    }

    public long getFinishTimestamp() {
        return finishTimestamp;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void initLoadProgress(TUniqueId loadId, Set<TUniqueId> fragmentIds, List<Long> relatedBackendIds) {
        loadStatistic.initLoad(loadId, fragmentIds, relatedBackendIds);
    }

    public void updateProgress(Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
                               long scannedBytes, boolean isDone) {
        loadStatistic.updateLoadProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
    }

    public void setLoadFileInfo(int fileNum, long fileSize) {
        this.loadStatistic.fileNum = fileNum;
        this.loadStatistic.totalFileSizeB = fileSize;
    }

    public TUniqueId getRequestId() {
        return requestId;
    }

    /**
     * Show table names for frontend
     * If table name could not be found by id, the table id will be used instead.
     *
     * @return
     */
    abstract Set<String> getTableNamesForShow();

    /**
     * Return the real table names by table ids.
     * The method is invoked by 'checkAuth' when authorization info is null in job.
     * Also it is invoked by 'gatherAuthInfo' which saves the auth info in the constructor of job.
     * Throw MetaNofFoundException when table name could not be found.
     *
     * @return
     */
    abstract Set<String> getTableNames() throws MetaNotFoundException;

    // return true if the corresponding transaction is done(COMMITTED, FINISHED, CANCELLED)
    public boolean isTxnDone() {
        return state == JobState.COMMITTED || state == JobState.FINISHED || state == JobState.CANCELLED;
    }

    // return true if job is done(FINISHED/CANCELLED/UNKNOWN)
    public boolean isCompleted() {
        return state == JobState.FINISHED || state == JobState.CANCELLED || state == JobState.UNKNOWN;
    }

    public void setJobProperties(Map<String, String> properties) throws DdlException {
        initDefaultJobProperties();

        // set property from session variables
        if (ConnectContext.get() != null) {
            jobProperties.put(LoadStmt.EXEC_MEM_LIMIT, ConnectContext.get().getSessionVariable().getMaxExecMemByte());
            jobProperties.put(LoadStmt.TIMEZONE, ConnectContext.get().getSessionVariable().getTimeZone());
            jobProperties.put(LoadStmt.SEND_BATCH_PARALLELISM,
                    ConnectContext.get().getSessionVariable().getSendBatchParallelism());
        }

        if (properties == null || properties.isEmpty()) {
            return;
        }

        // set property from specified job properties
        for (String key : LoadStmt.PROPERTIES_MAP.keySet()) {
            if (!properties.containsKey(key)) {
                continue;
            }
            try {
                jobProperties.put(key, LoadStmt.PROPERTIES_MAP.get(key).apply(properties.get(key)));
            } catch (Exception e) {
                throw new DdlException("Failed to set property " + key + ". Error: " + e.getMessage());
            }
        }
    }

    public UserIdentity getUserInfo() {
        return userInfo;
    }

    public void setUserInfo(UserIdentity userInfo) {
        this.userInfo = userInfo;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    private void initDefaultJobProperties() {
        long timeout = Config.broker_load_default_timeout_second;
        switch (jobType) {
            case SPARK:
                timeout = Config.spark_load_default_timeout_second;
                break;
            case HADOOP:
                timeout = Config.hadoop_load_default_timeout_second;
                break;
            case BROKER:
                timeout = Config.broker_load_default_timeout_second;
                break;
            case INSERT:
                timeout = Optional.ofNullable(ConnectContext.get())
                                    .map(ConnectContext::getExecTimeout)
                                    .orElse(Config.insert_load_default_timeout_second);
                break;
            case MINI:
                timeout = Config.stream_load_default_timeout_second;
                break;
            default:
                break;
        }
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, timeout);
        jobProperties.put(LoadStmt.EXEC_MEM_LIMIT, 2 * 1024 * 1024 * 1024L);
        jobProperties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, 0.0);
        jobProperties.put(LoadStmt.STRICT_MODE, false);
        jobProperties.put(LoadStmt.PARTIAL_COLUMNS, false);
        jobProperties.put(LoadStmt.TIMEZONE, TimeUtils.DEFAULT_TIME_ZONE);
        jobProperties.put(LoadStmt.LOAD_PARALLELISM, Config.default_load_parallelism);
        jobProperties.put(LoadStmt.SEND_BATCH_PARALLELISM, 1);
        jobProperties.put(LoadStmt.LOAD_TO_SINGLE_TABLET, false);
        jobProperties.put(LoadStmt.PRIORITY, LoadTask.Priority.NORMAL);
    }

    public void isJobTypeRead(boolean jobTypeRead) {
        isJobTypeRead = jobTypeRead;
    }

    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException,
            AnalysisException, DuplicatedRequestException, QuotaExceedException, MetaNotFoundException {
    }

    /**
     * create pending task for load job and add pending task into pool
     * if job has been cancelled, this step will be ignored
     *
     * @throws LabelAlreadyUsedException  the job is duplicated
     * @throws BeginTransactionException  the limit of load job is exceeded
     * @throws AnalysisException          there are error params in job
     * @throws DuplicatedRequestException
     */
    public void execute() throws LoadException {
        writeLock();
        try {
            unprotectedExecute();
        } finally {
            writeUnlock();
        }
    }

    public void unprotectedExecute() throws LoadException {
        // check if job state is pending
        if (state != JobState.PENDING) {
            return;
        }

        unprotectedExecuteJob();
    }

    public void processTimeout() {
        // this is only for jobs which transaction is not started.
        // if transaction is started, global transaction manager will handle the timeout.
        writeLock();
        try {
            if (state != JobState.PENDING) {
                return;
            }

            if (!isTimeout()) {
                return;
            }

            unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.TIMEOUT, "loading timeout to cancel"), false);
            logFinalOperation();
        } finally {
            writeUnlock();
        }
    }

    protected void unprotectedExecuteJob() throws LoadException {
    }

    /**
     * This method only support update state to finished and loading.
     * It will not be persisted when desired state is finished because txn visible will edit the log.
     * If you want to update state to cancelled, please use the cancelJob function.
     *
     * @param jobState
     */
    public boolean updateState(JobState jobState) {
        writeLock();
        try {
            return unprotectedUpdateState(jobState);
        } finally {
            writeUnlock();
        }
    }

    protected boolean unprotectedUpdateState(JobState jobState) {
        if (this.state.isFinalState()) {
            // This is a simple self-protection mechanism to prevent jobs
            // that have entered the final state from being placed in a non-terminating state again.
            // For example, when a LoadLoadingTask starts running, it tries to set the job state to LOADING,
            // but the job may have been cancelled (CANCELLED) due to a timeout.
            // At this point, the job state should not be set to LOADING again.
            // It is safe to return directly here without any processing,
            // and other processes will ensure that the job ends properly.
            LOG.warn("the load job {} is in final state: {}, should not update state to {} again",
                    id, this.state, jobState);
            return false;
        }
        switch (jobState) {
            case UNKNOWN:
                executeUnknown();
                return true;
            case LOADING:
                executeLoad();
                return true;
            case COMMITTED:
                executeCommitted();
                return true;
            case FINISHED:
                executeFinish();
                return true;
            default:
                return false;
        }
    }

    private void executeUnknown() {
        // set finished timestamp to create timestamp, so that this unknown job
        // can be remove due to label expiration so soon as possible
        finishTimestamp = createTimestamp;
        state = JobState.UNKNOWN;
    }

    private void executeLoad() {
        loadStartTimestamp = System.currentTimeMillis();
        state = JobState.LOADING;
    }

    private void executeCommitted() {
        state = JobState.COMMITTED;
    }

    // if needLog is false, no need to write edit log.
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        writeLock();
        try {
            unprotectedExecuteCancel(failMsg, abortTxn);
            if (needLog) {
                logFinalOperation();
            }
        } finally {
            writeUnlock();
        }
    }

    public void cancelJob(FailMsg failMsg) throws DdlException {
        writeLock();
        try {
            checkAuth("CANCEL LOAD");

            // mini load can not be cancelled by frontend
            if (jobType == EtlJobType.MINI) {
                throw new DdlException("Job could not be cancelled in type " + jobType.name());
            }
            if (isCommitting) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("error_msg", "The txn which belongs to job is committing. "
                                + "The job could not be cancelled in this step").build());
                throw new DdlException("Job could not be cancelled while txn is committing");
            }
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("state", state)
                        .add("error_msg", "Job could not be cancelled when job is " + state)
                        .build());
                throw new DdlException("Job could not be cancelled when job is finished or cancelled");
            }

            unprotectedExecuteCancel(failMsg, true);
            logFinalOperation();
        } finally {
            writeUnlock();
        }
    }

    private void checkAuth(String command) throws DdlException {
        if (authorizationInfo == null) {
            // use the old method to check priv
            checkAuthWithoutAuthInfo(command);
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkPrivByAuthInfo(ConnectContext.get(), authorizationInfo,
                PrivPredicate.LOAD)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    Privilege.LOAD_PRIV);
        }
    }

    /**
     * This method is compatible with old load job without authorization info
     * If db or table name could not be found by id, it will throw the NOT_EXISTS_ERROR
     *
     * @throws DdlException
     */
    private void checkAuthWithoutAuthInfo(String command) throws DdlException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);

        // check auth
        try {
            Set<String> tableNames = getTableNames();
            if (tableNames.isEmpty()) {
                // forward compatibility
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), db.getFullName(),
                        PrivPredicate.LOAD)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            Privilege.LOAD_PRIV);
                }
            } else {
                for (String tblName : tableNames) {
                    if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), db.getFullName(),
                            tblName, PrivPredicate.LOAD)) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                command,
                                ConnectContext.get().getQualifiedUser(),
                                ConnectContext.get().getRemoteIP(), db.getFullName() + ": " + tblName);
                    }
                }
            }
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }

    /**
     * This method will cancel job without edit log and lock
     *
     * @param failMsg
     * @param abortTxn true: abort txn when cancel job, false: only change the state of job and ignore abort txn
     */
    protected void unprotectedExecuteCancel(FailMsg failMsg, boolean abortTxn) {
        LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id).add("transaction_id", transactionId)
                .add("error_msg", "Failed to execute load with error: " + failMsg.getMsg()).build());

        // clean the loadingStatus
        loadingStatus.setState(TEtlState.CANCELLED);
        loadingStatus.setFailMsg(failMsg.getMsg());
        // get load ids of all loading tasks, we will cancel their coordinator process later
        List<TUniqueId> loadIds = Lists.newArrayList();
        for (LoadTask loadTask : idToTasks.values()) {
            if (loadTask instanceof LoadLoadingTask) {
                loadIds.add(((LoadLoadingTask) loadTask).getLoadId());
            }
        }
        idToTasks.clear();

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

        if (abortTxn) {
            // abort txn
            try {
                LOG.debug(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("transaction_id", transactionId)
                        .add("msg", "begin to abort txn")
                        .build());
                Env.getCurrentGlobalTransactionMgr().abortTransaction(dbId, transactionId, failMsg.getMsg());
            } catch (UserException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                        .add("transaction_id", transactionId)
                        .add("error_msg", "failed to abort txn when job is cancelled. " + e.getMessage())
                        .build());
            }
        }

        // cancel all running coordinators, so that the scheduler's worker thread will be released
        for (TUniqueId loadId : loadIds) {
            Coordinator coordinator = QeProcessorImpl.INSTANCE.getCoordinator(loadId);
            if (coordinator != null) {
                coordinator.cancel();
            }
        }

        // change state
        state = JobState.CANCELLED;
    }

    private void executeFinish() {
        progress = 100;
        finishTimestamp = System.currentTimeMillis();
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        state = JobState.FINISHED;
        // when load job finished, there is no need to hold the tasks which are the biggest memory consumers.
        idToTasks.clear();
    }

    protected boolean checkDataQuality() {
        Map<String, String> counters = loadingStatus.getCounters();
        if (!counters.containsKey(DPP_NORMAL_ALL) || !counters.containsKey(DPP_ABNORMAL_ALL)) {
            return true;
        }

        long normalNum = Long.parseLong(counters.get(DPP_NORMAL_ALL));
        long abnormalNum = Long.parseLong(counters.get(DPP_ABNORMAL_ALL));
        if (abnormalNum > (abnormalNum + normalNum) * getMaxFilterRatio()) {
            return false;
        }

        return true;
    }

    protected void logFinalOperation() {
        Env.getCurrentEnv().getEditLog().logEndLoadJob(
                new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp, finishTimestamp,
                        state, failMsg));
    }

    public void unprotectReadEndOperation(LoadJobFinalOperation loadJobFinalOperation) {
        loadingStatus = loadJobFinalOperation.getLoadingStatus();
        progress = loadJobFinalOperation.getProgress();
        loadStartTimestamp = loadJobFinalOperation.getLoadStartTimestamp();
        finishTimestamp = loadJobFinalOperation.getFinishTimestamp();
        state = loadJobFinalOperation.getJobState();
        failMsg = loadJobFinalOperation.getFailMsg();
    }

    public List<Comparable> getShowInfo() throws DdlException {
        readLock();
        try {
            // check auth
            checkAuth("SHOW LOAD");
            List<Comparable> jobInfo = Lists.newArrayList();
            // jobId
            jobInfo.add(id);
            // label
            jobInfo.add(label);
            // state
            jobInfo.add(state.name());

            // progress
            // check null
            String progress = Env.getCurrentProgressManager().getProgressInfo(String.valueOf(id));
            switch (state) {
                case PENDING:
                    jobInfo.add("0%");
                    break;
                case CANCELLED:
                    jobInfo.add(progress);
                    break;
                case ETL:
                    jobInfo.add(progress);
                    break;
                default:
                    jobInfo.add(progress);
                    break;
            }

            // type
            jobInfo.add(jobType);

            // etl info
            if (loadingStatus.getCounters().size() == 0) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add(Joiner.on("; ").withKeyValueSeparator("=").join(loadingStatus.getCounters()));
            }

            // task info
            jobInfo.add("cluster:" + getResourceName() + "; timeout(s):" + getTimeout()
                    + "; max_filter_ratio:" + getMaxFilterRatio() + "; priority:" + getPriority());
            // error msg
            if (failMsg == null) {
                jobInfo.add(FeConstants.null_string);
            } else {
                jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
            }

            // create time
            jobInfo.add(TimeUtils.longToTimeString(createTimestamp));
            // etl start time
            jobInfo.add(TimeUtils.longToTimeString(getEtlStartTimestamp()));
            // etl end time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // load start time
            jobInfo.add(TimeUtils.longToTimeString(loadStartTimestamp));
            // load end time
            jobInfo.add(TimeUtils.longToTimeString(finishTimestamp));
            // tracking url
            jobInfo.add(loadingStatus.getTrackingUrl());
            jobInfo.add(loadStatistic.toJson());
            // transaction id
            jobInfo.add(transactionId);
            // error tablets
            jobInfo.add(errorTabletsToJson());
            // user
            jobInfo.add(userInfo.getQualifiedUser());
            // comment
            jobInfo.add(comment);
            return jobInfo;
        } finally {
            readUnlock();
        }
    }

    public String errorTabletsToJson() {
        Map<Long, String> map = Maps.newHashMap();
        errorTabletInfos.stream().limit(Config.max_error_tablet_of_broker_load)
            .forEach(p -> map.put(p.getTabletId(), p.getMsg()));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(map);
    }

    protected String getResourceName() {
        return "N/A";
    }

    protected long getEtlStartTimestamp() {
        return loadStartTimestamp;
    }

    public void getJobInfo(Load.JobInfo jobInfo) throws DdlException {
        checkAuth("SHOW LOAD");
        jobInfo.tblNames.addAll(getTableNamesForShow());
        jobInfo.state = org.apache.doris.load.LoadJob.JobState.valueOf(state.name());
        if (failMsg != null) {
            jobInfo.failMsg = failMsg.getMsg();
        } else {
            jobInfo.failMsg = "";
        }
        jobInfo.trackingUrl = loadingStatus.getTrackingUrl();
    }

    public static LoadJob read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_127) {
            LoadJob job = null;
            EtlJobType type = EtlJobType.valueOf(Text.readString(in));
            if (type == EtlJobType.BROKER) {
                job = new BrokerLoadJob();
            } else if (type == EtlJobType.SPARK) {
                job = new SparkLoadJob();
            } else if (type == EtlJobType.INSERT) {
                job = new InsertLoadJob();
            } else if (type == EtlJobType.MINI) {
                job = new MiniLoadJob();
            } else {
                throw new IOException("Unknown load type: " + type.name());
            }

            job.isJobTypeRead(true);
            job.readFields(in);
            return job;
        }
        String json = Text.readString(in);
        JsonObject jsonObject = GsonUtils.GSON.fromJson(json, JsonObject.class);
        EtlJobType type = EtlJobType.valueOf(jsonObject.get("jobType").getAsString());
        switch (type) {
            case BROKER:
                return GsonUtils.GSON.fromJson(json, BrokerLoadJob.class);
            case SPARK:
                return GsonUtils.GSON.fromJson(json, SparkLoadJob.class);
            case INSERT:
                return GsonUtils.GSON.fromJson(json, InsertLoadJob.class);
            case MINI:
                return GsonUtils.GSON.fromJson(json, MiniLoadJob.class);
            default:
                throw new IOException("Unknown load type: " + type.name());
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
            if (isTxnDone()) {
                throw new TransactionException("txn could not be committed because job is: " + state);
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
            state = JobState.COMMITTED;
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            transactionId = txnState.getTransactionId();
            state = JobState.COMMITTED;
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will cancel job without edit log.
     * The job will be cancelled by replayOnAborted when journal replay
     *
     * @param txnState
     * @param txnOperated
     * @param txnStatusChangeReason
     * @throws UserException
     */
    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        if (!txnOperated) {
            return;
        }
        writeLock();
        try {
            if (isTxnDone()) {
                return;
            }
            // record attachment in load job
            replayTxnAttachment(txnState);
            // cancel load job
            unprotectedExecuteCancel(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnStatusChangeReason), false);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method is used to replay the cancelled state of load job
     *
     * @param txnState
     */
    @Override
    public void replayOnAborted(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, txnState.getReason());
            finishTimestamp = txnState.getFinishTime();
            state = JobState.CANCELLED;
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will finish the load job without edit log.
     * The job will be finished by replayOnVisible when txn journal replay
     *
     * @param txnState
     * @param txnOperated
     */
    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        if (!txnOperated) {
            return;
        }
        replayTxnAttachment(txnState);
        updateState(JobState.FINISHED);
        auditFinishedLoadJob();
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        writeLock();
        try {
            replayTxnAttachment(txnState);
            progress = 100;
            finishTimestamp = txnState.getFinishTime();
            state = JobState.FINISHED;
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().removeCallback(id);
        } finally {
            writeUnlock();
        }
    }

    protected void replayTxnAttachment(TransactionState txnState) {
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
    }

    @Override
    public void onTaskFailed(long taskId, FailMsg failMsg) {
    }

    // This analyze will be invoked after the replay is finished.
    // The edit log of LoadJob saves the origin param which is not analyzed.
    // So, the re-analyze must be invoked between the replay is finished and LoadJobScheduler is started.
    // Only, the PENDING load job need to be analyzed.
    public void analyze() {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LoadJob other = (LoadJob) obj;

        return this.id == other.id
                && this.dbId == other.dbId
                && this.label.equals(other.label)
                && this.state.equals(other.state)
                && this.jobType.equals(other.jobType);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {

        if (!isJobTypeRead) {
            jobType = EtlJobType.valueOf(Text.readString(in));
            isJobTypeRead = true;
        }

        id = in.readLong();
        dbId = in.readLong();
        label = Text.readString(in);
        state = JobState.valueOf(Text.readString(in));

        createTimestamp = in.readLong();
        loadStartTimestamp = in.readLong();
        finishTimestamp = in.readLong();
        if (in.readBoolean()) {
            failMsg = new FailMsg();
            failMsg.readFields(in);
        }
        progress = in.readInt();
        loadingStatus.readFields(in);
        transactionId = in.readLong();
        if (in.readBoolean()) {
            authorizationInfo = new AuthorizationInfo();
            authorizationInfo.readFields(in);
        }

        int size = in.readInt();
        Map<String, String> tmpProperties = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String val = Text.readString(in);
            tmpProperties.put(key, val);
        }
        // init jobProperties
        try {
            setJobProperties(tmpProperties);
        } catch (Exception e) {
            // should not happen
            throw new IOException("failed to replay job property", e);
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_117) {
            if (in.readBoolean()) {
                userInfo = UserIdentity.read(in);
                // must set is as analyzed, because when write the user info to meta image, it will be checked.
                userInfo.setIsAnalyzed();
            } else {
                userInfo = UserIdentity.UNKNOWN;
            }
            comment = Text.readString(in);
        } else {
            comment = "";
        }
    }

    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        state = info.getState();
        transactionId = info.getTransactionId();
        loadStartTimestamp = info.getLoadStartTimestamp();
    }

    protected void auditFinishedLoadJob() {
    }

    public static class LoadJobStateUpdateInfo implements Writable {
        @SerializedName(value = "jobId")
        private long jobId;
        @SerializedName(value = "state")
        private JobState state;
        @SerializedName(value = "transactionId")
        private long transactionId;
        @SerializedName(value = "loadStartTimestamp")
        private long loadStartTimestamp;

        public LoadJobStateUpdateInfo(long jobId, JobState state, long transactionId, long loadStartTimestamp) {
            this.jobId = jobId;
            this.state = state;
            this.transactionId = transactionId;
            this.loadStartTimestamp = loadStartTimestamp;
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public long getLoadStartTimestamp() {
            return loadStartTimestamp;
        }

        @Override
        public String toString() {
            return GsonUtils.GSON.toJson(this);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static LoadJobStateUpdateInfo read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, LoadJobStateUpdateInfo.class);
        }
    }

    // unit: second
    protected long getTimeout() {
        return (long) jobProperties.get(LoadStmt.TIMEOUT_PROPERTY);
    }

    protected void setTimeout(long timeout) {
        jobProperties.put(LoadStmt.TIMEOUT_PROPERTY, timeout);
    }

    protected long getExecMemLimit() {
        return (long) jobProperties.get(LoadStmt.EXEC_MEM_LIMIT);
    }

    protected double getMaxFilterRatio() {
        return (double) jobProperties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY);
    }

    protected void setMaxFilterRatio(double maxFilterRatio) {
        jobProperties.put(LoadStmt.MAX_FILTER_RATIO_PROPERTY, maxFilterRatio);
    }

    protected boolean isStrictMode() {
        return (boolean) jobProperties.get(LoadStmt.STRICT_MODE);
    }

    protected boolean isPartialUpdate() {
        return (boolean) jobProperties.get(LoadStmt.PARTIAL_COLUMNS);
    }

    protected String getTimeZone() {
        return (String) jobProperties.get(LoadStmt.TIMEZONE);
    }

    public int getLoadParallelism() {
        return (int) jobProperties.get(LoadStmt.LOAD_PARALLELISM);
    }

    public int getSendBatchParallelism() {
        return (int) jobProperties.get(LoadStmt.SEND_BATCH_PARALLELISM);
    }

    public LoadTask.Priority getPriority() {
        return (LoadTask.Priority) jobProperties.get(LoadStmt.PRIORITY);
    }

    public boolean isSingleTabletLoadPerSink() {
        return (boolean) jobProperties.get(LoadStmt.LOAD_TO_SINGLE_TABLET);
    }

    public boolean useNewLoadScanNode() {
        return (boolean) jobProperties.getOrDefault(LoadStmt.USE_NEW_LOAD_SCAN_NODE, false);
    }

    // Return true if this job is finished for a long time
    public boolean isExpired(long currentTimeMs) {
        if (!isCompleted()) {
            return false;
        }
        long expireTime = Config.label_keep_max_second;
        if (jobType == EtlJobType.INSERT) {
            expireTime = Config.streaming_label_keep_max_second;
        }

        return (currentTimeMs - getFinishTimestamp()) / 1000 > expireTime;
    }

    public FailMsg getFailMsg() {
        return failMsg;
    }

    public EtlStatus getLoadingStatus() {
        return loadingStatus;
    }

    public LoadStatistic getLoadStatistic() {
        return loadStatistic;
    }
}
