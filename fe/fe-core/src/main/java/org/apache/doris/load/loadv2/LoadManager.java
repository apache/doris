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

import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.CleanLabelStmt;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.Load;
import org.apache.doris.persist.CleanLabelOperationLog;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.DatabaseTransactionMgr;
import org.apache.doris.transaction.TransactionState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * The broker and mini load jobs(v2) are included in this class.
 * The lock sequence:
 * Database.lock
 * LoadManager.lock
 * LoadJob.lock
 */
public class LoadManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(LoadManager.class);

    private Map<Long, LoadJob> idToLoadJob = Maps.newConcurrentMap();
    private Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Maps.newConcurrentMap();
    private LoadJobScheduler loadJobScheduler;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LoadManager(LoadJobScheduler loadJobScheduler) {
        this.loadJobScheduler = loadJobScheduler;
    }

    /**
     * This method will be invoked by the broker load(v2) now.
     */
    public long createLoadJobFromStmt(LoadStmt stmt) throws DdlException {
        Database database = checkDb(stmt.getLabel().getDbName());
        long dbId = database.getId();
        LoadJob loadJob = null;
        writeLock();
        try {
            if (stmt.getBrokerDesc() != null && stmt.getBrokerDesc().isMultiLoadBroker()) {
                if (!Env.getCurrentEnv().getLoadInstance()
                        .isUncommittedLabel(dbId, stmt.getLabel().getLabelName())) {
                    throw new DdlException("label: " + stmt.getLabel().getLabelName() + " not found!");
                }
            } else {
                checkLabelUsed(dbId, stmt.getLabel().getLabelName());
                if (stmt.getBrokerDesc() == null && stmt.getResourceDesc() == null) {
                    throw new DdlException("LoadManager only support the broker and spark load.");
                }
                if (unprotectedGetUnfinishedJobNum() >= Config.desired_max_waiting_jobs) {
                    throw new DdlException(
                            "There are more than " + Config.desired_max_waiting_jobs
                                    + " unfinished load jobs, please retry later. "
                                    + "You can use `SHOW LOAD` to view submitted jobs");
                }
            }

            loadJob = BulkLoadJob.fromLoadStmt(stmt);
            createLoadJob(loadJob);
        } finally {
            writeUnlock();
        }
        Env.getCurrentEnv().getEditLog().logCreateLoadJob(loadJob);

        // The job must be submitted after edit log.
        // It guarantee that load job has not been changed before edit log.
        loadJobScheduler.submitJob(loadJob);
        return loadJob.getId();
    }

    private long unprotectedGetUnfinishedJobNum() {
        return idToLoadJob.values().stream()
                .filter(j -> (j.getState() != JobState.FINISHED && j.getState() != JobState.CANCELLED)).count();
    }

    /**
     * This method will be invoked by version1 of broker or hadoop load.
     * It is used to check the label of v1 and v2 at the same time.
     * Finally, the v1 of broker or hadoop load will belongs to load class.
     * Step1: lock the load manager
     * Step2: check the label in load manager
     * Step3: call the addLoadJob of load class
     * Step3.1: lock the load
     * Step3.2: check the label in load
     * Step3.3: add the loadJob in load rather than load manager
     * Step3.4: unlock the load
     * Step4: unlock the load manager
     *
     */
    public void createLoadJobV1FromStmt(LoadStmt stmt, EtlJobType jobType, long timestamp) throws DdlException {
        Database database = checkDb(stmt.getLabel().getDbName());
        writeLock();
        try {
            checkLabelUsed(database.getId(), stmt.getLabel().getLabelName());
            Env.getCurrentEnv().getLoadInstance().addLoadJob(stmt, jobType, timestamp);
        } finally {
            writeUnlock();
        }
    }

    /**
     * MultiLoadMgr use.
     **/
    public void createLoadJobV1FromMultiStart(String fullDbName, String label) throws DdlException {
        Database database = checkDb(fullDbName);
        writeLock();
        try {
            checkLabelUsed(database.getId(), label);
            Env.getCurrentEnv().getLoadInstance()
                    .registerMiniLabel(fullDbName, label, System.currentTimeMillis());
        } finally {
            writeUnlock();
        }
    }

    public void replayCreateLoadJob(LoadJob loadJob) {
        createLoadJob(loadJob);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId()).add("msg", "replay create load job").build());
    }

    // add load job and also add to callback factory
    private void createLoadJob(LoadJob loadJob) {
        if (loadJob.isExpired(System.currentTimeMillis())) {
            // This can happen in replay logic.
            return;
        }
        addLoadJob(loadJob);
        // add callback before txn if load job is uncompleted,
        // because callback will be performed on replay without txn begin
        // register txn state listener
        if (!loadJob.isCompleted()) {
            Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
        }
    }

    private void addLoadJob(LoadJob loadJob) {
        idToLoadJob.put(loadJob.getId(), loadJob);
        long dbId = loadJob.getDbId();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            dbIdToLabelToLoadJobs.put(loadJob.getDbId(), new ConcurrentHashMap<>());
        }
        Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
        if (!labelToLoadJobs.containsKey(loadJob.getLabel())) {
            labelToLoadJobs.put(loadJob.getLabel(), new ArrayList<>());
        }
        labelToLoadJobs.get(loadJob.getLabel()).add(loadJob);
    }

    /**
     * Record finished load job by editLog.
     **/
    public void recordFinishedLoadJob(String label, long transactionId, String dbName, long tableId, EtlJobType jobType,
            long createTimestamp, String failMsg, String trackingUrl) throws MetaNotFoundException {

        // get db id
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);

        LoadJob loadJob;
        switch (jobType) {
            case INSERT:
                loadJob = new InsertLoadJob(label, transactionId, db.getId(), tableId, createTimestamp, failMsg,
                        trackingUrl);
                break;
            default:
                return;
        }
        addLoadJob(loadJob);
        // persistent
        Env.getCurrentEnv().getEditLog().logCreateLoadJob(loadJob);
    }

    /**
     * Match need cancel loadJob by stmt.
     **/
    @VisibleForTesting
    public static void addNeedCancelLoadJob(CancelLoadStmt stmt, List<LoadJob> loadJobs, List<LoadJob> matchLoadJobs)
            throws AnalysisException {
        String label = stmt.getLabel();
        String state = stmt.getState();
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern(label,
                CaseSensibility.LABEL.getCaseSensibility());
        matchLoadJobs.addAll(
            loadJobs.stream()
                    .filter(job -> job.getState() != JobState.CANCELLED)
                    .filter(job -> {
                        if (stmt.getOperator() != null) {
                            // compound
                            boolean labelFilter =
                                    label.contains("%") ? matcher.match(job.getLabel())
                                            : job.getLabel().equalsIgnoreCase(label);
                            boolean stateFilter = job.getState().name().equalsIgnoreCase(state);
                            return Operator.AND.equals(stmt.getOperator()) ? labelFilter && stateFilter :
                                    labelFilter || stateFilter;
                        }
                        if (StringUtils.isNotEmpty(label)) {
                            return label.contains("%") ? matcher.match(job.getLabel())
                                    : job.getLabel().equalsIgnoreCase(label);
                        }
                        if (StringUtils.isNotEmpty(state)) {
                            return job.getState().name().equalsIgnoreCase(state);
                        }
                        return false;
                    }).collect(Collectors.toList())
        );
    }

    /**
     * Cancel load job by stmt.
     **/
    public void cancelLoadJob(CancelLoadStmt stmt) throws DdlException, AnalysisException {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(stmt.getDbName());
        // List of load jobs waiting to be cancelled
        List<LoadJob> matchLoadJobs = Lists.newArrayList();
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            addNeedCancelLoadJob(stmt,
                    labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                    matchLoadJobs);
            if (matchLoadJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }
            // check state here
            List<LoadJob> uncompletedLoadJob =
                    matchLoadJobs.stream().filter(entity -> !entity.isTxnDone()).collect(Collectors.toList());
            if (uncompletedLoadJob.isEmpty()) {
                throw new DdlException("There is no uncompleted job");
            }
        } finally {
            readUnlock();
        }
        for (LoadJob loadJob : matchLoadJobs) {
            try {
                loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
            } catch (DdlException e) {
                throw new DdlException(
                        "Cancel load job [" + loadJob.getId() + "] fail, " + "label=[" + loadJob.getLabel()
                                +
                                "] failed msg=" + e.getMessage());
            }
        }
    }

    /**
     * Replay end load job.
     **/
    public void replayEndLoadJob(LoadJobFinalOperation operation) {
        LoadJob job = idToLoadJob.get(operation.getId());
        if (job == null) {
            // This should not happen.
            // Last time I found that when user submit a job with already used label, an END_LOAD_JOB edit log
            // will be wrote but the job is not added to 'idToLoadJob', so this job here we got will be null.
            // And this bug has been fixed.
            // Just add a log here to observe.
            LOG.warn("job does not exist when replaying end load job edit log: {}", operation);
            return;
        }
        job.unprotectReadEndOperation(operation);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, operation.getId()).add("operation", operation)
                .add("msg", "replay end load job").build());
    }

    /**
     * Replay update load job.
     **/
    public void replayUpdateLoadJobStateInfo(LoadJob.LoadJobStateUpdateInfo info) {
        long jobId = info.getJobId();
        LoadJob job = idToLoadJob.get(jobId);
        if (job == null) {
            LOG.warn("replay update load job state failed. error: job not found, id: {}", jobId);
            return;
        }

        job.replayUpdateStateInfo(info);
    }

    /**
     * Get load job num, used by proc.
     **/
    public int getLoadJobNum(JobState jobState, long dbId) {
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs == null) {
                return 0;
            }
            List<LoadJob> loadJobList =
                    labelToLoadJobs.values().stream().flatMap(entity -> entity.stream()).collect(Collectors.toList());
            return (int) loadJobList.stream().filter(entity -> entity.getState() == jobState).count();
        } finally {
            readUnlock();
        }
    }


    /**
     * Get load job num, used by metric.
     **/
    public long getLoadJobNum(JobState jobState, EtlJobType jobType) {
        readLock();
        try {
            return idToLoadJob.values().stream().filter(j -> j.getState() == jobState && j.getJobType() == jobType)
                    .count();
        } finally {
            readUnlock();
        }
    }

    /**
     * Remove old load job.
     **/
    public void removeOldLoadJob() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, LoadJob>> iter = idToLoadJob.entrySet().iterator();
            while (iter.hasNext()) {
                LoadJob job = iter.next().getValue();
                if (job.isExpired(currentTimeMs)) {
                    iter.remove();
                    Map<String, List<LoadJob>> map = dbIdToLabelToLoadJobs.get(job.getDbId());
                    List<LoadJob> list = map.get(job.getLabel());
                    list.remove(job);
                    if (job instanceof SparkLoadJob) {
                        ((SparkLoadJob) job).clearSparkLauncherLog();
                    }
                    if (list.isEmpty()) {
                        map.remove(job.getLabel());
                    }
                    if (map.isEmpty()) {
                        dbIdToLabelToLoadJobs.remove(job.getDbId());
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Only for those jobs which have etl state, like SparkLoadJob.
     **/
    public void processEtlStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.ETL))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateEtlStatus();
                    } catch (DataQualityException e) {
                        LOG.info("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                                DataQualityException.QUALITY_FAIL_MSG), true, true);
                    } catch (UserException e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    /**
     * Only for those jobs which load by PushTask.
     **/
    public void processLoadingStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.LOADING))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateLoadingStatus();
                    } catch (UserException e) {
                        LOG.warn("update load job loading status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.LOAD_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job loading status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    /**
     * This method will return the jobs info which can meet the condition of input param.
     *
     * @param dbId          used to filter jobs which belong to this db
     * @param labelValue    used to filter jobs which's label is or like labelValue.
     * @param accurateMatch true: filter jobs which's label is labelValue. false: filter jobs which's label like itself.
     * @param statesValue   used to filter jobs which's state within the statesValue set.
     * @return The result is the list of jobInfo.
     *         JobInfo is a list which includes the comparable object: jobId, label, state etc.
     *         The result is unordered.
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String labelValue, boolean accurateMatch,
            Set<String> statesValue) throws AnalysisException {
        LinkedList<List<Comparable>> loadJobInfos = new LinkedList<List<Comparable>>();
        if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
            return loadJobInfos;
        }

        Set<JobState> states = Sets.newHashSet();
        if (statesValue == null || statesValue.size() == 0) {
            states.addAll(EnumSet.allOf(JobState.class));
        } else {
            for (String stateValue : statesValue) {
                try {
                    states.add(JobState.valueOf(stateValue));
                } catch (IllegalArgumentException e) {
                    // ignore this state
                }
            }
        }

        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            List<LoadJob> loadJobList = Lists.newArrayList();
            if (Strings.isNullOrEmpty(labelValue)) {
                loadJobList.addAll(
                        labelToLoadJobs.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
            } else {
                // check label value
                if (accurateMatch) {
                    if (!labelToLoadJobs.containsKey(labelValue)) {
                        return loadJobInfos;
                    }
                    loadJobList.addAll(labelToLoadJobs.get(labelValue));
                } else {
                    // non-accurate match
                    PatternMatcher matcher =
                            PatternMatcherWrapper.createMysqlPattern(labelValue,
                                    CaseSensibility.LABEL.getCaseSensibility());
                    for (Map.Entry<String, List<LoadJob>> entry : labelToLoadJobs.entrySet()) {
                        if (matcher.match(entry.getKey())) {
                            loadJobList.addAll(entry.getValue());
                        }
                    }
                }
            }

            // check state
            for (LoadJob loadJob : loadJobList) {
                try {
                    if (!states.contains(loadJob.getState())) {
                        continue;
                    }
                    // add load job info
                    loadJobInfos.add(loadJob.getShowInfo());
                } catch (DdlException e) {
                    continue;
                }
            }
            return loadJobInfos;
        } finally {
            readUnlock();
        }
    }

    /**
     * Get load job info.
     **/
    public void getLoadJobInfo(Load.JobInfo info) throws DdlException {
        String fullDbName = ClusterNamespace.getFullName(info.clusterName, info.dbName);
        info.dbName = fullDbName;
        Database database = checkDb(info.dbName);
        readLock();
        try {
            // find the latest load job by info
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(database.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("No jobs belong to database(" + info.dbName + ")");
            }
            List<LoadJob> loadJobList = labelToLoadJobs.get(info.label);
            if (loadJobList == null || loadJobList.isEmpty()) {
                throw new DdlException("Unknown job(" + info.label + ")");
            }

            LoadJob loadJob = loadJobList.get(loadJobList.size() - 1);
            loadJob.getJobInfo(info);
        } finally {
            readUnlock();
        }
    }

    public LoadJob getLoadJob(long jobId) {
        return idToLoadJob.get(jobId);
    }

    public void prepareJobs() {
        analyzeLoadJobs();
        submitJobs();
    }

    private void submitJobs() {
        loadJobScheduler.submitJob(idToLoadJob.values().stream().filter(loadJob -> loadJob.state == JobState.PENDING)
                .collect(Collectors.toList()));
    }

    private void analyzeLoadJobs() {
        for (LoadJob loadJob : idToLoadJob.values()) {
            if (loadJob.getState() == JobState.PENDING) {
                loadJob.analyze();
            }
        }
    }

    private Database checkDb(String dbName) throws DdlException {
        return Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
    }

    /**
     * step1: if label has been used in old load jobs which belong to load class.
     * step2: if label has been used in v2 load jobs.
     * step2.1: if label has been user in v2 load jobs, the create timestamp will be checked.
     *
     * @throws LabelAlreadyUsedException throw exception when label has been used by an unfinished job.
     */
    private void checkLabelUsed(long dbId, String label) throws DdlException {
        // if label has been used in old load jobs
        Env.getCurrentEnv().getLoadInstance().isLabelUsed(dbId, label);
        // if label has been used in v2 of load jobs
        if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                Optional<LoadJob> loadJobOptional = labelLoadJobs.stream()
                        .filter(entity -> entity.getState() != JobState.CANCELLED).findFirst();
                if (loadJobOptional.isPresent()) {
                    LOG.warn("Failed to add load job when label {} has been used.", label);
                    throw new LabelAlreadyUsedException(label);
                }
            }
        }
    }

    public void cleanLabel(CleanLabelStmt stmt) throws DdlException {
        String dbName = stmt.getDb();
        String label = stmt.getLabel();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        cleanLabelInternal(db.getId(), label, false);
    }

    public void replayCleanLabel(CleanLabelOperationLog log) {
        cleanLabelInternal(log.getDbId(), log.getLabel(), true);
    }

    /**
     * Clean the label with given database and label
     * It will only remove the load jobs which are already done.
     * 1. Remove from LoadManager
     * 2. Remove from DatabaseTransactionMgr
     *
     * @param dbId
     * @param label
     * @param isReplay
     */
    private void cleanLabelInternal(long dbId, String label, boolean isReplay) {
        // 1. Remove from LoadManager
        int counter = 0;
        writeLock();
        try {
            if (!dbIdToLabelToLoadJobs.containsKey(dbId)) {
                // no label in this db, just return
                return;
            }
            Map<String, List<LoadJob>> labelToJob = dbIdToLabelToLoadJobs.get(dbId);
            if (Strings.isNullOrEmpty(label)) {
                // clean all labels in this db
                Iterator<Map.Entry<String, List<LoadJob>>> iter = labelToJob.entrySet().iterator();
                while (iter.hasNext()) {
                    List<LoadJob> jobs = iter.next().getValue();
                    Iterator<LoadJob> innerIter = jobs.iterator();
                    while (innerIter.hasNext()) {
                        LoadJob job = innerIter.next();
                        if (!job.isCompleted()) {
                            continue;
                        }
                        innerIter.remove();
                        idToLoadJob.remove(job.getId());
                        ++counter;
                    }
                    if (jobs.isEmpty()) {
                        iter.remove();
                    }
                }
            } else {
                List<LoadJob> jobs = labelToJob.get(label);
                if (jobs == null) {
                    // no job for this label, just return
                    return;
                }
                Iterator<LoadJob> iter = jobs.iterator();
                while (iter.hasNext()) {
                    LoadJob job = iter.next();
                    if (!job.isCompleted()) {
                        continue;
                    }
                    iter.remove();
                    idToLoadJob.remove(job.getId());
                    ++counter;
                }
                if (jobs.isEmpty()) {
                    labelToJob.remove(label);
                }
            }
        } finally {
            writeUnlock();
        }
        LOG.info("clean {} labels on db {} with label '{}' in load mgr.", counter, dbId, label);

        // 2. Remove from DatabaseTransactionMgr
        try {
            DatabaseTransactionMgr dbTxnMgr = Env.getCurrentGlobalTransactionMgr().getDatabaseTransactionMgr(dbId);
            dbTxnMgr.cleanLabel(label);
        } catch (AnalysisException e) {
            // just ignore, because we don't want to throw any exception here.
        }

        // 3. Log
        if (!isReplay) {
            CleanLabelOperationLog log = new CleanLabelOperationLog(dbId, label);
            Env.getCurrentEnv().getEditLog().logCleanLabel(log);
        }
        LOG.info("finished to clean label on db {} with label {}. is replay: {}", dbId, label, isReplay);
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * Init.
     **/
    public void initJobProgress(Long jobId, TUniqueId loadId, Set<TUniqueId> fragmentIds,
            List<Long> relatedBackendIds) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.initLoadProgress(loadId, fragmentIds, relatedBackendIds);
        }
    }

    /**
     * Update.
     **/
    public void updateJobProgress(Long jobId, Long beId, TUniqueId loadId, TUniqueId fragmentId, long scannedRows,
            long scannedBytes, boolean isDone) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.updateProgress(beId, loadId, fragmentId, scannedRows, scannedBytes, isDone);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        long currentTimeMs = System.currentTimeMillis();
        List<LoadJob> loadJobs =
                idToLoadJob.values().stream().filter(t -> !t.isExpired(currentTimeMs)).collect(Collectors.toList());

        out.writeInt(loadJobs.size());
        for (LoadJob loadJob : loadJobs) {
            loadJob.write(out);
        }
    }

    /**
     * Read from file.
     **/
    public void readFields(DataInput in) throws IOException {
        long currentTimeMs = System.currentTimeMillis();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            LoadJob loadJob = LoadJob.read(in);
            if (loadJob.isExpired(currentTimeMs)) {
                continue;
            }

            if (loadJob.getJobType() == EtlJobType.MINI) {
                // This is a bug fix. the mini load job should not with state LOADING.
                if (loadJob.getState() == JobState.LOADING) {
                    LOG.warn("skip mini load job {} in db {} with LOADING state", loadJob.getId(), loadJob.getDbId());
                    continue;
                }

                if (loadJob.getState() == JobState.PENDING) {
                    // bad case. When a mini load job is created and then FE restart.
                    // the job will be in PENDING state forever.
                    // This is a temp solution to remove these jobs.
                    // And the mini load job should be deprecated in Doris v1.1
                    TransactionState state = Env.getCurrentEnv().getGlobalTransactionMgr()
                            .getTransactionState(loadJob.getDbId(), loadJob.getTransactionId());
                    if (state == null) {
                        LOG.warn("skip mini load job {} in db {} with PENDING state and with txn: {}", loadJob.getId(),
                                loadJob.getDbId(), loadJob.getTransactionId());
                        continue;
                    }
                }
            }
            idToLoadJob.put(loadJob.getId(), loadJob);
            Map<String, List<LoadJob>> map = dbIdToLabelToLoadJobs.get(loadJob.getDbId());
            if (map == null) {
                map = Maps.newConcurrentMap();
                dbIdToLabelToLoadJobs.put(loadJob.getDbId(), map);
            }

            List<LoadJob> jobs = map.get(loadJob.getLabel());
            if (jobs == null) {
                jobs = Lists.newArrayList();
                map.put(loadJob.getLabel(), jobs);
            }
            jobs.add(loadJob);
            // The callback of load job which is replayed by image need to be registered in callback factory.
            // The commit and visible txn will callback the unfinished load job.
            // Otherwise, the load job always does not be completed while the txn is visible.
            if (!loadJob.isCompleted()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
            }
        }
    }
}
