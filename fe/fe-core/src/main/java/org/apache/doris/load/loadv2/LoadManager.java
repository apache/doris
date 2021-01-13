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
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.Load;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMiniLoadBeginRequest;
import org.apache.doris.thrift.TMiniLoadRequest;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
 *
 * The lock sequence:
 * Database.lock
 *   LoadManager.lock
 *     LoadJob.lock
 */
public class LoadManager implements Writable{
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
     * @param stmt
     * @throws DdlException
     */
    public void createLoadJobFromStmt(LoadStmt stmt) throws DdlException {
        Database database = checkDb(stmt.getLabel().getDbName());
        long dbId = database.getId();
        LoadJob loadJob = null;
        writeLock();
        try {
            if (stmt.getBrokerDesc().isMultiLoadBroker()) {
                if (!Catalog.getCurrentCatalog().getLoadInstance()
                        .isUncommittedLabel(dbId, stmt.getLabel().getLabelName())) {
                    throw new DdlException("label: " + stmt.getLabel().getLabelName() + " not found!") ;
                }

            } else {
                checkLabelUsed(dbId, stmt.getLabel().getLabelName());
                if (stmt.getBrokerDesc() == null && stmt.getResourceDesc() == null) {
                    throw new DdlException("LoadManager only support the broker and spark load.");
                }
                if (loadJobScheduler.isQueueFull()) {
                    throw new DdlException("There are more than " + Config.desired_max_waiting_jobs + " load jobs in waiting queue, "
                            + "please retry later.");
                }
            }
            loadJob = BulkLoadJob.fromLoadStmt(stmt);
            createLoadJob(loadJob);
        } finally {
            writeUnlock();
        }
        Catalog.getCurrentCatalog().getEditLog().logCreateLoadJob(loadJob);

        // The job must be submitted after edit log.
        // It guarantee that load job has not been changed before edit log.
        loadJobScheduler.submitJob(loadJob);
    }

    /**
     * This method will be invoked by streaming mini load.
     * It will begin the txn of mini load immediately without any scheduler .
     *
     * @param request
     * @return
     * @throws UserException
     */
    public long createLoadJobFromMiniLoad(TMiniLoadBeginRequest request) throws UserException {
        String cluster = SystemInfoService.DEFAULT_CLUSTER;
        if (request.isSetCluster()) {
            cluster = request.getCluster();
        }
        Database database = checkDb(ClusterNamespace.getFullName(cluster, request.getDb()));
        Table table = database.getTable(request.tbl);
        checkTable(database, request.getTbl());
        LoadJob loadJob = null;
        writeLock();
        try {
            loadJob = new MiniLoadJob(database.getId(), table.getId(), request);
            // call unprotectedExecute before adding load job. so that if job is not started ok, no need to add.
            // NOTICE(cmy): this order is only for Mini Load, because mini load's unprotectedExecute() only do beginTxn().
            // for other kind of load job, execute the job after adding job.
            // Mini load job must be executed before release write lock.
            // Otherwise, the duplicated request maybe get the transaction id before transaction of mini load is begun.
            loadJob.unprotectedExecute();
            createLoadJob(loadJob);
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.info("duplicate request for mini load. request id: {}, txn: {}", e.getDuplicatedRequestId(), e.getTxnId());
            return e.getTxnId();
        } catch (UserException e) {
            if (loadJob != null) {
                loadJob.cancelJobWithoutCheck(new FailMsg(CancelType.LOAD_RUN_FAIL, e.getMessage()), false,
                        false /* no need to write edit log, because createLoadJob log is not wrote yet */);
            }
            throw e;
        } finally {
            writeUnlock();
        }

        // The persistence of mini load must be the final step of create mini load.
        // After mini load was executed, the txn id has been set and state has been changed to loading.
        // Those two need to be record in persistence.
        Catalog.getCurrentCatalog().getEditLog().logCreateLoadJob(loadJob);
        return loadJob.getTransactionId();
    }

    /**
     * This method will be invoked by version1 of broker or hadoop load.
     * It is used to check the label of v1 and v2 at the same time.
     * Finally, the v1 of broker or hadoop load will belongs to load class.
     * Step1: lock the load manager
     * Step2: check the label in load manager
     * Step3: call the addLoadJob of load class
     *     Step3.1: lock the load
     *     Step3.2: check the label in load
     *     Step3.3: add the loadJob in load rather than load manager
     *     Step3.4: unlock the load
     * Step4: unlock the load manager
     * @param stmt
     * @param timestamp
     * @throws DdlException
     */
    public void createLoadJobV1FromStmt(LoadStmt stmt, EtlJobType jobType, long timestamp) throws DdlException {
        Database database = checkDb(stmt.getLabel().getDbName());
        writeLock();
        try {
            checkLabelUsed(database.getId(), stmt.getLabel().getLabelName());
            Catalog.getCurrentCatalog().getLoadInstance().addLoadJob(stmt, jobType, timestamp);
        } finally {
            writeUnlock();
        }
    }

    /**
     * This method will be invoked by non-streaming of mini load.
     * It is used to check the label of v1 and v2 at the same time.
     * Finally, the non-streaming mini load will belongs to load class.
     *
     * @param request
     * @return if: mini load is a duplicated load, return false.
     *         else: return true.
     * @throws DdlException
     */
    @Deprecated
    public boolean createLoadJobV1FromRequest(TMiniLoadRequest request) throws DdlException {
        String cluster = SystemInfoService.DEFAULT_CLUSTER;
        if (request.isSetCluster()) {
            cluster = request.getCluster();
        }
        Database database = checkDb(ClusterNamespace.getFullName(cluster, request.getDb()));
        writeLock();
        try {
            checkLabelUsed(database.getId(), request.getLabel());
            return Catalog.getCurrentCatalog().getLoadInstance().addMiniLoadJob(request);
        } finally {
            writeUnlock();
        }
    }

    public void createLoadJobV1FromMultiStart(String fullDbName, String label) throws DdlException {
        Database database = checkDb(fullDbName);
        writeLock();
        try {
            checkLabelUsed(database.getId(), label);
            Catalog.getCurrentCatalog().getLoadInstance()
                    .registerMiniLabel(fullDbName, label, System.currentTimeMillis());
        } finally {
            writeUnlock();
        }
    }

    public void replayCreateLoadJob(LoadJob loadJob) {
        createLoadJob(loadJob);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                         .add("msg", "replay create load job")
                         .build());
    }

    // add load job and also add to to callback factory
    private void createLoadJob(LoadJob loadJob) {
        addLoadJob(loadJob);
        // add callback before txn created, because callback will be performed on replay without txn begin
        // register txn state listener
        Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
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

    public void recordFinishedLoadJob(String label, String dbName, long tableId, EtlJobType jobType,
            long createTimestamp, String failMsg, String trackingUrl) throws MetaNotFoundException {

        // get db id
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("Database[" + dbName + "] does not exist");
        }

        LoadJob loadJob;
        switch (jobType) {
            case INSERT:
                loadJob = new InsertLoadJob(label, db.getId(), tableId, createTimestamp, failMsg, trackingUrl);
                break;
            default:
                return;
        }
        addLoadJob(loadJob);
        // persistent
        Catalog.getCurrentCatalog().getEditLog().logCreateLoadJob(loadJob);
    }

    public void cancelLoadJob(CancelLoadStmt stmt, boolean isAccurateMatch) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDb(stmt.getDbName());
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + stmt.getDbName());
        }

        // List of load jobs waiting to be cancelled
        List<LoadJob> loadJobs = Lists.newArrayList();
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }

            // get jobs by label
            List<LoadJob> matchLoadJobs = Lists.newArrayList();
            if (isAccurateMatch) {
                if (labelToLoadJobs.containsKey(stmt.getLabel())) {
                    matchLoadJobs.addAll(labelToLoadJobs.get(stmt.getLabel()));
                }
            } else {
                for (Map.Entry<String, List<LoadJob>> entry : labelToLoadJobs.entrySet()) {
                    if (entry.getKey().contains(stmt.getLabel())) {
                        matchLoadJobs.addAll(entry.getValue());
                    }
                }
            }

            if (matchLoadJobs.isEmpty()) {
                throw new DdlException("Load job does not exist");
            }

            // check state here
            List<LoadJob> uncompletedLoadJob = matchLoadJobs.stream().filter(entity -> !entity.isTxnDone())
                    .collect(Collectors.toList());
            if (uncompletedLoadJob.isEmpty()) {
                throw new DdlException("There is no uncompleted job which label " +
                        (isAccurateMatch ? "is " : "like ") + stmt.getLabel());
            }

            loadJobs.addAll(uncompletedLoadJob);
        } finally {
            readUnlock();
        }

        for (LoadJob loadJob : loadJobs) {
            try {
                loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
            } catch (DdlException e) {
                throw new DdlException("Cancel load job [" + loadJob.getId() + "] fail, " +
                        "label=[" + loadJob.getLabel() + "] failed msg=" + e.getMessage());
            }
        }
    }

    public void cancelLoadJob(CancelLoadStmt stmt) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDb(stmt.getDbName());
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + stmt.getDbName());
        }

        LoadJob loadJob = null;
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(db.getId());
            if (labelToLoadJobs == null) {
                throw new DdlException("Load job does not exist");
            }
            List<LoadJob> loadJobList = labelToLoadJobs.get(stmt.getLabel());
            if (loadJobList == null) {
                throw new DdlException("Load job does not exist");
            }
            Optional<LoadJob> loadJobOptional = loadJobList.stream().filter(entity -> !entity.isTxnDone()).findFirst();
            if (!loadJobOptional.isPresent()) {
                throw new DdlException("There is no uncompleted job which label is " + stmt.getLabel());
            }
            loadJob = loadJobOptional.get();
        } finally {
            readUnlock();
        }

        loadJob.cancelJob(new FailMsg(FailMsg.CancelType.USER_CANCEL, "user cancel"));
    }

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
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, operation.getId())
                         .add("operation", operation)
                         .add("msg", "replay end load job")
                         .build());
    }

    public void replayUpdateLoadJobStateInfo(LoadJob.LoadJobStateUpdateInfo info) {
        long jobId = info.getJobId();
        LoadJob job = idToLoadJob.get(jobId);
        if (job == null) {
            LOG.warn("replay update load job state failed. error: job not found, id: {}", jobId);
            return;
        }

        job.replayUpdateStateInfo(info);
    }

    public int getLoadJobNum(JobState jobState, long dbId) {
        readLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs == null) {
                return 0;
            }
            List<LoadJob> loadJobList = labelToLoadJobs.values().stream()
                    .flatMap(entity -> entity.stream()).collect(Collectors.toList());
            return (int) loadJobList.stream().filter(entity -> entity.getState() == jobState).count();
        } finally {
            readUnlock();
        }
    }

    public long getLoadJobNum(JobState jobState, EtlJobType jobType) {
        readLock();
        try {
            return idToLoadJob.values().stream().filter(j -> j.getState() == jobState && j.getJobType() == jobType).count();
        } finally {
            readUnlock();
        }
    }

    public void removeOldLoadJob() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            Iterator<Map.Entry<Long, LoadJob>> iter = idToLoadJob.entrySet().iterator();
            while (iter.hasNext()) {
                LoadJob job = iter.next().getValue();
                if (job.isCompleted()
                        && ((currentTimeMs - job.getFinishTimestamp()) / 1000 > Config.label_keep_max_second)) {
                    iter.remove();
                    dbIdToLabelToLoadJobs.get(job.getDbId()).get(job.getLabel()).remove(job);
                    if (job instanceof SparkLoadJob) {
                        ((SparkLoadJob) job).clearSparkLauncherLog();
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    // only for those jobs which transaction is not started
    public void processTimeoutJobs() {
        idToLoadJob.values().stream().forEach(entity -> entity.processTimeout());
    }

    // only for those jobs which have etl state, like SparkLoadJob
    public void processEtlStateJobs() {
        idToLoadJob.values().stream().filter(job -> (job.jobType == EtlJobType.SPARK && job.state == JobState.ETL))
                .forEach(job -> {
                    try {
                        ((SparkLoadJob) job).updateEtlStatus();
                    } catch (DataQualityException e) {
                        LOG.info("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, DataQualityException.QUALITY_FAIL_MSG),
                                                  true, true);
                    } catch (UserException e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.ETL_RUN_FAIL, e.getMessage()), true, true);
                    } catch (Exception e) {
                        LOG.warn("update load job etl status failed. job id: {}", job.getId(), e);
                    }
                });
    }

    // only for those jobs which load by PushTask
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
     * @param dbId used to filter jobs which belong to this db
     * @param labelValue used to filter jobs which's label is or like labelValue.
     * @param accurateMatch true: filter jobs which's label is labelValue. false: filter jobs which's label like itself.
     * @param statesValue used to filter jobs which's state within the statesValue set.
     * @return The result is the list of jobInfo.
     *     JobInfo is a List<Comparable> which includes the comparable object: jobId, label, state etc.
     *     The result is unordered.
     */
    public List<List<Comparable>> getLoadJobInfosByDb(long dbId, String labelValue,
                                                      boolean accurateMatch, Set<String> statesValue) {
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
                loadJobList.addAll(labelToLoadJobs.values()
                                           .stream().flatMap(Collection::stream).collect(Collectors.toList()));
            } else {
                // check label value
                if (accurateMatch) {
                    if (!labelToLoadJobs.containsKey(labelValue)) {
                        return loadJobInfos;
                    }
                    loadJobList.addAll(labelToLoadJobs.get(labelValue));
                } else {
                    // non-accurate match
                    for (Map.Entry<String, List<LoadJob>> entry : labelToLoadJobs.entrySet()) {
                        if (entry.getKey().contains(labelValue)) {
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
        loadJobScheduler.submitJob(idToLoadJob.values().stream().filter(
                loadJob -> loadJob.state == JobState.PENDING).collect(Collectors.toList()));
    }

    private void analyzeLoadJobs() {
        for (LoadJob loadJob : idToLoadJob.values()) {
            if (loadJob.getState() == JobState.PENDING) {
                loadJob.analyze();
            }
        }
    }

    private Database checkDb(String dbName) throws DdlException {
        // get db
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            LOG.warn("Database {} does not exist", dbName);
            throw new DdlException("Database[" + dbName + "] does not exist");
        }
        return db;
    }

    /**
     * Please don't lock any load lock before check table
     * @param database
     * @param tableName
     * @throws DdlException
     */
    private void checkTable(Database database, String tableName) throws DdlException {
        if (database.getTable(tableName) == null) {
            LOG.info("Table {} is not belongs to database {}", tableName, database.getFullName());
            throw new DdlException("Table[" + tableName + "] does not exist");
        }
    }

    /**
     * step1: if label has been used in old load jobs which belong to load class
     * step2: if label has been used in v2 load jobs
     *     step2.1: if label has been user in v2 load jobs, the create timestamp will be checked
     *
     * @param dbId
     * @param label
     * @throws LabelAlreadyUsedException throw exception when label has been used by an unfinished job.
     */
    private void checkLabelUsed(long dbId, String label)
            throws DdlException {
        // if label has been used in old load jobs
        Catalog.getCurrentCatalog().getLoadInstance().isLabelUsed(dbId, label);
        // if label has been used in v2 of load jobs
        if (dbIdToLabelToLoadJobs.containsKey(dbId)) {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(dbId);
            if (labelToLoadJobs.containsKey(label)) {
                List<LoadJob> labelLoadJobs = labelToLoadJobs.get(label);
                Optional<LoadJob> loadJobOptional =
                        labelLoadJobs.stream().filter(entity -> entity.getState() != JobState.CANCELLED).findFirst();
                if (loadJobOptional.isPresent()) {
                    LOG.warn("Failed to add load job when label {} has been used.", label);
                    throw new LabelAlreadyUsedException(label);
                }
            }
        }
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

    // If load job will be removed by cleaner later, it will not be saved in image.
    private boolean needSave(LoadJob loadJob) {
        if (!loadJob.isCompleted()) {
            return true;
        }

        long currentTimeMs = System.currentTimeMillis();
        if (loadJob.isCompleted() && ((currentTimeMs - loadJob.getFinishTimestamp()) / 1000 <= Config.label_keep_max_second)) {
            return true;
        }

        return false;
    }

    public void initJobProgress(Long jobId, TUniqueId loadId, Set<TUniqueId> fragmentIds,
            List<Long> relatedBackendIds) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.initLoadProgress(loadId, fragmentIds, relatedBackendIds);
        }
    }

    public void updateJobProgress(Long jobId, Long beId, TUniqueId loadId, TUniqueId fragmentId,
                                  long scannedRows, boolean isDone) {
        LoadJob job = idToLoadJob.get(jobId);
        if (job != null) {
            job.updateProgress(beId, loadId, fragmentId, scannedRows, isDone);
        }
    }

    @Deprecated
    // Deprecated in version 0.12
    // This method is only for bug fix. And should be call after image and edit log are replayed.
    public void fixLoadJobMetaBugs(GlobalTransactionMgr txnMgr) {
        for (LoadJob job : idToLoadJob.values()) {
            /*
             * Bug 1:
             * in previous implementation, there is a bug that when the job's corresponding transaction is
             * COMMITTED but not VISIBLE, the load job's state is LOADING, so that the job may be CANCELLED
             * by timeout checker, which is not right.
             * So here we will check each LOADING load jobs' txn status, if it is COMMITTED, change load job's
             * state to COMMITTED.
             * this method should be removed at next upgrading.
             * only mini load job will be in LOADING state when persist, because mini load job is executed before writing
             * edit log.
             */
            if (job.getState() == JobState.LOADING) {
                // unfortunately, transaction id in load job is also not persisted, so we have to traverse
                // all transactions to find it.
                TransactionState txn = txnMgr.getTransactionStateByCallbackIdAndStatus(job.getDbId(), job.getCallbackId(),
                        Sets.newHashSet(TransactionStatus.COMMITTED));
                if (txn != null) {
                    job.updateState(JobState.COMMITTED);
                    LOG.info("transfer load job {} state from LOADING to COMMITTED, because txn {} is COMMITTED."
                            + " label: {}, db: {}", job.getId(), txn.getTransactionId(), job.getLabel(), job.getDbId());
                    continue;
                }
            }

            /*
             * Bug 2:
             * There is bug in Doris version 0.10.15. When a load job in PENDING or LOADING
             * state was replayed from image (not through the edit log), we forgot to add
             * the corresponding callback id in the CallbackFactory. As a result, the
             * subsequent finish txn edit logs cannot properly finish the job during the
             * replay process. This results in that when the FE restarts, these load jobs
             * that should have been completed are re-entered into the pending state,
             * resulting in repeated submission load tasks.
             *
             * Those wrong images are unrecoverable, so that we have to cancel all load jobs
             * in PENDING or LOADING state when restarting FE, to avoid submit jobs
             * repeatedly.
             *
             * This code can be remove when upgrading from 0.11.x to future version.
             */
            if (job.getState() == JobState.LOADING || job.getState() == JobState.PENDING) {
                JobState prevState = job.getState();
                TransactionState txn = txnMgr.getTransactionStateByCallbackId(job.getDbId(), job.getCallbackId());
                if (txn != null) {
                    // the txn has already been committed or visible, change job's state to committed or finished
                    if (txn.getTransactionStatus() == TransactionStatus.COMMITTED) {
                        job.updateState(JobState.COMMITTED);
                        LOG.info("transfer load job {} state from {} to COMMITTED, because txn {} is COMMITTED",
                                job.getId(), prevState, txn.getTransactionId());
                    } else if (txn.getTransactionStatus() == TransactionStatus.VISIBLE) {
                        job.updateState(JobState.FINISHED);
                        LOG.info("transfer load job {} state from {} to FINISHED, because txn {} is VISIBLE",
                                job.getId(), prevState, txn.getTransactionId());
                    } else if (txn.getTransactionStatus() == TransactionStatus.ABORTED) {
                        job.cancelJobWithoutCheck(new FailMsg(CancelType.LOAD_RUN_FAIL, "fe restart"), false, false);
                        LOG.info("transfer load job {} state from {} to CANCELLED, because txn {} is ABORTED",
                                job.getId(), prevState, txn.getTransactionId());
                    } else {
                        // pending txn, do nothing
                    }
                    continue;
                }

                if (job.getJobType() == EtlJobType.MINI) {
                    // for mini load job, just set it as CANCELLED, because mini load is a synchronous load.
                    // it would be failed if FE restart.
                    job.cancelJobWithoutCheck(new FailMsg(CancelType.LOAD_RUN_FAIL, "fe restart"), false, false);
                    LOG.info("transfer mini load job {} state from {} to CANCELLED, because transaction status is unknown"
                                    + ". label: {}, db: {}",
                            job.getId(), prevState, job.getLabel(), job.getDbId());
                } else {
                    // txn is not found. here are 2 cases:
                    // 1. txn is not start yet, so we can just set job to CANCELLED, and user need to submit the job again.
                    // 2. because of the bug, txn is ABORTED of VISIBLE, and job is not finished. and txn is expired and
                    //    be removed from transaction manager. So we don't know this job is finished or cancelled.
                    //    in this case, the job should has been submitted long ago (otherwise the txn could not have been 
                    //    removed by expiration). 
                    // Without affecting the first case of job, we set the job finish time to be the same as the create time. 
                    // In this way, the second type of job will be automatically cleared after running removeOldLoadJob();

                    // use CancelType.UNKNOWN, so that we can set finish time to be the same as the create time
                    job.cancelJobWithoutCheck(new FailMsg(CancelType.TXN_UNKNOWN, "transaction status is unknown"), false, false);
                    LOG.info("finish load job {} from {} to CANCELLED, because transaction status is unknown. label: {}, db: {}, create: {}",
                            job.getId(), prevState, job.getLabel(), job.getDbId(), TimeUtils.longToTimeString(job.getCreateTimestamp()));
                }
            }
        }

        removeOldLoadJob();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        List<LoadJob> loadJobs = idToLoadJob.values().stream().filter(this::needSave).collect(Collectors.toList());

        out.writeInt(loadJobs.size());
        for (LoadJob loadJob : loadJobs) {
            loadJob.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            LoadJob loadJob = LoadJob.read(in);
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
                Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(loadJob);
            }
        }
    }
}
