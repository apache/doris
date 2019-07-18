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

import static org.apache.doris.load.FailMsg.CancelType.LOAD_RUN_FAIL;

import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.Load;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TMiniLoadBeginRequest;
import org.apache.doris.thrift.TMiniLoadRequest;

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
            checkLabelUsed(dbId, stmt.getLabel().getLabelName(), -1);
            if (stmt.getBrokerDesc() == null) {
                throw new DdlException("LoadManager only support the broker load.");
            }
            if (loadJobScheduler.isQueueFull()) {
                throw new DdlException("There are more then " + Config.desired_max_waiting_jobs + " load jobs in waiting queue, "
                                               + "please retry later.");
            }
            loadJob = BrokerLoadJob.fromLoadStmt(stmt);
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
        checkTable(database, request.getTbl());
        LoadJob loadJob = null;
        writeLock();
        try {
            checkLabelUsed(database.getId(), request.getLabel(), request.getCreate_timestamp());
            loadJob = new MiniLoadJob(database.getId(), request);
            createLoadJob(loadJob);
            // Mini load job must be executed before release write lock.
            // Otherwise, the duplicated request maybe get the transaction id before transaction of mini load is begun.
            loadJob.unprotectedExecute();
        } catch (DuplicatedRequestException e) {
            LOG.info(new LogBuilder(LogKey.LOAD_JOB, e.getDuplicatedRequestId())
                             .add("msg", "the duplicated request returns the txn id "
                                     + "which was created by the same mini load")
                             .build());
            return dbIdToLabelToLoadJobs.get(database.getId()).get(request.getLabel())
                    .stream().filter(entity -> entity.getState() != JobState.CANCELLED).findFirst()
                    .get().getTransactionId();
        } catch (UserException e) {
            loadJob.cancelJobWithoutCheck(new FailMsg(LOAD_RUN_FAIL, e.getMessage()), false);
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
            checkLabelUsed(database.getId(), stmt.getLabel().getLabelName(), -1);
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
    public boolean createLoadJobV1FromRequest(TMiniLoadRequest request) throws DdlException {
        String cluster = SystemInfoService.DEFAULT_CLUSTER;
        if (request.isSetCluster()) {
            cluster = request.getCluster();
        }
        Database database = checkDb(ClusterNamespace.getFullName(cluster, request.getDb()));
        writeLock();
        try {
            checkLabelUsed(database.getId(), request.getLabel(), -1);
            return Catalog.getCurrentCatalog().getLoadInstance().addLoadJob(request);
        } finally {
            writeUnlock();
        }
    }

    public void createLoadJobV1FromMultiStart(String fullDbName, String label) throws DdlException {
        Database database = checkDb(fullDbName);
        writeLock();
        try {
            checkLabelUsed(database.getId(), label, -1);
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
            long createTimestamp, String failMsg) throws MetaNotFoundException {

        // get db id
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("Database[" + dbName + "] does not exist");
        }

        LoadJob loadJob;
        switch (jobType) {
            case INSERT:
                loadJob = new InsertLoadJob(label, db.getId(), tableId, createTimestamp, failMsg);
                break;
            default:
                return;
        }
        addLoadJob(loadJob);
        // persistent
        Catalog.getCurrentCatalog().getEditLog().logCreateLoadJob(loadJob);
    }

    public void cancelLoadJob(CancelLoadStmt stmt) throws DdlException {
        Database db = Catalog.getInstance().getDb(stmt.getDbName());
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
            Optional<LoadJob> loadJobOptional = loadJobList.stream().filter(entity -> !entity.isCompleted()).findFirst();
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
        job.unprotectReadEndOperation(operation);
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, operation.getId())
                         .add("operation", operation)
                         .add("msg", "replay end load job")
                         .build());
    }

    public List<LoadJob> getLoadJobByState(JobState jobState) {
        return idToLoadJob.values().stream()
                .filter(entity -> entity.getState() == jobState)
                .collect(Collectors.toList());
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
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void processTimeoutJobs() {
        idToLoadJob.values().stream().forEach(entity -> entity.processTimeout());
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

    public void submitJobs() {
        loadJobScheduler.submitJob(idToLoadJob.values().stream().filter(
                loadJob -> loadJob.state == JobState.PENDING).collect(Collectors.toList()));
    }

    private Map<Long, LoadJob> getIdToLoadJobs() {
        return idToLoadJob;
    }

    private Database checkDb(String dbName) throws DdlException {
        // get db
        Database db = Catalog.getInstance().getDb(dbName);
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
        database.readLock();
        try {
            if (database.getTable(tableName) == null) {
                LOG.info("Table {} is not belongs to database {}", tableName, database.getFullName());
                throw new DdlException("Table[" + tableName + "] does not exist");
            }
        } finally {
            database.readUnlock();
        }
    }

    /**
     * step1: if label has been used in old load jobs which belong to load class
     * step2: if label has been used in v2 load jobs
     *     step2.1: if label has been user in v2 load jobs, the create timestamp will be checked
     *
     * @param dbId
     * @param label
     * @param createTimestamp the create timestamp of stmt of request
     * @throws LabelAlreadyUsedException throw exception when label has been used by an unfinished job.
     */
    private void checkLabelUsed(long dbId, String label, long createTimestamp)
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
                    LoadJob loadJob = loadJobOptional.get();
                    if (loadJob.getCreateTimestamp() == createTimestamp) {
                        throw new DuplicatedRequestException(String.valueOf(loadJob.getId()),
                                                             "The request is duplicated with " + loadJob.getId());
                    }
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

    @Override
    public void write(DataOutput out) throws IOException {
        List<LoadJob> loadJobs = idToLoadJob.values().stream().filter(this::needSave).collect(Collectors.toList());

        out.writeInt(loadJobs.size());
        for (LoadJob loadJob : loadJobs) {
            loadJob.write(out);
        }
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

    @Override
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
        }
    }
}
