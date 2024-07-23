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

import org.apache.doris.analysis.AlterRoutineLoadStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.PauseRoutineLoadStmt;
import org.apache.doris.analysis.ResumeRoutineLoadStmt;
import org.apache.doris.analysis.StopRoutineLoadStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RoutineLoadManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadManager.class);

    // Long is beId, integer is the size of tasks in be
    private Map<Long, Integer> beIdToMaxConcurrentTasks = Maps.newHashMap();

    // routine load job meta
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
    private Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();

    private ConcurrentHashMap<Long, Long> multiLoadTaskTxnIdToRoutineLoadJobId = new ConcurrentHashMap<>();

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

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

    public RoutineLoadManager() {
    }

    public void addMultiLoadTaskTxnIdToRoutineLoadJobId(long txnId, long routineLoadJobId) {
        multiLoadTaskTxnIdToRoutineLoadJobId.put(txnId, routineLoadJobId);
    }

    public RoutineLoadJob getRoutineLoadJobByMultiLoadTaskTxnId(long txnId) {
        long routineLoadJobId = multiLoadTaskTxnIdToRoutineLoadJobId.get(txnId);
        if (routineLoadJobId == 0) {
            return null;
        }
        return idToRoutineLoadJob.get(routineLoadJobId);
    }

    public void removeMultiLoadTaskTxnIdToRoutineLoadJobId(long txnId) {
        multiLoadTaskTxnIdToRoutineLoadJobId.remove(txnId);
    }

    public void updateBeIdToMaxConcurrentTasks() {
        beIdToMaxConcurrentTasks = Env.getCurrentSystemInfo().getAllBackendIds(true).stream().collect(
                Collectors.toMap(beId -> beId, beId -> Config.max_routine_load_task_num_per_be));
    }

    // this is not real-time number
    public int getTotalMaxConcurrentTaskNum() {
        return beIdToMaxConcurrentTasks.values().stream().mapToInt(i -> i).sum();
    }

    // return the map of be id -> running tasks num
    private Map<Long, Integer> getBeCurrentTasksNumMap() {
        Map<Long, Integer> beCurrentTaskNumMap = Maps.newHashMap();
        for (RoutineLoadJob routineLoadJob : getRoutineLoadJobByState(
                Sets.newHashSet(RoutineLoadJob.JobState.RUNNING))) {
            Map<Long, Integer> jobBeCurrentTasksNumMap = routineLoadJob.getBeCurrentTasksNumMap();
            for (Map.Entry<Long, Integer> entry : jobBeCurrentTasksNumMap.entrySet()) {
                if (beCurrentTaskNumMap.containsKey(entry.getKey())) {
                    beCurrentTaskNumMap.put(entry.getKey(), beCurrentTaskNumMap.get(entry.getKey()) + entry.getValue());
                } else {
                    beCurrentTaskNumMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return beCurrentTaskNumMap;

    }

    // cloud override
    public void createRoutineLoadJob(CreateRoutineLoadStmt createRoutineLoadStmt)
            throws UserException {
        // check load auth
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME,
                createRoutineLoadStmt.getDBName(),
                createRoutineLoadStmt.getTableName(),
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    createRoutineLoadStmt.getDBName(),
                    createRoutineLoadStmt.getDBName() + ": " + createRoutineLoadStmt.getTableName());
        }

        RoutineLoadJob routineLoadJob = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(createRoutineLoadStmt.getTypeName());
        switch (type) {
            case KAFKA:
                routineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
                break;
            default:
                throw new UserException("Unknown data source type: " + type);
        }

        routineLoadJob.setOrigStmt(createRoutineLoadStmt.getOrigStmt());
        routineLoadJob.setComment(createRoutineLoadStmt.getComment());
        addRoutineLoadJob(routineLoadJob, createRoutineLoadStmt.getDBName(),
                    createRoutineLoadStmt.getTableName());
    }

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob, String dbName, String tableName)
                    throws UserException {
        writeLock();
        try {
            // check if db.routineLoadName has been used
            if (isNameUsed(routineLoadJob.getDbId(), routineLoadJob.getName())) {
                throw new DdlException("Name " + routineLoadJob.getName() + " already used in db "
                        + dbName);
            }
            if (getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE,
                    RoutineLoadJob.JobState.RUNNING, RoutineLoadJob.JobState.PAUSED)).size()
                    > Config.max_routine_load_job_num) {
                throw new DdlException("There are more than " + Config.max_routine_load_job_num
                        + " routine load jobs are running. exceed limit.");
            }

            unprotectedAddJob(routineLoadJob);
            Env.getCurrentEnv().getEditLog().logCreateRoutineLoadJob(routineLoadJob);
        } finally {
            writeUnlock();
        }

        LOG.info("create routine load job: id: {}, job name: {}, db name: {}, table name: {}",
                 routineLoadJob.getId(), routineLoadJob.getName(), dbName, tableName);
    }

    private void unprotectedAddJob(RoutineLoadJob routineLoadJob) {
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);

        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob
                .computeIfAbsent(routineLoadJob.getDbId(), k -> Maps.newConcurrentMap());
        List<RoutineLoadJob> routineLoadJobList = nameToRoutineLoadJob
                .computeIfAbsent(routineLoadJob.getName(), k -> Lists.newArrayList());
        routineLoadJobList.add(routineLoadJob);
        // add txn state callback in factory
        Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(routineLoadJob);
    }

    // TODO(ml): Idempotency
    private boolean isNameUsed(Long dbId, String name) {
        if (dbToNameToRoutineLoadJob.containsKey(dbId)) {
            Map<String, List<RoutineLoadJob>> labelToRoutineLoadJob = dbToNameToRoutineLoadJob.get(dbId);
            if (labelToRoutineLoadJob.containsKey(name)) {
                List<RoutineLoadJob> routineLoadJobList = labelToRoutineLoadJob.get(name);
                Optional<RoutineLoadJob> optional = routineLoadJobList.stream()
                        .filter(entity -> entity.getName().equals(name))
                        .filter(entity -> !entity.getState().isFinalState()).findFirst();
                return optional.isPresent();
            }
        }
        return false;
    }

    public RoutineLoadJob checkPrivAndGetJob(String dbName, String jobName)
            throws MetaNotFoundException, DdlException, AnalysisException {
        RoutineLoadJob routineLoadJob = getJob(dbName, jobName);
        if (routineLoadJob == null) {
            throw new DdlException("There is not operable routine load job with name " + jobName);
        }
        // check auth
        String dbFullName;
        String tableName;
        try {
            dbFullName = routineLoadJob.getDbFullName();
            tableName = routineLoadJob.getTableName();
        } catch (MetaNotFoundException e) {
            throw new DdlException("The metadata of job has been changed. The job will be cancelled automatically", e);
        }
        if (routineLoadJob.isMultiTable()) {
            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                    InternalCatalog.INTERNAL_CATALOG_NAME,
                    dbFullName,
                    PrivPredicate.LOAD)) {
                // todo add new error code
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbFullName);
            }
            return routineLoadJob;
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                InternalCatalog.INTERNAL_CATALOG_NAME,
                dbFullName,
                tableName,
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbFullName + ": " + tableName);
        }
        return routineLoadJob;
    }

    // get all jobs which state is not in final state from specified database
    public List<RoutineLoadJob> checkPrivAndGetAllJobs(String dbName)
            throws MetaNotFoundException, DdlException {

        List<RoutineLoadJob> result = Lists.newArrayList();
        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
        long dbId = database.getId();
        Map<String, List<RoutineLoadJob>> jobMap = dbToNameToRoutineLoadJob.get(dbId);
        if (jobMap == null) {
            // return empty result
            return result;
        }

        for (List<RoutineLoadJob> jobs : jobMap.values()) {
            for (RoutineLoadJob job : jobs) {
                if (!job.getState().isFinalState()) {
                    String tableName = job.getTableName();
                    if (!job.isMultiTable() && !Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                                    tableName, PrivPredicate.LOAD)) {
                        continue;
                    }
                    result.add(job);
                }
            }
        }

        return result;
    }

    public void pauseRoutineLoadJob(PauseRoutineLoadStmt pauseRoutineLoadStmt)
            throws UserException {
        List<RoutineLoadJob> jobs = Lists.newArrayList();
        // it needs lock when getting routine load job,
        // otherwise, it may cause the editLog out of order in the following scenarios:
        // thread A: create job and record job meta
        // thread B: change job state and persist in editlog according to meta
        // thread A: persist in editlog
        // which will cause the null pointer exception when replaying editLog
        readLock();
        try {
            if (pauseRoutineLoadStmt.isAll()) {
                jobs = checkPrivAndGetAllJobs(pauseRoutineLoadStmt.getDbFullName());
            } else {
                RoutineLoadJob routineLoadJob = checkPrivAndGetJob(pauseRoutineLoadStmt.getDbFullName(),
                        pauseRoutineLoadStmt.getName());
                jobs.add(routineLoadJob);
            }
        } finally {
            readUnlock();
        }

        for (RoutineLoadJob routineLoadJob : jobs) {
            try {
                routineLoadJob.updateState(RoutineLoadJob.JobState.PAUSED,
                        new ErrorReason(InternalErrorCode.MANUAL_PAUSE_ERR,
                                "User " + ConnectContext.get().getQualifiedUser() + " pauses routine load job"),
                        false /* not replay */);
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId()).add("current_state",
                        routineLoadJob.getState()).add("user", ConnectContext.get().getQualifiedUser()).add("msg",
                        "routine load job has been paused by user").build());
            } catch (UserException e) {
                LOG.warn("failed to pause routine load job {}", routineLoadJob.getName(), e);
                // if user want to pause a certain job and failed, return error.
                // if user want to pause all possible jobs, skip error jobs.
                if (!pauseRoutineLoadStmt.isAll()) {
                    throw e;
                }
                continue;
            }
        }
    }

    public void resumeRoutineLoadJob(ResumeRoutineLoadStmt resumeRoutineLoadStmt) throws UserException {

        List<RoutineLoadJob> jobs = Lists.newArrayList();
        if (resumeRoutineLoadStmt.isAll()) {
            jobs = checkPrivAndGetAllJobs(resumeRoutineLoadStmt.getDbFullName());
        } else {
            RoutineLoadJob routineLoadJob = checkPrivAndGetJob(resumeRoutineLoadStmt.getDbFullName(),
                    resumeRoutineLoadStmt.getName());
            jobs.add(routineLoadJob);
        }

        for (RoutineLoadJob routineLoadJob : jobs) {
            try {
                routineLoadJob.jobStatistic.errorRowsAfterResumed = 0;
                routineLoadJob.autoResumeCount = 0;
                routineLoadJob.latestResumeTimestamp = 0;
                routineLoadJob.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null, false /* not replay */);
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                        .add("current_state", routineLoadJob.getState())
                        .add("user", ConnectContext.get().getQualifiedUser())
                        .add("msg", "routine load job has been resumed by user")
                        .build());
            } catch (UserException e) {
                LOG.warn("failed to resume routine load job {}", routineLoadJob.getName(), e);
                // if user want to resume a certain job and failed, return error.
                // if user want to resume all possible jobs, skip error jobs.
                if (!resumeRoutineLoadStmt.isAll()) {
                    throw e;
                }
                continue;
            }
        }
    }

    public void stopRoutineLoadJob(StopRoutineLoadStmt stopRoutineLoadStmt)
            throws UserException {
        RoutineLoadJob routineLoadJob;
        // it needs lock when getting routine load job,
        // otherwise, it may cause the editLog out of order in the following scenarios:
        // thread A: create job and record job meta
        // thread B: change job state and persist in editlog according to meta
        // thread A: persist in editlog
        // which will cause the null pointer exception when replaying editLog
        readLock();
        try {
            routineLoadJob = checkPrivAndGetJob(stopRoutineLoadStmt.getDbFullName(),
                    stopRoutineLoadStmt.getName());
        } finally {
            readUnlock();
        }
        routineLoadJob.updateState(RoutineLoadJob.JobState.STOPPED,
                new ErrorReason(InternalErrorCode.MANUAL_STOP_ERR,
                        "User  " + ConnectContext.get().getQualifiedUser() + " stop routine load job"),
                false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("current_state", routineLoadJob.getState())
                .add("user", ConnectContext.get().getQualifiedUser())
                .add("msg", "routine load job has been stopped by user")
                .build());
    }

    public int getSizeOfIdToRoutineLoadTask() {
        int sizeOfTasks = 0;
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            sizeOfTasks += routineLoadJob.getSizeOfRoutineLoadTaskInfoList();
        }
        return sizeOfTasks;
    }

    public int getClusterIdleSlotNum() {
        readLock();
        try {
            int result = 0;
            Map<Long, Integer> beIdToConcurrentTasks = getBeCurrentTasksNumMap();
            for (Map.Entry<Long, Integer> entry : beIdToMaxConcurrentTasks.entrySet()) {
                if (beIdToConcurrentTasks.containsKey(entry.getKey())) {
                    result += entry.getValue() - beIdToConcurrentTasks.get(entry.getKey());
                } else {
                    result += entry.getValue();
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    // get the BE id with minimum running task on it
    // return -1 if no BE is available.
    // throw exception if unrecoverable errors happen.
    // ATTN: this is only used for unit test now.
    public long getMinTaskBeId(String clusterName) throws LoadException {
        List<Long> beIdsInCluster = Env.getCurrentSystemInfo().getAllBackendIds(true);
        if (beIdsInCluster == null) {
            throw new LoadException("The " + clusterName + " has been deleted");
        }

        readLock();
        try {
            long result = -1L;
            int maxIdleSlotNum = 0;
            updateBeIdToMaxConcurrentTasks();
            Map<Long, Integer> beIdToConcurrentTasks = getBeCurrentTasksNumMap();
            for (Long beId : beIdsInCluster) {
                if (beIdToMaxConcurrentTasks.containsKey(beId)) {
                    int idleTaskNum = 0;
                    if (beIdToConcurrentTasks.containsKey(beId)) {
                        idleTaskNum = beIdToMaxConcurrentTasks.get(beId) - beIdToConcurrentTasks.get(beId);
                    } else {
                        idleTaskNum = Config.max_routine_load_task_num_per_be;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("be {} has idle {}, concurrent task {}, max concurrent task {}", beId, idleTaskNum,
                                beIdToConcurrentTasks.get(beId), beIdToMaxConcurrentTasks.get(beId));
                    }
                    result = maxIdleSlotNum < idleTaskNum ? beId : result;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, idleTaskNum);
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    // check if the specified BE is available for running task
    // return true if it is available. return false if otherwise.
    // throw exception if unrecoverable errors happen.
    public long getAvailableBeForTask(long jobId, long previousBeId) throws LoadException {
        List<Long> availableBeIds = getAvailableBackendIds(jobId);

        // check if be has idle slot
        readLock();
        try {
            Map<Long, Integer> beIdToConcurrentTasks = getBeCurrentTasksNumMap();
            int previousBeIdleTaskNum = 0;

            // 1. Find if the given BE id has more than half of available slots
            if (previousBeId != -1L && availableBeIds.contains(previousBeId)) {
                // get the previousBackend info
                Backend previousBackend = Env.getCurrentSystemInfo().getBackend(previousBeId);
                // check previousBackend is not null && load available
                if (previousBackend != null && previousBackend.isLoadAvailable()) {
                    if (!beIdToMaxConcurrentTasks.containsKey(previousBeId)) {
                        previousBeIdleTaskNum = 0;
                    } else if (beIdToConcurrentTasks.containsKey(previousBeId)) {
                        previousBeIdleTaskNum = beIdToMaxConcurrentTasks.get(previousBeId)
                                - beIdToConcurrentTasks.get(previousBeId);
                    } else {
                        previousBeIdleTaskNum = beIdToMaxConcurrentTasks.get(previousBeId);
                    }
                    if (previousBeIdleTaskNum == Config.max_routine_load_task_num_per_be) {
                        return previousBeId;
                    }
                }
            }

            // 2. we believe that the benefits of load balance outweigh the benefits of object pool cache,
            //    so we try to find the one with the most idle slots as much as possible
            // 3. The previous BE is not in cluster && is not load available, find a new BE with min tasks
            int idleTaskNum = 0;
            long resultBeId = -1L;
            int maxIdleSlotNum = 0;
            for (Long beId : availableBeIds) {
                if (beIdToMaxConcurrentTasks.containsKey(beId)) {
                    if (beIdToConcurrentTasks.containsKey(beId)) {
                        idleTaskNum = beIdToMaxConcurrentTasks.get(beId) - beIdToConcurrentTasks.get(beId);
                    } else {
                        idleTaskNum = Config.max_routine_load_task_num_per_be;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("be {} has idle {}, concurrent task {}, max concurrent task {}", beId, idleTaskNum,
                                beIdToConcurrentTasks.get(beId), beIdToMaxConcurrentTasks.get(beId));
                    }
                    resultBeId = maxIdleSlotNum < idleTaskNum ? beId : resultBeId;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, idleTaskNum);
                }
            }
            // 4. on the basis of selecting the maximum idle slot be,
            //    try to reuse the object cache as much as possible
            if (previousBeIdleTaskNum == maxIdleSlotNum) {
                return previousBeId;
            }
            return resultBeId;
        } finally {
            readUnlock();
        }
    }

    /**
     * The routine load task can only be scheduled on backends which has proper resource tags.
     * The tags should be got from user property.
     * But in the old version, the routine load job does not have user info, so for compatibility,
     * if there is no user info, we will get tags from replica allocation of the first partition of the table.
     *
     * @param jobId
     * @param cluster
     * @return
     * @throws LoadException
     */
    protected List<Long> getAvailableBackendIds(long jobId) throws LoadException {
        RoutineLoadJob job = getJob(jobId);
        if (job == null) {
            throw new LoadException("job " + jobId + " does not exist");
        }
        Set<Tag> tags;
        if (job.getUserIdentity() == null) {
            // For old job, there may be no user info. So we have to use tags from replica allocation
            tags = getTagsFromReplicaAllocation(job.getDbId(), job.getTableId());
        } else {
            tags = Env.getCurrentEnv().getAuth().getResourceTags(job.getUserIdentity().getQualifiedUser());
            if (tags == UserProperty.INVALID_RESOURCE_TAGS) {
                // user may be dropped, or may not set resource tag property.
                // Here we fall back to use replica tag
                tags = getTagsFromReplicaAllocation(job.getDbId(), job.getTableId());
            }
        }
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder().needLoadAvailable().addTags(tags).build();
        return Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, -1 /* as many as possible */);
    }

    private Set<Tag> getTagsFromReplicaAllocation(long dbId, long tblId) throws LoadException {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(dbId);
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(tblId, Table.TableType.OLAP);
            tbl.readLock();
            try {
                PartitionInfo partitionInfo = tbl.getPartitionInfo();
                for (Partition partition : tbl.getPartitions()) {
                    ReplicaAllocation replicaAlloc = partitionInfo.getReplicaAllocation(partition.getId());
                    // just use the first one
                    return replicaAlloc.getAllocMap().keySet();
                }
                // Should not run into here. Just make compiler happy.
                return Sets.newHashSet();
            } finally {
                tbl.readUnlock();
            }
        } catch (MetaNotFoundException e) {
            throw new LoadException(e.getMessage());
        }
    }

    public RoutineLoadJob getJob(long jobId) {
        return idToRoutineLoadJob.get(jobId);
    }

    public RoutineLoadJob getJob(String dbFullName, String jobName) throws MetaNotFoundException {
        List<RoutineLoadJob> routineLoadJobList = getJob(dbFullName, jobName, false, null);
        if (CollectionUtils.isEmpty(routineLoadJobList)) {
            return null;
        } else {
            return routineLoadJobList.get(0);
        }
    }

    /*
      if dbFullName is null, result = all routine load job in all db
      else if jobName is null, result =  all routine load job in dbFullName

      if includeHistory is false, filter not running job in result
      else return all of result
     */
    public List<RoutineLoadJob> getJob(String dbFullName, String jobName,
            boolean includeHistory, PatternMatcher matcher)
            throws MetaNotFoundException {
        Preconditions.checkArgument(jobName == null || matcher == null,
                "jobName and matcher cannot be not null at the same time");
        // return all of routine load job
        List<RoutineLoadJob> result;
        RESULT:
        { // CHECKSTYLE IGNORE THIS LINE
            if (dbFullName == null) {
                result = new ArrayList<>(idToRoutineLoadJob.values());
                sortRoutineLoadJob(result);
                break RESULT;
            }

            Database database = Env.getCurrentInternalCatalog().getDbOrMetaException(dbFullName);
            long dbId = database.getId();
            if (!dbToNameToRoutineLoadJob.containsKey(dbId)) {
                result = new ArrayList<>();
                break RESULT;
            }
            if (jobName == null) {
                result = Lists.newArrayList();
                for (List<RoutineLoadJob> nameToRoutineLoadJob : dbToNameToRoutineLoadJob.get(dbId).values()) {
                    List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(nameToRoutineLoadJob);
                    sortRoutineLoadJob(routineLoadJobList);
                    result.addAll(routineLoadJobList);
                }
                break RESULT;
            }
            if (dbToNameToRoutineLoadJob.get(dbId).containsKey(jobName)) {
                result = new ArrayList<>(dbToNameToRoutineLoadJob.get(dbId).get(jobName));
                sortRoutineLoadJob(result);
                break RESULT;
            }
            return null;
        } // CHECKSTYLE IGNORE THIS LINE

        if (!includeHistory) {
            result = result.stream().filter(entity -> !entity.getState().isFinalState()).collect(Collectors.toList());
        }
        if (matcher != null) {
            result = result.stream().filter(entity -> matcher.match(entity.getName())).collect(Collectors.toList());
        }
        return result;
    }

    // return all routine load job named jobName in all of db
    public List<RoutineLoadJob> getJobByName(String jobName) {
        List<RoutineLoadJob> result = Lists.newArrayList();
        for (Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob : dbToNameToRoutineLoadJob.values()) {
            if (nameToRoutineLoadJob.containsKey(jobName)) {
                List<RoutineLoadJob> routineLoadJobList = new ArrayList<>(nameToRoutineLoadJob.get(jobName));
                sortRoutineLoadJob(routineLoadJobList);
                result.addAll(routineLoadJobList);
            }
        }
        return result;
    }

    // put history job in the end
    private void sortRoutineLoadJob(List<RoutineLoadJob> routineLoadJobList) {
        if (routineLoadJobList == null) {
            return;
        }
        int i = 0;
        int j = routineLoadJobList.size() - 1;
        while (i < j) {
            while (!routineLoadJobList.get(i).isFinal() && (i < j)) {
                i++;
            }
            while (routineLoadJobList.get(j).isFinal() && (i < j)) {
                j--;
            }
            if (i < j) {
                RoutineLoadJob routineLoadJob = routineLoadJobList.get(i);
                routineLoadJobList.set(i, routineLoadJobList.get(j));
                routineLoadJobList.set(j, routineLoadJob);
            }
        }
    }

    public boolean checkTaskInJob(RoutineLoadTaskInfo task) {
        RoutineLoadJob routineLoadJob = idToRoutineLoadJob.get(task.getJobId());
        if (routineLoadJob == null) {
            return false;
        }
        return routineLoadJob.containsTask(task.getId());
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(Set<RoutineLoadJob.JobState> desiredStates) {
        List<RoutineLoadJob> stateJobs = idToRoutineLoadJob.values().stream()
                .filter(entity -> desiredStates.contains(entity.getState())).collect(Collectors.toList());
        return stateJobs;
    }

    // RoutineLoadScheduler will run this method at fixed interval, and renew the timeout tasks
    public void processTimeoutTasks() {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.processTimeoutTasks();
        }
    }

    // Remove old routine load jobs from idToRoutineLoadJob
    // This function is called periodically.
    // Cancelled and stopped job will be removed after Configure.label_keep_max_second seconds
    public void cleanOldRoutineLoadJobs() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("begin to clean old routine load jobs ");
        }
        clearRoutineLoadJobIf(RoutineLoadJob::isExpired);
    }

    /**
     * Remove finished routine load jobs from idToRoutineLoadJob
     * This function is called periodically if Config.label_num_threshold is set.
     * Cancelled and stopped job will be removed.
     */
    public void cleanOverLimitRoutineLoadJobs() {
        if (Config.label_num_threshold < 0
                || idToRoutineLoadJob.size() <= Config.label_num_threshold) {
            return;
        }
        writeLock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("begin to clean routine load jobs");
            }
            Deque<RoutineLoadJob> finishedJobs = idToRoutineLoadJob
                    .values()
                    .stream()
                    .filter(RoutineLoadJob::isFinal)
                    .sorted(Comparator.comparingLong(o -> o.endTimestamp))
                    .collect(Collectors.toCollection(ArrayDeque::new));
            while (!finishedJobs.isEmpty()
                    && idToRoutineLoadJob.size() > Config.label_num_threshold) {
                RoutineLoadJob routineLoadJob = finishedJobs.pollFirst();
                unprotectedRemoveJobFromDb(routineLoadJob);
                idToRoutineLoadJob.remove(routineLoadJob.getId());
                RoutineLoadOperation operation = new RoutineLoadOperation(routineLoadJob.getId(),
                        routineLoadJob.getState());
                Env.getCurrentEnv().getEditLog().logRemoveRoutineLoadJob(operation);
            }
        } finally {
            writeUnlock();
        }
    }

    private void clearRoutineLoadJobIf(Predicate<RoutineLoadJob> pred) {
        writeLock();
        try {
            Iterator<Map.Entry<Long, RoutineLoadJob>> iterator = idToRoutineLoadJob.entrySet().iterator();
            long currentTimestamp = System.currentTimeMillis();
            while (iterator.hasNext()) {
                RoutineLoadJob routineLoadJob = iterator.next().getValue();
                if (pred.test(routineLoadJob)) {
                    unprotectedRemoveJobFromDb(routineLoadJob);
                    iterator.remove();
                    RoutineLoadOperation operation = new RoutineLoadOperation(routineLoadJob.getId(),
                            routineLoadJob.getState());
                    Env.getCurrentEnv().getEditLog().logRemoveRoutineLoadJob(operation);
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("end_timestamp", routineLoadJob.getEndTimestamp())
                            .add("current_timestamp", currentTimestamp)
                            .add("job_state", routineLoadJob.getState())
                            .add("msg", "old job has been cleaned")
                    );
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayRemoveOldRoutineLoad(RoutineLoadOperation operation) {
        writeLock();
        try {
            RoutineLoadJob job = idToRoutineLoadJob.remove(operation.getId());
            if (job != null) {
                unprotectedRemoveJobFromDb(job);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("replay remove routine load job: {}", operation.getId());
    }

    private void unprotectedRemoveJobFromDb(RoutineLoadJob routineLoadJob) {
        dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).remove(routineLoadJob);
        if (dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).isEmpty()) {
            dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).remove(routineLoadJob.getName());
        }
        if (dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).isEmpty()) {
            dbToNameToRoutineLoadJob.remove(routineLoadJob.getDbId());
        }
    }

    public void updateRoutineLoadJob() throws UserException {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            if (!routineLoadJob.state.isFinalState()) {
                routineLoadJob.update();
            }
        }
    }

    public void replayCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        unprotectedAddJob(routineLoadJob);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                .add("msg", "replay create routine load job")
                .build());
    }

    public void replayChangeRoutineLoadJob(RoutineLoadOperation operation) {
        RoutineLoadJob job = getJob(operation.getId());
        try {
            job.updateState(operation.getJobState(), operation.getErrorReason(), true /* is replay */);
        } catch (UserException e) {
            LOG.error("should not happened", e);
        } catch (NullPointerException npe) {
            LOG.error("cannot get job when replaying state change job, which is unexpected, job id: "
                    + operation.getId());
        }
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, operation.getId())
                .add("current_state", operation.getJobState())
                .add("msg", "replay change routine load job")
                .build());
    }

    /**
     * Enter of altering a routine load job
     */
    public void alterRoutineLoadJob(AlterRoutineLoadStmt stmt) throws UserException {
        RoutineLoadJob job;
        // it needs lock when getting routine load job,
        // otherwise, it may cause the editLog out of order in the following scenarios:
        // thread A: create job and record job meta
        // thread B: change job state and persist in editlog according to meta
        // thread A: persist in editlog
        // which will cause the null pointer exception when replaying editLog
        readLock();
        try {
            job = checkPrivAndGetJob(stmt.getDbName(), stmt.getLabel());
        } finally {
            readUnlock();
        }
        if (stmt.hasDataSourceProperty()
                && !stmt.getDataSourceProperties().getDataSourceType().equalsIgnoreCase(job.dataSourceType.name())) {
            throw new DdlException("The specified job type is not: "
                    + stmt.getDataSourceProperties().getDataSourceType());
        }
        job.modifyProperties(stmt);
    }

    public void replayAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) {
        RoutineLoadJob job = getJob(log.getJobId());
        Preconditions.checkNotNull(job, log.getJobId());
        job.replayModifyProperties(log);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(idToRoutineLoadJob.size());
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            RoutineLoadJob routineLoadJob = RoutineLoadJob.read(in);
            idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
            Map<String, List<RoutineLoadJob>> map = dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId());
            if (map == null) {
                map = Maps.newConcurrentMap();
                dbToNameToRoutineLoadJob.put(routineLoadJob.getDbId(), map);
            }

            List<RoutineLoadJob> jobs = map.get(routineLoadJob.getName());
            if (jobs == null) {
                jobs = Lists.newArrayList();
                map.put(routineLoadJob.getName(), jobs);
            }
            jobs.add(routineLoadJob);
            if (!routineLoadJob.getState().isFinalState()) {
                Env.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(routineLoadJob);
            }
        }
    }
}
