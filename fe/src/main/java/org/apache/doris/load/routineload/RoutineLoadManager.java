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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.PauseRoutineLoadStmt;
import org.apache.doris.analysis.ResumeRoutineLoadStmt;
import org.apache.doris.analysis.StopRoutineLoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RoutineLoadManager {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadManager.class);
    private static final int DEFAULT_BE_CONCURRENT_TASK_NUM = 100;

    // Long is beId, integer is the size of tasks in be
    private Map<Long, Integer> beIdToMaxConcurrentTasks;
    private Map<Long, Integer> beIdToConcurrentTasks;

    // stream load job meta
    private Map<String, RoutineLoadJob> idToRoutineLoadJob;
    private Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob;

    private Queue<RoutineLoadTaskInfo> needSchedulerTasksQueue;

    private ReentrantReadWriteLock lock;

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
        idToRoutineLoadJob = Maps.newConcurrentMap();
        dbToNameToRoutineLoadJob = Maps.newConcurrentMap();
        beIdToConcurrentTasks = Maps.newHashMap();
        needSchedulerTasksQueue = Queues.newLinkedBlockingQueue();
        lock = new ReentrantReadWriteLock(true);
    }

    public Queue<RoutineLoadTaskInfo> getNeedSchedulerTasksQueue() {
        return needSchedulerTasksQueue;
    }

    public void initBeIdToMaxConcurrentTasks() {
        if (beIdToMaxConcurrentTasks == null) {
            beIdToMaxConcurrentTasks = Catalog.getCurrentSystemInfo().getBackendIds(true)
                    .parallelStream().collect(Collectors.toMap(beId -> beId, beId -> DEFAULT_BE_CONCURRENT_TASK_NUM));
        }
    }

    public int getTotalMaxConcurrentTaskNum() {
        readLock();
        try {
            initBeIdToMaxConcurrentTasks();
            return beIdToMaxConcurrentTasks.values().stream().mapToInt(i -> i).sum();
        } finally {
            readUnlock();
        }
    }

    public void updateBeIdTaskMaps() {
        writeLock();
        try {
            initBeIdToMaxConcurrentTasks();
            List<Long> beIds = Catalog.getCurrentSystemInfo().getBackendIds(true);

            // diff beIds and beIdToMaxConcurrentTasks.keys()
            List<Long> newBeIds = beIds.parallelStream().filter(entity -> beIdToMaxConcurrentTasks.get(entity) == null)
                    .collect(Collectors.toList());
            List<Long> unavailableBeIds = beIdToMaxConcurrentTasks.keySet().parallelStream()
                    .filter(entity -> !beIds.contains(entity))
                    .collect(Collectors.toList());
            newBeIds.parallelStream().forEach(entity -> beIdToMaxConcurrentTasks.put(entity, DEFAULT_BE_CONCURRENT_TASK_NUM));
            for (long beId : unavailableBeIds) {
                beIdToMaxConcurrentTasks.remove(beId);
                beIdToConcurrentTasks.remove(beId);
            }
            LOG.info("There are {} backends which participate in routine load scheduler. "
                             + "There are {} new backends and {} unavailable backends for routine load",
                     beIdToMaxConcurrentTasks.size(), newBeIds.size(), unavailableBeIds.size());
        } finally {
            writeUnlock();
        }
    }

    public void addNumOfConcurrentTasksByBeId(long beId) {
        writeLock();
        try {
            if (beIdToConcurrentTasks.get(beId) == null) {
                beIdToConcurrentTasks.put(beId, 1);
            } else {
                int concurrentTaskNum = (int) beIdToConcurrentTasks.get(beId);
                concurrentTaskNum++;
                beIdToConcurrentTasks.put(beId, concurrentTaskNum);
            }
        } finally {
            writeUnlock();
        }
    }

    public void addRoutineLoadJob(CreateRoutineLoadStmt createRoutineLoadStmt)
            throws AnalysisException, DdlException, LoadException {
        // check load auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                createRoutineLoadStmt.getDBTableName().getDb(),
                                                                createRoutineLoadStmt.getDBTableName().getTbl(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                createRoutineLoadStmt.getDBTableName());
        }
        RoutineLoadJob routineLoadJob = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(createRoutineLoadStmt.getTypeName());
        switch (type) {
            case KAFKA:
                routineLoadJob = KafkaRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
                break;
            default:
                break;
        }
        if (routineLoadJob != null) {
            addRoutineLoadJob(routineLoadJob);
        }
    }

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob) throws DdlException {
        writeLock();
        try {
            // check if db.routineLoadName has been used
            if (isNameUsed(routineLoadJob.dbId, routineLoadJob.getName())) {
                throw new DdlException("Name " + routineLoadJob.getName() + " already used in db "
                                               + routineLoadJob.getDbId());
            }
            idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
            addJobToDbToNameToRoutineLoadJob(routineLoadJob);
            // TODO(ml): edit log
        } finally {
            writeUnlock();
        }

    }

    // TODO(ml): Idempotency
    private boolean isNameUsed(Long dbId, String name) {
        if (dbToNameToRoutineLoadJob.containsKey(dbId)) {
            Map<String, List<RoutineLoadJob>> labelToRoutineLoadJob = dbToNameToRoutineLoadJob.get(dbId);
            if (labelToRoutineLoadJob.containsKey(name)) {
                List<RoutineLoadJob> routineLoadJobList = labelToRoutineLoadJob.get(name);
                Optional<RoutineLoadJob> optional = routineLoadJobList.parallelStream()
                        .filter(entity -> entity.getName().equals(name))
                        .filter(entity -> !entity.getState().isFinalState()).findFirst();
                if (!optional.isPresent()) {
                    return true;
                }
            }
        }
        return false;
    }

    private void addJobToDbToNameToRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        if (dbToNameToRoutineLoadJob.containsKey(routineLoadJob.getDbId())) {
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob =
                    dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId());
            if (nameToRoutineLoadJob.containsKey(routineLoadJob.getName())) {
                nameToRoutineLoadJob.get(routineLoadJob.getName()).add(routineLoadJob);
            } else {
                List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
                routineLoadJobList.add(routineLoadJob);
                nameToRoutineLoadJob.put(routineLoadJob.getName(), routineLoadJobList);
            }
        } else {
            List<RoutineLoadJob> routineLoadJobList = Lists.newArrayList();
            routineLoadJobList.add(routineLoadJob);
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = Maps.newConcurrentMap();
            nameToRoutineLoadJob.put(routineLoadJob.getName(), routineLoadJobList);
            dbToNameToRoutineLoadJob.put(routineLoadJob.getDbId(), nameToRoutineLoadJob);
        }
    }

    public void pauseRoutineLoadJob(PauseRoutineLoadStmt pauseRoutineLoadStmt) throws DdlException, AnalysisException {
        RoutineLoadJob routineLoadJob = getJobByName(pauseRoutineLoadStmt.getName());
        if (routineLoadJob == null) {
            throw new DdlException("There is not routine load job with name " + pauseRoutineLoadStmt.getName());
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                routineLoadJob.getDbFullName(),
                                                                routineLoadJob.getTableName(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                routineLoadJob.getTableName());
        }

        routineLoadJob.pause("User " + ConnectContext.get().getQualifiedUser() + "pauses routine load job");
    }

    public void resumeRoutineLoadJob(ResumeRoutineLoadStmt resumeRoutineLoadStmt) throws DdlException,
            AnalysisException {
        RoutineLoadJob routineLoadJob = getJobByName(resumeRoutineLoadStmt.getName());
        if (routineLoadJob == null) {
            throw new DdlException("There is not routine load job with name " + resumeRoutineLoadStmt.getName());
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                routineLoadJob.getDbFullName(),
                                                                routineLoadJob.getTableName(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                routineLoadJob.getTableName());
        }
        routineLoadJob.resume();
    }

    public void stopRoutineLoadJob(StopRoutineLoadStmt stopRoutineLoadStmt) throws DdlException, AnalysisException {
        RoutineLoadJob routineLoadJob = getJobByName(stopRoutineLoadStmt.getName());
        if (routineLoadJob == null) {
            throw new DdlException("There is not routine load job with name " + stopRoutineLoadStmt.getName());
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                routineLoadJob.getDbFullName(),
                                                                routineLoadJob.getTableName(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                routineLoadJob.getTableName());
        }
        routineLoadJob.stop();
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
            initBeIdToMaxConcurrentTasks();
            for (Map.Entry<Long, Integer> entry : beIdToMaxConcurrentTasks.entrySet()) {
                if (beIdToConcurrentTasks.get(entry.getKey()) == null) {
                    result += entry.getValue();
                } else {
                    result += entry.getValue() - beIdToConcurrentTasks.get(entry.getKey());
                }
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public long getMinTaskBeId() throws LoadException {
        readLock();
        try {
            long result = -1L;
            int maxIdleSlotNum = 0;
            initBeIdToMaxConcurrentTasks();
            for (Map.Entry<Long, Integer> entry : beIdToMaxConcurrentTasks.entrySet()) {
                if (beIdToConcurrentTasks.get(entry.getKey()) == null) {
                    result = maxIdleSlotNum < entry.getValue() ? entry.getKey() : result;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, entry.getValue());
                } else {
                    int idelTaskNum = entry.getValue() - beIdToConcurrentTasks.get(entry.getKey());
                    result = maxIdleSlotNum < idelTaskNum ? entry.getKey() : result;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, idelTaskNum);
                }
            }
            if (result < 0) {
                throw new LoadException("There is no empty slot in cluster");
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    public RoutineLoadJob getJob(String jobId) {
        return idToRoutineLoadJob.get(jobId);
    }

    public RoutineLoadJob getJobByName(String jobName) {
        String dbfullName = ConnectContext.get().getDatabase();
        Database database = Catalog.getCurrentCatalog().getDb(dbfullName);
        if (database == null) {
            return null;
        }
        readLock();
        try {
            Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob.get(database.getId());
            if (nameToRoutineLoadJob == null) {
                return null;
            }
            List<RoutineLoadJob> routineLoadJobList = nameToRoutineLoadJob.get(jobName);
            if (routineLoadJobList == null) {
                return null;
            }
            Optional<RoutineLoadJob> optional = routineLoadJobList.parallelStream()
                    .filter(entity -> !entity.getState().isFinalState()).findFirst();
            if (optional.isPresent()) {
                return null;
            }
            return optional.get();
        } finally {
            readUnlock();
        }
    }

    public RoutineLoadJob getJobByTaskId(String taskId) throws MetaNotFoundException {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            if (routineLoadJob.containsTask(taskId)) {
                return routineLoadJob;
            }
        }
        throw new MetaNotFoundException("could not found task by id " + taskId);
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(RoutineLoadJob.JobState jobState) throws LoadException {
        List<RoutineLoadJob> jobs = new ArrayList<>();
        Collection<RoutineLoadJob> stateJobs = null;
        LOG.debug("begin to get routine load job by state {}", jobState.name());
        stateJobs = idToRoutineLoadJob.values().stream()
                .filter(entity -> entity.getState() == jobState).collect(Collectors.toList());
        if (stateJobs != null) {
            jobs.addAll(stateJobs);
            LOG.info("got {} routine load jobs by state {}", jobs.size(), jobState.name());
        }
        return jobs;
    }

    public List<RoutineLoadTaskInfo> processTimeoutTasks() {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadTaskInfoList.addAll(routineLoadJob.processTimeoutTasks());
        }
        return routineLoadTaskInfoList;
    }

    // Remove old routine load jobs from idToRoutineLoadJob
    // This function is called periodically.
    // Cancelled and stopped job will be remove after Configure.label_keep_max_second seconds
    public void removeOldRoutineLoadJobs() {
        // TODO(ml): remove old routine load job
    }

}
