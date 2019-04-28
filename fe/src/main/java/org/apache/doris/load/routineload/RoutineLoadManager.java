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
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RoutineLoadManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadManager.class);
    private static final int DEFAULT_BE_CONCURRENT_TASK_NUM = 10;

    // Long is beId, integer is the size of tasks in be
    private Map<Long, Integer> beIdToMaxConcurrentTasks = Maps.newHashMap();

    // stream load job meta
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
    private Map<Long, Map<String, List<RoutineLoadJob>>> dbToNameToRoutineLoadJob = Maps.newConcurrentMap();

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

    public void updateBeIdToMaxConcurrentTasks() {
        beIdToMaxConcurrentTasks = Catalog.getCurrentSystemInfo().getBackendIds(true)
                .stream().collect(Collectors.toMap(beId -> beId, beId -> DEFAULT_BE_CONCURRENT_TASK_NUM));
    }

    // this is not real-time number
    public int getTotalMaxConcurrentTaskNum() {
        return beIdToMaxConcurrentTasks.values().stream().mapToInt(i -> i).sum();
    }

    private Map<Long, Integer> getBeIdConcurrentTaskMaps() {
        Map<Long, Integer> beIdToConcurrentTasks = Maps.newHashMap();
        for (RoutineLoadJob routineLoadJob : getRoutineLoadJobByState(RoutineLoadJob.JobState.RUNNING)) {
            Map<Long, Integer> jobBeIdToConcurrentTaskNum = routineLoadJob.getBeIdToConcurrentTaskNum();
            for (Map.Entry<Long, Integer> entry : jobBeIdToConcurrentTaskNum.entrySet()) {
                if (beIdToConcurrentTasks.containsKey(entry.getKey())) {
                    beIdToConcurrentTasks.put(entry.getKey(), beIdToConcurrentTasks.get(entry.getKey()) + entry.getValue());
                } else {
                    beIdToConcurrentTasks.put(entry.getKey(), entry.getValue());
                }
            }
        }
        // LOG.debug("beIdToConcurrentTasks is {}", Joiner.on(",").withKeyValueSeparator(":").join(beIdToConcurrentTasks));
        return beIdToConcurrentTasks;

    }

    public void createRoutineLoadJob(CreateRoutineLoadStmt createRoutineLoadStmt, String origStmt)
            throws UserException {
        // check load auth
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                createRoutineLoadStmt.getDBName(),
                                                                createRoutineLoadStmt.getTableName(),
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                createRoutineLoadStmt.getDBName(),
                                                createRoutineLoadStmt.getTableName());
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

        routineLoadJob.setOrigStmt(origStmt);
        addRoutineLoadJob(routineLoadJob);
    }

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob) throws DdlException {
        writeLock();
        try {
            // check if db.routineLoadName has been used
            if (isNameUsed(routineLoadJob.getDbId(), routineLoadJob.getName())) {
                throw new DdlException("Name " + routineLoadJob.getName() + " already used in db "
                        + routineLoadJob.getDbId());
            }

            unprotectedAddJob(routineLoadJob);

            Catalog.getInstance().getEditLog().logCreateRoutineLoadJob(routineLoadJob);
            LOG.info("create routine load job: id: {}, name: {}", routineLoadJob.getId(), routineLoadJob.getName());
        } finally {
            writeUnlock();
        }
    }

    private void unprotectedAddJob(RoutineLoadJob routineLoadJob) {
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);

        Map<String, List<RoutineLoadJob>> nameToRoutineLoadJob = dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId());
        if (nameToRoutineLoadJob == null) {
            nameToRoutineLoadJob = Maps.newConcurrentMap();
            dbToNameToRoutineLoadJob.put(routineLoadJob.getDbId(), nameToRoutineLoadJob);
        }
        List<RoutineLoadJob> routineLoadJobList = nameToRoutineLoadJob.get(routineLoadJob.getName());
        if (routineLoadJobList == null) {
            routineLoadJobList = Lists.newArrayList();
            nameToRoutineLoadJob.put(routineLoadJob.getName(), routineLoadJobList);
        }
        routineLoadJobList.add(routineLoadJob);
        // register txn state listener
        Catalog.getCurrentGlobalTransactionMgr().getListenerRegistry().register(routineLoadJob);
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
                if (optional.isPresent()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void pauseRoutineLoadJob(PauseRoutineLoadStmt pauseRoutineLoadStmt)
            throws DdlException, AnalysisException, MetaNotFoundException {
        RoutineLoadJob routineLoadJob = getJob(pauseRoutineLoadStmt.getDbFullName(), pauseRoutineLoadStmt.getName());
        if (routineLoadJob == null) {
            throw new DdlException("There is not operable routine load job with name " + pauseRoutineLoadStmt.getName());
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
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                dbFullName,
                                                                tableName,
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tableName);
        }

        routineLoadJob.updateState(RoutineLoadJob.JobState.PAUSED,
                                   "User " + ConnectContext.get().getQualifiedUser() + " pauses routine load job",
                                   false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                         .add("current_state", routineLoadJob.getState())
                         .add("user", ConnectContext.get().getQualifiedUser())
                         .add("msg", "routine load job has been paused by user")
                         .build());
    }

    public void resumeRoutineLoadJob(ResumeRoutineLoadStmt resumeRoutineLoadStmt) throws DdlException,
            AnalysisException, MetaNotFoundException {
        RoutineLoadJob routineLoadJob = getJob(resumeRoutineLoadStmt.getDBFullName(), resumeRoutineLoadStmt.getName());
        if (routineLoadJob == null) {
            throw new DdlException("There is not operable routine load job with name " + resumeRoutineLoadStmt.getName() + ".");
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
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                dbFullName,
                                                                tableName,
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tableName);
        }
        routineLoadJob.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null, false /* not replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                         .add("current_state", routineLoadJob.getState())
                         .add("user", ConnectContext.get().getQualifiedUser())
                         .add("msg", "routine load job has been resumed by user")
                         .build());
    }

    public void stopRoutineLoadJob(StopRoutineLoadStmt stopRoutineLoadStmt)
            throws DdlException, AnalysisException, MetaNotFoundException {
        RoutineLoadJob routineLoadJob = getJob(stopRoutineLoadStmt.getDBFullName(), stopRoutineLoadStmt.getName());
        if (routineLoadJob == null) {
            throw new DdlException("There is not operable routine load job with name " + stopRoutineLoadStmt.getName());
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
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(),
                                                                dbFullName,
                                                                tableName,
                                                                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tableName);
        }
        routineLoadJob.updateState(RoutineLoadJob.JobState.STOPPED,
                                   "User  " + ConnectContext.get().getQualifiedUser() + " stop routine load job",
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
            updateBeIdToMaxConcurrentTasks();
            Map<Long, Integer> beIdToConcurrentTasks = getBeIdConcurrentTaskMaps();
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

    public long getMinTaskBeId(String clusterName) throws LoadException {
        List<Long> beIdsInCluster = Catalog.getCurrentSystemInfo().getClusterBackendIds(clusterName, true);
        if (beIdsInCluster == null) {
            throw new LoadException("The " + clusterName + " has been deleted");
        }

        readLock();
        try {
            long result = -1L;
            int maxIdleSlotNum = 0;
            updateBeIdToMaxConcurrentTasks();
            Map<Long, Integer> beIdToConcurrentTasks = getBeIdConcurrentTaskMaps();
            for (Long beId : beIdsInCluster) {
                if (beIdToMaxConcurrentTasks.containsKey(beId)) {
                    int idleTaskNum = 0;
                    if (beIdToConcurrentTasks.containsKey(beId)) {
                        idleTaskNum = beIdToMaxConcurrentTasks.get(beId) - beIdToConcurrentTasks.get(beId);
                    } else {
                        idleTaskNum = DEFAULT_BE_CONCURRENT_TASK_NUM;
                    }
                    LOG.debug("be {} has idle {}, concurrent task {}, max concurrent task {}", beId, idleTaskNum,
                              beIdToConcurrentTasks.get(beId), beIdToMaxConcurrentTasks.get(beId));
                    result = maxIdleSlotNum < idleTaskNum ? beId : result;
                    maxIdleSlotNum = Math.max(maxIdleSlotNum, idleTaskNum);
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

    public boolean checkBeToTask(long beId, String clusterName) throws LoadException {
        List<Long> beIdsInCluster = Catalog.getCurrentSystemInfo().getClusterBackendIds(clusterName, true);
        if (beIdsInCluster == null) {
            throw new LoadException("The " + clusterName + " has been deleted");
        }

        if (!beIdsInCluster.contains(beId)) {
            LOG.debug("the previous be id {} does not belong to cluster name {}", beId, clusterName);
            return false;
        }

        // check if be has idle slot
        readLock();
        try {
            int idleTaskNum = 0;
            Map<Long, Integer> beIdToConcurrentTasks = getBeIdConcurrentTaskMaps();
            if (beIdToConcurrentTasks.containsKey(beId)) {
                idleTaskNum = beIdToMaxConcurrentTasks.get(beId) - beIdToConcurrentTasks.get(beId);
            } else {
                idleTaskNum = DEFAULT_BE_CONCURRENT_TASK_NUM;
            }
            if (idleTaskNum > 0) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public RoutineLoadJob getJob(long jobId) {
        return idToRoutineLoadJob.get(jobId);
    }

    public RoutineLoadJob getJob(String dbFullName, String jobName) throws MetaNotFoundException {
        List<RoutineLoadJob> routineLoadJobList = getJob(dbFullName, jobName, false);
        if (routineLoadJobList == null || routineLoadJobList.size() == 0) {
            return null;
        } else {
            return routineLoadJobList.get(0);
        }
    }

    /*
      if dbFullName is null, result = all of routine load job in all of db
      else if jobName is null, result =  all of routine load job in dbFullName

      if includeHistory is false, filter not running job in result
      else return all of result
     */
    public List<RoutineLoadJob> getJob(String dbFullName, String jobName, boolean includeHistory)
            throws MetaNotFoundException {
        // return all of routine load job
        List<RoutineLoadJob> result;
        RESULT:
        {
            if (dbFullName == null) {
                result = new ArrayList<>(idToRoutineLoadJob.values());
                sortRoutineLoadJob(result);
                break RESULT;
            }

            long dbId = 0L;
            Database database = Catalog.getCurrentCatalog().getDb(dbFullName);
            if (database == null) {
                throw new MetaNotFoundException("failed to find database by dbFullName " + dbFullName);
            }
            dbId = database.getId();
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
        }

        if (!includeHistory) {
            result = result.stream().filter(entity -> !entity.getState().isFinalState()).collect(Collectors.toList());
        }
        return result;
    }

    // return all of routine load job named jobName in all of db
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
            while (!routineLoadJobList.get(i).isFinal() && (i < j))
                i++;
            while (routineLoadJobList.get(j).isFinal() && (i < j))
                j--;
            if (i < j) {
                RoutineLoadJob routineLoadJob = routineLoadJobList.get(i);
                routineLoadJobList.set(i, routineLoadJobList.get(j));
                routineLoadJobList.set(j, routineLoadJob);
            }
        }
    }

    public boolean checkTaskInJob(UUID taskId) {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            if (routineLoadJob.containsTask(taskId)) {
                return true;
            }
        }
        return false;
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(RoutineLoadJob.JobState jobState) {
        // LOG.debug("begin to get routine load job by state {}", jobState.name());
        List<RoutineLoadJob> stateJobs = idToRoutineLoadJob.values().stream()
                .filter(entity -> entity.getState() == jobState).collect(Collectors.toList());
        // LOG.debug("got {} routine load jobs by state {}", stateJobs.size(), jobState.name());
        return stateJobs;
    }

    public void processTimeoutTasks() {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.processTimeoutTasks();
        }
    }

    // Remove old routine load jobs from idToRoutineLoadJob
    // This function is called periodically.
    // Cancelled and stopped job will be remove after Configure.label_keep_max_second seconds
    public void cleanOldRoutineLoadJobs() {
        writeLock();
        try {
            Iterator<Map.Entry<Long, RoutineLoadJob>> iterator = idToRoutineLoadJob.entrySet().iterator();
            long currentTimestamp = System.currentTimeMillis();
            while (iterator.hasNext()) {
                RoutineLoadJob routineLoadJob = iterator.next().getValue();
                if (routineLoadJob.needRemove()) {
                    dbToNameToRoutineLoadJob.get(routineLoadJob.getDbId()).get(routineLoadJob.getName()).remove(routineLoadJob);
                    iterator.remove();

                    RoutineLoadOperation operation = new RoutineLoadOperation(routineLoadJob.getId(),
                            JobState.CANCELLED);
                    Catalog.getInstance().getEditLog().logRemoveRoutineLoadJob(operation);
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
                dbToNameToRoutineLoadJob.get(job.getDbId()).get(job.getName()).remove(job);
            }
            LOG.info("replay remove routine load job: {}", operation.getId());
        } finally {
            writeUnlock();
        }
    }

    public void updateRoutineLoadJob() {
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
        job.updateState(operation.getJobState(), null, true /* is replay */);
        LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, operation.getId())
                 .add("current_state", operation.getJobState())
                 .add("msg", "replay change routine load job")
                 .build());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(idToRoutineLoadJob.size());
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.write(out);
        }
    }

    @Override
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
                Catalog.getCurrentGlobalTransactionMgr().getListenerRegistry().register(routineLoadJob);
            }
        }
    }
}
