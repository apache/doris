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

import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RoutineLoad {
    private static final Logger LOG = LogManager.getLogger(RoutineLoad.class);
    private static final int DEFAULT_BE_CONCURRENT_TASK_NUM = 100;

    // TODO(ml): real-time calculate by be
    private Map<Long, Integer> beIdToMaxConcurrentTasks;

    // stream load job meta
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob;
    private Map<Long, RoutineLoadJob> idToNeedSchedulerRoutineLoadJob;
    private Map<Long, RoutineLoadJob> idToRunningRoutineLoadJob;
    private Map<Long, RoutineLoadJob> idToCancelledRoutineLoadJob;

    // stream load tasks meta (not persistent)
    private Map<Long, RoutineLoadTask> idToRoutineLoadTask;
    private Map<Long, RoutineLoadTask> idToNeedSchedulerRoutineLoadTask;

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

    public RoutineLoad() {
        idToRoutineLoadJob = Maps.newHashMap();
        idToNeedSchedulerRoutineLoadJob = Maps.newHashMap();
        idToRunningRoutineLoadJob = Maps.newHashMap();
        idToCancelledRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadTask = Maps.newHashMap();
        idToNeedSchedulerRoutineLoadTask = Maps.newHashMap();
        lock = new ReentrantReadWriteLock(true);
    }

    public int getTotalMaxConcurrentTaskNum() {
        readLock();
        try {
            if (beIdToMaxConcurrentTasks == null) {
                beIdToMaxConcurrentTasks = Catalog.getCurrentSystemInfo().getBackendIds(true)
                        .parallelStream().collect(Collectors.toMap(beId -> beId, beId -> DEFAULT_BE_CONCURRENT_TASK_NUM));
            }
            return beIdToMaxConcurrentTasks.values().stream().mapToInt(i -> i).sum();
        } finally {
            readUnlock();
        }
    }

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        writeLock();
        try {
            idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
        } finally {
            writeUnlock();
        }
    }

    public void addRoutineLoadTasks(List<RoutineLoadTask> routineLoadTaskList) {
        writeLock();
        try {
            idToRoutineLoadTask.putAll(routineLoadTaskList.parallelStream().collect(
                    Collectors.toMap(task -> task.getSignature(), task -> task)));
        } finally {
            writeUnlock();
        }
    }

    public Map<Long, RoutineLoadTask> getIdToRoutineLoadTask() {
        return idToRoutineLoadTask;
    }

    public void addNeedSchedulerRoutineLoadTasks(List<RoutineLoadTask> routineLoadTaskList) {
        writeLock();
        try {
            idToNeedSchedulerRoutineLoadTask.putAll(routineLoadTaskList.parallelStream().collect(
                    Collectors.toMap(task -> task.getSignature(), task -> task)));
        } finally {
            writeUnlock();
        }
    }

    public void removeRoutineLoadTasks(List<RoutineLoadTask> routineLoadTasks) {
        if (routineLoadTasks != null) {
            writeLock();
            try {
                routineLoadTasks.parallelStream().forEach(task -> idToRoutineLoadTask.remove(task.getSignature()));
                routineLoadTasks.parallelStream().forEach(task ->
                        idToNeedSchedulerRoutineLoadTask.remove(task.getSignature()));
            } finally {
                writeUnlock();
            }
        }
    }

    public Map<Long, RoutineLoadTask> getIdToNeedSchedulerRoutineLoadTasks() {
        readLock();
        try {
            return idToNeedSchedulerRoutineLoadTask;
        } finally {
            readUnlock();
        }
    }

    public List<RoutineLoadJob> getRoutineLoadJobByState(RoutineLoadJob.JobState jobState) throws LoadException {
        List<RoutineLoadJob> jobs = new ArrayList<>();
        Collection<RoutineLoadJob> stateJobs = null;
        readLock();
        LOG.debug("begin to get routine load job by state {}", jobState.name());
        try {
            switch (jobState) {
                case NEED_SCHEDULER:
                    stateJobs = idToNeedSchedulerRoutineLoadJob.values();
                    break;
                case PAUSED:
                    throw new LoadException("not support getting paused routine load jobs");
                case RUNNING:
                    stateJobs = idToRunningRoutineLoadJob.values();
                    break;
                case STOPPED:
                    throw new LoadException("not support getting stopped routine load jobs");
                default:
                    break;
            }
            if (stateJobs != null) {
                jobs.addAll(stateJobs);
                LOG.info("got {} routine load jobs by state {}", jobs.size(), jobState.name());
            }
        } finally {
            readUnlock();
        }
        return jobs;
    }

    public void updateRoutineLoadJobStateNoValid(RoutineLoadJob routineLoadJob, RoutineLoadJob.JobState jobState) {
        writeLock();
        try {
            RoutineLoadJob.JobState srcJobState = routineLoadJob.getState();
            long jobId = routineLoadJob.getId();
            LOG.info("begin to change job {} state from {} to {}", jobId, srcJobState, jobState);
            switch (jobState) {
                case NEED_SCHEDULER:
                    idToRunningRoutineLoadJob.remove(jobId);
                    idToNeedSchedulerRoutineLoadJob.put(jobId, routineLoadJob);
                    break;
                case PAUSED:
                    idToNeedSchedulerRoutineLoadJob.remove(jobId);
                    idToRunningRoutineLoadJob.remove(jobId);
                    break;
                case RUNNING:
                    idToNeedSchedulerRoutineLoadJob.remove(jobId, routineLoadJob);
                    idToRunningRoutineLoadJob.put(jobId, routineLoadJob);
                    break;
                case CANCELLED:
                    idToRunningRoutineLoadJob.remove(jobId);
                    idToNeedSchedulerRoutineLoadJob.remove(jobId);
                    idToCancelledRoutineLoadJob.put(jobId, routineLoadJob);
                    break;
                case STOPPED:
                    idToRunningRoutineLoadJob.remove(jobId);
                    idToNeedSchedulerRoutineLoadJob.remove(jobId);
                    break;
                default:
                    break;
            }
            routineLoadJob.setState(jobState);
            Catalog.getInstance().getEditLog().logRoutineLoadJob(routineLoadJob);
        } finally {
            writeUnlock();
        }
    }

    public void updateRoutineLoadJobState(RoutineLoadJob routineLoadJob, RoutineLoadJob.JobState jobState)
            throws LoadException {
        writeLock();
        try {
            RoutineLoadJob.JobState srcJobState = routineLoadJob.getState();
            long jobId = routineLoadJob.getId();
            LOG.info("begin to change job {} state from {} to {}", jobId, srcJobState, jobState);
            checkStateTransform(srcJobState, jobState);
            switch (jobState) {
                case NEED_SCHEDULER:
                    idToRunningRoutineLoadJob.remove(jobId);
                    idToNeedSchedulerRoutineLoadJob.put(jobId, routineLoadJob);
                    break;
                case PAUSED:
                    idToNeedSchedulerRoutineLoadJob.remove(jobId);
                    idToRunningRoutineLoadJob.remove(jobId);
                    break;
                case RUNNING:
                    idToNeedSchedulerRoutineLoadJob.remove(jobId, routineLoadJob);
                    idToRunningRoutineLoadJob.put(jobId, routineLoadJob);
                    break;
                case CANCELLED:
                    idToRunningRoutineLoadJob.remove(jobId);
                    idToNeedSchedulerRoutineLoadJob.remove(jobId);
                    idToCancelledRoutineLoadJob.put(jobId, routineLoadJob);
                    break;
                case STOPPED:
                    idToRunningRoutineLoadJob.remove(jobId);
                    idToNeedSchedulerRoutineLoadJob.remove(jobId);
                    break;
                default:
                    break;
            }
            routineLoadJob.setState(jobState);
            Catalog.getInstance().getEditLog().logRoutineLoadJob(routineLoadJob);
        } finally {
            writeUnlock();
        }
    }

    private void checkStateTransform(RoutineLoadJob.JobState currentState, RoutineLoadJob.JobState desireState)
            throws LoadException {
        if (currentState == RoutineLoadJob.JobState.PAUSED && desireState == RoutineLoadJob.JobState.NEED_SCHEDULER) {
            throw new LoadException("could not transform " + currentState + " to " + desireState);
        } else if (currentState == RoutineLoadJob.JobState.CANCELLED ||
                currentState == RoutineLoadJob.JobState.STOPPED) {
            throw new LoadException("could not transform " + currentState + " to " + desireState);
        }
    }

}
