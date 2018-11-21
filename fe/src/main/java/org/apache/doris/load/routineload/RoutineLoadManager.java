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
import com.google.common.collect.Queues;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class RoutineLoadManager {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadManager.class);
    private static final int DEFAULT_BE_CONCURRENT_TASK_NUM = 100;
    private static final int DEFAULT_TASK_TIMEOUT_MINUTES = 5;

    // Long is beId, integer is the size of tasks in be
    private Map<Long, Integer> beIdToMaxConcurrentTasks;
    private Map<Long, Integer> beIdToConcurrentTasks;

    // stream load job meta
    private Map<Long, RoutineLoadJob> idToRoutineLoadJob;
    private Map<Long, RoutineLoadJob> idToNeedSchedulerRoutineLoadJob;
    private Map<Long, RoutineLoadJob> idToRunningRoutineLoadJob;
    private Map<Long, RoutineLoadJob> idToCancelledRoutineLoadJob;

    // stream load tasks meta (not persistent)
    private Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask;
    // KafkaPartitions means that partitions belong to one task
    // kafka partitions == routine load task (logical)
    private Queue<RoutineLoadTaskInfo> needSchedulerRoutineLoadTasks;
    private Map<Long, Long> taskIdToJobId;

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
        idToRoutineLoadJob = Maps.newHashMap();
        idToNeedSchedulerRoutineLoadJob = Maps.newHashMap();
        idToRunningRoutineLoadJob = Maps.newHashMap();
        idToCancelledRoutineLoadJob = Maps.newHashMap();
        idToRoutineLoadTask = Maps.newHashMap();
        needSchedulerRoutineLoadTasks = Queues.newLinkedBlockingQueue();
        beIdToConcurrentTasks = Maps.newHashMap();
        taskIdToJobId = Maps.newHashMap();
        lock = new ReentrantReadWriteLock(true);
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

    public void addRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        writeLock();
        try {
            idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
        } finally {
            writeUnlock();
        }
    }

    public void addRoutineLoadTasks(List<RoutineLoadTaskInfo> routineLoadTaskList) {
        writeLock();
        try {
            idToRoutineLoadTask.putAll(routineLoadTaskList.parallelStream().collect(
                    Collectors.toMap(task -> task.getSignature(), task -> task)));
        } finally {
            writeUnlock();
        }
    }

    public Map<Long, RoutineLoadTaskInfo> getIdToRoutineLoadTask() {
        readLock();
        try {
            return idToRoutineLoadTask;
        } finally {
            readUnlock();
        }
    }

    public void addNeedSchedulerRoutineLoadTasks(List<RoutineLoadTaskInfo> routineLoadTaskList, long routineLoadJobId) {
        writeLock();
        try {
            routineLoadTaskList.parallelStream().forEach(entity -> needSchedulerRoutineLoadTasks.add(entity));
            routineLoadTaskList.parallelStream().forEach(entity ->
                    taskIdToJobId.put(entity.getSignature(), routineLoadJobId));
        } finally {
            writeUnlock();
        }
    }

    public void removeRoutineLoadTasks(List<RoutineLoadTaskInfo> routineLoadTasks) {
        if (routineLoadTasks != null) {
            writeLock();
            try {
                routineLoadTasks.parallelStream().forEach(task -> idToRoutineLoadTask.remove(task.getSignature()));
                routineLoadTasks.parallelStream().forEach(task ->
                        needSchedulerRoutineLoadTasks.remove(task));
                routineLoadTasks.parallelStream().forEach(task -> taskIdToJobId.remove(task.getSignature()));
            } finally {
                writeUnlock();
            }
        }
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

    public long getMinTaskBeId() {
        readLock();
        try {
            long result = 0L;
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
            return result;
        } finally {
            readUnlock();
        }
    }

    public Queue<RoutineLoadTaskInfo> getNeedSchedulerRoutineLoadTasks() {
        readLock();
        try {
            return needSchedulerRoutineLoadTasks;
        } finally {
            readUnlock();
        }
    }

    public RoutineLoadJob getJobByTaskId(long taskId) {
        readLock();
        try {
            return idToRoutineLoadJob.get(taskIdToJobId.get(taskId));
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

    public void processTimeOutTasks() {
        writeLock();
        try {
            List<RoutineLoadTaskInfo> runningTasks = new ArrayList<>(idToRoutineLoadTask.values());
            runningTasks.removeAll(needSchedulerRoutineLoadTasks);

            for (RoutineLoadTaskInfo routineLoadTaskInfo : runningTasks) {
                if ((System.currentTimeMillis() - routineLoadTaskInfo.getLoadStartTimeMs())
                        > DEFAULT_TASK_TIMEOUT_MINUTES * 60 * 1000) {
                    long oldSignature = routineLoadTaskInfo.getSignature();
                    if (routineLoadTaskInfo instanceof KafkaTaskInfo) {
                        // remove old task
                        idToRoutineLoadTask.remove(routineLoadTaskInfo.getSignature());
                        // add new task
                        KafkaTaskInfo kafkaTaskInfo = new KafkaTaskInfo((KafkaTaskInfo) routineLoadTaskInfo);
                        idToRoutineLoadTask.put(kafkaTaskInfo.getSignature(), kafkaTaskInfo);
                        needSchedulerRoutineLoadTasks.add(kafkaTaskInfo);
                    }
                    LOG.debug("Task {} was ran more then {} minutes. It was removed and rescheduled",
                            oldSignature, DEFAULT_TASK_TIMEOUT_MINUTES);
                }

            }
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
