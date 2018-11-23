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

    // Long is beId, integer is the size of tasks in be
    private Map<Long, Integer> beIdToMaxConcurrentTasks;
    private Map<Long, Integer> beIdToConcurrentTasks;

    // stream load job meta
    private Map<String, RoutineLoadJob> idToRoutineLoadJob;

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
        beIdToConcurrentTasks = Maps.newHashMap();
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
        idToRoutineLoadJob.put(routineLoadJob.getId(), routineLoadJob);
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

    public List<RoutineLoadTaskInfo> getNeedSchedulerRoutineLoadTasks() {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = new ArrayList<>();
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadTaskInfoList.addAll(routineLoadJob.getNeedSchedulerTaskInfoList());
        }
        return routineLoadTaskInfoList;
    }

    public RoutineLoadJob getJob(String jobId) {
        return idToRoutineLoadJob.get(jobId);
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

    public void processTimeoutTasks() {
        for (RoutineLoadJob routineLoadJob : idToRoutineLoadJob.values()) {
            routineLoadJob.processTimeoutTasks();
        }
    }

}
