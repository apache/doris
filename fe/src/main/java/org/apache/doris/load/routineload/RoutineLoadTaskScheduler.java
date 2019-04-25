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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRoutineLoadTask;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Routine load task scheduler is a function which allocate task to be.
 * Step1: get total idle task num of backends.
 *   Step1.1: if total idle task num == 0, exit this round and switch to the next round immediately
 * Step2: equally divide to be
 *   Step2.1: if there is no task in queue, waiting task until an element becomes available.
 *   Step2.2: divide task to be
 * Step3: submit tasks to be
 */
public class RoutineLoadTaskScheduler extends Daemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskScheduler.class);

    private static final long BACKEND_SLOT_UPDATE_INTERVAL_MS = 10000; // 10s

    private RoutineLoadManager routineLoadManager;
    private LinkedBlockingQueue<RoutineLoadTaskInfo> needScheduleTasksQueue = Queues.newLinkedBlockingQueue();

    private long lastBackendSlotUpdateTime = -1;

    @VisibleForTesting
    public RoutineLoadTaskScheduler() {
        super("routine load task", 0);
        this.routineLoadManager = Catalog.getInstance().getRoutineLoadManager();
    }

    public RoutineLoadTaskScheduler(RoutineLoadManager routineLoadManager) {
        super("routine load task", 0);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runOneCycle() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadTaskScheduler", e);
        }
    }

    private void process() throws LoadException, UserException, InterruptedException {
        updateBackendSlotIfNecessary();

        int sizeOfTasksQueue = needScheduleTasksQueue.size();
        int clusterIdleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        int needScheduleTaskNum = sizeOfTasksQueue < clusterIdleSlotNum ? sizeOfTasksQueue : clusterIdleSlotNum;

        if (needScheduleTaskNum == 0) {
            return;
        }

        LOG.info("There are {} tasks need to be scheduled in queue", needScheduleTasksQueue.size());

        int scheduledTaskNum = 0;
        Map<Long, List<TRoutineLoadTask>> beIdToBatchTask = Maps.newHashMap();
        while (needScheduleTaskNum-- > 0) {
            // allocate be to task and begin transaction for task
            RoutineLoadTaskInfo routineLoadTaskInfo = null;
            try {
                routineLoadTaskInfo = needScheduleTasksQueue.take();
            } catch (InterruptedException e) {
                LOG.warn("Taking routine load task from queue has been interrupted", e);
                return;
            }
            try {
                if (!routineLoadManager.checkTaskInJob(routineLoadTaskInfo.getId())) {
                    // task has been abandoned while renew task has been added in queue
                    // or database has been deleted
                    LOG.warn(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, routineLoadTaskInfo.getId())
                                     .add("error_msg", "task has been abandoned")
                                     .build());
                    continue;
                }
                routineLoadTaskInfo.beginTxn();
                allocateTaskToBe(routineLoadTaskInfo);
            } catch (LoadException e) {
                // todo(ml): if cluster has been deleted, the job will be cancelled.
                needScheduleTasksQueue.put(routineLoadTaskInfo);
                LOG.warn(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, routineLoadTaskInfo.getId())
                                 .add("error_msg", "put task to the rear of queue with error " + e.getMessage())
                                 .build(), e);
                continue;
            }

            // task to thrift
            TRoutineLoadTask tRoutineLoadTask = null;
            try {
                tRoutineLoadTask = routineLoadTaskInfo.createRoutineLoadTask();
            } catch (UserException e) {
                routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(JobState.PAUSED,
                        "failed to create task: " + e.getMessage(), false);
                throw e;
            }

            // set the executeStartTimeMs of task
            routineLoadTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis());
            // add to batch task map
            if (beIdToBatchTask.containsKey(routineLoadTaskInfo.getBeId())) {
                beIdToBatchTask.get(routineLoadTaskInfo.getBeId()).add(tRoutineLoadTask);
            } else {
                List<TRoutineLoadTask> tRoutineLoadTaskList = Lists.newArrayList();
                tRoutineLoadTaskList.add(tRoutineLoadTask);
                beIdToBatchTask.put(routineLoadTaskInfo.getBeId(), tRoutineLoadTaskList);
            }
            // count
            scheduledTaskNum++;
        }
        submitBatchTask(beIdToBatchTask);
        LOG.info("{} tasks have been allocated to be.", scheduledTaskNum);
    }

    private void updateBackendSlotIfNecessary() {
        long currentTime = System.currentTimeMillis();
        if (lastBackendSlotUpdateTime != -1
                && currentTime - lastBackendSlotUpdateTime > BACKEND_SLOT_UPDATE_INTERVAL_MS) {
            routineLoadManager.updateBeIdToMaxConcurrentTasks();
            lastBackendSlotUpdateTime = currentTime;
            if (LOG.isDebugEnabled()) {
                LOG.debug("update backend max slot for routine load task scheduling");
            }
        }
    }

    public void addTaskInQueue(RoutineLoadTaskInfo routineLoadTaskInfo) {
        needScheduleTasksQueue.add(routineLoadTaskInfo);
    }

    public void addTasksInQueue(List<RoutineLoadTaskInfo> routineLoadTaskInfoList) {
        needScheduleTasksQueue.addAll(routineLoadTaskInfoList);
    }

    private void submitBatchTask(Map<Long, List<TRoutineLoadTask>> beIdToRoutineLoadTask) {
        for (Map.Entry<Long, List<TRoutineLoadTask>> entry : beIdToRoutineLoadTask.entrySet()) {
            Backend backend = Catalog.getCurrentSystemInfo().getBackend(entry.getKey());
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
            BackendService.Client client = null;
            boolean ok = false;
            try {
                client = ClientPool.backendPool.borrowObject(address);
                client.submit_routine_load_task(entry.getValue());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} tasks sent to be {}", entry.getValue().size(), entry.getKey());
                }
                ok = true;
            } catch (Exception e) {
                LOG.warn("task send error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
    }

    // check if previous be has idle slot
    // true: allocate previous be to task
    // false: allocate the most idle be to task
    private void allocateTaskToBe(RoutineLoadTaskInfo routineLoadTaskInfo)
            throws MetaNotFoundException, LoadException {
        if (routineLoadTaskInfo.getPreviousBeId() != -1L) {
            if (routineLoadManager.checkBeToTask(routineLoadTaskInfo.getPreviousBeId(), routineLoadTaskInfo.getClusterName())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, routineLoadTaskInfo.getId())
                                      .add("job_id", routineLoadTaskInfo.getJobId())
                                      .add("previous_be_id", routineLoadTaskInfo.getPreviousBeId())
                                      .add("msg", "task use the previous be id")
                                      .build());
                }
                routineLoadTaskInfo.setBeId(routineLoadTaskInfo.getPreviousBeId());
                return;
            }
        }
        routineLoadTaskInfo.setBeId(routineLoadManager.getMinTaskBeId(routineLoadTaskInfo.getClusterName()));
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUINTE_LOAD_TASK, routineLoadTaskInfo.getId())
                              .add("job_id", routineLoadTaskInfo.getJobId())
                              .add("be_id", routineLoadTaskInfo.getBeId())
                              .add("msg", "task has been allocated to be")
                              .build());
        }
    }
}
