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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.transaction.BeginTransactionException;

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
 * Step1: update backend slot if interval more then BACKEND_SLOT_UPDATE_INTERVAL_MS
 * Step2: submit beIdToBatchTask when queue is empty
 * Step3: take a task from queue and schedule this task
 *
 * The scheduler will be blocked in step3 till the queue receive a new task
 */
public class RoutineLoadTaskScheduler extends Daemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskScheduler.class);

    private static final long BACKEND_SLOT_UPDATE_INTERVAL_MS = 10000; // 10s
    private static final long SLOT_FULL_SLEEP_MS = 1000; // 1s
    private static final long MIN_SCHEDULE_INTERVAL_MS = 1000; // 1s

    private RoutineLoadManager routineLoadManager;
    private LinkedBlockingQueue<RoutineLoadTaskInfo> needScheduleTasksQueue = Queues.newLinkedBlockingQueue();
    private Map<Long, List<TRoutineLoadTask>> beIdToBatchTask = Maps.newHashMap();

    private long lastBackendSlotUpdateTime = -1;

    @VisibleForTesting
    public RoutineLoadTaskScheduler() {
        super("Routine load task scheduler", 0);
        this.routineLoadManager = Catalog.getInstance().getRoutineLoadManager();
    }

    public RoutineLoadTaskScheduler(RoutineLoadManager routineLoadManager) {
        super("Routine load task scheduler", 0);
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

    private void process() throws UserException, InterruptedException {
        updateBackendSlotIfNecessary();

        // if size of queue is zero, tasks will be submit by batch
        int idleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        if (needScheduleTasksQueue.size() == 0 || idleSlotNum == 0) {
            submitBatchTasksIfNotEmpty(beIdToBatchTask);
        }

        // scheduler will be blocked when there is no space for task in cluster
        if (idleSlotNum == 0) {
            Thread.sleep(SLOT_FULL_SLEEP_MS);
            return;
        }

        try {
            // This step will be blocked when queue is empty
            RoutineLoadTaskInfo routineLoadTaskInfo = needScheduleTasksQueue.take();
            if (System.currentTimeMillis() - routineLoadTaskInfo.getLastScheduledTime() < MIN_SCHEDULE_INTERVAL_MS) {
                // delay this schedule, to void too many failure
                needScheduleTasksQueue.put(routineLoadTaskInfo);
                return;
            }
            scheduleOneTask(routineLoadTaskInfo);
        } catch (InterruptedException e) {
            LOG.warn("Taking routine load task from queue has been interrupted", e);
            return;
        }
    }

    private void scheduleOneTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws InterruptedException,
            UserException {
        routineLoadTaskInfo.setLastScheduledTime(System.currentTimeMillis());
        // check if task has been abandoned
        if (!routineLoadManager.checkTaskInJob(routineLoadTaskInfo.getId())) {
            // task has been abandoned while renew task has been added in queue
            // or database has been deleted
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                             .add("error_msg", "task has been abandoned when scheduling task")
                             .build());
            return;
        }

        // begin txn and allocate task to be
        try {
            routineLoadTaskInfo.beginTxn();
            allocateTaskToBe(routineLoadTaskInfo);
        } catch (LoadException | LabelAlreadyUsedException | BeginTransactionException | AnalysisException e) {
            // todo(ml): if cluster has been deleted, the job will be cancelled.
            needScheduleTasksQueue.put(routineLoadTaskInfo);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                             .add("error_msg", "put task to the rear of queue with error " + e.getMessage())
                             .build(), e);
            return;
        }

        // task to thrift
        TRoutineLoadTask tRoutineLoadTask = null;
        try {
            tRoutineLoadTask = routineLoadTaskInfo.createRoutineLoadTask();
        } catch (UserException e) {
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.PAUSED, "failed to create task: " + e.getMessage(), false);
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
    }

    private void updateBackendSlotIfNecessary() {
        long currentTime = System.currentTimeMillis();
        if (lastBackendSlotUpdateTime == -1
                || (currentTime - lastBackendSlotUpdateTime > BACKEND_SLOT_UPDATE_INTERVAL_MS)) {
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

    private void submitBatchTasksIfNotEmpty(Map<Long, List<TRoutineLoadTask>> beIdToRoutineLoadTask) {
        for (Map.Entry<Long, List<TRoutineLoadTask>> entry : beIdToRoutineLoadTask.entrySet()) {
            Backend backend = Catalog.getCurrentSystemInfo().getBackend(entry.getKey());
            if (backend == null) {
                LOG.warn("failed to send tasks to be {} when backend is unavailable", entry.getKey());
                continue;
            }
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
            boolean ok = false;
            BackendService.Client client = null;
            try {
                client = ClientPool.backendPool.borrowObject(address);
                client.submit_routine_load_task(entry.getValue());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} tasks sent to be {}", entry.getValue().size(), entry.getKey());
                }
                ok = true;
                entry.getValue().clear();
            } catch (Exception e) {
                LOG.warn("task send error. backend[{}]", entry.getKey(), e);
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
    private void allocateTaskToBe(RoutineLoadTaskInfo routineLoadTaskInfo) throws LoadException {
        if (routineLoadTaskInfo.getPreviousBeId() != -1L) {
            if (routineLoadManager.checkBeToTask(routineLoadTaskInfo.getPreviousBeId(), routineLoadTaskInfo.getClusterName())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
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
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                              .add("job_id", routineLoadTaskInfo.getJobId())
                              .add("be_id", routineLoadTaskInfo.getBeId())
                              .add("msg", "task has been allocated to be")
                              .build());
        }
    }
}
