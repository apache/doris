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
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.routineload.RoutineLoadJob.JobState;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRoutineLoadTask;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Routine load task scheduler is a function which allocate task to be.
 * Step1: update backend slot if interval more then BACKEND_SLOT_UPDATE_INTERVAL_MS
 * Step2: submit beIdToBatchTask when queue is empty
 * Step3: take a task from queue and schedule this task
 *
 * The scheduler will be blocked in step3 till the queue receive a new task
 */
public class RoutineLoadTaskScheduler extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskScheduler.class);

    private static final long BACKEND_SLOT_UPDATE_INTERVAL_MS = 10000; // 10s
    private static final long SLOT_FULL_SLEEP_MS = 10000; // 10s

    private RoutineLoadManager routineLoadManager;
    private LinkedBlockingQueue<RoutineLoadTaskInfo> needScheduleTasksQueue = Queues.newLinkedBlockingQueue();

    private long lastBackendSlotUpdateTime = -1;

    @VisibleForTesting
    public RoutineLoadTaskScheduler() {
        super("Routine load task scheduler", 0);
        this.routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();
    }

    public RoutineLoadTaskScheduler(RoutineLoadManager routineLoadManager) {
        super("Routine load task scheduler", 0);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadTaskScheduler", e);
        }
    }

    private void process() throws UserException, InterruptedException {
        updateBackendSlotIfNecessary();

        // if size of queue is zero, tasks will be submitted by batch
        int idleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        // scheduler will be blocked when there is no slot for task in cluster
        if (idleSlotNum == 0) {
            Thread.sleep(SLOT_FULL_SLEEP_MS);
            return;
        }

        try {
            // This step will be blocked when queue is empty
            RoutineLoadTaskInfo routineLoadTaskInfo = needScheduleTasksQueue.take();
            if (System.currentTimeMillis() - routineLoadTaskInfo.getLastScheduledTime() < routineLoadTaskInfo.getTimeoutMs()) {
                // try to delay scheduling this task for 'timeout', to void too many failure
                needScheduleTasksQueue.put(routineLoadTaskInfo);
                return;
            }
            scheduleOneTask(routineLoadTaskInfo);
        } catch (Exception e) {
            LOG.warn("Taking routine load task from queue has been interrupted", e);
            return;
        }
    }

    private void scheduleOneTask(RoutineLoadTaskInfo routineLoadTaskInfo) throws Exception {
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

        // allocate BE slot for this task.
        // this should be done before txn begin, or the txn may be begun successfully but failed to be allocated.
        try {
            if (!allocateTaskToBe(routineLoadTaskInfo)) {
                // allocate failed, push it back to the queue to wait next scheduling
                needScheduleTasksQueue.put(routineLoadTaskInfo);
            }
        } catch (UserException e) {
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).
                    updateState(JobState.PAUSED,
                    new ErrorReason(e.getErrorCode(), e.getMessage()), false);
            throw e;
        } catch (Exception e) {
            // exception happens, PAUSE the job
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR, "failed to allocate task: " + e.getMessage()), false);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId()).add("error_msg",
                    "allocate task encounter exception: " + e.getMessage()).build());
            throw e;
        }

        // begin txn
        try {
            if (!routineLoadTaskInfo.beginTxn()) {
                // begin txn failed. push it back to the queue to wait next scheduling
                // set BE id to -1 to release the BE slot
                routineLoadTaskInfo.setBeId(-1);
                needScheduleTasksQueue.put(routineLoadTaskInfo);
            }
        } catch (Exception e) {
            // exception happens, PAUSE the job
            // set BE id to -1 to release the BE slot
            routineLoadTaskInfo.setBeId(-1);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).updateState(JobState.PAUSED,
                    new ErrorReason(InternalErrorCode.CREATE_TASKS_ERR,
                            "failed to allocate task for txn: " + e.getMessage()), false);
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId()).add("error_msg",
                    "begin task txn encounter exception: " + e.getMessage()).build());
            throw e;
        }

        // create thrift object
        TRoutineLoadTask tRoutineLoadTask = null;
        try {
            long startTime = System.currentTimeMillis();
            tRoutineLoadTask = routineLoadTaskInfo.createRoutineLoadTask();
            LOG.debug("create routine load task cost(ms): {}, job id: {}", 
                    (System.currentTimeMillis() - startTime), routineLoadTaskInfo.getJobId());
        } catch (MetaNotFoundException e) {
            // this means database or table has been dropped, just stop this routine load job.
            // set BE id to -1 to release the BE slot
            routineLoadTaskInfo.setBeId(-1);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.CANCELLED,
                            new ErrorReason(InternalErrorCode.META_NOT_FOUND_ERR, "meta not found: " + e.getMessage()),
                            false);
            throw e;
        } catch (UserException e) {
            // set BE id to -1 to release the BE slot
            routineLoadTaskInfo.setBeId(-1);
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId())
                    .updateState(JobState.PAUSED,
                            new ErrorReason(e.getErrorCode(),
                                    "failed to create task: " + e.getMessage()), false);
            throw e;
        }

        try {
            long startTime = System.currentTimeMillis();
            submitTask(routineLoadTaskInfo.getBeId(), tRoutineLoadTask);
            LOG.debug("send routine load task cost(ms): {}, job id: {}", 
                    (System.currentTimeMillis() - startTime), routineLoadTaskInfo.getJobId());
        } catch (LoadException e) {
            // submit task failed (such as TOO_MANY_TASKS error), but txn has already begun.
            // Here we will still set the ExecuteStartTime of this task, which means
            // we "assume" that this task has been successfully submitted.
            // And this task will then be aborted because of a timeout.
            // In this way, we can prevent the entire job from being paused due to submit errors,
            // and we can also relieve the pressure on BE by waiting for the timeout period.
            LOG.warn("failed to submit routine load task {} to BE: {}",
                    DebugUtil.printId(routineLoadTaskInfo.getId()),
                    routineLoadTaskInfo.getBeId());
            routineLoadManager.getJob(routineLoadTaskInfo.getJobId()).setOtherMsg(e.getMessage());
            // fall through to set ExecuteStartTime
        }

        // set the executeStartTimeMs of task
        routineLoadTaskInfo.setExecuteStartTimeMs(System.currentTimeMillis());
    }

    private void updateBackendSlotIfNecessary() {
        long currentTime = System.currentTimeMillis();
        if (lastBackendSlotUpdateTime == -1
                || (currentTime - lastBackendSlotUpdateTime > BACKEND_SLOT_UPDATE_INTERVAL_MS)) {
            routineLoadManager.updateBeIdToMaxConcurrentTasks();
            lastBackendSlotUpdateTime = currentTime;
            LOG.debug("update backend max slot for routine load task scheduling. current task num per BE: {}",
                    Config.max_routine_load_task_num_per_be);
        }
    }

    public void addTaskInQueue(RoutineLoadTaskInfo routineLoadTaskInfo) {
        needScheduleTasksQueue.add(routineLoadTaskInfo);
        LOG.debug("total tasks num in routine load task queue: {}", needScheduleTasksQueue.size());
    }

    public void addTasksInQueue(List<RoutineLoadTaskInfo> routineLoadTaskInfoList) {
        needScheduleTasksQueue.addAll(routineLoadTaskInfoList);
        LOG.debug("total tasks num in routine load task queue: {}", needScheduleTasksQueue.size());
    }

    private void submitTask(long beId, TRoutineLoadTask tTask) throws LoadException {
        Backend backend = Catalog.getCurrentSystemInfo().getBackend(beId);
        if (backend == null) {
            throw new LoadException("failed to send tasks to backend " + beId + " because not exist");
        }

        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

        boolean ok = false;
        BackendService.Client client = null;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TStatus tStatus = client.submit_routine_load_task(Lists.newArrayList(tTask));
            ok = true;
           
            if (tStatus.getStatus_code() != TStatusCode.OK) {
                throw new LoadException("failed to submit task. error code: " + tStatus.getStatus_code()
                        + ", msg: " + (tStatus.getError_msgsSize() > 0 ? tStatus.getError_msgs().get(0) : "NaN"));
            }
            LOG.debug("send routine load task {} to BE: {}", DebugUtil.printId(tTask.id), beId);
        } catch (Exception e) {
            throw new LoadException("failed to send task: " + e.getMessage(), e);
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }

    // try to allocate a task to BE which has idle slot.
    // 1. First is to check if the previous allocated BE is available. If yes, allocate task to previous BE.
    // 2. If not, try to find a better one with most idle slots.
    // return true if allocate successfully. return false if failed.
    // throw exception if unrecoverable errors happen.
    private boolean allocateTaskToBe(RoutineLoadTaskInfo routineLoadTaskInfo) throws LoadException {
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
                return true;
            }
        }

        // the previous BE is not available, try to find a better one
        long beId = routineLoadManager.getMinTaskBeId(routineLoadTaskInfo.getClusterName());
        if (beId < 0) {
            return false;
        }

        routineLoadTaskInfo.setBeId(beId);
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.ROUTINE_LOAD_TASK, routineLoadTaskInfo.getId())
                              .add("job_id", routineLoadTaskInfo.getJobId())
                              .add("be_id", routineLoadTaskInfo.getBeId())
                              .add("msg", "task has been allocated to be")
                              .build());
        }
        return true;
    }
}
