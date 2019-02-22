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
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.RoutineLoadTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Date;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private RoutineLoadManager routineLoadManager;
    private LinkedBlockingQueue<RoutineLoadTaskInfo> needSchedulerTasksQueue;

    public RoutineLoadTaskScheduler() {
        super("routine load task", 0);
        routineLoadManager = Catalog.getInstance().getRoutineLoadManager();
        needSchedulerTasksQueue = (LinkedBlockingQueue) routineLoadManager.getNeedSchedulerTasksQueue();
    }

    @Override
    protected void runOneCycle() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadTaskScheduler with error message {}",
                     e.getMessage(), e);
        }
    }

    private void process() throws LoadException {
        // update current beIdMaps for tasks
        routineLoadManager.updateBeIdTaskMaps();

        LOG.info("There are {} need scheduler task in queue when {}",
                 needSchedulerTasksQueue.size(), System.currentTimeMillis());
        AgentBatchTask batchTask = new AgentBatchTask();
        int sizeOfTasksQueue = needSchedulerTasksQueue.size();
        int clusterIdleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        int needScheduledTaskNum = sizeOfTasksQueue < clusterIdleSlotNum ? sizeOfTasksQueue : clusterIdleSlotNum;
        int scheduledTaskNum = 0;
        // get idle be task num
        // allocate task to be
        while (needScheduledTaskNum > 0) {
            RoutineLoadTaskInfo routineLoadTaskInfo = null;
            try {
                routineLoadTaskInfo = needSchedulerTasksQueue.take();
            } catch (InterruptedException e) {
                LOG.warn("Taking routine load task from queue has been interrupted with error msg {}",
                         e.getMessage());
                return;
            }

            if (clusterIdleSlotNum > 0) {
                long beId = routineLoadManager.getMinTaskBeId();
                RoutineLoadJob routineLoadJob = null;
                try {
                    routineLoadJob = routineLoadManager.getJobByTaskId(routineLoadTaskInfo.getId());
                } catch (MetaNotFoundException e) {
                    LOG.warn("task {} has been abandoned", routineLoadTaskInfo.getId());
                    return;
                }
                RoutineLoadTask routineLoadTask = routineLoadTaskInfo.createStreamLoadTask(beId);
                if (routineLoadTask != null) {
                    // remove task for needSchedulerTasksList in job
                    routineLoadJob.removeNeedSchedulerTask(routineLoadTaskInfo);
                    routineLoadTaskInfo.setLoadStartTimeMs(System.currentTimeMillis());
                    AgentTaskQueue.addTask(routineLoadTask);
                    batchTask.addTask(routineLoadTask);
                    clusterIdleSlotNum--;
                    scheduledTaskNum++;
                    routineLoadManager.addNumOfConcurrentTasksByBeId(beId);
                } else {
                    LOG.debug("Task {} for job has been already discarded", routineLoadTaskInfo.getId());
                }

            }
            needScheduledTaskNum--;
        }
        LOG.info("{} tasks have bean allocated to be.", scheduledTaskNum);

        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
    }
}
