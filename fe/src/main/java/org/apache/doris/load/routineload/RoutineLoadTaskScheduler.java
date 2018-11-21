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
import org.apache.doris.common.util.Daemon;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;

/**
 * Routine load task scheduler is a function which allocate task to be.
 * Step1: get total idle task num of backends.
 * Step2: equally divide to be
 * Step3: submit tasks to be
 */
public class RoutineLoadTaskScheduler extends Daemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadTaskScheduler.class);

    private RoutineLoadManager routineLoadManager = Catalog.getInstance().getRoutineLoadInstance();

    @Override
    protected void runOneCycle() {
        try {
            process();
        } catch (Throwable e) {
            LOG.error("Failed to process one round of RoutineLoadTaskScheduler with error message {}",
                    e.getMessage(), e);
        }
    }

    private void process() {
        // update current beIdMaps for tasks
        routineLoadManager.updateBeIdTaskMaps();

        // check timeout tasks
        routineLoadManager.processTimeOutTasks();

        // get idle be task num
        int clusterIdleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        int scheduledTaskNum = 0;
        Queue<RoutineLoadTaskInfo> routineLoadTaskList = routineLoadManager.getNeedSchedulerRoutineLoadTasks();
        AgentBatchTask batchTask = new AgentBatchTask();

        // allocate task to be
        while (clusterIdleSlotNum > 0) {
            RoutineLoadTaskInfo routineLoadTaskInfo = routineLoadTaskList.poll();
            // queue is not empty
            if (routineLoadTaskInfo != null) {
                // when routine load task is not abandoned
                if (routineLoadManager.getIdToRoutineLoadTask().get(routineLoadTaskInfo.getSignature()) != null) {
                    long beId = routineLoadManager.getMinTaskBeId();
                    RoutineLoadJob routineLoadJob = routineLoadManager.getJobByTaskId(routineLoadTaskInfo.getSignature());
                    RoutineLoadTask routineLoadTask = routineLoadJob.createTask(routineLoadTaskInfo, beId);
                    if (routineLoadTask != null) {
                        routineLoadTaskInfo.setLoadStartTimeMs(System.currentTimeMillis());
                        AgentTaskQueue.addTask(routineLoadTask);
                        batchTask.addTask(routineLoadTask);
                        clusterIdleSlotNum--;
                        scheduledTaskNum++;
                        routineLoadManager.addNumOfConcurrentTasksByBeId(beId);
                    }
                } else {
                    LOG.debug("Task {} for job has been already discarded", routineLoadTaskInfo.getSignature());
                }
            } else {
                LOG.debug("The queue of need scheduler tasks is empty.");
                break;
            }
        }
        LOG.info("{} tasks have bean allocated to be. There are {} remaining idle slot in cluster.",
                scheduledTaskNum, routineLoadManager.getClusterIdleSlotNum());

        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
    }
}
