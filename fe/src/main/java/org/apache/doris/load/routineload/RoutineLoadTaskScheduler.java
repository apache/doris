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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRoutineLoadTask;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

    private RoutineLoadManager routineLoadManager;

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
            LOG.warn("Failed to process one round of RoutineLoadTaskScheduler with error message {}",
                     e.getMessage(), e);
        }
    }

    private void process() throws LoadException, UserException, InterruptedException {
        LinkedBlockingQueue<RoutineLoadTaskInfo> needScheduleTasksQueue =
                (LinkedBlockingQueue) routineLoadManager.getNeedScheduleTasksQueue();
        // update current beIdMaps for tasks
        routineLoadManager.updateBeIdTaskMaps();

        LOG.info("There are {} need schedule task in queue when {}",
                 needScheduleTasksQueue.size(), System.currentTimeMillis());
        Map<Long, List<TRoutineLoadTask>> beIdTobatchTask = Maps.newHashMap();
        int sizeOfTasksQueue = needScheduleTasksQueue.size();
        int clusterIdleSlotNum = routineLoadManager.getClusterIdleSlotNum();
        int needScheduleTaskNum = sizeOfTasksQueue < clusterIdleSlotNum ? sizeOfTasksQueue : clusterIdleSlotNum;
        int scheduledTaskNum = 0;
        // get idle be task num
        // allocate task to be
        if (needScheduleTaskNum == 0) {
            Thread.sleep(1000);
            return;
        }
        while (needScheduleTaskNum > 0) {
            // allocate be to task and begin transaction for task
            RoutineLoadTaskInfo routineLoadTaskInfo = needScheduleTasksQueue.peek();
            RoutineLoadJob routineLoadJob = null;
            try {
                routineLoadJob = routineLoadManager.getJobByTaskId(routineLoadTaskInfo.getId().toString());
            } catch (MetaNotFoundException e) {
                LOG.warn("task {} has been abandoned", routineLoadTaskInfo.getId());
                return;
            }
            long beId;
            try {
                beId = routineLoadManager.getMinTaskBeId(routineLoadJob.getClusterName());
                routineLoadTaskInfo.setTxn();
            } catch (Exception e) {
                LOG.warn("put task to the rear of queue with error " + e.getMessage());
                needScheduleTasksQueue.take();
                needScheduleTasksQueue.put(routineLoadTaskInfo);
                needScheduleTaskNum--;
                continue;
            }

            // task to thrift
            try {
                routineLoadTaskInfo = needScheduleTasksQueue.take();
            } catch (InterruptedException e) {
                LOG.warn("Taking routine load task from queue has been interrupted with error msg {}",
                         e.getMessage());
                return;
            }
            TRoutineLoadTask tRoutineLoadTask = routineLoadTaskInfo.createRoutineLoadTask();
            // remove task for needScheduleTasksList in job
            routineLoadJob.removeNeedScheduleTask(routineLoadTaskInfo);
            routineLoadTaskInfo.setLoadStartTimeMs(System.currentTimeMillis());
            // add to batch task map
            if (beIdTobatchTask.containsKey(beId)) {
                beIdTobatchTask.get(beId).add(tRoutineLoadTask);
            } else {
                List<TRoutineLoadTask> tRoutineLoadTaskList = Lists.newArrayList();
                tRoutineLoadTaskList.add(tRoutineLoadTask);
                beIdTobatchTask.put(beId, tRoutineLoadTaskList);
            }
            // count
            clusterIdleSlotNum--;
            scheduledTaskNum++;
            routineLoadManager.addNumOfConcurrentTasksByBeId(beId);
            needScheduleTaskNum--;
        }
        submitBatchTask(beIdTobatchTask);
        LOG.info("{} tasks have bean allocated to be.", scheduledTaskNum);
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
                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }

        }
    }
}
