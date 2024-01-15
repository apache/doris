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

package org.apache.doris.task;

import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Task queue
 */
public class AgentTaskQueue {
    private static final Logger LOG = LogManager.getLogger(AgentTaskQueue.class);

    // backend id -> (task type -> (signature -> agent task))
    private static Table<Long, TTaskType, Map<Long, AgentTask>> tasks = HashBasedTable.create();
    private static int taskNum = 0;

    public static synchronized void addBatchTask(AgentBatchTask batchTask) {
        for (AgentTask task : batchTask.getAllTasks()) {
            addTask(task);
        }
    }

    public static synchronized boolean addTask(AgentTask task) {
        long backendId = task.getBackendId();
        TTaskType type = task.getTaskType();

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        if (signatureMap == null) {
            signatureMap = Maps.newHashMap();
            tasks.put(backendId, type, signatureMap);
        }

        long signature = task.getSignature();
        if (signatureMap.containsKey(signature)) {
            return false;
        }
        signatureMap.put(signature, task);
        ++taskNum;
        LOG.debug("add task: type[{}], backend[{}], signature[{}]", type, backendId, signature);
        return true;
    }

    // remove all task in AgentBatchTask.
    // the caller should make sure all tasks in AgentBatchTask is type of 'type'
    public static synchronized void removeBatchTask(AgentBatchTask batchTask, TTaskType type) {
        for (AgentTask task : batchTask.getAllTasks()) {
            removeTask(task.getBackendId(), type, task.getSignature());
        }
    }

    public static synchronized void removeTask(long backendId, TTaskType type, long signature) {
        if (!tasks.contains(backendId, type)) {
            return;
        }

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        if (!signatureMap.containsKey(signature)) {
            return;
        }
        signatureMap.remove(signature);
        LOG.debug("remove task: type[{}], backend[{}], signature[{}]", type, backendId, signature);
        --taskNum;
    }

    /*
     * we cannot define a push task with only 'backendId', 'signature' and 'TTaskType'
     * add version and TPushType to help
     */
    public static synchronized void removePushTask(long backendId, long signature, long version,
                                                   TPushType pushType, TTaskType taskType) {
        if (!tasks.contains(backendId, taskType)) {
            return;
        }

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, taskType);
        AgentTask task = signatureMap.get(signature);
        if (task == null) {
            return;
        }

        PushTask pushTask = (PushTask) task;
        if (pushTask.getVersion() != version || pushTask.getPushType() != pushType) {
            return;
        }

        signatureMap.remove(signature);
        LOG.debug("remove task: type[{}], backend[{}], signature[{}]", taskType, backendId, signature);
        --taskNum;
    }

    public static synchronized void removeTaskOfType(TTaskType type, long signature) {
        // be id -> (signature -> task)
        Map<Long, Map<Long, AgentTask>> map = tasks.column(type);
        for (Map<Long, AgentTask> innerMap : map.values()) {
            innerMap.remove(signature);
        }
    }

    public static synchronized AgentTask getTask(long backendId, TTaskType type, long signature) {
        if (!tasks.contains(backendId, type)) {
            return null;
        }

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        return signatureMap.get(signature);
    }

    public static synchronized void updateTask(long backendId, TTaskType type, long signature, AgentTask newTask) {
        if (!tasks.contains(backendId, type)) {
            return;
        }

        Map<Long, AgentTask> signatureMap = tasks.get(backendId, type);
        if (!signatureMap.containsKey(signature)) {
            return;
        }
        signatureMap.put(signature, newTask);
    }

    // this is just for unit test
    public static synchronized List<AgentTask> getTask(TTaskType type) {
        List<AgentTask> res = Lists.newArrayList();
        for (Map<Long, AgentTask> agentTasks : tasks.column(TTaskType.ALTER).values()) {
            res.addAll(agentTasks.values());
        }
        return res;
    }

    public static synchronized List<AgentTask> getTask(long dbId, TTaskType type) {
        Map<Long, Map<Long, AgentTask>> taskMap = tasks.column(type);
        Map<Long, AgentTask> signatureMap = taskMap == null ? null : taskMap.get(dbId);
        return signatureMap == null ? new ArrayList<>() : new ArrayList<>(signatureMap.values());
    }

    public static synchronized List<AgentTask> getDiffTasks(long backendId, Map<TTaskType, Set<Long>> runningTasks) {
        List<AgentTask> diffTasks = new ArrayList<AgentTask>();
        if (!tasks.containsRow(backendId)) {
            return diffTasks;
        }

        Map<TTaskType, Map<Long, AgentTask>> backendAllTasks = tasks.row(backendId);
        for (Map.Entry<TTaskType, Map<Long, AgentTask>> entry : backendAllTasks.entrySet()) {
            TTaskType taskType = entry.getKey();
            Map<Long, AgentTask> tasks = entry.getValue();
            Set<Long> excludeSignatures = new HashSet<Long>();
            if (runningTasks.containsKey(taskType)) {
                excludeSignatures = runningTasks.get(taskType);
            }

            for (Map.Entry<Long, AgentTask> taskEntry : tasks.entrySet()) {
                long signature = taskEntry.getKey();
                AgentTask task = taskEntry.getValue();
                if (!excludeSignatures.contains(signature)) {
                    diffTasks.add(task);
                }
            } // end for tasks
        } // end for backendAllTasks

        return diffTasks;
    }

    public static synchronized void removeReplicaRelatedTasks(long backendId, long tabletId) {
        if (!tasks.containsRow(backendId)) {
            return;
        }

        Map<TTaskType, Map<Long, AgentTask>> backendTasks = tasks.row(backendId);
        for (TTaskType type : TTaskType.values()) {
            if (backendTasks.containsKey(type)) {
                Map<Long, AgentTask> typeTasks = backendTasks.get(type);
                if (type == TTaskType.REALTIME_PUSH) {
                    Iterator<AgentTask> taskIterator = typeTasks.values().iterator();
                    while (taskIterator.hasNext()) {
                        PushTask realTimePushTask = (PushTask) taskIterator.next();
                        if (tabletId == realTimePushTask.getTabletId()) {
                            taskIterator.remove();
                        }
                    }
                } else {
                    if (typeTasks.containsKey(tabletId)) {
                        typeTasks.remove(tabletId);
                        LOG.debug("remove task: type[{}], backend[{}], signature[{}]", type, backendId, tabletId);
                        --taskNum;
                    }
                }
            }
        } // end for types
    }

    // only for test now
    public static synchronized void clearAllTasks() {
        tasks.clear();
        taskNum = 0;
    }

    public static synchronized int getTaskNum() {
        return taskNum;
    }

    public static synchronized int getTaskNum(long backendId, TTaskType type, boolean isFailed) {
        int taskNum = 0;
        if (backendId != -1) {
            Map<Long, AgentTask> taskMap = tasks.get(backendId, type);
            if (taskMap != null) {
                if (isFailed) {
                    for (AgentTask task : taskMap.values()) {
                        if (task.getFailedTimes() > 0) {
                            ++taskNum;
                        }
                    }
                } else {
                    taskNum += taskMap.size();
                }
            }
        } else {
            Map<Long, Map<Long, AgentTask>> taskMap = tasks.column(type);
            if (taskMap != null) {
                for (Map<Long, AgentTask> signatureMap : taskMap.values()) {
                    if (isFailed) {
                        for (AgentTask task : signatureMap.values()) {
                            if (task.getFailedTimes() > 0) {
                                ++taskNum;
                            }
                        }
                    } else {
                        taskNum += signatureMap.size();
                    }
                }
            }
        }

        LOG.info("get task num with type[{}] in backend[{}]: {}. isFailed: {}",
                 type.name(), backendId, taskNum, isFailed);
        return taskNum;
    }

    public static synchronized List<AgentTask> getFailedTask(long backendId, TTaskType type) {
        Map<Long, AgentTask> taskMap = tasks.get(backendId, type);
        List<AgentTask> tasks = Lists.newArrayList();
        if (taskMap != null) {
            for (AgentTask task : taskMap.values()) {
                if (task.getFailedTimes() > 0) {
                    tasks.add(task);
                }
            }
        }
        return tasks;
    }
}
