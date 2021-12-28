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

package org.apache.doris.stack.component;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.doris.manager.common.domain.CommandResult;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.TaskResult;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@Component
@Slf4j
public class TaskInstanceComponent {

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    /**
     * The same host, the same tasktype, can only have one in a install process
     */
    public boolean checkTaskRunning(int processId, String host, ProcessTypeEnum processType, TaskTypeEnum taskType) {
        TaskInstanceEntity taskEntity = taskInstanceRepository.queryTask(processId, host, processType, taskType);
        if (taskEntity == null) {
            return false;
        } else if (taskEntity.getStatus().typeIsRunning()) {
            log.warn("task {} already running in host {}", taskType.name(), host);
            return true;
        } else {
            taskInstanceRepository.deleteById(taskEntity.getId());
            return false;
        }
    }

    /**
     * If the same task is already running on the host, skip it
     */
    public TaskInstanceEntity saveTask(int processId, String host, ProcessTypeEnum processType, TaskTypeEnum taskType, ExecutionStatus status) {
        if (!checkTaskRunning(processId, host, processType, taskType)) {
            return taskInstanceRepository.save(new TaskInstanceEntity(processId, host, processType, taskType, status));
        } else {
            log.warn("task {} is already running on the host {}", taskType, host);
            return null;
        }
    }

    public TaskInstanceEntity saveTask(TaskInstanceEntity task) {
        return saveTask(task.getProcessId(), task.getHost(), task.getProcessType(), task.getTaskType(), task.getStatus());
    }

    /**
     * refresh task status
     */
    public TaskInstanceEntity refreshTask(TaskInstanceEntity taskInstance, RResult result) {
        if (result == null || result.getData() == null) {
            taskInstance.setStatus(ExecutionStatus.FAILURE);
        } else {
            CommandResult commandResult = JSON.parseObject(JSON.toJSONString(result.getData()), CommandResult.class);
            if (commandResult == null) {
                taskInstance.setStatus(ExecutionStatus.FAILURE);
            } else {
                TaskResult taskResult = commandResult.getTaskResult();
                if (taskResult == null) {
                    taskInstance.setStatus(ExecutionStatus.FAILURE);
                } else {
                    taskInstance.setExecutorId(taskResult.getTaskId());
                    if (taskResult.getTaskState().typeIsRunning()) {
                        taskInstance.setStatus(ExecutionStatus.RUNNING);
                    } else if (taskResult.getRetCode() != null && taskResult.getRetCode() == 0) {
                        taskInstance.setStatus(ExecutionStatus.SUCCESS);
                        taskInstance.setFinish(Flag.YES);
                        taskInstance.setEndTime(new Date());
                    } else {
                        taskInstance.setStatus(ExecutionStatus.FAILURE);
                        taskInstance.setEndTime(new Date());
                    }
                }
            }
        }
        return taskInstanceRepository.save(taskInstance);
    }

    /**
     * Check whether the task is successful
     */
    public boolean checkTaskSuccess(int processId, ProcessTypeEnum processType) {
        List<TaskInstanceEntity> taskInstanceEntities = taskInstanceRepository.queryTasksByProcessStep(processId, processType);
        if (ObjectUtils.isEmpty(taskInstanceEntities)) {
            log.error("parent step {} has no task", processType.name());
            return false;
        }
        for (TaskInstanceEntity task : taskInstanceEntities) {
            if (Flag.NO.equals(task.getFinish())) {
                log.error("task {} is unfinish", task.getTaskType());
                return false;
            }
        }
        return true;
    }

    /**
     * query task by id
     */
    public TaskInstanceEntity queryTaskById(int taskId) {
        Optional<TaskInstanceEntity> optional = taskInstanceRepository.findById(taskId);
        if (optional.isPresent()) {
            return optional.get();
        }
        return null;
    }

    /**
     * query running task
     */
    public List<TaskInstanceEntity> queryRunningTasks(int processId, TaskTypeEnum taskType) {
        List<TaskInstanceEntity> runningTask = Lists.newArrayList();
        List<TaskInstanceEntity> tasks = taskInstanceRepository.queryTasks(processId, taskType);
        for (TaskInstanceEntity task : tasks) {
            if (task.getStatus().typeIsRunning()) {
                runningTask.add(task);
            }
        }
        return runningTask;
    }

    /**
     * query success task
     */
    public List<TaskInstanceEntity> querySuccessTasks(int processId, TaskTypeEnum taskType) {
        List<TaskInstanceEntity> successTask = Lists.newArrayList();
        List<TaskInstanceEntity> tasks = taskInstanceRepository.queryTasks(processId, taskType);
        for (TaskInstanceEntity task : tasks) {
            if (task.getStatus().typeIsSuccess()) {
                successTask.add(task);
            }
        }
        return successTask;
    }
}
