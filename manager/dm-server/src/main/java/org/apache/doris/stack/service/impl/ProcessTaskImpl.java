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

package org.apache.doris.stack.service.impl;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.component.ProcessInstanceComponent;
import org.apache.doris.stack.component.TaskInstanceComponent;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.response.CurrentProcessResp;
import org.apache.doris.stack.model.response.TaskInstanceResp;
import org.apache.doris.stack.model.task.AgentInstall;
import org.apache.doris.stack.model.task.BeJoin;
import org.apache.doris.stack.runner.TaskExecuteRunner;
import org.apache.doris.stack.service.ProcessTask;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

/**
 * agent task service
 **/
@Service
@Slf4j
public class ProcessTaskImpl implements ProcessTask {

    @Autowired
    private AgentRest agentRest;

    @Autowired
    private AgentCache agentCache;

    @Autowired
    private ProcessInstanceComponent processInstanceComponent;

    @Autowired
    private TaskInstanceComponent taskInstanceComponent;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private TaskExecuteRunner taskExecuteRunner;

    @Override
    public CurrentProcessResp currentProcess(HttpServletRequest request, HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processEntity = processInstanceComponent.queryProcessByuserId(userId);
        CurrentProcessResp processResp = null;
        if (processEntity != null) {
            List<TaskInstanceEntity> taskEntities = taskInstanceRepository.queryTasksByProcessStep(processEntity.getId(), processEntity.getProcessType());
            boolean hasUnFinishTask = false;
            for (TaskInstanceEntity task : taskEntities) {
                if (!task.getFinish().typeIsYes()) {
                    hasUnFinishTask = true;
                    break;
                }
            }
            processResp = processEntity.transToCurrentResp();
            if (!hasUnFinishTask && !processEntity.getProcessType().endProcess()) {
                processResp.setProcessStep(processResp.getProcessStep() + 1);
            }
        }
        return processResp;
    }

    @Override
    public List<TaskInstanceEntity> processProgress(HttpServletRequest request, HttpServletResponse response, int processId) {
        ProcessInstanceEntity processInstance = processInstanceComponent.queryProcessById(processId);
        if (processInstance == null) {
            log.error("The process {} does not exist", processId);
            throw new ServerException("The process does not exist");
        }
        refreshAgentTaskStatus(processId);
        List<TaskInstanceEntity> taskEntities = taskInstanceRepository.queryTasksByProcessId(processId);
        return taskEntities;
    }

    @Override
    public List<TaskInstanceResp> taskProgress(HttpServletRequest request, HttpServletResponse response, int processId) {
        ProcessInstanceEntity processEntity = processInstanceComponent.queryProcessById(processId);
        Preconditions.checkArgument(processEntity != null, "can not find processId " + processId);
        refreshAgentTaskStatus(processId);
        List<TaskInstanceEntity> taskEntities = taskInstanceRepository.queryTasksByProcessStep(processId, processEntity.getProcessType());
        List<TaskInstanceResp> resultTasks = Lists.newArrayList();
        for (TaskInstanceEntity task : taskEntities) {
            if (task.getSkip().typeIsYes()) {
                continue;
            }
            TaskInstanceResp taskResp = task.transToModel();
            if (taskResp.getStatus() == ExecutionStatus.SUBMITTED) {
                taskResp.setStatus(ExecutionStatus.RUNNING);
            }
            //set response
            if (task.getStatus().typeIsSuccess()) {
                taskResp.setResponse(task.getTaskType().getName() + " success");
            } else if (task.getStatus().typeIsFailure()) {
                if (StringUtils.isBlank(task.getResult())) {
                    taskResp.setResponse(task.getTaskType().getName() + " fail");
                } else {
                    taskResp.setResponse(task.getResult());
                }
            }
            taskResp.setTaskRole(task.getTaskType().parseTaskRole());
            resultTasks.add(taskResp);
        }
        return resultTasks;
    }

    @Override
    public void refreshAgentTaskStatus(int processId) {
        List<TaskInstanceEntity> taskEntities = taskInstanceRepository.queryTasksByProcessId(processId);
        for (TaskInstanceEntity task : taskEntities) {
            if (task.getTaskType().agentTask()
                    && task.getStatus().typeIsRunning()
                    && StringUtils.isNotBlank(task.getExecutorId())) {
                RResult rResult = agentRest.taskInfo(task.getHost(), agentPort(task.getHost()), task.getExecutorId());
                taskInstanceComponent.refreshTask(task, rResult);
            }
        }
    }

    @Override
    public void installComplete(HttpServletRequest request, HttpServletResponse response, int processId) throws Exception {
        authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processInstance = processInstanceComponent.queryProcessById(processId);
        Preconditions.checkNotNull(processInstance, "install process is not exist");
        processInstance.setStatus(ExecutionStatus.SUCCESS);
        processInstanceComponent.finishProcess(processInstance);
    }

    @Override
    public void cancelProcess(HttpServletRequest request, HttpServletResponse response, int processId) throws Exception {
        authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processInstance = processInstanceComponent.queryProcessById(processId);
        Preconditions.checkNotNull(processInstance, "install process is not exist");
        processInstance.setStatus(ExecutionStatus.FAILURE);
        processInstanceComponent.finishProcess(processInstance);
    }

    @Override
    public void skipTask(int taskId) {
        TaskInstanceEntity taskEntity = taskInstanceComponent.queryTaskById(taskId);
        Preconditions.checkNotNull(taskEntity, "taskId " + taskId + " not exist");
        Preconditions.checkArgument(!taskEntity.getFinish().typeIsYes(), "task is finish,can not skip");
        Preconditions.checkArgument(!taskEntity.getStatus().typeIsRunning(), "task is running,can not skip");
        if (taskEntity.getStatus().typeIsFailure()) {
            taskEntity.setFinish(Flag.YES);
            taskEntity.setSkip(Flag.YES);
            taskInstanceRepository.save(taskEntity);
        }
    }

    @Override
    public void retryTask(int taskId) {
        TaskInstanceEntity taskEntity = taskInstanceComponent.queryTaskById(taskId);
        Preconditions.checkNotNull(taskEntity, "task not exist");
        Preconditions.checkArgument(!taskEntity.getFinish().typeIsYes(), "task is finish,can not retry");
        Preconditions.checkArgument(!taskEntity.getStatus().typeIsRunning(), "task is running,can not retry");
        TaskTypeEnum taskType = taskEntity.getTaskType();
        switch (taskType) {
            case INSTALL_AGENT:
                taskExecuteRunner.execTask(taskEntity, JSON.parseObject(taskEntity.getTaskJson(), AgentInstall.class));
                break;
            case JOIN_BE:
                taskExecuteRunner.execTask(taskEntity, JSON.parseObject(taskEntity.getTaskJson(), BeJoin.class));
                break;
            case INSTALL_FE:
            case INSTALL_BE:
            case START_FE:
            case START_BE:
            case STOP_FE:
            case STOP_BE:
            case DEPLOY_BE_CONFIG:
            case DEPLOY_FE_CONFIG:
                RResult result = taskExecuteRunner.execAgentTask(taskEntity, JSON.parseObject(taskEntity.getTaskJson(), CommandRequest.class));
                taskInstanceComponent.refreshTask(taskEntity, result);
                break;
            default:
                throw new ServerException("can not support this task type " + taskType);
        }
        log.info("task {} execute retry success", taskId);
    }

    @Override
    public Object taskInfo(int taskId) {
        TaskInstanceEntity taskEntity = taskInstanceComponent.queryTaskById(taskId);
        Preconditions.checkNotNull(taskId, "taskId not exist");
        if (taskEntity.getTaskType().agentTask()) {
            RResult result = agentRest.taskInfo(taskEntity.getHost(), agentPort(taskEntity.getHost()), taskEntity.getExecutorId());
            return taskInstanceComponent.refreshTask(taskEntity, result);
        } else {
            return taskEntity;
        }
    }

    @Override
    public Object taskLog(int taskId) {
        TaskInstanceEntity taskEntity = taskInstanceComponent.queryTaskById(taskId);
        Preconditions.checkNotNull("taskId {} not exist", taskId);
        if (taskEntity.getTaskType().agentTask()) {
            RResult result = agentRest.taskLog(taskEntity.getHost(), agentPort(taskEntity.getHost()), taskEntity.getExecutorId());
            return result.getData();
        } else {
            Map<String, Object> result = Maps.newHashMap();
            result.put("log", taskEntity.getResult());
            return result;
        }
    }

    @Transactional
    @Override
    public void backPrevious(int processId) {
        ProcessInstanceEntity processEntity = processInstanceComponent.queryProcessById(processId);
        Preconditions.checkNotNull(processEntity, "process not exist");
        ProcessTypeEnum processType = processEntity.getProcessType();
        Preconditions.checkArgument(processType == ProcessTypeEnum.START_SERVICE, "process can not back");

        //remove history task
        List<TaskInstanceEntity> startServiceTasks = taskInstanceRepository.queryTasksByProcessStep(processEntity.getId(), ProcessTypeEnum.START_SERVICE);
        taskInstanceRepository.deleteAll(startServiceTasks);

        ProcessTypeEnum parent = ProcessTypeEnum.findParent(processType);
        processEntity.setProcessType(parent);
        processInstanceComponent.saveProcess(processEntity);
    }

    private int agentPort(String host) {
        AgentEntity agent = agentCache.agentInfo(host);
        if (agent == null) {
            throw new ServerException("query agent port fail");
        }
        return agent.getPort();
    }
}
