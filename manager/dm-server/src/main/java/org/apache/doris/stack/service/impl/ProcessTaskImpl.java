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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.component.ProcessInstanceComponent;
import org.apache.doris.stack.component.TaskInstanceComponent;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.service.ProcessTask;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;
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

    @Override
    public ProcessInstanceEntity historyProgress(HttpServletRequest request, HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processEntity = processInstanceComponent.queryProcessByuserId(userId);
        return processEntity;
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
    public List<TaskInstanceEntity> taskProgress(HttpServletRequest request, HttpServletResponse response, int processId) {
        ProcessInstanceEntity processEntity = processInstanceComponent.queryProcessById(processId);
        Preconditions.checkArgument(processEntity != null, "can not find processId " + processId);
        refreshAgentTaskStatus(processId);
        return taskInstanceRepository.queryTasksByProcessStep(processId, processEntity.getProcessType());
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
        ProcessInstanceEntity processInstance = processInstanceComponent.queryProcessById(processId);
        if (processInstance != null) {
            processInstance.setFinish(Flag.YES);
            processInstance.setUpdateTime(new Date());
            processInstanceComponent.updateProcess(processInstance);
        }
    }

    @Override
    public void skipTask(int taskId) {
        TaskInstanceEntity taskEntity = taskInstanceComponent.queryTaskById(taskId);
        Preconditions.checkNotNull("taskId {} not exist", taskId);
        if (taskEntity.getStatus().typeIsFailure()) {
            taskEntity.setFinish(Flag.YES);
            taskInstanceRepository.save(taskEntity);
        }
    }

    @Override
    public Object taskInfo(int taskId) {
        TaskInstanceEntity taskEntity = taskInstanceComponent.queryTaskById(taskId);
        Preconditions.checkNotNull("taskId {} not exist", taskId);
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

    private int agentPort(String host) {
        AgentEntity agent = agentCache.agentInfo(host);
        if (agent == null) {
            throw new ServerException("query agent port fail");
        }
        return agent.getPort();
    }
}
