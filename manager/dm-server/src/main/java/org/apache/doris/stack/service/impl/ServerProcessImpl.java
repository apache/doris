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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.component.AgentComponent;
import org.apache.doris.stack.component.AgentRoleComponent;
import org.apache.doris.stack.component.ProcessInstanceComponent;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.AgentInstall;
import org.apache.doris.stack.model.request.AgentInstallReq;
import org.apache.doris.stack.model.request.AgentRegister;
import org.apache.doris.stack.model.request.TaskInfoReq;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.runner.TaskExecCallback;
import org.apache.doris.stack.runner.TaskExecuteThread;
import org.apache.doris.stack.service.AgentTask;
import org.apache.doris.stack.service.ServerProcess;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * server
 **/
@Service
@Slf4j
public class ServerProcessImpl implements ServerProcess {

    /**
     * thread executor service
     */
    private final ListeningExecutorService taskExecService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

    @Autowired
    private AgentTask agentTask;

    @Autowired
    private AgentComponent agentComponent;

    @Autowired
    private AgentRoleComponent agentRoleComponent;

    @Autowired
    private ProcessInstanceComponent processInstanceComponent;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private AgentCache agentCache;

    @Autowired
    private AuthenticationService authenticationService;

    @Override
    public int historyProgress(HttpServletRequest request, HttpServletResponse response) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processEntity = processInstanceComponent.queryProcessByuserId(userId);
        if (processEntity != null) {
            return processEntity.getProcessType().getCode();
        } else {
            return -1;
        }
    }

    @Override
    public void processProgress(HttpServletRequest request, HttpServletResponse response, int processId) {
        ProcessInstanceEntity processInstance = processInstanceComponent.queryProcessById(processId);
        if (processInstance == null) {
            log.error("The process {} does not exist", processId);
            throw new ServerException("The process does not exist");
        }
        refreshAgentTaskStatus(processId);
    }

    @Override
    public void refreshAgentTaskStatus(int processId) {
        List<TaskInstanceEntity> taskEntities = taskInstanceRepository.queryTasksByProcessId(processId);
        for (TaskInstanceEntity task : taskEntities) {
            if (task.getTaskType().agentTask()
                    && task.getStatus().typeIsRunning()) {
                RResult rResult = agentTask.taskInfo(new TaskInfoReq(task.getHost(), task.getExecutorId()));
                //update task status
            }
        }
    }

    @Override
    public void installComplete(HttpServletRequest request, HttpServletResponse response, int processId) throws Exception {
        ProcessInstanceEntity processInstance = processInstanceComponent.queryProcessById(processId);
        if (processInstance != null) {
            processInstance.setStatus(Flag.NO);
            processInstanceComponent.updateProcess(processInstance);
        }
    }

    @Override
    public void installAgent(HttpServletRequest request, HttpServletResponse response, AgentInstallReq installReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processInstance = new ProcessInstanceEntity(installReq.getClusterId(), userId, ProcessTypeEnum.INSTALL_AGENT);
        int processId = processInstanceComponent.saveProcessInstance(processInstance);
        //install agent for per host
        for (String host : installReq.getHosts()) {
            TaskInstanceEntity installAgent = new TaskInstanceEntity(processId, host, TaskTypeEnum.INSTALL_AGENT, ExecutionStatus.SUBMITTED_SUCCESS);
            taskInstanceRepository.save(installAgent);

            TaskContext taskContext = new TaskContext(TaskTypeEnum.INSTALL_AGENT, installAgent, new AgentInstall(host, installReq));
            ListenableFuture<Object> submit = taskExecService.submit(new TaskExecuteThread(taskContext));
            Futures.addCallback(submit, new TaskExecCallback(taskContext));

            //save agent
            agentComponent.saveAgent(new AgentEntity(host, installReq.getInstallDir(), AgentStatus.INIT, installReq.getClusterId()));
        }
    }

    @Override
    public List<AgentEntity> agentList() {
        List<AgentEntity> agentEntities = agentComponent.queryAgentNodes(Lists.newArrayList());
        return agentEntities;
    }

    @Override
    public List<AgentRoleEntity> agentRole(String host) {
        List<AgentRoleEntity> agentRoles = agentRoleComponent.queryAgentByHost(host);
        return agentRoles;
    }

    @Override
    public void heartbeat(String host, Integer port) {
        agentComponent.refreshAgentStatus(host, port);
    }

    @Override
    public boolean register(AgentRegister agent) {
        AgentEntity agentEntity = agentComponent.agentInfo(agent.getHost());
        if (agentEntity != null) {
            log.warn("agent already register");
            return true;
        }
        AgentEntity newAgentEntity = agentComponent.registerAgent(agent);
        agentCache.putAgent(newAgentEntity);
        log.info("agent register success");
        return true;
    }
}
