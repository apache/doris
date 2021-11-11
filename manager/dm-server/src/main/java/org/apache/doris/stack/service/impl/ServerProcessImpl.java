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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.component.AgentComponent;
import org.apache.doris.stack.component.AgentRoleComponent;
import org.apache.doris.stack.component.ProcessInstanceComponent;
import org.apache.doris.stack.component.TaskInstanceComponent;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.model.request.AgentInstallReq;
import org.apache.doris.stack.model.request.AgentRegister;
import org.apache.doris.stack.model.request.TestConnectionReq;
import org.apache.doris.stack.model.response.TestConnectionResp;
import org.apache.doris.stack.model.task.AgentInstall;
import org.apache.doris.stack.runner.TaskExecutor;
import org.apache.doris.stack.service.ServerProcess;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.shell.SSH;
import org.apache.doris.stack.util.TelnetUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * server
 **/
@Service
@Slf4j
public class ServerProcessImpl implements ServerProcess {

    @Autowired
    private AgentComponent agentComponent;

    @Autowired
    private AgentRoleComponent agentRoleComponent;

    @Autowired
    private ProcessInstanceComponent processInstanceComponent;

    @Autowired
    private TaskInstanceComponent taskInstanceComponent;

    @Autowired
    private AgentCache agentCache;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private TaskExecutor taskExecutor;

    @Override
    public int installAgent(HttpServletRequest request, HttpServletResponse response, AgentInstallReq installReq) throws Exception {
        Preconditions.checkArgument(StringUtils.isNotBlank(installReq.getInstallDir()), "agent install dir not empty!");
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        ProcessInstanceEntity processInstance = new ProcessInstanceEntity(installReq.getClusterId(), userId, ProcessTypeEnum.INSTALL_AGENT, installReq.getPackageUrl(), installReq.getInstallDir());
        int processId = processInstanceComponent.saveProcess(processInstance);
        //install agent for per host
        for (String host : installReq.getHosts()) {
            TaskInstanceEntity installAgent = taskInstanceComponent.saveTask(processId, host, ProcessTypeEnum.INSTALL_AGENT, TaskTypeEnum.INSTALL_AGENT, ExecutionStatus.SUBMITTED);
            if (installAgent == null) {
                continue;
            }
            AgentInstall agentInstall = new AgentInstall(host, installReq);
            installAgent.setTaskJson(JSON.toJSONString(agentInstall));
            taskExecutor.execTask(installAgent, agentInstall);
            //save agent
            agentComponent.saveAgent(new AgentEntity(host, installReq.getInstallDir(), AgentStatus.INIT, installReq.getClusterId()));
            log.info("host {} installing agent.", host);
        }
        return processId;
    }

    @Override
    public List<TestConnectionResp> testConnection(HttpServletRequest request, HttpServletResponse response, TestConnectionReq testConReq) {
        Preconditions.checkArgument(ObjectUtils.isNotEmpty(testConReq.getHosts()), "host is empty!");
        final String checkJavaHome = "java -version && echo $JAVA_HOME";
        File sshKeyFile = SSH.buildSshKeyFile();
        SSH.writeSshKeyFile(testConReq.getSshKey(), sshKeyFile);
        List<TestConnectionResp> result = Lists.newArrayList();
        for (String host : testConReq.getHosts()) {
            TestConnectionResp testResp = new TestConnectionResp();
            testResp.setHost(host);
            testResp.setStatus(true);
            if (!TelnetUtil.telnet(host, testConReq.getSshPort())) {
                testResp.setStatus(false);
                testResp.setErrorResponse("Connect failed " + host + ":" + testConReq.getSshPort());
            } else {
                SSH ssh = new SSH(testConReq.getUser(), testConReq.getSshPort(),
                        sshKeyFile.getAbsolutePath(), host, checkJavaHome);
                if (!ssh.run()) {
                    String errorResponse = ssh.getErrorResponse();
                    testResp.setStatus(false);
                    testResp.setErrorResponse(errorResponse);
                }
            }
            result.add(testResp);
        }
        return result;
    }

    @Override
    public List<AgentEntity> agentList(int clusterId) {
        List<AgentEntity> agentEntities = agentComponent.queryAgentNodes(clusterId);
        return agentEntities;
    }

    @Override
    public List<AgentRoleEntity> roleList(int clusterId) {
        List<AgentRoleEntity> agentRoleEntities = agentRoleComponent.queryAgentRoles(clusterId);
        return agentRoleEntities;
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
        if (agentEntity == null) {
            agentEntity = new AgentEntity(agent.getHost(), agent.getPort(), agent.getInstallDir(), AgentStatus.REGISTER);
        } else if (AgentStatus.INIT.equals(agentEntity.getStatus())) {
            agentEntity.setStatus(AgentStatus.REGISTER);
            agentEntity.setPort(agent.getPort());
            agentEntity.setInstallDir(agent.getInstallDir());
            agentEntity.setRegisterTime(new Date());
        } else {
            log.warn("agent already register");
            return true;
        }
        agentCache.putAgent(agentComponent.saveAgent(agentEntity));
        log.info("agent register success");
        return true;
    }
}
