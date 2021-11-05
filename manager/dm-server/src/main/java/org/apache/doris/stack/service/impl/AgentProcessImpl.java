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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.BeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.FeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.FeStartCommandRequestBody;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.manager.common.domain.WriteBeConfCommandRequestBody;
import org.apache.doris.manager.common.domain.WriteFeConfCommandRequestBody;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.component.AgentRoleComponent;
import org.apache.doris.stack.component.ProcessInstanceComponent;
import org.apache.doris.stack.component.TaskInstanceComponent;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.constants.CmdTypeEnum;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.Flag;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.constants.TaskTypeEnum;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.BeJoin;
import org.apache.doris.stack.model.DeployConfig;
import org.apache.doris.stack.model.DorisStart;
import org.apache.doris.stack.model.InstallInfo;
import org.apache.doris.stack.model.request.BeJoinReq;
import org.apache.doris.stack.model.request.DeployConfigReq;
import org.apache.doris.stack.model.request.DorisExecReq;
import org.apache.doris.stack.model.request.DorisInstallReq;
import org.apache.doris.stack.model.request.DorisStartReq;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.runner.TaskExecCallback;
import org.apache.doris.stack.runner.TaskExecuteThread;
import org.apache.doris.stack.service.AgentProcess;
import org.apache.doris.stack.service.RestService;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * server agent
 **/
@Service
@Slf4j
public class AgentProcessImpl implements AgentProcess {

    /**
     * thread executor service
     */
    private final ListeningExecutorService taskExecService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

    @Autowired
    private AgentRest agentRest;

    @Autowired
    private AgentCache agentCache;

    @Autowired
    private AgentRoleComponent agentRoleComponent;

    @Autowired
    private ProcessInstanceComponent processInstanceComponent;

    @Autowired
    private TaskInstanceComponent taskInstanceComponent;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private RestService restService;

    @Override
    @Transactional
    public void installService(HttpServletRequest request, HttpServletResponse response,
                               DorisInstallReq installReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkHasUnfinishProcess(userId, installReq.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(installReq.getProcessId(), ProcessTypeEnum.INSTALL_SERVICE);
        Preconditions.checkArgument(success, "The agent is not installed successfully and the service cannot be installed");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(installReq.getProcessId(), ProcessTypeEnum.INSTALL_SERVICE);
        //Installed host and service
        List<String> agentRoleList = agentRoleComponent.queryAgentRoles(process.getClusterId()).stream()
                .map(m -> (m.getHost() + "-" + m.getRole()))
                .collect(Collectors.toList());
        List<InstallInfo> installInfos = installReq.getInstallInfos();
        if (installInfos == null) {
            throw new ServerException("Please specify the host configuration to be installed");
        }
        for (InstallInfo install : installInfos) {
            String key = install.getHost() + "-" + install.getRole();
            if (agentRoleList.contains(key)) {
                log.warn("agent {} already install doris {}", install.getHost(), install.getRole());
                continue;
            }
            String installDir = process.getInstallDir();
            if (!installDir.endsWith(File.separator)) {
                installDir = installDir + File.separator;
            }
            installDir = installDir + install.getRole().toLowerCase();
            installDoris(process.getId(), install, process.getPackageUrl(), installDir);

            agentRoleComponent.saveAgentRole(new AgentRoleEntity(install.getHost(), install.getRole(), install.getFeNodeType(), installDir, Flag.NO));
            log.info("agent {} installing doris {}", install.getHost(), install.getRole());
        }
    }

    private void installDoris(int processId, InstallInfo install, String packageUrl, String installDir) {
        CommandRequest creq = new CommandRequest();
        TaskInstanceEntity installService = new TaskInstanceEntity(processId, install.getHost(), ProcessTypeEnum.INSTALL_SERVICE);
        if (ServiceRole.FE.name().equals(install.getRole())) {
            FeInstallCommandRequestBody feBody = new FeInstallCommandRequestBody();
            feBody.setMkFeMetadir(true);
            feBody.setPackageUrl(packageUrl);
            feBody.setInstallDir(installDir);
            creq.setCommandType(CommandType.INSTALL_FE.name());
            creq.setBody(JSON.toJSONString(feBody));
            installService.setTaskType(TaskTypeEnum.INSTALL_FE);
        } else if (ServiceRole.BE.name().equals(install.getRole())) {
            BeInstallCommandRequestBody beBody = new BeInstallCommandRequestBody();
            beBody.setMkBeStorageDir(true);
            beBody.setPackageUrl(packageUrl);
            beBody.setInstallDir(installDir);
            creq.setCommandType(CommandType.INSTALL_BE.name());
            creq.setBody(JSON.toJSONString(beBody));
            installService.setTaskType(TaskTypeEnum.INSTALL_BE);
        } else {
            throw new ServerException("The service installation is not currently supported");
        }
        boolean isRunning = taskInstanceComponent.checkTaskRunning(installService.getProcessId(), installService.getHost(), installService.getProcessType(), installService.getTaskType());
        if (isRunning) {
            return;
        }
        RResult result = agentRest.commandExec(install.getHost(), agentPort(install.getHost()), creq);
        taskInstanceComponent.refreshTask(installService, result);
    }

    @Override
    public void deployConfig(HttpServletRequest request, HttpServletResponse response, DeployConfigReq deployConfigReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkHasUnfinishProcess(userId, deployConfigReq.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(deployConfigReq.getProcessId(), ProcessTypeEnum.DEPLOY_CONFIG);
        Preconditions.checkArgument(success, "doris is not installed successfully and the configuration cannot be delivered");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(deployConfigReq.getProcessId(), ProcessTypeEnum.DEPLOY_CONFIG);
        List<DeployConfig> deployConfigs = deployConfigReq.getDeployConfigs();
        for (DeployConfig config : deployConfigs) {
            deployConf(process.getId(), config);
            log.info("agent {} deploy {} conf", config.getHost(), config.getRole());
        }
    }

    private void deployConf(int processId, DeployConfig deployConf) {
        if (StringUtils.isBlank(deployConf.getConf())) {
            return;
        }
        Base64.Encoder encoder = Base64.getEncoder();
        try {
            deployConf.setConf(new String(encoder.encode(deployConf.getConf().getBytes()), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            log.error("conf {} can not encoding:", deployConf.getConf(), e);
        }
        CommandRequest creq = new CommandRequest();
        TaskInstanceEntity deployTask = new TaskInstanceEntity(processId, deployConf.getHost(), ProcessTypeEnum.DEPLOY_CONFIG);
        if (ServiceRole.FE.name().equals(deployConf.getRole())) {
            WriteFeConfCommandRequestBody feConf = new WriteFeConfCommandRequestBody();
            feConf.setContent(deployConf.getConf());
            feConf.setCreateMetaDir(true);
            creq.setBody(JSON.toJSONString(feConf));
            creq.setCommandType(CommandType.WRITE_FE_CONF.name());
            deployTask.setTaskType(TaskTypeEnum.DEPLOY_FE_CONFIG);
        } else if (ServiceRole.BE.name().equals(deployConf.getRole())) {
            WriteBeConfCommandRequestBody beConf = new WriteBeConfCommandRequestBody();
            beConf.setContent(deployConf.getConf());
            beConf.setCreateStorageDir(true);
            creq.setBody(JSON.toJSONString(beConf));
            creq.setCommandType(CommandType.WRITE_BE_CONF.name());
            deployTask.setTaskType(TaskTypeEnum.DEPLOY_BE_CONFIG);
        }
        boolean isRunning = taskInstanceComponent.checkTaskRunning(deployTask.getProcessId(), deployTask.getHost(), deployTask.getProcessType(), deployTask.getTaskType());
        if (isRunning) {
            return;
        }
        RResult result = agentRest.commandExec(deployConf.getHost(), agentPort(deployConf.getHost()), creq);
        taskInstanceComponent.refreshTask(deployTask, result);
    }

    @Override
    public void startService(HttpServletRequest request, HttpServletResponse response, DorisStartReq dorisStart) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkHasUnfinishProcess(userId, dorisStart.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(dorisStart.getProcessId(), ProcessTypeEnum.START_SERVICE);
        Preconditions.checkArgument(success, "The configuration was not successfully delivered and the service could not be started");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(dorisStart.getProcessId(), ProcessTypeEnum.START_SERVICE);

        String leaderFe = getLeaderFeHostPort(process.getClusterId(), request);
        List<DorisStart> dorisStarts = dorisStart.getDorisStarts();
        for (DorisStart start : dorisStarts) {
            CommandType commandType = transAgentCmd(CmdTypeEnum.START, ServiceRole.findByName(start.getRole()));
            if (commandType == null) {
                log.error("not support command {} {}", CmdTypeEnum.START, start.getRole());
                continue;
            }
            CommandRequest creq = new CommandRequest();
            TaskInstanceEntity execTask = new TaskInstanceEntity(process.getId(), start.getHost(), ProcessTypeEnum.START_SERVICE);
            switch (commandType) {
                case START_FE:
                    FeStartCommandRequestBody feBody = new FeStartCommandRequestBody();
                    if (StringUtils.isNotBlank(leaderFe)) {
                        feBody.setHelpHostPort(leaderFe);
                    }
                    creq.setBody(JSON.toJSONString(feBody));
                    execTask.setTaskType(TaskTypeEnum.START_FE);
                    break;
                case START_BE:
                    execTask.setTaskType(TaskTypeEnum.START_BE);
                    break;
                default:
                    log.error("not support command: {}", commandType.name());
                    break;
            }
            creq.setCommandType(commandType.name());
            boolean isRunning = taskInstanceComponent.checkTaskRunning(execTask.getProcessId(), execTask.getHost(), execTask.getProcessType(), execTask.getTaskType());
            if (isRunning) {
                return;
            }
            RResult result = agentRest.commandExec(start.getHost(), agentPort(start.getHost()), creq);
            taskInstanceComponent.refreshTask(execTask, result);
            log.info("agent {} starting {} ", start.getHost(), start.getRole());
        }
    }

    /**
     * get fe http port
     **/
    public Integer getFeHttpPort(String host, Integer port) {
        Properties feConf = agentRest.roleConfig(host, port, ServiceRole.FE.name());
        try {
            Integer httpPort = Integer.valueOf(feConf.getProperty(Constants.KEY_FE_HTTP_PORT));
            return httpPort;
        } catch (NumberFormatException e) {
            log.warn("get fe http port fail,return default port 8030");
            return Constants.DORIS_DEFAULT_FE_HTTP_PORT;
        }
    }

    /**
     * get alive agent
     */
    public AgentEntity getAliveAgent(int cluserId) {
        List<AgentRoleEntity> agentRoleEntities = agentRoleComponent.queryAgentByRole(ServiceRole.FE.name(), cluserId);
        AgentEntity aliveAgent = null;
        for (AgentRoleEntity agentRole : agentRoleEntities) {
            aliveAgent = agentCache.agentInfo(agentRole.getHost());
            if (AgentStatus.RUNNING.equals(aliveAgent.getStatus())) {
                break;
            }
        }
        Preconditions.checkNotNull(aliveAgent, "no agent alive");
        return aliveAgent;
    }

    /**
     * query leader fe host editLogPort
     */
    public String getLeaderFeHostPort(int clusterId, HttpServletRequest request) {
        AgentEntity aliveAgent = getAliveAgent(clusterId);
        Integer httpPort = getFeHttpPort(aliveAgent.getHost(), aliveAgent.getPort());
        Map<String, String> headers = Maps.newHashMap();
        RestService.setAuthHeader(request, headers);
        List<Map<String, String>> frontends = restService.frontends(aliveAgent.getHost(), httpPort, headers);
        String leaderFe = null;
        for (Map<String, String> frontend : frontends) {
            if (Boolean.valueOf(frontend.get("IsMaster"))) {
                String ip = frontend.get("IP");
                String editLogPort = frontend.get("EditLogPort");
                leaderFe = ip + ":" + editLogPort;
                break;
            }
        }
        if (StringUtils.isBlank(leaderFe)) {
            log.error("can not get leader fe");
        }
        return leaderFe;
    }

    /**
     * trans server command to agent command
     */
    public CommandType transAgentCmd(CmdTypeEnum cmdType, ServiceRole role) {
        Preconditions.checkNotNull(cmdType, "unrecognized cmd type " + cmdType);
        Preconditions.checkNotNull(role, "unrecognized role " + role);
        String cmd = cmdType.name() + "_" + role.name();
        return CommandType.findByName(cmd);
    }

    private int agentPort(String host) {
        AgentEntity agent = agentCache.agentInfo(host);
        if (agent == null) {
            throw new ServerException("query agent port fail");
        }
        return agent.getPort();
    }

    @Override
    public void joinBe(HttpServletRequest request, HttpServletResponse response, BeJoinReq beJoinReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkHasUnfinishProcess(userId, beJoinReq.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(beJoinReq.getProcessId(), ProcessTypeEnum.BUILD_CLUSTER);
        Preconditions.checkArgument(success, "The service has not been started and completed, and the component cannot be clustered");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(beJoinReq.getProcessId(), ProcessTypeEnum.BUILD_CLUSTER);
        Map<String, String> headers = Maps.newHashMap();
        RestService.setAuthHeader(request, headers);
        RestService.setPostHeader(headers);

        for (String be : beJoinReq.getHosts()) {
            addBeToCluster(process.getId(), be, process.getClusterId(), headers);
        }
    }

    private void addBeToCluster(int processId, String be, int clusterId, Map<String, String> headers) {
        int agentPort = agentPort(be);
        AgentEntity aliveAgent = getAliveAgent(clusterId);
        Integer httpPort = getFeHttpPort(aliveAgent.getHost(), aliveAgent.getPort());
        TaskInstanceEntity joinBeTask = taskInstanceComponent.saveTask(processId, be, ProcessTypeEnum.BUILD_CLUSTER, TaskTypeEnum.JOIN_BE, ExecutionStatus.SUBMITTED);
        if (joinBeTask == null) {
            return;
        }
        TaskContext taskContext = new TaskContext(TaskTypeEnum.JOIN_BE, joinBeTask, new BeJoin(aliveAgent.getHost(), httpPort, be, agentPort, headers));
        ListenableFuture<Object> submit = taskExecService.submit(new TaskExecuteThread(taskContext));
        Futures.addCallback(submit, new TaskExecCallback(taskContext));
    }

    @Override
    public boolean register(AgentRoleRegister agentReg) {
        AgentRoleEntity agent = agentRoleComponent.queryByHostRole(agentReg.getHost(), agentReg.getRole());
        if (agent == null) {
            log.error("can not find agent {} role {}", agentReg.getHost(), agentReg.getRole());
            throw new ServerException("can not register " + agentReg.getHost() + " role " + agentReg.getRole());
        } else if (Flag.NO.equals(agent.getRegister())) {
            agent.setRegister(Flag.YES);
        } else {
            log.info("agent {} role {} already register.", agentReg.getHost(), agentReg.getRole());
        }

        AgentRoleEntity agentRoleEntity = agentRoleComponent.saveAgentRole(agent);
        return agentRoleEntity != null;
    }

    @Override
    public List<Integer> execute(DorisExecReq dorisExec) {
        List<Integer> taskIds = Lists.newArrayList();
        List<Integer> roles = dorisExec.getRoles();
        Preconditions.checkArgument(ObjectUtils.isNotEmpty(roles), "roles can not empty");
        List<AgentRoleEntity> agentRoleEntities = agentRoleComponent.queryAllAgentRoles();
        for (AgentRoleEntity agentRole : agentRoleEntities) {
            if (roles.contains(agentRole.getId())) {
                int taskId = execute(agentRole.getHost(), agentRole.getRole(), dorisExec.getCommand());
                taskIds.add(taskId);
            }
        }
        return taskIds;
    }

    /**
     * execute a command return taskId
     */
    private int execute(String host, String roleName, String commandName) {
        CommandRequest creq = new CommandRequest();
        TaskInstanceEntity execTask = new TaskInstanceEntity(host);
        CommandType command = CommandType.findByName(commandName + "_" + roleName);
        switch (command) {
            case START_FE:
                execTask.setTaskType(TaskTypeEnum.START_FE);
                break;
            case START_BE:
                execTask.setTaskType(TaskTypeEnum.START_BE);
                break;
            case STOP_FE:
                execTask.setTaskType(TaskTypeEnum.STOP_FE);
                break;
            case STOP_BE:
                execTask.setTaskType(TaskTypeEnum.STOP_BE);
                break;
            default:
                log.error("not support command: {}", command.name());
                break;
        }

        creq.setCommandType(command.name());
        RResult result = agentRest.commandExec(host, agentPort(host), creq);
        TaskInstanceEntity taskInstanceEntity = taskInstanceComponent.refreshTask(execTask, result);
        log.info("agent {} {} {} ", host, execTask.getTaskType().name(), roleName);
        return taskInstanceEntity.getId();
    }
}
