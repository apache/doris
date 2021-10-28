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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.doris.stack.component.AgentComponent;
import org.apache.doris.stack.component.AgentRoleComponent;
import org.apache.doris.stack.component.ProcessInstanceComponent;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.constants.CmdTypeEnum;
import org.apache.doris.stack.constants.Constants;
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
import org.apache.doris.stack.model.BeJoin;
import org.apache.doris.stack.model.DeployConfig;
import org.apache.doris.stack.model.DorisExec;
import org.apache.doris.stack.model.InstallInfo;
import org.apache.doris.stack.model.request.BeJoinReq;
import org.apache.doris.stack.model.request.DeployConfigReq;
import org.apache.doris.stack.model.request.DorisExecReq;
import org.apache.doris.stack.model.request.DorisInstallReq;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.runner.TaskExecCallback;
import org.apache.doris.stack.runner.TaskExecuteThread;
import org.apache.doris.stack.service.ServerAgent;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.util.JdbcUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * server agent
 **/
@Service
@Slf4j
public class ServerAgentImpl implements ServerAgent {

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
    private AgentComponent agentComponent;

    @Autowired
    private ProcessInstanceComponent processInstanceComponent;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private AuthenticationService authenticationService;

    @Override
    @Transactional
    public void installService(HttpServletRequest request, HttpServletResponse response,
                               DorisInstallReq installReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        int processId = processInstanceComponent.saveProcessInstance(new ProcessInstanceEntity(installReq.getClusterId(), userId, ProcessTypeEnum.INSTALL_SERVICE));
        //Installed host and service
        List<String> agentRoleList = agentRoleComponent.queryAgentRoles().stream()
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
            installDoris(processId, install);
            agentRoleComponent.saveAgentRole(new AgentRoleEntity(install.getHost(), install.getRole(), install.getInstallDir(), Flag.NO));
        }
    }

    private RResult installDoris(int processId, InstallInfo install) {
        CommandRequest creq = new CommandRequest();
        TaskInstanceEntity installService = new TaskInstanceEntity(processId, install.getHost());
        if (ServiceRole.FE.name().equals(install.getRole())) {
            FeInstallCommandRequestBody feBody = new FeInstallCommandRequestBody();
            feBody.setMkFeMetadir(install.isMkFeMetadir());
            feBody.setPackageUrl(install.getPackageUrl());
            feBody.setInstallDir(install.getInstallDir());
            creq.setCommandType(CommandType.INSTALL_FE.name());
            creq.setBody(JSON.toJSONString(feBody));
            installService.setTaskType(TaskTypeEnum.INSTALL_FE);
        } else if (ServiceRole.BE.name().equals(install.getRole())) {
            BeInstallCommandRequestBody beBody = new BeInstallCommandRequestBody();
            beBody.setMkBeStorageDir(install.isMkBeStorageDir());
            beBody.setInstallDir(install.getInstallDir());
            beBody.setPackageUrl(install.getPackageUrl());
            creq.setCommandType(CommandType.INSTALL_BE.name());
            creq.setBody(JSON.toJSONString(beBody));
            installService.setTaskType(TaskTypeEnum.INSTALL_BE);
        } else {
            throw new ServerException("The service installation is not currently supported");
        }
        RResult result = agentRest.commandExec(install.getHost(), agentPort(install.getHost()), creq);
        if (result != null && result.isSuccess()) {
            installService.setStatus(ExecutionStatus.RUNNING);
        } else {
            installService.setStatus(ExecutionStatus.FAILURE);
        }
        taskInstanceRepository.save(installService);
        return result;
    }

    @Override
    public void deployConfig(HttpServletRequest request, HttpServletResponse response, DeployConfigReq deployConfigReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        int processId = processInstanceComponent.saveProcessInstance(new ProcessInstanceEntity(deployConfigReq.getClusterId(), userId, ProcessTypeEnum.DEPLOY_CONFIG));
        List<DeployConfig> deployConfigs = deployConfigReq.getDeployConfigs();
        for (DeployConfig config : deployConfigs) {
            deployConf(processId, config);
        }
    }

    private RResult deployConf(int processId, DeployConfig deployConf) {
        CommandRequest creq = new CommandRequest();
        TaskInstanceEntity deployTask = new TaskInstanceEntity(processId, deployConf.getHost());
        if (ServiceRole.FE.name().equals(deployConf.getRole())) {
            WriteFeConfCommandRequestBody feConf = new WriteFeConfCommandRequestBody();
            feConf.setContent(deployConf.getConf());
            creq.setBody(JSON.toJSONString(feConf));
            creq.setCommandType(CommandType.WRITE_FE_CONF.name());
            deployTask.setTaskType(TaskTypeEnum.DEPLOY_FE_CONFIG);
        } else if (ServiceRole.BE.name().equals(deployConf.getRole())) {
            WriteBeConfCommandRequestBody beConf = new WriteBeConfCommandRequestBody();
            beConf.setContent(deployConf.getConf());
            creq.setBody(JSON.toJSONString(beConf));
            creq.setCommandType(CommandType.WRITE_BE_CONF.name());
            deployTask.setTaskType(TaskTypeEnum.DEPLOY_BE_CONFIG);
        }
        RResult result = agentRest.commandExec(deployConf.getHost(), agentPort(deployConf.getHost()), creq);
        if (result != null && result.isSuccess()) {
            deployTask.setStatus(ExecutionStatus.RUNNING);
        } else {
            deployTask.setStatus(ExecutionStatus.FAILURE);
        }
        taskInstanceRepository.save(deployTask);
        return result;
    }

    @Override
    public void execute(HttpServletRequest request, HttpServletResponse response, DorisExecReq dorisExec) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        int processId = processInstanceComponent.saveProcessInstance(new ProcessInstanceEntity(dorisExec.getClusterId(), userId, ProcessTypeEnum.START_SERVICE));
        CmdTypeEnum cmdType = CmdTypeEnum.findByName(dorisExec.getCommand());

        String leaderFe = getLeaderFeHostPort();
        List<DorisExec> dorisExecs = dorisExec.getDorisExecs();
        for (DorisExec exec : dorisExecs) {
            CommandType commandType = transAgentCmd(cmdType, ServiceRole.findByName(exec.getRole()));
            CommandRequest creq = new CommandRequest();
            TaskInstanceEntity execTask = new TaskInstanceEntity(processId, exec.getHost());
            if (CommandType.START_FE.equals(commandType)) {
                FeStartCommandRequestBody feBody = new FeStartCommandRequestBody();
                if (StringUtils.isNotBlank(leaderFe)) {
                    feBody.setHelpHostPort(leaderFe);
                }
                creq.setBody(JSON.toJSONString(feBody));
                execTask.setTaskType(TaskTypeEnum.START_FE);
            } else if (CommandType.START_BE.equals(commandType)) {
                execTask.setTaskType(TaskTypeEnum.START_BE);
            }
            creq.setCommandType(commandType.name());
            RResult result = agentRest.commandExec(exec.getHost(), agentPort(exec.getHost()), creq);

            if (result != null && result.isSuccess()) {
                execTask.setStatus(ExecutionStatus.RUNNING);
            } else {
                execTask.setStatus(ExecutionStatus.FAILURE);
            }
            taskInstanceRepository.save(execTask);
        }
    }

    /**
     * get fe jdbc port
     **/
    public Integer getFeQueryPort(String host, Integer port) {
        Properties feConf = agentRest.roleConfig(host, port, ServiceRole.FE.name());
        try {
            Integer jdbcPort = Integer.valueOf(feConf.getProperty(Constants.KEY_FE_QUERY_PORT));
            return jdbcPort;
        } catch (NumberFormatException e) {
            log.warn("get fe query port fail,return default port 9030");
            return Constants.DORIS_DEFAULT_FE_QUERY_PORT;
        }
    }

    /**
     * get alive agent
     */
    public AgentEntity getAliveAgent() {
        List<AgentRoleEntity> agentRoleEntities = agentRoleComponent.queryAgentByRole(ServiceRole.FE.name());
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
    public String getLeaderFeHostPort() {
        AgentEntity aliveAgent = getAliveAgent();
        Integer jdbcPort = getFeQueryPort(aliveAgent.getHost(), aliveAgent.getPort());
        //query leader fe
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String leaderFe = null;
        try {
            conn = JdbcUtil.getConnection(aliveAgent.getHost(), jdbcPort);
            stmt = conn.prepareStatement("SHOW PROC '/frontends'");
            rs = stmt.executeQuery();
            while (rs.next()) {
                boolean isMaster = rs.getBoolean("IsMaster");
                if (isMaster) {
                    String ip = rs.getString("IP");
                    String editLogPort = rs.getString("EditLogPort");
                    leaderFe = ip + ":" + editLogPort;
                    break;
                }
            }
        } catch (SQLException e) {
            log.error("query show frontends fail", e);
        } finally {
            JdbcUtil.closeConn(conn);
            JdbcUtil.closeStmt(stmt);
            JdbcUtil.closeRs(rs);
        }
        if (StringUtils.isBlank(leaderFe)) {
            log.error("can not get leader fe info");
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
        int processId = processInstanceComponent.saveProcessInstance(new ProcessInstanceEntity(beJoinReq.getClusterId(), userId, ProcessTypeEnum.BUILD_CLUSTER));
        for (String be : beJoinReq.getHosts()) {
            addBeToCluster(processId, be);
        }
    }

    private void addBeToCluster(int processId, String be) {
        int agentPort = agentPort(be);
        AgentEntity aliveAgent = getAliveAgent();
        Integer jdbcPort = getFeQueryPort(aliveAgent.getHost(), aliveAgent.getPort());

        TaskInstanceEntity installAgent = new TaskInstanceEntity(processId, be, TaskTypeEnum.JOIN_BE, ExecutionStatus.RUNNING);
        taskInstanceRepository.save(installAgent);
        TaskContext taskContext = new TaskContext(TaskTypeEnum.JOIN_BE, installAgent, new BeJoin(aliveAgent.getHost(), jdbcPort, be, agentPort));
        ListenableFuture<Object> submit = taskExecService.submit(new TaskExecuteThread(taskContext));
        Futures.addCallback(submit, new TaskExecCallback(taskContext));
    }

    @Override
    public boolean register(AgentRoleRegister agentReg) {
        AgentEntity agent = agentComponent.agentInfo(agentReg.getHost());
        if (agent == null) {
            throw new ServerException("can not find " + agentReg.getHost() + " agent");
        }
        AgentRoleEntity agentRoleEntity = agentRoleComponent.registerAgentRole(agentReg);
        return agentRoleEntity != null;
    }
}
