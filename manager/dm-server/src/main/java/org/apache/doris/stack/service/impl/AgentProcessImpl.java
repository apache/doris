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
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.BeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.BrokerInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.FeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.FeStartCommandRequestBody;
import org.apache.doris.manager.common.domain.HardwareInfo;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.manager.common.domain.WriteBeConfCommandRequestBody;
import org.apache.doris.manager.common.domain.WriteBrokerConfCommandRequestBody;
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
import org.apache.doris.stack.dao.TaskInstanceRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.entity.TaskInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.request.BeJoinReq;
import org.apache.doris.stack.model.request.DeployConfigReq;
import org.apache.doris.stack.model.request.DorisExecReq;
import org.apache.doris.stack.model.request.DorisInstallReq;
import org.apache.doris.stack.model.request.DorisStartReq;
import org.apache.doris.stack.model.task.BeJoin;
import org.apache.doris.stack.model.task.DeployConfig;
import org.apache.doris.stack.model.task.DorisInstall;
import org.apache.doris.stack.model.task.DorisStart;
import org.apache.doris.stack.runner.TaskExecuteRunner;
import org.apache.doris.stack.service.AgentProcess;
import org.apache.doris.stack.service.user.AuthenticationService;
import org.apache.doris.stack.util.JdbcUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * server agent
 **/
@Service
@Slf4j
public class AgentProcessImpl implements AgentProcess {

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
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private TaskExecuteRunner taskExecuteRunner;

    @Override
    @Transactional
    public void installService(HttpServletRequest request, HttpServletResponse response,
                               DorisInstallReq installReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkProcessFinish(installReq.getProcessId());
        processInstanceComponent.checkHasUnfinishProcess(userId, installReq.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(installReq.getProcessId(), ProcessTypeEnum.INSTALL_SERVICE);
        Preconditions.checkArgument(success, "The agent is not installed successfully and the service cannot be installed");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(installReq.getProcessId(), ProcessTypeEnum.INSTALL_SERVICE);
        //Installed host and service
        List<String> agentRoleList = agentRoleComponent.queryAgentRoles(process.getClusterId()).stream()
                .map(m -> (m.getHost() + "-" + m.getRole()))
                .collect(Collectors.toList());
        List<DorisInstall> installInfos = installReq.getInstallInfos();
        if (installInfos == null) {
            throw new ServerException("Please specify the host configuration to be installed");
        }
        for (DorisInstall install : installInfos) {
            String key = install.getHost() + "-" + install.getRole();
            if (agentRoleList.contains(key)) {
                log.warn("agent {} already install doris {}", install.getHost(), install.getRole());
                continue;
            }

            //check parent task has skipped
            boolean parentTaskSkip = checkParentTaskSkip(installReq.getProcessId(), install.getHost(), TaskTypeEnum.INSTALL_AGENT);
            if (parentTaskSkip) {
                log.warn("host {} parent install agent task has skipped,so the current install {} task is also skipped", install.getHost(), install.getRole());
                continue;
            }
            installDoris(process.getId(), install, process.getPackageUrl(), process.getInstallDir());
        }
    }

    private void installDoris(int processId, DorisInstall install, String packageUrl, String installDir) {
        if (!installDir.endsWith(File.separator)) {
            installDir = installDir + File.separator;
        }
        CommandRequest creq = new CommandRequest();
        TaskInstanceEntity installServiceTmp = new TaskInstanceEntity(processId, install.getHost(), ProcessTypeEnum.INSTALL_SERVICE, ExecutionStatus.SUBMITTED);
        if (ServiceRole.FE.name().equals(install.getRole())) {
            FeInstallCommandRequestBody feBody = new FeInstallCommandRequestBody();
            feBody.setMkFeMetadir(true);
            feBody.setPackageUrl(packageUrl);
            feBody.setInstallDir(installDir + ServiceRole.FE.getInstallName());
            creq.setCommandType(CommandType.INSTALL_FE.name());
            creq.setBody(JSON.toJSONString(feBody));
            installServiceTmp.setTaskType(TaskTypeEnum.INSTALL_FE);
        } else if (ServiceRole.BE.name().equals(install.getRole())) {
            BeInstallCommandRequestBody beBody = new BeInstallCommandRequestBody();
            beBody.setMkBeStorageDir(true);
            beBody.setPackageUrl(packageUrl);
            beBody.setInstallDir(installDir + ServiceRole.BE.getInstallName());
            creq.setCommandType(CommandType.INSTALL_BE.name());
            creq.setBody(JSON.toJSONString(beBody));
            installServiceTmp.setTaskType(TaskTypeEnum.INSTALL_BE);
        } else if (ServiceRole.BROKER.name().equals(install.getRole())) {
            BrokerInstallCommandRequestBody broker = new BrokerInstallCommandRequestBody();
            broker.setInstallDir(installDir + ServiceRole.BROKER.getInstallName());
            broker.setPackageUrl(packageUrl);
            creq.setCommandType(CommandType.INSTALL_BROKER.name());
            creq.setBody(JSON.toJSONString(broker));
            installServiceTmp.setTaskType(TaskTypeEnum.INSTALL_BROKER);
        } else {
            throw new ServerException("The service installation is not currently supported");
        }
        TaskInstanceEntity installService = taskInstanceComponent.saveTask(installServiceTmp);
        if (installService == null) {
            return;
        }
        handleAgentTask(installService, creq);
        agentRoleComponent.saveAgentRole(new AgentRoleEntity(install.getHost(), install.getRole(), install.getFeNodeType(), installDir, Flag.NO));
        log.info("agent {} installing doris {}", install.getHost(), install.getRole());
    }

    /**
     * In the current installation progress,
     * check whether there are skipped tasks in the predecessor tasks on the same host
     */
    private boolean checkParentTaskSkip(int processId, String host, TaskTypeEnum parentTask) {
        List<TaskInstanceEntity> taskEntities = taskInstanceRepository.queryTasksByProcessId(processId);
        for (TaskInstanceEntity task : taskEntities) {
            if (!task.getSkip().typeIsYes() && host.equals(task.getHost()) && task.getTaskType().equals(parentTask)) {
                return false;
            }
        }
        return true;
    }

    private void handleAgentTask(TaskInstanceEntity taskInstance, CommandRequest creq) {
        taskInstance.setTaskJson(JSON.toJSONString(creq));
        RResult result = taskExecuteRunner.execAgentTask(taskInstance, creq);
        taskInstanceComponent.refreshTask(taskInstance, result);
    }

    @Override
    public void deployConfig(HttpServletRequest request, HttpServletResponse response, DeployConfigReq deployConfigReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkProcessFinish(deployConfigReq.getProcessId());
        processInstanceComponent.checkHasUnfinishProcess(userId, deployConfigReq.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(deployConfigReq.getProcessId(), ProcessTypeEnum.DEPLOY_CONFIG);
        Preconditions.checkArgument(success, "doris is not installed successfully and the configuration cannot be delivered");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(deployConfigReq.getProcessId(), ProcessTypeEnum.DEPLOY_CONFIG);
        List<DeployConfig> deployConfigs = deployConfigReq.getDeployConfigs();
        for (DeployConfig deployConf : deployConfigs) {
            if (StringUtils.isBlank(deployConf.getConf())) {
                continue;
            }
            String encodeConf = encodeConfig(deployConf.getConf());
            for (String host : deployConf.getHosts()) {
                CommandRequest creq = new CommandRequest();
                TaskTypeEnum parentTask;
                TaskInstanceEntity deployTaskTmp = new TaskInstanceEntity(process.getId(), host, ProcessTypeEnum.DEPLOY_CONFIG, ExecutionStatus.SUBMITTED);
                if (ServiceRole.FE.name().equals(deployConf.getRole())) {
                    WriteFeConfCommandRequestBody feConf = new WriteFeConfCommandRequestBody();
                    feConf.setContent(encodeConf);
                    feConf.setCreateMetaDir(true);
                    creq.setBody(JSON.toJSONString(feConf));
                    creq.setCommandType(CommandType.WRITE_FE_CONF.name());
                    deployTaskTmp.setTaskType(TaskTypeEnum.DEPLOY_FE_CONFIG);
                    parentTask = TaskTypeEnum.INSTALL_FE;
                } else if (ServiceRole.BE.name().equals(deployConf.getRole())) {
                    WriteBeConfCommandRequestBody beConf = new WriteBeConfCommandRequestBody();
                    beConf.setContent(encodeConf);
                    beConf.setCreateStorageDir(true);
                    creq.setBody(JSON.toJSONString(beConf));
                    creq.setCommandType(CommandType.WRITE_BE_CONF.name());
                    deployTaskTmp.setTaskType(TaskTypeEnum.DEPLOY_BE_CONFIG);
                    parentTask = TaskTypeEnum.INSTALL_BE;
                } else if (ServiceRole.BROKER.name().equals(deployConf.getRole())) {
                    WriteBrokerConfCommandRequestBody brokerConf = new WriteBrokerConfCommandRequestBody();
                    brokerConf.setContent(encodeConf);
                    creq.setBody(JSON.toJSONString(brokerConf));
                    creq.setCommandType(CommandType.WRITE_BROKER_CONF.name());
                    deployTaskTmp.setTaskType(TaskTypeEnum.DEPLOY_BROKER_CONFIG);
                    parentTask = TaskTypeEnum.INSTALL_BROKER;
                } else {
                    throw new ServerException("The service deploy config is not currently supported");
                }

                TaskInstanceEntity deployTask = taskInstanceComponent.saveTask(deployTaskTmp);
                if (deployTask == null) {
                    continue;
                }
                //check parent task skipped
                boolean parentTaskSkip = checkParentTaskSkip(deployConfigReq.getProcessId(), host, parentTask);
                if (parentTaskSkip) {
                    log.warn("host {} parent install {} task has skipped,so the current deploy {} config task is also skipped", host, deployConf.getRole(), deployConf.getRole());
                    continue;
                }
                handleAgentTask(deployTask, creq);
                log.info("agent {} deploy {} conf", host, deployConf.getRole());
            }
        }
    }

    private String encodeConfig(String conf) {
        if (StringUtils.isBlank(conf)) {
            return conf;
        }
        Base64.Encoder encoder = Base64.getEncoder();
        try {
            return new String(encoder.encode(conf.getBytes()), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("conf {} can not encoding:", conf, e);
            return conf;
        }
    }

    @Override
    public void startService(HttpServletRequest request, HttpServletResponse response, DorisStartReq dorisStart) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkProcessFinish(dorisStart.getProcessId());
        processInstanceComponent.checkHasUnfinishProcess(userId, dorisStart.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(dorisStart.getProcessId(), ProcessTypeEnum.START_SERVICE);
        Preconditions.checkArgument(success, "The configuration was not successfully delivered and the service could not be started");

        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(dorisStart.getProcessId(), ProcessTypeEnum.START_SERVICE);
        List<DorisStart> dorisStarts = dorisStart.getDorisStarts();
        for (DorisStart start : dorisStarts) {
            TaskInstanceEntity execTaskTmp = new TaskInstanceEntity(process.getId(), start.getHost(), ProcessTypeEnum.START_SERVICE, ExecutionStatus.SUBMITTED);
            TaskTypeEnum parentTask;
            ServiceRole serviceRole = ServiceRole.findByName(start.getRole());
            if (ServiceRole.FE.equals(serviceRole)) {
                execTaskTmp.setTaskType(TaskTypeEnum.START_FE);
                parentTask = TaskTypeEnum.DEPLOY_FE_CONFIG;
            } else if (ServiceRole.BE.equals(serviceRole)) {
                execTaskTmp.setTaskType(TaskTypeEnum.START_BE);
                parentTask = TaskTypeEnum.DEPLOY_BE_CONFIG;
            } else if (ServiceRole.BROKER.equals(serviceRole)) {
                execTaskTmp.setTaskType(TaskTypeEnum.START_BROKER);
                parentTask = TaskTypeEnum.DEPLOY_BROKER_CONFIG;
            } else {
                log.error("not support role {}", start.getRole());
                continue;
            }
            TaskInstanceEntity execTask = taskInstanceComponent.saveTask(execTaskTmp);
            if (execTask == null) {
                continue;
            }

            //check parent task has skipped
            boolean parentTaskSkip = checkParentTaskSkip(process.getId(), start.getHost(), parentTask);
            if (parentTaskSkip) {
                log.warn("host {} parent deploy config {} task has skipped,so the current start {} task is also skipped", start.getHost(), start.getRole(), start.getRole());
                continue;
            }
            //start service
            if (TaskTypeEnum.START_FE.equals(execTask.getTaskType())) {
                List<TaskInstanceEntity> startFeTasks = taskInstanceComponent.queryRunningTasks(process.getId(), TaskTypeEnum.START_FE);
                if (ObjectUtils.isEmpty(startFeTasks)) {
                    continue;
                }
                if (startFeTasks.get(0).getId() == execTask.getId()) {
                    //start master fe
                    CommandRequest creq = buildStartCmd(ServiceRole.FE, null);
                    handleAgentTask(execTask, creq);
                } else {
                    //wait master fe start success,then start no-master fe
                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                String leaderFeHost;
                                while (true) {
                                    //Polling until there has a successful fe
                                    List<TaskInstanceEntity> taskInstanceEntities = taskInstanceComponent.querySuccessTasks(process.getId(), TaskTypeEnum.START_FE);
                                    if (!taskInstanceEntities.isEmpty()) {
                                        leaderFeHost = taskInstanceEntities.get(0).getHost();
                                        log.info("master fe {} start success", leaderFeHost);
                                        break;
                                    }
                                    Thread.sleep(1000);
                                    log.warn("waiting master fe start success");
                                }

                                Integer leaderFeQueryPort = getFeQueryPort(leaderFeHost, agentCache.agentPort(leaderFeHost));
                                joinFe(leaderFeHost, leaderFeQueryPort, start.getHost());
                                CommandRequest creq = buildStartCmd(ServiceRole.FE, leaderFeHost);
                                handleAgentTask(execTask, creq);
                            } catch (Exception e) {
                                log.error("task execute error,", e);
                                execTask.setStatus(ExecutionStatus.FAILURE);
                                taskInstanceRepository.save(execTask);
                            }
                        }
                    });
                    executor.shutdown();
                }
            } else {
                CommandRequest creq = buildStartCmd(ServiceRole.BE, null);
                handleAgentTask(execTask, creq);
            }
            log.info("agent {} starting {} ", start.getHost(), start.getRole());
        }
    }

    private CommandRequest buildStartCmd(ServiceRole serviceRole, String leaderFeHost) {
        CommandRequest creq = new CommandRequest();
        CommandType commandType = transAgentCmd(CmdTypeEnum.START, serviceRole);
        if (commandType == null) {
            log.error("not support command {} {}", CmdTypeEnum.START, serviceRole.name());
            return creq;
        }
        switch (commandType) {
            case START_FE:
                FeStartCommandRequestBody feBody = new FeStartCommandRequestBody();
                if (StringUtils.isNotBlank(leaderFeHost)) {
                    Integer leaderFeEditLogPort = getFeEditLogPort(leaderFeHost, agentCache.agentPort(leaderFeHost));
                    feBody.setHelpHostPort(leaderFeHost + ":" + leaderFeEditLogPort);
                }
                creq.setBody(JSON.toJSONString(feBody));
                break;
            case START_BE:
                break;
            default:
                log.error("not support command: {}", commandType.name());
                break;
        }
        creq.setCommandType(commandType.name());
        return creq;
    }

    /**
     * add fe to cluster
     */
    private void joinFe(String leaderHost, int leaderQueryPort, String addFeHost) {
        AgentRoleEntity agentRole = agentRoleComponent.queryByHostRole(addFeHost, ServiceRole.FE.name());
        Integer addFePort = getFeEditLogPort(addFeHost, agentCache.agentPort(addFeHost));
        Connection connection = null;
        try {
            connection = JdbcUtil.getConnection(leaderHost, leaderQueryPort);
            String sql = "ALTER SYSTEM ADD %s \"%s:%s\"";
            String joinFeSql = String.format(sql, agentRole.getFeNodeType().toUpperCase(), addFeHost, addFePort);
            JdbcUtil.execute(connection, joinFeSql);
            log.info("execute {}", joinFeSql);
        } catch (SQLException ex) {
            log.error("add fe to cluster failed:{}", addFeHost, ex);
        } finally {
            JdbcUtil.closeConn(connection);
        }
    }

    /**
     * get fe http port
     **/
    public Integer getFeQueryPort(String host, Integer port) {
        Properties feConf = agentRest.roleConfig(host, port, ServiceRole.FE.name());
        try {
            Integer httpPort = Integer.valueOf(feConf.getProperty(Constants.KEY_FE_QUERY_PORT));
            return httpPort;
        } catch (NumberFormatException e) {
            log.warn("get fe query port fail,return default port 9030");
            return Constants.DORIS_DEFAULT_FE_QUERY_PORT;
        }
    }

    /**
     * get fe edit log port
     **/
    public Integer getFeEditLogPort(String host, Integer port) {
        Properties feConf = agentRest.roleConfig(host, port, ServiceRole.FE.name());
        try {
            Integer httpPort = Integer.valueOf(feConf.getProperty(Constants.KEY_FE_EDIT_LOG_PORT));
            return httpPort;
        } catch (NumberFormatException e) {
            log.warn("get fe http port fail,return default port 9010");
            return Constants.DORIS_DEFAULT_FE_EDIT_LOG_PORT;
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
     * trans server command to agent command
     */
    public CommandType transAgentCmd(CmdTypeEnum cmdType, ServiceRole role) {
        Preconditions.checkNotNull(cmdType, "unrecognized cmd type " + cmdType);
        Preconditions.checkNotNull(role, "unrecognized role " + role);
        String cmd = cmdType.name() + "_" + role.name();
        return CommandType.findByName(cmd);
    }

    @Override
    public void joinBe(HttpServletRequest request, HttpServletResponse response, BeJoinReq beJoinReq) throws Exception {
        int userId = authenticationService.checkAllUserAuthWithCookie(request, response);
        processInstanceComponent.checkProcessFinish(beJoinReq.getProcessId());
        processInstanceComponent.checkHasUnfinishProcess(userId, beJoinReq.getProcessId());
        boolean success = taskInstanceComponent.checkParentTaskSuccess(beJoinReq.getProcessId(), ProcessTypeEnum.BUILD_CLUSTER);
        Preconditions.checkArgument(success, "The service has not been started and completed, and the component cannot be clustered");
        ProcessInstanceEntity process = processInstanceComponent.refreshProcess(beJoinReq.getProcessId(), ProcessTypeEnum.BUILD_CLUSTER);

        //Query the alive agent that installed fe and get fe's query port
        AgentEntity aliveAgent = getAliveAgent(process.getClusterId());
        Integer queryPort = getFeQueryPort(aliveAgent.getHost(), aliveAgent.getPort());
        for (String be : beJoinReq.getHosts()) {
            int agentPort = agentCache.agentPort(be);
            TaskInstanceEntity joinBeTask = taskInstanceComponent.saveTask(process.getId(), be, ProcessTypeEnum.BUILD_CLUSTER, TaskTypeEnum.JOIN_BE, ExecutionStatus.SUBMITTED);
            if (joinBeTask == null) {
                return;
            }
            boolean parentTaskSkip = checkParentTaskSkip(beJoinReq.getProcessId(), be, TaskTypeEnum.START_BE);
            if (parentTaskSkip) {
                log.warn("host {} parent start be task has skipped,so the current add be to cluster task is also skipped", be);
                continue;
            }
            BeJoin beJoin = new BeJoin(aliveAgent.getHost(), queryPort, be, agentPort);
            joinBeTask.setTaskJson(JSON.toJSONString(beJoin));
            taskExecuteRunner.execTask(joinBeTask, beJoin);
        }
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
        RResult result = taskExecuteRunner.execAgentTask(execTask, creq);
        TaskInstanceEntity taskInstanceEntity = taskInstanceComponent.refreshTask(execTask, result);
        log.info("agent {} {} {} ", host, execTask.getTaskType().name(), roleName);
        return taskInstanceEntity.getId();
    }

    @Override
    public Object log(String host, String type) {
        RResult rResult = agentRest.serverLog(host, agentCache.agentPort(host), type);
        if (rResult != null && rResult.isSuccess()) {
            return rResult.getData();
        } else {
            return "fetch log fail";
        }
    }

    @Override
    public HardwareInfo hardwareInfo(String host) {
        int port = agentCache.agentPort(host);
        HardwareInfo hardwareInfo = agentRest.hardwareInfo(host, port);
        return hardwareInfo;
    }
}
