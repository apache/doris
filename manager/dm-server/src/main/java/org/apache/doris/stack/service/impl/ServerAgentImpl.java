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
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.constants.CmdTypeEnum;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.exceptions.JdbcException;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.req.DeployConfig;
import org.apache.doris.stack.req.DorisExec;
import org.apache.doris.stack.req.DorisExecReq;
import org.apache.doris.stack.req.DorisInstallReq;
import org.apache.doris.stack.req.InstallInfo;
import org.apache.doris.stack.req.TaskInfoReq;
import org.apache.doris.stack.req.TaskLogReq;
import org.apache.doris.stack.service.ServerAgent;
import org.apache.doris.stack.util.JdbcUtil;
import org.apache.doris.stack.util.Preconditions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * server agent
 **/
@Service
@Slf4j
public class ServerAgentImpl implements ServerAgent {

    @Autowired
    private AgentRest agentRest;

    @Autowired
    private AgentCache agentCache;

    @Autowired
    private AgentRoleComponent agentRoleComponent;

    @Autowired
    private AgentComponent agentComponent;

    @Override
    @Transactional
    public List<Object> install(DorisInstallReq installReq) {
        List<String> agentRoleList = agentRoleComponent.queryAgentRoles().stream()
                .map(m -> (m.getHost() + "-" + m.getRole()))
                .collect(Collectors.toList());
        List<Object> results = new ArrayList<>();
        List<InstallInfo> installInfos = installReq.getInstallInfos();
        for (InstallInfo install : installInfos) {
            String key = install.getHost() + "-" + install.getRole();
            if (agentRoleList.contains(key)) {
                log.warn("agent {} already install doris {}", install.getHost(), install.getRole());
                continue;
            }
            RResult result = installDoris(install);
            agentRoleComponent.registerAgentRole(new AgentRoleRegister(install.getHost(), install.getRole(), install.getInstallDir()));
            results.add(result.getData());
        }
        return results;
    }

    private RResult deployConf(DeployConfig deployConf) {
        CommandRequest creq = new CommandRequest();
        if (ServiceRole.FE.name().equals(deployConf.getRole())) {
            WriteFeConfCommandRequestBody feConf = new WriteFeConfCommandRequestBody();
            feConf.setContent(deployConf.getConf());
            creq.setBody(JSON.toJSONString(feConf));
            creq.setCommandType(CommandType.WRITE_FE_CONF.name());
        } else if (ServiceRole.BE.name().equals(deployConf.getRole())) {
            WriteBeConfCommandRequestBody beConf = new WriteBeConfCommandRequestBody();
            beConf.setContent(deployConf.getConf());
            creq.setBody(JSON.toJSONString(beConf));
            creq.setCommandType(CommandType.WRITE_BE_CONF.name());
        }
        RResult result = agentRest.commandExec(deployConf.getHost(), agentPort(deployConf.getHost()), creq);
        return result;
    }

    private RResult installDoris(InstallInfo install) {
        CommandRequest creq = new CommandRequest();
        if (ServiceRole.FE.name().equals(install.getRole())) {
            FeInstallCommandRequestBody feBody = new FeInstallCommandRequestBody();
            feBody.setMkFeMetadir(install.isMkFeMetadir());
            feBody.setPackageUrl(install.getPackageUrl());
            feBody.setInstallDir(install.getInstallDir());
            creq.setCommandType(CommandType.INSTALL_FE.name());
            creq.setBody(JSON.toJSONString(feBody));
        } else if (ServiceRole.BE.name().equals(install.getRole())) {
            BeInstallCommandRequestBody beBody = new BeInstallCommandRequestBody();
            beBody.setMkBeStorageDir(install.isMkBeStorageDir());
            beBody.setInstallDir(install.getInstallDir());
            beBody.setPackageUrl(install.getPackageUrl());
            creq.setCommandType(CommandType.INSTALL_BE.name());
            creq.setBody(JSON.toJSONString(beBody));
        }
        RResult result = agentRest.commandExec(install.getHost(), agentPort(install.getHost()), creq);
        return result;
    }

    @Override
    public List<Object> execute(DorisExecReq dorisExec) {
        List<Object> results = new ArrayList<>();
        CmdTypeEnum cmdType = CmdTypeEnum.findByName(dorisExec.getCommand());
        List<DorisExec> dorisExecs = dorisExec.getDorisExecs();
        for (DorisExec exec : dorisExecs) {
            CommandType commandType = transAgentCmd(cmdType, ServiceRole.findByName(exec.getRole()));
            CommandRequest creq = new CommandRequest();
            if (CommandType.START_FE.equals(commandType) && !exec.isMaster()) {
                FeStartCommandRequestBody feBody = new FeStartCommandRequestBody();
                feBody.setHelpHostPort(getLeaderFeHostPort());
                creq.setBody(JSON.toJSONString(feBody));
            }
            creq.setCommandType(commandType.name());
            RResult result = agentRest.commandExec(exec.getHost(), agentPort(exec.getHost()), creq);
            Object data = result.getData();
            results.add(data);
        }
        return results;
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
            if (AgentStatus.RUNNING.name().equals(aliveAgent.getStatus())) {
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
        Preconditions.checkArgument(StringUtils.isNotBlank(leaderFe), "can not get leader fe info");
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

    @Override
    public RResult taskInfo(TaskInfoReq taskInfo) {
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskInfo.getTaskId());
        RResult result = agentRest.taskInfo(taskInfo.getHost(), agentPort(taskInfo.getHost()), param);
        return result;
    }

    @Override
    public RResult taskStdlog(TaskLogReq taskInfo) {
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskInfo.getTaskId());
        param.put("offset", taskInfo.getOffset());
        RResult result = agentRest.taskStdLog(taskInfo.getHost(), agentPort(taskInfo.getHost()), param);
        return result;
    }

    @Override
    public RResult taskErrlog(TaskLogReq taskInfo) {
        Map<String, Object> param = new HashMap<>();
        param.put("taskId", taskInfo.getTaskId());
        param.put("offset", taskInfo.getOffset());
        RResult result = agentRest.taskErrLog(taskInfo.getHost(), agentPort(taskInfo.getHost()), param);
        return result;
    }

    private Integer agentPort(String host) {
        AgentEntity agent = agentCache.agentInfo(host);
        if (agent == null) {
            throw new ServerException("query agent port fail");
        }
        return agent.getPort();
    }

    @Override
    public void joinBe(List<String> hosts) {
        AgentEntity aliveAgent = getAliveAgent();
        Integer jdbcPort = getFeQueryPort(aliveAgent.getHost(), aliveAgent.getPort());
        Connection conn = null;
        try {
            conn = JdbcUtil.getConnection(aliveAgent.getHost(), jdbcPort);
        } catch (SQLException e) {
            throw new JdbcException("Failed to get fe's jdbc connection");
        }
        List<Boolean> result = new ArrayList<>();
        for (String be : hosts) {
            //query be's doris port
            Properties beConf = agentRest.roleConfig(be, agentPort(be), ServiceRole.BE.name());
            String port = beConf.getProperty(Constants.KEY_BE_HEARTBEAT_PORT);
            String addBeSqlFormat = "ALTER SYSTEM ADD BACKEND %s:%s";
            String beSql = String.format(addBeSqlFormat, be, port);
            boolean flag = JdbcUtil.execute(conn, beSql);
            if (!flag) {
                log.error("add be node fail:{}", beSql);
            }
            result.add(flag);
        }
        JdbcUtil.closeConn(conn);
        RResult.success(result);
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
