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
import org.apache.doris.manager.common.domain.BeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.FeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.FeStartCommandRequestBody;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.Role;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.constants.CmdTypeEnum;
import org.apache.doris.stack.dao.AgentRoleRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.req.DorisExec;
import org.apache.doris.stack.req.DorisExecReq;
import org.apache.doris.stack.req.DorisInstallReq;
import org.apache.doris.stack.req.InstallInfo;
import org.apache.doris.stack.req.TaskInfoReq;
import org.apache.doris.stack.req.TaskLogReq;
import org.apache.doris.stack.service.ServerAgent;
import org.apache.doris.stack.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * server agent
 **/
@Service
public class ServerAgentImpl implements ServerAgent {

    private static final Logger log = LoggerFactory.getLogger(ServerAgentImpl.class);

    @Autowired
    private AgentRest agentRest;

    @Autowired
    private AgentCache agentCache;

    @Autowired
    private AgentRoleRepository roleRepository;

    @Override
    @Transactional
    public List<Object> install(DorisInstallReq installReq) {
        List<String> agentRoleList = roleRepository.findAll().stream()
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
            roleRepository.save(new AgentRoleEntity(install.getHost(), install.getRole(), install.getInstallDir()));

            results.add(result.getData());
        }
        return results;
    }

    private RResult installDoris(InstallInfo install) {
        CommandRequest creq = new CommandRequest();
        if (Role.FE.name().equals(install.getRole())) {
            FeInstallCommandRequestBody feBody = new FeInstallCommandRequestBody();
            feBody.setMkFeMetadir(install.isMkFeMetadir());
            feBody.setPackageUrl(install.getPackageUrl());
            feBody.setInstallDir(install.getInstallDir());
            creq.setCommandType(CommandType.INSTALL_FE.name());
            creq.setBody(JSON.toJSONString(feBody));
        } else if (Role.BE.name().equals(install.getRole())) {
            BeInstallCommandRequestBody beBody = new BeInstallCommandRequestBody();
            beBody.setMkBeStorageDir(install.isMkBeStorageDir());
            beBody.setInstallDir(install.getInstallDir());
            beBody.setPackageUrl(install.getPackageUrl());
            creq.setCommandType(CommandType.INSTALL_BE.name());
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
            CommandType commandType = transAgentCmd(cmdType, Role.findByName(exec.getRole()));
            CommandRequest creq = new CommandRequest();
            if (CommandType.START_FE.equals(commandType) && exec.isMaster()) {
                FeStartCommandRequestBody feBody = new FeStartCommandRequestBody();
                feBody.setHelper(true);
                // request fe master ip and port
                feBody.setHelpHostPort("");
            }
            creq.setCommandType(commandType.name());
            RResult result = agentRest.commandExec(exec.getHost(), agentPort(exec.getHost()), creq);
            Object data = result.getData();
            results.add(data);
        }
        return results;
    }

    /**
     * trans server command to agent command
     */
    public CommandType transAgentCmd(CmdTypeEnum cmdType, Role role) {
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

}
