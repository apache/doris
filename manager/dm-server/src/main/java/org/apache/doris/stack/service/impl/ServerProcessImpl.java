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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.doris.stack.agent.AgentCache;
import org.apache.doris.stack.component.AgentComponent;
import org.apache.doris.stack.component.AgentRoleComponent;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.entity.AgentRoleEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.req.AgentRegister;
import org.apache.doris.stack.req.SshInfo;
import org.apache.doris.stack.service.ServerProcess;
import org.apache.doris.stack.shell.SCP;
import org.apache.doris.stack.shell.SSH;
import org.apache.doris.stack.util.Preconditions;
import org.apache.doris.stack.util.PropertiesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.system.ApplicationHome;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * server
 **/
@Service
@Slf4j
public class ServerProcessImpl implements ServerProcess {

    private static final String AGENT_INSTALL_DIR = PropertiesUtil.getPropValue(Constants.KEY_DORIS_AGENT_INSTALL_DIR);
    private static final String AGENT_START_SCRIPT = PropertiesUtil.getPropValue(Constants.KEY_DORIS_AGENT_START_SCRIPT);

    @Autowired
    private AgentComponent agentComponent;

    @Autowired
    private AgentRoleComponent agentRoleComponent;

    @Autowired
    private AgentCache agentCache;

    @Override
    public void initAgent(SshInfo sshInfo) {
        ApplicationHome applicationHome = new ApplicationHome(ServerProcessImpl.class);
        String dorisManagerHome = applicationHome.getSource().getParentFile().getParentFile().toString();
        log.info("doris manager home : {}", dorisManagerHome);
        String agentHome = dorisManagerHome + File.separator + "agent";
        Preconditions.checkNotNull(sshInfo.getHosts(), "hosts is empty");
        File sshKeyFile = buildSshKeyFile();
        writeSshKeyFile(sshInfo.getSshKey(), sshKeyFile);
        scpFile(sshInfo, agentHome, AGENT_INSTALL_DIR);
    }

    @Override
    public void startAgent(SshInfo sshInfo) {
        String command = "sh " + AGENT_INSTALL_DIR + File.separator + AGENT_START_SCRIPT + " --server " + getServerAddr() + " --agent %s";
        List<String> hosts = sshInfo.getHosts();
        for (String host : hosts) {
            File sshKeyFile = buildSshKeyFile();
            String cmd = String.format(command, host);
            SSH ssh = new SSH(sshInfo.getUser(), sshInfo.getSshPort(),
                    sshKeyFile.getAbsolutePath(), host, cmd);
            Integer run = ssh.run();
            if (run != 0) {
                throw new ServerException("agent start failed");
            } else {
                log.info("agent start success");
            }
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

    /**
     * scp agent package
     */
    private void scpFile(SshInfo sshDesc, String localPath, String remotePath) {
        List<String> hosts = sshDesc.getHosts();
        String checkFileExistCmd = "if test -e " + remotePath + "; then echo ok; else mkdir -p " + remotePath + " ;fi";
        File sshKeyFile = buildSshKeyFile();
        for (String host : hosts) {
            //check remote dir exist
            SSH ssh = new SSH(sshDesc.getUser(), sshDesc.getSshPort(),
                    sshKeyFile.getAbsolutePath(), host, checkFileExistCmd);
            if (ssh.run() != 0) {
                throw new ServerException("scp create remote dir failed");
            }
            SCP scp = new SCP(sshDesc.getUser(), sshDesc.getSshPort(),
                    sshKeyFile.getAbsolutePath(), host, localPath, remotePath);
            Integer run = scp.run();
            if (run != 0) {
                log.error("scp agent package failed:{} to {}", localPath, remotePath);
                throw new ServerException("scp agent package failed");
            }
        }
    }

    /**
     * sshkey trans to file
     */
    private void writeSshKeyFile(String sshKey, File sshKeyFile) {
        try {
            if (sshKeyFile.exists()) {
                sshKeyFile.delete();
            }
            FileUtils.writeStringToFile(sshKeyFile, sshKey, Charset.defaultCharset());
        } catch (IOException e) {
            log.error("build sshKey file failed:", e);
            throw new ServerException("build sshKey file failed");
        }
    }

    /**
     * build sshkeyfile per request
     */
    private File buildSshKeyFile() {
        File sshKeyFile = new File("conf", "sshkey");

        // chmod ssh key file permission to 600
        try {
            Set<PosixFilePermission> permission = new HashSet<>();
            permission.add(PosixFilePermission.OWNER_READ);
            permission.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(Paths.get(sshKeyFile.getAbsolutePath()), permission);
        } catch (IOException e) {
            log.error("set ssh key file permission fail");
        }
        return sshKeyFile;
    }

    /**
     * get server address
     */
    private String getServerAddr() {
        String host = null;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new ServerException("get server ip fail");
        }
        String port = PropertiesUtil.getPropValue(Constants.KEY_SERVER_PORT);
        return host + ":" + port;
    }

}
