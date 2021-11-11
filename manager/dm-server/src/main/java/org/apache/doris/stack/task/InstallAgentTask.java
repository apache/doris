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

package org.apache.doris.stack.task;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.constant.EnvironmentDefine;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.task.AgentInstall;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.service.impl.ServerProcessImpl;
import org.apache.doris.stack.shell.SCP;
import org.apache.doris.stack.shell.SSH;
import org.springframework.boot.system.ApplicationHome;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * intall agent task
 **/
@Slf4j
public class InstallAgentTask extends AbstractTask {

    private static final String AGENT_START_SCRIPT = Constants.KEY_DORIS_AGENT_START_SCRIPT;

    public InstallAgentTask(TaskContext taskContext) {
        super(taskContext);
    }

    @Override
    public Object handle() {
        AgentInstall taskDesc = (AgentInstall) taskContext.getTaskDesc();
        distAgentPackage(taskDesc);
        startAgent(taskDesc);
        return "Install Agent Success";
    }

    /**
     * scp agent to host
     */
    private void distAgentPackage(AgentInstall agentInstall) {
        ApplicationHome applicationHome = new ApplicationHome(ServerProcessImpl.class);
        String dorisManagerHome = applicationHome.getSource().getParentFile().getParentFile().getParentFile().toString();
        log.info("doris manager home : {}", dorisManagerHome);
        String agentHome = dorisManagerHome + File.separator + "agent";
        Preconditions.checkNotNull(agentInstall.getHost(), "host is empty");
        File sshKeyFile = SSH.buildSshKeyFile();
        SSH.writeSshKeyFile(agentInstall.getSshKey(), sshKeyFile);
        scpFile(agentInstall, agentHome, agentInstall.getInstallDir());
    }

    /**
     * start agent
     */
    private void startAgent(AgentInstall agentInstall) {
        String agentHome = agentInstall.getInstallDir() + File.separator + "agent";
        String command = "cd %s && sh %s  --server %s --agent %s";
        File sshKeyFile = SSH.buildSshKeyFile();
        String cmd = String.format(command, agentHome, AGENT_START_SCRIPT, getServerAddr(), agentInstall.getHost());
        SSH ssh = new SSH(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), cmd);
        if (ssh.run()) {
            throw new ServerException("agent start failed");
        } else {
            log.info("agent start success");
        }
    }

    /**
     * scp agent package
     */
    private void scpFile(AgentInstall agentInstall, String localPath, String remotePath) {
        String checkFileExistCmd = "if test -e " + remotePath + "; then echo ok; else mkdir -p " + remotePath + " ;fi";
        File sshKeyFile = SSH.buildSshKeyFile();
        //check remote dir exist
        SSH ssh = new SSH(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), checkFileExistCmd);
        if (ssh.run()) {
            throw new ServerException("scp create remote dir failed");
        }
        SCP scp = new SCP(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), localPath, remotePath);
        if (scp.run()) {
            log.error("scp agent package failed:{} to {}", localPath, remotePath);
            throw new ServerException("scp agent package failed");
        }
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
        String port = System.getenv(EnvironmentDefine.STUDIO_PORT_ENV);
        return host + ":" + port;
    }
}
