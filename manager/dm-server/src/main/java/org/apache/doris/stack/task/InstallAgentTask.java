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
import org.apache.doris.stack.util.TelnetUtil;
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
        checkInstallEnv(taskDesc);
        distAgentPackage(taskDesc);
        startAgent(taskDesc);
        return "Install Agent Success";
    }

    /**
     * check telnet, ssh, jdk, installDir
     */
    private void checkInstallEnv(AgentInstall taskDesc) {
        File sshKeyFile = SSH.buildSshKeyFile(buildKey());
        SSH.writeSshKeyFile(taskDesc.getSshKey(), sshKeyFile);
        //check telnet
        if (!TelnetUtil.telnet(taskDesc.getHost(), taskDesc.getSshPort())) {
            log.error("can not telnet host {} port {}", taskDesc.getHost(), taskDesc.getSshPort());
            throw new ServerException("SSH is not available");
        }
        //check ssh
        SSH ssh = new SSH(taskDesc.getUser(), taskDesc.getSshPort(),
                sshKeyFile.getAbsolutePath(), taskDesc.getHost(), "echo ok");
        if (!ssh.run()) {
            log.error("ssh is not available: {}", ssh.getErrorResponse());
            throw new ServerException("SSH is not available");
        }
        //check jdk
        final String checkJavaHome = "source /etc/profile && source ~/.bash_profile && java -version && echo $JAVA_HOME";
        ssh.setCommand(checkJavaHome);
        if (!ssh.run()) {
            log.error("jdk is not available: {}", ssh.getErrorResponse());
            throw new ServerException("jdk is not available");
        }
        //check installDir exist
        String checkFileExistCmd = "if test -e " + taskDesc.getInstallDir() + "; then echo ok; else mkdir -p " + taskDesc.getInstallDir() + " ;fi";
        ssh.setCommand(checkFileExistCmd);
        if (!ssh.run()) {
            log.error("installation path is not available:{}", ssh.getErrorResponse());
            throw new ServerException("The installation path is not available");
        }
    }

    private String buildKey() {
        return "sshkey-" + taskContext.getTaskInstance().getId();
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
        scpFile(agentInstall, agentHome, agentInstall.getInstallDir());
    }

    /**
     * start agent
     */
    private void startAgent(AgentInstall agentInstall) {
        String agentHome = agentInstall.getInstallDir() + File.separator + "agent";
        String command = "cd %s && sh %s  --server %s --agent %s";
        File sshKeyFile = SSH.buildSshKeyFile(buildKey());
        String cmd = String.format(command, agentHome, AGENT_START_SCRIPT, getServerAddr(), agentInstall.getHost());
        SSH ssh = new SSH(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), cmd);
        if (!ssh.run()) {
            log.error("agent start failed:{}", ssh.getErrorResponse());
            throw new ServerException("agent start failed");
        } else {
            log.info("agent start success");
        }
    }

    /**
     * scp agent package
     */
    private void scpFile(AgentInstall agentInstall, String localPath, String remotePath) {
        File sshKeyFile = SSH.buildSshKeyFile(buildKey());
        SCP scp = new SCP(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), localPath, remotePath);
        if (!scp.run()) {
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

    @Override
    public void after() {
        File sshKeyFile = SSH.buildSshKeyFile(buildKey());
        if (sshKeyFile.exists()) {
            sshKeyFile.delete();
        }
    }
}
