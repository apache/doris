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
import org.apache.commons.io.FileUtils;
import org.apache.doris.stack.constant.EnvironmentDefine;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.AgentInstall;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.service.impl.ServerProcessImpl;
import org.apache.doris.stack.shell.SCP;
import org.apache.doris.stack.shell.SSH;
import org.springframework.boot.system.ApplicationHome;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

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
    public void handle() {
        AgentInstall requestParams = (AgentInstall) taskContext.getRequestParams();
        distAgentPackage(requestParams);
        startAgent(requestParams);
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
        File sshKeyFile = buildSshKeyFile();
        writeSshKeyFile(agentInstall.getSshKey(), sshKeyFile);
        scpFile(agentInstall, agentHome, agentInstall.getInstallDir());
    }

    /**
     * start agent
     */
    private void startAgent(AgentInstall agentInstall) {
        String command = "sh " + agentInstall.getInstallDir() + File.separator + AGENT_START_SCRIPT + " --server " + getServerAddr() + " --agent %s";
        File sshKeyFile = buildSshKeyFile();
        String cmd = String.format(command, agentInstall.getHost());
        SSH ssh = new SSH(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), cmd);
        Integer run = ssh.run();
        if (run != 0) {
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
        File sshKeyFile = buildSshKeyFile();
        //check remote dir exist
        SSH ssh = new SSH(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), checkFileExistCmd);
        if (ssh.run() != 0) {
            throw new ServerException("scp create remote dir failed");
        }
        SCP scp = new SCP(agentInstall.getUser(), agentInstall.getSshPort(),
                sshKeyFile.getAbsolutePath(), agentInstall.getHost(), localPath, remotePath);
        Integer run = scp.run();
        if (run != 0) {
            log.error("scp agent package failed:{} to {}", localPath, remotePath);
            throw new ServerException("scp agent package failed");
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
        String port = System.getenv(EnvironmentDefine.STUDIO_PORT_ENV);
        return host + ":" + port;
    }
}
