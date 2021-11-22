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

package org.apache.doris.stack.shell;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.doris.stack.exceptions.ServerException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

/**
 * ssh
 **/
@Slf4j
@Data
public class SSH extends BaseCommand {

    private String user;

    private int sshPort;

    private String sshKeyFile;

    private String host;

    private String command;

    public SSH(String user, int sshPort, String sshKeyFile, String host, String command) {
        this.user = user;
        this.sshPort = sshPort;
        this.sshKeyFile = sshKeyFile;
        this.host = host;
        this.command = command;
    }

    /**
     * sshkey trans to file
     */
    public static void writeSshKeyFile(String sshKey, File sshKeyFile) {
        try {
            if (sshKeyFile.exists()) {
                sshKeyFile.delete();
            }
            FileUtils.writeStringToFile(sshKeyFile, sshKey, Charset.defaultCharset());
            chmodSshKey(sshKeyFile);
        } catch (IOException e) {
            log.error("build sshKey file failed:", e);
            throw new ServerException("build sshKey file failed");
        }
    }

    private static void chmodSshKey(File sshKeyFile) {
        // chmod ssh key file permission to 600
        try {
            Set<PosixFilePermission> permission = new HashSet<>();
            permission.add(PosixFilePermission.OWNER_READ);
            permission.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(Paths.get(sshKeyFile.getAbsolutePath()), permission);
        } catch (IOException e) {
            log.error("set ssh key file permission fail,", e);
            throw new ServerException("set ssh key file permission fail");
        }
    }

    /**
     * build sshkeyfile
     */
    public static File buildSshKeyFile(String fileName) {
        File sshKeyFile = new File("conf", fileName);
        return sshKeyFile;
    }

    protected void buildCommand() {
        String[] command = new String[]{"ssh",
                "-o", "ConnectTimeOut=60",
                "-o", "StrictHostKeyChecking=no",
                "-o", "BatchMode=yes",
                "-i", this.sshKeyFile,
                "-p", String.valueOf(this.sshPort),
                this.user + "@" + this.host, this.command
        };
        this.resultCommand = command;
    }
}
