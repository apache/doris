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
package org.apache.doris.manager.server.shell;

/**
 * ssh
 **/
public class SSH extends BaseCommand {

    private String user;

    private Integer sshPort;

    private String sshKeyFile;

    private String host;

    private String command;

    public SSH(String user, Integer sshPort, String sshKeyFile, String host, String command) {
        this.user = user;
        this.sshPort = sshPort;
        this.sshKeyFile = sshKeyFile;
        this.host = host;
        this.command = command;
    }

    protected void buildCommand() {
        String[] command = new String[]{"ssh",
                "-o", "ConnectTimeOut=60",
                "-o", "StrictHostKeyChecking=no",
                "-o", "BatchMode=yes",
                "-tt",
                "-i", this.sshKeyFile,
                "-p", this.sshPort.toString(),
                this.user + "@" + this.host, this.command
        };
        this.resultCommand = command;
    }
}
