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

package org.apache.doris.manager.agent.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.common.domain.ServiceRole;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;

@Slf4j
public class BrokerService extends Service {
    private Integer ipcPort;

    public BrokerService(String installDir) {
        super(ServiceRole.BROKER, installDir, installDir + AgentConstants.BROKER_CONFIG_FILE_RELATIVE_PATH);
        doLoad();
    }

    @Override
    public void doLoad() {
        String port = getConfig().getProperty(AgentConstants.BROKER_CONFIG_KEY_IPC_PORT);
        if (Objects.isNull(port)) {
            throw new AgentException("get config failed, key:" + AgentConstants.BROKER_CONFIG_KEY_IPC_PORT + ", configFile:" + getConfigFilePath());
        }
        ipcPort = Integer.valueOf(port);
    }

    @Override
    public boolean isHealth() {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress("localhost", ipcPort));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    @Override
    public String serviceProcessKeyword() {
        return AgentConstants.PROCESS_KEYWORD_BROKER;
    }
}
