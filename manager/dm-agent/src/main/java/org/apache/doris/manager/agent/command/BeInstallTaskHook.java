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
package org.apache.doris.manager.agent.command;

import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.common.domain.Role;
import org.apache.doris.manager.agent.task.TaskHook;
import org.apache.logging.log4j.util.Strings;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class BeInstallTaskHook extends TaskHook<BeInstallTaskDesc> {
    @Override
    public void onSuccess(BeInstallTaskDesc taskDesc) {
        AgentContext.setRole(Role.BE);
        AgentContext.setServiceInstallDir(taskDesc.getInstallDir());

        String configFile = AgentContext.getServiceInstallDir().endsWith("/") ? AgentContext.getServiceInstallDir() + "conf/be.conf" : AgentContext.getServiceInstallDir() + "/conf/be.conf";
        Properties props = null;
        try {
            props = new Properties();
            props.load(new FileReader(configFile));
        } catch (IOException e) {
            e.printStackTrace();
            throw new AgentException("load be conf file fail:" + configFile);
        }

        if (props != null && Strings.isNotBlank(props.getProperty("http_port"))) {
            AgentContext.setHealthCheckPort(Integer.valueOf(props.getProperty("http_port")));
        } else {
            AgentContext.setHealthCheckPort(AgentConstants.BE_HTTP_PORT_DEFAULT);
        }
    }
}