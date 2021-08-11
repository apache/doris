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
package org.apache.doris.manager.agent;

import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.agent.register.AgentHeartbeat;
import org.apache.doris.manager.agent.register.AgentRegister;
import org.apache.doris.manager.agent.register.AgentRole;
import org.apache.doris.manager.agent.register.ApplicationOption;
import org.apache.doris.manager.common.domain.Role;
import org.apache.doris.manager.agent.common.PropertiesUtil;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.system.ApplicationHome;

@SpringBootApplication
public class DmAgentApplication {
    private static final Logger log = LoggerFactory.getLogger(DmAgentApplication.class);

    public static void main(String[] args) {
        ApplicationOption option = new ApplicationOption();
        CmdLineParser parser = new CmdLineParser(option);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            return;
        }

        String agentPort = PropertiesUtil.getPropValue("server.port");

        ApplicationHome applicationHome = new ApplicationHome(DmAgentApplication.class);
        String agentInstallDir = applicationHome.getSource().getParentFile().getParentFile().toString();

        AgentContext.init(option.role, option.agentIp, Integer.valueOf(agentPort), option.agentServer, option.dorisHomeDir, agentInstallDir);


        String role = AgentRole.queryRole();
        AgentContext.setRole(Role.findByName(role));

        // register agent
        boolean register = AgentRegister.register();
        if (register) {
            SpringApplication.run(DmAgentApplication.class, args);
            AgentHeartbeat.start();
        } else {
            log.error("agent register fail!");
            return;
        }
    }

}
