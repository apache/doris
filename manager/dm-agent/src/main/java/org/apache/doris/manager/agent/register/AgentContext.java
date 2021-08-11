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
package org.apache.doris.manager.agent.register;


import org.apache.doris.manager.common.domain.Role;
import org.apache.logging.log4j.util.Strings;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AgentContext {
    private static Role role = null;
    private static String serviceInstallDir = null;
    private static Integer healthCheckPort = null;
    private static String agentIp = null;
    private static Integer agentPort = null;
    private static String agentServer = null;
    private static String agentInstallDir = null;
    private static String bashBin = "/bin/sh ";

    public static void init(String role, String agentIp, Integer agentPort, String agentServer, String dorisHomeDir, String agentInstallDir) {
        if (Strings.isNotBlank(role) && Objects.nonNull(Role.findByName(role))) {
            AgentContext.setRole(Role.findByName(role));
        }

        if (Strings.isNotBlank(agentIp)) {
            AgentContext.setAgentIp(agentIp);
        }

        if (Objects.nonNull(agentPort)) {
            AgentContext.setAgentPort(agentPort);
        }

        if (Strings.isNotBlank(agentServer)) {
            AgentContext.setAgentServer(agentServer);
        }

        if (Strings.isNotBlank(dorisHomeDir)) {
            AgentContext.setServiceInstallDir(dorisHomeDir);
        }

        if (Strings.isNotBlank(agentInstallDir)) {
            AgentContext.setAgentInstallDir(agentInstallDir);
        }
    }


    public static String getBashBin() {
        return bashBin;
    }

    public static void setBashBin(String bashBin) {
        AgentContext.bashBin = bashBin;
    }

    public static String getAgentInstallDir() {
        return agentInstallDir;
    }

    public static void setAgentInstallDir(String agentInstallDir) {
        AgentContext.agentInstallDir = agentInstallDir;
    }

    public static Integer getAgentPort() {
        return agentPort;
    }

    public static void setAgentPort(Integer agentPort) {
        AgentContext.agentPort = agentPort;
    }

    public static void setRole(Role r) {
        AgentContext.role = r;
    }

    public static Role getRole() {
        return AgentContext.role;
    }

    public static void setServiceInstallDir(String dir) {
        AgentContext.serviceInstallDir = dir;
    }

    public static String getServiceInstallDir() {
        return AgentContext.serviceInstallDir;
    }

    public static Integer getHealthCheckPort() {
        return AgentContext.healthCheckPort;
    }

    public static void setHealthCheckPort(Integer healthCheckPort) {
        AgentContext.healthCheckPort = healthCheckPort;
    }

    public static String getAgentIp() {
        return agentIp;
    }

    public static void setAgentIp(String agentIp) {
        AgentContext.agentIp = agentIp;
    }

    public static String getAgentServer() {
        return agentServer;
    }

    public static void setAgentServer(String agentServer) {
        AgentContext.agentServer = agentServer;
    }

    public static Map<String, Object> getContext() {
        HashMap<String, Object> context = new HashMap<>();
        context.put("role", AgentContext.role);
        context.put("serviceInstallDir", AgentContext.serviceInstallDir);
        context.put("healthCheckPort", AgentContext.healthCheckPort);
        context.put("agentIp", AgentContext.agentIp);
        context.put("agentPort", AgentContext.agentPort);
        context.put("agentServer", AgentContext.agentServer);
        context.put("agentInstallDir", AgentContext.agentInstallDir);
        return context;
    }
}
