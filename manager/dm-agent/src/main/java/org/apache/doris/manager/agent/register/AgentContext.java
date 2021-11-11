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

import com.alibaba.fastjson.JSON;
import org.apache.doris.manager.agent.util.Request;
import org.apache.doris.manager.common.domain.RResult;

import java.util.HashMap;
import java.util.Map;

public class AgentContext {
    private static String agentIp = null;
    private static Integer agentPort = null;
    private static String agentServer = null;
    private static String agentInstallDir = null;

    public static boolean register(String agentIp, Integer agentPort, String agentServer, String agentInstallDir) {
        boolean registerSuccess = registerToServer(agentIp, agentPort, agentServer, agentInstallDir);
        if (!registerSuccess) {
            return false;
        }

        AgentContext.agentIp = agentIp;
        AgentContext.agentPort = agentPort;
        AgentContext.agentServer = agentServer;
        AgentContext.agentInstallDir = agentInstallDir;

        return true;
    }

    private static boolean registerToServer(String agentIp, Integer agentPort, String agentServer, String agentInstallDir) {
        String requestUrl = "http://" + agentServer + "/api/server/register";
        Map<String, Object> map = new HashMap<>();
        map.put("host", agentIp);
        map.put("port", agentPort);
        map.put("installDir", agentInstallDir);

        RResult res = null;
        try {
            String result = Request.sendPostRequest(requestUrl, map);
            res = JSON.parseObject(result, RResult.class);
        } catch (Exception ex) {
            return false;
        }
        if (res != null && new Boolean(true).equals(res.getData())) {
            return true;
        }
        return false;
    }

    public static String getAgentIp() {
        return agentIp;
    }

    public static Integer getAgentPort() {
        return agentPort;
    }

    public static String getAgentServer() {
        return agentServer;
    }

    public static String getAgentInstallDir() {
        return agentInstallDir;
    }

    public static Map<String, Object> getContext() {
        HashMap<String, Object> context = new HashMap<>();
        context.put("agentIp", AgentContext.agentIp);
        context.put("agentPort", AgentContext.agentPort);
        context.put("agentServer", AgentContext.agentServer);
        context.put("agentInstallDir", AgentContext.agentInstallDir);
        return context;
    }
}
