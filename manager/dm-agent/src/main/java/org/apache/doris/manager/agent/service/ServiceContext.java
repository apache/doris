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

import com.alibaba.fastjson.JSON;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.agent.util.Request;
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.ServiceRole;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ServiceContext {
    private static Map<ServiceRole, Service> serviceMap = new HashMap<>();

    private static synchronized void registeService(Service service) {
        serviceMap.put(service.getServiceRole(), service);
    }

    public static Map<ServiceRole, Service> getServiceMap() {
        return new HashMap<>(serviceMap);
    }

    public static boolean register(Service service) {
        boolean b = registerToServer(AgentContext.getAgentServer(), AgentContext.getAgentIp(), service.getServiceRole(), service.getInstallDir());
        if (!b) {
            throw new AgentException("register to server failed,server:" + AgentContext.getAgentServer()
                    + ",agentIp:" + AgentContext.getAgentIp()
                    + ",service:" + service.toString());
        }
        registeService(service);
        return true;
    }

    public static boolean registerFromServer(String serverIpPort, String agentIp) {
        List<AgentRoleRegister> list = queryServiceList(serverIpPort, agentIp);
        if (Objects.isNull(list)) {
            throw new AgentException("query service from server failed, server:" + serverIpPort + ",agentIp:" + agentIp);
        }

        if (list.isEmpty()) {
            return true;
        }

        for (AgentRoleRegister service : list) {
            if (ServiceRole.findByName(service.getRole()) == ServiceRole.FE) {
                FeService feService = new FeService(service.getInstallDir());
                registeService(feService);
            } else if (ServiceRole.findByName(service.getRole()) == ServiceRole.BE) {
                BeService beService = new BeService(service.getInstallDir());
                registeService(beService);
            } else if (ServiceRole.findByName(service.getRole()) == ServiceRole.BROKER) {
                BrokerService brokerService = new BrokerService(service.getInstallDir());
                registeService(brokerService);
            }
        }

        return true;
    }

    private static boolean registerToServer(String serverIpPort, String agentIp, ServiceRole role, String serviceInstallDir) {
        String requestUrl = "http://" + serverIpPort + "/api/agent/register";
        Map<String, Object> map = new HashMap<>();
        map.put("host", agentIp);
        map.put("role", role.name());
        map.put("installDir", serviceInstallDir);

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

    private static List<AgentRoleRegister> queryServiceList(String serverIpPort, String agentIp) {
        String requestUrl = "http://" + serverIpPort + "/api/server/agentRole";
        Map<String, Object> map = new HashMap<>();
        map.put("host", agentIp);

        String result = Request.sendGetRequest(requestUrl, map);
        RResult resultResp = JSON.parseObject(result, RResult.class);
        if (resultResp == null || resultResp.getCode() != 0) {
            return null;
        }
        List<AgentRoleRegister> agentRoles = JSON.parseArray(JSON.toJSONString(resultResp.getData()), AgentRoleRegister.class);
        return agentRoles;
    }
}
