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
import org.apache.doris.manager.common.domain.AgentRoleRegister;
import org.apache.doris.manager.common.domain.RResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * query agent role
 **/
public class AgentRole {

    public static List<AgentRoleRegister> queryRole() {
        String requestUrl = "http://" + AgentContext.getAgentServer() + "/server/agentRole";
        Map<String, Object> map = new HashMap<>();
        map.put("host", AgentContext.getAgentIp());

        String result = Request.sendGetRequest(requestUrl, map);
        RResult resultResp = JSON.parseObject(result, RResult.class);
        if (resultResp == null || resultResp.getCode() != 0) {
            return null;
        }
        List<AgentRoleRegister> agentRoles = JSON.parseArray(JSON.toJSONString(resultResp.getData()), AgentRoleRegister.class);
        return agentRoles;
    }
}
