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

package org.apache.doris.stack.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.dao.AgentRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.req.AgentRegister;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Component
@Slf4j
public class AgentComponent {

    @Autowired
    private AgentRepository agentRepository;

    public List<AgentEntity> queryAgentNodes(List<String> hosts) {
        if (hosts == null || hosts.isEmpty()) {
            return agentRepository.findAll();
        }
        return agentRepository.queryAgentNodes(hosts);
    }

    public AgentEntity agentInfo(String host) {
        List<AgentEntity> agentEntities = agentRepository.agentInfo(host);
        if (agentEntities != null && !agentEntities.isEmpty()) {
            return agentEntities.get(0);
        } else {
            return null;
        }
    }

    public int refreshAgentStatus(String host, Integer port) {
        AgentEntity agentInfo = agentInfo(host);
        if (agentInfo == null) {
            return 0;
        }
        agentInfo.setStatus(AgentStatus.RUNNING.name());
        agentInfo.setLastReportedTime(new Date());
        agentRepository.save(agentInfo);
        return 1;
    }

    public AgentEntity registerAgent(AgentRegister agent) {
        AgentEntity agentEntity = new AgentEntity(agent.getHost(), agent.getPort(), agent.getInstallDir(), AgentStatus.RUNNING.name());
        return agentRepository.save(agentEntity);
    }
}
