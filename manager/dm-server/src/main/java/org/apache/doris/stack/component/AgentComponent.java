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
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.dao.AgentRepository;
import org.apache.doris.stack.entity.AgentEntity;
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

    public List<AgentEntity> queryAgentNodes(int clusterId) {
        return agentRepository.queryAgentNodesByClusterId(clusterId);
    }

    public AgentEntity agentInfo(String host) {
        List<AgentEntity> agentEntities = agentRepository.agentInfo(host);
        if (agentEntities != null && !agentEntities.isEmpty()) {
            return agentEntities.get(0);
        } else {
            return null;
        }
    }

    public boolean refreshAgentStatus(String host, Integer port) {
        AgentEntity agentInfo = agentInfo(host);
        if (agentInfo == null) {
            return false;
        }
        agentInfo.setStatus(AgentStatus.RUNNING);
        agentInfo.setLastReportedTime(new Date());
        agentRepository.save(agentInfo);
        return true;
    }

    public AgentEntity saveAgent(AgentEntity agent) {
        if (agent.getId() == 0 && StringUtils.isNotBlank(agent.getHost())) {
            //When retry installing the agent, overwrite the previously installed
            AgentEntity agentEntity = agentInfo(agent.getHost());
            if (agentEntity != null) {
                agent.setId(agentEntity.getId());
            }
        }
        return agentRepository.save(agent);
    }

    public void removeAgent(String agentHost) {
        AgentEntity agentEntity = agentInfo(agentHost);
        if (agentEntity != null) {
            agentRepository.deleteById(agentEntity.getId());
        }
    }

}
