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
package org.apache.doris.manager.server.agent;

import org.apache.doris.manager.server.dao.ServerDao;
import org.apache.doris.manager.server.entity.AgentEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * agent host port cache
 **/
@Component
public class AgentCache {

    private static final Map<String, AgentEntity> hostAgentCache = new ConcurrentHashMap<>();

    @Autowired
    private ServerDao serverDao;

    @PostConstruct
    public void init() {
        loadAgents();
    }

    private void loadAgents() {
        List<AgentEntity> agentEntities = serverDao.queryAgentNodes(new ArrayList<>());
        if (agentEntities != null && !agentEntities.isEmpty()) {
            Map<String, AgentEntity> agentsMap = agentEntities.stream().collect(Collectors.toMap(AgentEntity::getHost, v -> v));
            hostAgentCache.putAll(agentsMap);
        }
    }

    /**
     * get agent from cache by host
     */
    public AgentEntity agentInfo(String host) {
        return hostAgentCache.get(host);
    }

    /**
     * put agent to cache
     */
    public void putAgent(AgentEntity agent) {
        hostAgentCache.put(agent.getHost(),agent);
    }
}
