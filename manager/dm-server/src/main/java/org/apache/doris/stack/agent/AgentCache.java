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

package org.apache.doris.stack.agent;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.component.AgentComponent;
import org.apache.doris.stack.entity.AgentEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * agent host port cache
 **/
@Slf4j
@Component
public class AgentCache {

    private static Map<String, AgentEntity> hostAgentCache = new ConcurrentHashMap<>();

    @Autowired
    private AgentComponent agentComponent;

    @PostConstruct
    public void init() {
        loadAgents();
    }

    private void loadAgents() {
        List<AgentEntity> agentEntities = agentComponent.queryAgentNodes(Lists.newArrayList());
        if (agentEntities != null && !agentEntities.isEmpty()) {
            Map<String, AgentEntity> agentsMap = agentEntities.stream().collect(Collectors.toMap(AgentEntity::getHost, v -> v, (v1, v2) -> v1));
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
     * get agent from cache by host
     */
    public Boolean containsAgent(String host) {
        return hostAgentCache.containsKey(host);
    }

    /**
     * put agent to cache
     */
    public void putAgent(AgentEntity agent) {
        hostAgentCache.put(agent.getHost(), agent);
    }

    /**
     * remove agent from cache
     */
    public void removeAgent(String host) {
        hostAgentCache.remove(host);
    }

    /**
     * query agent port by host
     */
    public int agentPort(String host) {
        AgentEntity agent = agentInfo(host);
        if (agent == null) {
            log.error("failed query agent {} port", host);
            throw new ServerException("query agent port fail");
        }
        return agent.getPort();
    }

    public void refresh(List<AgentEntity> agentEntities) {
        hostAgentCache.clear();
        Map<String, AgentEntity> agentsMap = agentEntities.stream().collect(Collectors.toMap(AgentEntity::getHost, v -> v, (v1, v2) -> v1));
        hostAgentCache.putAll(agentsMap);
    }
}
