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

import org.apache.doris.manager.server.constants.AgentStatus;
import org.apache.doris.manager.server.entity.AgentEntity;
import org.apache.doris.manager.server.service.ServerProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * agent status check
 **/
@Component
public class AgentHeatbeatRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AgentHeatbeatRunner.class);
    private static final long HEALTH_TIME = 60 * 1000l;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Autowired
    private ServerProcess serverProcess;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.scheduler.scheduleWithFixedDelay(() -> {
            try {
                heartbeatCheck();
            } catch (Exception ex) {
                log.error("heartbeat check fail:", ex);
            }
        }, 0, HEALTH_TIME, TimeUnit.MILLISECONDS);
    }

    /**
     * The last heartbeat time exceeds the HEALTH_TIME and is recognized as unhealthy
     */
    private void heartbeatCheck() {
        long currTime = System.currentTimeMillis();
        List<AgentEntity> unHealthAgent = new ArrayList<>();
        List<AgentEntity> agents = serverProcess.agentList();
        for (AgentEntity agent : agents) {
            Date lastReportedTime = agent.getLastReportedTime();
            long diff = HEALTH_TIME + 1;
            if(lastReportedTime != null){
                diff = currTime - lastReportedTime.getTime();
            }
            if (diff > HEALTH_TIME) {
                AgentEntity agentEntity = new AgentEntity();
                agentEntity.setId(agent.getId());
                agentEntity.setStatus(AgentStatus.STOP.name());
                unHealthAgent.add(agentEntity);
                log.warn("agent {} is unhealthly", agent.getHost());
            }
        }
        if (!unHealthAgent.isEmpty()) {
            serverProcess.updateBatchAgentStatus(unHealthAgent);
        }
    }
}
