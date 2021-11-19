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

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.constants.AgentStatus;
import org.apache.doris.stack.dao.AgentRepository;
import org.apache.doris.stack.entity.AgentEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * agent status check
 **/
@Component
@Slf4j
public class AgentHeatbeatRunner implements ApplicationRunner {

    private static final long HEALTH_TIME = 60 * 1000L;
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Autowired
    private AgentRepository agentRepository;

    @Autowired
    private AgentCache agentCache;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        this.scheduler.scheduleWithFixedDelay(() -> {
            try {
                heartbeatCheck();
            } catch (Exception ex) {
                log.error("heartbeat check fail:", ex);
                ex.printStackTrace();
            }
        }, HEALTH_TIME, HEALTH_TIME, TimeUnit.MILLISECONDS);
    }

    /**
     * The last heartbeat time exceeds the HEALTH_TIME and is recognized as unhealthy
     */
    private void heartbeatCheck() {
        long currTime = System.currentTimeMillis();
        List<AgentEntity> agents = agentRepository.findAll();
        for (AgentEntity agent : agents) {
            Date lastReportedTime = agent.getLastReportedTime();
            if (lastReportedTime == null) {
                continue;
            }
            long diff = currTime - lastReportedTime.getTime();
            if (diff > HEALTH_TIME) {
                agent.setStatus(AgentStatus.STOP);
                agentRepository.save(agent);
                agentCache.putAgent(agent);
                log.warn("agent {} is unhealthly", agent.getHost());
            }
        }
    }
}
