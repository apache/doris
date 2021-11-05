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

package org.apache.doris.stack.task;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.doris.stack.agent.AgentRest;
import org.apache.doris.stack.bean.SpringApplicationContext;
import org.apache.doris.stack.constants.Constants;
import org.apache.doris.stack.exceptions.ServerException;
import org.apache.doris.stack.model.BeJoin;
import org.apache.doris.stack.runner.TaskContext;
import org.apache.doris.stack.service.RestService;

import java.util.Map;
import java.util.Properties;

/**
 * join be to cluster
 **/
@Slf4j
public class JoinBeTask extends AbstractTask {

    private AgentRest agentRest;

    private RestService restService;

    public JoinBeTask(TaskContext taskContext) {
        super(taskContext);
        this.agentRest = SpringApplicationContext.getBean(AgentRest.class);
        this.restService = SpringApplicationContext.getBean(RestService.class);
    }

    @Override
    public void handle() {
        BeJoin requestParams = (BeJoin) taskContext.getRequestParams();
        String backendHost = requestParams.getBeHost();
        Map<String, Integer> backends = Maps.newHashMap();
        Properties beConf = agentRest.roleConfig(backendHost, requestParams.getAgentPort(), ServiceRole.BE.name());
        Integer beHeatPort = Integer.valueOf(beConf.getProperty(Constants.KEY_BE_HEARTBEAT_PORT));
        backends.put(backendHost, beHeatPort);
        Map<String, Boolean> statusMap = restService.addBackends(backendHost, requestParams.getFeHttpPort(), backends, requestParams.getHeaders());

        if (statusMap == null
                || statusMap.get(backendHost) == null
                || !statusMap.get(backendHost)) {
            log.error("failed add backend: {}:{}", backendHost, beHeatPort);
            throw new ServerException("failed add backend:" + backendHost);
        }
    }
}
