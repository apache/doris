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

import org.apache.doris.manager.common.domain.RResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * agent rest api
 **/
@Component
public class AgentRest {

    @Autowired
    private RestTemplate restTemplate;

    private static final String AGENT_COMMAND_EXEC = "http://%s:%s/command/execute";
    private static final String AGENT_TASK_INFO = "http://%s:%s/command/result?taskId={taskId}";
    private static final String AGENT_TASK_STD_LOG = "http://%s:%s/command/stdlog?taskId={taskId}&offset={offset}";
    private static final String AGENT_TASK_ERR_LOG = "http://%s:%s/command/errlog?taskId={taskId}&offset={offset}";

    public RResult commandExec(String host, Integer port, Object param) {
        String restUrl = String.format(AGENT_COMMAND_EXEC, host, port);
        RResult result = restTemplate.postForObject(restUrl, param, RResult.class);
        return result;
    }

    public RResult taskInfo(String host, Integer port, Map<String, Object> param) {
        String restUrl = String.format(AGENT_TASK_INFO, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        return result;
    }

    public RResult taskStdLog(String host, Integer port, Map<String, Object> param) {
        String restUrl = String.format(AGENT_TASK_STD_LOG, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        return result;
    }

    public RResult taskErrLog(String host, Integer port, Map<String, Object> param) {
        String restUrl = String.format(AGENT_TASK_ERR_LOG, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        return result;
    }
}
