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

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import org.apache.doris.manager.common.domain.HardwareInfo;
import org.apache.doris.manager.common.domain.RResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.Properties;

/**
 * agent rest api
 **/
@Component
public class AgentRest {

    private static final String AGENT_COMMAND_EXEC = "http://%s:%s/command/execute";
    private static final String AGENT_TASK_INFO = "http://%s:%s/command/result?taskId={taskId}";
    private static final String SERVICE_CONFIG = "http://%s:%s/service/config?serviceRole={serviceRole}";
    private static final String SERVICE_LOG = "http://%s:%s/log?type={type}";
    private static final String AGENT_TASK_LOG = "http://%s:%s/log/task?taskId={taskId}";
    private static final String HARDWARE_INFO = "http://%s:%s/hardware/view";

    @Autowired
    private RestTemplate restTemplate;

    public RResult commandExec(String host, Integer port, Object param) {
        String restUrl = String.format(AGENT_COMMAND_EXEC, host, port);
        RResult result = restTemplate.postForObject(restUrl, param, RResult.class);
        return result;
    }

    public RResult serverLog(String host, Integer port, String type) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("type", type);
        String restUrl = String.format(SERVICE_LOG, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        return result;
    }

    public RResult taskInfo(String host, Integer port, String taskId) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("taskId", taskId);
        String restUrl = String.format(AGENT_TASK_INFO, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        return result;
    }

    public RResult taskLog(String host, Integer port, String taskId) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("taskId", taskId);
        String restUrl = String.format(AGENT_TASK_LOG, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        return result;
    }

    public Properties roleConfig(String host, Integer port, String serviceRole) {
        String restUrl = String.format(SERVICE_CONFIG, host, port);
        Map<String, Object> param = Maps.newHashMap();
        param.put("serviceRole", serviceRole);
        RResult result = restTemplate.getForObject(restUrl, RResult.class, param);
        Properties roleConf = new Properties();
        if (result != null && result.isSuccess()) {
            roleConf = JSON.parseObject(JSON.toJSONString(result.getData()), Properties.class);
        }
        return roleConf;
    }

    public HardwareInfo hardwareInfo(String host, Integer port) {
        String restUrl = String.format(HARDWARE_INFO, host, port);
        RResult result = restTemplate.getForObject(restUrl, RResult.class);
        HardwareInfo hardware = null;
        if (result != null && result.isSuccess()) {
            hardware = JSON.parseObject(JSON.toJSONString(result.getData()), HardwareInfo.class);
        }
        return hardware;
    }
}
