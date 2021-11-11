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
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.agent.util.Request;
import org.apache.doris.manager.common.domain.RResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AgentHeartbeat {

    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    private static final long HEARTBEAT_TIME = 10000L;

    public static void start() {
        SCHEDULER.scheduleWithFixedDelay(() -> {
            if (!heartbeat()) {
                log.error("agent heartbeat fail!");
            } else {
                log.info("send heartbeat");
            }
        }, 0, HEARTBEAT_TIME, TimeUnit.MILLISECONDS);
    }

    private static boolean heartbeat() {
        String requestUrl = "http://" + AgentContext.getAgentServer() + "/api/server/heartbeat";
        Map<String, Object> map = new HashMap<>();
        map.put("host", AgentContext.getAgentIp());
        map.put("port", AgentContext.getAgentPort());

        RResult res = null;
        try {
            String result = Request.sendPostRequest(requestUrl, map);
            res = JSON.parseObject(result, RResult.class);
        } catch (Exception ex) {
            return false;
        }
        if (res != null && res.getCode() == 0) {
            return true;
        }
        return false;
    }
}
