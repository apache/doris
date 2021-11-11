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

package org.apache.doris.manager.agent.api;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.agent.service.Service;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * fe be log
 **/
@Slf4j
@RestController
@RequestMapping("/log")
public class LogController {

    private static final long WEB_LOG_BYTES = 1024 * 1024;  // 1MB

    /**
     * Obtain service logs according to type: fe.log/fe.out/be.log/be.out
     */
    @GetMapping
    public RResult serviceLog(@RequestParam String type) {
        String logPath = getLogPath(type);
        Map<String, String> result = appendLogInfo(logPath);
        return RResult.success(result);
    }

    /**
     * Obtain task log
     */
    @GetMapping("/task")
    public RResult taskLog(@RequestParam String taskId) {
        String logPath = AgentContext.getAgentInstallDir() + AgentConstants.TASK_LOG_FILE_RELATIVE_PATH;
        Map<String, String> result = appendLogInfo(logPath);
        return RResult.success(result);
    }

    private String getLogPath(String type) {
        Preconditions.checkArgument(StringUtils.isNotBlank(type), "type is required");
        Map<ServiceRole, Service> serviceMap = ServiceContext.getServiceMap();
        if (type.startsWith(ServiceRole.FE.name().toLowerCase())) {
            return serviceMap.get(ServiceRole.FE).getInstallDir() + AgentConstants.LOG_FILE_RELATIVE_PATH + type;
        } else if (type.startsWith(ServiceRole.BE.name().toLowerCase())) {
            return serviceMap.get(ServiceRole.BE).getInstallDir() + AgentConstants.LOG_FILE_RELATIVE_PATH + type;
        } else {
            throw new AgentException("can not find this type log path:" + type);
        }
    }

    private Map<String, String> appendLogInfo(String logPath) {
        Map<String, String> map = new HashMap<>();
        map.put("logPath", logPath);

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(logPath, "r");
            long fileSize = raf.length();
            long startPos = fileSize < WEB_LOG_BYTES ? 0L : fileSize - WEB_LOG_BYTES;
            long webContentLength = fileSize < WEB_LOG_BYTES ? fileSize : WEB_LOG_BYTES;
            raf.seek(startPos);
            map.put("showingLast", webContentLength + " bytes of log");
            StringBuilder sb = new StringBuilder();
            String line = "";
            while ((line = raf.readLine()) != null) {
                sb.append(line).append("\n");
            }
            map.put("log", sb.toString());

        } catch (FileNotFoundException e) {
            map.put("error", "Couldn't open log file: " + logPath);
        } catch (IOException e) {
            map.put("error", "Failed to read log file: " + logPath);
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                log.warn("fail to close log file: " + logPath, e);
            }
        }
        return map;
    }
}
