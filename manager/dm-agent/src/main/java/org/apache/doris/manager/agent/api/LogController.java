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
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.agent.service.Service;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.apache.doris.manager.common.domain.RResult;
import org.apache.doris.manager.common.domain.ServiceRole;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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
@RestController
@RequestMapping("/log")
public class LogController {

    private static final Logger LOG = LogManager.getLogger(LogController.class);
    private static final long WEB_LOG_BYTES = 1024 * 1024;  // 1MB

    /**
     * Obtain service logs according to type: fe.log/fe.out/be.log/be.out
     */
    @RequestMapping(path = "/log", method = RequestMethod.GET)
    public RResult log(@RequestParam String type) {
        Preconditions.checkArgument(StringUtils.isNotBlank(type), "type is required");
        String logPath = getLogPath(type);
        Map<String, String> result = appendLogInfo(logPath);
        return RResult.success(result);
    }

    private String getLogPath(String type) {
        Map<ServiceRole, Service> serviceMap = ServiceContext.getServiceMap();
        switch (type) {
            case "fe.log":
                return serviceMap.get(ServiceRole.FE).getInstallDir() + AgentConstants.FE_LOG_FILE_RELATIVE_PATH;
            case "fe.out":
                return serviceMap.get(ServiceRole.FE).getInstallDir() + AgentConstants.FE_LOG_OUT_FILE_RELATIVE_PATH;
            case "be.log":
                return serviceMap.get(ServiceRole.BE).getInstallDir() + AgentConstants.BE_LOG_FILE_RELATIVE_PATH;
            case "be.out":
                return serviceMap.get(ServiceRole.BE).getInstallDir() + AgentConstants.BE_LOG_OUT_FILE_RELATIVE_PATH;
            default:
                throw new AgentException("can not find log path:" + type);
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
                LOG.warn("fail to close log file: " + logPath, e);
            }
        }
        return map;
    }
}
