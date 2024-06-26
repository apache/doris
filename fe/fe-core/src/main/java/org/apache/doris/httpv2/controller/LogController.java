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

package org.apache.doris.httpv2.controller;

import org.apache.doris.common.Config;
import org.apache.doris.common.Log4jConfig;
import org.apache.doris.httpv2.config.ReadEnvironment;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/rest/v1")
public class LogController {

    private static final Logger LOG = LogManager.getLogger(LogController.class);
    private static final long WEB_LOG_BYTES = 1024 * 1024;  // 1MB

    @Autowired
    private ReadEnvironment readEnvironment;

    @RequestMapping(path = "/log", method = RequestMethod.GET)
    public Object log() {
        Map<String, Map<String, String>> map = new HashMap<>();
        appendLogConf(map, StringUtils.EMPTY, StringUtils.EMPTY);
        appendLogInfo(map);
        return ResponseEntityBuilder.ok(map);
    }

    @RequestMapping(path = "/log", method = RequestMethod.POST)
    public Object logLevel(HttpServletRequest request) {
        Map<String, Map<String, String>> map = new HashMap<>();
        // get parameters
        String addVerboseName = request.getParameter("add_verbose");
        String delVerboseName = request.getParameter("del_verbose");
        LOG.info("add verbose name: {}, del verbose name: {}", addVerboseName, delVerboseName);
        appendLogConf(map, addVerboseName, delVerboseName);
        return ResponseEntityBuilder.ok(map);
    }

    private void appendLogConf(Map<String, Map<String, String>> content, String addVerboseName, String delVerboseName) {
        Map<String, String> map = new HashMap<>();

        try {
            Log4jConfig.Tuple<String, String, String[], String[]> configs =
                    Log4jConfig.updateLogging(null, null, null, null);
            if (!Strings.isNullOrEmpty(addVerboseName)) {
                addVerboseName = addVerboseName.trim();
                List<String> verboseNames = Lists.newArrayList(configs.z);
                if (!verboseNames.contains(addVerboseName)) {
                    verboseNames.add(addVerboseName);
                    configs = Log4jConfig.updateLogging(null, null,
                            verboseNames.toArray(new String[verboseNames.size()]), null);
                    readEnvironment.reinitializeLoggingSystem();
                }
            }
            if (!Strings.isNullOrEmpty(delVerboseName)) {
                delVerboseName = delVerboseName.trim();
                List<String> verboseNames = Lists.newArrayList(configs.z);
                if (verboseNames.contains(delVerboseName)) {
                    verboseNames.remove(delVerboseName);
                    configs = Log4jConfig.updateLogging(null, null,
                            verboseNames.toArray(new String[verboseNames.size()]), null);
                    readEnvironment.reinitializeLoggingSystem();
                }
            }

            map.put("Level", configs.x);
            map.put("Mode", configs.y);
            map.put("VerboseNames", StringUtils.join(configs.z, ","));
            map.put("AuditNames", StringUtils.join(configs.u, ","));
            content.put("LogConfiguration", map);
        } catch (IOException e) {
            LOG.error(e);
        }
    }

    private void appendLogInfo(Map<String, Map<String, String>> content) {
        Map<String, String> map = new HashMap<>();

        String logDir = Strings.isNullOrEmpty(Config.sys_log_dir) ? System.getenv("LOG_DIR") : Config.sys_log_dir;
        final String logPath = logDir + "/fe.warn.log";
        map.put("logPath", logPath);

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(logPath, "r");
            long fileSize = raf.length();
            long startPos = fileSize < WEB_LOG_BYTES ? 0L : fileSize - WEB_LOG_BYTES;
            long webContentLength = Math.min(fileSize, WEB_LOG_BYTES);
            raf.seek(startPos);
            map.put("showingLast", webContentLength + " bytes of log");
            StringBuilder sb = new StringBuilder();
            String line = "";
            sb.append("<pre>");
            while ((line = raf.readLine()) != null) {
                sb.append(line).append("</br>");
            }
            sb.append("</pre>");
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
        content.put("LogContents", map);
    }
}
