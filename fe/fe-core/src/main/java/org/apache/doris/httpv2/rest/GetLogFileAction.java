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

package org.apache.doris.httpv2.rest;

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 *  get log file infos:
 *      curl -I http://fe_host:http_port/api/get_log_file?type=fe.audit.log
 *      return:
 *          HTTP/1.1 200 OK
 *          file_infos: {"fe.audit.log":24759,"fe.audit.log.20190528.1":132934}
 *          content-type: text/html
 *          connection: keep-alive
 *
 *  get log file:
 *      curl -X GET http://fe_host:http_port/api/get_log_file?type=fe.audit.log&file=fe.audit.log.20190528.1
 */
@RestController
public class GetLogFileAction extends RestBaseController {
    private final Set<String> logFileTypes = Sets.newHashSet("fe.audit.log");

    @RequestMapping(path = "/api/get_log_file", method = {RequestMethod.GET, RequestMethod.HEAD})
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String logType = request.getParameter("type");
        String logFile = request.getParameter("file");

        // check param empty
        if (Strings.isNullOrEmpty(logType)) {
            return ResponseEntityBuilder.badRequest("Miss type parameter");
        }

        // check type valid or not
        if (!logFileTypes.contains(logType)) {
            return ResponseEntityBuilder.badRequest("log type: " + logType + " is invalid!");
        }

        String method = request.getMethod();
        if (method.equals(RequestMethod.HEAD.name())) {
            String fileInfos = getFileInfos(logType);
            response.setHeader("file_infos", fileInfos);
            return ResponseEntityBuilder.ok();
        } else if (method.equals(RequestMethod.GET.name())) {
            File log = getLogFile(logType, logFile);
            if (!log.exists() || !log.isFile()) {
                return ResponseEntityBuilder.okWithCommonError("Log file not exist: " + log.getName());
            }
            if (log != null) {
                try {
                    getFile(request, response, log, log.getName());
                } catch (IOException e) {
                    return ResponseEntityBuilder.internalError(e.getMessage());
                }
            } else {
                return ResponseEntityBuilder.okWithCommonError("Log file not exist: " + log.getName());
            }
        }
        return ResponseEntityBuilder.ok();
    }

    private String getFileInfos(String logType) {
        Map<String, Long> fileInfos = Maps.newTreeMap();
        if (logType.equals("fe.audit.log")) {
            File logDir = new File(Config.audit_log_dir);
            File[] files = logDir.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isFile() && files[i].getName().startsWith("fe.audit.log")) {
                    fileInfos.put(files[i].getName(), files[i].length());
                }
            }
        }

        String result = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            result = mapper.writeValueAsString(fileInfos);
        } catch (Exception e) {
            // do nothing
        }
        return result;
    }

    private File getLogFile(String logType, String logFile) {
        String logPath = "";
        if ("fe.audit.log".equals(logType)) {
            logPath = Config.audit_log_dir + "/" + logFile;
        }
        return new File(logPath);
    }
}
