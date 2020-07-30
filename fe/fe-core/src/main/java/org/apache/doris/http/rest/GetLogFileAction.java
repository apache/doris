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

package org.apache.doris.http.rest;

import org.apache.doris.common.Config;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.util.Map;
import java.util.Set;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

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
public class GetLogFileAction extends RestBaseAction {
    private final Set<String> logFileTypes = Sets.newHashSet("fe.audit.log");

    public GetLogFileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/get_log_file", new GetLogFileAction(controller));
        controller.registerHandler(HttpMethod.HEAD, "/api/get_log_file", new GetLogFileAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String logType = request.getSingleParameter("type");
        String logFile = request.getSingleParameter("file");
        
        // check param empty
        if (Strings.isNullOrEmpty(logType)) {
            response.appendContent("Miss type parameter");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        
        // check type valid or not
        if (!logFileTypes.contains(logType)) {
            response.appendContent("log type: " + logType + " is invalid!");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        
        HttpMethod method = request.getRequest().method();
        if (method.equals(HttpMethod.HEAD)) {
            String fileInfos = getFileInfos(logType);
            response.updateHeader("file_infos", fileInfos);
            writeResponse(request, response, HttpResponseStatus.OK);
            return;
        } else if (method.equals(HttpMethod.GET)) {
            File log = getLogFile(logType, logFile);
            if (!log.exists() || !log.isFile()) {
                response.appendContent("Log file not exist: " + log.getName());
                writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
                return;
            }
            writeObjectResponse(request, response, HttpResponseStatus.OK, log, log.getName(), true);
        } else {
            response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
            writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
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
