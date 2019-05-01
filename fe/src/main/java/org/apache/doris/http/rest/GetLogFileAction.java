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
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Set;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class GetLogFileAction extends RestBaseAction {
    Set<String> logFileTypes = Sets.newHashSet("fe.audit.log");
    
    private static final Logger LOG = LogManager.getLogger(GetLogFileAction.class);

    public GetLogFileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/get_log_file", new GetLogFileAction(controller));
        controller.registerHandler(HttpMethod.HEAD, "/api/get_log_file", new GetLogFileAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String logType = request.getSingleParameter("type");
        String logDate = request.getSingleParameter("date");
        
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
        
        // check log file exist
        File logFile = getLogFileName(logType, logDate);
        if (logFile == null || !logFile.exists()) {
            response.appendContent("Log file not exist.");
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }
        
        // get file or just file size
        HttpMethod method = request.getRequest().method();
        if (method.equals(HttpMethod.GET)) {
            writeFileResponse(request, response, HttpResponseStatus.OK, logFile);
        } else if (method.equals(HttpMethod.HEAD)) {
            long fileLength = logFile.length();
            response.updateHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), String.valueOf(fileLength));
            writeResponse(request, response, HttpResponseStatus.OK);
        } else {
            response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
            writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }
    
    private File getLogFileName(String type, String date) {
        String logPath = "";
        
        if ("fe.audit.log".equals(type)) {
            logPath = Config.audit_log_dir + "/fe.audit.log";
            if (!Strings.isNullOrEmpty(date)) {
                logPath += "." + date;
            }
        }

        return new File(logPath);
    }
}
