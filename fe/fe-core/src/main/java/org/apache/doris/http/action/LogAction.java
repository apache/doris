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

package org.apache.doris.http.action;

import org.apache.doris.common.Config;
import org.apache.doris.common.Log4jConfig;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.netty.handler.codec.http.HttpMethod;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class LogAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(LogAction.class);
    private static long WEB_LOG_BYTES = 1024 * 1024;  // 1MB

    private String addVerboseName;
    private String delVerboseName;

    public LogAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/log", new LogAction(controller));
    }
    
    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        // get parameters
        addVerboseName = request.getSingleParameter("add_verbose");
        delVerboseName = request.getSingleParameter("del_verbose");
        LOG.info("add verbose name: {}, del verbose name: {}", addVerboseName, delVerboseName);

        appendLogConf(response.getContent());
        appendLogInfo(response.getContent());
        
        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendLogConf(StringBuilder buffer) {
        buffer.append("<h2>Log Configuration</h2>");
        try {
            Log4jConfig.Tuple<String, String[], String[]> configs = Log4jConfig.updateLogging(null, null, null);
            if (!Strings.isNullOrEmpty(addVerboseName)) {
                addVerboseName = addVerboseName.trim();
                List<String> verboseNames = Lists.newArrayList(configs.y);
                if (!verboseNames.contains(addVerboseName)) {
                    verboseNames.add(addVerboseName);
                    configs = Log4jConfig.updateLogging(null, verboseNames.toArray(new String[verboseNames.size()]),
                                                        null);
                }
            }
            if (!Strings.isNullOrEmpty(delVerboseName)) {
                delVerboseName = delVerboseName.trim();
                List<String> verboseNames = Lists.newArrayList(configs.y);
                if (verboseNames.contains(delVerboseName)) {
                    verboseNames.remove(delVerboseName);
                    configs = Log4jConfig.updateLogging(null, verboseNames.toArray(new String[verboseNames.size()]),
                                                        null);
                }
            }

            buffer.append("Level: " + configs.x + "<br/>");
            buffer.append("Verbose Names: " + StringUtils.join(configs.y, ",") + "<br/>");
            buffer.append("Audit Names: " + StringUtils.join(configs.z, ",") + "<br/>");
            appendUpdateVerboseButton(buffer, "add_verbose");
            appendUpdateVerboseButton(buffer, "del_verbose");
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }

    private void appendUpdateVerboseButton(StringBuilder buffer, String type) {
        String placeHolder = "";
        String buttonName = "";
        if (type.equals("add_verbose")) {
            placeHolder = "new verbose name";
            buttonName = "Add";
        } else if (type.equals("del_verbose")) {
            placeHolder = "del verbose name";
            buttonName = "Delete";
        } else {
            return;
        }

        buffer.append("<form>"
                + "<div class=\"col-lg-3\" style=\"padding-left: 0px;\">"
                + "    <div class=\"input-group\">"
                + "        <input name = \"" + type + "\" type=\"text\" class=\"form-control\" placeholder=\""
                + placeHolder + "\">"
                + "        <span class=\"input-group-btn\" style=\"padding-left: 0px;\">"
                + "            <button class=\"btn btn-default\" type=\"submit\">" + buttonName + "</button>"
                + "        </span>\n"
                + "    </div>\n"
                + "</div>"
                + "</form>");
    }

    private void appendLogInfo(StringBuilder buffer) {
        buffer.append("<br/><h2>Log Contents</h2>");

        final String logPath = Config.sys_log_dir + "/fe.warn.log";
        buffer.append("Log path is: " + logPath + "<br/>");
        
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(logPath, "r");
            long fileSize = raf.length();
            long startPos = fileSize < WEB_LOG_BYTES ? 0L : fileSize - WEB_LOG_BYTES;
            long webContentLength = fileSize < WEB_LOG_BYTES ? fileSize : WEB_LOG_BYTES;
            raf.seek(startPos);
            buffer.append("<p>Showing last " + webContentLength + " bytes of log</p>");
            buffer.append("<pre>");
            String fileBuffer = null;
            while ((fileBuffer = raf.readLine()) != null) {
                buffer.append(fileBuffer).append("\n");
            }
            buffer.append("</pre>");
        } catch (FileNotFoundException e) {
            buffer.append("<p class=\"text-error\">Couldn't open log file: " 
                    + logPath + "</p>"); 
        } catch (IOException e) {
            buffer.append("<p class=\"text-error\">Failed to read log file: " 
                    + logPath + "</p>"); 
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                LOG.warn("fail to close log file: " + logPath, e);
            }
        }
    }
}

