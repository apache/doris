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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.ProcDirInterface;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/rest/v1")
public class SystemController extends BaseController {

    private static final Logger LOG = LogManager.getLogger(SystemController.class);


    @RequestMapping(path = "/system", method = RequestMethod.GET)
    public Object system(HttpServletRequest request) {
        String currentPath = request.getParameter("path");
        if (Strings.isNullOrEmpty(currentPath)) {
            currentPath = "/";
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("get /system request, thread id: {}", Thread.currentThread().getId());
        }
        ResponseEntity entity = appendSystemInfo(currentPath, currentPath, request);
        return entity;
    }

    protected ProcNodeInterface getProcNode(String path) {
        ProcService instance = ProcService.getInstance();
        ProcNodeInterface node;
        try {
            if (Strings.isNullOrEmpty(path)) {
                node = instance.open("/");
            } else {
                node = instance.open(path);
            }
        } catch (AnalysisException e) {
            LOG.warn(e.getMessage());
            return null;
        }
        return node;
    }

    private ResponseEntity appendSystemInfo(String procPath, String path, HttpServletRequest request) {
        UrlValidator validator = new UrlValidator();

        ProcNodeInterface procNode = getProcNode(procPath);
        if (procNode == null) {
            return ResponseEntityBuilder.notFound("No such proc path[" + path + "]");
        }
        boolean isDir = (procNode instanceof ProcDirInterface);

        List<String> columnNames = null;
        List<List<String>> rows = null;
        if (!Env.getCurrentEnv().isMaster()) {
            // forward to master
            String showProcStmt = "SHOW PROC \"" + procPath + "\"";

            MasterOpExecutor masterOpExecutor = new MasterOpExecutor(new OriginStatement(showProcStmt, 0),
                    ConnectContext.get(), RedirectStatus.FORWARD_NO_SYNC, true);
            try {
                masterOpExecutor.execute();
            } catch (Exception e) {
                LOG.warn("Fail to forward. ", e);
                return ResponseEntityBuilder.internalError("Failed to forward request to master: " + e.getMessage());
            }

            ShowResultSet resultSet = masterOpExecutor.getProxyResultSet();
            if (resultSet == null) {
                return ResponseEntityBuilder.internalError("Failed to get result from master");
            }

            columnNames = resultSet.getMetaData().getColumns().stream().map(c -> c.getName()).collect(
                    Collectors.toList());
            rows = resultSet.getResultRows();
        } else {
            ProcResult result;
            try {
                result = procNode.fetchResult();
            } catch (AnalysisException e) {
                return ResponseEntityBuilder.internalError("The result is null."
                        + "Maybe haven't be implemented completely[" + e.getMessage() + "], please check."
                        + "INFO: ProcNode type is [" + procNode.getClass().getName() + "]: "
                        + e.getMessage());
            }

            columnNames = result.getColumnNames();
            rows = result.getRows();
        }

        Preconditions.checkNotNull(columnNames);
        Preconditions.checkNotNull(rows);

        Map<String, Object> result = Maps.newHashMap();
        result.put("column_names", columnNames);
        List<String> hrefColumns = Lists.newArrayList();
        if (isDir) {
            hrefColumns.add(columnNames.get(0));
        }
        List<Map<String, Object>> list = Lists.newArrayList();
        for (List<String> strList : rows) {
            Map<String, Object> rowColumns = new HashMap<>();
            List<String> hrefPaths = Lists.newArrayList();

            for (int i = 0; i < strList.size(); i++) {
                String str = strList.get(i);
                if (isDir && i == 0) {
                    // the first column of dir proc is always a href column
                    String escapeStr = str.replace("%", "%25");
                    String uriPath = "path=" + path + "/" + escapeStr;
                    hrefPaths.add("/rest/v1/system?" + uriPath);
                } else if (validator.isValid(str)) {
                    // if the value is a URL, add it to href columns, and change the content to "URL"
                    hrefPaths.add(str);
                    str = "URL";
                    if (!hrefColumns.contains(columnNames.get(i))) {
                        hrefColumns.add(columnNames.get(i));
                    }
                }

                rowColumns.put(columnNames.get(i), str);
            }
            if (!hrefPaths.isEmpty()) {
                rowColumns.put("__hrefPaths", hrefPaths);
            }
            list.add(rowColumns);
        }
        result.put("rows", list);

        // assemble href column names
        if (!hrefColumns.isEmpty()) {
            result.put("href_columns", hrefColumns);
        }

        // add parent url
        result.put("parent_url", getParentUrl(path));

        ResponseEntity entity = ResponseEntityBuilder.ok(result);
        ((ResponseBody) entity.getBody()).setCount(list.size());
        return entity;
    }

    private String getParentUrl(String pathStr) {
        Path path = Paths.get(pathStr);
        path = path.getParent();
        if (path == null) {
            return "/rest/v1/system";
        } else {
            final String windowsFileSystemSeparator = "\\";
            if (windowsFileSystemSeparator.equals(path.getFileSystem().getSeparator())) {
                return "/rest/v1/system?path=" + path.toString().replace("\\", "/");
            }
            return "/rest/v1/system?path=" + path.toString();
        }
    }
}
