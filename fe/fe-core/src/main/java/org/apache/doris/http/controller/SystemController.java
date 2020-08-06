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

package org.apache.doris.http.controller;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.ProcDirInterface;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.http.entity.ResponseBody;
import org.apache.doris.http.entity.ResponseEntityBuilder;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/rest/v1")
public class SystemController extends BaseController{

    private static final Logger LOG = LogManager.getLogger(SystemController.class);


    @RequestMapping(path = "/system", method = RequestMethod.GET)
    public Object system(HttpServletRequest request) {
        String currentPath = request.getParameter("path");
        if (Strings.isNullOrEmpty(currentPath)) {
            currentPath = "/";
        }
        LOG.debug("get /system requset, thread id: {}", Thread.currentThread().getId());
        ResponseEntity entity = appendSystemInfo(currentPath, currentPath,request);
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

    private ResponseEntity appendSystemInfo(String procPath, String path,HttpServletRequest request) {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();

        ProcNodeInterface procNode = getProcNode(procPath);
        if (procNode == null) {
            return ResponseEntityBuilder.notFound("No such proc path[" + path + "]");
        }
        boolean isDir = (procNode instanceof ProcDirInterface);

        List<String> columnNames = null;
        List<List<String>> rows = null;
        if (!Catalog.getCurrentCatalog().isMaster()) {
            // forward to master
            String showProcStmt = "SHOW PROC \"" + procPath + "\"";

            MasterOpExecutor masterOpExecutor = new MasterOpExecutor(new OriginStatement(showProcStmt, 0),
                    ConnectContext.get(), RedirectStatus.FORWARD_NO_SYNC);
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


        for (List<String> strList : rows) {
            Map<String, String> resultMap = new HashMap<>();

            int columnIndex = 1;
            for (int i = 0; i < strList.size(); i++) {
                if (isDir && columnIndex == 1) {
                    String str = strList.get(i);
                    resultMap.put(columnNames.get(0), str);
                    String escapeStr = str.replace("%", "%25");
                    String uriPath = "path=" + path + "/" + escapeStr;
                    resultMap.put("hrefPath", uriPath);
                } else {
                    resultMap.put(columnNames.get(i), strList.get(i));
                }
                ++columnIndex;
            }
            list.add(resultMap);
        }
        ResponseEntity entity = ResponseEntityBuilder.ok(list);
        ((ResponseBody) entity.getBody()).setCount(list.size());
        return entity;
    }

    // some expamle:
    //   '/'            => '/'
    //   '///aaa'       => '///'
    //   '/aaa/bbb///'  => '/aaa'
    //   '/aaa/bbb/ccc' => '/aaa/bbb'
    // ATTN: the root path's parent is itself.
    private String getParentPath(String path) {
        int lastSlashIndex = path.length() - 1;
        while (lastSlashIndex > 0) {
            int tempIndex = path.lastIndexOf('/', lastSlashIndex);
            if (tempIndex > 0) {
                if (tempIndex == lastSlashIndex) {
                    lastSlashIndex = tempIndex - 1;
                    continue;
                } else if (tempIndex < lastSlashIndex) { // '//aaa/bbb'
                    lastSlashIndex = tempIndex;
                    return path.substring(0, lastSlashIndex);
                }
            }
        }
        return "/";
    }
}
