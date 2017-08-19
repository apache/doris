// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http.action;

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.proc.ProcDirInterface;
import com.baidu.palo.common.proc.ProcNodeInterface;
import com.baidu.palo.common.proc.ProcResult;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;

import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

public class SystemAction extends WebBaseAction {

    public SystemAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/system", new SystemAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        String currentPath = request.getSingleParameter("path");
        ProcNodeInterface node = null;
        // root path is default path
        if (Strings.isNullOrEmpty(currentPath)) {
            currentPath = "/";
        }

        node = getProcNode(currentPath);
        appendSystemInfo(response.getContent(), node, currentPath);

        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendSystemInfo(StringBuilder buffer, ProcNodeInterface procNode, String path) {
        buffer.append("<h2>System Info</h2>");
        buffer.append("<p>This page lists the system info, like /proc in Linux.</p>");
        buffer.append("<p class=\"text-info\"> Current path: " + path + "</p>");

        if (procNode == null) {
            buffer.append("<p class=\"text-error\"> No such proc path["
                    + path
                    + "]</p>");
            return;
        }

        boolean isDir = false;
        if (procNode instanceof ProcDirInterface) {
            isDir = true;
        }

        ProcResult result;
        try {
            result = procNode.fetchResult();
        } catch (AnalysisException e) {
            buffer.append("<p class=\"text-error\"> The result is null, "
                    + "maybe haven't be implemented completely[" + e.getMessage() + "], please check.</p>");
            buffer.append("<p class=\"text-info\"> "
                    + "INFO: ProcNode type is [" + procNode.getClass().getName()
                    + "]</p>");
            return;
        }

        List<String> columnNames = result.getColumnNames();
        List<List<String>> rows = result.getRows();

        appendBackButton(buffer, path);
        appendTableHeader(buffer, columnNames);
        appendSystemTableBody(buffer, rows, isDir, path);
        appendTableFooter(buffer);
    }

    private void appendBackButton(StringBuilder buffer, String path) {
        String parentDir = getParentPath(path);
        buffer.append("<a href=\"?path=" + parentDir
                + "\" class=\"btn btn-primary\">"
                + "parent dir</a>");
    }

    private void appendSystemTableBody(StringBuilder buffer, List<List<String>> rows, boolean isDir, String path) {
        for ( List<String> strList : rows) {
            buffer.append("<tr>");
            int columnIndex = 1;
            for (String str : strList) {
                buffer.append("<td>");
                if (isDir && columnIndex == 1) {
                    buffer.append("<a href=\"?path=" + path + "/" + str + "\">");
                    buffer.append(str);
                    buffer.append("</a>");
                } else {
                    buffer.append(str.replaceAll("\\n", "<br/>"));
                }
                buffer.append("</td>");
                ++columnIndex;
            }
            buffer.append("</tr>");
        }
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
