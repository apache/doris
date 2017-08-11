// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.http.action;

import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.qe.ConnectContext;

import com.google.common.collect.Lists;

import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.List;

public class SessionAction extends WebBaseAction {
    // we make 
    private static final ArrayList<String> SESSION_TABLE_HEADER = Lists.newArrayList();
    
    static {
        SESSION_TABLE_HEADER.add("Id");
        SESSION_TABLE_HEADER.add("User");
        SESSION_TABLE_HEADER.add("Host");
        SESSION_TABLE_HEADER.add("Db");
        SESSION_TABLE_HEADER.add("Command");
        SESSION_TABLE_HEADER.add("Time");
        SESSION_TABLE_HEADER.add("State");
        SESSION_TABLE_HEADER.add("Info");
    }

    public SessionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/session", new SessionAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());
        appendSessionInfo(response.getContent());
        getPageFooter(response.getContent());
        writeResponse(request, response);
    }
    
    private void appendSessionInfo(StringBuilder buffer) {
        buffer.append("<h2>Session Info</h2>");
        
        List<ConnectContext.ThreadInfo> threadInfos = 
                qeService.getMysqlServer().getScheduler().listConnection("root");
        List<List<String>> rowSet = Lists.newArrayList();
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(nowMs));
        }
        
        buffer.append("<p>This page lists the session info, there are "
                + rowSet.size()
                + "active sessions.</p>");
        
        appendTableHeader(buffer, SESSION_TABLE_HEADER);
        appendTableBody(buffer, rowSet);
        appendTableFooter(buffer);
    }

}
