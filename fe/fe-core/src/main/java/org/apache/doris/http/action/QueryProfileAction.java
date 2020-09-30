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

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class QueryProfileAction extends WebBaseAction {

    public QueryProfileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/query_profile", new QueryProfileAction(controller));
    }

    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());
        
        String queryId = request.getSingleParameter("query_id");
        if (Strings.isNullOrEmpty(queryId)) {
            response.appendContent("");
            response.appendContent("<p class=\"text-error\"> Must specify a query_id[]</p>");
        }
        
        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr != null) {
            appendQueryProfile(response.getContent(), queryProfileStr);
            getPageFooter(response.getContent());
            writeResponse(request, response);
        } else {
            appendQueryProfile(response.getContent(), "query id " + queryId + " not found.");
            getPageFooter(response.getContent());
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }
    
    private void appendQueryProfile(StringBuilder buffer, String queryProfileStr) {
        buffer.append("<pre>");
        buffer.append(queryProfileStr);
        buffer.append("</pre>");
    }

}
