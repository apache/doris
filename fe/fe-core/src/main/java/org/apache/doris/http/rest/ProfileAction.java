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

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

// This class is a RESTFUL interface to get query profile.
// It will be used in query monitor to collect profiles.   
// Usage:
//      wget http://fe_host:fe_http_port/api/profile?query_id=123456
public class ProfileAction extends RestBaseAction {

    public ProfileAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/profile", new ProfileAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String queryId = request.getSingleParameter("query_id");
        if (queryId == null) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr != null) {
            response.getContent().append(queryProfileStr);
            sendResult(request, response);
        } else {
            response.getContent().append("query id " + queryId + " not found.");
            sendResult(request, response, HttpResponseStatus.NOT_FOUND);
        }
    }
}
