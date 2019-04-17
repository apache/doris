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

import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseAction;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.UnauthorizedException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public class RestBaseAction extends BaseAction {
    private static final Logger LOG = LogManager.getLogger(RestBaseAction.class);

    public RestBaseAction(ActionController controller) {
        super(controller);
    }

    @Override
    public void handleRequest(BaseRequest request) throws Exception {
        BaseResponse response = new BaseResponse();
        try {
            execute(request, response);
        } catch (DdlException e) {
            if (e instanceof UnauthorizedException) {
                response.updateHeader(HttpHeaderNames.WWW_AUTHENTICATE.toString(), "Basic realm=\"\"");
                writeResponse(request, response, HttpResponseStatus.UNAUTHORIZED);
            } else {
                sendResult(request, response, new RestBaseResult(e.getMessage()));
            }
        }
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        AuthorizationInfo authInfo = getAuthorizationInfo(request);
        // check password
        checkPassword(authInfo);
        executeWithoutPassword(authInfo, request, response);
    }

    // all derived classed should implement this method, NOT 'execute'
    protected void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
            throws DdlException {
        throw new DdlException("Not implemented");
    }

    public void sendResult(BaseRequest request, BaseResponse response, RestBaseResult result) {
        response.appendContent(result.toJson());
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    public void sendResult(BaseRequest request, BaseResponse response) {
        writeResponse(request, response, HttpResponseStatus.OK);
    }
}
