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

package com.baidu.palo.http.rest;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseAction;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.UnauthorizedException;
import com.baidu.palo.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

import io.netty.handler.codec.http.HttpHeaders;
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
                response.updateHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Basic realm=\"\"");
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

    public void redirectTo(BaseRequest request, BaseResponse response, TNetworkAddress addr)
            throws DdlException {
        String urlStr = request.getRequest().uri();
        URI urlObj = null;
        URI resultUriObj = null;
        try {
            urlObj = new URI(urlStr);
            resultUriObj = new URI("http", null, addr.getHostname(),
                                   addr.getPort(), urlObj.getPath(), urlObj.getQuery(), null);
        } catch (URISyntaxException e) {
            LOG.warn(e.getMessage());
            throw new DdlException(e.getMessage());
        }
        response.updateHeader(HttpHeaders.Names.LOCATION, resultUriObj.toString());
        writeResponse(request, response, HttpResponseStatus.TEMPORARY_REDIRECT);
    }

    public boolean redirectToMaster(BaseRequest request, BaseResponse response) throws DdlException {
        Catalog catalog = Catalog.getInstance();
        if (catalog.isMaster()) {
            return false;
        }
        redirectTo(request, response, new TNetworkAddress(catalog.getMasterIp(), catalog.getMasterHttpPort()));
        return true;
    }
}
