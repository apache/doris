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

package org.apache.doris.httpv2.interceptor;

import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.security.UiSecurityTokens;
import org.apache.doris.httpv2.ui.UiApiException;
import org.apache.doris.httpv2.ui.UiRequestContext;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.google.common.collect.ImmutableSet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Set;

public class UiAuthInterceptor extends BaseController implements HandlerInterceptor {
    private static final Set<String> MUTATING_METHODS = ImmutableSet.of("POST", "PUT", "PATCH", "DELETE");
    private static final String LOGIN_PATH = "/rest/v1/ui/login";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String requestId = UiRequestContext.newRequestId();
        request.setAttribute(UiRequestContext.REQUEST_ID_ATTRIBUTE, requestId);
        response.setHeader(UiRequestContext.REQUEST_ID_HEADER, requestId);

        if (request.getMethod().equalsIgnoreCase(RequestMethod.OPTIONS.toString())) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
            return true;
        }

        if (LOGIN_PATH.equals(request.getRequestURI())) {
            return true;
        }

        SessionValue session;
        try {
            session = authenticate(request, response);
        } catch (UnauthorizedException exception) {
            throw UiApiException.unauthenticated();
        }
        request.setAttribute(UiRequestContext.SESSION_ATTRIBUTE, session);

        if (!hasAdminPrivilege(session)) {
            throw UiApiException.adminRequired();
        }

        if (MUTATING_METHODS.contains(request.getMethod().toUpperCase())
                && !UiSecurityTokens.csrfTokenMatches(
                        session.csrfToken, request.getHeader(UiRequestContext.CSRF_HEADER))) {
            throw UiApiException.invalidCsrf();
        }
        return true;
    }

    protected SessionValue authenticate(HttpServletRequest request, HttpServletResponse response) {
        return checkUiAuthWithCookie(request, response);
    }

    protected boolean hasAdminPrivilege(SessionValue session) {
        return Env.getCurrentEnv().getAccessManager().checkGlobalPriv(session.currentUser, PrivPredicate.ADMIN);
    }
}
