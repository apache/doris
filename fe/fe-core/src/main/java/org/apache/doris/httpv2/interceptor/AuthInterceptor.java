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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.security.CsrfTokenUtils;
import org.apache.doris.httpv2.ui.UiApiException;
import org.apache.doris.httpv2.ui.UiRequestContext;
import org.apache.doris.httpv2.websql.WebSqlError;
import org.apache.doris.httpv2.websql.WebSqlException;
import org.apache.doris.httpv2.websql.WebSqlRequestContext;
import org.apache.doris.mysql.privilege.PrivPredicate;

import com.google.common.collect.ImmutableSet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Set;

public class AuthInterceptor extends BaseController implements HandlerInterceptor {

    private static final Logger LOG = LogManager.getLogger(AuthInterceptor.class);
    private static final String UI_API_PREFIX = "/rest/v1/ui/";
    private static final String WEB_SQL_API_PREFIX = "/rest/v1/sql-sessions";
    private static final Set<String> MUTATING_METHODS = ImmutableSet.of("POST", "PUT", "PATCH", "DELETE");

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("get prehandle. thread: {}", Thread.currentThread().getId());
        }

        String method = request.getMethod();
        if (method.equalsIgnoreCase(RequestMethod.OPTIONS.toString())) {
            response.setStatus(HttpStatus.NO_CONTENT.value());
            return true;
        }

        if (request.getRequestURI().startsWith(UI_API_PREFIX)) {
            authenticateUiRequest(request, response);
        } else if (isWebSqlApi(request.getRequestURI())) {
            authenticateWebSqlRequest(request, response);
        } else {
            checkAuthWithCookie(request, response);
        }
        return true;
    }

    private boolean isWebSqlApi(String requestUri) {
        return WEB_SQL_API_PREFIX.equals(requestUri)
                || requestUri.startsWith(WEB_SQL_API_PREFIX + "/");
    }

    protected void authenticateUiRequest(HttpServletRequest request, HttpServletResponse response) {
        SessionValue session;
        try {
            session = requireCookieSession(request, response);
        } catch (UnauthorizedException exception) {
            throw UiApiException.unauthenticated();
        }
        request.setAttribute(UiRequestContext.SESSION_ATTRIBUTE, session);
        if (!hasAdminPrivilege(session)) {
            throw UiApiException.adminRequired();
        }
        if (MUTATING_METHODS.contains(request.getMethod().toUpperCase())
                && !CsrfTokenUtils.csrfTokenMatches(
                        session.csrfToken, request.getHeader(CsrfTokenUtils.HEADER_NAME))) {
            throw UiApiException.invalidCsrf();
        }
    }

    protected void authenticateWebSqlRequest(HttpServletRequest request, HttpServletResponse response) {
        try {
            if (request.getHeader("Authorization") != null) {
                ActionAuthorizationInfo authInfo = getAuthorizationInfo(request);
                UserIdentity user = checkPassword(authInfo, request);
                if (Config.isCloudMode()) {
                    checkInstanceOverdue(user);
                }
                requireAdmin(user);
                WebSqlRequestContext.set(request, user, authInfo.password);
                return;
            }

            SessionValue session = requireCookieSession(request, response);
            requireAdmin(session.currentUser);
            if (MUTATING_METHODS.contains(request.getMethod().toUpperCase())
                    && !CsrfTokenUtils.csrfTokenMatches(
                            session.csrfToken, request.getHeader(CsrfTokenUtils.HEADER_NAME))) {
                throw new WebSqlException(WebSqlError.CSRF_INVALID);
            }
            WebSqlRequestContext.set(request, session.currentUser, session.password);
        } catch (UnauthorizedException exception) {
            throw new WebSqlException(WebSqlError.AUTHENTICATION_REQUIRED, exception);
        }
    }

    protected boolean hasAdminPrivilege(SessionValue session) {
        return hasAdminPrivilege(session.currentUser);
    }

    protected boolean hasAdminPrivilege(UserIdentity user) {
        return Env.getCurrentEnv().getAccessManager().checkGlobalPriv(user, PrivPredicate.ADMIN);
    }

    private void requireAdmin(UserIdentity user) {
        if (!hasAdminPrivilege(user)) {
            throw new WebSqlException(WebSqlError.ACCESS_DENIED);
        }
    }
}
