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
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.security.CsrfTokenUtils;
import org.apache.doris.httpv2.websql.WebSqlError;
import org.apache.doris.httpv2.websql.WebSqlException;
import org.apache.doris.httpv2.websql.WebSqlRequestContext;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

class WebSqlAuthInterceptorTest {
    private final UserIdentity admin = UserIdentity.createAnalyzedUserIdentWithIp("admin", "%");
    private final Map<String, Object> attributes = new HashMap<>();

    @Test
    void acceptsAdminCookieWithCsrfToken() {
        SessionValue session = session();
        AuthInterceptor interceptor = cookieInterceptor(session, true);

        Assertions.assertTrue(interceptor.preHandle(
                request(null, session.csrfToken), response(), new Object()));
        WebSqlRequestContext.Authentication authentication =
                (WebSqlRequestContext.Authentication) attributes.get(WebSqlRequestContext.AUTH_ATTRIBUTE);
        Assertions.assertEquals(admin.getQualifiedUser(), authentication.getOwner());
        Assertions.assertEquals("secret", authentication.getPassword());
    }

    @Test
    void rejectsCookieMutationWithoutCsrfToken() {
        WebSqlException exception = Assertions.assertThrows(WebSqlException.class,
                () -> cookieInterceptor(session(), true).preHandle(request(null, null), response(), new Object()));
        Assertions.assertEquals(WebSqlError.CSRF_INVALID, exception.getError());
    }

    @Test
    void acceptsAdminBasicAuthenticationWithoutCsrfToken() {
        AuthInterceptor interceptor = new AuthInterceptor() {
            @Override
            public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request) {
                ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
                authInfo.fullUserName = "admin";
                authInfo.password = "secret";
                return authInfo;
            }

            @Override
            protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo) {
                return admin;
            }

            @Override
            protected boolean hasAdminPrivilege(UserIdentity user) {
                return true;
            }
        };

        Assertions.assertTrue(interceptor.preHandle(
                request("Basic ignored-by-test", null), response(), new Object()));
        WebSqlRequestContext.Authentication authentication =
                (WebSqlRequestContext.Authentication) attributes.get(WebSqlRequestContext.AUTH_ATTRIBUTE);
        Assertions.assertEquals(admin.getQualifiedUser(), authentication.getOwner());
    }

    @Test
    void rejectsAuthenticatedUserWithoutAdminPrivilege() {
        WebSqlException exception = Assertions.assertThrows(WebSqlException.class,
                () -> cookieInterceptor(session(), false).preHandle(
                        request(null, session().csrfToken), response(), new Object()));
        Assertions.assertEquals(WebSqlError.ACCESS_DENIED, exception.getError());
    }

    private AuthInterceptor cookieInterceptor(SessionValue session, boolean adminPrivilege) {
        return new AuthInterceptor() {
            @Override
            public SessionValue requireCookieSession(
                    HttpServletRequest request, HttpServletResponse response) {
                return session;
            }

            @Override
            protected boolean hasAdminPrivilege(UserIdentity user) {
                return adminPrivilege;
            }
        };
    }

    private SessionValue session() {
        SessionValue session = new SessionValue();
        session.currentUser = admin;
        session.password = "secret";
        return session;
    }

    private HttpServletRequest request(String authorization, String csrfToken) {
        attributes.clear();
        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getMethod":
                            return "POST";
                        case "getRequestURI":
                            return "/rest/v1/sql-sessions";
                        case "getHeader":
                            if ("Authorization".equals(args[0])) {
                                return authorization;
                            }
                            if (CsrfTokenUtils.HEADER_NAME.equals(args[0])) {
                                return csrfToken;
                            }
                            return null;
                        case "setAttribute":
                            attributes.put((String) args[0], args[1]);
                            return null;
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
    }

    private HttpServletResponse response() {
        return (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(),
                new Class<?>[] {HttpServletResponse.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return 0;
    }
}
