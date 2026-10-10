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

package org.apache.doris.httpv2.controller;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.interceptor.AuthInterceptor;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * HTTP Basic requests to the /rest/v1 surface, outside cloud mode.
 */
class BaseControllerBasicAuthTest {
    private final UserIdentity analyst = UserIdentity.createAnalyzedUserIdentWithIp("analyst", "%");

    private PrivPredicate checkedPredicate;
    private SessionValue issuedSession;

    @BeforeEach
    void setUp() {
        Assertions.assertFalse(Config.isCloudMode());
        checkedPredicate = null;
        issuedSession = null;
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void rejectsBasicAuthenticationWithoutPrivilege() {
        AuthInterceptor interceptor = interceptor(false);

        Assertions.assertThrows(UnauthorizedException.class,
                () -> interceptor.preHandle(request("/rest/v1/system"), response(), new Object()));
        Assertions.assertEquals(PrivPredicate.ADMIN_OR_NODE, checkedPredicate);
        Assertions.assertNull(issuedSession);
    }

    @Test
    void acceptsBasicAuthenticationWithPrivilege() {
        AuthInterceptor interceptor = interceptor(true);

        Assertions.assertTrue(interceptor.preHandle(request("/rest/v1/system"), response(), new Object()));
        Assertions.assertEquals(PrivPredicate.ADMIN_OR_NODE, checkedPredicate);
        Assertions.assertEquals(analyst, issuedSession.currentUser);
    }

    @Test
    void loginAuthenticatesAnAccountWithoutPrivilege() {
        // The UI tells an account that lacks the privilege apart from a failed sign-in, so login itself only
        // authenticates; the privilege is checked on the requests that follow.
        LoginController controller = new LoginController() {
            @Override
            public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request) {
                return authorizationInfo();
            }

            @Override
            protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo, HttpServletRequest request) {
                return analyst;
            }

            @Override
            protected void checkGlobalAuth(ConnectContext ctx, PrivPredicate predicate) {
                checkedPredicate = predicate;
                throw new UnauthorizedException("Access denied");
            }

            @Override
            protected void addSession(HttpServletRequest request, HttpServletResponse response,
                    SessionValue value) {
                issuedSession = value;
            }
        };

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) controller.login(request("/rest/v1/login"), response());
        Assertions.assertEquals(200, result.get("code"));
        Assertions.assertNull(checkedPredicate);
        Assertions.assertEquals(analyst, issuedSession.currentUser);
    }

    private AuthInterceptor interceptor(boolean privileged) {
        return new AuthInterceptor() {
            @Override
            public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request) {
                return authorizationInfo();
            }

            @Override
            protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo, HttpServletRequest request) {
                return analyst;
            }

            @Override
            protected void checkGlobalAuth(ConnectContext ctx, PrivPredicate predicate) {
                checkedPredicate = predicate;
                Assertions.assertEquals(analyst, ctx.getCurrentUserIdentity());
                if (!privileged) {
                    throw new UnauthorizedException("Access denied");
                }
            }

            @Override
            protected void addSession(HttpServletRequest request, HttpServletResponse response,
                    SessionValue value) {
                issuedSession = value;
            }
        };
    }

    private ActionAuthorizationInfo authorizationInfo() {
        ActionAuthorizationInfo authInfo = new ActionAuthorizationInfo();
        authInfo.fullUserName = analyst.getQualifiedUser();
        authInfo.password = "secret";
        authInfo.remoteIp = "127.0.0.1";
        return authInfo;
    }

    private HttpServletRequest request(String requestUri) {
        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getMethod":
                            return "GET";
                        case "getRequestURI":
                            return requestUri;
                        case "getHeader":
                            return "Authorization".equals(args[0]) ? "Basic ignored-by-test" : null;
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
