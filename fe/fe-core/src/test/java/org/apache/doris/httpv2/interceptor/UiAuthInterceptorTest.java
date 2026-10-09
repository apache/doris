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
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.ui.UiApiException;
import org.apache.doris.httpv2.ui.UiRequestContext;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

class UiAuthInterceptorTest {
    private SessionValue session;
    private HttpServletResponse response;
    private final Map<String, String> responseHeaders = new HashMap<>();
    private final Map<String, Object> requestAttributes = new HashMap<>();

    @BeforeEach
    void setUp() {
        session = new SessionValue();
        session.currentUser = UserIdentity.createAnalyzedUserIdentWithIp("analyst", "%");
        responseHeaders.clear();
        requestAttributes.clear();
        response = response();
    }

    @Test
    void acceptsAValidCookieSessionForReads() {
        HttpServletRequest request = request("GET", null);
        AuthInterceptor interceptor = interceptorReturning(session);

        Assertions.assertTrue(interceptor.preHandle(request, response, new Object()));
        Assertions.assertSame(session, requestAttributes.get(UiRequestContext.SESSION_ATTRIBUTE));
    }

    @Test
    void rejectsAValidSessionAfterAdminPrivilegeIsRevoked() {
        HttpServletRequest request = request("GET", null);
        AuthInterceptor interceptor = new AuthInterceptor() {
            @Override
            public SessionValue requireCookieSession(
                    HttpServletRequest ignoredRequest, HttpServletResponse ignoredResponse) {
                return session;
            }

            @Override
            protected boolean hasAdminPrivilege(SessionValue ignoredSession) {
                return false;
            }
        };

        UiApiException exception = Assertions.assertThrows(
                UiApiException.class, () -> interceptor.preHandle(request, response, new Object()));
        Assertions.assertEquals(403, exception.getStatus().value());
        Assertions.assertEquals("UI_ADMIN_REQUIRED", exception.getCode());
    }

    @Test
    void rejectsAnInvalidOrExpiredCookieSession() {
        HttpServletRequest request = request("GET", null);
        AuthInterceptor interceptor = new AuthInterceptor() {
            @Override
            public SessionValue requireCookieSession(
                    HttpServletRequest ignoredRequest, HttpServletResponse ignoredResponse) {
                throw new UnauthorizedException("Cookie is invalid");
            }
        };

        UiApiException exception = Assertions.assertThrows(
                UiApiException.class, () -> interceptor.preHandle(request, response, new Object()));
        Assertions.assertEquals(401, exception.getStatus().value());
        Assertions.assertEquals("UI_UNAUTHENTICATED", exception.getCode());
    }

    @Test
    void rejectsMissingAndIncorrectCsrfTokensForMutations() {
        AuthInterceptor interceptor = interceptorReturning(session);

        UiApiException missing = Assertions.assertThrows(
                UiApiException.class,
                () -> interceptor.preHandle(request("POST", null), response, new Object()));
        UiApiException incorrect = Assertions.assertThrows(
                UiApiException.class,
                () -> interceptor.preHandle(request("DELETE", "wrong"), response, new Object()));

        Assertions.assertEquals("UI_CSRF_INVALID", missing.getCode());
        Assertions.assertEquals(403, incorrect.getStatus().value());
    }

    @Test
    void acceptsTheSessionCsrfTokenForMutations() {
        AuthInterceptor interceptor = interceptorReturning(session);

        Assertions.assertTrue(interceptor.preHandle(
                request("PATCH", session.csrfToken), response, new Object()));
    }

    private HttpServletRequest request(String method, String csrfToken) {
        return request(method, csrfToken, "/rest/v1/ui/me");
    }

    private HttpServletRequest request(String method, String csrfToken, String requestUri) {
        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, calledMethod, args) -> {
                    switch (calledMethod.getName()) {
                        case "getMethod":
                            return method;
                        case "getRequestURI":
                            return requestUri;
                        case "getHeader":
                            return UiRequestContext.CSRF_HEADER.equals(args[0]) ? csrfToken : null;
                        case "setAttribute":
                            requestAttributes.put((String) args[0], args[1]);
                            return null;
                        case "getAttribute":
                            return requestAttributes.get(args[0]);
                        default:
                            return defaultValue(calledMethod.getReturnType());
                    }
                });
    }

    private HttpServletResponse response() {
        return (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(),
                new Class<?>[] {HttpServletResponse.class},
                (proxy, calledMethod, args) -> {
                    if (calledMethod.getName().equals("setHeader")) {
                        responseHeaders.put((String) args[0], (String) args[1]);
                    }
                    return defaultValue(calledMethod.getReturnType());
                });
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
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        return 0D;
    }

    private AuthInterceptor interceptorReturning(SessionValue value) {
        return new AuthInterceptor() {
            @Override
            public SessionValue requireCookieSession(
                    HttpServletRequest ignoredRequest, HttpServletResponse ignoredResponse) {
                return value;
            }

            @Override
            protected boolean hasAdminPrivilege(SessionValue ignoredSession) {
                return true;
            }
        };
    }
}
