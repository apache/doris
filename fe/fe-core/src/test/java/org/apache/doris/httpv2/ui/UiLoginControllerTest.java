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

package org.apache.doris.httpv2.ui;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.security.LoginAttemptLimiter;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

public class UiLoginControllerTest {
    @Test
    void rootAndNonRootAdminsCanLogin() {
        assertAdminCanLogin(UserIdentity.ROOT);
        assertAdminCanLogin(UserIdentity.createAnalyzedUserIdentWithIp("admin", "%"));
    }

    @Test
    void authenticatedNonAdminIsRejectedBeforeASessionIsCreated() {
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("analyst", "%");
        TestController controller = new TestController(user, false, false);

        UiApiException exception = Assertions.assertThrows(
                UiApiException.class, () -> controller.login(request(), response()));

        Assertions.assertEquals(403, exception.getStatus().value());
        Assertions.assertEquals("UI_ADMIN_REQUIRED", exception.getCode());
        Assertions.assertFalse(controller.sessionCreated);
    }

    @Test
    void invalidCredentialsUseANondisclosingError() {
        TestController controller = new TestController(null, false, true);

        UiApiException exception = Assertions.assertThrows(
                UiApiException.class, () -> controller.login(request(), response()));

        Assertions.assertEquals(401, exception.getStatus().value());
        Assertions.assertEquals("UI_LOGIN_FAILED", exception.getCode());
        Assertions.assertFalse(controller.sessionCreated);
    }

    private void assertAdminCanLogin(UserIdentity user) {
        TestController controller = new TestController(user, true, false);
        UiApiResponse<UiMe> response = controller.login(request(), response());

        Assertions.assertEquals(user.getQualifiedUser(), response.getData().getUser());
        Assertions.assertTrue(response.getData().getCapabilities().contains(UiCapability.CONFIGURATION_MODIFY));
        Assertions.assertTrue(controller.sessionCreated);
    }

    private HttpServletRequest request() {
        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getRemoteAddr")) {
                        return "127.0.0.1";
                    }
                    if (method.getName().equals("getHeader") && "Authorization".equals(args[0])) {
                        return "Basic ignored-by-test-subclass";
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private HttpServletResponse response() {
        return (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(),
                new Class<?>[] {HttpServletResponse.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(Class<?> returnType) {
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

    private static class TestController extends UiLoginController {
        private final UserIdentity user;
        private final boolean admin;
        private final boolean invalidCredentials;
        private boolean sessionCreated;

        TestController(UserIdentity user, boolean admin, boolean invalidCredentials) {
            super(new LoginAttemptLimiter());
            this.user = user;
            this.admin = admin;
            this.invalidCredentials = invalidCredentials;
        }

        @Override
        public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request) {
            ActionAuthorizationInfo info = new ActionAuthorizationInfo();
            info.password = "secret";
            return info;
        }

        @Override
        protected UserIdentity authenticate(ActionAuthorizationInfo authInfo) {
            if (invalidCredentials) {
                throw new UnauthorizedException("invalid credentials");
            }
            return user;
        }

        @Override
        protected boolean hasAdminPrivilege(UserIdentity currentUser) {
            return admin;
        }

        @Override
        protected SessionValue createSession(HttpServletRequest request, HttpServletResponse response,
                UserIdentity currentUser, String password) {
            sessionCreated = true;
            SessionValue session = new SessionValue();
            session.currentUser = currentUser;
            session.password = password;
            return session;
        }
    }
}
