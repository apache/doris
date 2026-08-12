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
import org.apache.doris.httpv2.HttpAuthManager;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.controller.BaseController;
import org.apache.doris.httpv2.ui.websql.WebSqlSessionManager;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.UUID;

public class UiSessionControllerTest {
    @Test
    void logoutInvalidatesTheServerSessionAndExpiresTheCookie() {
        String sessionId = UUID.randomUUID().toString();
        SessionValue session = new SessionValue();
        session.currentUser = UserIdentity.createAnalyzedUserIdentWithIp("operator", "%");
        HttpAuthManager.getInstance().addSessionValue(sessionId, session);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getAttribute(UiRequestContext.SESSION_ATTRIBUTE)).thenReturn(session);
        Mockito.when(request.getAttribute(UiRequestContext.REQUEST_ID_ATTRIBUTE)).thenReturn("req-logout");
        Mockito.when(request.getCookies()).thenReturn(
                new Cookie[] {new Cookie(BaseController.PALO_SESSION_ID, sessionId)});

        WebSqlSessionManager webSqlSessionManager = Mockito.mock(WebSqlSessionManager.class);
        UiApiResponse<UiLogoutResult> result = new UiSessionController(webSqlSessionManager).logout(request, response);

        Assertions.assertTrue(result.getData().isLoggedOut());
        Assertions.assertNull(HttpAuthManager.getInstance().getSessionValue(Collections.singletonList(sessionId)));
        ArgumentCaptor<Cookie> cookieCaptor = ArgumentCaptor.forClass(Cookie.class);
        Mockito.verify(response).addCookie(cookieCaptor.capture());
        Cookie expired = cookieCaptor.getValue();
        Assertions.assertEquals(BaseController.PALO_SESSION_ID, expired.getName());
        Assertions.assertEquals(0, expired.getMaxAge());
        Assertions.assertTrue(expired.isHttpOnly());
        Assertions.assertEquals("/", expired.getPath());
        Assertions.assertEquals("Lax", expired.getAttribute("SameSite"));
        Mockito.verify(webSqlSessionManager).closeSessionsForOwner(session.currentUser.getQualifiedUser());
    }
}
