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

package org.apache.doris.httpv2.ui.websql;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.ui.UiRequestContext;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Collections;
import java.util.Map;

public class WebSqlSessionControllerTest {
    private WebSqlSessionManager manager;
    private WebSqlSessionController controller;
    private HttpServletRequest request;
    private SessionValue login;

    @BeforeEach
    void setUp() {
        manager = Mockito.mock(WebSqlSessionManager.class);
        controller = new WebSqlSessionController(manager);
        request = Mockito.mock(HttpServletRequest.class);
        login = new SessionValue();
        login.currentUser = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
        login.password = "secret";
        Mockito.when(request.getAttribute(UiRequestContext.SESSION_ATTRIBUTE)).thenReturn(login);
    }

    @Test
    void createUsesAuthenticatedCookieIdentityAndPassword() {
        WebSqlSession session = session("session-1");
        Mockito.when(manager.createSession(login.currentUser.getQualifiedUser(), "secret")).thenReturn(session);

        WebSqlSessionInfo response = controller.create(request);

        Assertions.assertEquals("session-1", response.getSessionId());
        Mockito.verify(manager).createSession(login.currentUser.getQualifiedUser(), "secret");
    }

    @Test
    void executeCancelResetAndCloseRemainOwnerBound() {
        String owner = login.currentUser.getQualifiedUser();
        WebSqlExecutionResult execution = new WebSqlExecutionResult(Collections.emptyList(),
                Collections.emptyList(), 0, 2, "query-1", Collections.emptyList(),
                "internal", "tpcds", false);
        Mockito.when(manager.execute("session-1", owner, "SELECT 1")).thenReturn(execution);
        Mockito.when(manager.cancel("session-1", owner)).thenReturn(true);
        Mockito.when(manager.reset("session-1", owner, "secret")).thenReturn(session("session-1"));
        Mockito.when(manager.closeSession("session-1", owner)).thenReturn(true);
        Map<String, String> statement = Collections.singletonMap("sql", "SELECT 1");

        Assertions.assertSame(execution, controller.execute("session-1", statement, request));
        Assertions.assertTrue(controller.cancel("session-1", request).get("cancelRequested"));
        Assertions.assertEquals("session-1", controller.reset("session-1", request).getSessionId());
        Assertions.assertTrue(controller.close("session-1", request).get("closed"));

        Mockito.verify(manager).execute("session-1", owner, "SELECT 1");
        Mockito.verify(manager).cancel("session-1", owner);
        Mockito.verify(manager).reset("session-1", owner, "secret");
        Mockito.verify(manager).closeSession("session-1", owner);
    }

    private WebSqlSession session(String id) {
        return new WebSqlSession(id, login.currentUser.getQualifiedUser(), Mockito.mock(Connection.class), 10);
    }
}
