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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Pair;
import org.apache.doris.httpv2.HttpAuthManager.SessionValue;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.httpv2.ui.UiApiException;
import org.apache.doris.mysql.privilege.PrivPredicate;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class NodeActionTest {
    @Test
    void retainsOnlyRequestedClusterMembers() {
        List<Pair<String, Integer>> requested = List.of(
                Pair.of("127.0.0.1", 8030), Pair.of("evil.example.com", 8030));
        List<Pair<String, Integer>> members = List.of(Pair.of("127.0.0.1", 8030));

        List<Pair<String, Integer>> retained = NodeAction.retainClusterMembers(requested, members);

        Assertions.assertEquals(1, retained.size());
        Assertions.assertEquals("127.0.0.1", retained.get(0).first);
        Assertions.assertEquals(8030, retained.get(0).second);
    }

    @Test
    void retainsNothingWhenNoRequestedNodeIsAClusterMember() {
        List<Pair<String, Integer>> requested = List.of(Pair.of("evil.example.com", 8030));
        List<Pair<String, Integer>> members = List.of(Pair.of("127.0.0.1", 8030));

        Assertions.assertTrue(NodeAction.retainClusterMembers(requested, members).isEmpty());
    }

    /**
     * The configuration write surface stays password-authenticated. A caller with no Authorization
     * header must be rejected before any node is contacted, whatever cookie or token they present.
     */
    @Test
    void setConfigRejectsCallersWithoutBasicCredentials() {
        NodeAction action = actionWithAuthorizedSession(adminSession());
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assertions.assertThrows(UnauthorizedException.class,
                () -> action.setConfigFe(request, response, Map.of()));
        Assertions.assertThrows(UnauthorizedException.class,
                () -> action.setConfigBe(request, response, Map.of()));
    }

    @Test
    void configurationInfoHandlerRejectsCookieWithoutCsrf() {
        NodeAction action = actionWithAuthorizedSession(adminSession());

        UiApiException exception = Assertions.assertThrows(UiApiException.class,
                () -> action.configurationInfo(
                        Mockito.mock(HttpServletRequest.class), Mockito.mock(HttpServletResponse.class), "fe", null));

        Assertions.assertEquals(403, exception.getStatus().value());
        Assertions.assertEquals("UI_CSRF_INVALID", exception.getCode());
    }

    @Test
    void encodesConfigurationNamesAndValuesForDownstreamRequests() {
        Assertions.assertEquals("name+with+space", NodeAction.encodeQueryParameter("name with space"));
        Assertions.assertEquals("8+%26+4%3D2", NodeAction.encodeQueryParameter("8 & 4=2"));
    }

    private SessionValue adminSession() {
        SessionValue session = new SessionValue();
        session.currentUser = UserIdentity.createAnalyzedUserIdentWithIp("admin", "%");
        return session;
    }

    private NodeAction actionWithAuthorizedSession(SessionValue session) {
        return new NodeAction() {
            @Override
            public SessionValue requireCookieSession(
                    HttpServletRequest ignoredRequest, HttpServletResponse ignoredResponse) {
                return session;
            }

            @Override
            protected void checkGlobalAuth(UserIdentity ignoredUser, PrivPredicate ignoredPredicate) {
                // Tests isolate the CSRF boundary after successful cookie and ADMIN checks.
            }
        };
    }
}
