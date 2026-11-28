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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.controller.BaseController.ActionAuthorizationInfo;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.qe.ConnectContext;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * /metrics follows enable_all_http_auth: any authenticated account may scrape it, and a scrape
 * without valid credentials gets a real HTTP 401 instead of the JSON envelope.
 */
public class MetricsActionTest {
    private static final String SCRAPER_CREDENTIALS = "Basic "
            + Base64.getEncoder().encodeToString("scraper:secret".getBytes(StandardCharsets.UTF_8));

    // An account with no privileges at all: /metrics checks the password only.
    private final UserIdentity scraper = UserIdentity.createAnalyzedUserIdentWithIp("scraper", "%");

    private boolean originalEnableAllHttpAuth;
    private StringWriter body;
    private HttpServletResponse response;

    @BeforeEach
    void setUp() throws Exception {
        originalEnableAllHttpAuth = Config.enable_all_http_auth;
        body = new StringWriter();
        response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(response.getWriter()).thenReturn(new PrintWriter(body));
    }

    @AfterEach
    void tearDown() {
        Config.enable_all_http_auth = originalEnableAllHttpAuth;
        ConnectContext.remove();
    }

    @Test
    void rejectsScrapeWithoutCredentials() {
        Config.enable_all_http_auth = true;

        new MetricsAction().execute(request(null), response);

        Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        Mockito.verify(response).setHeader("WWW-Authenticate", "Basic realm=\"Restricted\"");
        Assertions.assertEquals("Need auth information.", body.toString());
    }

    @Test
    void rejectsScrapeWithWrongPassword() {
        Config.enable_all_http_auth = true;
        MetricsAction action = new MetricsAction() {
            @Override
            protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo, HttpServletRequest request) {
                Assertions.assertEquals("scraper", authInfo.fullUserName);
                throw new UnauthorizedException("Access denied for user 'scraper'");
            }
        };

        action.execute(request(SCRAPER_CREDENTIALS), response);

        Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        Mockito.verify(response).setHeader("WWW-Authenticate", "Basic realm=\"Restricted\"");
        Assertions.assertEquals("Access denied for user 'scraper'", body.toString());
    }

    @Test
    void servesAnyAuthenticatedAccount() {
        Config.enable_all_http_auth = true;
        MetricsAction action = new MetricsAction() {
            @Override
            protected UserIdentity checkPassword(ActionAuthorizationInfo authInfo, HttpServletRequest request) {
                return scraper;
            }
        };

        action.execute(request(SCRAPER_CREDENTIALS), response);

        Mockito.verify(response, Mockito.never()).setStatus(Mockito.anyInt());
        Mockito.verify(response, Mockito.never()).setHeader(Mockito.eq("WWW-Authenticate"), Mockito.anyString());
        Mockito.verify(response).setContentType("text/plain");
        Assertions.assertEquals(scraper, ConnectContext.get().getCurrentUserIdentity());
    }

    @Test
    void servesAnonymousScrapeWhenAuthIsOff() {
        Config.enable_all_http_auth = false;
        MetricsAction action = new MetricsAction() {
            @Override
            public ActionAuthorizationInfo getAuthorizationInfo(HttpServletRequest request) {
                throw new AssertionError("/metrics must not authenticate while enable_all_http_auth is off");
            }
        };

        action.execute(request(null), response);

        Mockito.verify(response, Mockito.never()).setStatus(Mockito.anyInt());
        Mockito.verify(response).setContentType("text/plain");
    }

    private HttpServletRequest request(String authorization) {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getHeader("Authorization")).thenReturn(authorization);
        Mockito.when(request.getRequestURI()).thenReturn("/metrics");
        Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        return request;
    }
}
